"""
Books Library Module
====================
Completely separate from the main TGDrive file browser.
Uses BOOKS_CHANNEL for storage and its own books.data file for metadata.
Only accessible via /api/books/* endpoints (for the Vercel frontend).

Two ways a book can get in:
1. Website upload (POST /api/books/upload) -> upload_book_to_telegram()
   sends the file to BOOKS_CHANNEL and registers it directly.
2. Manual Telegram upload -> a file posted straight into BOOKS_CHANNEL is
   picked up by the listener registered in register_books_channel_listener()
   and auto-registered the same way, so it shows up on the website too.
"""

from __future__ import annotations

import json
import os
import random
import string
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

import dill
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.types import Message

import config
from utils.logger import Logger
from utils.clients import get_client

# File types accepted both from the website uploader and from manual
# Telegram uploads picked up by the channel listener.
ALLOWED_BOOK_EXTENSIONS = {".pdf", ".epub", ".mobi", ".azw3", ".txt", ".djvu"}

logger = Logger(__name__)

cache_dir = Path("./cache")
cache_dir.mkdir(parents=True, exist_ok=True)
books_cache_path = cache_dir / "books.data"


def _generate_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=10))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Book:
    def __init__(
        self,
        title: str,
        message_id: int,
        size: int,
        filename: str,
        author: str = "",
        description: str = "",
        tags: Optional[List[str]] = None,
        language: str = "",
        cover_message_id: Optional[int] = None,
        book_id: Optional[str] = None,
    ):
        self.id = book_id or _generate_id()
        self.title = title
        self.author = author
        self.description = description
        self.tags = tags or []
        self.language = language
        self.filename = filename
        self.message_id = message_id          # Telegram message_id in BOOKS_CHANNEL
        self.cover_message_id = cover_message_id
        self.size = size
        self.uploaded_at = _utc_now()
        self.updated_at = self.uploaded_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "author": self.author,
            "description": self.description,
            "tags": self.tags,
            "language": self.language,
            "filename": self.filename,
            "message_id": self.message_id,
            "cover_message_id": self.cover_message_id,
            "size": self.size,
            "uploaded_at": self.uploaded_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Book":
        book = cls(
            title=data.get("title", "Untitled"),
            message_id=data["message_id"],
            size=data.get("size", 0),
            filename=data.get("filename", ""),
            author=data.get("author", ""),
            description=data.get("description", ""),
            tags=data.get("tags", []),
            language=data.get("language", ""),
            cover_message_id=data.get("cover_message_id"),
            book_id=data.get("id"),
        )
        book.uploaded_at = data.get("uploaded_at", book.uploaded_at)
        book.updated_at = data.get("updated_at", book.updated_at)
        return book


class BooksLibrary:
    def __init__(self):
        self.books: Dict[str, Book] = {}
        self.is_updated = False
        # Message id of the last books.data backup sent to BOOKS_CHANNEL,
        # so future backups can edit it instead of sending a new file each time.
        self.backup_message_id: Optional[int] = None

    def save(self) -> None:
        with open(books_cache_path, "wb") as f:
            dill.dump(self, f)
        self.is_updated = True
        logger.info("Books library saved successfully.")

    def add_book(self, book: Book) -> Book:
        self.books[book.id] = book
        self.save()
        return book

    def get_book(self, book_id: str) -> Optional[Book]:
        return self.books.get(book_id)

    def delete_book(self, book_id: str) -> bool:
        if book_id in self.books:
            del self.books[book_id]
            self.save()
            return True
        return False

    def update_book(self, book_id: str, **kwargs) -> Optional[Book]:
        book = self.books.get(book_id)
        if not book:
            return None
        for key, value in kwargs.items():
            if hasattr(book, key) and value is not None:
                setattr(book, key, value)
        book.updated_at = _utc_now()
        self.save()
        return book

    def list_books(
        self,
        query: str = "",
        tag: str = "",
        author: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> List[Book]:
        results = list(self.books.values())

        if query:
            q = query.lower()
            results = [
                b
                for b in results
                if q in b.title.lower()
                or q in b.author.lower()
                or q in b.description.lower()
                or any(q in t.lower() for t in b.tags)
            ]

        if tag:
            t = tag.lower()
            results = [b for b in results if any(t == x.lower() for x in b.tags)]

        if author:
            a = author.lower()
            results = [b for b in results if a in b.author.lower()]

        # Newest first
        results.sort(key=lambda b: b.uploaded_at, reverse=True)
        return results[offset : offset + limit]

    def all_tags(self) -> List[str]:
        tags = set()
        for b in self.books.values():
            tags.update(b.tags)
        return sorted(tags)


BOOKS_DATA: Optional[BooksLibrary] = None


def load_books_data() -> BooksLibrary:
    global BOOKS_DATA
    if books_cache_path.exists():
        try:
            with open(books_cache_path, "rb") as f:
                BOOKS_DATA = dill.load(f)
            logger.info(f"Loaded books library ({len(BOOKS_DATA.books)} books)")
            return BOOKS_DATA
        except Exception as e:
            logger.error(f"Failed to load books.data: {e}")

    BOOKS_DATA = BooksLibrary()
    BOOKS_DATA.save()
    logger.info("Created new empty books library")
    return BOOKS_DATA


def get_books_data() -> BooksLibrary:
    """
    Safe accessor for the current books library.

    Always call this (instead of importing BOOKS_DATA directly) from other
    modules such as utils/books_routes.py. A plain
    `from utils.books import BOOKS_DATA` only captures the value that existed
    at import time and will never see later reassignments performed inside
    load_books_data() (which rebinds the module-level global here) — that
    was the cause of a bug where the books API routes always saw BOOKS_DATA
    as None even after the library had been loaded.
    """
    global BOOKS_DATA
    if BOOKS_DATA is None:
        load_books_data()
    return BOOKS_DATA


async def upload_book_to_telegram(
    file_path: str,
    filename: str,
    title: str,
    author: str = "",
    description: str = "",
    tags: Optional[List[str]] = None,
    language: str = "",
) -> Book:
    """Upload a book file to BOOKS_CHANNEL and register it in the library."""
    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")

    if BOOKS_DATA is None:
        load_books_data()

    client: Client = get_client()
    file_size = os.path.getsize(file_path)

    # Caption with basic metadata (helps if you ever scan the channel manually)
    caption_parts = [f"📚 {title}"]
    if author:
        caption_parts.append(f"Author: {author}")
    if tags:
        caption_parts.append("Tags: " + ", ".join(tags))
    caption = "\n".join(caption_parts)[:1024]

    message: Message = await client.send_document(
        config.BOOKS_CHANNEL,
        file_path,
        caption=caption,
        file_name=filename,
        disable_notification=True,
    )

    size = (
        message.document
        or message.photo
        or message.video
        or message.audio
    ).file_size

    book = Book(
        title=title or Path(filename).stem,
        message_id=message.id,
        size=size,
        filename=filename,
        author=author,
        description=description,
        tags=tags or [],
        language=language,
    )
    BOOKS_DATA.add_book(book)
    logger.info(f"Book uploaded: {book.title} (id={book.id}, msg={message.id})")
    return book


def _book_from_channel_message(message: Message) -> Optional[Book]:
    """
    Build a Book from a raw Telegram message posted in BOOKS_CHANNEL.
    Used to auto-register files that were uploaded directly in Telegram
    (i.e. not through the website's /api/books/upload endpoint).
    Returns None if the message isn't a supported book file.
    """
    media = message.document or message.video or message.audio
    if not media:
        return None

    filename = getattr(media, "file_name", None) or f"book_{message.id}"
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_BOOK_EXTENSIONS:
        return None

    # Optionally parse "Title: ...", "Author: ...", "Tags: a, b" lines out
    # of the caption (this is exactly the caption format upload_book_to_telegram
    # writes, so books uploaded via the website parse back out cleanly too).
    title, author, tags = None, "", []
    caption = message.caption or ""
    for line in caption.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip().lower()
        value = value.strip()
        if key == "title":
            title = value
        elif key == "author":
            author = value
        elif key == "tags":
            tags = [t.strip() for t in value.split(",") if t.strip()]

    if not title:
        first_line = caption.strip().splitlines()[0] if caption.strip() else ""
        title = first_line.lstrip("📚").strip() or Path(filename).stem

    return Book(
        title=title,
        message_id=message.id,
        size=media.file_size,
        filename=filename,
        author=author,
        tags=tags,
    )


def register_books_channel_listener(clients: List[Client]) -> None:
    """
    Watch BOOKS_CHANNEL for messages and auto-register any supported book
    file that shows up there — whether it was uploaded through the website
    or posted directly in Telegram by hand. This is what makes "drop a file
    into the channel and it appears on the site" work.

    Call once at startup, after clients are connected, with the list of
    bot clients (they must already be admins of BOOKS_CHANNEL).
    """
    if not config.BOOKS_CHANNEL:
        return
    if not clients:
        logger.warning("No Telegram clients available; books channel listener not registered.")
        return

    async def _on_channel_message(client: Client, message: Message):
        global BOOKS_DATA
        if BOOKS_DATA is None:
            load_books_data()

        # If this message was just sent by upload_book_to_telegram(), it may
        # already be registered (or about to be, a moment before this event
        # is delivered). Give that a brief head start, then re-check, so we
        # don't create a duplicate, metadata-poorer entry for the same file.
        for _ in range(2):
            if any(b.message_id == message.id for b in BOOKS_DATA.books.values()):
                return
            await asyncio.sleep(1)

        # Skip the periodic books.data metadata backup file itself.
        if message.document and message.document.file_name == "books.data":
            return

        book = _book_from_channel_message(message)
        if not book:
            return

        BOOKS_DATA.add_book(book)
        logger.info(
            f"Auto-registered book posted directly in BOOKS_CHANNEL: "
            f"{book.title} (id={book.id}, msg={message.id})"
        )

    handler_filter = filters.chat(config.BOOKS_CHANNEL) & (
        filters.document | filters.video | filters.audio
    )

    for client in clients:
        client.add_handler(MessageHandler(_on_channel_message, handler_filter))

    logger.info(f"Books channel listener registered on {len(clients)} client(s).")


async def stream_book(book_id: str, request):
    """Stream a book file from BOOKS_CHANNEL."""
    from utils.streamer import media_streamer

    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")

    if BOOKS_DATA is None:
        load_books_data()

    book = BOOKS_DATA.get_book(book_id)
    if not book:
        raise FileNotFoundError("Book not found")

    return await media_streamer(
        config.BOOKS_CHANNEL,
        book.message_id,
        book.filename,
        request,
    )


async def backup_books_data(loop: bool = True):
    """Periodically backup books.data to BOOKS_CHANNEL (similar to drive backup)."""
    global BOOKS_DATA
    logger.info("Starting books backup task.")

    while True:
        try:
            if BOOKS_DATA and BOOKS_DATA.is_updated and config.BOOKS_CHANNEL:
                logger.info("Backing up books.data to BOOKS_CHANNEL...")
                client = get_client()
                from pyrogram.types import InputMediaDocument

                caption = (
                    "Do not edit or delete this message. "
                    "This is a backup of the books library metadata.\n\n"
                    f"Books: {len(BOOKS_DATA.books)}"
                )

                # backup_message_id may not exist on libraries pickled before
                # this field was introduced, so fall back to None safely.
                existing_id = getattr(BOOKS_DATA, "backup_message_id", None)
                sent_or_edited = False

                if existing_id:
                    try:
                        media = InputMediaDocument(
                            books_cache_path,
                            caption=caption,
                            file_name="books.data",
                        )
                        await client.edit_message_media(
                            config.BOOKS_CHANNEL,
                            existing_id,
                            media=media,
                        )
                        sent_or_edited = True
                        logger.info("Books backup edited existing message.")
                    except Exception as e:
                        logger.warning(
                            f"Could not edit existing books backup message "
                            f"({existing_id}), will send a new one: {e}"
                        )

                if not sent_or_edited:
                    message = await client.send_document(
                        config.BOOKS_CHANNEL,
                        books_cache_path,
                        caption=caption,
                        file_name="books.data",
                        disable_notification=True,
                    )
                    BOOKS_DATA.backup_message_id = message.id

                BOOKS_DATA.is_updated = False
                logger.info("Books library backup completed.")
        except Exception as e:
            logger.error(f"Books backup error: {e}")

        if not loop:
            break
        await asyncio.sleep(max(config.DATABASE_BACKUP_TIME, 60))
