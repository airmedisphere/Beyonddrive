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
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import dill
from pyrogram import Client, filters, enums
from pyrogram.handlers import MessageHandler
from pyrogram.types import Message

import config
from utils.logger import Logger
from utils.clients import get_client
from utils.book_converter import (
    needs_conversion,
    NATIVE_READER_FORMATS,
    convert_book,
)
from utils.book_covers import generate_cover_image

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
    # reader_status values: "ready" | "converting" | "failed" | "unsupported"
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
        reader_message_id: Optional[int] = None,
        reader_format: Optional[str] = None,
        reader_status: str = "unsupported",
        reader_error: Optional[str] = None,
        file_hash: Optional[str] = None,
        source_channel: Optional[str] = None,
        enriched: bool = False,
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
        # SHA-256 of the file content, computed at upload time from the
        # website. Used to catch "I already have this exact file" before
        # it gets uploaded a second/third time. None for books that were
        # posted directly into BOOKS_CHANNEL by hand (no local file to
        # hash) or uploaded before this field existed — those are only
        # caught by the filename+size fallback, see find_duplicate().
        self.file_hash = file_hash

        # Reader support: which message holds a reader-friendly version of
        # this book, in what format, and whether it's ready yet. For
        # PDF/EPUB/TXT this is set immediately (== the original file, no
        # conversion needed). For MOBI/AZW3/DJVU it starts as "converting"
        # and gets filled in once background conversion finishes.
        self.reader_message_id = reader_message_id
        self.reader_format = reader_format
        self.reader_status = reader_status
        self.reader_error = reader_error

        # Bulk import provenance. source_channel is the @username / id of the
        # channel this book was imported from (None for website uploads and
        # for files posted straight into BOOKS_CHANNEL by hand), kept so an
        # admin can tell at a glance where a batch came from.
        self.source_channel = source_channel
        # enriched=True once the post-import pass has read embedded metadata
        # out of the actual file. Books are registered immediately with only
        # filename-derived metadata (that costs nothing), then enrichment
        # runs strictly one file at a time afterwards — see
        # utils/books_import.enrich_books(). This flag is what lets that pass
        # be resumable: an import interrupted by a restart or an OOM can be
        # re-run and will only touch what it never got to.
        self.enriched = enriched

    def __setstate__(self, state: dict) -> None:
        """
        Called by dill/pickle when restoring a Book from books.data instead
        of __init__. Books saved before a new field existed (file_hash was
        added after this library already had hundreds of books in it)
        come back from an old pickle *without* that attribute at all —
        every book already in the library before that deploy would then
        AttributeError the instant anything called .to_dict() on it,
        which is exactly what happened here (500s that surfaced as
        "Book not found" on individual pages and an empty admin panel,
        even though the data itself was completely intact).
        Defaulting any newly-added field here means restoring an old
        library self-heals on load — no data migration step needed, and
        no future field addition can trigger this same class of crash.
        """
        self.__dict__.update(state)
        self.__dict__.setdefault("file_hash", None)
        # Added with the bulk-import feature, long after this library had
        # books in it — same reasoning as file_hash above.
        self.__dict__.setdefault("source_channel", None)
        self.__dict__.setdefault("enriched", False)

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
            "reader_format": self.reader_format,
            "reader_status": self.reader_status,
            "reader_error": self.reader_error,
            # getattr, not self.file_hash, as a second line of defense —
            # __setstate__ above should always guarantee this exists, but
            # this way to_dict() itself can never be the thing that crashes
            # on an old object even if that guarantee were ever bypassed.
            "file_hash": getattr(self, "file_hash", None),
            "source_channel": getattr(self, "source_channel", None),
            "enriched": getattr(self, "enriched", False),
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
            reader_message_id=data.get("reader_message_id"),
            reader_format=data.get("reader_format"),
            reader_status=data.get("reader_status", "unsupported"),
            reader_error=data.get("reader_error"),
            file_hash=data.get("file_hash"),
            source_channel=data.get("source_channel"),
            enriched=data.get("enriched", False),
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

    def __setstate__(self, state: dict) -> None:
        """Same self-healing purpose as Book.__setstate__ above — guards
        this class against the same crash if a future field is ever added
        here too."""
        self.__dict__.update(state)
        self.__dict__.setdefault("backup_message_id", None)
        self.__dict__.setdefault("is_updated", False)

    def save(self) -> None:
        with open(books_cache_path, "wb") as f:
            dill.dump(self, f)
        self.is_updated = True
        logger.info("Books library saved successfully.")

    def add_book(self, book: Book) -> Book:
        self.books[book.id] = book
        self.save()
        return book

    def register_book(self, book: Book) -> Book:
        """
        Add a book to the library *without* writing books.data to disk.

        add_book() saves on every single call, which is right for a one-off
        upload but wrong for a bulk import: pickling the entire library once
        per file turns a 500-book import into 500 full-library dumps, and the
        dump itself allocates a serialized copy of everything in memory. On a
        512MB instance that is both the slowest and the most dangerous part
        of the import.

        Bulk callers use register_book() per file and bulk_save() once per
        batch instead. The tradeoff is deliberate and bounded: if the process
        dies mid-batch, the files are already safe in BOOKS_CHANNEL and only
        that batch's registrations are lost — re-running the import picks
        them up again (and the duplicate check stops the ones that landed).
        """
        self.books[book.id] = book
        return book

    def bulk_save(self) -> int:
        """Persist the library once after a run of register_book() calls.
        Returns the number of books now in the library, for logging."""
        self.save()
        return len(self.books)

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

    def set_reader_state(self, book_id: str, **kwargs) -> Optional[Book]:
        """
        Like update_book(), but allows explicitly setting a field to None
        (needed to clear reader_error once a conversion succeeds — the
        general update_book() intentionally ignores None so it can't be
        used to blank out a field via the public PATCH endpoint).
        Only used internally by the reader-conversion pipeline.
        """
        book = self.books.get(book_id)
        if not book:
            return None
        for key, value in kwargs.items():
            if hasattr(book, key):
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

    def find_duplicate(
        self, file_hash: Optional[str], filename: str, size: int
    ) -> Optional[Book]:
        """
        Look for a book that's already the same file. Exact content match
        (file_hash) wins when we have one. Otherwise fall back to
        "same filename and same byte size", which reliably catches
        re-uploads of the exact same file even for older library entries
        that predate file_hash existing.
        """
        if file_hash:
            for b in self.books.values():
                if getattr(b, "file_hash", None) and b.file_hash == file_hash:
                    return b

        norm_name = filename.strip().lower()
        for b in self.books.values():
            if b.size == size and b.filename.strip().lower() == norm_name:
                return b
        return None

    def find_duplicate_groups(self) -> List[Dict[str, Any]]:
        """
        Scan the whole library for likely-duplicate books, for the admin
        "Find duplicates" tool. Groups by file_hash where available
        (exact-content matches), and by (filename, size) for everything
        else (covers books uploaded before file_hash existed, or posted
        directly into the Telegram channel). Only groups with 2+ books
        are returned. Within each group, oldest upload is flagged as
        "keep" by default — the admin can still delete whichever they want.
        """
        by_hash: Dict[str, List[Book]] = {}
        by_name_size: Dict[Tuple[str, int], List[Book]] = {}

        for b in self.books.values():
            if b.file_hash:
                by_hash.setdefault(b.file_hash, []).append(b)
            else:
                key = (b.filename.strip().lower(), b.size)
                by_name_size.setdefault(key, []).append(b)

        groups: List[Dict[str, Any]] = []

        def _add_group(members: List[Book], method: str):
            if len(members) < 2:
                return
            members = sorted(members, key=lambda b: b.uploaded_at)
            groups.append(
                {
                    "method": method,
                    "books": [
                        {**b.to_dict(), "suggested_keep": i == 0}
                        for i, b in enumerate(members)
                    ],
                }
            )

        for members in by_hash.values():
            _add_group(members, "identical_file")
        for members in by_name_size.values():
            _add_group(members, "same_name_and_size")

        return groups


BOOKS_DATA: Optional[BooksLibrary] = None


# ---------------------------------------------------------------------------
# Import window guard
# ---------------------------------------------------------------------------
# A bulk import forwards files into BOOKS_CHANNEL, which means the channel
# listener below fires for every single one of them — and the listener's whole
# job is to auto-register anything new it sees. Left alone, the two would race:
# the listener waits ~2s for a registration to appear, but the importer only
# writes registrations once per batch, so anything forwarded early in a batch
# would get registered twice (once properly by the importer, once as a
# metadata-poorer duplicate by the listener).
#
# The importer wraps its work in begin_import_window()/end_import_window() and
# claims message ids as soon as forward_messages() hands them back. The
# listener consults both: a claimed id is dropped outright, and while a window
# is open it waits much longer before concluding "nobody else is going to
# register this". A file a human posts by hand mid-import still gets picked up
# — it just takes a few seconds longer to appear.
_import_window_depth = 0
_import_claimed_ids: set = set()


def begin_import_window() -> None:
    global _import_window_depth
    if _import_window_depth == 0:
        # Clear at the *start* of a fresh import rather than at the end of the
        # previous one: a listener callback can still be sitting in its wait
        # loop when the import finishes, and it needs the claim to still be
        # there to know the file was not its business.
        _import_claimed_ids.clear()
    _import_window_depth += 1


def end_import_window() -> None:
    """Close one nesting level.

    Deliberately does not drop the claimed-id set — see begin_import_window().
    The importer must have registered and saved its books *before* calling
    this, so that any listener still waiting falls through to the
    "already registered" check and drops the message on that basis instead.
    """
    global _import_window_depth
    _import_window_depth = max(0, _import_window_depth - 1)


def is_import_window_active() -> bool:
    return _import_window_depth > 0


def claim_import_message_ids(message_ids) -> None:
    """Tell the listener 'the importer owns these, don't touch them'."""
    # Bound the set so a very long-lived process running many large imports
    # can't accumulate ids indefinitely; 50k ints is well under a megabyte and
    # far more than any single import will forward.
    if len(_import_claimed_ids) > 50_000:
        _import_claimed_ids.clear()
    for mid in message_ids:
        if mid is not None:
            _import_claimed_ids.add(int(mid))


def is_claimed_by_import(message_id: int) -> bool:
    return int(message_id) in _import_claimed_ids


async def _load_books_data_from_message(client: Client, msg: Message) -> Optional["BooksLibrary"]:
    """Given a Telegram message, download+unpickle it as a books.data file.
    Returns None (and logs) if the message doesn't actually hold one."""
    if not (msg and msg.document and msg.document.file_name == "books.data"):
        return None
    dl_path = await msg.download()
    with open(dl_path, "rb") as f:
        data = dill.load(f)
    # Make sure future backups know which message to edit, even if this
    # was discovered rather than read from config.
    data.backup_message_id = msg.id
    return data


async def _discover_books_backup_message(client: Client) -> Optional[Message]:
    """
    Find the books.data backup message in BOOKS_CHANNEL without relying on
    BOOKS_DB_MSG_ID being set. backup_books_data() always pins whichever
    message currently holds the backup, so the pinned message is the fast
    path. If pinning ever failed (or got overridden), fall back to scanning
    recent channel history for a "books.data" document, newest first.
    """
    # ── Fast path: the pinned message ──────────────────────────────────
    try:
        chat = await client.get_chat(config.BOOKS_CHANNEL)
        pinned = getattr(chat, "pinned_message", None)
        if pinned and pinned.document and pinned.document.file_name == "books.data":
            return pinned
    except Exception as e:
        logger.warning(f"Could not check pinned message in BOOKS_CHANNEL: {e}")

    # ── Slow path: scan recent history for the backup document ─────────
    try:
        async for msg in client.search_messages(
            config.BOOKS_CHANNEL, filter=enums.MessagesFilter.DOCUMENT
        ):
            if msg.document and msg.document.file_name == "books.data":
                return msg
    except Exception as e:
        logger.warning(f"Could not search BOOKS_CHANNEL history for books.data: {e}")

    return None


async def load_books_data() -> BooksLibrary:
    """
    Restore the books library the same way drive.data is restored:
    Telegram BOOKS_CHANNEL backup (survives redeploys) first, then the
    local disk cache (dev convenience only — Render wipes this on every
    deploy), then a fresh empty library as a last resort.

    Unlike the main drive (which requires DATABASE_BACKUP_MSG_ID to be set
    up front), BOOKS_DB_MSG_ID is optional: if it's missing or stale we
    auto-discover the current backup message from BOOKS_CHANNEL itself
    (pinned message, or a history search as a fallback) so the library
    survives redeploys even when nobody copied the message ID into env
    vars. This is the fix for books data getting wiped on redeploy while
    the main storage (which always had its ID set) kept working.
    """
    global BOOKS_DATA

    if not config.BOOKS_CHANNEL:
        BOOKS_DATA = BooksLibrary()
        BOOKS_DATA.save()
        logger.info("Books library disabled (BOOKS_CHANNEL not set)")
        return BOOKS_DATA

    client = get_client()

    # ── 1. Try the explicit BOOKS_DB_MSG_ID, if set ─────────────────────
    msg_id = getattr(config, "BOOKS_DB_MSG_ID", None)
    if msg_id:
        try:
            msg: Message = await client.get_messages(config.BOOKS_CHANNEL, msg_id)
            loaded = await _load_books_data_from_message(client, msg)
            if loaded:
                BOOKS_DATA = loaded
                BOOKS_DATA.save()
                logger.info(
                    f"Loaded books library from Telegram backup via BOOKS_DB_MSG_ID "
                    f"({len(BOOKS_DATA.books)} books)"
                )
                return BOOKS_DATA
            logger.warning("BOOKS_DB_MSG_ID did not point to a books.data file.")
        except Exception as e:
            logger.warning(f"Books Telegram backup load via BOOKS_DB_MSG_ID failed: {e}")

    # ── 2. Auto-discover the backup message (no env var needed) ────────
    try:
        discovered = await _discover_books_backup_message(client)
        if discovered:
            loaded = await _load_books_data_from_message(client, discovered)
            if loaded:
                BOOKS_DATA = loaded
                BOOKS_DATA.save()
                logger.info(
                    f"Loaded books library by auto-discovering backup message "
                    f"{discovered.id} in BOOKS_CHANNEL ({len(BOOKS_DATA.books)} books). "
                    f"Optionally set BOOKS_DB_MSG_ID={discovered.id} to skip the lookup."
                )
                return BOOKS_DATA
    except Exception as e:
        logger.warning(f"Books auto-discovery failed: {e}")

    # ── 3. Fall back to whatever is on local disk (same-boot dev use) ──
    if books_cache_path.exists():
        try:
            with open(books_cache_path, "rb") as f:
                BOOKS_DATA = dill.load(f)
            logger.info(f"Loaded books library from local cache ({len(BOOKS_DATA.books)} books)")
            return BOOKS_DATA
        except Exception as e:
            logger.error(f"Failed to load books.data from local cache: {e}")

    # ── 4. Nothing to restore from — start fresh ────────────────────────
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
        # Safety net only: normal startup always awaits load_books_data()
        # in main.py's lifespan before any request can reach here. If we
        # ever do land here it means that step was skipped, so fall back
        # to local disk / a fresh library rather than crashing — we can't
        # await the Telegram restore from this sync accessor.
        if books_cache_path.exists():
            try:
                with open(books_cache_path, "rb") as f:
                    BOOKS_DATA = dill.load(f)
            except Exception as e:
                logger.error(f"get_books_data: failed to load local cache: {e}")
        if BOOKS_DATA is None:
            BOOKS_DATA = BooksLibrary()
            BOOKS_DATA.save()
    return BOOKS_DATA


async def upload_book_to_telegram(
    file_path: str,
    filename: str,
    title: str,
    author: str = "",
    description: str = "",
    tags: Optional[List[str]] = None,
    language: str = "",
    file_hash: Optional[str] = None,
) -> Book:
    """
    Upload a book file to BOOKS_CHANNEL and register it in the library.

    PDF/EPUB/TXT are reader-ready immediately (no conversion needed).
    MOBI/AZW3/DJVU are registered with reader_status="converting" and a
    background task is kicked off to convert them to EPUB/PDF and attach
    the result — this call returns right away rather than blocking the
    HTTP request on a conversion that can take a while. The frontend
    polls GET /api/books/{id}/reader-info until reader_status flips to
    "ready" (or "failed").
    """
    global BOOKS_DATA
    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")

    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    client: Client = get_client()
    ext = Path(filename).suffix.lower()

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

    if ext in NATIVE_READER_FORMATS:
        reader_kwargs = dict(
            reader_message_id=message.id,
            reader_format=NATIVE_READER_FORMATS[ext],
            reader_status="ready",
        )
    elif needs_conversion(ext):
        reader_kwargs = dict(reader_status="converting")
    else:
        reader_kwargs = dict(reader_status="unsupported")

    book = Book(
        title=title or Path(filename).stem,
        message_id=message.id,
        size=size,
        filename=filename,
        author=author,
        description=description,
        tags=tags or [],
        language=language,
        file_hash=file_hash,
        **reader_kwargs,
    )
    BOOKS_DATA.add_book(book)
    logger.info(f"Book uploaded: {book.title} (id={book.id}, msg={message.id})")

    if needs_conversion(ext):
        # Work off an independent copy of the file so this survives the
        # caller deleting its own tmp upload file right after we return.
        import shutil as _shutil
        import tempfile as _tempfile

        fd, copy_path = _tempfile.mkstemp(suffix=ext)
        os.close(fd)
        _shutil.copy2(file_path, copy_path)
        asyncio.create_task(_convert_and_attach_reader(book.id, copy_path, ext))

    return book


async def _convert_and_attach_reader(book_id: str, source_path: str, ext: str) -> None:
    """
    Background job: convert a MOBI/AZW3/DJVU file to a reader-friendly
    format, upload the result to BOOKS_CHANNEL, and attach it to the book.
    Always cleans up `source_path` when done, regardless of outcome.
    """
    global BOOKS_DATA
    try:
        out_path, out_format, err = await convert_book(source_path, ext)
        if not out_path:
            if BOOKS_DATA:
                BOOKS_DATA.set_reader_state(
                    book_id,
                    reader_status="failed",
                    reader_error=err or "Conversion failed",
                )
            logger.error(f"Reader conversion failed for book {book_id}: {err}")
            return

        try:
            client = get_client()
            message = await client.send_document(
                config.BOOKS_CHANNEL,
                out_path,
                caption="Reader version (auto-converted). Do not delete.",
                file_name=Path(out_path).name,
                disable_notification=True,
            )
            if BOOKS_DATA:
                BOOKS_DATA.set_reader_state(
                    book_id,
                    reader_message_id=message.id,
                    reader_format=out_format,
                    reader_status="ready",
                    reader_error=None,
                )
            logger.info(f"Reader version ready for book {book_id} (format={out_format})")
        finally:
            try:
                os.remove(out_path)
            except OSError:
                pass
    except Exception as e:
        logger.error(f"Reader conversion crashed for book {book_id}: {e}")
        if BOOKS_DATA:
            BOOKS_DATA.set_reader_state(book_id, reader_status="failed", reader_error=str(e))
    finally:
        try:
            os.remove(source_path)
        except OSError:
            pass


async def upload_cover_to_telegram(book_id: str, image_path: str, source: str = "upload") -> Book:
    """
    Upload a cover image (manually provided, or freshly generated) to
    BOOKS_CHANNEL as a document (not a Telegram "photo" — photos get
    recompressed/resized by Telegram, documents don't) and attach it to
    the book via cover_message_id.
    """
    global BOOKS_DATA
    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")
    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    book = BOOKS_DATA.get_book(book_id)
    if not book:
        raise FileNotFoundError("Book not found")

    client: Client = get_client()
    ext = Path(image_path).suffix.lower() or ".jpg"
    cover_filename = f"cover_{book.id}{ext}"
    caption = f"🖼 Cover for: {book.title}" + (" (auto-generated)" if source == "generated" else "")

    message: Message = await client.send_document(
        config.BOOKS_CHANNEL,
        image_path,
        caption=caption[:1024],
        file_name=cover_filename,
        disable_notification=True,
    )

    BOOKS_DATA.update_book(book_id, cover_message_id=message.id)
    logger.info(f"Cover attached to book {book_id} ({source}), msg={message.id}")
    return BOOKS_DATA.get_book(book_id)


async def generate_cover_for_book(book_id: str, force: bool = True) -> Tuple[Optional[Book], str]:
    """
    Auto-generate a cover for one book (renders page 1 for PDF/DJVU,
    extracts the embedded cover for EPUB/MOBI/AZW3, or falls back to a
    generated title card) and upload it to BOOKS_CHANNEL.

    If force=False and the book already has a cover, this is a no-op that
    returns ("skipped", ...) — used by "Generate for all" so it never
    overwrites covers you already set (manually or previously generated).
    force=True (the default, used by the per-book "Generate" button) always
    (re)generates, since clicking Generate on one specific book is an
    explicit request for a new cover.

    Returns (book_or_None, status) where status is one of:
    "generated", "skipped", "not_found", "error".
    """
    global BOOKS_DATA
    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    book = BOOKS_DATA.get_book(book_id)
    if not book:
        return None, "not_found"

    if not force and book.cover_message_id:
        return book, "skipped"

    ext = Path(book.filename).suffix.lower()
    download_path = None
    cover_path = None
    try:
        download_path = await _download_book_source_message(book)
        cover_path, err = await generate_cover_image(
            download_path, ext, title=book.title, author=book.author
        )
        if not cover_path:
            logger.error(f"Cover generation failed for book {book_id}: {err}")
            return book, "error"

        updated = await upload_cover_to_telegram(book_id, cover_path, source="generated")
        return updated, "generated"
    except Exception as e:
        logger.error(f"Cover generation crashed for book {book_id}: {e}")
        return book, "error"
    finally:
        for p in (download_path, cover_path):
            if p:
                try:
                    os.remove(p)
                except OSError:
                    pass


async def _download_book_source_message(book: Book) -> str:
    """Download a book's original file from BOOKS_CHANNEL to a temp path
    (preserving its extension), for feeding to a local render tool.
    Caller must delete the returned path when done."""
    client: Client = get_client()
    message: Message = await client.get_messages(config.BOOKS_CHANNEL, book.message_id)
    if not message:
        raise RuntimeError("Original book message not found in BOOKS_CHANNEL")

    ext = Path(book.filename).suffix.lower() or ".bin"
    fd, download_path = tempfile.mkstemp(suffix=ext)
    os.close(fd)
    await client.download_media(message, file_name=download_path)
    return download_path


async def generate_all_covers() -> Dict[str, Any]:
    """
    Generate covers for every book that doesn't already have one.
    Existing covers (manually uploaded or previously generated) are always
    left untouched — this only fills in the gaps. Runs with a small
    concurrency limit so it doesn't hammer Telegram/the conversion tools
    when the library is large.
    """
    global BOOKS_DATA
    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    todo = [b.id for b in BOOKS_DATA.books.values() if not b.cover_message_id]
    results = {"total_missing": len(todo), "generated": 0, "skipped": 0, "failed": []}

    # Fully sequential on purpose. The render step is already internally
    # capped to one-at-a-time (book_covers._render_semaphore), but on a
    # memory-limited instance (Render free tier is 512MB, shared with
    # everything else this app is doing — streaming, Pyrogram, etc.) even
    # a couple of concurrent downloads/uploads was enough headroom loss to
    # tip things over. One book fully finishing (downloaded, rendered,
    # uploaded, temp files removed) before the next starts keeps the peak
    # as low as this feature can go.
    semaphore = asyncio.Semaphore(1)

    async def _one(book_id: str):
        async with semaphore:
            _, status = await generate_cover_for_book(book_id, force=False)
            if status == "generated":
                results["generated"] += 1
            elif status == "skipped":
                results["skipped"] += 1
            else:
                results["failed"].append(book_id)
            # A brief pause between books gives the event loop a chance to
            # actually run garbage collection and lets the OS reclaim freed
            # native buffers (PyMuPDF/Pillow) before the next book starts,
            # instead of piling straight into the next allocation.
            await asyncio.sleep(0.3)

    await asyncio.gather(*(_one(bid) for bid in todo))
    return results


async def stream_cover(book_id: str, request):
    """Stream a book's cover image from BOOKS_CHANNEL."""
    global BOOKS_DATA
    from utils.streamer import media_streamer

    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")
    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    book = BOOKS_DATA.get_book(book_id)
    if not book:
        raise FileNotFoundError("Book not found")
    if not book.cover_message_id:
        raise LookupError("No cover set for this book")

    cover_filename = f"cover_{book.id}.jpg"
    return await media_streamer(
        config.BOOKS_CHANNEL,
        book.cover_message_id,
        cover_filename,
        request,
    )


def reader_kwargs_for_ext(ext: str, message_id: int) -> Dict[str, Any]:
    """
    Decide the reader_* fields for a freshly-registered book based on its
    extension. PDF/EPUB/TXT are readable in the browser as-is, so the original
    message doubles as the reader file. MOBI/AZW3/DJVU need converting first,
    so they start as "converting" and get filled in later. Anything else is
    downloadable but not readable in-browser.

    Shared by the channel listener and the bulk importer so the two can never
    disagree about what counts as reader-ready.
    """
    if ext in NATIVE_READER_FORMATS:
        return dict(
            reader_message_id=message_id,
            reader_format=NATIVE_READER_FORMATS[ext],
            reader_status="ready",
        )
    if needs_conversion(ext):
        return dict(reader_status="converting")
    return dict(reader_status="unsupported")


def parse_book_caption(caption: str) -> Dict[str, Any]:
    """
    Pull "Title: ...", "Author: ...", "Tags: a, b", "Language: ..." and
    "Description: ..." lines out of a Telegram caption. This is exactly the
    format upload_book_to_telegram() writes, so website uploads round-trip
    cleanly; channels that happen to use the same convention get parsed for
    free, and ones that don't just yield an empty dict.
    """
    out: Dict[str, Any] = {}
    for line in (caption or "").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip().lower().lstrip("📚").strip()
        value = value.strip()
        if not value:
            continue
        if key == "title":
            out["title"] = value
        elif key == "author":
            out["author"] = value
        elif key in ("tags", "tag"):
            out["tags"] = [t.strip() for t in value.split(",") if t.strip()]
        elif key in ("language", "lang"):
            out["language"] = value
        elif key in ("description", "desc"):
            out["description"] = value
    return out


def book_media(message: Message):
    """The media object of a message, if it could plausibly be a book file."""
    return message.document or message.video or message.audio


def _book_from_channel_message(message: Message) -> Optional[Book]:
    """
    Build a Book from a raw Telegram message posted in BOOKS_CHANNEL.
    Used to auto-register files that were uploaded directly in Telegram
    (i.e. not through the website's /api/books/upload endpoint).
    Returns None if the message isn't a supported book file.
    """
    media = book_media(message)
    if not media:
        return None

    filename = getattr(media, "file_name", None) or f"book_{message.id}"
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_BOOK_EXTENSIONS:
        return None

    caption = message.caption or ""
    meta = parse_book_caption(caption)
    title = meta.get("title")
    if not title:
        first_line = caption.strip().splitlines()[0] if caption.strip() else ""
        title = first_line.lstrip("📚").strip() or Path(filename).stem

    return Book(
        title=title,
        message_id=message.id,
        size=media.file_size,
        filename=filename,
        author=meta.get("author", ""),
        description=meta.get("description", ""),
        tags=meta.get("tags", []),
        language=meta.get("language", ""),
        **reader_kwargs_for_ext(ext, message.id),
    )


def register_books_channel_listener(clients: List[Client]) -> None:
    """
    Watch BOOKS_CHANNEL for messages and auto-register any supported book
    file that shows up there — whether it was uploaded through the website
    or posted directly in Telegram by hand. This is what makes "drop a file
    into the channel and it appears on the site" work.

    For MOBI/AZW3/DJVU files posted manually (no local copy on our server
    yet), the file is downloaded from Telegram and the same background
    conversion pipeline used for website uploads is kicked off, so manual
    uploads get an in-browser reader too, not just website uploads.

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
            BOOKS_DATA = get_books_data()

        # A bulk import forwards files in here itself and registers them with
        # richer metadata than this listener can produce, so anything it has
        # claimed is not ours to touch. See the import window guard above.
        if is_claimed_by_import(message.id):
            return

        # If this message was just sent by upload_book_to_telegram(), it may
        # already be registered (or about to be, a moment before this event
        # is delivered). Give that a brief head start, then re-check, so we
        # don't create a duplicate, metadata-poorer entry for the same file.
        #
        # While an import is running the head start has to be much longer:
        # the importer saves registrations once per batch (up to 100 files),
        # so "not registered yet" says nothing about whether it's about to be.
        # Re-check the claim each time round, since the importer may only
        # learn the forwarded message id after this handler already started.
        attempts = 40 if is_import_window_active() else 2
        for _ in range(attempts):
            if is_claimed_by_import(message.id):
                return
            if any(b.message_id == message.id for b in BOOKS_DATA.books.values()):
                return
            await asyncio.sleep(1)
            # Stop waiting early once the import has finished and it's clear
            # this message was never one of its files.
            if attempts > 2 and not is_import_window_active():
                if is_claimed_by_import(message.id):
                    return
                if any(b.message_id == message.id for b in BOOKS_DATA.books.values()):
                    return
                break

        # Skip the periodic books.data metadata backup file, and any
        # already-uploaded reader-conversion output files (these carry a
        # distinctive caption and a ".converted." marker in the filename).
        doc_name = message.document.file_name if message.document else None
        is_reader_output = doc_name and ".converted." in doc_name
        if doc_name == "books.data" or is_reader_output:
            return

        book = _book_from_channel_message(message)
        if not book:
            return

        BOOKS_DATA.add_book(book)
        logger.info(
            f"Auto-registered book posted directly in BOOKS_CHANNEL: "
            f"{book.title} (id={book.id}, msg={message.id})"
        )

        ext = Path(book.filename).suffix.lower()
        if needs_conversion(ext):
            import tempfile as _tempfile

            fd, download_path = _tempfile.mkstemp(suffix=ext)
            os.close(fd)
            try:
                await client.download_media(message, file_name=download_path)
            except Exception as e:
                logger.error(f"Failed to download manually-posted book for conversion: {e}")
                BOOKS_DATA.set_reader_state(
                    book.id, reader_status="failed", reader_error=str(e)
                )
                try:
                    os.remove(download_path)
                except OSError:
                    pass
                return
            asyncio.create_task(_convert_and_attach_reader(book.id, download_path, ext))

    handler_filter = filters.chat(config.BOOKS_CHANNEL) & (
        filters.document | filters.video | filters.audio
    )

    for client in clients:
        client.add_handler(MessageHandler(_on_channel_message, handler_filter))

    logger.info(f"Books channel listener registered on {len(clients)} client(s).")


async def stream_book(book_id: str, request):
    """Stream a book file from BOOKS_CHANNEL."""
    global BOOKS_DATA
    from utils.streamer import media_streamer

    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")

    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    book = BOOKS_DATA.get_book(book_id)
    if not book:
        raise FileNotFoundError("Book not found")

    return await media_streamer(
        config.BOOKS_CHANNEL,
        book.message_id,
        book.filename,
        request,
    )


async def stream_reader_file(book_id: str, request):
    """
    Stream the reader-ready version of a book (used by the in-browser
    reader). This is the original file for PDF/EPUB/TXT, or the converted
    output for MOBI/AZW3/DJVU once conversion has finished.
    Raises FileNotFoundError if the book doesn't exist, and
    RuntimeError("not_ready") if a conversion is still in progress or failed.
    """
    global BOOKS_DATA
    from utils.streamer import media_streamer

    if not config.BOOKS_CHANNEL:
        raise RuntimeError("BOOKS_CHANNEL is not configured")

    if BOOKS_DATA is None:
        BOOKS_DATA = get_books_data()

    book = BOOKS_DATA.get_book(book_id)
    if not book:
        raise FileNotFoundError("Book not found")

    if book.reader_status != "ready" or not book.reader_message_id:
        raise RuntimeError("not_ready")

    reader_filename = f"{Path(book.filename).stem}.{book.reader_format}"
    return await media_streamer(
        config.BOOKS_CHANNEL,
        book.reader_message_id,
        reader_filename,
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
                    # Persist the new backup_message_id into the local cache
                    # file too (save() re-pickles BOOKS_DATA as it stands
                    # now), so it survives even if the process restarts
                    # before the *next* backup cycle would have edited it.
                    BOOKS_DATA.save()

                    if not config.BOOKS_DB_MSG_ID:
                        logger.warning(
                            f"books.data sent to BOOKS_CHANNEL for the first time. "
                            f"Message ID: {message.id} — "
                            f"Add BOOKS_DB_MSG_ID={message.id} to your Render env vars "
                            f"so this library survives future deploys."
                        )
                    try:
                        await message.pin()
                    except Exception as pin_e:
                        # Not fatal — load_books_data() also falls back to a
                        # history search if the pinned message can't be
                        # found, but pinning is what makes that lookup fast.
                        logger.warning(f"Could not pin books backup message: {pin_e}")

                BOOKS_DATA.is_updated = False
                logger.info("Books library backup completed.")

                # Backup books.data to GitHub too, in its own "books/" folder
                # so it never collides with the main drive.data backup.
                try:
                    from utils.github_backup import backup_to_github, is_github_enabled
                    if is_github_enabled():
                        asyncio.create_task(
                            backup_to_github(
                                str(books_cache_path),
                                remote_name="books.data",
                                folder="books",
                            )
                        )
                except Exception as _ge:
                    logger.error(f"Books GitHub backup error: {_ge}")

        except Exception as e:
            logger.error(f"Books backup error: {e}")

        if not loop:
            break
        await asyncio.sleep(max(config.DATABASE_BACKUP_TIME, 60))
