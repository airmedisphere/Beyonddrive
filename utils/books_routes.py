"""
Books API Routes
================
Clean endpoints for the Vercel Books frontend.
All books live in BOOKS_CHANNEL and are completely hidden from the main TGDrive UI.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, Request, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

import config
from utils.logger import Logger
from utils.books import (
    get_books_data,
    upload_book_to_telegram,
    stream_book,
)

logger = Logger(__name__)

router = APIRouter(prefix="/api/books", tags=["Books"])


def _ensure_books_enabled():
    if not config.BOOKS_CHANNEL:
        raise HTTPException(
            status_code=503,
            detail="Books feature is disabled. Set BOOKS_CHANNEL environment variable.",
        )
    # get_books_data() always returns the live library, loading it on first
    # use if necessary. Do NOT import BOOKS_DATA by name at module level —
    # that captures a stale None reference and never sees later updates.
    return get_books_data()


@router.get("")
@router.get("/")
async def list_books(
    q: str = Query("", description="Search query"),
    tag: str = Query("", description="Filter by tag"),
    author: str = Query("", description="Filter by author"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List / search books. Used by the Vercel frontend catalog."""
    books_data = _ensure_books_enabled()
    books = books_data.list_books(query=q, tag=tag, author=author, limit=limit, offset=offset)
    return {
        "status": "ok",
        "total": len(books_data.books),
        "count": len(books),
        "books": [b.to_dict() for b in books],
    }


@router.get("/tags")
async def list_tags():
    """Return all unique tags."""
    books_data = _ensure_books_enabled()
    return {"status": "ok", "tags": books_data.all_tags()}


@router.get("/{book_id}")
async def get_book(book_id: str):
    """Get a single book by ID."""
    books_data = _ensure_books_enabled()
    book = books_data.get_book(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return {"status": "ok", "book": book.to_dict()}


@router.get("/{book_id}/download")
@router.get("/{book_id}/stream")
async def download_book(book_id: str, request: Request):
    """Stream / download the book file from BOOKS_CHANNEL."""
    _ensure_books_enabled()
    try:
        return await stream_book(book_id, request)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Book not found")
    except Exception as e:
        logger.error(f"Error streaming book {book_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_book(
    file: UploadFile = File(...),
    title: str = Form(""),
    author: str = Form(""),
    description: str = Form(""),
    tags: str = Form(""),          # comma-separated
    language: str = Form(""),
):
    """
    Upload a PDF / EPUB from the Books website.
    The file is stored only in BOOKS_CHANNEL and never appears in the main TGDrive.
    """
    _ensure_books_enabled()

    filename = file.filename or "book.pdf"
    ext = Path(filename).suffix.lower()
    if ext not in {".pdf", ".epub", ".mobi", ".azw3", ".txt", ".djvu"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {ext}. Allowed: pdf, epub, mobi, azw3, txt, djvu",
        )

    # Save to a temporary file first
    suffix = ext or ".bin"
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")

    try:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        book = await upload_book_to_telegram(
            file_path=tmp_path,
            filename=filename,
            title=title or Path(filename).stem,
            author=author,
            description=description,
            tags=tag_list,
            language=language,
        )
        return {"status": "ok", "book": book.to_dict()}
    except Exception as e:
        logger.error(f"Book upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


@router.patch("/{book_id}")
async def update_book_metadata(book_id: str, request: Request):
    """Update title, author, description, tags, language of a book."""
    books_data = _ensure_books_enabled()
    data = await request.json()
    book = books_data.update_book(
        book_id,
        title=data.get("title"),
        author=data.get("author"),
        description=data.get("description"),
        tags=data.get("tags"),
        language=data.get("language"),
    )
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return {"status": "ok", "book": book.to_dict()}


@router.delete("/{book_id}")
async def delete_book(book_id: str):
    """
    Remove a book from the library metadata.
    Note: the file remains in the Telegram channel (safe default).
    """
    books_data = _ensure_books_enabled()
    ok = books_data.delete_book(book_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Book not found")
    return {"status": "ok", "message": "Book removed from library"}
