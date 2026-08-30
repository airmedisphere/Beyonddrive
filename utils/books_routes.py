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

from fastapi import APIRouter, Request, UploadFile, File, Form, HTTPException, Query, Header, Depends
from fastapi.responses import JSONResponse, StreamingResponse

import config
from utils.logger import Logger
from utils.books import (
    get_books_data,
    upload_book_to_telegram,
    stream_book,
    stream_reader_file,
    upload_cover_to_telegram,
    generate_cover_for_book,
    generate_all_covers,
    stream_cover,
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


def _require_admin(x_admin_password: Optional[str] = Header(None)):
    """
    Guards write/management operations (edit, delete). The website's admin
    page prompts for a password once and sends it back on this header for
    every admin action; nothing here is stored server-side beyond the
    single ADMIN_PASSWORD env var already used elsewhere in this project.
    """
    if not config.ADMIN_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="Admin password is not configured on the server.",
        )
    if not x_admin_password or x_admin_password != config.ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid or missing admin password")


@router.post("/admin/verify")
async def verify_admin_password(request: Request):
    """
    Check a password against ADMIN_PASSWORD without performing any action.
    Used by the website's admin page to gate access before showing the
    edit/delete UI.
    """
    if not config.ADMIN_PASSWORD:
        raise HTTPException(status_code=503, detail="Admin password is not configured on the server.")
    data = await request.json()
    if data.get("password") != config.ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return {"status": "ok"}


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


@router.get("/admin/backup-status")
async def backup_status(_admin: None = Depends(_require_admin)):
    """
    Diagnostic: shows whether the books library's Telegram backup is
    configured to survive a redeploy, without needing to read Render logs.
    - backup_message_id: the message in BOOKS_CHANNEL holding books.data
      right now (None if nothing has been backed up yet).
    - books_db_msg_id_env: whether BOOKS_DB_MSG_ID is set in the environment.
    If backup_message_id is set but books_db_msg_id_env is false, the
    library still survives redeploys (auto-discovered via the pinned
    message), but setting BOOKS_DB_MSG_ID to that value skips the lookup.
    """
    books_data = _ensure_books_enabled()
    return {
        "status": "ok",
        "book_count": len(books_data.books),
        "backup_message_id": getattr(books_data, "backup_message_id", None),
        "books_db_msg_id_env": bool(getattr(config, "BOOKS_DB_MSG_ID", None)),
        "books_channel_configured": bool(config.BOOKS_CHANNEL),
    }


@router.post("/covers/generate-all")
async def generate_all_covers_route(_admin: None = Depends(_require_admin)):
    """
    Generate covers (first-page render / embedded-cover extraction / title
    card fallback) for every book that doesn't already have one. Books that
    already have a cover — manually uploaded or previously generated — are
    always left alone. Requires admin password.
    """
    _ensure_books_enabled()
    try:
        summary = await generate_all_covers()
        return {"status": "ok", **summary}
    except Exception as e:
        logger.error(f"Bulk cover generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
    """Stream / download the original book file from BOOKS_CHANNEL."""
    _ensure_books_enabled()
    try:
        return await stream_book(book_id, request)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Book not found")
    except Exception as e:
        logger.error(f"Error streaming book {book_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{book_id}/reader-info")
async def get_reader_info(book_id: str):
    """
    Tell the frontend reader what format to render and whether it's ready
    yet. PDF/EPUB/TXT are ready immediately; MOBI/AZW3/DJVU go through a
    background conversion first, so the frontend should poll this
    endpoint (every couple of seconds) while status == "converting".
    """
    books_data = _ensure_books_enabled()
    book = books_data.get_book(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return {
        "status": "ok",
        "reader_status": book.reader_status,
        "reader_format": book.reader_format,
        "reader_error": book.reader_error,
        "reader_url": f"/api/books/{book_id}/reader-file" if book.reader_status == "ready" else None,
    }


@router.get("/{book_id}/reader-file")
async def get_reader_file(book_id: str, request: Request):
    """Stream the reader-ready version of the book (original or converted)."""
    _ensure_books_enabled()
    try:
        return await stream_reader_file(book_id, request)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Book not found")
    except RuntimeError:
        raise HTTPException(
            status_code=409,
            detail="Reader version isn't ready yet. Poll /reader-info until reader_status is 'ready'.",
        )
    except Exception as e:
        logger.error(f"Error streaming reader file for {book_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{book_id}/cover")
async def get_cover(book_id: str, request: Request):
    """Stream a book's cover image. 404 if no cover has been set yet —
    the frontend should fall back to a placeholder icon in that case."""
    _ensure_books_enabled()
    try:
        return await stream_cover(book_id, request)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Book not found")
    except LookupError:
        raise HTTPException(status_code=404, detail="No cover set for this book")
    except Exception as e:
        logger.error(f"Error streaming cover for {book_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{book_id}/cover")
async def upload_cover(
    book_id: str,
    file: UploadFile = File(...),
    _admin: None = Depends(_require_admin),
):
    """
    Upload a cover image for one book from the admin section. Stored only
    in BOOKS_CHANNEL (as a document, to avoid Telegram's photo
    recompression) and attached via cover_message_id. Requires admin
    password. Replaces any existing cover for this book.
    """
    books_data = _ensure_books_enabled()
    if not books_data.get_book(book_id):
        raise HTTPException(status_code=404, detail="Book not found")

    filename = file.filename or "cover.jpg"
    ext = Path(filename).suffix.lower()
    if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported image type: {ext}. Allowed: jpg, jpeg, png, webp",
        )

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            tmp_path = tmp.name
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")

    try:
        book = await upload_cover_to_telegram(book_id, tmp_path, source="upload")
        return {"status": "ok", "book": book.to_dict()}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Book not found")
    except Exception as e:
        logger.error(f"Cover upload failed for {book_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


@router.post("/{book_id}/cover/generate")
async def generate_cover(
    book_id: str,
    force: bool = Query(True, description="Regenerate even if a cover already exists"),
    _admin: None = Depends(_require_admin),
):
    """
    Auto-generate a cover for one book: renders the book's first page
    (PDF/DJVU), extracts the embedded cover (EPUB/MOBI/AZW3), or falls back
    to a generated title card — then uploads it to BOOKS_CHANNEL. Requires
    admin password. Defaults to always (re)generating for this one book;
    pass force=false to only generate if it doesn't have a cover yet.
    """
    _ensure_books_enabled()
    book, status = await generate_cover_for_book(book_id, force=force)
    if status == "not_found":
        raise HTTPException(status_code=404, detail="Book not found")
    if status == "error":
        raise HTTPException(status_code=500, detail="Cover generation failed for this book")
    return {"status": "ok", "result": status, "book": book.to_dict()}


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
async def update_book_metadata(
    book_id: str, request: Request, _admin: None = Depends(_require_admin)
):
    """Update title, author, description, tags, language of a book. Requires admin password."""
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
async def delete_book(book_id: str, _admin: None = Depends(_require_admin)):
    """
    Remove a book from the library metadata. Requires admin password.
    Note: the file remains in the Telegram channel (safe default).
    """
    books_data = _ensure_books_enabled()
    ok = books_data.delete_book(book_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Book not found")
    return {"status": "ok", "message": "Book removed from library"}
