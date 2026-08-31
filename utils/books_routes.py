"""
Books API Routes
================
Clean endpoints for the Vercel Books frontend.
All books live in BOOKS_CHANNEL and are completely hidden from the main TGDrive UI.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import tempfile
import time
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


# ---------------------------------------------------------------------------
# Bulk import from a Telegram channel
# ---------------------------------------------------------------------------
# Declared above the "/{book_id}" routes below. FastAPI matches in declaration
# order and a path parameter never spans a "/", so two-segment paths like
# /admin/import could not be swallowed by /{book_id} anyway — but keeping the
# literal routes first means that stays true even if someone later adds a
# catch-all.

# Background import tasks live here rather than on app.state (which a router
# has no handle on). The set holds a strong reference for the task's whole
# life: without it the only reference is the event loop's, and a task sitting
# in the asyncio.sleep() between forward batches can be garbage collected
# mid-import. Same reason main.py keeps app.state.import_tasks.
_BOOKS_IMPORT_TASKS: set = set()


def _spawn_import_task(coro) -> None:
    task = asyncio.create_task(coro)
    _BOOKS_IMPORT_TASKS.add(task)
    task.add_done_callback(_BOOKS_IMPORT_TASKS.discard)


@router.post("/admin/import")
async def start_channel_import(
    request: Request, _admin: None = Depends(_require_admin)
):
    """
    Start a bulk import from a Telegram channel into the books library and
    return immediately with an import_id to poll.

    Body:
      channel          (required) @username, invite-free public link, or -100… id
      start_msg_id     optional, first message id to consider
      end_msg_id       optional, last message id to consider (inclusive)
      skip_duplicates  default true  — skip files already in the library
      enrich           default true  — read embedded metadata after importing
      generate_covers  default true  — generate covers during enrichment

    This returns as soon as the task is scheduled because a real import takes
    minutes: the forwarding is fast, but enrichment is deliberately sequential
    (one file downloaded at a time) to stay inside the instance's memory
    budget, so there is no HTTP request worth holding open for it.
    """
    _ensure_books_enabled()

    from utils.books_import import BOOKS_IMPORT_MANAGER, IMPORT_PROGRESS
    from utils.clients import get_client

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Expected a JSON body")

    channel = (data.get("channel") or "").strip()
    if not channel:
        raise HTTPException(status_code=400, detail="'channel' is required")

    def _opt_int(key: str) -> Optional[int]:
        value = data.get(key)
        if value in (None, "", 0, "0"):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"'{key}' must be a number")

    start_msg_id = _opt_int("start_msg_id")
    end_msg_id = _opt_int("end_msg_id")
    if bool(start_msg_id) != bool(end_msg_id):
        raise HTTPException(
            status_code=400,
            detail="Give both start_msg_id and end_msg_id, or neither.",
        )

    skip_duplicates = bool(data.get("skip_duplicates", True))
    enrich = bool(data.get("enrich", True))
    generate_covers = bool(data.get("generate_covers", True))

    import_id = secrets.token_hex(8)
    client = get_client()

    async def _run():
        try:
            await BOOKS_IMPORT_MANAGER.import_from_channel(
                client,
                channel,
                start_msg_id=start_msg_id,
                end_msg_id=end_msg_id,
                skip_duplicates=skip_duplicates,
                enrich=enrich,
                generate_covers=generate_covers,
                import_id=import_id,
            )
        except Exception as e:
            # import_from_channel already records errors in IMPORT_PROGRESS;
            # this is the belt-and-braces case where it failed before it could.
            logger.error(f"Background books import {import_id} error: {e}")
            entry = IMPORT_PROGRESS.setdefault(import_id, {})
            entry["status"] = "error"
            entry["error_msg"] = str(e)

    _spawn_import_task(_run())
    logger.info(f"Books import {import_id} started from {channel}")
    return {"status": "started", "import_id": import_id}


@router.get("/admin/import/{import_id}")
async def get_channel_import_progress(
    import_id: str, _admin: None = Depends(_require_admin)
):
    """Poll a running or finished import. status cycles through validating →
    scanning → fetching → deduplicating → importing → enriching → done, or
    lands on cancelled / error."""
    from utils.books_import import get_import_progress

    progress = get_import_progress(import_id)
    if progress is None:
        raise HTTPException(status_code=404, detail="Unknown import_id")
    return {"status": "ok", "data": progress}


@router.post("/admin/import/{import_id}/cancel")
async def cancel_channel_import(
    import_id: str, _admin: None = Depends(_require_admin)
):
    """
    Ask a running import to stop at its next safe point.

    This is a stop, not an undo: books already forwarded and registered stay in
    the library. Rolling them back would mean deleting files out of
    BOOKS_CHANNEL, which is a far more destructive thing to hang off a Cancel
    button — use the admin book list to remove any you didn't want.
    """
    from utils.books_import import cancel_import

    if not cancel_import(import_id):
        raise HTTPException(status_code=404, detail="Unknown import_id")
    return {"status": "cancelling", "import_id": import_id}


@router.get("/admin/enrich-status")
async def enrich_status(_admin: None = Depends(_require_admin)):
    """How many books have never been through an enrichment pass."""
    _ensure_books_enabled()
    from utils.books_import import count_unenriched

    return {"status": "ok", "unenriched": count_unenriched()}


@router.post("/admin/enrich")
async def start_enrich_pass(
    request: Request, _admin: None = Depends(_require_admin)
):
    """
    Run the enrichment pass over every book that has never had one: read
    embedded PDF/EPUB metadata, fill in blank fields, and generate a cover.

    Also the recovery path — enrichment marks each book as it finishes, so if a
    previous run was cancelled or the instance restarted mid-pass, this picks
    up exactly where it stopped instead of re-downloading everything.

    Body: generate_covers (default true).
    Returns an import_id so the same progress endpoint can be polled.
    """
    _ensure_books_enabled()

    from utils.books_import import (
        IMPORT_CANCEL,
        IMPORT_PROGRESS,
        count_unenriched,
        enrich_books,
    )

    try:
        data = await request.json()
    except Exception:
        data = {}
    generate_covers = bool(data.get("generate_covers", True))

    pending = count_unenriched()
    if not pending:
        return {"status": "ok", "unenriched": 0, "import_id": None}

    import_id = secrets.token_hex(8)
    IMPORT_PROGRESS[import_id] = {
        "import_id": import_id,
        "status": "enriching",
        "imported": 0,
        "enriched": 0,
        "covers": 0,
        "enrich_total": pending,
        "enrich_done": 0,
        "errors": 0,
        "start_time": time.time(),
        "channel_name": "library re-enrichment",
        "error_msg": None,
    }

    async def _run():
        entry = IMPORT_PROGRESS[import_id]
        try:
            summary = await enrich_books(
                None, import_id=import_id, generate_covers=generate_covers
            )
            entry.update(summary)
            entry["status"] = "cancelled" if import_id in IMPORT_CANCEL else "done"
        except Exception as e:
            logger.error(f"Books enrichment {import_id} error: {e}")
            entry["status"] = "error"
            entry["error_msg"] = str(e)
        finally:
            entry["elapsed"] = round(time.time() - entry["start_time"], 1)
            IMPORT_CANCEL.discard(import_id)

    _spawn_import_task(_run())
    logger.info(f"Books enrichment {import_id} started for {pending} book(s)")
    return {"status": "started", "import_id": import_id, "unenriched": pending}


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
    allow_duplicate: bool = Form(False),
):
    """
    Upload a PDF / EPUB from the Books website.
    The file is stored only in BOOKS_CHANNEL and never appears in the main TGDrive.

    Rejects (409) an upload that's already in the library — same file
    content (or, for older entries without a stored hash, same filename
    + size) — unless allow_duplicate=true is sent, so someone who really
    does want to keep two copies (e.g. a different scan/edition sharing a
    filename) still can.
    """
    _ensure_books_enabled()

    filename = file.filename or "book.pdf"
    ext = Path(filename).suffix.lower()
    if ext not in {".pdf", ".epub", ".mobi", ".azw3", ".txt", ".djvu"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {ext}. Allowed: pdf, epub, mobi, azw3, txt, djvu",
        )

    # Save to a temporary file first, hashing as we go so we don't have to
    # read the file twice.
    suffix = ext or ".bin"
    hasher = hashlib.sha256()
    size = 0
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
                size += len(chunk)
                tmp.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")

    file_hash = hasher.hexdigest()

    try:
        if not allow_duplicate:
            books_data = get_books_data()
            existing = books_data.find_duplicate(file_hash, filename, size)
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": f'This book is already in the library as "{existing.title}".',
                        "existing_book": existing.to_dict(),
                    },
                )

        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        book = await upload_book_to_telegram(
            file_path=tmp_path,
            filename=filename,
            title=title or Path(filename).stem,
            author=author,
            description=description,
            tags=tag_list,
            language=language,
            file_hash=file_hash,
        )
        return {"status": "ok", "book": book.to_dict()}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Book upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


@router.get("/admin/duplicates")
async def list_duplicate_books(_admin: None = Depends(_require_admin)):
    """
    Find books that are likely duplicates of each other, already sitting
    in the library — for cleaning up ones uploaded 2-3 times before this
    check existed. Groups by identical file content where we have a hash,
    otherwise by matching filename+size.
    """
    books_data = _ensure_books_enabled()
    groups = books_data.find_duplicate_groups()
    return {
        "status": "ok",
        "group_count": len(groups),
        "duplicate_book_count": sum(len(g["books"]) - 1 for g in groups),
        "groups": groups,
    }


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
