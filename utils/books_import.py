"""
books_import.py — Bulk import books from any Telegram channel into the library
=============================================================================

The books equivalent of utils/fast_import.py, and it borrows that module's
central trick:

  copy_message()     = re-uploads the file bytes          -> slow
  forward_messages() = server-side copy, one MTProto call -> fast

forward_messages() takes up to 100 message ids per call and Telegram does the
copy on its own servers: no bytes touch this process, which matters twice over
here because this app runs on a 512MB instance.

ONE IMPORTANT DIFFERENCE FROM THE COURSES IMPORTER
--------------------------------------------------
The drive/courses importer has a "fast" mode that registers files by
(channel_id, message_id) pointing straight at the *source* channel, skipping
the copy entirely. Books cannot do that: utils/books.stream_book() and
stream_cover() both read from config.BOOKS_CHANNEL by message id, and the
whole library (covers, converted reader files, the books.data backup) assumes
a single channel. So every imported book really is forwarded into
BOOKS_CHANNEL. It is still one server-side call per 100 files.

MEMORY DISCIPLINE (Render free tier: 512MB, shared with everything else)
-----------------------------------------------------------------------
The import splits into two phases with very different costs:

  Phase A - forward + register. No file content is ever downloaded. Metadata
    comes from the filename only (utils.book_metadata.parse_filename, pure
    string work, no I/O). Registrations go through
    BooksLibrary.register_book() with a single bulk_save() per batch, so the
    library is pickled once per 100 books instead of once per book.

  Phase B - enrichment. This is the expensive half: it downloads the actual
    file to read embedded PDF/EPUB metadata and render a cover. It runs
    strictly one book at a time, never concurrently, skips files above a size
    cap, deletes temp files in finally, and checks the cgroup memory reading
    before each book so it pauses instead of pushing the instance over. It is
    also resumable: each book is flagged enriched=True as it completes, so an
    interrupted run can simply be started again.

Phase B is deliberately slow. On this hosting tier "slow but survives" beats
"fast but gets OOM-killed mid-request", which takes the whole service down.
"""

from __future__ import annotations

import asyncio
import gc
import os
import secrets
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyrogram import Client

import config
from utils.logger import Logger
from utils.book_covers import generate_cover_image
from utils.book_metadata import (
    merge_metadata,
    parse_filename,
    read_embedded_metadata,
)
from utils.books import (
    ALLOWED_BOOK_EXTENSIONS,
    Book,
    begin_import_window,
    claim_import_message_ids,
    end_import_window,
    get_books_data,
    reader_kwargs_for_ext,
    upload_cover_to_telegram,
)

logger = Logger(__name__)

# ── Shared state, polled by the API / bot / admin UI ────────────────────────
IMPORT_PROGRESS: Dict[str, Dict[str, Any]] = {}
IMPORT_CANCEL: set = set()

# Progress dicts are tiny but unbounded growth is still growth on a 512MB
# instance, and nobody polls an import from three days ago. Keep the most
# recent handful and drop the rest whenever a new import starts.
MAX_PROGRESS_ENTRIES = 12

# ── Tuning ──────────────────────────────────────────────────────────────────
SCAN_BATCH_SIZE = 200      # ids per get_messages call (Pyrogram/TG max)
FORWARD_BATCH_SIZE = 100   # ids per forward_messages call (TG hard max)
INTER_BATCH_DELAY = 2.5    # seconds between forward batches, avoids flood wait
FLOOD_WAIT_CAP = 35        # never sleep longer than this on a FLOOD_WAIT

# Phase B pacing. A pause between books is not politeness — it is what lets
# the event loop actually run a GC cycle and lets the OS reclaim the native
# buffers PyMuPDF/Pillow/Calibre allocated before the next book starts
# allocating its own.
ENRICH_PAUSE_SECONDS = 0.5
# Files bigger than this are registered and left with filename-derived
# metadata; downloading them to peek at their metadata is not worth the risk.
MAX_ENRICH_SOURCE_BYTES = 120 * 1024 * 1024  # 120MB
# Refuse to start another book unless this much of the memory limit is free.
MIN_FREE_MEMORY_BYTES = 80 * 1024 * 1024     # 80MB
MEMORY_WAIT_SECONDS = 2.0
MEMORY_WAIT_MAX_TRIES = 15


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _media(msg) -> Optional[Any]:
    """The media object of a message, or None. Photos/stickers are not books,
    but they are still media — they get filtered out later by extension, which
    keeps this check identical to the courses importer's."""
    if not msg or getattr(msg, "empty", True):
        return None
    return msg.document or msg.video or msg.audio


def _parse_flood_wait(err_str: str) -> int:
    """Pull the wait seconds out of a Telegram FLOOD_WAIT_x error string."""
    try:
        return min(int(err_str.split("_")[-1]), FLOOD_WAIT_CAP)
    except Exception:
        return FLOOD_WAIT_CAP


def _prune_progress() -> None:
    if len(IMPORT_PROGRESS) <= MAX_PROGRESS_ENTRIES:
        return
    ordered = sorted(
        IMPORT_PROGRESS.items(), key=lambda kv: kv[1].get("start_time", 0)
    )
    for import_id, _ in ordered[: len(ordered) - MAX_PROGRESS_ENTRIES]:
        IMPORT_PROGRESS.pop(import_id, None)


def _is_book_filename(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_BOOK_EXTENSIONS


def memory_headroom() -> Optional[Tuple[int, int]]:
    """
    Current and maximum memory for this container, in bytes, read from the
    cgroup the process is actually confined by — os/psutil figures report the
    *host* machine, which on a shared instance is wildly optimistic and would
    happily tell us there are gigabytes free right up until the OOM kill.

    Returns (used, limit) or None when the values aren't readable (running
    outside a container, cgroup v1 with a different layout, "max" limit, etc.),
    in which case callers just proceed — this is a safety bonus, not a
    dependency.
    """
    try:
        with open("/sys/fs/cgroup/memory.current") as f:      # cgroup v2
            used = int(f.read().strip())
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw == "max":
            return None
        return used, int(raw)
    except Exception:
        pass
    try:
        with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f:  # v1
            used = int(f.read().strip())
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read().strip())
        # v1 reports an absurd sentinel (near 2^63) when unlimited.
        if limit > (1 << 62):
            return None
        return used, limit
    except Exception:
        return None


async def wait_for_memory_headroom(label: str = "") -> bool:
    """
    Block until at least MIN_FREE_MEMORY_BYTES of the container limit is free,
    running a GC pass between checks. Returns True if there is headroom (or if
    it can't be measured), False if it stayed tight for the whole wait — in
    which case the caller should skip this item rather than force it through.
    """
    for attempt in range(MEMORY_WAIT_MAX_TRIES):
        reading = memory_headroom()
        if reading is None:
            return True
        used, limit = reading
        if limit - used >= MIN_FREE_MEMORY_BYTES:
            return True
        if attempt == 0:
            logger.warning(
                f"Low memory headroom before {label or 'next item'}: "
                f"{(limit - used) / 1024 / 1024:.0f}MB free of "
                f"{limit / 1024 / 1024:.0f}MB — pausing."
            )
        gc.collect()
        await asyncio.sleep(MEMORY_WAIT_SECONDS)
    logger.warning(f"Skipping {label or 'item'}: memory never recovered.")
    return False


# ---------------------------------------------------------------------------
# Phase A — scan the source channel, forward, register
# ---------------------------------------------------------------------------
class BooksImportManager:
    """Stateless; the singleton at the bottom of this module exists only so
    callers have something to hold onto, matching SMART_IMPORT_MANAGER."""

    async def validate_channel_access(
        self, client: Client, channel_identifier: str
    ) -> Tuple[bool, Any, bool]:
        """
        Returns (is_valid, chat_or_error_message, is_admin).

        Public channels can be read without the bot being a member. Private
        ones need the bot added first, and there is no way to tell the user
        that after the fact, so it is checked up front — an import that dies
        200 files in is much worse than one that refuses to start.
        """
        try:
            channel = await client.get_chat(channel_identifier)
        except Exception as e:
            err = str(e)
            if any(
                k in err
                for k in ("No chat", "No user", "USERNAME_INVALID", "PEER_ID_INVALID")
            ):
                return (
                    False,
                    f"Channel '{channel_identifier}' not found. "
                    "Check the @username, invite-free public link, or numeric id.",
                    False,
                )
            return False, f"Cannot access channel: {err}", False

        is_public = bool(getattr(channel, "username", None))
        is_admin = False
        try:
            me = await client.get_chat_member(channel.id, "me")
            priv = getattr(me, "privileges", None)
            if priv:
                is_admin = bool(
                    priv.can_delete_messages
                    or priv.can_edit_messages
                    or priv.can_post_messages
                    or getattr(priv, "is_anonymous", False)
                )
        except Exception:
            if not is_public:
                title = getattr(channel, "title", channel_identifier)
                return (
                    False,
                    f"Bot is not a member of '{title}'.\n"
                    "Add the bot to the private channel first. "
                    "Public channels work without adding the bot.",
                    False,
                )
        return True, channel, is_admin

    async def _scan_media(
        self,
        client: Client,
        channel_id: int,
        msg_ids: List[int],
        import_id: str,
    ) -> List[Tuple[int, str, int]]:
        """
        Batch-fetch messages and keep the ones that are actually book files.
        Returns [(msg_id, filename, size), ...].

        Unlike the courses importer this filters on extension here rather than
        forwarding everything: a books channel is usually a mixed bag of PDFs,
        cover images, chat and forwarded posts, and forwarding the non-books
        would clutter BOOKS_CHANNEL with files the library then has to ignore
        forever.
        """
        results: List[Tuple[int, str, int]] = []
        total = len(msg_ids)
        progress = IMPORT_PROGRESS[import_id]

        for i in range(0, total, SCAN_BATCH_SIZE):
            if import_id in IMPORT_CANCEL:
                break
            batch = msg_ids[i : i + SCAN_BATCH_SIZE]
            msgs = None

            for attempt in range(3):
                try:
                    raw = await client.get_messages(channel_id, batch)
                    msgs = raw if isinstance(raw, list) else [raw]
                    break
                except Exception as e:
                    err = str(e)
                    wait = _parse_flood_wait(err) if "FLOOD_WAIT" in err else 2
                    if attempt < 2:
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(
                            f"Scan batch {batch[0]}-{batch[-1]} failed: {e}"
                        )
                        progress["skipped"] += len(batch)

            if msgs is None:
                continue

            for msg in msgs:
                media = _media(msg)
                if media is None:
                    progress["skipped"] += 1
                    continue
                filename = getattr(media, "file_name", None) or ""
                if not filename or not _is_book_filename(filename):
                    progress["skipped_not_book"] += 1
                    continue
                results.append((msg.id, filename, getattr(media, "file_size", 0) or 0))

            progress.update({"fetched": min(i + len(batch), total), "total_scan": total})
            await asyncio.sleep(0)  # yield so progress polling stays responsive

        return results

    def _filter_duplicates(
        self, file_list: List[Tuple[int, str, int]], import_id: str
    ) -> List[Tuple[int, str, int]]:
        """
        Drop anything already in the library before forwarding it.

        Done *before* the forward on purpose: skipping after the fact would
        still have copied the file into BOOKS_CHANNEL, leaving an orphaned
        message that nothing references and nothing cleans up. The check uses
        filename+size, the same fallback BooksLibrary.find_duplicate() applies
        for books with no file_hash — there is no hash available here without
        downloading the file, which is exactly what this phase avoids.

        Duplicates *within the same source channel* are caught too, so a
        channel that posted the same PDF three times only imports it once.
        """
        books_data = get_books_data()
        progress = IMPORT_PROGRESS[import_id]
        seen_in_batch: set = set()
        keep: List[Tuple[int, str, int]] = []

        for msg_id, filename, size in file_list:
            key = (filename.strip().lower(), size)
            if key in seen_in_batch:
                progress["skipped_duplicate"] += 1
                continue
            if books_data.find_duplicate(None, filename, size):
                progress["skipped_duplicate"] += 1
                seen_in_batch.add(key)
                continue
            seen_in_batch.add(key)
            keep.append((msg_id, filename, size))

        return keep

    async def _forward_and_register(
        self,
        client: Client,
        file_list: List[Tuple[int, str, int]],
        channel_id: int,
        channel_label: str,
        import_id: str,
    ) -> List[str]:
        """
        Forward in batches of FORWARD_BATCH_SIZE into BOOKS_CHANNEL and
        register each resulting message as a Book.

        Batches run sequentially with INTER_BATCH_DELAY between them. Running
        them in parallel does not help: Telegram rate-limits forwards per bot,
        so parallel calls just trade throughput for flood waits and retries.

        Returns the ids of the books created, in order, for the enrichment
        phase to walk through.
        """
        progress = IMPORT_PROGRESS[import_id]
        books_data = get_books_data()
        new_book_ids: List[str] = []

        all_ids = [row[0] for row in file_list]
        batches = [
            all_ids[i : i + FORWARD_BATCH_SIZE]
            for i in range(0, len(all_ids), FORWARD_BATCH_SIZE)
        ]
        total_batches = len(batches)

        for batch_num, batch_ids in enumerate(batches):
            if import_id in IMPORT_CANCEL:
                break

            for attempt in range(5):
                try:
                    forwarded = await client.forward_messages(
                        chat_id=config.BOOKS_CHANNEL,
                        from_chat_id=channel_id,
                        message_ids=batch_ids,
                        hide_sender_name=True,
                        disable_notification=True,
                    )
                    if not isinstance(forwarded, list):
                        forwarded = [forwarded] if forwarded else []

                    # Claim the new message ids immediately. The channel
                    # listener is watching BOOKS_CHANNEL and has already been
                    # woken by these forwards; without the claim it would race
                    # this loop and register the same files a second time with
                    # worse metadata. See the import window guard in books.py.
                    claim_import_message_ids(m.id for m in forwarded if m)

                    for fwd in forwarded:
                        if not fwd:
                            continue
                        media = _media(fwd)
                        if not media:
                            continue  # TG dropped it (deleted/protected source)
                        filename = (
                            getattr(media, "file_name", None) or f"book_{fwd.id}"
                        )
                        if not _is_book_filename(filename):
                            progress["skipped_not_book"] += 1
                            continue
                        book = self._build_book(fwd.id, filename, media, channel_label)
                        books_data.register_book(book)
                        new_book_ids.append(book.id)
                        progress["imported"] += 1

                    # ONE save for the whole batch, never one per book — see
                    # BooksLibrary.register_book() for why that matters here.
                    books_data.bulk_save()

                    logger.info(
                        f"[BooksImport] batch {batch_num + 1}/{total_batches} "
                        f"({len(batch_ids)} ids) done, "
                        f"imported so far: {progress['imported']}"
                    )
                    break

                except Exception as e:
                    err = str(e)
                    if "FLOOD_WAIT" in err:
                        wait = _parse_flood_wait(err)
                        logger.warning(
                            f"[BooksImport] flood wait {wait}s on batch "
                            f"{batch_num + 1}/{total_batches}"
                        )
                        await asyncio.sleep(wait)
                    elif attempt < 4:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error(
                            f"[BooksImport] batch {batch_num + 1} failed: {e}"
                        )
                        progress["errors"] += len(batch_ids)

            progress["batches_done"] = batch_num + 1
            if batch_num < total_batches - 1 and import_id not in IMPORT_CANCEL:
                await asyncio.sleep(INTER_BATCH_DELAY)

        return new_book_ids

    @staticmethod
    def _build_book(
        message_id: int, filename: str, media, channel_label: str
    ) -> Book:
        """
        Turn a forwarded message into a Book using filename-derived metadata
        only. No file content is read here — parse_filename() is pure string
        work, so this costs nothing per book and is safe to run for thousands
        of files. Anything better (real title, author, description, language,
        cover) comes later from enrich_books().
        """
        ext = Path(filename).suffix.lower()
        parsed = parse_filename(filename)
        title = parsed.get("title") or Path(filename).stem
        return Book(
            title=title,
            message_id=message_id,
            size=getattr(media, "file_size", 0) or 0,
            filename=filename,
            author=parsed.get("author", ""),
            source_channel=channel_label,
            **reader_kwargs_for_ext(ext, message_id),
        )

    # ── Master entry point ─────────────────────────────────────────────────
    async def import_from_channel(
        self,
        client: Client,
        channel_identifier: str,
        start_msg_id: Optional[int] = None,
        end_msg_id: Optional[int] = None,
        skip_duplicates: bool = True,
        enrich: bool = True,
        generate_covers: bool = True,
        import_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Import every book file from `channel_identifier` into BOOKS_CHANNEL and
        register it in the library.

        start_msg_id/end_msg_id limit the import to a message-id range (both
        ends inclusive); leave them out to walk the channel's whole history.
        Live progress is written to IMPORT_PROGRESS[import_id] for the admin UI
        and the bot to poll.

        Returns the final progress dict.
        """
        if import_id is None:
            import_id = secrets.token_hex(8)

        _prune_progress()
        progress: Dict[str, Any] = {
            "import_id": import_id,
            "status": "validating",
            "imported": 0,
            "skipped": 0,
            "skipped_duplicate": 0,
            "skipped_not_book": 0,
            "errors": 0,
            "fetched": 0,
            "total_scan": 0,
            "total_media": 0,
            "batches_done": 0,
            "enriched": 0,
            "covers": 0,
            "enrich_total": 0,
            "start_time": time.time(),
            "channel": channel_identifier,
            "channel_name": channel_identifier,
            "error_msg": None,
        }
        IMPORT_PROGRESS[import_id] = progress

        if not config.BOOKS_CHANNEL:
            progress.update({"status": "error", "error_msg": "BOOKS_CHANNEL is not configured"})
            return progress

        # Everything from here on runs inside the import window so the
        # BOOKS_CHANNEL listener stands down while we forward into it. The
        # try/finally is what guarantees the window closes even if the import
        # blows up — leaving it open would leave the listener half-deaf for
        # the rest of the process's life.
        begin_import_window()
        try:
            is_valid, result, is_admin = await self.validate_channel_access(
                client, channel_identifier
            )
            if not is_valid:
                progress.update({"status": "error", "error_msg": result})
                return progress

            channel = result
            channel_id = channel.id
            channel_label = (
                f"@{channel.username}"
                if getattr(channel, "username", None)
                else str(channel_id)
            )
            progress["channel_name"] = getattr(channel, "title", channel_identifier)
            progress["source_channel"] = channel_label
            progress["is_admin"] = is_admin

            # Build the candidate id list.
            if start_msg_id and end_msg_id:
                if end_msg_id < start_msg_id:
                    start_msg_id, end_msg_id = end_msg_id, start_msg_id
                msg_ids = list(range(start_msg_id, end_msg_id + 1))
            else:
                progress["status"] = "scanning"
                msg_ids = []
                async for msg in client.get_chat_history(channel_id):
                    if _media(msg):
                        msg_ids.append(msg.id)
                    if import_id in IMPORT_CANCEL:
                        break
                msg_ids.reverse()  # oldest first, so ids stay in posting order

            progress.update({"total_scan": len(msg_ids), "status": "fetching"})

            # Always scan, even for an explicit range: we need each file's name
            # to tell books from everything else in the channel, and to run the
            # duplicate check before anything is copied.
            file_list = await self._scan_media(client, channel_id, msg_ids, import_id)
            progress["total_media"] = len(file_list)

            if skip_duplicates:
                progress["status"] = "deduplicating"
                file_list = self._filter_duplicates(file_list, import_id)
                progress["to_import"] = len(file_list)

            if import_id in IMPORT_CANCEL:
                progress["status"] = "cancelled"
                return progress
            if not file_list:
                progress.update(
                    {
                        "status": "done",
                        "elapsed": round(time.time() - progress["start_time"], 1),
                    }
                )
                return progress

            progress["status"] = "importing"
            new_book_ids = await self._forward_and_register(
                client, file_list, channel_id, channel_label, import_id
            )

            if enrich and new_book_ids and import_id not in IMPORT_CANCEL:
                progress["status"] = "enriching"
                progress["enrich_total"] = len(new_book_ids)
                await enrich_books(
                    new_book_ids,
                    import_id=import_id,
                    generate_covers=generate_covers,
                )

            progress.update(
                {
                    "status": "cancelled" if import_id in IMPORT_CANCEL else "done",
                    "elapsed": round(time.time() - progress["start_time"], 1),
                }
            )
            return progress

        except Exception as e:
            logger.error(f"[BooksImport] import {import_id} crashed: {e}")
            progress.update({"status": "error", "error_msg": str(e)})
            return progress
        finally:
            IMPORT_CANCEL.discard(import_id)
            # Registrations are already saved by now (bulk_save runs per batch),
            # so any listener callback still waiting will fall through to its
            # "already registered" check. Closing the window here is safe.
            end_import_window()


BOOKS_IMPORT_MANAGER = BooksImportManager()

# ---------------------------------------------------------------------------
# Phase B — enrichment (the expensive half)
# ---------------------------------------------------------------------------
# books.data is pickled once per this many enriched books rather than after
# every one. Enrichment is dominated by the download, so a save every 10 books
# is invisible in wall-clock terms while cutting the number of full-library
# serializations by 10x.
ENRICH_SAVE_EVERY = 10


def _apply_metadata(book, merged: Dict[str, str]) -> bool:
    """
    Copy merged metadata onto a Book, but only where it is an improvement.

    Never overwrites a non-empty existing value with an empty one, and never
    replaces a description or tags an admin may have already edited by hand.
    Title and author *are* replaced when embedded metadata supplies them,
    because the value being replaced is a filename guess and the embedded one
    came from the file itself.
    """
    changed = False
    for field in ("title", "author", "language"):
        value = (merged.get(field) or "").strip()
        if value and value != getattr(book, field, ""):
            setattr(book, field, value)
            changed = True

    description = (merged.get("description") or "").strip()
    if description and not (book.description or "").strip():
        book.description = description[:2000]
        changed = True

    tags = merged.get("tags") or ""
    if tags and not book.tags:
        book.tags = [t.strip() for t in tags.split(",") if t.strip()][:8]
        changed = True

    return changed


async def _enrich_one(book_id: str, generate_covers: bool) -> Dict[str, Any]:
    """
    Enrich exactly one book: read its embedded metadata, render a cover, and
    (for MOBI/AZW3/DJVU) build the in-browser reader version — all from a
    single download of the file.

    Doing all three off one download is the main memory and bandwidth win
    here. The pre-existing per-book cover route downloads the file itself, so
    running "generate covers" separately after an import would fetch every
    file a second time.

    Returns {"status": ..., "metadata": bool, "cover": bool, "error": str|None}
    where status is one of: enriched, skipped_large, skipped_memory,
    not_found, error.
    """
    # Imported lazily and by their private names on purpose: these live in
    # books.py alongside the data they mutate, and re-exporting them would
    # invite calling them from outside this package, where the memory
    # discipline this module enforces would not be in force.
    from utils.books import _convert_and_attach_reader, _download_book_source_message
    from utils.book_converter import needs_conversion

    books_data = get_books_data()
    book = books_data.get_book(book_id)
    if not book:
        return {"status": "not_found", "metadata": False, "cover": False, "error": None}

    ext = Path(book.filename).suffix.lower()
    label = f"{book.title[:40]} ({ext or 'no ext'})"

    if book.size and book.size > MAX_ENRICH_SOURCE_BYTES:
        # Too big to safely pull onto a 512MB instance. Flag it as enriched so
        # a re-run doesn't keep retrying something that can never succeed; the
        # filename-derived title/author it already has stand.
        book.enriched = True
        logger.info(
            f"[Enrich] skipping {label}: {book.size / 1024 / 1024:.0f}MB is over "
            f"the {MAX_ENRICH_SOURCE_BYTES / 1024 / 1024:.0f}MB cap"
        )
        return {"status": "skipped_large", "metadata": False, "cover": False, "error": None}

    if not await wait_for_memory_headroom(label):
        # Left un-enriched deliberately: unlike the size cap this is a
        # transient condition, so a later run should try again.
        return {"status": "skipped_memory", "metadata": False, "cover": False, "error": None}

    download_path: Optional[str] = None
    cover_path: Optional[str] = None
    got_metadata = False
    got_cover = False
    error: Optional[str] = None

    try:
        download_path = await _download_book_source_message(book)

        try:
            embedded = await read_embedded_metadata(download_path, ext)
            merged = merge_metadata(parse_filename(book.filename), embedded)
            got_metadata = _apply_metadata(book, merged)
        except Exception as e:
            error = f"metadata: {e}"
            logger.warning(f"[Enrich] metadata failed for {label}: {e}")

        if generate_covers and not book.cover_message_id:
            try:
                cover_path, cover_err = await generate_cover_image(
                    download_path, ext, title=book.title, author=book.author
                )
                if cover_path:
                    await upload_cover_to_telegram(
                        book.id, cover_path, source="generated"
                    )
                    got_cover = True
                else:
                    logger.warning(f"[Enrich] no cover for {label}: {cover_err}")
            except Exception as e:
                error = f"{error + '; ' if error else ''}cover: {e}"
                logger.warning(f"[Enrich] cover failed for {label}: {e}")

        # Reader conversion last, because it consumes (and deletes) the
        # downloaded file. Awaited rather than fired off as a task so it stays
        # inside this book's turn — the whole point of this loop is that only
        # one file is ever being worked on at a time.
        if needs_conversion(ext) and book.reader_status == "converting":
            source_for_conversion, download_path = download_path, None
            try:
                await _convert_and_attach_reader(book.id, source_for_conversion, ext)
            except Exception as e:
                logger.warning(f"[Enrich] reader conversion failed for {label}: {e}")

        book.enriched = True
        return {
            "status": "enriched",
            "metadata": got_metadata,
            "cover": got_cover,
            "error": error,
        }

    except Exception as e:
        logger.error(f"[Enrich] crashed on {label}: {e}")
        return {"status": "error", "metadata": False, "cover": False, "error": str(e)}
    finally:
        for path in (download_path, cover_path):
            if path:
                try:
                    os.remove(path)
                except OSError:
                    pass


async def enrich_books(
    book_ids: Optional[List[str]] = None,
    import_id: Optional[str] = None,
    generate_covers: bool = True,
    only_unenriched: bool = True,
) -> Dict[str, Any]:
    """
    Walk a list of books one at a time, reading embedded metadata and
    generating covers.

    Pass book_ids to enrich a specific import's output, or omit it to sweep the
    whole library for books that were never enriched (this is what makes the
    pass resumable after a restart, a cancel, or an OOM kill).

    STRICTLY SEQUENTIAL, BY DESIGN. There is no semaphore here because there
    is no concurrency to limit: each book is fully finished — downloaded,
    parsed, cover uploaded, temp files deleted — before the next one is even
    looked at. The existing generate_all_covers() in books.py learned this the
    hard way; its comment records that even two concurrent downloads were
    enough to tip a 512MB instance over.

    Between books it forces a GC pass and sleeps briefly, which is what
    actually lets the OS reclaim the native buffers PyMuPDF, Pillow and
    Calibre allocate. Before each book it re-checks the container's memory
    reading and waits, or skips that book, rather than pushing through.
    """
    books_data = get_books_data()

    if book_ids is None:
        book_ids = [
            b.id
            for b in books_data.books.values()
            if not getattr(b, "enriched", False)
        ]
    elif only_unenriched:
        book_ids = [
            bid
            for bid in book_ids
            if (b := books_data.get_book(bid)) and not getattr(b, "enriched", False)
        ]

    results: Dict[str, Any] = {
        "total": len(book_ids),
        "enriched": 0,
        "metadata_updated": 0,
        "covers": 0,
        "skipped_large": 0,
        "skipped_memory": 0,
        "errors": 0,
    }
    progress = IMPORT_PROGRESS.get(import_id) if import_id else None
    if progress is not None:
        progress["enrich_total"] = len(book_ids)

    if not book_ids:
        return results

    logger.info(f"[Enrich] starting sequential pass over {len(book_ids)} book(s)")
    since_save = 0

    for index, book_id in enumerate(book_ids):
        if import_id and import_id in IMPORT_CANCEL:
            logger.info("[Enrich] cancelled by request")
            break

        outcome = await _enrich_one(book_id, generate_covers)
        status = outcome["status"]

        if status == "enriched":
            results["enriched"] += 1
            if outcome["metadata"]:
                results["metadata_updated"] += 1
            if outcome["cover"]:
                results["covers"] += 1
        elif status == "skipped_large":
            results["skipped_large"] += 1
        elif status == "skipped_memory":
            results["skipped_memory"] += 1
        elif status != "not_found":
            results["errors"] += 1

        if progress is not None:
            progress["enriched"] = results["enriched"]
            progress["covers"] = results["covers"]
            progress["enrich_done"] = index + 1

        since_save += 1
        if since_save >= ENRICH_SAVE_EVERY:
            books_data.bulk_save()
            since_save = 0

        # Give the loop a real chance to reclaim memory before the next file.
        gc.collect()
        await asyncio.sleep(ENRICH_PAUSE_SECONDS)

    # Always save the tail, including on the cancel path — work already done
    # should not be thrown away just because the run was stopped early.
    books_data.bulk_save()
    logger.info(
        f"[Enrich] done: {results['enriched']}/{results['total']} enriched, "
        f"{results['covers']} covers, {results['skipped_large']} too large, "
        f"{results['skipped_memory']} deferred, {results['errors']} errors"
    )
    return results


def get_import_progress(import_id: str) -> Optional[Dict[str, Any]]:
    return IMPORT_PROGRESS.get(import_id)


def cancel_import(import_id: str) -> bool:
    """Ask a running import to stop at the next safe point. Files already
    forwarded and registered stay — this is a stop, not an undo."""
    if import_id not in IMPORT_PROGRESS:
        return False
    IMPORT_CANCEL.add(import_id)
    IMPORT_PROGRESS[import_id]["status"] = "cancelling"
    return True


def count_unenriched() -> int:
    """How many books are still waiting on an enrichment pass — powers the
    'Enrich N books' button in the admin panel."""
    try:
        books_data = get_books_data()
    except Exception:
        return 0
    return sum(
        1 for b in books_data.books.values() if not getattr(b, "enriched", False)
    )
