"""
fast_import.py — Maximum-speed bulk/fast import via Pyrogram MTProto

Speed strategy:
  1. Public channels: bot never needs to be a member
  2. Batch get_messages: 200 IDs per single API call (200x faster scan)
  3. Concurrent copy_message: up to 15 parallel workers (15x faster copy)
  4. asyncio.gather on batches: no sequential waiting between files
  5. Real-time progress via IMPORT_PROGRESS dict (polled by frontend)
  6. Cancellable tasks via IMPORT_CANCEL set
  7. Automatic FLOOD_WAIT back-off
"""

import asyncio
import secrets
import time
from typing import Optional, Dict, Any, List, Tuple

from pyrogram import Client
from pyrogram.types import Message
from utils.logger import Logger
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL

logger = Logger(__name__)

# ── Shared state (keyed by import_id) ────────────────────────────────────────
IMPORT_PROGRESS: Dict[str, Dict[str, Any]] = {}
IMPORT_CANCEL: set = set()

# ── Speed knobs ───────────────────────────────────────────────────────────────
BATCH_SIZE       = 200   # Pyrogram max per get_messages call
COPY_WORKERS     = 15    # Parallel copy_message coroutines
FAST_WORKERS     = 50    # Fast-import (no network) can be much higher
FLOOD_WAIT_CAP   = 20    # Never sleep more than this many seconds on flood wait


# ── Helper: extract media from a message ─────────────────────────────────────
def _media_from_msg(msg) -> Optional[Any]:
    if not msg or msg.empty:
        return None
    return (
        msg.document or msg.video or msg.audio
        or msg.photo  or msg.sticker
    )


# ── Main class ────────────────────────────────────────────────────────────────
class SmartImportManager:

    # ── Channel validation ────────────────────────────────────────────────────
    async def validate_channel_access(
        self, client: Client, channel_identifier: str
    ) -> Tuple[bool, Any, bool]:
        """
        Returns (is_valid, channel_or_error_str, is_admin).

        Public channels work without the bot being a member at all.
        Private channels/groups require bot membership.
        """
        try:
            channel = await client.get_chat(channel_identifier)
        except Exception as e:
            err = str(e)
            if any(k in err for k in ("No chat", "No user", "USERNAME_INVALID", "PEER_ID_INVALID")):
                return False, f"Channel '{channel_identifier}' not found. Check username/ID.", False
            return False, f"Cannot access channel: {err}", False

        is_public = bool(getattr(channel, "username", None))

        # Try to determine admin status (best-effort)
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
            # Not a member — OK for public channels
            if not is_public:
                return False, (
                    f"Bot is not a member of '{getattr(channel, 'title', channel_identifier)}'.\n"
                    "For private channels/groups please add the bot first.\n"
                    "Public channels work without adding the bot."
                ), False

        return True, channel, is_admin

    # ── Phase 1: batch-fetch message list ─────────────────────────────────────
    async def _fetch_file_list(
        self,
        client: Client,
        channel_id: int,
        msg_ids: List[int],
        import_id: str,
    ) -> List[Tuple[int, str, int, int]]:
        """
        Returns list of (msg_id, file_name, file_size, duration).
        Uses BATCH_SIZE-sized get_messages calls instead of one-by-one.
        """
        results: List[Tuple[int, str, int, int]] = []
        total = len(msg_ids)

        for batch_start in range(0, total, BATCH_SIZE):
            if import_id in IMPORT_CANCEL:
                break

            batch = msg_ids[batch_start : batch_start + BATCH_SIZE]

            # Try up to 2 times
            messages = None
            for attempt in range(2):
                try:
                    raw = await client.get_messages(channel_id, batch)
                    messages = raw if isinstance(raw, list) else [raw]
                    break
                except Exception as e:
                    if attempt == 0:
                        await asyncio.sleep(1)
                    else:
                        logger.warning(f"Batch {batch[0]}-{batch[-1]} fetch failed: {e}")
                        IMPORT_PROGRESS[import_id]["skipped"] += len(batch)

            if messages is None:
                continue

            for msg in messages:
                media = _media_from_msg(msg)
                if media is None:
                    IMPORT_PROGRESS[import_id]["skipped"] += 1
                    continue
                fname = getattr(media, "file_name", None) or f"file_{msg.id}"
                fsize = getattr(media, "file_size", 0) or 0
                fdur  = getattr(media, "duration", 0) if hasattr(media, "duration") else 0
                results.append((msg.id, fname, fsize, fdur))

            IMPORT_PROGRESS[import_id]["fetched"] = batch_start + len(batch)
            IMPORT_PROGRESS[import_id]["total_scan"] = total
            # Yield control to event loop
            await asyncio.sleep(0)

        return results

    # ── Phase 2a: fast import (no copy) ──────────────────────────────────────
    async def _do_fast_import(
        self,
        file_list: List[Tuple[int, str, int, int]],
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ) -> None:
        sem = asyncio.Semaphore(FAST_WORKERS)

        async def one(msg_id: int, fname: str, fsize: int, fdur: int):
            async with sem:
                if import_id in IMPORT_CANCEL:
                    return
                try:
                    DRIVE_DATA.new_fast_import_file(
                        destination_folder, fname, msg_id, fsize, fdur, channel_id
                    )
                    IMPORT_PROGRESS[import_id]["imported"] += 1
                except Exception as e:
                    logger.error(f"[Fast] {fname} msg {msg_id}: {e}")
                    IMPORT_PROGRESS[import_id]["errors"] += 1

        await asyncio.gather(*[one(*f) for f in file_list])

    # ── Phase 2b: regular import (copy to storage) ───────────────────────────
    async def _do_regular_import(
        self,
        client: Client,
        file_list: List[Tuple[int, str, int, int]],
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ) -> None:
        sem = asyncio.Semaphore(COPY_WORKERS)

        async def one(msg_id: int, fname: str, fsize: int, fdur: int):
            async with sem:
                if import_id in IMPORT_CANCEL:
                    return
                for attempt in range(3):
                    try:
                        copied = await client.copy_message(
                            chat_id=STORAGE_CHANNEL,
                            from_chat_id=channel_id,
                            message_id=msg_id,
                            disable_notification=True,
                        )
                        cm = _media_from_msg(copied)
                        real_name = (getattr(cm, "file_name", None) or fname) if cm else fname
                        real_size = (getattr(cm, "file_size", fsize) or fsize) if cm else fsize
                        DRIVE_DATA.new_file(
                            destination_folder, real_name, copied.id, real_size, fdur
                        )
                        IMPORT_PROGRESS[import_id]["imported"] += 1
                        return
                    except Exception as e:
                        err = str(e)
                        if "FLOOD_WAIT" in err:
                            # Extract wait seconds if present
                            wait = FLOOD_WAIT_CAP
                            try:
                                wait = min(int(err.split("_")[-1]), FLOOD_WAIT_CAP)
                            except Exception:
                                pass
                            logger.warning(f"Flood wait {wait}s on msg {msg_id}")
                            await asyncio.sleep(wait)
                        elif attempt < 2:
                            await asyncio.sleep(1)
                        else:
                            logger.error(f"[Regular] msg {msg_id} ({fname}) failed: {e}")
                            IMPORT_PROGRESS[import_id]["errors"] += 1

        await asyncio.gather(*[one(*f) for f in file_list])

    # ── Master entry point ────────────────────────────────────────────────────
    async def smart_bulk_import(
        self,
        client: Client,
        channel_identifier: str,
        destination_folder: str,
        start_msg_id: Optional[int] = None,
        end_msg_id: Optional[int] = None,
        import_mode: str = "auto",
        import_id: Optional[str] = None,
    ) -> Tuple[int, int, bool]:
        """
        Run a full bulk import and return (imported, total_media, used_fast).
        Progress is written to IMPORT_PROGRESS[import_id] for polling.
        """
        if import_id is None:
            import_id = secrets.token_hex(8)

        IMPORT_PROGRESS[import_id] = {
            "status":      "validating",
            "imported":    0,
            "skipped":     0,
            "errors":      0,
            "fetched":     0,
            "total_scan":  0,
            "total_media": 0,
            "method":      import_mode,
            "start_time":  time.time(),
            "channel":     channel_identifier,
            "channel_name": channel_identifier,
        }

        # ── Validate channel ──
        is_valid, result, is_admin = await self.validate_channel_access(
            client, channel_identifier
        )
        if not is_valid:
            IMPORT_PROGRESS[import_id].update({"status": "error", "error_msg": result})
            raise Exception(result)

        channel    = result
        channel_id = channel.id
        IMPORT_PROGRESS[import_id]["channel_name"] = getattr(channel, "title", channel_identifier)

        # ── Decide method ──
        if import_mode == "auto":
            use_fast = is_admin
        elif import_mode == "fast":
            if not is_admin:
                msg = "Fast import requires the bot to be admin in the source channel."
                IMPORT_PROGRESS[import_id].update({"status": "error", "error_msg": msg})
                raise Exception(msg)
            use_fast = True
        else:
            use_fast = False

        IMPORT_PROGRESS[import_id]["method"] = "fast" if use_fast else "regular"

        # ── Build message ID list ──
        if start_msg_id and end_msg_id:
            msg_ids = list(range(start_msg_id, end_msg_id + 1))
        else:
            IMPORT_PROGRESS[import_id]["status"] = "scanning"
            msg_ids = []
            async for msg in client.get_chat_history(channel_id):
                if _media_from_msg(msg):
                    msg_ids.append(msg.id)
            msg_ids.reverse()  # oldest first

        IMPORT_PROGRESS[import_id].update({
            "total_scan": len(msg_ids),
            "status":     "fetching",
        })

        # ── Phase 1: fetch file list (batched) ──
        file_list = await self._fetch_file_list(client, channel_id, msg_ids, import_id)

        IMPORT_PROGRESS[import_id].update({
            "total_media": len(file_list),
            "status":      "importing",
        })

        if import_id in IMPORT_CANCEL:
            IMPORT_PROGRESS[import_id]["status"] = "cancelled"
            return 0, len(file_list), use_fast

        # ── Phase 2: import ──
        if use_fast:
            await self._do_fast_import(file_list, channel_id, destination_folder, import_id)
        else:
            await self._do_regular_import(client, file_list, channel_id, destination_folder, import_id)

        imported = IMPORT_PROGRESS[import_id]["imported"]
        IMPORT_PROGRESS[import_id].update({
            "status":  "done" if import_id not in IMPORT_CANCEL else "cancelled",
            "elapsed": round(time.time() - IMPORT_PROGRESS[import_id]["start_time"], 1),
        })
        IMPORT_CANCEL.discard(import_id)

        return imported, len(file_list), use_fast


# ── Global singletons ─────────────────────────────────────────────────────────
SMART_IMPORT_MANAGER = SmartImportManager()
FAST_IMPORT_MANAGER  = SMART_IMPORT_MANAGER   # backwards-compat alias
