"""
fast_import.py — Maximum-speed bulk import via Pyrogram MTProto

THE KEY INSIGHT:
  copy_message()     = get_messages() + re-upload bytes → SLOW (uses bandwidth)
  forward_messages() = messages.ForwardMessages raw API  → INSTANT (server-side)

  forward_messages accepts a LIST of IDs in ONE API call.
  So 1000 files = ~10 API calls instead of 1000 separate uploads.
  With drop_author=True (hide_sender_name) there is no forward tag shown.

Speed strategy:
  1. Batch get_messages: 200 IDs per call to scan (6 calls for 1016 files)
  2. Batch forward_messages: 100 IDs per call to copy (10 calls for 1016 files)
  3. Public channels work without bot being a member
  4. Real-time progress via IMPORT_PROGRESS dict (polled by frontend)
  5. Cancellable via IMPORT_CANCEL set
  6. Automatic FLOOD_WAIT back-off
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

# ── Shared state ──────────────────────────────────────────────────────────────
IMPORT_PROGRESS: Dict[str, Dict[str, Any]] = {}
IMPORT_CANCEL: set = set()

# ── Speed knobs ───────────────────────────────────────────────────────────────
SCAN_BATCH_SIZE    = 200   # IDs per get_messages call (Pyrogram/TG max)
FORWARD_BATCH_SIZE = 100   # IDs per forward_messages call (TG allows up to 100)
FORWARD_WORKERS    = 4     # Parallel forward batches (4 × 100 = 400 files at once)
FLOOD_WAIT_CAP     = 30    # Max seconds to sleep on flood wait


# ── Helper ────────────────────────────────────────────────────────────────────
def _media_from_msg(msg) -> Optional[Any]:
    if not msg or getattr(msg, "empty", True):
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
        Public channels work without the bot being a member.
        """
        try:
            channel = await client.get_chat(channel_identifier)
        except Exception as e:
            err = str(e)
            if any(k in err for k in ("No chat", "No user", "USERNAME_INVALID", "PEER_ID_INVALID")):
                return False, f"Channel '{channel_identifier}' not found. Check username/ID.", False
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
                return False, (
                    f"Bot is not a member of '{getattr(channel, 'title', channel_identifier)}'.\n"
                    "For private channels/groups please add the bot first.\n"
                    "Public channels work without adding the bot."
                ), False

        return True, channel, is_admin

    # ── Phase 1: scan channel for media messages ──────────────────────────────
    async def _scan_media_messages(
        self,
        client: Client,
        channel_id: int,
        msg_ids: List[int],
        import_id: str,
    ) -> List[Tuple[int, str, int, int]]:
        """
        Batch-fetch messages and return list of (msg_id, fname, fsize, fdur)
        for messages that have media. Uses SCAN_BATCH_SIZE per API call.
        """
        results: List[Tuple[int, str, int, int]] = []
        total = len(msg_ids)

        for i in range(0, total, SCAN_BATCH_SIZE):
            if import_id in IMPORT_CANCEL:
                break

            batch = msg_ids[i : i + SCAN_BATCH_SIZE]
            messages = None

            for attempt in range(3):
                try:
                    raw = await client.get_messages(channel_id, batch)
                    messages = raw if isinstance(raw, list) else [raw]
                    break
                except Exception as e:
                    err = str(e)
                    if "FLOOD_WAIT" in err:
                        wait = FLOOD_WAIT_CAP
                        try: wait = min(int(err.split("_")[-1]), FLOOD_WAIT_CAP)
                        except Exception: pass
                        await asyncio.sleep(wait)
                    elif attempt < 2:
                        await asyncio.sleep(2)
                    else:
                        logger.warning(f"Scan batch {batch[0]}-{batch[-1]} failed: {e}")
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

            IMPORT_PROGRESS[import_id]["fetched"]    = i + len(batch)
            IMPORT_PROGRESS[import_id]["total_scan"] = total
            await asyncio.sleep(0)  # yield to event loop

        return results

    # ── Phase 2: fast import (direct reference, no copy) ─────────────────────
    async def _do_fast_import(
        self,
        file_list: List[Tuple[int, str, int, int]],
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ) -> None:
        """Register files pointing to source channel — zero network transfer."""
        for msg_id, fname, fsize, fdur in file_list:
            if import_id in IMPORT_CANCEL:
                break
            try:
                DRIVE_DATA.new_fast_import_file(
                    destination_folder, fname, msg_id, fsize, fdur, channel_id
                )
                IMPORT_PROGRESS[import_id]["imported"] += 1
            except Exception as e:
                logger.error(f"[Fast] {fname}: {e}")
                IMPORT_PROGRESS[import_id]["errors"] += 1

    # ── Phase 2: regular import using bulk forward_messages ───────────────────
    async def _do_bulk_forward_import(
        self,
        client: Client,
        file_list: List[Tuple[int, str, int, int]],
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ) -> None:
        """
        Use forward_messages(hide_sender_name=True) with batches of 100 IDs.
        This is a SINGLE raw API call per batch — pure server-side copy.
        No file bytes are downloaded or uploaded. This is why it's instant.
        """
        # Build a lookup: msg_id → (fname, fsize, fdur)
        meta = {row[0]: row[1:] for row in file_list}
        all_ids = [row[0] for row in file_list]
        total = len(all_ids)

        sem = asyncio.Semaphore(FORWARD_WORKERS)

        async def forward_one_batch(batch_ids: List[int]):
            if import_id in IMPORT_CANCEL:
                return

            async with sem:
                for attempt in range(4):
                    try:
                        # ONE API call forwards all IDs in batch — server-side, instant
                        forwarded = await client.forward_messages(
                            chat_id=STORAGE_CHANNEL,
                            from_chat_id=channel_id,
                            message_ids=batch_ids,
                            hide_sender_name=True,    # No "forwarded from" tag
                            disable_notification=True,
                        )

                        if not isinstance(forwarded, list):
                            forwarded = [forwarded] if forwarded else []

                        for fwd_msg in forwarded:
                            if not fwd_msg:
                                continue
                            fwd_media = _media_from_msg(fwd_msg)

                            # Match back to original metadata by position
                            # forward_messages preserves order
                            orig_id = None
                            # Try to find via forward_origin or by order
                            fwd_origin = getattr(fwd_msg, "forward_origin", None)
                            if fwd_origin:
                                orig_id = getattr(fwd_origin, "message_id", None)

                            if orig_id and orig_id in meta:
                                fname, fsize, fdur = meta[orig_id]
                            else:
                                # Fallback: use forwarded message's own media info
                                fname = (getattr(fwd_media, "file_name", None) or f"file_{fwd_msg.id}") if fwd_media else f"file_{fwd_msg.id}"
                                fsize = (getattr(fwd_media, "file_size", 0) or 0) if fwd_media else 0
                                fdur  = (getattr(fwd_media, "duration", 0) if fwd_media and hasattr(fwd_media, "duration") else 0)

                            DRIVE_DATA.new_file(
                                destination_folder, fname, fwd_msg.id, fsize, fdur
                            )
                            IMPORT_PROGRESS[import_id]["imported"] += 1

                        return  # success

                    except Exception as e:
                        err = str(e)
                        if "FLOOD_WAIT" in err:
                            wait = FLOOD_WAIT_CAP
                            try: wait = min(int(err.split("_")[-1]), FLOOD_WAIT_CAP)
                            except Exception: pass
                            logger.warning(f"Flood wait {wait}s on forward batch")
                            await asyncio.sleep(wait)
                        elif attempt < 3:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            logger.error(f"Forward batch {batch_ids[0]}-{batch_ids[-1]} failed: {e}")
                            IMPORT_PROGRESS[import_id]["errors"] += len(batch_ids)

        # Split into batches of FORWARD_BATCH_SIZE and run with concurrency
        batches = [
            all_ids[i : i + FORWARD_BATCH_SIZE]
            for i in range(0, total, FORWARD_BATCH_SIZE)
        ]

        await asyncio.gather(*[forward_one_batch(b) for b in batches])

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
        Run a full bulk import. Returns (imported, total_media, used_fast).
        Progress written to IMPORT_PROGRESS[import_id] for frontend polling.
        """
        if import_id is None:
            import_id = secrets.token_hex(8)

        IMPORT_PROGRESS[import_id] = {
            "status":       "validating",
            "imported":     0,
            "skipped":      0,
            "errors":       0,
            "fetched":      0,
            "total_scan":   0,
            "total_media":  0,
            "method":       import_mode,
            "start_time":   time.time(),
            "channel":      channel_identifier,
            "channel_name": channel_identifier,
        }

        # ── Validate ──
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
                msg = "Fast import requires bot admin in source channel."
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
            msg_ids.reverse()

        IMPORT_PROGRESS[import_id].update({
            "total_scan": len(msg_ids),
            "status":     "fetching",
        })

        # ── Phase 1: scan ──
        file_list = await self._scan_media_messages(client, channel_id, msg_ids, import_id)

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
            await self._do_bulk_forward_import(client, file_list, channel_id, destination_folder, import_id)

        imported = IMPORT_PROGRESS[import_id]["imported"]
        IMPORT_PROGRESS[import_id].update({
            "status":  "done" if import_id not in IMPORT_CANCEL else "cancelled",
            "elapsed": round(time.time() - IMPORT_PROGRESS[import_id]["start_time"], 1),
        })
        IMPORT_CANCEL.discard(import_id)

        return imported, len(file_list), use_fast


# ── Global singletons ─────────────────────────────────────────────────────────
SMART_IMPORT_MANAGER = SmartImportManager()
FAST_IMPORT_MANAGER  = SMART_IMPORT_MANAGER  # backwards-compat alias
