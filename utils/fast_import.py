"""
fast_import.py — Maximum-speed bulk import via Pyrogram MTProto

THE KEY INSIGHT:
  copy_message()     = re-uploads file bytes           → SLOW (~105 min for 1000 files)
  forward_messages() = server-side copy, ONE API call  → FAST  (~20-30s for 1000 files)

  forward_messages accepts a LIST of up to 100 IDs in ONE raw MTProto call.
  Telegram processes this server-side with no bandwidth usage.

FLOOD WAIT STRATEGY:
  Telegram allows ~1 forward batch per 2 seconds per bot.
  Running them in parallel just causes flood waits + retries = same net speed.
  Sequential batches with a 2.5s delay between each is optimal:
    100 files / 2.5s × total_batches = ~40 files/sec
    1016 files ÷ 100 per batch = 11 batches × 2.5s = ~28 seconds total
"""

import asyncio
import secrets
import time
from typing import Optional, Dict, Any, List, Tuple

from pyrogram import Client
from utils.logger import Logger
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL

logger = Logger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────
IMPORT_PROGRESS: Dict[str, Dict[str, Any]] = {}
IMPORT_CANCEL:   set = set()

# ── Tuning ────────────────────────────────────────────────────────────────────
SCAN_BATCH_SIZE   = 200   # IDs per get_messages call (TG/Pyrogram max)
FORWARD_BATCH_SIZE = 100  # IDs per forward_messages call (TG max = 100)
INTER_BATCH_DELAY = 2.5   # Seconds between forward batches (avoids flood wait)
FLOOD_WAIT_CAP    = 35    # Max seconds to sleep on a flood wait error


def _media(msg) -> Optional[Any]:
    if not msg or getattr(msg, "empty", True):
        return None
    return msg.document or msg.video or msg.audio or msg.photo or msg.sticker


def _parse_flood_wait(err_str: str) -> int:
    """Extract wait seconds from FLOOD_WAIT error string."""
    try:
        return min(int(err_str.split("_")[-1]), FLOOD_WAIT_CAP)
    except Exception:
        return FLOOD_WAIT_CAP


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
                return False, f"Channel '{channel_identifier}' not found. Check the username or ID.", False
            return False, f"Cannot access channel: {err}", False

        is_public = bool(getattr(channel, "username", None))
        is_admin  = False

        try:
            me   = await client.get_chat_member(channel.id, "me")
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
                return False, (
                    f"Bot is not a member of '{title}'.\n"
                    "For private channels/groups please add the bot first.\n"
                    "Public channels work without adding the bot."
                ), False

        return True, channel, is_admin

    # ── Phase 1: scan for media (batched get_messages) ────────────────────────
    async def _scan_media(
        self,
        client: Client,
        channel_id: int,
        msg_ids: List[int],
        import_id: str,
    ) -> List[Tuple[int, str, int, int]]:
        """
        Batch-fetch messages in groups of SCAN_BATCH_SIZE.
        Returns list of (msg_id, fname, fsize, fdur) for media messages only.
        """
        results: List[Tuple[int, str, int, int]] = []
        total = len(msg_ids)

        for i in range(0, total, SCAN_BATCH_SIZE):
            if import_id in IMPORT_CANCEL:
                break
            batch = msg_ids[i : i + SCAN_BATCH_SIZE]
            msgs  = None

            for attempt in range(3):
                try:
                    raw  = await client.get_messages(channel_id, batch)
                    msgs = raw if isinstance(raw, list) else [raw]
                    break
                except Exception as e:
                    err = str(e)
                    wait = _parse_flood_wait(err) if "FLOOD_WAIT" in err else 2
                    if attempt < 2:
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(f"Scan batch {batch[0]}-{batch[-1]} failed: {e}")
                        IMPORT_PROGRESS[import_id]["skipped"] += len(batch)

            if msgs is None:
                continue

            for msg in msgs:
                m = _media(msg)
                if m is None:
                    IMPORT_PROGRESS[import_id]["skipped"] += 1
                    continue
                fname = getattr(m, "file_name", None) or f"file_{msg.id}"
                fsize = getattr(m, "file_size", 0) or 0
                fdur  = getattr(m, "duration", 0) if hasattr(m, "duration") else 0
                results.append((msg.id, fname, fsize, fdur))

            IMPORT_PROGRESS[import_id].update({
                "fetched":    i + len(batch),
                "total_scan": total,
            })
            await asyncio.sleep(0)  # yield to event loop

        return results

    # ── Phase 2a: fast import (direct reference, no copy needed) ─────────────
    async def _do_fast_import(
        self,
        file_list: List[Tuple[int, str, int, int]],
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ) -> None:
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

    # ── Phase 2b: regular import using forward_messages (server-side) ─────────
    async def _do_bulk_forward(
        self,
        client: Client,
        file_list: List[Tuple[int, str, int, int]],
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ) -> None:
        """
        Uses forward_messages() with up to 100 IDs per call.
        This is a single raw MTProto call — Telegram copies server-side.
        No bytes are downloaded or uploaded. Files appear instantly.

        We run batches SEQUENTIALLY with INTER_BATCH_DELAY between each.
        This avoids flood waits from parallel requests, which just cause
        retries and achieve the same or worse net throughput.
        """
        meta    = {row[0]: row[1:] for row in file_list}
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
                        chat_id=STORAGE_CHANNEL,
                        from_chat_id=channel_id,
                        message_ids=batch_ids,
                        hide_sender_name=True,
                        disable_notification=True,
                    )
                    if not isinstance(forwarded, list):
                        forwarded = [forwarded] if forwarded else []

                    for fwd in forwarded:
                        if not fwd:
                            continue
                        fm = _media(fwd)
                        if not fm:
                            continue  # message had no media, skipped by TG

                        fname = getattr(fm, "file_name", None) or f"file_{fwd.id}"
                        fsize = getattr(fm, "file_size", 0) or 0
                        fdur  = getattr(fm, "duration", 0) if hasattr(fm, "duration") else 0

                        # register_file: add to memory only, no save() per file
                        DRIVE_DATA.register_file(destination_folder, fname, fwd.id, fsize, fdur)
                        IMPORT_PROGRESS[import_id]["imported"] += 1

                    # ONE save for the entire batch — not one per file
                    DRIVE_DATA.save()

                    logger.info(
                        f"[Forward] Batch {batch_num+1}/{total_batches} "
                        f"({len(batch_ids)} files) done. "
                        f"Total: {IMPORT_PROGRESS[import_id]['imported']}"
                    )
                    break  # success — move to next batch

                except Exception as e:
                    err = str(e)
                    if "FLOOD_WAIT" in err:
                        wait = _parse_flood_wait(err)
                        logger.warning(f"[Forward] Flood wait {wait}s on batch {batch_num+1}")
                        await asyncio.sleep(wait)
                    elif attempt < 4:
                        await asyncio.sleep(2 ** attempt)  # 1, 2, 4, 8s
                    else:
                        logger.error(f"[Forward] Batch {batch_num+1} permanently failed: {e}")
                        IMPORT_PROGRESS[import_id]["errors"] += len(batch_ids)

            # Polite delay between batches — prevents flood wait on next call
            if batch_num < total_batches - 1 and import_id not in IMPORT_CANCEL:
                await asyncio.sleep(INTER_BATCH_DELAY)

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
        Writes live progress to IMPORT_PROGRESS[import_id] for frontend polling.
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

        # Validate
        is_valid, result, is_admin = await self.validate_channel_access(client, channel_identifier)
        if not is_valid:
            IMPORT_PROGRESS[import_id].update({"status": "error", "error_msg": result})
            raise Exception(result)

        channel    = result
        channel_id = channel.id
        IMPORT_PROGRESS[import_id]["channel_name"] = getattr(channel, "title", channel_identifier)

        # Decide method
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

        # Build message ID list
        if start_msg_id and end_msg_id:
            # Range provided: skip scan entirely — forward_messages skips non-media silently
            all_ids = list(range(start_msg_id, end_msg_id + 1))
            IMPORT_PROGRESS[import_id].update({
                "total_scan":  len(all_ids),
                "fetched":     len(all_ids),
                "status":      "importing",
            })
            # For regular import, use IDs directly as file_list placeholders
            # (fname/fsize/fdur will be extracted from forwarded messages)
            file_list = [(mid, f"file_{mid}", 0, 0) for mid in all_ids]
        else:
            # No range: must scan history to find media message IDs
            IMPORT_PROGRESS[import_id]["status"] = "scanning"
            msg_ids = []
            async for msg in client.get_chat_history(channel_id):
                if _media(msg):
                    msg_ids.append(msg.id)
            msg_ids.reverse()

            IMPORT_PROGRESS[import_id].update({
                "total_scan": len(msg_ids),
                "status":     "fetching",
            })
            file_list = await self._scan_media(client, channel_id, msg_ids, import_id)

        IMPORT_PROGRESS[import_id].update({
            "total_media": len(file_list),
            "status":      "importing",
        })

        if import_id in IMPORT_CANCEL:
            IMPORT_PROGRESS[import_id]["status"] = "cancelled"
            return 0, len(file_list), use_fast

        # Phase 2: import
        if use_fast:
            await self._do_fast_import(file_list, channel_id, destination_folder, import_id)
        else:
            await self._do_bulk_forward(client, file_list, channel_id, destination_folder, import_id)

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
