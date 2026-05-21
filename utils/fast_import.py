"""
fast_import.py — High-speed bulk/fast import via Pyrogram MTProto

Key improvements over the old version:
  1. Public channels never need the bot to be a member
  2. Batch get_messages (up to 200 IDs per API call) instead of one-by-one
  3. Concurrent copy_message workers (configurable concurrency)
  4. Real-time progress tracking via IMPORT_PROGRESS dict
  5. Cancellable tasks via IMPORT_CANCEL set
"""

import asyncio
import time
from typing import Optional, Dict, Any
from pyrogram import Client
from pyrogram.types import Message
from utils.logger import Logger
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL

logger = Logger(__name__)

# ── Progress / cancel state (keyed by import_id) ─────────────────────────────
IMPORT_PROGRESS: Dict[str, Dict[str, Any]] = {}
IMPORT_CANCEL: set = set()

# Tuning knobs
BATCH_SIZE = 200          # Pyrogram supports up to 200 IDs per get_messages call
COPY_CONCURRENCY = 8      # Parallel copy_message workers for regular import
FAST_CONCURRENCY = 20     # Fast import (no network copy) can run much higher

# ─────────────────────────────────────────────────────────────────────────────

class SmartImportManager:
    def __init__(self):
        pass

    # ── Channel access check ──────────────────────────────────────────────────
    async def validate_channel_access(self, client: Client, channel_identifier: str):
        """
        Check whether the bot can read the channel.

        For *public* channels (username or public link) the bot can call
        get_messages without being a member.  We only require membership for
        private channels/groups.

        Returns: (is_valid: bool, channel_or_error_msg, is_admin: bool)
        """
        try:
            channel = await client.get_chat(channel_identifier)
        except Exception as e:
            err = str(e)
            if "No chat" in err or "No user" in err or "USERNAME_INVALID" in err:
                return False, f"Channel '{channel_identifier}' not found. Check the username or ID.", False
            return False, f"Cannot access channel: {err}", False

        # Public channels — bot can always read them without being a member
        is_public = bool(getattr(channel, "username", None))

        if is_public:
            # Still try to figure out admin status (for fast-import decision)
            is_admin = False
            try:
                me = await client.get_chat_member(channel.id, "me")
                is_admin = bool(me.privileges and (
                    me.privileges.can_delete_messages or
                    me.privileges.can_edit_messages or
                    me.privileges.can_post_messages
                ))
            except Exception:
                # Not a member — that's fine for public channels
                is_admin = False
            return True, channel, is_admin

        # Private channel / group — bot must be a member
        try:
            me = await client.get_chat_member(channel.id, "me")
            is_admin = bool(me.privileges and (
                me.privileges.can_delete_messages or
                me.privileges.can_edit_messages or
                me.privileges.can_post_messages
            ))
            return True, channel, is_admin
        except Exception as e:
            err = str(e)
            if "USER_NOT_PARTICIPANT" in err or "not a member" in err.lower():
                return False, (
                    f"Bot is not a member of '{channel.title}'. "
                    f"For private channels/groups please add the bot first. "
                    f"Public channels work without the bot being a member."
                ), False
            return False, f"Cannot verify membership: {err}", False

    # ── Batch message fetching ────────────────────────────────────────────────
    async def _fetch_messages_batched(
        self,
        client: Client,
        channel_id: int,
        msg_ids: list,
        import_id: str,
    ):
        """
        Fetch messages in batches of BATCH_SIZE using a single API call per batch.
        Returns list of (message_id, media_object, file_name, file_size, duration).
        """
        results = []
        total = len(msg_ids)
        fetched = 0

        for i in range(0, total, BATCH_SIZE):
            if import_id in IMPORT_CANCEL:
                break

            batch = msg_ids[i : i + BATCH_SIZE]
            try:
                messages = await client.get_messages(channel_id, batch)
                if not isinstance(messages, list):
                    messages = [messages]
            except Exception as e:
                logger.warning(f"Batch fetch error (ids {batch[0]}-{batch[-1]}): {e}")
                # Retry once with a short delay
                await asyncio.sleep(1)
                try:
                    messages = await client.get_messages(channel_id, batch)
                    if not isinstance(messages, list):
                        messages = [messages]
                except Exception as e2:
                    logger.error(f"Batch fetch failed on retry: {e2}")
                    fetched += len(batch)
                    IMPORT_PROGRESS[import_id]["skipped"] += len(batch)
                    continue

            for msg in messages:
                if not msg or msg.empty:
                    IMPORT_PROGRESS[import_id]["skipped"] += 1
                    fetched += 1
                    continue

                media = (
                    msg.document or msg.video or msg.audio
                    or msg.photo or msg.sticker
                )
                if not media:
                    IMPORT_PROGRESS[import_id]["skipped"] += 1
                    fetched += 1
                    continue

                fname = getattr(media, "file_name", None) or f"file_{msg.id}"
                fsize = getattr(media, "file_size", 0) or 0
                fdur  = getattr(media, "duration", 0) if hasattr(media, "duration") else 0
                results.append((msg.id, media, fname, fsize, fdur))
                fetched += 1

            IMPORT_PROGRESS[import_id]["fetched"] = fetched
            IMPORT_PROGRESS[import_id]["total_scan"] = total

            # Small yield so other coroutines run
            await asyncio.sleep(0)

        return results

    # ── Fast import (no copy) ─────────────────────────────────────────────────
    async def _fast_import_batch(
        self,
        file_list,
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ):
        """Register files as fast-import entries (no network copy needed)."""
        sem = asyncio.Semaphore(FAST_CONCURRENCY)

        async def do_one(msg_id, media, fname, fsize, fdur):
            async with sem:
                if import_id in IMPORT_CANCEL:
                    return
                try:
                    DRIVE_DATA.new_fast_import_file(
                        destination_folder, fname, msg_id, fsize, fdur, channel_id
                    )
                    IMPORT_PROGRESS[import_id]["imported"] += 1
                    logger.info(f"[Fast] Registered {fname} (msg {msg_id})")
                except Exception as e:
                    logger.error(f"[Fast] Failed {fname}: {e}")
                    IMPORT_PROGRESS[import_id]["errors"] += 1

        await asyncio.gather(*[do_one(*f) for f in file_list])

    # ── Regular import (copy to storage channel) ──────────────────────────────
    async def _regular_import_batch(
        self,
        client: Client,
        file_list,
        channel_id: int,
        destination_folder: str,
        import_id: str,
    ):
        """Copy messages to STORAGE_CHANNEL concurrently."""
        sem = asyncio.Semaphore(COPY_CONCURRENCY)

        async def do_one(msg_id, media, fname, fsize, fdur):
            async with sem:
                if import_id in IMPORT_CANCEL:
                    return
                try:
                    copied = await client.copy_message(
                        chat_id=STORAGE_CHANNEL,
                        from_chat_id=channel_id,
                        message_id=msg_id,
                        disable_notification=True,
                    )
                    copied_media = (
                        copied.document or copied.video or copied.audio
                        or copied.photo or copied.sticker
                    )
                    real_fname = (
                        getattr(copied_media, "file_name", None) or fname
                    )
                    real_size = getattr(copied_media, "file_size", fsize) or fsize
                    DRIVE_DATA.new_file(
                        destination_folder, real_fname, copied.id, real_size, fdur
                    )
                    IMPORT_PROGRESS[import_id]["imported"] += 1
                    logger.info(f"[Regular] Copied {real_fname} → msg {copied.id}")
                except Exception as e:
                    logger.error(f"[Regular] Failed msg {msg_id} ({fname}): {e}")
                    IMPORT_PROGRESS[import_id]["errors"] += 1
                    # Back-off on flood wait
                    if "FLOOD_WAIT" in str(e):
                        wait = int(str(e).split("_")[-1]) if str(e).split("_")[-1].isdigit() else 5
                        logger.warning(f"Flood wait {wait}s")
                        await asyncio.sleep(min(wait, 30))

        await asyncio.gather(*[do_one(*f) for f in file_list])

    # ── Master entry point ────────────────────────────────────────────────────
    async def smart_bulk_import(
        self,
        client: Client,
        channel_identifier: str,
        destination_folder: str,
        start_msg_id: Optional[int] = None,
        end_msg_id: Optional[int] = None,
        import_mode: str = "auto",
        import_id: str = None,
    ):
        """
        High-speed bulk import.
        Returns (imported_count, total_media_found, used_fast_import).
        """
        if import_id is None:
            import import secrets
            import_id = secrets.token_hex(8)

        # Init progress
        IMPORT_PROGRESS[import_id] = {
            "status": "validating",
            "imported": 0,
            "skipped": 0,
            "errors": 0,
            "fetched": 0,
            "total_scan": 0,
            "total_media": 0,
            "method": import_mode,
            "start_time": time.time(),
            "channel": channel_identifier,
        }

        # Validate channel
        is_valid, result, is_admin = await self.validate_channel_access(
            client, channel_identifier
        )
        if not is_valid:
            IMPORT_PROGRESS[import_id]["status"] = "error"
            IMPORT_PROGRESS[import_id]["error_msg"] = result
            raise Exception(result)

        channel = result
        channel_id = channel.id

        # Determine import method
        if import_mode == "auto":
            use_fast = is_admin
        elif import_mode == "fast":
            if not is_admin:
                IMPORT_PROGRESS[import_id]["status"] = "error"
                IMPORT_PROGRESS[import_id]["error_msg"] = "Fast import requires bot admin in source channel"
                raise Exception("Fast import requires bot admin in source channel")
            use_fast = True
        else:  # regular
            use_fast = False

        IMPORT_PROGRESS[import_id]["method"] = "fast" if use_fast else "regular"
        IMPORT_PROGRESS[import_id]["channel_name"] = getattr(channel, "title", channel_identifier)

        # Build message ID list
        if start_msg_id and end_msg_id:
            msg_ids = list(range(start_msg_id, end_msg_id + 1))
        else:
            # No range provided — fetch all history (may be slow for huge channels)
            IMPORT_PROGRESS[import_id]["status"] = "scanning"
            msg_ids = []
            async for msg in client.get_chat_history(channel_id):
                media = (
                    msg.document or msg.video or msg.audio
                    or msg.photo or msg.sticker
                )
                if media:
                    msg_ids.append(msg.id)
            msg_ids.reverse()  # oldest first

        IMPORT_PROGRESS[import_id]["total_scan"] = len(msg_ids)
        IMPORT_PROGRESS[import_id]["status"] = "fetching"

        # Batch fetch messages
        file_list = await self._fetch_messages_batched(
            client, channel_id, msg_ids, import_id
        )

        IMPORT_PROGRESS[import_id]["total_media"] = len(file_list)
        IMPORT_PROGRESS[import_id]["status"] = "importing"

        # Import
        if use_fast:
            await self._fast_import_batch(file_list, channel_id, destination_folder, import_id)
        else:
            await self._regular_import_batch(client, file_list, channel_id, destination_folder, import_id)

        # Final
        imported = IMPORT_PROGRESS[import_id]["imported"]
        IMPORT_PROGRESS[import_id]["status"] = "done" if import_id not in IMPORT_CANCEL else "cancelled"
        IMPORT_PROGRESS[import_id]["elapsed"] = round(time.time() - IMPORT_PROGRESS[import_id]["start_time"], 1)

        IMPORT_CANCEL.discard(import_id)
        return imported, len(file_list), use_fast


# Global instance
SMART_IMPORT_MANAGER = SmartImportManager()

# Backwards compat alias
FAST_IMPORT_MANAGER = SMART_IMPORT_MANAGER
