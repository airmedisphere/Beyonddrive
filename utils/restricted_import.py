"""
restricted_import.py — Background importer for RESTRICTED Telegram content.

Used when:
  • Source channel has "protected content" enabled
  • Bot is not allowed to forward/copy
  • Only a logged-in USER (member of the channel) can read media

Flow:
  1. Use a premium/user client (must already be a member of source chat)
  2. Download media bytes to RAM (for small files) or disk (for large)
  3. Upload to STORAGE_CHANNEL via bot client
  4. Register the new message in DRIVE_DATA so it appears in the drive

Progress is tracked in RESTRICTED_PROGRESS — same polling pattern as fast_import.
"""

import asyncio
import io
import os
import re
import secrets
import time
from typing import Optional, Dict, Any, List, Tuple

from pyrogram import Client
from pyrogram.errors import FloodWait, FileReferenceExpired
from pyrogram.utils import get_channel_id

from utils.logger import Logger
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL

logger = Logger(__name__)

# ── Shared state (polled by frontend) ─────────────────────────────────────────
RESTRICTED_PROGRESS: Dict[str, Dict[str, Any]] = {}
RESTRICTED_CANCEL:   set = set()

# ── Tuning ────────────────────────────────────────────────────────────────────
INMEM_THRESHOLD = 200 * 1024 * 1024   # 200 MB → use BytesIO; larger → disk
PER_FILE_DELAY  = 1.5                  # sec between files
FLOOD_WAIT_CAP  = 60


# ── Link parsing (handles topic links, public, private, ranges) ───────────────
def _parse_link(link: str):
    """
    Returns (chat_id, message_id, topic_id) for any Telegram post link:
      https://t.me/username/123
      https://t.me/username/5/123       (topic)
      https://t.me/c/1234567890/123
      https://t.me/c/1234567890/5/123   (private topic)
    """
    link = link.strip().rstrip("/")
    if "?" in link:
        link = link.split("?")[0]
    parts = link.split("/")
    path = parts[3:]  # everything after t.me/

    if not path:
        raise ValueError(f"Invalid link: {link}")

    if path[0] == "c":
        if len(path) == 4:
            return get_channel_id(int(path[1])), int(path[3]), int(path[2])
        elif len(path) == 3:
            return get_channel_id(int(path[1])), int(path[2]), None
        raise ValueError(f"Invalid private link: {link}")
    else:
        if len(path) == 3:
            return path[0], int(path[2]), int(path[1])
        elif len(path) == 2:
            return path[0], int(path[1]), None
        raise ValueError(f"Invalid public link: {link}")


def parse_input_lines(text: str) -> List[Tuple[Any, int, int, Optional[int]]]:
    """
    Parses a multi-line text input. Each line is either:
      • a single link
      • a range: link1 - link2
    Returns list of (chat_id, start_id, end_id, topic_id) tuples.
    """
    jobs = []
    for raw_line in text.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if " - " in line:
            a, b = [x.strip() for x in line.split(" - ", 1)]
            chat_a, id_a, top_a = _parse_link(a)
            chat_b, id_b, top_b = _parse_link(b)
            if str(chat_a) != str(chat_b):
                raise ValueError(f"Range mismatch: {a} and {b} are different chats")
            if top_a != top_b:
                raise ValueError(f"Range mismatch: {a} and {b} are different topics")
            if id_a > id_b:
                raise ValueError(f"Start ID > end ID: {line}")
            jobs.append((chat_a, id_a, id_b, top_a))
        else:
            chat, mid, top = _parse_link(line)
            jobs.append((chat, mid, mid, top))
    return jobs


# ── Filename sanitization (path traversal guard) ─────────────────────────────
def _sanitize_filename(name: str, fallback: str) -> str:
    if not name:
        return fallback
    name = os.path.basename(name.replace("\\", "/"))
    name = "".join(c for c in name if c.isprintable() and c not in "\x00\r\n")
    name = name.lstrip(".")[:200]
    return name or fallback


def _get_media_and_filename(msg, msg_id: int) -> Tuple[Any, str, int, int]:
    """Returns (media_obj, filename, file_size, duration)."""
    if msg.document:
        m = msg.document
        return m, _sanitize_filename(m.file_name or "", f"{msg_id}"), m.file_size or 0, 0
    if msg.video:
        m = msg.video
        return m, _sanitize_filename(m.file_name or "", f"{msg_id}.mp4"), m.file_size or 0, m.duration or 0
    if msg.audio:
        m = msg.audio
        return m, _sanitize_filename(m.file_name or "", f"{msg_id}.mp3"), m.file_size or 0, m.duration or 0
    if msg.photo:
        return msg.photo, f"{msg_id}.jpg", msg.photo.file_size or 0, 0
    if msg.voice:
        return msg.voice, f"{msg_id}.ogg", msg.voice.file_size or 0, msg.voice.duration or 0
    if msg.video_note:
        return msg.video_note, f"{msg_id}.mp4", msg.video_note.file_size or 0, msg.video_note.duration or 0
    if msg.animation:
        m = msg.animation
        return m, _sanitize_filename(m.file_name or "", f"{msg_id}.gif"), m.file_size or 0, 0
    if msg.sticker:
        return msg.sticker, f"{msg_id}.webp", msg.sticker.file_size or 0, 0
    return None, str(msg_id), 0, 0


# ── Main worker ───────────────────────────────────────────────────────────────
class RestrictedImportManager:

    async def _download_and_upload(
        self,
        user_client: Client,
        bot_client: Client,
        msg,
        msg_id: int,
        destination_folder: str,
        import_id: str,
    ) -> bool:
        """Download via user client, upload via bot client to STORAGE_CHANNEL."""
        media_obj, fname, fsize, fdur = _get_media_and_filename(msg, msg_id)
        if not media_obj:
            return False

        # Update current-file label
        RESTRICTED_PROGRESS[import_id]["current_file"] = fname

        use_inmem = fsize > 0 and fsize <= INMEM_THRESHOLD
        media_path = None
        media_buf = None

        try:
            # Download (with retries on FloodWait / FileRef expiry)
            for attempt in range(3):
                try:
                    if use_inmem:
                        media_buf = await msg.download(in_memory=True)
                    else:
                        os.makedirs("downloads", exist_ok=True)
                        media_path = await msg.download(file_name=f"downloads/{fname}")
                    break
                except FloodWait as e:
                    wait = min(int(getattr(e, "value", 30)), FLOOD_WAIT_CAP)
                    logger.warning(f"[Restricted] FloodWait {wait}s on download msg {msg_id}")
                    await asyncio.sleep(wait + 1)
                except FileReferenceExpired:
                    # Re-fetch the message
                    msg = await user_client.get_messages(msg.chat.id, msg_id)
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2)

            if not media_buf and not (media_path and os.path.exists(media_path)):
                return False

            source = media_buf if use_inmem else media_path
            if use_inmem:
                source.name = fname
                source.seek(0)

            # Decide which send method
            send_kwargs = {"chat_id": STORAGE_CHANNEL, "disable_notification": True}
            for attempt in range(3):
                try:
                    if msg.video:
                        sent = await bot_client.send_video(
                            video=source, supports_streaming=True,
                            duration=fdur or 0, **send_kwargs,
                        )
                    elif msg.audio:
                        sent = await bot_client.send_audio(audio=source, **send_kwargs)
                    elif msg.photo:
                        sent = await bot_client.send_photo(photo=source, **send_kwargs)
                    else:
                        sent = await bot_client.send_document(document=source, **send_kwargs)
                    break
                except FloodWait as e:
                    wait = min(int(getattr(e, "value", 30)), FLOOD_WAIT_CAP)
                    logger.warning(f"[Restricted] FloodWait {wait}s on upload msg {msg_id}")
                    await asyncio.sleep(wait + 1)
                    if use_inmem:
                        source.seek(0)
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2)
                    if use_inmem:
                        source.seek(0)
            else:
                return False

            # Register in DRIVE_DATA — same pattern as fast_import
            DRIVE_DATA.register_file(destination_folder, fname, sent.id, fsize, fdur)
            return True

        except Exception as e:
            logger.error(f"[Restricted] msg {msg_id} failed: {e}")
            return False
        finally:
            if media_path and os.path.exists(media_path):
                try:
                    os.remove(media_path)
                except Exception:
                    pass
            if media_buf:
                try:
                    media_buf.close()
                except Exception:
                    pass

    async def run(
        self,
        user_client: Client,
        bot_client: Client,
        jobs: List[Tuple[Any, int, int, Optional[int]]],
        destination_folder: str,
        import_id: str,
    ) -> None:
        """Run the full restricted-content import job."""
        # Initialize progress record
        total = sum(end - start + 1 for _, start, end, _ in jobs)
        RESTRICTED_PROGRESS[import_id] = {
            "status":        "starting",
            "method":        "restricted",
            "imported":      0,
            "skipped":       0,
            "errors":        0,
            "total":         total,
            "current_file":  "",
            "current_job":   0,
            "total_jobs":    len(jobs),
            "start_time":    time.time(),
        }

        try:
            for job_idx, (chat_id, start_id, end_id, topic_id) in enumerate(jobs):
                RESTRICTED_PROGRESS[import_id]["current_job"] = job_idx + 1
                RESTRICTED_PROGRESS[import_id]["status"] = "importing"

                # Process in chunks of 200 (max get_messages batch size)
                msg_id_current = start_id
                while msg_id_current <= end_id:
                    if import_id in RESTRICTED_CANCEL:
                        break

                    chunk_end = min(msg_id_current + 199, end_id)
                    chunk_ids = list(range(msg_id_current, chunk_end + 1))

                    try:
                        msgs = await user_client.get_messages(chat_id, chunk_ids)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                    except Exception as e:
                        logger.error(f"[Restricted] get_messages failed: {e}")
                        RESTRICTED_PROGRESS[import_id]["errors"] += len(chunk_ids)
                        msg_id_current = chunk_end + 1
                        continue

                    for msg in msgs:
                        if import_id in RESTRICTED_CANCEL:
                            break
                        if not msg or getattr(msg, "empty", True):
                            RESTRICTED_PROGRESS[import_id]["skipped"] += 1
                            continue

                        media_obj, _, _, _ = _get_media_and_filename(msg, msg.id)
                        if not media_obj:
                            RESTRICTED_PROGRESS[import_id]["skipped"] += 1
                            continue

                        ok = await self._download_and_upload(
                            user_client, bot_client, msg, msg.id,
                            destination_folder, import_id,
                        )
                        if ok:
                            RESTRICTED_PROGRESS[import_id]["imported"] += 1
                        else:
                            RESTRICTED_PROGRESS[import_id]["errors"] += 1

                        # Save drive every 10 files so progress is durable
                        if (RESTRICTED_PROGRESS[import_id]["imported"] % 10) == 0:
                            try:
                                DRIVE_DATA.save()
                            except Exception:
                                pass

                        await asyncio.sleep(PER_FILE_DELAY)

                    msg_id_current = chunk_end + 1

                if import_id in RESTRICTED_CANCEL:
                    break

            # Final save
            try:
                DRIVE_DATA.save()
            except Exception:
                pass

            RESTRICTED_PROGRESS[import_id]["status"] = (
                "cancelled" if import_id in RESTRICTED_CANCEL else "done"
            )
            RESTRICTED_PROGRESS[import_id]["elapsed"] = round(
                time.time() - RESTRICTED_PROGRESS[import_id]["start_time"], 1
            )

        except Exception as e:
            logger.error(f"[Restricted] Fatal error: {e}")
            RESTRICTED_PROGRESS[import_id]["status"] = "error"
            RESTRICTED_PROGRESS[import_id]["error_msg"] = str(e)
        finally:
            RESTRICTED_CANCEL.discard(import_id)


RESTRICTED_IMPORT_MANAGER = RestrictedImportManager()
