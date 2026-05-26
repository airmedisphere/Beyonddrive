"""
bulk_delete.py — Delete a RANGE of files from STORAGE_CHANNEL + the drive.

Flow:
  1. User provides start_link & end_link (from STORAGE_CHANNEL)
  2. Server parses message IDs from those links
  3. Server scans DRIVE_DATA to find all File entries whose file_id falls in [start, end]
  4. Returns a PREVIEW (count, names, total size, paths) — no destructive action yet
  5. User confirms → server deletes Telegram messages in batches, then drive entries

Progress tracked in BULK_DELETE_PROGRESS so UI can poll.

NOTE: Deletion is PERMANENT. Once the Telegram message is gone, the file's bytes
are unrecoverable. The preview step is required to prevent accidents.
"""

import asyncio
import re
import secrets
import time
from typing import Optional, Dict, Any, List, Tuple

from pyrogram import Client
from pyrogram.errors import FloodWait

from utils.logger import Logger
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL

logger = Logger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────
# Preview tokens (server stashes preview here, frontend confirms with token)
DELETE_PREVIEWS: Dict[str, Dict[str, Any]] = {}
PREVIEW_TTL = 600  # seconds — preview tokens expire after 10 minutes

# Live progress of a running deletion (polled by frontend)
BULK_DELETE_PROGRESS: Dict[str, Dict[str, Any]] = {}
BULK_DELETE_CANCEL:   set = set()

# ── Tuning ────────────────────────────────────────────────────────────────────
DELETE_BATCH_SIZE  = 100   # Telegram allows deleting 100 message IDs per call
INTER_BATCH_DELAY  = 1.5   # seconds between batches
FLOOD_WAIT_CAP     = 60


def _parse_storage_link(link: str) -> int:
    """
    Extract the message ID from a link like:
      https://t.me/c/1234567890/123       → 123
      https://t.me/channelname/123        → 123
      https://t.me/c/1234567890/5/123     → 123 (topic link, last segment)
    """
    link = link.strip().rstrip("/")
    if "?" in link:
        link = link.split("?")[0]

    m = re.search(r"t\.me/(?:c/)?[^/]+/(?:\d+/)?(\d+)$", link)
    if not m:
        raise ValueError(f"Could not parse a message ID from link: {link}")
    return int(m.group(1))


def _iter_drive_files():
    """Yield (file_obj, parent_folder_data) for every File in the drive."""
    def walk(folder):
        for item in list(folder.contents.values()):
            if getattr(item, "type", None) == "file":
                yield item, folder
            elif getattr(item, "type", None) == "folder":
                yield from walk(item)
    yield from walk(DRIVE_DATA.contents["/"])


def _expire_old_previews():
    now = time.time()
    expired = [k for k, v in DELETE_PREVIEWS.items() if v.get("expires_at", 0) < now]
    for k in expired:
        DELETE_PREVIEWS.pop(k, None)


def build_preview(start_msg_id: int, end_msg_id: int) -> Dict[str, Any]:
    """
    Scan the drive for any File whose file_id is in [start_msg_id, end_msg_id].
    Returns a preview dict + a confirmation token (saved in DELETE_PREVIEWS).
    """
    _expire_old_previews()

    if start_msg_id > end_msg_id:
        start_msg_id, end_msg_id = end_msg_id, start_msg_id

    matches: List[Dict[str, Any]] = []
    for file_obj, parent in _iter_drive_files():
        fid = getattr(file_obj, "file_id", None)
        if not isinstance(fid, int):
            continue
        if start_msg_id <= fid <= end_msg_id:
            # Skip fast-imports — those live in OTHER channels, not STORAGE_CHANNEL
            if getattr(file_obj, "is_fast_import", False):
                continue
            matches.append({
                "name":      file_obj.name,
                "path":      file_obj.path,
                "id":        file_obj.id,        # drive ID (random)
                "file_id":   fid,                 # telegram message ID
                "size":      getattr(file_obj, "size", 0),
            })

    matches.sort(key=lambda x: x["file_id"])

    token = secrets.token_hex(12)
    total_size = sum(m["size"] for m in matches)
    DELETE_PREVIEWS[token] = {
        "matches":     matches,
        "start_id":    start_msg_id,
        "end_id":      end_msg_id,
        "total_size":  total_size,
        "created_at":  time.time(),
        "expires_at":  time.time() + PREVIEW_TTL,
    }

    return {
        "preview_token": token,
        "count":         len(matches),
        "total_size":    total_size,
        "start_id":      start_msg_id,
        "end_id":        end_msg_id,
        "range_size":    end_msg_id - start_msg_id + 1,
        "matches":       matches[:50],  # cap preview list at 50 for UI
        "truncated":     len(matches) > 50,
        "expires_in":    PREVIEW_TTL,
    }


def _drive_path_for_file(file_id_drive: str) -> Optional[str]:
    """Return the drive path '/folder/.../<random_id>' for a File.id."""
    for file_obj, parent in _iter_drive_files():
        if file_obj.id == file_id_drive:
            return f"{file_obj.path}/{file_obj.id}".replace("//", "/")
    return None


async def execute_delete(
    client: Client,
    preview_token: str,
    delete_id: str,
) -> None:
    """Run the actual deletion. Updates BULK_DELETE_PROGRESS as it goes."""
    preview = DELETE_PREVIEWS.get(preview_token)
    if not preview:
        BULK_DELETE_PROGRESS[delete_id] = {
            "status":    "error",
            "error_msg": "Preview expired or not found. Please re-preview.",
        }
        return

    matches = preview["matches"]
    BULK_DELETE_PROGRESS[delete_id] = {
        "status":        "deleting",
        "total":         len(matches),
        "telegram_deleted": 0,
        "drive_deleted":    0,
        "errors":           0,
        "start_time":       time.time(),
        "current_file":     "",
    }

    if not matches:
        BULK_DELETE_PROGRESS[delete_id].update({
            "status":  "done",
            "elapsed": 0,
        })
        DELETE_PREVIEWS.pop(preview_token, None)
        return

    # ── Phase 1: delete from Telegram in batches ─────────────────────────────
    msg_ids_to_delete = [m["file_id"] for m in matches]
    deleted_from_tg: set = set()

    for i in range(0, len(msg_ids_to_delete), DELETE_BATCH_SIZE):
        if delete_id in BULK_DELETE_CANCEL:
            break

        batch = msg_ids_to_delete[i : i + DELETE_BATCH_SIZE]

        for attempt in range(4):
            try:
                await client.delete_messages(
                    chat_id=STORAGE_CHANNEL,
                    message_ids=batch,
                )
                deleted_from_tg.update(batch)
                BULK_DELETE_PROGRESS[delete_id]["telegram_deleted"] += len(batch)
                logger.info(f"[BulkDelete] Removed {len(batch)} TG messages")
                break
            except FloodWait as e:
                wait = min(int(getattr(e, "value", 30)), FLOOD_WAIT_CAP)
                logger.warning(f"[BulkDelete] FloodWait {wait}s")
                await asyncio.sleep(wait + 1)
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"[BulkDelete] Permanent TG delete failure: {e}")
                    BULK_DELETE_PROGRESS[delete_id]["errors"] += len(batch)

        await asyncio.sleep(INTER_BATCH_DELAY)

    # ── Phase 2: remove drive entries (always — even if TG delete failed, the
    # message may still actually be gone, and stale entries are bad) ─────────
    for m in matches:
        if delete_id in BULK_DELETE_CANCEL:
            break
        try:
            drive_path = _drive_path_for_file(m["id"])
            if drive_path:
                DRIVE_DATA.delete_file_folder(drive_path)
                BULK_DELETE_PROGRESS[delete_id]["drive_deleted"] += 1
                BULK_DELETE_PROGRESS[delete_id]["current_file"] = m["name"]
        except Exception as e:
            logger.error(f"[BulkDelete] Drive remove failed for {m['name']}: {e}")
            BULK_DELETE_PROGRESS[delete_id]["errors"] += 1

    try:
        DRIVE_DATA.save()
    except Exception:
        pass

    BULK_DELETE_PROGRESS[delete_id]["status"] = (
        "cancelled" if delete_id in BULK_DELETE_CANCEL else "done"
    )
    BULK_DELETE_PROGRESS[delete_id]["elapsed"] = round(
        time.time() - BULK_DELETE_PROGRESS[delete_id]["start_time"], 1
    )

    # Cleanup
    BULK_DELETE_CANCEL.discard(delete_id)
    DELETE_PREVIEWS.pop(preview_token, None)
