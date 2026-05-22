"""
backup_manager.py — Mirror everything to a backup channel.

Uses the MAIN BOT to forward files from main storage channel to backup channel.
The main bot must be admin in BOTH main channel AND backup channel.

The backup bot is only used to update drive.data in the backup channel
(since DATABASE_BACKUP_MSG_ID belongs to the backup bot's session).

Setup:
  BACKUP_BOT_TOKEN = token of backup bot (created on second account)
  BACKUP_CHANNEL   = channel ID owned by second account
  BACKUP_DB_MSG_ID = message ID in backup channel for drive.data (auto-created if empty)

  Add MAIN BOT as admin to backup channel too.
"""

import asyncio
from utils.logger import Logger
import config

logger = Logger(__name__)

_backup_client = None


def is_backup_enabled() -> bool:
    return bool(config.BACKUP_BOT_TOKEN and config.BACKUP_CHANNEL)


async def get_backup_client():
    """Lazy-init the backup bot client (used only for drive.data backup)."""
    global _backup_client
    if not is_backup_enabled():
        return None
    if _backup_client is not None:
        return _backup_client
    try:
        from pyrogram import Client
        _backup_client = Client(
            name="backup_bot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.BACKUP_BOT_TOKEN,
            sleep_threshold=config.SLEEP_THRESHOLD,
            no_updates=True,
        )
        await _backup_client.start()
        me = await _backup_client.get_me()
        logger.info(f"Backup bot started: @{me.username}")
    except Exception as e:
        logger.error(f"Failed to start backup bot: {e}")
        _backup_client = None
    return _backup_client


async def stop_backup_client():
    global _backup_client
    if _backup_client:
        try:
            await _backup_client.stop()
        except Exception:
            pass
        _backup_client = None


# ── Mirror a single file using MAIN BOT ──────────────────────────────────────
async def mirror_file(message_id: int) -> None:
    """
    Forward a file from main storage to backup channel using the MAIN BOT.
    Main bot must be admin in backup channel.
    """
    if not is_backup_enabled():
        return

    try:
        from utils.clients import get_client
        client = get_client()
        await client.forward_messages(
            chat_id=config.BACKUP_CHANNEL,
            from_chat_id=config.STORAGE_CHANNEL,
            message_ids=[message_id],
            hide_sender_name=True,
            disable_notification=True,
        )
        logger.info(f"Mirrored msg {message_id} to backup channel")
    except Exception as e:
        logger.error(f"mirror_file failed for msg {message_id}: {e}")


# ── Mirror a batch of files using MAIN BOT ────────────────────────────────────
async def mirror_batch(message_ids: list) -> None:
    """
    Forward a batch of files from main storage to backup channel using MAIN BOT.
    """
    if not is_backup_enabled():
        return

    try:
        from utils.clients import get_client
        client = get_client()
    except Exception as e:
        logger.error(f"mirror_batch: cannot get client: {e}")
        return

    BATCH = 100
    DELAY = 3.0   # slightly longer than main import to avoid rate limit competition

    # Small initial delay so main operation finishes first
    await asyncio.sleep(2)

    for i in range(0, len(message_ids), BATCH):
        batch = message_ids[i:i+BATCH]
        try:
            await client.forward_messages(
                chat_id=config.BACKUP_CHANNEL,
                from_chat_id=config.STORAGE_CHANNEL,
                message_ids=batch,
                hide_sender_name=True,
                disable_notification=True,
            )
            logger.info(f"Backup: mirrored batch {i//BATCH+1} ({len(batch)} files)")
        except Exception as e:
            logger.error(f"mirror_batch failed batch {i//BATCH+1}: {e}")
        if i + BATCH < len(message_ids):
            await asyncio.sleep(DELAY)


# ── Mirror drive.data using BACKUP BOT ───────────────────────────────────────
async def mirror_drive_data(drive_cache_path: str) -> None:
    """
    Send/update drive.data in backup channel using the BACKUP BOT.
    Backup bot must be admin in backup channel.
    """
    if not is_backup_enabled():
        return

    client = await get_backup_client()
    if not client:
        return

    from pyrogram.types import InputMediaDocument

    caption = (
        "🔐 **TG Drive Backup — Secondary Storage**\n\n"
        "Do not delete this message.\n\n"
        "**To failover to this backup:**\n"
        "1. MAIN_BOT_TOKEN → BACKUP_BOT_TOKEN value\n"
        "2. STORAGE_CHANNEL → BACKUP_CHANNEL value\n"
        "3. DATABASE_BACKUP_MSG_ID → BACKUP_DB_MSG_ID value\n"
        "4. Redeploy"
    )

    try:
        if config.BACKUP_DB_MSG_ID:
            media_doc = InputMediaDocument(
                drive_cache_path,
                caption=caption,
                file_name="drive.data"
            )
            await client.edit_message_media(
                config.BACKUP_CHANNEL,
                config.BACKUP_DB_MSG_ID,
                media=media_doc,
            )
            logger.info("drive.data mirrored to backup channel")
        else:
            msg = await client.send_document(
                config.BACKUP_CHANNEL,
                drive_cache_path,
                caption=caption,
                file_name="drive.data",
                disable_notification=True,
            )
            config.BACKUP_DB_MSG_ID = msg.id
            logger.info(
                f"drive.data sent to backup channel for first time. "
                f"Message ID: {msg.id} — "
                f"Add BACKUP_DB_MSG_ID={msg.id} to your Render env vars."
            )
            try:
                await client.pin_chat_message(config.BACKUP_CHANNEL, msg.id)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"mirror_drive_data failed: {e}")
