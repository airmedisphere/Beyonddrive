"""
backup_manager.py — Mirror everything to a backup bot + channel.

When BACKUP_BOT_TOKEN and BACKUP_CHANNEL are set:
  - Every file uploaded → also forwarded to backup channel
  - drive.data → also backed up to backup channel
  - On failover: swap 3 env vars, redeploy, done

Usage:
  from utils.backup_manager import BACKUP, mirror_file, mirror_drive_data
"""

import asyncio
from utils.logger import Logger
import config

logger = Logger(__name__)

# ── Backup bot client (lazy init) ─────────────────────────────────────────────
_backup_client = None


def is_backup_enabled() -> bool:
    return bool(config.BACKUP_BOT_TOKEN and config.BACKUP_CHANNEL)


async def get_backup_client():
    """Lazy-init the backup bot client."""
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
            no_updates=True,  # backup bot doesn't need to receive updates
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


# ── Mirror a single file message to backup channel ────────────────────────────
async def mirror_file(main_channel_id: int, message_id: int) -> int | None:
    """
    Forward a file from main storage channel to backup channel.
    Returns the backup message ID or None on failure.
    """
    if not is_backup_enabled():
        return None

    client = await get_backup_client()
    if not client:
        return None

    try:
        forwarded = await client.forward_messages(
            chat_id=config.BACKUP_CHANNEL,
            from_chat_id=main_channel_id,
            message_ids=[message_id],
            hide_sender_name=True,
            disable_notification=True,
        )
        if isinstance(forwarded, list):
            forwarded = forwarded[0] if forwarded else None
        if forwarded:
            logger.info(f"Mirrored msg {message_id} → backup msg {forwarded.id}")
            return forwarded.id
    except Exception as e:
        logger.error(f"Mirror file failed for msg {message_id}: {e}")
    return None


# ── Mirror drive.data to backup channel ──────────────────────────────────────
async def mirror_drive_data(drive_cache_path: str) -> None:
    """
    Send/update drive.data in backup channel.
    Creates the message if BACKUP_DB_MSG_ID is not set,
    otherwise edits the existing message.
    """
    if not is_backup_enabled():
        return

    client = await get_backup_client()
    if not client:
        return

    from pyrogram.types import InputMediaDocument

    caption = (
        "🔐 **TG Drive Backup — Secondary Storage**\n\n"
        "This is the backup drive.data file.\n"
        "Do not delete this message.\n\n"
        "To switch to this backup:\n"
        "1. Set MAIN_BOT_TOKEN = BACKUP_BOT_TOKEN\n"
        "2. Set STORAGE_CHANNEL = BACKUP_CHANNEL\n"
        "3. Set DATABASE_BACKUP_MSG_ID = this message ID\n"
        "4. Redeploy"
    )

    try:
        if config.BACKUP_DB_MSG_ID:
            # Edit existing backup message
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
            logger.info("drive.data mirrored to backup channel (edited)")
        else:
            # First time — send new message and log the ID
            msg = await client.send_document(
                config.BACKUP_CHANNEL,
                drive_cache_path,
                caption=caption,
                file_name="drive.data",
                disable_notification=True,
            )
            # Save the message ID to config so next time we edit it
            config.BACKUP_DB_MSG_ID = msg.id
            logger.info(
                f"drive.data sent to backup channel for first time. "
                f"Message ID: {msg.id} — "
                f"Add BACKUP_DB_MSG_ID={msg.id} to your env vars."
            )
            # Also pin it
            try:
                await client.pin_chat_message(config.BACKUP_CHANNEL, msg.id)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"mirror_drive_data failed: {e}")


# ── Mirror a batch of files (for bulk import) ─────────────────────────────────
async def mirror_batch(main_channel_id: int, message_ids: list) -> None:
    """
    Mirror a batch of files to backup channel using forward_messages.
    Fire-and-forget — errors are logged but don't block main flow.
    """
    if not is_backup_enabled():
        return

    client = await get_backup_client()
    if not client:
        return

    BATCH = 100
    DELAY = 2.5

    for i in range(0, len(message_ids), BATCH):
        batch = message_ids[i:i+BATCH]
        try:
            await client.forward_messages(
                chat_id=config.BACKUP_CHANNEL,
                from_chat_id=main_channel_id,
                message_ids=batch,
                hide_sender_name=True,
                disable_notification=True,
            )
            logger.info(f"Mirrored batch {i//BATCH + 1}: {len(batch)} files to backup")
        except Exception as e:
            logger.error(f"mirror_batch failed for batch {i//BATCH + 1}: {e}")
        if i + BATCH < len(message_ids):
            await asyncio.sleep(DELAY)
