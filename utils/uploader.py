from utils.clients import get_client
from pyrogram import Client
from pyrogram.types import Message
from config import STORAGE_CHANNEL
import os
from utils.logger import Logger
from urllib.parse import unquote_plus
import subprocess
import json
from utils.video_encoder import VIDEO_ENCODER

logger = Logger(__name__)
PROGRESS_CACHE = {}
STOP_TRANSMISSION = []


async def progress_callback(current, total, id, client: Client, file_path):
    global PROGRESS_CACHE, STOP_TRANSMISSION

    PROGRESS_CACHE[id] = ("running", current, total)
    if id in STOP_TRANSMISSION:
        logger.info(f"Stopping transmission {id}")
        client.stop_transmission()
        try:
            os.remove(file_path)
        except:
            pass


async def copy_file_to_storage(client: Client, source_channel_id: int, message_id: int):
    """Copy a file from source channel to storage channel"""
    try:
        # Get the message from source channel
        source_message = await client.get_messages(source_channel_id, message_id)
        
        if not source_message or source_message.empty:
            raise Exception("Source message not found")
        
        # Forward/copy the message to storage channel
        copied_message = await client.copy_message(
            chat_id=STORAGE_CHANNEL,
            from_chat_id=source_channel_id,
            message_id=message_id,
            disable_notification=True
        )
        
        return copied_message.id
        
    except Exception as e:
        logger.error(f"Error copying file to storage: {e}")
        raise


def get_video_duration(file_path):
    """Extract video duration using ffprobe"""
    try:
        cmd = [
            'ffprobe', 
            '-v', 'quiet', 
            '-print_format', 'json', 
            '-show_format', 
            str(file_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            duration = float(data.get('format', {}).get('duration', 0))
            return int(duration)
        else:
            logger.warning(f"ffprobe failed for {file_path}: {result.stderr}")
            return 0
            
    except subprocess.TimeoutExpired:
        logger.warning(f"ffprobe timeout for {file_path}")
        return 0
    except (subprocess.SubprocessError, json.JSONDecodeError, ValueError, KeyError) as e:
        logger.warning(f"Error getting duration for {file_path}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error getting duration for {file_path}: {e}")
        return 0


def is_video_file(filename):
    """Check if file is a video based on extension"""
    video_extensions = {'.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', 
                       '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'}
    extension = os.path.splitext(filename.lower())[1]
    return extension in video_extensions


async def start_file_uploader(
    file_path, id, directory_path, filename, file_size, delete=True
):
    global PROGRESS_CACHE
    from utils.directoryHandler import DRIVE_DATA

    logger.info(f"Uploading file {file_path} {id}")

    if file_size > 1.98 * 1024 * 1024 * 1024:
        # Use premium client for files larger than 2 GB
        client: Client = get_client(premium_required=True)
    else:
        client: Client = get_client()

    PROGRESS_CACHE[id] = ("running", 0, 0)

    # Extract video duration if it's a video file
    duration = 0
    if is_video_file(filename):
        duration = get_video_duration(file_path)
        logger.info(f"Video duration for {filename}: {duration} seconds")

    message: Message = await client.send_document(
        STORAGE_CHANNEL,
        file_path,
        progress=progress_callback,
        progress_args=(id, client, file_path),
        disable_notification=True,
    )
    size = (
        message.photo
        or message.document
        or message.video
        or message.audio
        or message.sticker
    ).file_size

    filename = unquote_plus(filename)

    # Check if it's a video file and encode to multiple resolutions
    encoded_versions = {}
    if is_video_file(filename):
        logger.info(f"Starting video encoding for {filename}")
        PROGRESS_CACHE[id] = ("encoding", size, size)
        
        try:
            encoded_versions = await VIDEO_ENCODER.encode_video(
                file_path, id, filename
            )
            logger.info(f"Video encoding completed. Generated {len(encoded_versions)} versions")
        except Exception as e:
            logger.error(f"Video encoding failed: {e}")
            # Continue without encoding if it fails

    DRIVE_DATA.new_file(directory_path, filename, message.id, size, duration)
    PROGRESS_CACHE[id] = ("completed", size, size)

    logger.info(f"Uploaded file {file_path} {id}")

    # Mirror file to backup channel (fire-and-forget)
    try:
        from utils.backup_manager import mirror_file
        import asyncio as _asyncio
        _asyncio.create_task(mirror_file(STORAGE_CHANNEL, message.id))
    except Exception as _be:
        logger.error(f"Backup mirror error: {_be}")

    # Fire upload_complete webhooks (import lazily to avoid circular deps)
    try:
        from main import fire_webhooks
        import asyncio as _asyncio
        _asyncio.create_task(fire_webhooks("upload_complete", {
            "filename": filename,
            "size": size,
            "path": directory_path,
            "duration": duration,
        }))
    except Exception:
        pass

    if delete:
        try:
            os.remove(file_path)
        except Exception as e:
            pass