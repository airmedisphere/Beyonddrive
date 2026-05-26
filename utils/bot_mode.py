import asyncio
import base64
import json
import os
import re
import secrets
import traceback
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import config
from utils.logger import Logger
from pathlib import Path

logger = Logger(f"{__name__}")

# Strong references to background import tasks — prevents Python GC from
# collecting tasks during asyncio.sleep() between forward batches
_BULK_IMPORT_TASKS: set = set()

# File to persist import state across server restarts




START_CMD = """🚀 **Welcome To TG Drive's Bot Mode**

You can use this bot to upload files to your TG Drive website directly instead of doing it from website.

🗄 **Commands:**
/set_folder - Set folder for file uploads
/current_folder - Check current folder
/create_folder - Create a new folder in current directory
/bulk_import - Import files in bulk from Telegram channel/group
/restricted_import - Import from RESTRICTED channels (uses user session)
/bulk_delete - Delete a range of files (with preview + confirm)
/fast_import - Import files directly without copying (requires admin access)
/stats - Show drive storage statistics
/search <query> - Search files and folders on the drive
/send_to @channel - Forward all files from current folder to a channel/group
/send_file FILE_ID @channel - Forward a single file to a channel/group
/generate_link - Generate a shareable link for current folder files

📤 **How To Upload Files:** Send a file to this bot and it will be uploaded to your TG Drive website. You can also set a folder for file uploads using /set_folder command.

📁 **How To Create Folders:** Use /create_folder command to create new folders in your current directory.

📦 **How To Bulk Import:** Use /bulk_import command to import multiple files from a Telegram channel/group by providing a range of message links.

🔒 **How To Restricted Import:** Use /restricted_import for channels where the bot is not admin. Requires a logged-in user session (PREMIUM_ACCOUNTS).

🗑️ **How To Bulk Delete:** Use /bulk_delete and paste a start + end link from your STORAGE channel. You'll see a preview before anything is deleted.

⚡ **How To Fast Import:** Use /fast_import command to import files directly from channels without copying them. The bot must be admin in the source channel.

📤 **How To Send Files:** Use /send_to @channel to forward all files from your current folder to any channel or group. Use /send_file to send a single file.

Read more about [TG Drive's Bot Mode](https://github.com/TechShreyash/TGDrive#tg-drives-bot-mode)
"""

SET_FOLDER_PATH_CACHE = {}
DRIVE_DATA = None
BOT_MODE = None 

session_cache_path = Path(f"./cache")
session_cache_path.parent.mkdir(parents=True, exist_ok=True)

DEFAULT_FOLDER_CONFIG_FILE = Path("./default_folder_config.json")

main_bot = Client(
    name="main_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.MAIN_BOT_TOKEN,
    sleep_threshold=config.SLEEP_THRESHOLD,
    workdir=session_cache_path,
)

# --- Manual 'ask' implementation setup ---
# Stores {chat_id: (asyncio.Queue, asyncio.Event, pyrogram.filters)}
_pending_requests = {}

async def manual_ask(client: Client, chat_id: int, text: str, timeout: int = 60, filters=None) -> Message:
    """
    A manual implementation of the 'ask' functionality for older Pyrogram versions.
    Sends a message and waits for a response from the specified chat_id.
    """
    queue = asyncio.Queue(1)
    event = asyncio.Event()
    
    _pending_requests[chat_id] = (queue, event, filters)

    await client.send_message(chat_id, text)

    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        response_message = await queue.get()
        return response_message
    except asyncio.TimeoutError:
        raise asyncio.TimeoutError # Re-raise if timed out
    finally:
        # Clean up the pending request regardless of outcome
        if chat_id in _pending_requests:
            del _pending_requests[chat_id]
# --- End Manual 'ask' implementation setup ---


# --- COMMAND HANDLERS (Prioritized) ---

@main_bot.on_message(
    filters.command(["start", "help"])
    & filters.private,
)
async def start_handler(client: Client, message: Message):
    """
    Handles /start and /help.
    - Plain /start → admin only, show welcome
    - /start PAYLOAD → anyone, decode share link and ask where to send files
    """
    parts = message.text.split(None, 1)
    payload = parts[1].strip() if len(parts) > 1 else None

    # Deep link with payload — open to anyone
    if payload:
        await handle_share_link(client, message, payload)
        return

    # Plain /start — admin only
    if message.from_user.id not in config.TELEGRAM_ADMIN_IDS:
        await message.reply_text("👋 Hi! This bot is private.")
        return

    await message.reply_text(START_CMD)


@main_bot.on_message(
    filters.command("stats")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def stats_handler(client: Client, message: Message):
    """Show drive storage statistics."""
    from utils.directoryHandler import DRIVE_DATA
    from pathlib import Path as _Path

    if DRIVE_DATA is None:
        await message.reply_text("❌ Drive not initialized.")
        return

    total_files = 0
    total_folders = 0
    total_size = 0
    breakdown = {}

    VIDEO_EXTS = {".mp4",".mkv",".avi",".mov",".webm",".m4v",".ts",".flv",".wmv",".3gp",".mpg",".mpeg",".ogv"}
    AUDIO_EXTS = {".mp3",".wav",".flac",".aac",".ogg",".m4a",".opus",".wma"}
    DOC_EXTS   = {".pdf",".doc",".docx",".xls",".xlsx",".ppt",".pptx",".txt",".epub",".csv"}
    IMAGE_EXTS = {".jpg",".jpeg",".png",".gif",".webp",".bmp",".svg"}

    def classify(name):
        ext = _Path(name).suffix.lower()
        if ext in VIDEO_EXTS:  return "🎬 Video"
        if ext in AUDIO_EXTS:  return "🎵 Audio"
        if ext in DOC_EXTS:    return "📄 Document"
        if ext in IMAGE_EXTS:  return "🖼️ Image"
        return "📦 Other"

    def traverse(folder):
        nonlocal total_files, total_folders, total_size
        for item in folder.contents.values():
            if item.type == "folder":
                total_folders += 1
                traverse(item)
            else:
                total_files += 1
                total_size  += item.size
                cat = classify(item.name)
                breakdown[cat] = breakdown.get(cat, 0) + item.size

    traverse(DRIVE_DATA.get_directory("/"))

    def fmt(b):
        if b >= 1073741824: return f"{b/1073741824:.2f} GB"
        if b >= 1048576:    return f"{b/1048576:.2f} MB"
        if b >= 1024:       return f"{b/1024:.2f} KB"
        return f"{b} B"

    lines = [
        "📊 **Drive Statistics**\n",
        f"📁 Folders : `{total_folders}`",
        f"📄 Files   : `{total_files}`",
        f"💾 Total   : `{fmt(total_size)}`\n",
        "**Breakdown by type:**",
    ]
    for cat, size in sorted(breakdown.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"  {cat}: `{fmt(size)}`")

    await message.reply_text("\n".join(lines))


@main_bot.on_message(
    filters.command("search")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def search_handler(client: Client, message: Message):
    """Search files and folders on the drive."""
    from utils.directoryHandler import DRIVE_DATA

    if DRIVE_DATA is None:
        await message.reply_text("❌ Drive not initialized.")
        return

    query = " ".join(message.command[1:]).strip()
    if not query:
        await message.reply_text("❌ Usage: `/search <query>`")
        return

    results = DRIVE_DATA.search_file_folder(query)

    if not results:
        await message.reply_text(f"🔍 No results found for `{query}`")
        return

    lines = [f"🔍 **Search results for:** `{query}`\n"]
    count = 0
    for item_id, item in results.items():
        if count >= 20:
            lines.append(f"\n_...and {len(results) - 20} more results_")
            break
        icon = "📁" if item.type == "folder" else "📄"
        size_str = ""
        if item.type == "file":
            s = item.size
            if s >= 1073741824:   size_str = f" · {s/1073741824:.1f} GB"
            elif s >= 1048576:    size_str = f" · {s/1048576:.1f} MB"
            elif s >= 1024:       size_str = f" · {s/1024:.1f} KB"
            else:                 size_str = f" · {s} B"
        lines.append(f"{icon} **{item.name}**{size_str}")
        count += 1

    await message.reply_text("\n".join(lines))



async def fast_import_handler(client: Client, message: Message):
    """
    Handles the /fast_import command for importing files directly without copying.
    """
    global BOT_MODE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the required information or /cancel.")
        return 

    # Check if current folder is set
    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No current folder set. Please use /set_folder to set a folder first before fast importing files."
        )
        return

    await message.reply_text(
        "⚡ **Fast Import Files**\n\n"
        "This feature allows you to import files directly from a Telegram channel without copying them to your storage channel. "
        "The files will be streamed directly from the source channel.\n\n"
        "**Requirements:**\n"
        "• The bot must be admin in the source channel\n"
        "• You need to provide the channel username or ID\n"
        "• Optionally, you can specify a message range\n\n"
        "**How to use:**\n"
        "1. Provide the channel username (e.g., @mychannel) or ID\n"
        "2. Optionally provide start and end message IDs for a specific range\n"
        "3. Files will be imported instantly without copying\n\n"
        "**Note:** Fast imported files are streamed directly from the source channel.\n\n"
        "Let's start! Send /cancel to cancel anytime."
    )

    # Get the channel identifier
    try:
        channel_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📺 **Step 1: Channel Information**\n\n"
                "Please send the channel username or ID:\n\n"
                "**Examples:**\n"
                "• @mychannel\n"
                "• mychannel\n"
                "• -1001234567890\n\n"
                "Send /cancel to cancel"
            ),
            timeout=300,  # 5 minutes timeout
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nFast import cancelled. Use /fast_import to try again.")
        return

    if channel_msg.text.lower() == "/cancel":
        await message.reply_text("❌ **Cancelled**\n\nFast import cancelled.")
        return

    channel_identifier = channel_msg.text.strip()
    
    # Ask for message range (optional)
    try:
        range_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📋 **Step 2: Message Range (Optional)**\n\n"
                "Do you want to import all files or specify a range?\n\n"
                "**Options:**\n"
                "• Send 'all' to import all files from the channel\n"
                "• Send 'range' to specify start and end message IDs\n"
                "• Send /cancel to cancel\n\n"
                f"**Channel:** {channel_identifier}"
            ),
            timeout=300,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nFast import cancelled. Use /fast_import to try again.")
        return

    if range_msg.text.lower() == "/cancel":
        await message.reply_text("❌ **Cancelled**\n\nFast import cancelled.")
        return

    start_msg_id = None
    end_msg_id = None

    if range_msg.text.lower() == "range":
        # Get start message ID
        try:
            start_msg = await manual_ask(
                client=client,
                chat_id=message.chat.id,
                text=(
                    "🔢 **Start Message ID**\n\n"
                    "Please send the starting message ID:\n\n"
                    "Send /cancel to cancel"
                ),
                timeout=300,
                filters=filters.text,
            )
        except asyncio.TimeoutError:
            await message.reply_text("⏰ **Timeout**\n\nFast import cancelled.")
            return

        if start_msg.text.lower() == "/cancel":
            await message.reply_text("❌ **Cancelled**\n\nFast import cancelled.")
            return

        try:
            start_msg_id = int(start_msg.text.strip())
        except ValueError:
            await message.reply_text("❌ **Invalid Message ID**\n\nPlease provide a valid number.")
            return

        # Get end message ID
        try:
            end_msg = await manual_ask(
                client=client,
                chat_id=message.chat.id,
                text=(
                    "🔢 **End Message ID**\n\n"
                    "Please send the ending message ID:\n\n"
                    f"**Starting from:** {start_msg_id}\n\n"
                    "Send /cancel to cancel"
                ),
                timeout=300,
                filters=filters.text,
            )
        except asyncio.TimeoutError:
            await message.reply_text("⏰ **Timeout**\n\nFast import cancelled.")
            return

        if end_msg.text.lower() == "/cancel":
            await message.reply_text("❌ **Cancelled**\n\nFast import cancelled.")
            return

        try:
            end_msg_id = int(end_msg.text.strip())
        except ValueError:
            await message.reply_text("❌ **Invalid Message ID**\n\nPlease provide a valid number.")
            return

        if start_msg_id >= end_msg_id:
            await message.reply_text("❌ **Invalid Range**\n\nStart message ID must be less than end message ID.")
            return

    # Confirm the import
    range_text = "All files" if not start_msg_id else f"Messages {start_msg_id} to {end_msg_id}"
    confirmation_msg = await message.reply_text(
        f"⚡ **Confirm Fast Import**\n\n"
        f"**Channel:** {channel_identifier}\n"
        f"**Range:** {range_text}\n"
        f"**Destination folder:** {BOT_MODE.current_folder_name}\n\n"
        f"**Important:** This will import files directly without copying them. "
        f"The bot must be admin in the source channel.\n\n"
        f"Type **YES** to confirm or **NO** to cancel."
    )

    try:
        confirm_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text="Please type **YES** to confirm or **NO** to cancel:",
            timeout=60,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nFast import cancelled due to timeout.")
        return

    if confirm_msg.text.upper() not in ["YES", "Y"]:
        await message.reply_text("❌ **Cancelled**\n\nFast import cancelled by user.")
        return

    # Start the fast import process
    await message.reply_text(
        f"⚡ **Starting Fast Import**\n\n"
        f"Importing files from {channel_identifier}...\n"
        f"This should be much faster than regular import!\n\n"
        f"**Current folder:** {BOT_MODE.current_folder_name}"
    )

    # Start the fast import task
    asyncio.create_task(
        fast_import_files(
            client, 
            message.chat.id, 
            channel_identifier, 
            BOT_MODE.current_folder,
            start_msg_id,
            end_msg_id
        )
    )


async def fast_import_files(client, user_chat_id, channel_identifier, destination_folder, start_msg_id=None, end_msg_id=None):
    """
    Fast import files from a channel without copying them.
    """
    global DRIVE_DATA
    
    try:
        from utils.fast_import import FAST_IMPORT_MANAGER
        
        imported_count, total_files = await FAST_IMPORT_MANAGER.fast_import_files(
            client, 
            channel_identifier, 
            destination_folder, 
            start_msg_id, 
            end_msg_id
        )

        # Send completion message
        await client.send_message(
            user_chat_id,
            f"✅ **Fast Import Completed**\n\n"
            f"**Successfully imported:** {imported_count:,} files\n"
            f"**Total files processed:** {total_files:,}\n"
            f"**Success rate:** {(imported_count / total_files * 100):.1f}%\n"
            f"**Destination folder:** {BOT_MODE.current_folder_name}\n\n"
            f"⚡ **Fast imported files are now available on your TG Drive website!**\n"
            f"Files are streamed directly from the source channel for maximum efficiency! 🎉"
        )

    except Exception as e:
        logger.error(f"Fast import failed: {e}")
        await client.send_message(
            user_chat_id,
            f"❌ **Fast Import Failed**\n\n"
            f"An error occurred during the fast import process.\n\n"
            f"**Error:** {str(e)}\n\n"
            f"**Possible solutions:**\n"
            f"• Make sure the bot is admin in the source channel\n"
            f"• Check that the channel identifier is correct\n"
            f"• Verify the message range is valid\n\n"
            f"Please try again or contact support if the issue persists."
        )


@main_bot.on_message(
    filters.command("bulk_import")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def bulk_import_handler(client: Client, message: Message):
    """
    Handles the /bulk_import command to import files in bulk from Telegram channels/groups.
    """
    global BOT_MODE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the required information or /cancel.")
        return 

    # Check if current folder is set
    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No current folder set. Please use /set_folder to set a folder first before bulk importing files."
        )
        return

    await message.reply_text(
        "📦 **Bulk Import Files**\n\n"
        "This feature allows you to import multiple files from a Telegram channel or group.\n\n"
        "**How to use:**\n"
        "1. Get the link of the first file you want to import\n"
        "2. Get the link of the last file you want to import\n"
        "3. I'll import all files between these two messages\n\n"
        "**Example:**\n"
        "From: `https://t.me/ParmarEnglishPyqSeriesPart1/3`\n"
        "To: `https://t.me/ParmarEnglishPyqSeriesPart1/79`\n\n"
        "**Note:** Both links must be from the same channel/group.\n"
        "**Maximum:** Up to 5,000 files per bulk import.\n\n"
        "Let's start! Send /cancel to cancel anytime."
    )

    # Get the starting link
    try:
        start_link_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📎 **Step 1/2: Starting Link**\n\n"
                "Please send the Telegram link of the **first file** you want to import.\n\n"
                "**Format:** `https://t.me/channel_name/message_id`\n\n"
                "Send /cancel to cancel"
            ),
            timeout=300,  # 5 minutes timeout
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nBulk import cancelled. Use /bulk_import to try again.")
        return

    if start_link_msg.text.lower() == "/cancel":
        await message.reply_text("❌ **Cancelled**\n\nBulk import cancelled.")
        return

    start_link = start_link_msg.text.strip()
    
    # Validate and parse the starting link
    start_parsed = parse_telegram_link(start_link)
    if not start_parsed:
        await message.reply_text(
            "❌ **Invalid Link Format**\n\n"
            "Please provide a valid Telegram link in the format:\n"
            "`https://t.me/channel_name/message_id`\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Get the ending link
    try:
        end_link_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📎 **Step 2/2: Ending Link**\n\n"
                "Please send the Telegram link of the **last file** you want to import.\n\n"
                "**Format:** `https://t.me/channel_name/message_id`\n\n"
                f"**Starting from:** {start_parsed['channel']}/{start_parsed['message_id']}\n\n"
                "Send /cancel to cancel"
            ),
            timeout=300,  # 5 minutes timeout
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nBulk import cancelled. Use /bulk_import to try again.")
        return

    if end_link_msg.text.lower() == "/cancel":
        await message.reply_text("❌ **Cancelled**\n\nBulk import cancelled.")
        return

    end_link = end_link_msg.text.strip()
    
    # Validate and parse the ending link
    end_parsed = parse_telegram_link(end_link)
    if not end_parsed:
        await message.reply_text(
            "❌ **Invalid Link Format**\n\n"
            "Please provide a valid Telegram link in the format:\n"
            "`https://t.me/channel_name/message_id`\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Validate that both links are from the same channel
    if start_parsed['channel'] != end_parsed['channel']:
        await message.reply_text(
            "❌ **Channel Mismatch**\n\n"
            "Both links must be from the same channel or group.\n\n"
            f"**Starting link:** {start_parsed['channel']}\n"
            f"**Ending link:** {end_parsed['channel']}\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Validate message ID range
    start_id = start_parsed['message_id']
    end_id = end_parsed['message_id']
    
    if start_id >= end_id:
        await message.reply_text(
            "❌ **Invalid Range**\n\n"
            "The starting message ID must be less than the ending message ID.\n\n"
            f"**Starting ID:** {start_id}\n"
            f"**Ending ID:** {end_id}\n\n"
            "Use /bulk_import to try again."
        )
        return

    # Calculate the number of files to import
    file_count = end_id - start_id + 1
    
    # Increased limit to 5000 files
    if file_count > 5000:
        await message.reply_text(
            "❌ **Too Many Files**\n\n"
            f"You're trying to import {file_count:,} files. The maximum allowed is 5,000 files per bulk import.\n\n"
            "**Suggestions:**\n"
            "• Split your import into smaller ranges\n"
            "• Import in batches of 5,000 or fewer files\n\n"
            "Please reduce the range and try again."
        )
        return

    # Show warning for large imports
    warning_message = ""
    if file_count > 1000:
        warning_message = (
            f"⚠️ **Large Import Warning:** You're importing {file_count:,} files. "
            f"This may take a significant amount of time (estimated: {file_count // 60 + 1} minutes).\n\n"
        )

    # Confirm the import
    confirmation_msg = await message.reply_text(
        f"📋 **Confirm Bulk Import**\n\n"
        f"**Channel:** {start_parsed['channel']}\n"
        f"**Range:** {start_id:,} to {end_id:,}\n"
        f"**Total files:** {file_count:,}\n"
        f"**Destination folder:** {BOT_MODE.current_folder_name}\n\n"
        f"{warning_message}"
        f"**Important:** This will import {file_count:,} files. Make sure you have enough storage space.\n\n"
        f"Type **YES** to confirm or **NO** to cancel."
    )

    try:
        confirm_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text="Please type **YES** to confirm or **NO** to cancel:",
            timeout=60,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ **Timeout**\n\nBulk import cancelled due to timeout.")
        return

    if confirm_msg.text.upper() not in ["YES", "Y"]:
        await message.reply_text("❌ **Cancelled**\n\nBulk import cancelled by user.")
        return

    # Start the bulk import process
    await message.reply_text(
        f"🚀 **Starting Bulk Import**\n\n"
        f"Importing {file_count:,} files from {start_parsed['channel']}...\n"
        f"This may take a while. I'll send you updates every 50 files.\n\n"
        f"**Current folder:** {BOT_MODE.current_folder_name}\n\n"
        f"**Estimated time:** {file_count // 60 + 1} minutes"
    )

    # create_task: non-blocking so event loop stays free for HTTP health checks.
    # State saved to disk after every batch — auto-resumes on server restart.
    _task = asyncio.create_task(
        bulk_import_files(
            client,
            message.chat.id,
            start_parsed['channel'],
            start_id,
            end_id,
            BOT_MODE.current_folder,
        )
    )
    _BULK_IMPORT_TASKS.add(_task)
    _task.add_done_callback(_BULK_IMPORT_TASKS.discard)


def parse_telegram_link(link):
    """
    Parse a Telegram link and extract channel name and message ID.
    Returns a dict with 'channel' and 'message_id' or None if invalid.
    """
    # Pattern to match Telegram links
    patterns = [
        r'https://t\.me/([^/]+)/(\d+)',  # https://t.me/channel/123
        r'https://telegram\.me/([^/]+)/(\d+)',  # https://telegram.me/channel/123
        r't\.me/([^/]+)/(\d+)',  # t.me/channel/123
    ]
    
    for pattern in patterns:
        match = re.match(pattern, link.strip())
        if match:
            channel = match.group(1)
            message_id = int(match.group(2))
            return {
                'channel': channel,
                'message_id': message_id
            }
    
    return None


async def bulk_import_files(client, user_chat_id, channel_name, start_id, end_id, destination_folder, resume_from_batch=0):
    """
    Resumable bulk import using forward_messages (server-side, no re-upload).

    State is stored in DRIVE_DATA.pending_import which is serialized into
    drive.data and backed up to Telegram automatically. This survives
    Render deployments, restarts, and container wipes — because drive.data
    is reloaded from Telegram on every startup before this code runs.
    """
    global DRIVE_DATA

    FORWARD_BATCH = 100
    INTER_DELAY   = 2.5

    try:
        try:
            channel = await client.get_chat(channel_name)
            channel_id = channel.id
        except Exception as e:
            await client.send_message(user_chat_id,
                f"\u274c **Error accessing channel**\n\n`{channel_name}`\n\n**Error:** {str(e)}")
            DRIVE_DATA.pending_import = None
            DRIVE_DATA.isUpdated = True
            return

        all_ids       = list(range(start_id, end_id + 1))
        batches       = [all_ids[i:i+FORWARD_BATCH] for i in range(0, len(all_ids), FORWARD_BATCH)]
        total_batches = len(batches)
        total_range   = end_id - start_id + 1
        is_resume     = resume_from_batch > 0
        eta_s         = int((total_batches - resume_from_batch) * INTER_DELAY)

        # Store state in DRIVE_DATA — the background backup task will
        # persist this to Telegram without blocking the import loop
        DRIVE_DATA.pending_import = {
            "user_chat_id":  user_chat_id,
            "channel_name":  channel_name,
            "start_id":      start_id,
            "end_id":        end_id,
            "destination":   destination_folder,
            "total_batches": total_batches,
            "resume_from":   resume_from_batch,
        }
        DRIVE_DATA.isUpdated = True  # signal backup task to save, don't block here

        status_msg = await client.send_message(
            user_chat_id,
            f"{'\U0001f504 **Resuming' if is_resume else '\u26a1 **Starting'} Bulk Forward Import**\n\n"
            f"**Channel:** {channel_name}\n"
            f"**Range:** {start_id:,} \u2192 {end_id:,} ({total_range:,} messages)\n"
            f"**Batches:** {total_batches} \u00d7 {FORWARD_BATCH} IDs\n"
            f"{'**Resuming from batch:** ' + str(resume_from_batch+1) + chr(10) if is_resume else ''}"
            f"**Est. remaining:** ~{eta_s}s\n"
            f"**Method:** Server-side copy (no re-upload)"
        )

        imported_count = 0
        error_count    = 0

        for batch_num in range(resume_from_batch, total_batches):
            batch_ids = batches[batch_num]

            for attempt in range(5):
                try:
                    forwarded = await client.forward_messages(
                        chat_id=config.STORAGE_CHANNEL,
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
                        fm = fwd.document or fwd.video or fwd.audio or fwd.photo or fwd.sticker
                        if not fm:
                            continue
                        fname = getattr(fm, "file_name", None) or f"file_{fwd.id}"
                        fsize = getattr(fm, "file_size", 0) or 0
                        fdur  = getattr(fm, "duration", 0) if hasattr(fm, "duration") else 0
                        # register_file adds to memory WITHOUT saving each time
                        DRIVE_DATA.register_file(destination_folder, fname, fwd.id, fsize, fdur)
                        imported_count += 1

                    # ONE save for the whole batch — not 99 individual saves
                    DRIVE_DATA.save()
                    break  # success

                except Exception as e:
                    err = str(e)
                    if "FLOOD_WAIT" in err:
                        wait = 35
                        try: wait = min(int(err.split("_")[-1]), 35)
                        except Exception: pass
                        logger.warning(f"Flood wait {wait}s on batch {batch_num+1}")
                        try:
                            await status_msg.edit_text(
                                f"\u23f3 **Flood wait {wait}s**\n\n"
                                f"**Imported:** {imported_count:,} | **Batch:** {batch_num+1}/{total_batches}\n"
                                f"Resuming automatically..."
                            )
                        except Exception: pass
                        await asyncio.sleep(wait)
                    elif attempt < 4:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        tb = traceback.format_exc()
                        logger.error(f"Batch {batch_num+1} failed: {e}\n{tb}")
                        error_count += len(batch_ids)
                        try:
                            await status_msg.edit_text(
                                f"\u26a0\ufe0f **Batch {batch_num+1} failed:** {str(e)[:150]}\nContinuing..."
                            )
                        except Exception: pass

            # Update resume point — save() already called above after batch
            if DRIVE_DATA.pending_import:
                DRIVE_DATA.pending_import["resume_from"] = batch_num + 1

            pct       = int((batch_num + 1) / total_batches * 100)
            remaining = int((total_batches - batch_num - 1) * INTER_DELAY)
            try:
                await status_msg.edit_text(
                    f"\U0001f4ca **Progress: {pct}%**\n\n"
                    f"**Imported:** {imported_count:,}\n"
                    f"**Batch:** {batch_num+1}/{total_batches}\n"
                    f"**Remaining:** ~{remaining}s\n"
                    f"**Method:** \u26a1 Bulk Forward"
                )
            except Exception:
                pass

            if batch_num < total_batches - 1:
                await asyncio.sleep(INTER_DELAY)

        # Done — clear pending state
        DRIVE_DATA.pending_import = None
        DRIVE_DATA.isUpdated = True

        # Mirror ALL imported files to backup in one go AFTER import completes
        # This way backup never interferes with main import speed
        try:
            from utils.backup_manager import mirror_batch, is_backup_enabled
            if is_backup_enabled():
                all_imported_ids = [
                    item.file_id
                    for item in DRIVE_DATA.get_directory(destination_folder).contents.values()
                    if item.type == "file" and not getattr(item, "trash", False)
                ]
                if all_imported_ids:
                    asyncio.create_task(mirror_batch(all_imported_ids))
        except Exception as _be:
            logger.error(f"Post-import backup mirror error: {_be}")

        await client.send_message(
            user_chat_id,
            f"\u2705 **Bulk Import Completed!**\n\n"
            f"**Range:** {total_range:,} messages\n"
            f"**Imported:** {imported_count:,}\n"
            f"**Errors:** {error_count:,}\n"
            f"**Destination:** {BOT_MODE.current_folder_name}\n\n"
            f"Files are now available on your TG Drive website! \U0001f389"
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Bulk import failed: {e}\n{tb}")
        # Keep pending_import so we resume on next restart
        try:
            await client.send_message(
                user_chat_id,
                f"\u274c **Bulk Import Crashed**\n\n"
                f"**Error:** {str(e)[:300]}\n\n"
                f"\U0001f504 **Will resume automatically on next restart.**"
            )
        except Exception:
            pass


@main_bot.on_message(
    filters.command("create_folder")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def create_folder_handler(client: Client, message: Message):
    """
    Handles the /create_folder command to create new folders.
    Supports /create_folder <folder_name> for direct creation,
    or falls back to interactive mode if no argument provided.
    """
    global BOT_MODE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the folder name or /cancel.")
        return 

    # Check if current folder is set
    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No current folder set. Please use /set_folder to set a folder first before creating new folders."
        )
        return

    # Extract the argument from the command, if any
    command_args = message.command
    folder_name_from_command = None
    if len(command_args) > 1:
        folder_name_from_command = " ".join(command_args[1:]).strip()

    target_folder_name = None
    if folder_name_from_command:
        # If a folder name was provided in the command, try to create it directly
        logger.info(f"Attempting direct folder creation: '{folder_name_from_command}'")
        
        # Validate folder name
        if not is_valid_folder_name(folder_name_from_command):
            await message.reply_text(
                "❌ **Invalid folder name!**\n\n"
                "Folder names can only contain:\n"
                "• Letters (a-z, A-Z)\n"
                "• Numbers (0-9)\n"
                "• Spaces, hyphens (-), underscores (_)\n"
                "• Brackets [ ] ( )\n"
                "• Some special characters: @ # ! $ % * + = { } : ; < > , . ? / | \\ ~ `"
            )
            return

        # Check if folder already exists
        current_folder_data = DRIVE_DATA.get_directory(BOT_MODE.current_folder)
        for item in current_folder_data.contents.values():
            if item.type == "folder" and item.name.lower() == folder_name_from_command.lower():
                await message.reply_text(
                    f"❌ **Folder already exists!**\n\n"
                    f"A folder named '{folder_name_from_command}' already exists in the current directory.\n\n"
                    f"**Current folder:** {BOT_MODE.current_folder_name}"
                )
                return

        # Create the folder
        try:
            new_folder_path = DRIVE_DATA.new_folder(BOT_MODE.current_folder, folder_name_from_command)
            await message.reply_text(
                f"✅ **Folder created successfully!**\n\n"
                f"**Folder name:** {folder_name_from_command}\n"
                f"**Location:** {BOT_MODE.current_folder_name}\n"
                f"**Full path:** {new_folder_path}"
            )
            logger.info(f"Folder '{folder_name_from_command}' created successfully at {new_folder_path}")
            return
        except Exception as e:
            await message.reply_text(
                f"❌ **Error creating folder!**\n\n"
                f"Failed to create folder '{folder_name_from_command}': {str(e)}"
            )
            logger.error(f"Failed to create folder '{folder_name_from_command}': {e}")
            return

    # If no argument was provided, proceed with the interactive 'ask' process
    while True:
        try:
            folder_name_input_msg = await manual_ask(
                client=client,
                chat_id=message.chat.id,
                text=(
                    f"📁 **Create New Folder**\n\n"
                    f"**Current location:** {BOT_MODE.current_folder_name}\n\n"
                    f"Please send the name for the new folder:\n\n"
                    f"**Rules:**\n"
                    f"• Use only letters, numbers, spaces, and basic symbols\n"
                    f"• Avoid special characters like: < > : \" | ? * \\\n"
                    f"• Maximum 255 characters\n\n"
                    f"Send /cancel to cancel"
                ),
                timeout=60,
                filters=filters.text,
            )
        except asyncio.TimeoutError:
            await message.reply_text("⏰ **Timeout**\n\nFolder creation cancelled. Use /create_folder to try again.")
            return

        if folder_name_input_msg.text.lower() == "/cancel":
            await message.reply_text("❌ **Cancelled**\n\nFolder creation cancelled.")
            return

        target_folder_name = folder_name_input_msg.text.strip()
        
        # Validate folder name
        if not target_folder_name:
            await message.reply_text("❌ **Empty name!** Please provide a valid folder name or /cancel.")
            continue
            
        if not is_valid_folder_name(target_folder_name):
            await message.reply_text(
                "❌ **Invalid folder name!**\n\n"
                "Folder names can only contain:\n"
                "• Letters (a-z, A-Z)\n"
                "• Numbers (0-9)\n"
                "• Spaces, hyphens (-), underscores (_)\n"
                "• Brackets [ ] ( )\n"
                "• Some special characters: @ # ! $ % * + = { } : ; < > , . ? / | \\ ~ `\n\n"
                "Please try again or /cancel."
            )
            continue

        # Check if folder already exists
        current_folder_data = DRIVE_DATA.get_directory(BOT_MODE.current_folder)
        folder_exists = False
        for item in current_folder_data.contents.values():
            if item.type == "folder" and item.name.lower() == target_folder_name.lower():
                folder_exists = True
                break

        if folder_exists:
            await message.reply_text(
                f"❌ **Folder already exists!**\n\n"
                f"A folder named '{target_folder_name}' already exists in the current directory.\n"
                f"Please choose a different name or /cancel."
            )
            continue

        # Create the folder
        try:
            new_folder_path = DRIVE_DATA.new_folder(BOT_MODE.current_folder, target_folder_name)
            await message.reply_text(
                f"✅ **Folder created successfully!**\n\n"
                f"**Folder name:** {target_folder_name}\n"
                f"**Location:** {BOT_MODE.current_folder_name}\n"
                f"**Full path:** {new_folder_path}"
            )
            logger.info(f"Folder '{target_folder_name}' created successfully at {new_folder_path}")
            break
        except Exception as e:
            await message.reply_text(
                f"❌ **Error creating folder!**\n\n"
                f"Failed to create folder '{target_folder_name}': {str(e)}\n\n"
                f"Please try again or /cancel."
            )
            logger.error(f"Failed to create folder '{target_folder_name}': {e}")
            continue


def is_valid_folder_name(name):
    """
    Validate folder name according to common file system restrictions.
    """
    if not name or len(name) > 255:
        return False
    
    # Check for invalid characters (similar to the web interface validation)
    import re
    pattern = r'^[a-zA-Z0-9 \-_\\[\]()@#!$%*+={}:;<>,.?/|\\~`]*$'
    return bool(re.match(pattern, name))


@main_bot.on_message(
    filters.command("set_folder")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def set_folder_handler(client: Client, message: Message):
    """
    Handles the /set_folder command.
    Supports /set_folder <folder_name> for direct setting,
    or falls back to interactive mode if no argument or ambiguity.
    """
    global SET_FOLDER_PATH_CACHE, DRIVE_DATA

    # Check if there's already a pending ask for this chat to prevent re-triggering
    if message.chat.id in _pending_requests:
        await message.reply_text("I'm already waiting for your input. Please provide the folder name or /cancel.")
        return 

    # Extract the argument from the command, if any
    # message.text will be something like "/set_folder grammar"
    # message.command will be ["set_folder", "grammar"]
    command_args = message.command
    folder_name_from_command = None
    if len(command_args) > 1:
        folder_name_from_command = " ".join(command_args[1:]).strip()

    target_folder_name = None
    if folder_name_from_command:
        # If a folder name was provided in the command, try to find it directly
        logger.info(f"Attempting direct set_folder for: '{folder_name_from_command}'")
        search_result = DRIVE_DATA.search_file_folder(folder_name_from_command)
        
        found_folders = {}
        for item in search_result.values():
            if item.type == "folder":
                found_folders[item.id] = item

        if len(found_folders) == 1:
            # Exactly one folder found, set it directly
            folder_id = list(found_folders.keys())[0]
            folder = found_folders[folder_id]
            path_segments = [seg for seg in folder.path.strip("/").split("/") if seg]
            folder_path = "/" + ("/".join(path_segments + [folder.id]))
            
            BOT_MODE.set_folder(folder_path, folder.name)

            # Persist the selected folder to the configuration file
            try:
                with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
                    json.dump({"current_folder": folder_path, "current_folder_name": folder.name}, f)
                logger.info(f"Saved default folder to config: {folder.name} -> {folder_path}")
            except Exception as e:
                logger.error(f"Failed to save default folder config: {e}")

            await message.reply_text(
                f"📁 **Folder Set Successfully!**\n\n"
                f"**Current folder:** {folder.name}\n\n"
                f"Now you can send/forward files to me and they will be uploaded to this folder.\n"
                f"You can also use /create_folder to create new folders in this location."
            )
            return # Exit handler after direct setting

        elif len(found_folders) > 1:
            # Multiple folders found, proceed to interactive selection
            await message.reply_text(f"Multiple folders found with name '{folder_name_from_command}'. Please select one:")
            target_folder_name = folder_name_from_command # Use this for generating buttons below
        else:
            # No folder found with the given name, prompt for interactive input
            await message.reply_text(f"No folder found with name '{folder_name_from_command}'. Please send the exact folder name:")
            target_folder_name = None # Will trigger the manual_ask below


    # If no argument was provided, or if direct setting was ambiguous/failed,
    # proceed with the interactive 'ask' process.
    if target_folder_name is None: # This means we need to ask the user for input
        while True:
            try:
                folder_name_input_msg = await manual_ask(
                    client=client,
                    chat_id=message.chat.id,
                    text="Send the folder name where you want to upload files\n\n/cancel to cancel",
                    timeout=60,
                    filters=filters.text,
                )
            except asyncio.TimeoutError:
                await message.reply_text("Timeout\n\nUse /set_folder to set folder again")
                return

            if folder_name_input_msg.text.lower() == "/cancel":
                await message.reply_text("Cancelled")
                return

            target_folder_name = folder_name_input_msg.text.strip()
            if not target_folder_name: # Handle empty input after ask
                await message.reply_text("Folder name cannot be empty. Please send a valid name or /cancel.")
                continue # Ask again
            
            search_result = DRIVE_DATA.search_file_folder(target_folder_name)
            
            folders = {}
            for item in search_result.values():
                if item.type == "folder":
                    folders[item.id] = item

            if len(folders) == 0:
                await message.reply_text(f"No Folder found with name '{target_folder_name}'")
            else:
                break # Found folders, proceed to show buttons
    else: # If target_folder_name was set due to ambiguity, we re-search with it
          # This branch handles the case where direct command resulted in multiple matches.
        search_result = DRIVE_DATA.search_file_folder(target_folder_name)
        folders = {}
        for item in search_result.values():
            if item.type == "folder":
                folders[item.id] = item

    # Proceed to show inline buttons for selection if interactive mode is needed
    if folders: # Only show buttons if there are folders to select
        buttons = []
        folder_cache = {}
        folder_cache_id = len(SET_FOLDER_PATH_CACHE) + 1

        for folder in folders.values():
            path_segments = [seg for seg in folder.path.strip("/").split("/") if seg]
            folder_path = "/" + ("/".join(path_segments + [folder.id]))
            
            folder_cache[folder.id] = (folder_path, folder.name)
            buttons.append(
                [
                    InlineKeyboardButton(
                        folder.name,
                        callback_data=f"set_folder_{folder_cache_id}_{folder.id}",
                    )
                ]
            )
        SET_FOLDER_PATH_CACHE[folder_cache_id] = folder_cache

        await message.reply_text(
            "Select the folder where you want to upload files",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    else:
        # This case should ideally be caught by len(folders) == 0 check earlier,
        # but as a safeguard if interactive input also yields no results.
        await message.reply_text(f"No folders found for '{target_folder_name}' after search. Please try /set_folder again.")


@main_bot.on_callback_query(
    filters.user(config.TELEGRAM_ADMIN_IDS) & filters.regex(r"set_folder_")
)
async def set_folder_callback(client: Client, callback_query: CallbackQuery):
    """
    Handles the callback query when a user selects a folder from the inline buttons.
    Sets the selected folder as the current default and saves it to a config file.
    """
    global SET_FOLDER_PATH_CACHE, BOT_MODE

    folder_cache_id_str, folder_id = callback_query.data.split("_")[2:]
    folder_cache_id = int(folder_cache_id_str)

    folder_path_cache = SET_FOLDER_PATH_CACHE.get(folder_cache_id)
    if folder_path_cache is None:
        await callback_query.answer("Request Expired, Send /set_folder again")
        await callback_query.message.delete()
        return

    folder_path, name = folder_path_cache.get(folder_id)
    if folder_path is None:
        await callback_query.answer("Selected folder not found in cache. Please try again.")
        await callback_query.message.delete()
        return

    del SET_FOLDER_PATH_CACHE[folder_cache_id]

    BOT_MODE.set_folder(folder_path, name)

    try:
        with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
            json.dump({"current_folder": folder_path, "current_folder_name": name}, f)
        logger.info(f"Saved default folder to config: {name} -> {folder_path}")
    except Exception as e:
        logger.error(f"Failed to save default folder config: {e}")

    await callback_query.answer(f"Folder Set Successfully To : {name}")
    await callback_query.message.edit(
        f"📁 **Folder Set Successfully!**\n\n"
        f"**Current folder:** {name}\n\n"
        f"Now you can send/forward files to me and they will be uploaded to this folder.\n"
        f"You can also use /create_folder to create new folders in this location."
    )


@main_bot.on_message(
    filters.command("current_folder")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def current_folder_handler(client: Client, message: Message):
    """
    Handles the /current_folder command, displaying the currently set default folder.
    """
    global BOT_MODE

    if BOT_MODE.current_folder:
        await message.reply_text(
            f"📁 **Current Folder Information**\n\n"
            f"**Folder:** {BOT_MODE.current_folder_name}\n"
            f"**Path:** {BOT_MODE.current_folder}\n\n"
            f"💡 **Available commands:**\n"
            f"• Send files to upload them here\n"
            f"• /create_folder - Create new folders\n"
            f"• /set_folder - Change current folder\n"
            f"• /bulk_import - Import files in bulk\n"
            f"• /fast_import - Import files directly (fast)"
        )
    else:
        await message.reply_text(
            f"❌ **No current folder set**\n\n"
            f"Use /set_folder to set a folder first.\n\n"
            f"💡 **Available commands:**\n"
            f"• /set_folder - Set current folder\n"
            f"• /help - Show all commands"
        )


@main_bot.on_message(
    filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS)
    & (
        filters.document
        | filters.video
        | filters.audio
        | filters.photo
        | filters.sticker
    )
)
async def file_handler(client: Client, message: Message):
    """
    Handles incoming file messages (documents, videos, audio, photos, stickers).
    Uploads the file to the currently set default folder.
    """
    global BOT_MODE, DRIVE_DATA

    # Ensure there's no pending ask request for this chat before processing files
    # This prevents file uploads from interfering with an active /set_folder conversation
    if message.chat.id in _pending_requests:
        logger.debug(f"Ignoring file from {message.chat.id} due to pending ask request.")
        return # Do not process file if waiting for text input

    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **Error:** No default folder set.\n\n"
            "Please use /set_folder to set one before uploading files.\n\n"
            "💡 **Quick start:**\n"
            "1. Use /set_folder to choose a folder\n"
            "2. Send files to upload them\n"
            "3. Use /create_folder to create new folders\n"
            "4. Use /bulk_import to import files in bulk\n"
            "5. Use /fast_import to import files directly (fast)"
        )
        return

    try:
        copied_message = await message.copy(config.STORAGE_CHANNEL)
        file = (
            copied_message.document
            or copied_message.video
            or copied_message.audio
            or copied_message.photo
            or copied_message.sticker
        )

        DRIVE_DATA.new_file(
            BOT_MODE.current_folder,
            file.file_name,
            copied_message.id,
            file.file_size,
        )

        await message.reply_text(
            f"""✅ **File Uploaded Successfully!**

**File Name:** {file.file_name}
**Folder:** {BOT_MODE.current_folder_name}
**Size:** {file.file_size / (1024*1024):.2f} MB

💡 **What's next?**
• Send more files to upload them
• Use /create_folder to create new folders
• Use /set_folder to change location
• Use /bulk_import to import files in bulk
• Use /fast_import to import files directly (fast)
"""
        )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        await message.reply_text(
            f"❌ **Error uploading file**\n\n"
            f"Failed to upload the file to the storage channel.\n\n"
            f"**Error:** {str(e)}\n\n"
            f"Please try again or contact support if the issue persists."
        )

# --- GENERIC MESSAGE HANDLER (Lowest Priority) ---
# This handler MUST be defined AFTER all specific command and file handlers.
@main_bot.on_message(
    filters.command("send_to")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def send_to_handler(client: Client, message: Message):
    """
    /send_to — Forward all files from a drive folder to a Telegram channel/group.

    Usage:
      /send_to                     → uses current folder, asks for destination
      /send_to @channel            → uses current folder, sends to @channel
      /send_to @channel /FOLDER_PATH → sends specific folder to @channel
    """
    global DRIVE_DATA, BOT_MODE

    args = message.text.split()[1:]  # everything after /send_to

    # Parse arguments
    destination = None
    folder_path = None

    for arg in args:
        if arg.startswith("@") or arg.lstrip("-").isdigit():
            destination = arg
        elif arg.startswith("/"):
            folder_path = arg

    # Use current folder if none specified
    if not folder_path:
        folder_path = BOT_MODE.current_folder
        folder_name = BOT_MODE.current_folder_name
    else:
        try:
            folder_data = DRIVE_DATA.get_directory(folder_path)
            folder_name = getattr(folder_data, "name", folder_path)
        except Exception:
            await message.reply_text(f"❌ Folder `{folder_path}` not found.")
            return

    # Ask for destination if not provided
    if not destination:
        try:
            dest_msg = await manual_ask(
                client=client,
                chat_id=message.chat.id,
                text=(
                    f"📤 **Send Folder to Channel/Group**\n\n"
                    f"**Folder:** {folder_name}\n"
                    f"**Path:** `{folder_path}`\n\n"
                    f"Send the destination channel username or ID\n"
                    f"Examples: `@mychannel` or `-1001234567890`"
                ),
                timeout=60,
                filters=filters.text,
            )
        except asyncio.TimeoutError:
            await message.reply_text("⏰ Timeout. Cancelled.")
            return

        if dest_msg.text.lower() == "/cancel":
            await message.reply_text("❌ Cancelled.")
            return

        destination = dest_msg.text.strip()

    # Validate destination
    try:
        dest_chat = await client.get_chat(destination)
        dest_id   = dest_chat.id
        dest_name = getattr(dest_chat, "title", destination)
    except Exception as e:
        await message.reply_text(f"❌ Cannot access `{destination}`\n\n**Error:** {str(e)}")
        return

    # Get all files from the folder
    try:
        folder_data = DRIVE_DATA.get_directory(folder_path)
        files = [
            item for item in folder_data.contents.values()
            if item.type == "file" and not getattr(item, "trash", False)
        ]
    except Exception as e:
        await message.reply_text(f"❌ Error reading folder: {str(e)}")
        return

    if not files:
        await message.reply_text(f"⚠️ No files found in **{folder_name}**.")
        return

    total = len(files)
    FORWARD_BATCH = 100
    INTER_DELAY   = 2.5

    # Get Telegram message IDs (f.file_id), not drive IDs (f.id which is a hash string)
    msg_ids = [f.file_id for f in files]
    source_channel = config.STORAGE_CHANNEL  # default source

    # Check if all files are from the same source (fast import case)
    fast_files = [f for f in files if getattr(f, "is_fast_import", False) and f.source_channel]
    if len(fast_files) == len(files):
        # All fast-import from same channel
        channels = set(f.source_channel for f in fast_files)
        if len(channels) == 1:
            source_channel = fast_files[0].source_channel

    batches = [msg_ids[i:i+FORWARD_BATCH] for i in range(0, len(msg_ids), FORWARD_BATCH)]
    total_batches = len(batches)
    eta_s = int(total_batches * INTER_DELAY)

    status_msg = await message.reply_text(
        f"📤 **Sending {total:,} files to {dest_name}**\n\n"
        f"**Source folder:** {folder_name}\n"
        f"**Destination:** {dest_name}\n"
        f"**Batches:** {total_batches} × {FORWARD_BATCH}\n"
        f"**Est. time:** ~{eta_s}s\n"
        f"**Method:** ⚡ Bulk Forward (server-side)"
    )

    sent_count  = 0
    error_count = 0

    for batch_num, batch_ids in enumerate(batches):
        for attempt in range(5):
            try:
                forwarded = await client.forward_messages(
                    chat_id=dest_id,
                    from_chat_id=source_channel,
                    message_ids=batch_ids,
                    hide_sender_name=True,
                    disable_notification=True,
                )
                if not isinstance(forwarded, list):
                    forwarded = [forwarded] if forwarded else []
                sent_count += len([f for f in forwarded if f])
                break
            except Exception as e:
                err = str(e)
                if "FLOOD_WAIT" in err:
                    wait = 35
                    try: wait = min(int(err.split("_")[-1]), 35)
                    except Exception: pass
                    await asyncio.sleep(wait)
                elif attempt < 4:
                    await asyncio.sleep(2 ** attempt)
                else:
                    error_count += len(batch_ids)
                    logger.error(f"send_to batch {batch_num+1} failed: {e}")

        pct = int((batch_num + 1) / total_batches * 100)
        try:
            await status_msg.edit_text(
                f"📤 **Sending: {pct}%**\n\n"
                f"**Sent:** {sent_count:,}/{total:,}\n"
                f"**Batch:** {batch_num+1}/{total_batches}\n"
                f"**Destination:** {dest_name}"
            )
        except Exception:
            pass

        if batch_num < total_batches - 1:
            await asyncio.sleep(INTER_DELAY)

    await client.send_message(
        message.chat.id,
        f"✅ **Send Complete!**\n\n"
        f"**Folder:** {folder_name}\n"
        f"**Destination:** {dest_name}\n"
        f"**Sent:** {sent_count:,}\n"
        f"**Errors:** {error_count}\n\n"
        f"All files forwarded to {dest_name}! 🎉"
    )


@main_bot.on_message(
    filters.command("send_file")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def send_file_handler(client: Client, message: Message):
    """
    /send_file FILE_ID @destination
    Send a single file from storage to a channel/group.
    """
    args = message.text.split()[1:]
    if len(args) < 2:
        await message.reply_text(
            "**Usage:** `/send_file FILE_MESSAGE_ID @destination`\n\n"
            "Example: `/send_file 12345 @mychannel`"
        )
        return

    try:
        file_msg_id = int(args[0])
        destination = args[1]
    except (ValueError, IndexError):
        await message.reply_text("❌ Invalid arguments. Usage: `/send_file FILE_ID @destination`")
        return

    try:
        dest_chat = await client.get_chat(destination)
    except Exception as e:
        await message.reply_text(f"❌ Cannot access `{destination}`: {str(e)}")
        return

    try:
        await client.forward_messages(
            chat_id=dest_chat.id,
            from_chat_id=config.STORAGE_CHANNEL,
            message_ids=[file_msg_id],
            hide_sender_name=True,
            disable_notification=True,
        )
        await message.reply_text(f"✅ File sent to **{dest_chat.title or destination}**!")
    except Exception as e:
        await message.reply_text(f"❌ Failed to send: {str(e)}")




# ── Share link helpers ────────────────────────────────────────────────────────

def encode_share_payload(key: str) -> str:
    """Encode share link payload — just a short key pointing to DRIVE_DATA.share_links."""
    data = {"k": key}
    raw  = json.dumps(data, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_share_payload(payload: str) -> dict:
    """Decode a share link payload. Returns dict or raises ValueError."""
    padding = 4 - len(payload) % 4
    if padding != 4:
        payload += "=" * padding
    try:
        raw  = base64.urlsafe_b64decode(payload).decode()
        data = json.loads(raw)
        return data
    except Exception as e:
        raise ValueError(f"Invalid share link: {e}")


def create_share_link_entry(file_ids: list, source_channel: int, title: str) -> str:
    """Store share data in DRIVE_DATA and return the key."""
    key = secrets.token_hex(6)  # 12 char unique key
    if not hasattr(DRIVE_DATA, "share_links") or DRIVE_DATA.share_links is None:
        DRIVE_DATA.share_links = {}
    DRIVE_DATA.share_links[key] = {
        "file_ids":       file_ids,
        "source_channel": source_channel,
        "title":          title,
    }
    DRIVE_DATA.isUpdated = True  # background task will persist this
    return key


def get_share_link_entry(key: str) -> dict:
    """Retrieve share data from DRIVE_DATA by key. Returns None if not found."""
    if not hasattr(DRIVE_DATA, "share_links") or not DRIVE_DATA.share_links:
        return None
    return DRIVE_DATA.share_links.get(key)


async def handle_share_link(client: Client, message: Message, payload: str):
    """
    Called when someone opens the bot via a share link.
    Looks up the key in DRIVE_DATA.share_links, then asks where to send.
    """
    try:
        data = decode_share_payload(payload)
    except ValueError as e:
        await message.reply_text(f"❌ Invalid share link.\n{e}")
        return

    key = data.get("k")
    if not key:
        await message.reply_text("❌ Invalid share link format.")
        return

    entry = get_share_link_entry(key)
    if not entry:
        await message.reply_text(
            "❌ Share link not found or expired.\n\n"
            "The link may have been generated on a different server instance.\n"
            "Please ask the sender to generate a new link."
        )
        return

    file_ids      = entry["file_ids"]
    source_channel = entry["source_channel"]
    title         = entry.get("title", "Shared Files")
    total         = len(file_ids)

    # Store key in callback token store — 64 byte limit safe (key is 12 chars)
    token = secrets.token_hex(4)
    _PAYLOAD_STORE[token] = key  # token → share key

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Send here (this chat)", callback_data=f"share_here|{token}")],
        [InlineKeyboardButton("📢 Send to channel/group", callback_data=f"share_channel|{token}")],
    ])

    await message.reply_text(
        f"📦 **Shared Files: {title}**\n\n"
        f"**Total files:** {total:,}\n\n"
        f"Where do you want to receive these files?",
        reply_markup=keyboard
    )


@main_bot.on_callback_query(filters.regex(r"^share_"))
async def share_link_callback(client: Client, callback_query: CallbackQuery):
    """Handle share link destination choice."""
    data    = callback_query.data
    action, token = data.split("|", 1)
    user_id = callback_query.from_user.id

    # Look up share key from token store
    share_key = _PAYLOAD_STORE.get(token)
    if not share_key:
        await callback_query.answer("❌ Link expired. Please click the share link again.", show_alert=True)
        return

    entry = get_share_link_entry(share_key)
    if not entry:
        await callback_query.answer("❌ Share link data not found. Please generate a new link.", show_alert=True)
        return

    file_ids       = entry["file_ids"]
    source_channel = entry["source_channel"]
    title          = entry.get("title", "Shared Files")

    await callback_query.message.edit_reply_markup(reply_markup=None)

    if action == "share_here":
        _PAYLOAD_STORE.pop(token, None)
        dest_id   = callback_query.message.chat.id
        dest_name = "this chat"
        await _do_share_send(client, callback_query.message, file_ids, source_channel, dest_id, dest_name, title)

    elif action == "share_channel":
        await callback_query.message.reply_text(
            "📢 **Send to Channel/Group**\n\n"
            "Send the channel or group username or ID.\n"
            "**Note:** The bot must be admin there.\n\n"
            "Example: `@mychannel` or `-1001234567890`"
        )
        _PENDING_SHARES[user_id] = (file_ids, source_channel, title, callback_query.message, token)

    await callback_query.answer()


# Pending share destinations keyed by user_id
_PENDING_SHARES: dict = {}

# Short-lived payload store to keep callback_data under Telegram's 64-byte limit
# Key: 8-char token, Value: full payload string
_PAYLOAD_STORE: dict = {}


async def _do_share_send(client, status_target, file_ids: list, source_channel_id: int,
                         dest_id: int, dest_name: str, title: str):
    """Forward specific file_ids from source_channel to dest_id."""
    FORWARD_BATCH = 100
    INTER_DELAY   = 2.5

    batches       = [file_ids[i:i+FORWARD_BATCH] for i in range(0, len(file_ids), FORWARD_BATCH)]
    total_batches = len(batches)
    total         = len(file_ids)
    eta_s         = int(total_batches * INTER_DELAY)

    status_msg = await status_target.reply_text(
        f"⚡ **Sending {total:,} files to {dest_name}**\n\n"
        f"**Title:** {title}\n"
        f"**Batches:** {total_batches} × {FORWARD_BATCH}\n"
        f"**Est. time:** ~{eta_s}s\n"
        f"**Method:** Server-side forward (no re-upload)"
    )

    sent_count  = 0
    error_count = 0

    for batch_num, batch_ids in enumerate(batches):
        for attempt in range(5):
            try:
                forwarded = await client.forward_messages(
                    chat_id=dest_id,
                    from_chat_id=source_channel_id,
                    message_ids=batch_ids,
                    hide_sender_name=True,
                    disable_notification=True,
                )
                if not isinstance(forwarded, list):
                    forwarded = [forwarded] if forwarded else []
                sent_count += len([f for f in forwarded if f])
                break
            except Exception as e:
                err = str(e)
                if "FLOOD_WAIT" in err:
                    wait = 35
                    try: wait = min(int(err.split("_")[-1]), 35)
                    except Exception: pass
                    await asyncio.sleep(wait)
                elif "CHAT_SEND_MEDIA_FORBIDDEN" in err or "CHAT_WRITE_FORBIDDEN" in err:
                    await status_msg.edit_text(
                        f"❌ **Bot is not admin in {dest_name}**\n\n"
                        f"Please add the bot as admin to the channel/group first, then try again."
                    )
                    return
                elif attempt < 4:
                    await asyncio.sleep(2 ** attempt)
                else:
                    error_count += len(batch_ids)

        pct = int((batch_num + 1) / total_batches * 100)
        try:
            await status_msg.edit_text(
                f"📊 **Sending: {pct}%**\n\n"
                f"**Sent:** {sent_count:,}\n"
                f"**Batch:** {batch_num+1}/{total_batches}\n"
                f"**Destination:** {dest_name}"
            )
        except Exception:
            pass

        if batch_num < total_batches - 1:
            await asyncio.sleep(INTER_DELAY)

    await status_msg.edit_text(
        f"✅ **Done!**\n\n"
        f"**Title:** {title}\n"
        f"**Destination:** {dest_name}\n"
        f"**Sent:** {sent_count:,}\n"
        f"**Errors:** {error_count}"
    )


@main_bot.on_message(
    filters.command("generate_link")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def generate_link_handler(client: Client, message: Message):
    """
    /generate_link — Generate a shareable link for files in current folder.

    Usage:
      /generate_link                    → generates link for current folder
      /generate_link t.me/CHANNEL/3 t.me/CHANNEL/529  → custom range
    """
    global DRIVE_DATA, BOT_MODE

    try:
        await _generate_link_impl(client, message)
    except Exception as e:
        import traceback as _tb
        logger.error(f"generate_link error: {e}\n{_tb.format_exc()}")
        await message.reply_text(f"❌ Error: {str(e)}")


async def _generate_link_impl(client, message):
    global DRIVE_DATA, BOT_MODE

    FORWARD_BATCH = 100
    INTER_DELAY   = 2.5

    parts = message.text.split()[1:]

    bot_info     = await client.get_me()
    bot_username = bot_info.username

    # ── Case 1: Custom range from two t.me links ──────────────────────────────
    if len(parts) >= 2:
        link1 = parse_telegram_link(parts[0])
        link2 = parse_telegram_link(parts[1])
        if not link1 or not link2:
            await message.reply_text(
                "❌ Invalid links. Use format:\n"
                "`/generate_link https://t.me/CHANNEL/3 https://t.me/CHANNEL/529`"
            )
            return
        if link1["channel"] != link2["channel"]:
            await message.reply_text("❌ Both links must be from the same channel.")
            return
        try:
            ch = await client.get_chat(link1["channel"])
            source_channel_id = ch.id
        except Exception as e:
            await message.reply_text(f"❌ Cannot access channel: {e}")
            return

        orig_start = min(link1["message_id"], link2["message_id"])
        orig_end   = max(link1["message_id"], link2["message_id"])
        title      = getattr(ch, "title", link1["channel"])
        total_range = orig_end - orig_start + 1

        # Copy files to OUR storage channel so the link works forever
        status_msg = await message.reply_text(
            f"⚡ **Copying {total_range:,} messages to your storage...**\n\n"
            f"**Source:** {title}\n"
            f"**Range:** {orig_start:,} → {orig_end:,}\n"
            f"**This makes the link permanent** — works even if source channel is deleted.\n\n"
            f"Please wait..."
        )

        all_ids       = list(range(orig_start, orig_end + 1))
        batches       = [all_ids[i:i+FORWARD_BATCH] for i in range(0, len(all_ids), FORWARD_BATCH)]
        total_batches = len(batches)
        copied_ids    = []   # message IDs in OUR storage channel
        error_count   = 0

        for batch_num, batch_ids in enumerate(batches):
            for attempt in range(5):
                try:
                    forwarded = await client.forward_messages(
                        chat_id=config.STORAGE_CHANNEL,
                        from_chat_id=source_channel_id,
                        message_ids=batch_ids,
                        hide_sender_name=True,
                        disable_notification=True,
                    )
                    if not isinstance(forwarded, list):
                        forwarded = [forwarded] if forwarded else []
                    for fwd in forwarded:
                        if fwd:
                            fm = fwd.document or fwd.video or fwd.audio or fwd.photo or fwd.sticker
                            if fm:
                                fname = getattr(fm, "file_name", None) or f"file_{fwd.id}"
                                fsize = getattr(fm, "file_size", 0) or 0
                                fdur  = getattr(fm, "duration", 0) if hasattr(fm, "duration") else 0
                                DRIVE_DATA.register_file(BOT_MODE.current_folder, fname, fwd.id, fsize, fdur)
                                copied_ids.append(fwd.id)
                    break
                except Exception as e:
                    err = str(e)
                    if "FLOOD_WAIT" in err:
                        wait = 35
                        try: wait = min(int(err.split("_")[-1]), 35)
                        except Exception: pass
                        await asyncio.sleep(wait)
                    elif attempt < 4:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        error_count += len(batch_ids)

            pct = int((batch_num + 1) / total_batches * 100)
            try:
                await status_msg.edit_text(
                    f"📋 **Copying to storage: {pct}%**\n\n"
                    f"**Copied:** {len(copied_ids):,}\n"
                    f"**Batch:** {batch_num+1}/{total_batches}\n"
                    f"**Errors:** {error_count}"
                )
            except Exception:
                pass

            if batch_num < total_batches - 1:
                await asyncio.sleep(INTER_DELAY)

        # Save all registered files in one write
        DRIVE_DATA.save()

        if not copied_ids:
            await status_msg.edit_text("❌ No files could be copied. Check that the source channel is accessible.")
            return

        # Store file_ids in DRIVE_DATA — permanent, survives restarts
        share_key = create_share_link_entry(copied_ids, config.STORAGE_CHANNEL, title)
        payload   = encode_share_payload(share_key)
        share_url = f"https://t.me/{bot_username}?start={payload}"

        await status_msg.edit_text(
            f"🔗 **Permanent Share Link Generated!**\n\n"
            f"**Title:** {title}\n"
            f"**Files copied:** {len(copied_ids):,} (errors: {error_count})\n"
            f"**Stored in:** Your storage channel ✅\n"
            f"**Link works even if source is deleted** ✅\n\n"
            f"**Share Link:**\n`{share_url}`\n\n"
            f"Anyone who clicks this link can receive these files."
        )

    # ── Case 2: Current folder — generate link from actual file locations ───────
    else:
        folder_path = BOT_MODE.current_folder
        try:
            folder_data = DRIVE_DATA.get_directory(folder_path)
            files = [
                item for item in folder_data.contents.values()
                if item.type == "file" and not getattr(item, "trash", False)
            ]
        except Exception as e:
            await message.reply_text(f"❌ Error reading folder: {e}")
            return

        if not files:
            await message.reply_text("⚠️ No files in current folder.")
            return

        title = BOT_MODE.current_folder_name

        # Check if files are fast-import (point to source channel) or regular (storage channel)
        fast_files = [f for f in files if getattr(f, "is_fast_import", False) and getattr(f, "source_channel", None)]
        regular_files = [f for f in files if not getattr(f, "is_fast_import", False)]

        if fast_files and not regular_files:
            # All fast-import — all point to same source channel
            channels = set(f.source_channel for f in fast_files)
            if len(channels) == 1:
                # Single source — link works but depends on that channel staying public
                src_channel = fast_files[0].source_channel
                file_ids = [f.file_id for f in fast_files]
                start_id = min(file_ids)
                end_id   = max(file_ids)
                file_ids_list = [f.file_id for f in fast_files]
                share_key = create_share_link_entry(file_ids_list, src_channel, title)
                payload   = encode_share_payload(share_key)
                share_url = f"https://t.me/{bot_username}?start={payload}"
                await message.reply_text(
                    f"🔗 **Share Link Generated!**\n\n"
                    f"**Title:** {title}\n"
                    f"**Files:** {len(files):,}\n"
                    f"⚠️ **Fast-import files** — link depends on source channel staying accessible.\n"
                    f"Use `/generate_link` with message range to make a permanent copy.\n\n"
                    f"**Share Link:**\n`{share_url}`\n\n"
                    f"Anyone who clicks this link can receive these files."
                )
                return
            else:
                await message.reply_text(
                    "⚠️ Files in this folder point to multiple source channels.\n"
                    "Please use `/generate_link t.me/CHANNEL/START t.me/CHANNEL/END` for a specific range."
                )
                return

        # Regular files — stored in your storage channel, permanent
        file_ids_list = [f.file_id for f in (regular_files if regular_files else files)]
        share_key = create_share_link_entry(file_ids_list, config.STORAGE_CHANNEL, title)
        payload   = encode_share_payload(share_key)
        share_url = f"https://t.me/{bot_username}?start={payload}"

        await message.reply_text(
            f"🔗 **Share Link Generated!**\n\n"
            f"**Title:** {title}\n"
            f"**Files:** {len(files):,}\n"
            f"**Stored in:** Your storage channel ✅\n"
            f"**Link is permanent** ✅\n\n"
            f"**Share Link:**\n`{share_url}`\n\n"
            f"Anyone who clicks this link can receive these files."
        )


@main_bot.on_message(filters.private & filters.user(config.TELEGRAM_ADMIN_IDS) & filters.text)
async def _handle_all_messages(client: Client, message: Message):
    """
    This handler listens for all private text messages from authorized users.
    If a pending 'ask' request exists for this chat, it fulfills it and
    then explicitly returns to prevent further handler processing for this message.
    This handler is placed last to give precedence to specific command handlers.
    """
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None

    # Check pending share link destination first (anyone waiting for channel input)
    if user_id and user_id in _PENDING_SHARES:
        pending = _PENDING_SHARES.pop(user_id)
        file_ids, source_channel, title, orig_msg = pending[:4]
        token = pending[4] if len(pending) > 4 else None
        _PAYLOAD_STORE.pop(token, None)  # clean up token
        destination = message.text.strip()
        try:
            dest_chat = await client.get_chat(destination)
            dest_id   = dest_chat.id
            dest_name = getattr(dest_chat, "title", destination)
            await _do_share_send(client, message, file_ids, source_channel,
                                 dest_id, dest_name, title)
        except Exception as e:
            await message.reply_text(f"❌ Cannot access `{destination}`\n\n**Error:** {str(e)}")
        return

    if chat_id in _pending_requests:
        queue, event, msg_filters = _pending_requests[chat_id]

        if msg_filters is None or msg_filters(None, message): 
            await queue.put(message)
            event.set() # Signal that a response has been received
            return # CRITICAL: Stop processing this message, it's been handled for 'ask'
        else:
            logger.debug(f"Message from {chat_id} did not match pending ask filter. Allowing other handlers.")


async def start_bot_mode(d, b):
    """
    Initializes the bot mode, starts the main bot client, and sets the initial
    default folder based on saved configuration or searches for any available folder.
    """
    global DRIVE_DATA, BOT_MODE
    DRIVE_DATA = d
    BOT_MODE = b

    if DRIVE_DATA is None:
        logger.error("DRIVE_DATA is None, cannot start bot mode")
        raise Exception("Drive data not initialized")

    logger.info("Starting Main Bot")
    await main_bot.start()

    default_folder_path = None
    default_folder_name_to_use = None

    # Try to load previously saved folder configuration
    if DEFAULT_FOLDER_CONFIG_FILE.exists():
        try:
            with open(DEFAULT_FOLDER_CONFIG_FILE, "r") as f:
                config_data = json.load(f)
                default_folder_path = config_data.get("current_folder")
                default_folder_name_to_use = config_data.get("current_folder_name")

            if default_folder_path and default_folder_name_to_use:
                # Validate that the folder still exists in drive data
                try:
                    folder_exists = DRIVE_DATA.get_directory(default_folder_path)
                    if folder_exists:
                        BOT_MODE.set_folder(default_folder_path, default_folder_name_to_use)
                        logger.info(f"Loaded default folder from config: {default_folder_name_to_use} -> {default_folder_path}")
                    else:
                        logger.warning(f"Previously configured folder no longer exists. Will search for a new default.")
                        default_folder_path = None
                        default_folder_name_to_use = None
                except Exception as e:
                    logger.warning(f"Error validating folder from config: {e}. Will search for a new default.")
                    default_folder_path = None
                    default_folder_name_to_use = None
            else:
                logger.warning("Default folder config file found but data is incomplete.")
                default_folder_path = None
                default_folder_name_to_use = None
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error reading default folder config file: {e}.")
            default_folder_path = None
            default_folder_name_to_use = None

    # If no valid folder from config, search for any available folder
    if not (default_folder_path and default_folder_name_to_use):
        logger.info("Searching for available folders in drive data...")
        all_folders = []

        try:
            # Get root directory
            root_dir = DRIVE_DATA.get_directory("/")

            # Recursively collect all folders
            def collect_folders(folder):
                for item in folder.contents.values():
                    if item.type == "folder" and not item.trash:
                        all_folders.append(item)
                        # Recursively search subfolders
                        collect_folders(item)

            collect_folders(root_dir)

            if all_folders:
                # Use the first available folder
                first_folder = all_folders[0]
                path_segments = [seg for seg in first_folder.path.strip("/").split("/") if seg]
                folder_path = "/" + ("/".join(path_segments + [first_folder.id]))

                BOT_MODE.set_folder(folder_path, first_folder.name)
                logger.info(f"Default folder set to: {first_folder.name} -> {folder_path}")

                # Save to config for next time
                try:
                    with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
                        json.dump({"current_folder": folder_path, "current_folder_name": first_folder.name}, f)
                    logger.info(f"Saved initial default folder to config.")
                except Exception as e:
                    logger.error(f"Failed to save initial default folder config: {e}")

                message_to_send = f"Main Bot Started -> TG Drive's Bot Mode Enabled with default folder: {first_folder.name}"
            else:
                logger.warning(f"No folders found in drive. Setting root as default.")
                BOT_MODE.set_folder("/", "/ (root directory)")

                # Save root as default
                try:
                    with open(DEFAULT_FOLDER_CONFIG_FILE, "w") as f:
                        json.dump({"current_folder": "/", "current_folder_name": "/ (root directory)"}, f)
                    logger.info(f"Saved root directory as default folder to config.")
                except Exception as e:
                    logger.error(f"Failed to save default folder config: {e}")

                message_to_send = "Main Bot Started -> TG Drive's Bot Mode Enabled. Using root directory. Use /set_folder to choose a different folder."
        except Exception as e:
            logger.error(f"Error searching for folders: {e}")
            # Fallback to root directory
            BOT_MODE.set_folder("/", "/ (root directory)")
            message_to_send = "Main Bot Started -> TG Drive's Bot Mode Enabled. Using root directory due to error finding folders."
    else:
        message_to_send = f"Main Bot Started -> TG Drive's Bot Mode Enabled with previously set folder: {default_folder_name_to_use}"

    await main_bot.send_message(
        config.STORAGE_CHANNEL,
        message_to_send,
    )
    logger.info(message_to_send)

    # Resume any interrupted bulk import — state lives in DRIVE_DATA which is
    # already loaded from Telegram backup before this function runs
    try:
        state = getattr(DRIVE_DATA, "pending_import", None)
        if state:
            resume_from   = state.get("resume_from", 0)
            total_batches = state.get("total_batches", 1)
            if resume_from < total_batches:
                logger.info(f"Resuming interrupted import from batch {resume_from+1}/{total_batches}")
                _resume_task = asyncio.create_task(
                    bulk_import_files(
                        client=main_bot,
                        user_chat_id=state["user_chat_id"],
                        channel_name=state["channel_name"],
                        start_id=state["start_id"],
                        end_id=state["end_id"],
                        destination_folder=state["destination"],
                        resume_from_batch=resume_from,
                    )
                )
                _BULK_IMPORT_TASKS.add(_resume_task)
                _resume_task.add_done_callback(_BULK_IMPORT_TASKS.discard)
            else:
                DRIVE_DATA.pending_import = None
                DRIVE_DATA.save()
    except Exception as e:
        logger.error(f"Failed to check/resume pending import: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# /restricted_import — handle restricted-content downloads via the bot
# ═══════════════════════════════════════════════════════════════════════════════
@main_bot.on_message(
    filters.command("restricted_import")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def restricted_import_handler(client: Client, message: Message):
    """Import restricted content via the bot: paste links (multi-line)."""
    global BOT_MODE

    if message.chat.id in _pending_requests:
        await message.reply_text(
            "I'm already waiting for your input. Please respond or /cancel."
        )
        return

    if not BOT_MODE.current_folder:
        await message.reply_text(
            "❌ **No current folder set.** Use /set_folder first."
        )
        return

    await message.reply_text(
        "🔒 **Restricted Content Import**\n\n"
        "Send all your Telegram links — one per line.\n"
        "Each line can be:\n"
        "  • A single link\n"
        "  • A range: `link1 - link2`\n"
        "  • A topic link\n\n"
        "**Examples:**\n"
        "`https://t.me/channel/123`\n"
        "`https://t.me/c/1234567890/100 - https://t.me/c/1234567890/200`\n\n"
        "Send /cancel anytime."
    )

    try:
        links_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text="📎 **Paste your links now** (one per line):",
            timeout=600,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ Timeout. Use /restricted_import to try again.")
        return

    if links_msg.text.lower().strip() == "/cancel":
        await message.reply_text("❌ Cancelled.")
        return

    links_text = links_msg.text.strip()

    # Parse
    from utils.restricted_import import (
        parse_input_lines, RESTRICTED_IMPORT_MANAGER, RESTRICTED_PROGRESS,
    )
    from utils.clients import get_client
    import secrets as _secrets

    try:
        jobs = parse_input_lines(links_text)
    except Exception as e:
        await message.reply_text(f"❌ Could not parse links: {e}")
        return

    if not jobs:
        await message.reply_text("❌ No valid links found.")
        return

    try:
        user_client = get_client(premium_required=True)
    except Exception:
        await message.reply_text(
            "❌ **No user account configured.**\n\n"
            "Restricted import needs a logged-in user session that is a member "
            "of the source channel. Ask the admin to set up `PREMIUM_ACCOUNTS`."
        )
        return

    bot_client = client  # current bot uploads to STORAGE_CHANNEL
    destination_folder = BOT_MODE.current_folder
    import_id = _secrets.token_hex(8)

    total = sum(end - start + 1 for _, start, end, _ in jobs)
    status_msg = await message.reply_text(
        f"🔒 **Restricted Import Started**\n\n"
        f"📦 Jobs: **{len(jobs)}** | Total messages: **{total}**\n"
        f"🆔 ID: `{import_id}`\n\n"
        f"Status updates every 5s..."
    )

    async def _runner():
        try:
            await RESTRICTED_IMPORT_MANAGER.run(
                user_client, bot_client, jobs, destination_folder, import_id,
            )
        except Exception as e:
            logger.error(f"Bot restricted import error: {e}")

    asyncio.create_task(_runner())

    # Progress poller
    async def _poll():
        last_text = ""
        while True:
            await asyncio.sleep(5)
            prog = RESTRICTED_PROGRESS.get(import_id)
            if not prog:
                continue
            status = prog.get("status", "?")
            txt = (
                f"🔒 **Restricted Import**\n\n"
                f"Status: `{status}`\n"
                f"Job: {prog.get('current_job', 0)}/{prog.get('total_jobs', 0)}\n"
                f"Imported: **{prog.get('imported', 0)}** | "
                f"Errors: **{prog.get('errors', 0)}** | "
                f"Skipped: **{prog.get('skipped', 0)}**\n"
                f"📄 Current: `{(prog.get('current_file') or '...')[:40]}`"
            )
            if txt != last_text:
                try:
                    await status_msg.edit_text(txt)
                    last_text = txt
                except Exception:
                    pass
            if status in ("done", "error", "cancelled"):
                final = (
                    f"✅ **Restricted Import Complete**\n\n"
                    f"Imported: **{prog.get('imported', 0)}**\n"
                    f"Errors: **{prog.get('errors', 0)}**\n"
                    f"Skipped: **{prog.get('skipped', 0)}**\n"
                    f"⏱ {prog.get('elapsed', 0)}s"
                ) if status == "done" else (
                    f"⚠️ **Import {status}**\n{prog.get('error_msg', '')}"
                )
                try:
                    await status_msg.edit_text(final)
                except Exception:
                    pass
                break

    asyncio.create_task(_poll())


# ═══════════════════════════════════════════════════════════════════════════════
# /bulk_delete — delete a range of messages from STORAGE_CHANNEL + drive
# ═══════════════════════════════════════════════════════════════════════════════
@main_bot.on_message(
    filters.command("bulk_delete")
    & filters.private
    & filters.user(config.TELEGRAM_ADMIN_IDS),
)
async def bulk_delete_handler(client: Client, message: Message):
    """Delete a range of files. Two-step: preview, then confirm."""
    if message.chat.id in _pending_requests:
        await message.reply_text(
            "I'm already waiting for your input. Please respond or /cancel."
        )
        return

    await message.reply_text(
        "🗑️ **Bulk Delete by Range**\n\n"
        "I'll delete a range of files from your STORAGE channel AND your drive.\n\n"
        "⚠️ **This is permanent.** You'll see a preview first before anything is deleted.\n\n"
        "Send /cancel anytime."
    )

    # Step 1: start link
    try:
        start_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📎 **Step 1/2: Start link**\n\n"
                "Send the link of the **first message** to delete.\n"
                "Must be from your STORAGE channel.\n\n"
                "Format: `https://t.me/c/1234567890/100`"
            ),
            timeout=300,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ Timeout. Use /bulk_delete to try again.")
        return

    if start_msg.text.lower().strip() == "/cancel":
        await message.reply_text("❌ Cancelled.")
        return
    start_link = start_msg.text.strip()

    # Step 2: end link
    try:
        end_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=(
                "📎 **Step 2/2: End link**\n\n"
                "Send the link of the **last message** to delete.\n"
                "Must be from the same channel."
            ),
            timeout=300,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ Timeout. Use /bulk_delete to try again.")
        return

    if end_msg.text.lower().strip() == "/cancel":
        await message.reply_text("❌ Cancelled.")
        return
    end_link = end_msg.text.strip()

    # Parse + preview
    from utils.bulk_delete import (
        _parse_storage_link, build_preview, DELETE_PREVIEWS,
        execute_delete, BULK_DELETE_PROGRESS,
    )

    try:
        start_id = _parse_storage_link(start_link)
        end_id = _parse_storage_link(end_link)
    except Exception as e:
        await message.reply_text(f"❌ Link parse error: {e}")
        return

    try:
        preview = build_preview(start_id, end_id)
    except Exception as e:
        await message.reply_text(f"❌ Preview failed: {e}")
        return

    count = preview["count"]
    if count == 0:
        await message.reply_text(
            f"✅ **No files found** in range {preview['start_id']} → {preview['end_id']}.\n"
            f"Nothing to delete."
        )
        return

    def _human_size(n):
        units = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while n >= 1024 and i < 4:
            n /= 1024
            i += 1
        return f"{n:.2f} {units[i]}"

    preview_text = (
        f"⚠️ **Preview: {count} file(s) will be PERMANENTLY deleted**\n\n"
        f"📍 Range: `{preview['start_id']}` → `{preview['end_id']}` "
        f"({preview['range_size']} message IDs scanned)\n"
        f"💾 Total size: **{_human_size(preview['total_size'])}**\n\n"
        f"**First few files:**\n"
    )
    for m in preview["matches"][:8]:
        preview_text += f"  • `{m['name'][:40]}` ({_human_size(m['size'])})\n"
    if preview["truncated"] or count > 8:
        preview_text += f"  ... and {count - min(8, count)} more\n"
    preview_text += "\nReply **YES** to confirm deletion, anything else to cancel."

    try:
        confirm_msg = await manual_ask(
            client=client,
            chat_id=message.chat.id,
            text=preview_text,
            timeout=120,
            filters=filters.text,
        )
    except asyncio.TimeoutError:
        await message.reply_text("⏰ Timeout. Deletion cancelled.")
        return

    if confirm_msg.text.strip().upper() != "YES":
        await message.reply_text("❌ Cancelled. No files were deleted.")
        return

    # Execute
    import secrets as _secrets
    delete_id = _secrets.token_hex(8)
    preview_token = preview["preview_token"]

    status_msg = await message.reply_text("🗑️ **Deletion started**\nWorking...")

    async def _runner():
        try:
            await execute_delete(client, preview_token, delete_id)
        except Exception as e:
            logger.error(f"Bot bulk delete error: {e}")

    asyncio.create_task(_runner())

    async def _poll():
        last_text = ""
        while True:
            await asyncio.sleep(3)
            prog = BULK_DELETE_PROGRESS.get(delete_id)
            if not prog:
                continue
            status = prog.get("status", "?")
            txt = (
                f"🗑️ **Bulk Delete**\n\n"
                f"Status: `{status}`\n"
                f"Telegram: **{prog.get('telegram_deleted', 0)}** / {prog.get('total', 0)}\n"
                f"Drive: **{prog.get('drive_deleted', 0)}** / {prog.get('total', 0)}\n"
                f"Errors: **{prog.get('errors', 0)}**"
            )
            if txt != last_text:
                try:
                    await status_msg.edit_text(txt)
                    last_text = txt
                except Exception:
                    pass
            if status in ("done", "error", "cancelled"):
                final = (
                    f"✅ **Bulk Delete Complete**\n\n"
                    f"Telegram deleted: **{prog.get('telegram_deleted', 0)}**\n"
                    f"Drive removed: **{prog.get('drive_deleted', 0)}**\n"
                    f"Errors: **{prog.get('errors', 0)}**\n"
                    f"⏱ {prog.get('elapsed', 0)}s"
                ) if status == "done" else (
                    f"⚠️ **Delete {status}**\n{prog.get('error_msg', '')}"
                )
                try:
                    await status_msg.edit_text(final)
                except Exception:
                    pass
                break

    asyncio.create_task(_poll())
