import asyncio
import re
from pyrogram import Client
from pyrogram.types import Message
from utils.logger import Logger
from utils.directoryHandler import DRIVE_DATA
from config import STORAGE_CHANNEL

logger = Logger(__name__)

class SmartImportManager:
    def __init__(self):
        self.import_channels = {}  # Store channel info for smart import
        
    async def validate_channel_access(self, client: Client, channel_identifier: str):
        """Validate that the bot has admin access to the channel"""
        try:
            # Try to get channel info
            channel = await client.get_chat(channel_identifier)

            # Check if bot is admin
            try:
                bot_member = await client.get_chat_member(channel.id, "me")
                is_admin = bot_member.privileges and (bot_member.privileges.can_delete_messages or bot_member.privileges.can_edit_messages)
            except Exception as member_err:
                # Bot is not a member or not an admin
                error_msg = str(member_err)
                if "USER_NOT_PARTICIPANT" in error_msg or "not a member" in error_msg.lower():
                    return False, f"Bot is not a member of '{channel.title}'. Please add the bot to the channel first and make it an admin for fast import, or a regular member for standard import.", False
                is_admin = False

            return True, channel, is_admin
        except Exception as e:
            error_msg = str(e)
            if "USER_NOT_PARTICIPANT" in error_msg:
                return False, f"Cannot access channel '{channel_identifier}'. Make sure the bot is added to the channel as a member.", False
            elif "No user has" in error_msg or "No chat" in error_msg:
                return False, f"Channel '{channel_identifier}' not found. Please check the channel username or ID.", False
            return False, f"Cannot access channel: {error_msg}", False
    
    async def get_channel_files(self, client: Client, channel_id: int, start_msg_id: int = None, end_msg_id: int = None):
        """Get all files from a channel within the specified range"""
        files = []
        
        try:
            if start_msg_id and end_msg_id:
                # Get files in range
                for msg_id in range(start_msg_id, end_msg_id + 1):
                    try:
                        message = await client.get_messages(channel_id, msg_id)
                        if message and not message.empty:
                            file_info = self.extract_file_info(message, channel_id)
                            if file_info:
                                files.append(file_info)
                    except:
                        continue
            else:
                # Get all files from channel
                async for message in client.get_chat_history(channel_id):
                    file_info = self.extract_file_info(message, channel_id)
                    if file_info:
                        files.append(file_info)
                        
        except Exception as e:
            logger.error(f"Error getting channel files: {e}")
            
        return files
    
    def extract_file_info(self, message: Message, source_channel_id: int):
        """Extract file information from a message"""
        media = (
            message.document
            or message.video
            or message.audio
            or message.photo
            or message.sticker
        )
        
        if not media:
            return None
            
        return {
            'message_id': message.id,
            'file_name': getattr(media, 'file_name', f'file_{message.id}'),
            'file_size': getattr(media, 'file_size', 0),
            'duration': getattr(media, 'duration', 0) if hasattr(media, 'duration') else 0,
            'source_channel': source_channel_id,
            'file_id': media.file_id,
            'file_unique_id': media.file_unique_id
        }
    
    async def smart_bulk_import(self, client: Client, channel_identifier: str, destination_folder: str, 
                               start_msg_id: int = None, end_msg_id: int = None, import_mode: str = "auto"):
        """Smart bulk import with user choice"""
        
        # Validate channel access
        is_valid, result, is_admin = await self.validate_channel_access(client, channel_identifier)
        if not is_valid:
            raise Exception(result)
            
        channel = result
        channel_id = channel.id
        
        # Get files from channel
        files = await self.get_channel_files(client, channel_id, start_msg_id, end_msg_id)
        
        if not files:
            raise Exception("No files found in the specified range")
        
        # Determine import method based on mode and admin status
        if import_mode == "auto":
            use_fast_import = is_admin
        elif import_mode == "fast":
            if not is_admin:
                raise Exception("Fast import requires bot to be admin in source channel")
            use_fast_import = True
        else:  # regular
            use_fast_import = False
        
        imported_count = 0
        
        if use_fast_import:
            # Fast import - direct reference without copying
            for file_info in files:
                try:
                    DRIVE_DATA.new_fast_import_file(
                        destination_folder,
                        file_info['file_name'],
                        file_info['message_id'],
                        file_info['file_size'],
                        file_info['duration'],
                        file_info['source_channel']
                    )
                    imported_count += 1
                    logger.info(f"Fast imported: {file_info['file_name']} from channel {channel_id}")
                    
                except Exception as e:
                    logger.error(f"Error fast importing file {file_info['file_name']}: {e}")
                    continue
        else:
            # Regular import - copy to storage channel
            from utils.uploader import copy_file_to_storage
            
            for file_info in files:
                try:
                    # Copy file from source to storage channel
                    new_message_id = await copy_file_to_storage(client, channel_id, file_info['message_id'])
                    
                    # Add to drive data with storage channel reference
                    DRIVE_DATA.new_file(
                        destination_folder,
                        file_info['file_name'],
                        new_message_id,
                        file_info['file_size'],
                        file_info['duration']
                    )
                    imported_count += 1
                    logger.info(f"Regular imported: {file_info['file_name']} to storage channel")
                    
                except Exception as e:
                    logger.error(f"Error regular importing file {file_info['file_name']}: {e}")
                    continue
        
        return imported_count, len(files), use_fast_import

# Global instance
SMART_IMPORT_MANAGER = SmartImportManager()