"""
Enhanced ByteStreamer for Telegram file streaming.
Fixed for Pyrogram version compatibility (KurimuzonAkuma fork).
Optimized for YouTube-like video streaming with proper chunking.
Uses Pyrogram's built-in streaming for reliability and automatic DC migration handling.
"""

import asyncio
from typing import Dict
from pyrogram import Client
from .file_properties import get_file_ids
from pyrogram.file_id import FileId
from utils.logger import Logger

logger = Logger(__name__)


class ByteStreamer:
    """
    Handles streaming of files from Telegram with optimized chunking.
    Supports range requests for video seeking.
    """
    
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60  # 30 minutes cache cleanup
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, channel, message_id: int) -> FileId:
        """Get file properties, using cache if available."""
        if message_id not in self.cached_file_ids:
            await self.generate_file_properties(channel, message_id)
        return self.cached_file_ids[message_id]

    async def generate_file_properties(self, channel, message_id: int) -> FileId:
        """Generate and cache file properties from Telegram."""
        file_id = await get_file_ids(self.client, channel, message_id)
        if not file_id:
            raise Exception("FileNotFound")
        self.cached_file_ids[message_id] = file_id
        return self.cached_file_ids[message_id]

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """
        Async generator that yields the bytes of the media file using Pyrogram's stream_media method.

        Optimized for video streaming with:
        - Reliable DC handling via Pyrogram's internal client
        - Configurable chunk sizes (default 1MB for optimal streaming)
        - Proper byte slicing for range requests
        - Automatic DC migration handling

        Args:
            file_id: The Telegram file ID object
            offset: Starting byte offset (aligned to chunk_size)
            first_part_cut: Bytes to skip in first chunk
            last_part_cut: Bytes to include in last chunk
            part_count: Total number of chunks to stream
            chunk_size: Size of each chunk in bytes
        """
        client = self.client
        logger.debug(f"Starting file stream: offset={offset}, parts={part_count}, chunk_size={chunk_size}")

        # Calculate which chunk to start from (offset is in bytes, stream_media offset is in chunks)
        chunk_offset = offset // chunk_size

        current_part = 1
        chunk_count = 0

        try:
            # Use Pyrogram's stream_media which returns an async generator
            # offset and limit are in chunks (not bytes), each chunk is max 1MB
            async for chunk in client.stream_media(
                file_id.file_id,
                offset=chunk_offset,
                limit=part_count
            ):
                if not chunk:
                    logger.debug(f"Empty chunk received at part {current_part}")
                    break

                chunk_count += 1

                # Apply byte slicing based on position
                if part_count == 1:
                    # Single chunk: slice both start and end
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    # First chunk: slice start only
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    # Last chunk: slice end only
                    yield chunk[:last_part_cut]
                else:
                    # Middle chunk: yield full chunk
                    yield chunk

                current_part += 1

                if current_part > part_count:
                    break

            logger.debug(f"Stream completed: yielded {chunk_count} chunks")

        except (TimeoutError, AttributeError) as e:
            logger.warning(f"Stream interrupted: {e}")
        except Exception as e:
            logger.error(f"Stream error: {e}")
            raise
        finally:
            logger.debug(f"Finished streaming file: {current_part - 1} parts delivered")

    async def clean_cache(self) -> None:
        """
        Periodically clean the file ID cache to reduce memory usage.
        Runs every 30 minutes.
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            cache_size = len(self.cached_file_ids)
            self.cached_file_ids.clear()
            logger.debug(f"Cleaned file ID cache: {cache_size} entries removed")
