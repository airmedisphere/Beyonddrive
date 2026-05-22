"""
Enhanced media streaming module with full HTTP Range Request support.
Optimized for YouTube-like video playback with instant start and smooth seeking.
"""

import math
import mimetypes
from pathlib import Path
from fastapi.responses import StreamingResponse, Response
from utils.logger import Logger
from utils.streamer.custom_dl import ByteStreamer
from utils.streamer.file_properties import get_name
from utils.clients import get_client
from urllib.parse import quote
from utils.directoryHandler import DRIVE_DATA

logger = Logger(__name__)

class_cache = {}

# Video MIME type mapping for common extensions
VIDEO_MIME_TYPES = {
    '.mp4': 'video/mp4',
    '.mkv': 'video/x-matroska',
    '.webm': 'video/webm',
    '.avi': 'video/x-msvideo',
    '.mov': 'video/quicktime',
    '.m4v': 'video/mp4',
    '.flv': 'video/x-flv',
    '.wmv': 'video/x-ms-wmv',
    '.3gp': 'video/3gpp',
    '.ts': 'video/mp2t',
}

AUDIO_MIME_TYPES = {
    '.mp3': 'audio/mpeg',
    '.wav': 'audio/wav',
    '.flac': 'audio/flac',
    '.aac': 'audio/aac',
    '.ogg': 'audio/ogg',
    '.m4a': 'audio/mp4',
    '.wma': 'audio/x-ms-wma',
}

DOCUMENT_MIME_TYPES = {
    '.pdf':  'application/pdf',
    '.epub': 'application/epub+zip',
}


def parse_range_header(range_header: str, file_size: int) -> tuple:
    """
    Parse HTTP Range header and return (start, end) bytes.
    
    Supports formats:
    - bytes=0-499 (first 500 bytes)
    - bytes=500-999 (second 500 bytes)  
    - bytes=-500 (last 500 bytes)
    - bytes=9500- (from byte 9500 to end)
    
    Args:
        range_header: The Range header value (e.g., "bytes=0-1023")
        file_size: Total size of the file in bytes
        
    Returns:
        Tuple of (start_byte, end_byte)
    """
    if not range_header or not range_header.startswith('bytes='):
        return 0, file_size - 1
    
    range_spec = range_header[6:]  # Remove 'bytes='
    
    try:
        if range_spec.startswith('-'):
            # Suffix range: bytes=-500 (last 500 bytes)
            suffix_length = int(range_spec[1:])
            start = max(0, file_size - suffix_length)
            end = file_size - 1
        elif range_spec.endswith('-'):
            # Open-ended range: bytes=9500- (from 9500 to end)
            start = int(range_spec[:-1])
            end = file_size - 1
        elif '-' in range_spec:
            # Standard range: bytes=0-499
            parts = range_spec.split('-', 1)
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else file_size - 1
        else:
            # Invalid format, return full file
            return 0, file_size - 1
        
        # Validate and clamp ranges
        start = max(0, min(start, file_size - 1))
        end = max(start, min(end, file_size - 1))
        
        logger.info(f"Range parsed: {range_header} -> bytes {start}-{end}/{file_size}")
        return start, end
        
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse range header '{range_header}': {e}")
        return 0, file_size - 1


def get_mime_type(file_name: str) -> str:
    """
    Get the MIME type for a file, with special handling for video/audio files.
    
    Args:
        file_name: Name of the file
        
    Returns:
        MIME type string
    """
    ext = Path(file_name).suffix.lower()
    
    # Check video types first
    if ext in VIDEO_MIME_TYPES:
        return VIDEO_MIME_TYPES[ext]
    
    # Then audio types
    if ext in AUDIO_MIME_TYPES:
        return AUDIO_MIME_TYPES[ext]
    
    # Then document types (PDF, EPUB)
    if ext in DOCUMENT_MIME_TYPES:
        return DOCUMENT_MIME_TYPES[ext]
    
    # Fall back to mimetypes module
    mime_type = mimetypes.guess_type(file_name.lower())[0]
    return mime_type or "application/octet-stream"


async def media_streamer(channel: int, message_id: int, file_name: str, request):
    """
    Stream media files from Telegram with full HTTP Range Request support.
    
    Features:
    - Proper 206 Partial Content responses for seeking
    - 1MB chunk size for optimal streaming performance
    - Correct Content-Range headers for browser compatibility
    - CORS headers for cross-origin playback
    - Fast import channel support
    
    Args:
        channel: Telegram channel ID where the file is stored
        message_id: Message ID containing the file
        file_name: Name of the file for Content-Disposition
        request: FastAPI Request object
        
    Returns:
        StreamingResponse or error Response
    """
    global class_cache

    range_header = request.headers.get("Range", "")
    
    logger.info(f"Stream request: file={file_name}, channel={channel}, msg_id={message_id}, range='{range_header}'")

    # Check if this is a fast import file that needs source channel
    try:
        file_path = request.query_params.get("path", "")
        if file_path and DRIVE_DATA is not None:
            file_obj = DRIVE_DATA.get_file(file_path)
            if hasattr(file_obj, 'is_fast_import') and file_obj.is_fast_import and file_obj.source_channel:
                channel = file_obj.source_channel
                logger.info(f"Using fast import source channel {channel} for file {file_name}")
    except Exception as e:
        logger.debug(f"Could not check fast import status: {e}")

    # Get or create ByteStreamer for client
    faster_client = get_client()
    
    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
    else:
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect

    try:
        # Get file properties from Telegram
        file_id = await tg_connect.get_file_properties(channel, message_id)
        file_size = file_id.file_size
        
        logger.info(f"File properties: size={file_size} bytes, mime={getattr(file_id, 'mime_type', 'unknown')}")
        
        if file_size == 0:
            logger.error(f"File size is 0 for {file_name}")
            return Response(
                status_code=404,
                content="File not found or empty",
                headers={"Accept-Ranges": "bytes"}
            )
        
    except Exception as e:
        logger.error(f"Failed to get file properties for {file_name}: {e}")
        return Response(
            status_code=500,
            content=f"Failed to retrieve file: {str(e)}",
            headers={"Accept-Ranges": "bytes"}
        )

    # Parse range request
    from_bytes, until_bytes = parse_range_header(range_header, file_size)

    # Validate range
    if from_bytes >= file_size or until_bytes >= file_size or from_bytes > until_bytes:
        logger.warning(f"Range not satisfiable: {from_bytes}-{until_bytes} for size {file_size}")
        return Response(
            status_code=416,
            content="Range Not Satisfiable",
            headers={
                "Content-Range": f"bytes */{file_size}",
                "Accept-Ranges": "bytes"
            },
        )

    # Ensure until_bytes is within bounds
    until_bytes = min(until_bytes, file_size - 1)

    # Calculate streaming parameters
    # Use 1MB chunks for optimal streaming performance
    chunk_size = 1024 * 1024  # 1MB
    
    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    # last_part_cut: how many bytes to keep from the last chunk.
    # If until_bytes is the last byte of a chunk, we keep the full chunk.
    last_part_cut = (until_bytes % chunk_size) + 1
    if last_part_cut == chunk_size:
        last_part_cut = chunk_size  # keep full last chunk (already correct)

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil((until_bytes + 1) / chunk_size) - math.floor(offset / chunk_size)
    
    logger.info(f"Streaming params: offset={offset}, parts={part_count}, length={req_length}")
    
    # Create streaming body generator
    body = tg_connect.yield_file(
        file_id, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    # Determine MIME type and content disposition
    mime_type = get_mime_type(file_name)
    
    # Use inline disposition for streamable content
    disposition = "inline" if any(x in mime_type for x in ["video/", "audio/", "image/", "/html", "/pdf", "epub"]) else "attachment"

    # Determine status code
    is_range_request = bool(range_header)
    status_code = 206 if is_range_request else 200

    # Build response headers
    headers = {
        "Content-Type": mime_type,
        "Content-Length": str(req_length),
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'{disposition}; filename="{quote(file_name)}"',
        # CORS headers for browser playback
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        # Caching for performance
        "Cache-Control": "public, max-age=3600",
        # Security
        "X-Content-Type-Options": "nosniff",
    }
    
    # Add Content-Range header for range requests
    if is_range_request:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
    
    logger.info(f"Streaming response: status={status_code}, range=bytes {from_bytes}-{until_bytes}/{file_size}")

    return StreamingResponse(
        status_code=status_code,
        content=body,
        headers=headers,
        media_type=mime_type,
    )