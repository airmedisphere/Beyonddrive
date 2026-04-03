"""
TG Drive - Fixed Video Streaming Server
Proper HTTP Range Requests + 206 Partial Content
"""

from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import mimetypes
from pathlib import Path
from urllib.parse import quote
import math
import asyncio

from utils.logger import Logger
from utils.clients import get_client
from utils.directoryHandler import DRIVE_DATA
from utils.streamer.custom_dl import ByteStreamer
from config import ADMIN_PASSWORD, STORAGE_CHANNEL, MAX_FILE_SIZE

logger = Logger(__name__)

app = FastAPI(title="TG Drive - Fixed Streaming")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Length", "Content-Range", "Accept-Ranges"],
)

class_cache = {}

def parse_range_header(range_header: str, file_size: int):
    """Parse Range: bytes=... header"""
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1

    try:
        spec = range_header[6:].strip()
        if spec.startswith("-"):
            suffix = int(spec[1:])
            start = max(0, file_size - suffix)
            end = file_size - 1
        elif spec.endswith("-"):
            start = int(spec[:-1])
            end = file_size - 1
        else:
            parts = spec.split("-")
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else file_size - 1

        start = max(0, min(start, file_size - 1))
        end = max(start, min(end, file_size - 1))
        return start, end
    except:
        return 0, file_size - 1


async def stream_media(channel: int, message_id: int, filename: str, request: Request):
    """Core streaming function with proper Range support"""
    range_header = request.headers.get("Range", "")
    logger.info(f"Streaming: {filename} | Range: {range_header}")

    client = get_client()
    if client not in class_cache:
        class_cache[client] = ByteStreamer(client)
    streamer = class_cache[client]

    try:
        file_id = await streamer.get_file_properties(channel, message_id)
        file_size = file_id.file_size

        if file_size == 0:
            raise HTTPException(404, "Empty file")

        start, end = parse_range_header(range_header, file_size)
        if start > end:
            raise HTTPException(416, "Range Not Satisfiable")

        chunk_size = 1024 * 1024  # 1MB chunks - optimal for video
        offset = start - (start % chunk_size)
        first_cut = start - offset
        last_cut = (end % chunk_size) + 1 if end < file_size - 1 else None

        part_count = math.ceil((end + 1) / chunk_size) - math.floor(offset / chunk_size)

        async def generate():
            try:
                async for chunk in streamer.yield_file(
                    file_id, offset, first_cut, last_cut, part_count, chunk_size
                ):
                    yield chunk
            except Exception as e:
                logger.error(f"Stream error: {e}")
                raise

        mime_type = mimetypes.guess_type(filename.lower())[0] or "video/mp4"
        is_range = bool(range_header)
        status = 206 if is_range else 200

        headers = {
            "Content-Type": mime_type,
            "Accept-Ranges": "bytes",
            "Content-Disposition": f'inline; filename="{quote(filename)}"',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
            "Cache-Control": "public, max-age=3600",
        }

        content_length = end - start + 1
        headers["Content-Length"] = str(content_length)

        if is_range:
            headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

        return StreamingResponse(
            generate(),
            status_code=status,
            headers=headers,
            media_type=mime_type,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Streaming failed for {filename}: {e}")
        raise HTTPException(500, "Streaming failed")


# ====================== ROUTES ======================

@app.get("/")
async def home():
    return FileResponse("website/home.html")


@app.get("/stream")
@app.get("/player")
async def video_player():
    return FileResponse("website/VideoPlayer.html")


@app.get("/smart-player")
async def smart_player():
    return FileResponse("website/SmartPlayer.html")


@app.get("/pdf-viewer")
async def pdf_viewer():
    return FileResponse("website/PDFViewer.html")


@app.get("/static/{file_path:path}")
async def static(file_path: str):
    if "apiHandler.js" in file_path:
        with open(f"website/static/js/apiHandler.js") as f:
            content = f.read()
            content = content.replace("MAX_FILE_SIZE__SDGJDG", str(MAX_FILE_SIZE))
        return Response(content=content, media_type="application/javascript")
    return FileResponse(f"website/static/{file_path}")


@app.options("/file")
async def file_options():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
        },
    )


@app.head("/file")
async def file_head(request: Request):
    path = request.query_params.get("path")
    if not path:
        raise HTTPException(400, "path required")

    try:
        file = DRIVE_DATA.get_file(path)
        mime = mimetypes.guess_type(file.name.lower())[0] or "video/mp4"
        return Response(
            status_code=200,
            headers={
                "Content-Type": mime,
                "Content-Length": str(file.size),
                "Accept-Ranges": "bytes",
            },
        )
    except:
        raise HTTPException(404, "File not found")


@app.get("/file")
async def serve_file(request: Request):
    """Main file streaming endpoint"""
    path = request.query_params.get("path")
    quality = request.query_params.get("quality", "original")

    if not path:
        raise HTTPException(400, "path required")

    try:
        file = DRIVE_DATA.get_file(path)
    except:
        raise HTTPException(404, "File not found")

    # Quality switching support
    if quality != "original" and hasattr(file, "encoded_versions") and quality in file.encoded_versions:
        encoded = file.encoded_versions[quality]
        return await stream_media(STORAGE_CHANNEL, encoded["message_id"], f"{file.name}_{quality}.mp4", request)

    # Fast import vs normal
    channel = file.source_channel if hasattr(file, "is_fast_import") and file.is_fast_import and file.source_channel else STORAGE_CHANNEL

    return await stream_media(channel, file.file_id, file.name, request)


async def stream_media(channel: int, message_id: int, filename: str, request: Request):
    """Unified streaming function"""
    return await stream_media_core(channel, message_id, filename, request)


# Secure token access (optional but recommended)
@app.get("/secure/{token}")
async def secure_file(token: str, request: Request, password: str = None):
    # Add your token validation logic here if needed
    # For now, just forward to normal streaming
    # You can integrate your token system later
    path = request.query_params.get("path")
    if path:
        return await serve_file(request)
    raise HTTPException(400, "path required")


# Health check
@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "2.1.0-fixed"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
