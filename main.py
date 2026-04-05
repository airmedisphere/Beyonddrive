from utils.downloader import (
    download_file,
    get_file_info_from_url,
)
import asyncio
import secrets
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pathlib import Path
from contextlib import asynccontextmanager
import aiofiles
from fastapi import FastAPI, HTTPException, Request, File, UploadFile, Form, Response, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from config import ADMIN_PASSWORD, MAX_FILE_SIZE, STORAGE_CHANNEL
from utils.clients import initialize_clients
from utils.directoryHandler import getRandomID
from utils.extra import auto_ping_website, convert_class_to_dict, reset_cache_dir
from utils.streamer import media_streamer
from utils.uploader import start_file_uploader
from utils.logger import Logger
import urllib.parse


# ============================================================================
# TOKEN MANAGEMENT FOR SECURE ACCESS
# ============================================================================

ACCESS_TOKENS: Dict[str, Dict[str, Any]] = {}
TOKEN_EXPIRY_HOURS = 24

def generate_secure_token() -> str:
    """Generate a cryptographically secure token"""
    return secrets.token_urlsafe(32)

def hash_password(password: str) -> str:
    """Hash a password for secure storage"""
    return hashlib.sha256(password.encode()).hexdigest()

def create_access_token(file_path: str, expiry_hours: int = TOKEN_EXPIRY_HOURS, password: Optional[str] = None) -> Dict[str, Any]:
    """Create a temporary access token for a file"""
    token = generate_secure_token()
    expires_at = datetime.now() + timedelta(hours=expiry_hours)
    
    ACCESS_TOKENS[token] = {
        "file_path": file_path,
        "expires_at": expires_at,
        "password_protected": password is not None,
        "password_hash": hash_password(password) if password else None,
        "created_at": datetime.now(),
        "access_count": 0
    }
    return {"token": token, "expires_at": expires_at.isoformat()}

def validate_access_token(token: str, password: Optional[str] = None) -> Optional[str]:
    """Validate an access token and return the file path if valid"""
    if token not in ACCESS_TOKENS:
        return None
    
    token_data = ACCESS_TOKENS[token]
    
    if datetime.now() > token_data["expires_at"]:
        del ACCESS_TOKENS[token]
        return None
    
    if token_data["password_protected"]:
        if not password or hash_password(password) != token_data["password_hash"]:
            return None
    
    token_data["access_count"] += 1
    return token_data["file_path"]

async def cleanup_expired_tokens():
    """Periodically clean up expired tokens"""
    while True:
        await asyncio.sleep(3600)
        now = datetime.now()
        expired = [t for t, d in ACCESS_TOKENS.items() if now > d["expires_at"]]
        for token in expired:
            del ACCESS_TOKENS[token]


# ============================================================================
# TAGS/CATEGORIES MANAGEMENT
# ============================================================================

FILE_TAGS: Dict[str, list] = {}

def add_tags_to_file(file_id: str, tags: list) -> None:
    """Add tags to a file"""
    if file_id not in FILE_TAGS:
        FILE_TAGS[file_id] = []
    FILE_TAGS[file_id].extend([t.lower().strip() for t in tags if t.strip()])
    FILE_TAGS[file_id] = list(set(FILE_TAGS[file_id]))

def get_file_tags(file_id: str) -> list:
    """Get tags for a file"""
    return FILE_TAGS.get(file_id, [])

def search_by_tags(tags: list) -> list:
    """Search files by tags"""
    tags = [t.lower().strip() for t in tags]
    results = []
    for file_id, file_tags in FILE_TAGS.items():
        if any(tag in file_tags for tag in tags):
            results.append(file_id)
    return results


# Startup Event
def check_drive_initialized():
    """Helper function to check if DRIVE_DATA is initialized"""
    from utils.directoryHandler import DRIVE_DATA
    if DRIVE_DATA is None:
        raise HTTPException(
            status_code=503,
            detail="Service unavailable: Drive not initialized. Please configure Telegram credentials in config.py"
        )
    return DRIVE_DATA

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Reset the cache directory, delete cache files
    reset_cache_dir()

    # Initialize the clients
    try:
        await initialize_clients()
    except Exception as e:
        logger.error(f"Failed to initialize clients: {e}")
        logger.warning("Server will start but file operations will be unavailable")

    # Start the website auto ping task
    asyncio.create_task(auto_ping_website())

    # Start token cleanup task
    asyncio.create_task(cleanup_expired_tokens())

    yield


app = FastAPI(docs_url="/docs", redoc_url="/redoc", lifespan=lifespan)
logger = Logger(__name__)

# Add CORS middleware for browser compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Length", "Content-Range", "Accept-Ranges"]
)


@app.get("/")
async def home_page():
    return FileResponse("website/home.html")


@app.get("/stream")
async def stream_page():
    return FileResponse("website/VideoPlayer.html")


@app.get("/smart-player")
async def smart_player_page():
    """Serve the enhanced smart video player"""
    return FileResponse("website/SmartPlayer.html")


@app.get("/fast-player")
async def fast_player_page():
    return FileResponse("website/FastPlayer.html")


@app.get("/pdf-viewer")
async def pdf_viewer_page():
    return FileResponse("website/PDFViewer.html")


@app.get("/static/{file_path:path}")
async def static_files(file_path):
    if "apiHandler.js" in file_path:
        with open(Path("website/static/js/apiHandler.js")) as f:
            content = f.read()
            content = content.replace("MAX_FILE_SIZE__SDGJDG", str(MAX_FILE_SIZE))
        return Response(content=content, media_type="application/javascript")
    return FileResponse(f"website/static/{file_path}")


@app.get("/file")
async def dl_file(request: Request):
    DRIVE_DATA = check_drive_initialized()

    path = request.query_params.get("path")
    if not path:
        raise HTTPException(status_code=400, detail="Missing 'path' parameter")

    quality = request.query_params.get("quality", "original")  # original, 240p, 360p, 480p

    try:
        file = DRIVE_DATA.get_file(path)
    except Exception as e:
        logger.error(f"File not found at path '{path}': {e}")
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    
    # Check if a specific quality is requested and available
    if quality != "original" and hasattr(file, 'encoded_versions') and file.encoded_versions:
        if quality in file.encoded_versions:
            encoded_version = file.encoded_versions[quality]
            # Use the encoded version's message ID
            return await media_streamer(STORAGE_CHANNEL, encoded_version['message_id'], f"{file.name}_{quality}", request)
    
    # Determine which channel to use for streaming
    if hasattr(file, 'is_fast_import') and file.is_fast_import and file.source_channel:
        # Use source channel for fast import files
        channel = file.source_channel
    else:
        # Use storage channel for regular files
        channel = STORAGE_CHANNEL
    
    return await media_streamer(channel, file.file_id, file.name, request)


# CORS preflight handler for /file endpoint
@app.options("/file")
async def file_options():
    """Handle CORS preflight for file endpoint"""
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type, Authorization",
            "Access-Control-Max-Age": "86400"
        }
    )


# HEAD request handler for file metadata
@app.head("/file")
async def file_head(request: Request):
    """Handle HEAD requests for file metadata"""
    from utils.directoryHandler import DRIVE_DATA
    import mimetypes
    from urllib.parse import quote
    
    path = request.query_params.get("path")
    if not path:
        raise HTTPException(status_code=400, detail="Path parameter required")
    
    try:
        file = DRIVE_DATA.get_file(path)
        mime_type = mimetypes.guess_type(file.name.lower())[0] or "application/octet-stream"
        
        return Response(
            status_code=200,
            headers={
                "Content-Type": mime_type,
                "Content-Length": str(file.size),
                "Accept-Ranges": "bytes",
                "Content-Disposition": f'inline; filename="{quote(file.name)}"'
            }
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")


# Secure token-based file access
@app.get("/secure/{token}")
async def secure_file_access(token: str, request: Request, password: Optional[str] = Query(None)):
    """
    Access a file using a secure temporary token.
    URL format: /secure/{token}?password=optional_password
    """
    file_path = validate_access_token(token, password)
    if not file_path:
        raise HTTPException(status_code=403, detail="Invalid, expired, or password-protected token")
    
    from utils.directoryHandler import DRIVE_DATA
    
    try:
        file = DRIVE_DATA.get_file(file_path)
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")
    
    if hasattr(file, 'is_fast_import') and file.is_fast_import and file.source_channel:
        channel = file.source_channel
    else:
        channel = STORAGE_CHANNEL
    
    return await media_streamer(channel, file.file_id, file.name, request)


# Api Routes


@app.post("/api/checkPassword")
async def check_password(request: Request):
    data = await request.json()
    if data["pass"] == ADMIN_PASSWORD:
        return JSONResponse({"status": "ok"})
    return JSONResponse({"status": "Invalid password"})


@app.post("/api/createNewFolder")
async def api_new_folder(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"createNewFolder {data}")
    folder_data = DRIVE_DATA.get_directory(data["path"]).contents
    for id in folder_data:
        f = folder_data[id]
        if f.type == "folder":
            if f.name == data["name"]:
                return JSONResponse(
                    {
                        "status": "Folder with the name already exist in current directory"
                    }
                )

    DRIVE_DATA.new_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})


@app.post("/api/getDirectory")
async def api_get_directory(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] == ADMIN_PASSWORD:
        is_admin = True
    else:
        is_admin = False

    auth = data.get("auth")
    sort_by = data.get("sort_by", "date")  # name, date, size
    sort_order = data.get("sort_order", "desc")  # asc, desc

    logger.info(f"getFolder {data}")

    if data["path"] == "/trash":
        data = {"contents": DRIVE_DATA.get_trashed_files_folders()}
        folder_data = convert_class_to_dict(data, isObject=False, showtrash=True, sort_by=sort_by, sort_order=sort_order)

    elif "/search_" in data["path"]:
        query = urllib.parse.unquote(data["path"].split("_", 1)[1])
        print(query)
        data = {"contents": DRIVE_DATA.search_file_folder(query)}
        print(data)
        folder_data = convert_class_to_dict(data, isObject=False, showtrash=False, sort_by=sort_by, sort_order=sort_order)
        print(folder_data)

    elif "/share_" in data["path"]:
        path = data["path"].split("_", 1)[1]
        folder_data, auth_home_path = DRIVE_DATA.get_directory(path, is_admin, auth)
        auth_home_path= auth_home_path.replace("//", "/") if auth_home_path else None
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False, sort_by=sort_by, sort_order=sort_order)
        return JSONResponse(
            {"status": "ok", "data": folder_data, "auth_home_path": auth_home_path}
        )

    else:
        folder_data = DRIVE_DATA.get_directory(data["path"])
        folder_data = convert_class_to_dict(folder_data, isObject=True, showtrash=False, sort_by=sort_by, sort_order=sort_order)
    return JSONResponse({"status": "ok", "data": folder_data, "auth_home_path": None})


SAVE_PROGRESS = {}


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    path: str = Form(...),
    password: str = Form(...),
    id: str = Form(...),
    total_size: str = Form(...),
):
    global SAVE_PROGRESS

    if password != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    total_size = int(total_size)
    SAVE_PROGRESS[id] = ("running", 0, total_size)

    ext = file.filename.lower().split(".")[-1]

    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    file_location = cache_dir / f"{id}.{ext}"

    file_size = 0

    async with aiofiles.open(file_location, "wb") as buffer:
        while chunk := await file.read(1024 * 1024):  # Read file in chunks of 1MB
            SAVE_PROGRESS[id] = ("running", file_size, total_size)
            file_size += len(chunk)
            if file_size > MAX_FILE_SIZE:
                await buffer.close()
                file_location.unlink()  # Delete the partially written file
                raise HTTPException(
                    status_code=400,
                    detail=f"File size exceeds {MAX_FILE_SIZE} bytes limit",
                )
            await buffer.write(chunk)

    SAVE_PROGRESS[id] = ("completed", file_size, file_size)

    asyncio.create_task(
        start_file_uploader(file_location, id, path, file.filename, file_size)
    )

    return JSONResponse({"id": id, "status": "ok"})


@app.post("/api/getSaveProgress")
async def get_save_progress(request: Request):
    global SAVE_PROGRESS

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getUploadProgress {data}")
    try:
        progress = SAVE_PROGRESS[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/getUploadProgress")
async def get_upload_progress(request: Request):
    from utils.uploader import PROGRESS_CACHE

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getUploadProgress {data}")

    try:
        progress = PROGRESS_CACHE[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/cancelUpload")
async def cancel_upload(request: Request):
    from utils.uploader import STOP_TRANSMISSION
    from utils.downloader import STOP_DOWNLOAD

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"cancelUpload {data}")
    STOP_TRANSMISSION.append(data["id"])
    STOP_DOWNLOAD.append(data["id"])
    return JSONResponse({"status": "ok"})


@app.post("/api/renameFileFolder")
async def rename_file_folder(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"renameFileFolder {data}")
    DRIVE_DATA.rename_file_folder(data["path"], data["name"])
    return JSONResponse({"status": "ok"})


@app.post("/api/trashFileFolder")
async def trash_file_folder(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"trashFileFolder {data}")
    DRIVE_DATA.trash_file_folder(data["path"], data["trash"])
    return JSONResponse({"status": "ok"})


@app.post("/api/deleteFileFolder")
async def delete_file_folder(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"deleteFileFolder {data}")
    DRIVE_DATA.delete_file_folder(data["path"])
    return JSONResponse({"status": "ok"})


@app.post("/api/moveFileFolder")
async def move_file_folder(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"moveFileFolder {data}")
    try:
        DRIVE_DATA.move_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/copyFileFolder")
async def copy_file_folder(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"copyFileFolder {data}")
    try:
        DRIVE_DATA.copy_file_folder(data["source_path"], data["destination_path"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFolderTree")
async def get_folder_tree(request: Request):
    DRIVE_DATA = check_drive_initialized()

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFolderTree {data}")
    try:
        folder_tree = DRIVE_DATA.get_folder_tree()
        return JSONResponse({"status": "ok", "data": folder_tree})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFileInfoFromUrl")
async def getFileInfoFromUrl(request: Request):

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFileInfoFromUrl {data}")
    try:
        file_info = await get_file_info_from_url(data["url"])
        return JSONResponse({"status": "ok", "data": file_info})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/startFileDownloadFromUrl")
async def startFileDownloadFromUrl(request: Request):
    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"startFileDownloadFromUrl {data}")
    try:
        id = getRandomID()
        asyncio.create_task(
            download_file(data["url"], id, data["path"], data["filename"], data["singleThreaded"])
        )
        return JSONResponse({"status": "ok", "id": id})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getFileDownloadProgress")
async def getFileDownloadProgress(request: Request):
    from utils.downloader import DOWNLOAD_PROGRESS

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFileDownloadProgress {data}")

    try:
        progress = DOWNLOAD_PROGRESS[data["id"]]
        return JSONResponse({"status": "ok", "data": progress})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/getFolderShareAuth")
async def getFolderShareAuth(request: Request):
    from utils.directoryHandler import DRIVE_DATA

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"getFolderShareAuth {data}")

    try:
        auth = DRIVE_DATA.get_folder_auth(data["path"])
        return JSONResponse({"status": "ok", "auth": auth})
    except:
        return JSONResponse({"status": "not found"})


@app.post("/api/smartBulkImport")
async def smart_bulk_import(request: Request):
    """API endpoint for smart bulk import functionality"""
    from utils.fast_import import SMART_IMPORT_MANAGER
    from utils.clients import get_client

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    logger.info(f"smartBulkImport {data}")

    try:
        client = get_client()
        channel_identifier = data["channel"]
        destination_folder = data["path"]
        start_msg_id = data.get("start_msg_id")
        end_msg_id = data.get("end_msg_id")
        import_mode = data.get("import_mode", "auto")  # auto, fast, regular

        imported_count, total_files, used_fast_import = await SMART_IMPORT_MANAGER.smart_bulk_import(
            client, 
            channel_identifier, 
            destination_folder, 
            start_msg_id, 
            end_msg_id,
            import_mode
        )

        return JSONResponse({
            "status": "ok", 
            "imported": imported_count, 
            "total": total_files,
            "method": "fast_import" if used_fast_import else "regular_import"
        })
    except Exception as e:
        logger.error(f"Smart bulk import error: {e}")
        return JSONResponse({"status": str(e)})


@app.post("/api/checkChannelAdmin")
async def check_channel_admin(request: Request):
    """Check if bot is admin in a channel"""
    from utils.fast_import import SMART_IMPORT_MANAGER
    from utils.clients import get_client

    data = await request.json()

    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})

    try:
        client = get_client()
        channel_identifier = data["channel"]
        
        is_valid, result, is_admin = await SMART_IMPORT_MANAGER.validate_channel_access(client, channel_identifier)
        
        if not is_valid:
            return JSONResponse({"status": "error", "message": result})
        
        return JSONResponse({
            "status": "ok",
            "is_admin": is_admin,
            "channel_name": result.title or result.username or str(result.id)
        })
    except Exception as e:
        logger.error(f"Check channel admin error: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


@app.post("/api/encodeVideo")
async def encode_video(request: Request):
    """API endpoint for manual video encoding"""
    from utils.video_encoder import VIDEO_ENCODER
    
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    logger.info(f"encodeVideo {data}")
    
    try:
        file_path = data["file_path"]
        qualities = data["qualities"]  # List of qualities to encode
        encoding_id = getRandomID()
        
        # Start encoding in background
        asyncio.create_task(
            VIDEO_ENCODER.encode_video_manual(file_path, qualities, encoding_id)
        )
        
        return JSONResponse({"status": "ok", "encoding_id": encoding_id})
        
    except Exception as e:
        logger.error(f"Error starting video encoding: {e}")
        return JSONResponse({"status": str(e)})


@app.post("/api/getEncodingProgress")
async def get_encoding_progress(request: Request):
    """Get encoding progress for a specific encoding job"""
    from utils.video_encoder import VIDEO_ENCODER
    
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        encoding_id = data["encoding_id"]
        progress = VIDEO_ENCODER.get_encoding_progress(encoding_id)
        return JSONResponse({"status": "ok", "data": progress})
        
    except Exception as e:
        logger.error(f"Error getting encoding progress: {e}")
        return JSONResponse({"status": str(e)})


@app.post("/api/checkVideoEncodingSupport")
async def check_video_encoding_support(request: Request):
    """Check if video encoding is supported (FFmpeg available)"""
    from utils.video_encoder import VIDEO_ENCODER
    
    data = await request.json()
    
    if data["password"] != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    try:
        ffmpeg_available = VIDEO_ENCODER.check_ffmpeg()
        return JSONResponse({
            "status": "ok", 
            "ffmpeg_available": ffmpeg_available,
            "supported_qualities": list(VIDEO_ENCODER.resolutions.keys()) if ffmpeg_available else []
        })
        
    except Exception as e:
        logger.error(f"Error checking encoding support: {e}")
        return JSONResponse({"status": str(e)})



# ============================================================================
# TOKEN MANAGEMENT API
# ============================================================================

@app.post("/api/createShareToken")
async def create_share_token(request: Request):
    """Create a temporary share token for a file"""
    from utils.directoryHandler import DRIVE_DATA
    
    data = await request.json()
    
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    file_path = data.get("path")
    expiry_hours = data.get("expiry_hours", TOKEN_EXPIRY_HOURS)
    file_password = data.get("file_password")
    
    if not file_path:
        return JSONResponse({"status": "Path required"})
    
    try:
        DRIVE_DATA.get_file(file_path)
    except Exception:
        return JSONResponse({"status": "File not found"})
    
    token_info = create_access_token(file_path, expiry_hours, file_password)
    
    base_url = str(request.base_url).rstrip('/')
    share_url = f"{base_url}/secure/{token_info['token']}"
    if file_password:
        share_url += "?password=YOUR_PASSWORD"
    
    return JSONResponse({
        "status": "ok",
        "token": token_info["token"],
        "expires_at": token_info["expires_at"],
        "share_url": share_url,
        "password_protected": file_password is not None
    })


@app.post("/api/revokeShareToken")
async def revoke_share_token(request: Request):
    """Revoke a share token"""
    data = await request.json()
    
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    token = data.get("token")
    if token in ACCESS_TOKENS:
        del ACCESS_TOKENS[token]
        return JSONResponse({"status": "ok", "message": "Token revoked"})
    
    return JSONResponse({"status": "Token not found"})


# ============================================================================
# TAGS/CATEGORIES API
# ============================================================================

@app.post("/api/addTags")
async def add_tags(request: Request):
    """Add tags to a file"""
    from utils.directoryHandler import DRIVE_DATA
    
    data = await request.json()
    
    if data.get("password") != ADMIN_PASSWORD:
        return JSONResponse({"status": "Invalid password"})
    
    file_path = data.get("path")
    tags = data.get("tags", [])
    
    if not file_path:
        return JSONResponse({"status": "Path required"})
    
    try:
        file = DRIVE_DATA.get_file(file_path)
        add_tags_to_file(file.id, tags)
        return JSONResponse({"status": "ok", "tags": get_file_tags(file.id)})
    except Exception as e:
        return JSONResponse({"status": str(e)})


@app.post("/api/getTags")
async def get_tags(request: Request):
    """Get tags for a file"""
    from utils.directoryHandler import DRIVE_DATA
    
    data = await request.json()
    file_path = data.get("path")
    
    if not file_path:
        return JSONResponse({"status": "Path required"})
    
    try:
        file = DRIVE_DATA.get_file(file_path)
        return JSONResponse({"status": "ok", "tags": get_file_tags(file.id)})
    except Exception:
        return JSONResponse({"status": "File not found"})


@app.post("/api/searchByTags")
async def search_tags(request: Request):
    """Search files by tags"""
    data = await request.json()
    tags = data.get("tags", [])
    
    if not tags:
        return JSONResponse({"status": "Tags required"})
    
    file_ids = search_by_tags(tags)
    return JSONResponse({"status": "ok", "file_ids": file_ids, "count": len(file_ids)})


@app.get("/api/allTags")
async def get_all_tags():
    """Get all unique tags in the system"""
    all_tags = set()
    for tags in FILE_TAGS.values():
        all_tags.update(tags)
    return JSONResponse({"status": "ok", "tags": sorted(list(all_tags))})


# ============================================================================
# ENHANCED SEARCH API
# ============================================================================

@app.post("/api/search")
async def enhanced_search(request: Request):
    """
    Enhanced search endpoint supporting:
    - Filename search
    - Tag search
    - Combined search
    """
    from utils.directoryHandler import DRIVE_DATA
    
    data = await request.json()
    query = data.get("query", "").strip()
    tags = data.get("tags", [])
    file_type = data.get("type")
    
    results = []
    
    if query:
        name_results = DRIVE_DATA.search_file_folder(query)
        for file_id, file_obj in name_results.items():
            if file_obj.type == "file":
                results.append({
                    "id": file_obj.id,
                    "name": file_obj.name,
                    "size": file_obj.size,
                    "path": file_obj.path,
                    "tags": get_file_tags(file_obj.id)
                })
    
    if tags:
        tag_file_ids = search_by_tags(tags)
        if query:
            results = [r for r in results if r["id"] in tag_file_ids]
        else:
            for file_id in tag_file_ids:
                results.append({
                    "id": file_id,
                    "tags": get_file_tags(file_id)
                })
    
    if file_type:
        type_extensions = {
            "video": [".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"],
            "audio": [".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a"],
            "document": [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt"],
            "image": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"]
        }
        if file_type in type_extensions:
            exts = type_extensions[file_type]
            results = [r for r in results if any(r.get("name", "").lower().endswith(ext) for ext in exts)]
    
    return JSONResponse({
        "status": "ok",
        "results": results,
        "count": len(results)
    })


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse({
        "status": "ok",
        "version": "2.0.0",
        "features": ["streaming", "range-requests", "tokens", "tags", "search"]
    })