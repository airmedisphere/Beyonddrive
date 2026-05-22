"""
transcoder.py — Real-time FFmpeg transcoding for unsupported video formats.

Architecture:
  Browser → /api/transcodeStream → download from Telegram → pipe to FFmpeg → stream to browser

FFmpeg receives raw bytes via stdin, transcodes to fragmented MP4 (fMP4)
which is streamable without needing to seek — critical for pipe-based transcoding.

Supports: MKV, AVI, TS, FLV, WMV, 3GP, and any other format FFmpeg can read.
Audio tracks: select specific audio stream index for dual-audio files.
Seeking: use ?start=N to seek to N seconds (re-pipes from that point).
"""

import asyncio
import subprocess
import json
from utils.logger import Logger
from utils.clients import get_client
from utils.streamer.custom_dl import ByteStreamer
from utils.streamer import class_cache

logger = Logger(__name__)


async def get_file_audio_info(channel: int, message_id: int) -> dict:
    """
    Use ffprobe on a small sample of the file to detect:
    - Audio codec
    - Number of audio tracks
    - Whether transcoding is needed
    """
    client = get_client()
    
    if client not in class_cache:
        class_cache[client] = ByteStreamer(client)
    tg_connect = class_cache[client]

    try:
        file_id = await tg_connect.get_file_properties(channel, message_id)
    except Exception as e:
        logger.error(f"get_file_audio_info: failed to get file properties: {e}")
        return {"codec": "unknown", "needs_transcode": True, "audio_tracks": []}

    # Stream first 5MB to probe
    PROBE_BYTES = 5 * 1024 * 1024
    chunk_size  = 1024 * 1024
    probe_parts = min(5, (PROBE_BYTES // chunk_size))

    probe_data = b""
    try:
        async for chunk in client.stream_media(file_id.file_id, offset=0, limit=probe_parts):
            probe_data += chunk
            if len(probe_data) >= PROBE_BYTES:
                break
    except Exception as e:
        logger.error(f"Probe stream error: {e}")

    if not probe_data:
        return {"codec": "unknown", "needs_transcode": True, "audio_tracks": []}

    # Run ffprobe on the probe data
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-i", "pipe:0",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=probe_data),
            timeout=15
        )
        data = json.loads(stdout.decode())
        streams = data.get("streams", [])

        audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
        video_streams = [s for s in streams if s.get("codec_type") == "video"]

        audio_codec = audio_streams[0].get("codec_name", "unknown") if audio_streams else "unknown"
        video_codec = video_streams[0].get("codec_name", "unknown") if video_streams else "unknown"

        BROWSER_SAFE_AUDIO = {"aac", "mp3", "opus", "vorbis", "flac", "pcm_s16le", "pcm_u8"}
        BROWSER_SAFE_VIDEO = {"h264", "vp8", "vp9", "av1"}

        needs_transcode = (
            audio_codec not in BROWSER_SAFE_AUDIO or
            video_codec not in BROWSER_SAFE_VIDEO
        )

        audio_tracks = []
        for i, s in enumerate(audio_streams):
            tags  = s.get("tags", {})
            title = tags.get("title") or tags.get("language") or f"Track {i+1}"
            audio_tracks.append({"index": i, "label": title, "codec": s.get("codec_name")})

        return {
            "codec":          audio_codec,
            "video_codec":    video_codec,
            "needs_transcode": needs_transcode,
            "audio_tracks":   audio_tracks,
        }

    except Exception as e:
        logger.error(f"ffprobe error: {e}")
        return {"codec": "unknown", "needs_transcode": True, "audio_tracks": []}


async def transcode_stream(channel: int, message_id: int,
                           start_sec: float = 0,
                           audio_track: int = 0):
    """
    Async generator that:
    1. Streams file from Telegram
    2. Pipes bytes to FFmpeg stdin
    3. Yields FFmpeg stdout (fragmented MP4) to browser

    Yields bytes chunks suitable for StreamingResponse.
    """
    client = get_client()

    if client not in class_cache:
        class_cache[client] = ByteStreamer(client)
    tg_connect = class_cache[client]

    try:
        file_id_obj = await tg_connect.get_file_properties(channel, message_id)
    except Exception as e:
        logger.error(f"transcode_stream: failed to get file properties: {e}")
        return

    # ── FFmpeg command ────────────────────────────────────────────────────────
    # Key flags for pipe-based streaming:
    # -fflags +genpts     : generate PTS if missing (common in MKV/AVI)
    # -analyzeduration 0  : don't spend time analyzing (we want fast start)
    # -probesize 32       : minimal probe (fast start)
    # -ss start_sec       : seek BEFORE input (fast, keyframe seek)
    # -map 0:v:0          : first video stream
    # -map 0:a:N          : Nth audio stream
    # -c:v copy           : copy video if already H264 (no re-encode = fast)
    # -c:v libx264        : re-encode video if needed
    # -c:a aac            : always encode audio to AAC (browser compatible)
    # -movflags frag_keyframe+empty_moov+default_base_moof : fragmented MP4
    # -f mp4 pipe:1       : output to stdout

    # Try copy video first (fastest), fall back to re-encode
    ffmpeg_cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-fflags", "+genpts+discardcorrupt",
        "-analyzeduration", "1000000",   # 1 second max analysis
        "-probesize", "1000000",         # 1MB max probe
    ]

    if start_sec > 0:
        ffmpeg_cmd += ["-ss", str(start_sec)]

    ffmpeg_cmd += [
        "-i", "pipe:0",                   # read from stdin
        "-map", "0:v:0",                  # first video stream
        "-map", f"0:a:{audio_track}",     # selected audio stream
        "-c:v", "copy",                   # try to copy video (no re-encode)
        "-c:a", "aac",                    # always transcode audio to AAC
        "-b:a", "128k",
        "-ac", "2",                       # stereo
        "-movflags", "frag_keyframe+empty_moov+default_base_moof+faststart",
        "-f", "mp4",
        "pipe:1",                         # output to stdout
    ]

    logger.info(f"Starting transcode: msg={message_id}, start={start_sec}s, audio={audio_track}")

    try:
        proc = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        logger.error("FFmpeg not found")
        return

    # ── Feed Telegram stream to FFmpeg stdin in background ────────────────────
    async def feed_stdin():
        try:
            chunk_size = 1024 * 1024  # 1MB chunks
            async for chunk in client.stream_media(
                file_id_obj.file_id,
                offset=0,
                limit=9999,  # stream everything
            ):
                if proc.stdin.is_closing():
                    break
                proc.stdin.write(chunk)
                await proc.stdin.drain()
        except Exception as e:
            logger.debug(f"stdin feed ended: {e}")
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass

    feed_task = asyncio.create_task(feed_stdin())

    # ── Yield FFmpeg stdout to browser ────────────────────────────────────────
    OUT_CHUNK = 65536  # 64KB output chunks — good balance for streaming
    try:
        while True:
            chunk = await asyncio.wait_for(
                proc.stdout.read(OUT_CHUNK),
                timeout=30
            )
            if not chunk:
                break
            yield chunk
    except asyncio.TimeoutError:
        logger.error("FFmpeg output timeout")
    except Exception as e:
        logger.error(f"Transcode output error: {e}")
    finally:
        feed_task.cancel()
        try:
            proc.kill()
        except Exception:
            pass
        await proc.wait()
        logger.info(f"Transcode complete: msg={message_id}")
