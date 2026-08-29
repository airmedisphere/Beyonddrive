"""
Book Converter
==============
Converts ebook formats that the in-browser reader can't render natively
(MOBI, AZW3, DJVU) into a format it can (EPUB or PDF), so every uploaded
format ends up readable on the website, not just downloadable.

Requires these CLI tools to be present in the runtime image:
  - `ebook-convert` (from Calibre)     -> used for MOBI / AZW3 -> EPUB
  - `ddjvu`         (from djvulibre-bin) -> used for DJVU -> PDF

Both are invoked as subprocesses. If a tool isn't installed, conversion
fails gracefully (tools_available() lets callers check up front) and the
book simply falls back to download-only in the UI.
"""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Optional, Tuple

from utils.logger import Logger

logger = Logger(__name__)

CONVERT_TIMEOUT_SECONDS = 180

# Which reader-native format each source extension converts to.
_CONVERSION_TARGETS = {
    ".mobi": "epub",
    ".azw3": "epub",
    ".djvu": "pdf",
}

# Formats the reader already handles with zero conversion.
NATIVE_READER_FORMATS = {".pdf": "pdf", ".epub": "epub", ".txt": "txt"}


def needs_conversion(ext: str) -> bool:
    return ext.lower() in _CONVERSION_TARGETS


def target_format_for(ext: str) -> Optional[str]:
    return _CONVERSION_TARGETS.get(ext.lower())


def tools_available() -> dict:
    """Report which converter binaries are actually installed."""
    return {
        "ebook-convert": shutil.which("ebook-convert") is not None,
        "ddjvu": shutil.which("ddjvu") is not None,
    }


async def _run(cmd: list) -> Tuple[bool, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=CONVERT_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            proc.kill()
            return False, "Conversion timed out"

        if proc.returncode != 0:
            return False, (stderr or stdout or b"").decode(errors="ignore")[:500]
        return True, ""
    except FileNotFoundError:
        return False, f"Required tool not installed: {cmd[0]}"
    except Exception as e:
        return False, str(e)


async def convert_book(
    source_path: str, ext: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Convert `source_path` (a MOBI/AZW3/DJVU file) into a reader-friendly
    format.

    Returns (output_path, output_format, error).
      - On success: error is None, output_path points at a new temp file
        the caller is responsible for deleting once it's uploaded.
      - On failure: output_path/output_format are None and error explains
        what went wrong (missing tool, bad file, timeout, etc).
    """
    ext = ext.lower()
    target = target_format_for(ext)
    if not target:
        return None, None, f"No conversion path defined for {ext}"

    src = Path(source_path)
    out_path = src.with_name(src.stem + f".converted.{target}")

    if ext in (".mobi", ".azw3"):
        ok, err = await _run(["ebook-convert", str(src), str(out_path)])
    elif ext == ".djvu":
        ok, err = await _run(["ddjvu", "-format=pdf", str(src), str(out_path)])
    else:
        return None, None, f"No conversion path defined for {ext}"

    if not ok:
        logger.error(f"Book conversion failed for {src.name}: {err}")
        return None, None, err or "Conversion failed"

    if not out_path.exists() or out_path.stat().st_size == 0:
        return None, None, "Conversion produced no output file"

    return str(out_path), target, None
