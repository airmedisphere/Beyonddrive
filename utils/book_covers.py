"""
Book Cover Generation
======================
Generates a cover image for a book that doesn't have one yet.

Strategy per format:
  - PDF                -> render page 1 with PyMuPDF (fitz)
  - EPUB / MOBI / AZW3  -> extract the embedded cover via Calibre's
                           `ebook-meta --get-cover`; if the book has no
                           embedded cover, try rendering page 1 with
                           PyMuPDF (it can open EPUB too) as a second
                           attempt before giving up
  - DJVU               -> render page 1 with `ddjvu` (djvulibre-bin) and
                           convert the result to JPEG
  - anything else /     -> a generated title card (accent color + title +
    any failed attempt     author), so "Generate cover" always produces
                           something usable instead of leaving the book
                           coverless.

Requires (already used elsewhere in this project, or added alongside this
feature): PyMuPDF (pip), Pillow (pip), `ebook-meta` (from Calibre, already
installed for the reader-conversion feature), `ddjvu` (from djvulibre-bin,
already installed for the reader-conversion feature).
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image, ImageDraw, ImageFont

from utils.logger import Logger

logger = Logger(__name__)

COVER_TIMEOUT_SECONDS = 60
# Roughly a 3:4 book-cover aspect ratio, big enough to look sharp on retina
# screens but small enough to stay a lightweight Telegram document.
COVER_SIZE = (900, 1200)


async def _run(cmd: list) -> Tuple[bool, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=COVER_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            proc.kill()
            return False, "Cover generation timed out"
        if proc.returncode != 0:
            return False, (stderr or stdout or b"").decode(errors="ignore")[:500]
        return True, ""
    except FileNotFoundError:
        return False, f"Required tool not installed: {cmd[0]}"
    except Exception as e:
        return False, str(e)


def _save_jpeg_fit(img: Image.Image, out_path: Path) -> None:
    """Downscale to COVER_SIZE (contain, never upscale) and save as JPEG."""
    img = img.convert("RGB")
    img.thumbnail(COVER_SIZE, Image.LANCZOS)
    img.save(out_path, "JPEG", quality=88)


async def _render_first_page_with_fitz(source_path: str, out_path: Path) -> Tuple[bool, str]:
    """Render page 1 of a PDF (or EPUB, which PyMuPDF can also open) to a
    JPEG. Runs the (blocking, C-extension) work in a thread."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return False, "PyMuPDF not installed"

    def _do():
        doc = fitz.open(source_path)
        try:
            if doc.page_count == 0:
                return False, "Document has no pages"
            page = doc.load_page(0)
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), colorspace=fitz.csRGB)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            _save_jpeg_fit(img, out_path)
            return True, ""
        finally:
            doc.close()

    try:
        return await asyncio.to_thread(_do)
    except Exception as e:
        return False, str(e)


async def _extract_calibre_cover(source_path: str, out_path: Path) -> Tuple[bool, str]:
    """Pull the embedded cover image out of an EPUB/MOBI/AZW3 with Calibre.
    Returns (False, reason) both on tool errors and on the very common case
    of "this book just doesn't have an embedded cover" — callers should
    treat both the same way (try the next fallback)."""
    if not shutil.which("ebook-meta"):
        return False, "ebook-meta not installed"

    tmp_cover = out_path.with_suffix(".raw.jpg")
    ok, err = await _run(["ebook-meta", source_path, "--get-cover", str(tmp_cover)])
    if not ok:
        return False, err
    if not tmp_cover.exists() or tmp_cover.stat().st_size == 0:
        return False, "No embedded cover in file"
    try:
        img = await asyncio.to_thread(Image.open, tmp_cover)
        await asyncio.to_thread(img.load)
        await asyncio.to_thread(_save_jpeg_fit, img, out_path)
        return True, ""
    except Exception as e:
        return False, str(e)
    finally:
        try:
            tmp_cover.unlink(missing_ok=True)
        except Exception:
            pass


async def _render_djvu_first_page(source_path: str, out_path: Path) -> Tuple[bool, str]:
    if not shutil.which("ddjvu"):
        return False, "ddjvu not installed"

    tmp_ppm = out_path.with_suffix(".raw.ppm")
    ok, err = await _run(["ddjvu", "-format=ppm", "-page=1", source_path, str(tmp_ppm)])
    if not ok:
        return False, err
    if not tmp_ppm.exists() or tmp_ppm.stat().st_size == 0:
        return False, "ddjvu produced no output"
    try:
        img = await asyncio.to_thread(Image.open, tmp_ppm)
        await asyncio.to_thread(img.load)
        await asyncio.to_thread(_save_jpeg_fit, img, out_path)
        return True, ""
    except Exception as e:
        return False, str(e)
    finally:
        try:
            tmp_ppm.unlink(missing_ok=True)
        except Exception:
            pass


def _pick_font(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]
    for c in candidates:
        if Path(c).exists():
            try:
                return ImageFont.truetype(c, size)
            except Exception:
                pass
    return ImageFont.load_default()


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font, max_width: int, max_lines: int) -> list:
    words = text.split()
    lines, cur = [], ""
    for word in words:
        test = (cur + " " + word).strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width or not cur:
            cur = test
        else:
            lines.append(cur)
            cur = word
        if len(lines) == max_lines:
            break
    if cur and len(lines) < max_lines:
        lines.append(cur)
    return lines


def _placeholder_card(title: str, author: str, out_path: Path) -> None:
    """A clean, deliberately-designed generated cover for books we can't
    render a real page/embedded cover for (plain TXT, or any format whose
    render attempt failed). Color is derived from the title so different
    books get visually distinct cards rather than all looking identical."""
    w, h = COVER_SIZE
    seed = int(hashlib.sha256((title or "Untitled").encode("utf-8")).hexdigest(), 16)
    palette = [
        ((30, 41, 59), (99, 102, 241)),   # slate / indigo
        ((15, 23, 42), (56, 189, 248)),   # navy / sky
        ((24, 24, 27), (244, 114, 182)),  # zinc / pink
        ((20, 30, 25), (74, 222, 128)),   # forest / green
        ((36, 24, 12), (251, 191, 36)),   # brown / amber
        ((30, 20, 40), (192, 132, 252)),  # plum / violet
    ]
    bg, accent = palette[seed % len(palette)]

    img = Image.new("RGB", (w, h), bg)
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, w, 16], fill=accent)
    draw.rectangle([0, h - 16, w, h], fill=accent)

    margin = 80
    max_width = w - 2 * margin
    title_font = _pick_font(64)
    author_font = _pick_font(34)

    title_lines = _wrap_text(draw, title or "Untitled", title_font, max_width, 6)
    line_height = title_font.size + 16
    total_h = len(title_lines) * line_height
    y = h // 2 - total_h // 2 - 20

    for line in title_lines:
        bbox = draw.textbbox((0, 0), line, font=title_font)
        x = (w - (bbox[2] - bbox[0])) // 2
        draw.text((x, y), line, font=title_font, fill=(241, 245, 249))
        y += line_height

    if author:
        author_lines = _wrap_text(draw, author, author_font, max_width, 2)
        y += 24
        for line in author_lines:
            bbox = draw.textbbox((0, 0), line, font=author_font)
            x = (w - (bbox[2] - bbox[0])) // 2
            draw.text((x, y), line, font=author_font, fill=accent)
            y += author_font.size + 10

    img.save(out_path, "JPEG", quality=90)


async def generate_cover_image(
    source_path: str, ext: str, title: str = "", author: str = ""
) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate a cover JPEG for a book file already downloaded to disk.

    Returns (cover_path, error). `error` is only set if something raised
    unexpectedly; a book we simply can't render a "real" page for (TXT, or
    a PDF/EPUB/DJVU whose render attempt failed) still gets a generated
    title-card cover rather than an error, so "Generate cover" always
    leaves the book with *something* rather than nothing.

    Caller is responsible for deleting the returned file after uploading it.
    """
    ext = ext.lower()
    fd, tmp_name = tempfile.mkstemp(suffix=".jpg")
    os.close(fd)
    out_path = Path(tmp_name)

    try:
        ok, err = False, "Unsupported format for page rendering"

        if ext == ".pdf":
            ok, err = await _render_first_page_with_fitz(source_path, out_path)
        elif ext in (".epub", ".mobi", ".azw3"):
            ok, err = await _extract_calibre_cover(source_path, out_path)
            if not ok:
                try:
                    ok2, err2 = await _render_first_page_with_fitz(source_path, out_path)
                    if ok2:
                        ok, err = True, ""
                except Exception:
                    pass
        elif ext == ".djvu":
            ok, err = await _render_djvu_first_page(source_path, out_path)

        if not ok:
            logger.info(f"No page render available for {ext} ({err}); using a generated title card")
            await asyncio.to_thread(
                _placeholder_card, title or Path(source_path).stem, author, out_path
            )

        return str(out_path), None
    except Exception as e:
        logger.error(f"Cover generation crashed: {e}")
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, str(e)
