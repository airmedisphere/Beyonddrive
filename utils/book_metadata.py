"""
Book Metadata Extraction
========================
Two independent ways to work out what an imported book actually *is*, used
by utils/books_import.py when pulling a whole channel into BOOKS_CHANNEL.

1. parse_filename()  — pure string heuristics. Zero I/O, zero memory risk.
   Turns "Clear, James - Atomic Habits (2018) [Z-Library].pdf" into
   {title: "Atomic Habits", author: "James Clear", year: "2018"}.
   Runs for every imported file.

2. read_embedded_metadata() — opens the actual file and reads the real
   title/author/language the publisher put in it. More accurate, but needs
   the file on local disk, so it only runs in the enrichment pass which
   downloads books one at a time (see books_import.py). Deliberately reads
   *only* metadata, never renders or parses page content:
     PDF        -> PyMuPDF trailer/XMP read (no page rendering)
     EPUB       -> zipfile, read the OPF XML entry only (a few KB)
     MOBI/AZW3  -> `ebook-meta` subprocess (Calibre, already in the image)
     TXT/DJVU   -> nothing to read, returns {}

Everything here is best-effort: a failure returns an empty dict rather than
raising, because bad metadata must never be the reason a book fails to
import.
"""

from __future__ import annotations

import asyncio
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Dict, Optional

from utils.logger import Logger

logger = Logger(__name__)

# Reading embedded metadata means opening the file. PDFs/EPUBs are opened
# lazily (we never touch page content) so this is cheap, but a pathological
# file can still cost real memory on a 512MB instance — above this size we
# skip straight to filename-derived metadata.
MAX_METADATA_SOURCE_BYTES = 150 * 1024 * 1024  # 150MB

EBOOK_META_TIMEOUT = 45

# ── Filename noise ────────────────────────────────────────────────────────────
# Bracketed junk that ebook sites staple onto filenames. Removed before any
# author/title parsing so it can't be mistaken for either.
_NOISE_PATTERNS = [
    r"\(\s*z-?library\s*\)",
    r"\[\s*z-?library\s*\]",
    r"\(\s*annas?[-_ ]archive\s*\)",
    r"\[\s*annas?[-_ ]archive\s*\]",
    r"\(\s*libgen[^)]*\)",
    r"\[\s*libgen[^\]]*\]",
    r"\(\s*pdfdrive[^)]*\)",
    r"\[\s*pdfdrive[^\]]*\]",
    # Underscores are word characters, so \b won't fire next to them — every
    # pattern here therefore runs *after* underscores become spaces (see
    # parse_filename), otherwise "..._www.pdfdrive.com" leaks a stray "www".
    r"(?<![a-z0-9])(?:www\.)?[a-z0-9-]+\.(?:com|net|org|in|io|me|co|info|xyz)\b",
    r"@[A-Za-z0-9_]{4,}",          # trailing @channel credit
    r"\[\s*(?:ebook|e-book|retail|scan|ocr|dpi\d+)\s*\]",
    r"\((?:\s*(?:ebook|e-book|retail|scan|ocr)\s*)\)",
    r"\b(?:epub|mobi|azw3|pdf|djvu)\b\s*$",
    r"\bfree\s+download\b",
]


# "1st Edition", "2nd ed.", "Revised Edition" — noise for the title, but we
# keep it out of the author field rather than deleting it outright.
_EDITION_RE = re.compile(
    r"\b(\d+(?:st|nd|rd|th)|first|second|third|fourth|revised|annotated)"
    r"\s+(?:ed\.?|edition)\b",
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r"[\(\[\{]?\b(1[5-9]\d{2}|20[0-4]\d)\b[\)\]\}]?")

# Words that strongly suggest a string is a *title* rather than a person's
# name. Articles/prepositions plus the common nouns and adjectives that show
# up in book titles. Purely a heuristic tiebreaker — for PDF/EPUB the
# enrichment pass overrides whatever this guesses with the publisher's real
# metadata (see merge_metadata).
_TITLE_WORD_HINTS = {
    "a", "an", "and", "are", "art", "as", "at", "be", "beginners", "beginner",
    "book", "chapter", "code", "complete", "design", "development", "essential",
    "for", "from", "guide", "habits", "handbook", "history", "how", "in",
    "into", "introduction", "is", "it", "its", "language", "learning", "life",
    "manual", "mastering", "mind", "modern", "money", "notes", "of", "on",
    "or", "part", "people", "power", "practical", "principles", "programming",
    "reference", "rules", "science", "secrets", "series", "story", "summary",
    "system", "systems", "the", "theory", "things", "thinking", "to", "vol",
    "volume", "ways", "what", "when", "why", "with", "work", "world", "your",
    "you",
}

# Positive evidence in the other direction: a compact set of very common
# given names. Not remotely exhaustive — it only needs to break ties that
# capitalisation alone can't, and a miss just falls back to the dominant
# "Author - Title" filename convention.
_GIVEN_NAMES = {
    "aaron", "adam", "alan", "albert", "alex", "alexander", "alice", "amy",
    "andrew", "ann", "anna", "anne", "anthony", "arthur", "barbara", "ben",
    "benjamin", "bill", "bob", "brian", "bruce", "carl", "carol", "charles",
    "chris", "christopher", "clara", "daniel", "dave", "david", "dean",
    "deborah", "dennis", "diana", "donald", "douglas", "edward", "elizabeth",
    "emily", "emma", "eric", "frank", "fred", "gary", "george", "gordon",
    "greg", "harry", "helen", "henry", "ian", "isaac", "jack", "james", "jane",
    "janet", "jason", "jeff", "jeffrey", "jennifer", "jeremy", "jerry",
    "jessica", "jim", "joan", "joe", "john", "jonathan", "jordan", "joseph",
    "joshua", "julia", "karen", "keith", "ken", "kenneth", "kevin", "kim",
    "larry", "laura", "laurence", "lawrence", "linda", "lisa", "louis", "luke",
    "margaret", "maria", "marie", "mark", "martin", "mary", "matthew",
    "michael", "michelle", "mike", "nancy", "nathan", "neil", "nicholas",
    "nick", "oliver", "patricia", "patrick", "paul", "peter", "philip",
    "rachel", "ralph", "raymond", "rebecca", "richard", "rick", "robert",
    "roger", "ronald", "rose", "roy", "ruth", "ryan", "sam", "samuel", "sandra",
    "sarah", "scott", "sean", "sharon", "simon", "stephen", "steve", "steven",
    "susan", "tara", "ted", "teresa", "thomas", "tim", "timothy", "todd",
    "tom", "tony", "victor", "vincent", "walter", "wayne", "william", "yuval",
}

# "Tolkien, J.R.R." / "Clear, James" — the "Surname, Forename" convention.
# The forename half is deliberately restricted to name-shaped text (words
# starting with a capital, or initials) so that an ordinary title containing
# a comma — "Thinking, Fast and Slow" — can't be misread as a name.
_SURNAME_FIRST_RE = re.compile(
    r"^([A-Z][\w'’-]+(?:\s+[A-Z][\w'’-]+)?)\s*,\s*"
    r"((?:[A-Z](?:\.|[\w'’-]*)\s*){1,3})$"
)

# Separator between author and title. The group is captured so parse_filename
# can tell a directional separator (" by " always means title-by-author) from
# an ambiguous dash.
_SPLIT_RE = re.compile(r"(\s+-\s+|\s+–\s+|\s+—\s+|\s+_\s+|\s+by\s+)", re.IGNORECASE)



def _strip_noise(text: str) -> str:
    for pat in _NOISE_PATTERNS:
        text = re.sub(pat, " ", text, flags=re.IGNORECASE)
    return text


def _tidy(text: str) -> str:
    """Collapse separators/whitespace and trim leftover punctuation."""
    text = text.replace("_", " ").replace("+", " ")
    # Only treat dots as separators when they're clearly standing in for
    # spaces ("Atomic.Habits") — never inside initials ("J.R.R.").
    text = re.sub(r"(?<=[a-z])\.(?=[A-Za-z])", " ", text)
    text = re.sub(r"\s*[\(\[\{]\s*[\)\]\}]\s*", " ", text)   # empty brackets
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -–—_.,;:·[](){}")


def _titlecase_if_shouting(text: str) -> str:
    """"ATOMIC HABITS" -> "Atomic Habits"; leave normal casing alone so
    deliberate capitalisation ("iRobot", "NASA") survives untouched."""
    letters = [c for c in text if c.isalpha()]
    if len(letters) >= 4 and all(c.isupper() for c in letters):
        return " ".join(w.capitalize() for w in text.split())
    return text


def _name_score(text: str) -> int:
    """
    How much this string looks like a person's name rather than a book title.
    Higher = more name-like; negative = clearly a title. Used only to decide
    which side of "X - Y" is the author.
    """
    text = _tidy(text)
    if not text:
        return -99
    # Digits and long strings are titles, not names.
    if any(ch.isdigit() for ch in text) or len(text) > 60:
        return -50

    words = [w for w in text.replace(".", " ").split() if w]
    if not words or len(words) > 5:
        return -20

    score = 0
    lowered = [w.lower().strip(".'’-") for w in words]

    title_hits = sum(1 for w in lowered if w in _TITLE_WORD_HINTS)
    score -= title_hits * 6

    if any(w in _GIVEN_NAMES for w in lowered):
        score += 8

    # Initials ("J.R.R.", "C.") are a strong name signal.
    if re.search(r"\b[A-Z]\.", text):
        score += 5

    capitalised = sum(1 for w in words if w[:1].isupper())
    if capitalised == len(words):
        score += 3
    elif capitalised >= len(words) - 1:
        score += 1
    else:
        score -= 4

    # Two or three capitalised words with no title vocabulary is the classic
    # "Firstname Lastname" / "Firstname M. Lastname" shape.
    if 2 <= len(words) <= 3 and title_hits == 0:
        score += 3

    return score


def _looks_like_author(text: str) -> bool:
    return _name_score(text) > 0



def _tidy_name(text: str) -> str:
    """
    _tidy() for a person's name, with initials normalised back to a canonical
    dotted form. _tidy() strips trailing punctuation, which turns "J.R.R."
    into "J.R.R" — correct for titles, wrong for a name.
    """
    text = _tidy(text)
    if not text:
        return ""
    words = []
    for word in text.split():
        # A run of single letters, dotted or not ("J.R.R", "JRR", "C") is an
        # initials block; render it uniformly as "J.R.R.".
        letters = word.replace(".", "")
        if letters and len(letters) <= 3 and letters.isupper() and letters.isalpha():
            words.append("".join(f"{c}." for c in letters))
        else:
            words.append(word)
    return " ".join(words)


def _fmt_surname_first(match: "re.Match") -> str:
    """Build "Forename Surname" from a "Surname, Forename" match."""
    return _tidy_name(f"{match.group(2)} {match.group(1)}")


def parse_filename(filename: str) -> Dict[str, str]:
    """
    Derive {title, author, year} from a filename. Pure string work — no I/O,
    no memory cost — so this runs for every file in an import.

    Handles the common ebook-dump conventions:
        "Author - Title.pdf"
        "Title - Author.epub"
        "Surname, Forename - Title (2019).pdf"
        "Title by Author.pdf"
        "Title_(2019)_[Z-Library].pdf"
    Anything it can't confidently split becomes the title, with the author
    left empty — a wrong author is worse than a missing one, since the
    enrichment pass or a human can still fill it in later.
    """
    stem = Path(filename or "").stem
    if not stem:
        return {}

    stem = _strip_noise(stem)

    year = ""
    year_match = _YEAR_RE.search(stem)
    if year_match:
        year = year_match.group(1)
        stem = stem[: year_match.start()] + " " + stem[year_match.end():]

    edition = ""
    ed_match = _EDITION_RE.search(stem)
    if ed_match:
        edition = ed_match.group(0)
        stem = stem[: ed_match.start()] + " " + stem[ed_match.end():]

    stem = _tidy(stem)
    if not stem:
        return {}

    title, author = stem, ""

    # Split on the first separator, keeping it so " by " (which is
    # unambiguously "title by author") can be told apart from a dash.
    split = _SPLIT_RE.split(stem, maxsplit=1)
    if len(split) == 3:
        left, sep, right = _tidy(split[0]), split[1], _tidy(split[2])
    else:
        left = sep = right = ""

    if left and right:
        left_sf = _SURNAME_FIRST_RE.match(left)
        right_sf = _SURNAME_FIRST_RE.match(right)

        if "by" in sep.lower():
            # "Atomic Habits by James Clear" — direction is explicit.
            title, author = left, right
            if right_sf:
                author = _fmt_surname_first(right_sf)
        elif left_sf:
            # "Clear, James - Atomic Habits"
            author = _fmt_surname_first(left_sf)
            title = right
        elif right_sf:
            # "The Lord of the Rings - Tolkien, J.R.R."
            title = left
            author = _fmt_surname_first(right_sf)
        else:
            left_score, right_score = _name_score(left), _name_score(right)
            # Only accept a split when one side actually looks like a person.
            # Otherwise it's one long title that happened to contain a dash
            # ("Part 1 - Introduction") and inventing an author out of half of
            # it would be worse than leaving the author blank.
            if right_score > left_score and right_score > 0:
                title, author = left, right
            elif left_score > right_score and left_score > 0:
                author, title = left, right
            elif left_score > 0 and left_score == right_score:
                # Genuinely ambiguous and both name-like: "Author - Title" is
                # the dominant convention in bulk ebook dumps.
                author, title = left, right
            else:
                title = f"{left} - {right}"

    # A bare "Surname, Forename" and nothing else. Only treat it as a name
    # when it really looks like one — "Thinking, Fast and Slow" must stay a
    # title.
    if not author:
        sf = _SURNAME_FIRST_RE.match(title)
        if sf:
            candidate = _fmt_surname_first(sf)
            if _name_score(candidate) > 0:
                author = candidate
                title = ""

    result = {}
    title = _titlecase_if_shouting(_tidy(title))
    author = _titlecase_if_shouting(_tidy_name(author))

    # Re-attach the edition after the final tidy — _tidy strips trailing
    # brackets, which would otherwise leave "Deep Work (1st Edition".
    if edition and title:
        title = f"{title} ({_tidy(edition)})"

    if title:
        result["title"] = title
    if author:
        result["author"] = author
    if year:
        result["year"] = year
    return result


# ── Embedded metadata ─────────────────────────────────────────────────────────

def _clean_meta_value(value: Optional[str]) -> str:
    """Normalise a metadata string and reject the useless defaults tools emit."""
    if not value:
        return ""
    value = _tidy(str(value))
    if not value or len(value) > 300:
        return ""
    if value.lower() in {
        "unknown", "untitled", "none", "n/a", "na", "null",
        "calibre", "microsoft word", "adobe acrobat", "anonymous",
        "no author", "unknown author", "default", "title",
    }:
        return ""
    # Some PDFs carry the source filename as the title — no better than what
    # parse_filename() already worked out, and usually messier.
    if re.search(r"\.(pdf|epub|mobi|azw3|docx?|indd|txt)$", value, re.IGNORECASE):
        return ""
    return value


def _read_pdf_metadata(path: str) -> Dict[str, str]:
    """PyMuPDF metadata dict only — the document is opened but no page is
    ever loaded or rendered, so this stays cheap even for a large PDF."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return {}

    doc = None
    try:
        doc = fitz.open(path)
        meta = doc.metadata or {}
        out = {}
        title = _clean_meta_value(meta.get("title"))
        author = _clean_meta_value(meta.get("author"))
        if title:
            out["title"] = title
        if author:
            out["author"] = author
        kw = _clean_meta_value(meta.get("keywords"))
        if kw:
            out["keywords"] = kw
        try:
            out["pages"] = str(doc.page_count)
        except Exception:
            pass
        return out
    except Exception as e:
        logger.warning(f"PDF metadata read failed for {os.path.basename(path)}: {e}")
        return {}
    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass


_OPF_NS = {
    "opf": "http://www.idpf.org/2007/opf",
    "dc": "http://purl.org/dc/elements/1.1/",
}


def _read_epub_metadata(path: str) -> Dict[str, str]:
    """
    Read only the OPF package document out of the EPUB zip. An EPUB is just
    a zip, and the OPF is a few KB of XML, so this never loads the book's
    actual content into memory regardless of how big the file is.
    """
    try:
        with zipfile.ZipFile(path) as zf:
            # container.xml points at the OPF; fall back to scanning names.
            opf_name = None
            try:
                container = zf.read("META-INF/container.xml")
                root = ET.fromstring(container)
                for rf in root.iter():
                    if rf.tag.endswith("rootfile"):
                        opf_name = rf.attrib.get("full-path")
                        break
            except Exception:
                pass

            if not opf_name:
                opf_name = next(
                    (n for n in zf.namelist() if n.lower().endswith(".opf")), None
                )
            if not opf_name:
                return {}

            # Guard: a malformed/hostile OPF entry shouldn't be read into RAM.
            info = zf.getinfo(opf_name)
            if info.file_size > 4 * 1024 * 1024:
                return {}

            opf_root = ET.fromstring(zf.read(opf_name))

        out: Dict[str, str] = {}

        def _first_text(tag: str) -> str:
            node = opf_root.find(f".//dc:{tag}", _OPF_NS)
            if node is None:
                node = next(
                    (el for el in opf_root.iter() if el.tag.endswith(f"}}{tag}")),
                    None,
                )
            return _clean_meta_value(node.text if node is not None else "")

        title = _first_text("title")
        if title:
            out["title"] = title

        # Prefer creators explicitly roled as author; several EPUBs list
        # illustrators/editors as additional dc:creator entries.
        authors = []
        for el in opf_root.iter():
            if not el.tag.endswith("}creator"):
                continue
            role = ""
            for key, val in el.attrib.items():
                if key.endswith("role"):
                    role = (val or "").lower()
            if role and role != "aut":
                continue
            name = _clean_meta_value(el.text)
            if name and name not in authors:
                authors.append(name)
        if authors:
            out["author"] = ", ".join(authors[:3])

        lang = _first_text("language")
        if lang:
            out["language"] = lang.split("-")[0].lower()[:5]

        desc = _first_text("description")
        if desc:
            # EPUB descriptions are often HTML — strip tags, cap the length.
            desc = _tidy(re.sub(r"<[^>]+>", " ", desc))
            if desc:
                out["description"] = desc[:2000]

        subjects = []
        for el in opf_root.iter():
            if el.tag.endswith("}subject"):
                s = _clean_meta_value(el.text)
                if s and s.lower() not in [x.lower() for x in subjects]:
                    subjects.append(s)
        if subjects:
            out["subjects"] = ", ".join(subjects[:8])

        return out
    except zipfile.BadZipFile:
        return {}
    except Exception as e:
        logger.warning(f"EPUB metadata read failed for {os.path.basename(path)}: {e}")
        return {}


async def _read_via_ebook_meta(path: str) -> Dict[str, str]:
    """
    MOBI/AZW3 have no simple container to peek into, so shell out to
    Calibre's `ebook-meta` (already installed in the Docker image for the
    reader conversion feature). Runs as a separate process, so its memory
    use is bounded by the OS and released the moment it exits — safer than
    parsing these formats in-process.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "ebook-meta", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning(f"ebook-meta could not start: {e}")
        return {}

    try:
        stdout, _ = await asyncio.wait_for(
            proc.communicate(), timeout=EBOOK_META_TIMEOUT
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return {}
    except Exception:
        return {}

    if proc.returncode != 0:
        return {}

    out: Dict[str, str] = {}
    field_map = {
        "title": "title",
        "author(s)": "author",
        "languages": "language",
        "comments": "description",
        "tags": "subjects",
    }
    for line in stdout.decode(errors="ignore").splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        field = field_map.get(key.strip().lower())
        if not field:
            continue
        # ebook-meta renders unknown authors as "Unknown"; _clean_meta_value
        # already drops that. It also appends "[Surname, Forename]" after a
        # normalised author name — keep just the display form.
        value = re.sub(r"\s*\[[^\]]*\]\s*$", "", value.strip())
        cleaned = _clean_meta_value(value)
        if not cleaned:
            continue
        if field == "language":
            cleaned = cleaned.split(",")[0].split("-")[0].strip().lower()[:5]
        if field == "description":
            cleaned = _tidy(re.sub(r"<[^>]+>", " ", cleaned))[:2000]
        if cleaned:
            out[field] = cleaned
    return out


async def read_embedded_metadata(path: str, ext: str) -> Dict[str, str]:
    """
    Best-effort real metadata from the file itself. Returns {} on any
    failure, unsupported format, or oversized source — callers fall back to
    whatever parse_filename() produced.

    Keys that may be present: title, author, language, description,
    subjects, keywords, pages.

    Blocking readers (PyMuPDF, zipfile) run in a worker thread so they never
    stall the event loop while a stream is being served.
    """
    ext = (ext or "").lower()
    try:
        if not os.path.exists(path):
            return {}
        if os.path.getsize(path) > MAX_METADATA_SOURCE_BYTES:
            logger.info(
                f"Skipping embedded metadata for oversized file "
                f"({os.path.getsize(path)} bytes): {os.path.basename(path)}"
            )
            return {}
    except OSError:
        return {}

    try:
        if ext == ".pdf":
            return await asyncio.to_thread(_read_pdf_metadata, path)
        if ext == ".epub":
            return await asyncio.to_thread(_read_epub_metadata, path)
        if ext in {".mobi", ".azw3"}:
            return await _read_via_ebook_meta(path)
    except Exception as e:
        logger.warning(f"Embedded metadata read failed ({ext}): {e}")
    # .txt / .djvu carry no metadata worth reading.
    return {}


def merge_metadata(
    from_filename: Dict[str, str], embedded: Dict[str, str]
) -> Dict[str, str]:
    """
    Combine both sources into the fields a Book actually stores.

    Embedded metadata wins on title/author/language when present — it came
    from the publisher rather than from guessing at a filename. Filename
    output is the fallback, and the year it found is kept either way since
    embedded metadata rarely carries one usefully.
    """
    merged: Dict[str, str] = {}

    for field in ("title", "author", "language", "description"):
        value = _clean_meta_value(embedded.get(field)) or from_filename.get(field, "")
        if value:
            merged[field] = value

    tags = []
    for source in (embedded.get("subjects", ""), embedded.get("keywords", "")):
        for raw in re.split(r"[,;/]", source or ""):
            tag = _tidy(raw).lower()
            # Long "subjects" entries are usually BISAC-style sentences, not
            # tags anyone would want to filter by.
            if tag and 2 <= len(tag) <= 30 and tag not in tags:
                tags.append(tag)
    if tags:
        merged["tags"] = ", ".join(tags[:8])

    if from_filename.get("year"):
        merged["year"] = from_filename["year"]

    return merged



