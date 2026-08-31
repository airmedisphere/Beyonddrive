# Books Library API

This project now supports a **completely separate** books library that uses its own Telegram channel (`BOOKS_CHANNEL`).

Books uploaded via these endpoints **never appear** in the normal TGDrive file browser.

---

## Setup

1. Create a new **private Telegram channel** for books.
2. Add all your bot(s) as **administrators** of that channel.
3. Get the channel ID (forward a message from the channel to `@userinfobot` or use any ID bot → it looks like `-100xxxxxxxxxx`).
4. Set the environment variable on Render:

```env
BOOKS_CHANNEL=-100xxxxxxxxxx
```

Restart the service after adding the variable.

---

## Two ways to add a book

**1. From the website** — `POST /api/books/upload` sends the file to `BOOKS_CHANNEL`
and registers it immediately (see below).

**2. Directly in Telegram** — just send/forward a PDF, EPUB, MOBI, AZW3, TXT or
DJVU file straight into `BOOKS_CHANNEL`. A background listener watches the
channel and auto-registers any supported file within a couple of seconds, so
it shows up on the website automatically — no API call needed.

To set title/author/tags for a manual upload, add a caption in this format
(all lines optional):

```
Title: The Hobbit
Author: J.R.R. Tolkien
Tags: fantasy, classic
```

If you skip the caption, the title falls back to the filename.

Note: for the listener to see the message, at least one of your bots
(`BOT_TOKENS`) must be an **admin** of `BOOKS_CHANNEL` (same requirement as
the website uploads).

---

## API Endpoints (for your Vercel frontend)

Base URL = your backend URL, e.g. `https://your-app.onrender.com`

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/api/books` | — | List / search books |
| `GET` | `/api/books/tags` | — | All unique tags |
| `GET` | `/api/books/{id}` | — | Single book metadata |
| `GET` | `/api/books/{id}/download` | — | Stream / download the original file |
| `GET` | `/api/books/{id}/stream` | — | Same as download (alias) |
| `GET` | `/api/books/{id}/reader-info` | — | Reader status/format for the in-browser reader |
| `GET` | `/api/books/{id}/reader-file` | — | Stream the reader-ready version (original or converted) |
| `POST` | `/api/books/upload` | — | Upload a new book (multipart) |
| `POST` | `/api/books/admin/verify` | password in body | Check an admin password |
| `PATCH` | `/api/books/{id}` | `X-Admin-Password` header | Update metadata |
| `DELETE` | `/api/books/{id}` | `X-Admin-Password` header | Remove from library |
| `GET` | `/api/books/{id}/cover` | — | Stream the book's cover image (404 if none set) |
| `POST` | `/api/books/{id}/cover` | `X-Admin-Password` header | Upload/replace a cover image (multipart, `file=`) |
| `POST` | `/api/books/{id}/cover/generate` | `X-Admin-Password` header | Auto-generate a cover for this one book (`?force=true\|false`, default `true`) |
| `POST` | `/api/books/covers/generate-all` | `X-Admin-Password` header | Auto-generate covers for every book that doesn't have one yet (never overwrites existing covers) |
| `POST` | `/api/books/admin/import` | `X-Admin-Password` header | Start a bulk import from a source channel; returns an `import_id` immediately |
| `GET` | `/api/books/admin/import/{import_id}` | `X-Admin-Password` header | Poll progress for a running or finished import |
| `POST` | `/api/books/admin/import/{import_id}/cancel` | `X-Admin-Password` header | Stop a running import (books already added are kept) |
| `GET` | `/api/books/admin/enrich-status` | `X-Admin-Password` header | How many books still need metadata/covers |
| `POST` | `/api/books/admin/enrich` | `X-Admin-Password` header | Run the metadata/cover pass over every book that still needs it |

### Bulk import from a channel

`POST /api/books/admin/import` forwards book files out of any channel the bot
can read into `BOOKS_CHANNEL` and registers them in the library. It is the
books equivalent of the drive's `/fast_import`, with one deliberate
difference: books are **forwarded into `BOOKS_CHANNEL`**, not registered
in place against the source channel. Everything downstream — cover
streaming, reader conversion, the `books.data` backup — addresses a single
channel by message id, so a book living anywhere else would be unreadable.

Request body:

```json
{
  "channel": "@sourcebooks",
  "start_msg_id": 100,
  "end_msg_id": 500,
  "skip_duplicates": true,
  "enrich": true,
  "generate_covers": true
}
```

`channel` accepts `@username`, a bare username, or a `-100…` id.
`start_msg_id`/`end_msg_id` must be given together or not at all; omit both
to import the whole channel. The response is `{"status": "started",
"import_id": "…"}` — poll
`GET /api/books/admin/import/{import_id}` for progress, which reports
`status` (`validating` → `scanning` → `fetching` → `deduplicating` →
`importing` → `enriching` → `done`/`cancelled`/`error`) plus counters for
`imported`, `skipped_duplicate`, `skipped_not_book`, `enrich_done`,
`covers` and `errors`.

Two phases, with very different costs:

1. **Forward** — cheap. Message ids are scanned in batches, non-book
   extensions and files already in the library are filtered out *before*
   anything is forwarded (so `BOOKS_CHANNEL` never accumulates orphans),
   then up to 100 ids at a time are forwarded server-side. No file bytes
   pass through the server, and the library is pickled once per batch
   rather than once per file.
2. **Enrich** — expensive, and the reason the whole thing is asynchronous.
   Each book is downloaded exactly once to do three jobs at once: read its
   embedded title/author, render a cover, and build the reader version for
   formats that need converting. This runs strictly one book at a time, with
   a `gc.collect()` and a short sleep between books, a 120 MB per-file cap,
   and a cgroup-based free-memory check that defers a book rather than
   risking the process. Each book is flagged `enriched` as it completes, so
   the pass is resumable: `POST /api/books/admin/enrich` picks up whatever
   is still outstanding, including books deferred for memory.

Setting `"enrich": false` gives a fast, filename-only import — titles and
authors are parsed from filenames (`Clear, James - Atomic Habits (2018).pdf`
→ title *Atomic Habits*, author *James Clear*) and you can run the enrich
pass later.

Progress is held in memory, so a server restart loses it; imports already
written to the library are unaffected.

The same flows are available from the Telegram bot (`/import_books`,
`/enrich_books`, `/cancel_import <id>`) and from the admin panel on the
books frontend.

### Cover images

Every book can have a cover image, stored the same way as the book itself:
as a document message in `BOOKS_CHANNEL`, referenced by `cover_message_id`
on the book object. There are two ways to set one:

1. **Upload manually** — `POST /api/books/{id}/cover` with a `file=` field
   (jpg/jpeg/png/webp). Replaces any existing cover for that book.
2. **Auto-generate** — `POST /api/books/{id}/cover/generate` renders the
   book's actual first page as the cover:
   - PDF / DJVU — the first page is rendered directly.
   - EPUB / MOBI / AZW3 — the book's embedded cover image is extracted
     (falling back to a first-page render if it has none).
   - Anything else (e.g. TXT), or any format whose render attempt fails —
     falls back to a generated title card (book title + author on a
     colored background) so the book still ends up with *something*
     rather than staying coverless.

   By default this endpoint always (re)generates, since clicking
   "Generate" on one specific book is an explicit request for a new cover.
   Pass `?force=false` to only generate if the book doesn't have a cover
   yet.

   `POST /api/books/covers/generate-all` runs the same generation for
   every book that doesn't have a cover yet, in one call — this one always
   skips books that already have a cover (manually uploaded or previously
   generated), so re-running it is always safe and only fills in the gaps.

The frontend can display a cover with a plain `<img src="{API_URL}/api/books/{id}/cover" />` and fall back to a placeholder icon on a 404 (i.e. only render the `<img>` when `book.cover_message_id` is set, or handle `onError`).

### Query parameters for `GET /api/books`

- `q` – search in title / author / description / tags
- `tag` – filter by exact tag
- `author` – filter by author
- `limit` – default 50, max 200
- `offset` – pagination

### Upload example (`POST /api/books/upload`)

```bash
curl -X POST https://your-backend.onrender.com/api/books/upload \
  -F "file=@Atomic_Habits.pdf" \
  -F "title=Atomic Habits" \
  -F "author=James Clear" \
  -F "description=..." \
  -F "tags=self-help,productivity" \
  -F "language=en"
```

### Admin-protected example (`PATCH` / `DELETE`)

```bash
curl -X DELETE https://your-backend.onrender.com/api/books/a1b2c3d4e5 \
  -H "X-Admin-Password: $ADMIN_PASSWORD"
```

Uses the same `ADMIN_PASSWORD` env var as the rest of the project (defaults
to `"admin"` if unset — **change it** before going live). The website's
`/admin` page prompts once for the password (via `POST /api/books/admin/verify`)
and then sends it back on this header for every edit/delete action; it's
never persisted server-side beyond that single env var.

### Response shape (book object)

```json
{
  "id": "a1b2c3d4e5",
  "title": "Atomic Habits",
  "author": "James Clear",
  "description": "...",
  "tags": ["self-help", "productivity"],
  "language": "en",
  "filename": "Atomic_Habits.pdf",
  "message_id": 12345,
  "size": 2457600,
  "uploaded_at": "2026-08-29T10:00:00+00:00",
  "updated_at": "2026-08-29T10:00:00+00:00",
  "reader_format": "pdf",
  "reader_status": "ready",
  "reader_error": null
}
```

---

## In-browser reader

Every book gets a "Read" option, not just "Download":

- **PDF / EPUB / TXT** — reader-ready immediately, no conversion needed.
- **MOBI / AZW3** — auto-converted to **EPUB** in the background using
  Calibre's `ebook-convert` (installed in the Docker image).
- **DJVU** — auto-converted to **PDF** in the background using
  `ddjvu` from `djvulibre-bin` (installed in the Docker image).

`reader_status` on a book is one of:

| Value | Meaning |
|---|---|
| `ready` | `GET /api/books/{id}/reader-file` will stream a renderable file |
| `converting` | conversion is running in the background — poll `reader-info` every couple of seconds |
| `failed` | conversion failed (`reader_error` has details) — fall back to Download |
| `unsupported` | file type has no reader path at all (shouldn't normally happen given the upload allow-list) |

Conversion runs for both website uploads and files dropped directly into
`BOOKS_CHANNEL` in Telegram.

**Docker image size note:** Calibre pulls in a lot of dependencies and will
noticeably increase build time / image size on Render. If you don't need
MOBI/AZW3/DJVU reader support, remove `calibre djvulibre-bin` from the
`Dockerfile`'s `apt-get install` line — those formats will just fall back to
download-only (`reader_status: "unsupported"`) instead of breaking anything.

---

## Important Notes

- Books are stored **only** in `BOOKS_CHANNEL`.
- Metadata is stored in `./cache/books.data` and periodically backed up to the same channel.
- The main TGDrive (`STORAGE_CHANNEL`) is completely unaffected.
- CORS is already open (`*`) so your Vercel frontend can call these APIs directly.
- **Change `ADMIN_PASSWORD`** from its default (`"admin"`) before going live — it now gates book editing/deletion too, not just the main drive admin actions.
