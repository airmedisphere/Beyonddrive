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
