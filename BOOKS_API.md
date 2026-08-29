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

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/books` | List / search books |
| `GET` | `/api/books/tags` | All unique tags |
| `GET` | `/api/books/{id}` | Single book metadata |
| `GET` | `/api/books/{id}/download` | Stream / download the file |
| `GET` | `/api/books/{id}/stream` | Same as download (alias) |
| `POST` | `/api/books/upload` | Upload a new book (multipart) |
| `PATCH` | `/api/books/{id}` | Update metadata |
| `DELETE` | `/api/books/{id}` | Remove from library |

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
  "updated_at": "2026-08-29T10:00:00+00:00"
}
```

---

## Important Notes

- Books are stored **only** in `BOOKS_CHANNEL`.
- Metadata is stored in `./cache/books.data` and periodically backed up to the same channel.
- The main TGDrive (`STORAGE_CHANNEL`) is completely unaffected.
- CORS is already open (`*`) so your Vercel frontend can call these APIs directly.
