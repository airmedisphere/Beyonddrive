"""
github_backup.py — Automatic drive.data / books.data backup to GitHub.

Every time drive.data (or books.data) is backed up to Telegram, this module
also commits it to a private GitHub repository, in its own folder so both
backups can live side by side in the same repo.

Setup:
  GITHUB_TOKEN = personal access token (repo scope)
  GITHUB_REPO  = username/repo-name  e.g. piyush/beyondbooks-backup

The repo will contain:
  drive.data              ← latest main drive backup
  backups/                ← timestamped history of drive.data (last 10 kept)
    drive_2026-05-22_10-30-00.data
    ...
  books/books.data        ← latest books library backup
  books/backups/          ← timestamped history of books.data (last 10 kept)
    books_2026-05-22_10-30-00.data
    ...

Recovery (drive.data):
  1. Go to github.com/username/repo-name
  2. Download drive.data
  3. Send it to your Telegram storage channel
  4. Update DATABASE_BACKUP_MSG_ID env var
  5. Redeploy

Recovery (books.data):
  1. Go to github.com/username/repo-name/books
  2. Download books.data
  3. Send it to your Telegram BOOKS_CHANNEL
  4. Update BOOKS_DB_MSG_ID env var
  5. Redeploy
"""

import asyncio
import base64
from datetime import datetime
from utils.logger import Logger
import config

logger = Logger(__name__)

GITHUB_API = "https://api.github.com"
MAX_HISTORY = 10  # Keep last 10 timestamped backups per folder


def is_github_enabled() -> bool:
    return bool(
        getattr(config, "GITHUB_TOKEN", None) and
        getattr(config, "GITHUB_REPO", None)
    )


async def _github_request(method: str, path: str, data: dict = None) -> dict:
    """Make a GitHub API request using aiohttp."""
    import aiohttp
    token = config.GITHUB_TOKEN
    repo  = config.GITHUB_REPO

    url     = f"{GITHUB_API}/repos/{repo}{path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github.v3+json",
        "Content-Type":  "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.request(
            method, url, headers=headers,
            json=data, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            return await resp.json(), resp.status


async def _get_file_sha(path: str):
    """Get SHA of existing file (needed for updates)."""
    try:
        data, status = await _github_request("GET", f"/contents/{path}")
        if status == 200:
            return data.get("sha")
    except Exception:
        pass
    return None


async def backup_to_github(
    local_path: str,
    remote_name: str = "drive.data",
    folder: str = "",
) -> bool:
    """
    Push a local backup file to GitHub.
    - Updates <folder>/<remote_name> (latest)
    - Adds a timestamped copy in <folder>/backups/
    - Removes oldest backup in that folder if > MAX_HISTORY
    Returns True on success.

    folder="" backs up to repo root (used for drive.data).
    folder="books" backs up under books/ (used for books.data), keeping it
    completely separate from the main drive backup in the same repo.
    """
    if not is_github_enabled():
        return False

    try:
        with open(local_path, "rb") as f:
            content_bytes = f.read()

        content_b64 = base64.b64encode(content_bytes).decode()
        timestamp   = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        commit_msg  = f"{remote_name} backup {timestamp} UTC"

        latest_path = f"{folder}/{remote_name}" if folder else remote_name
        backups_dir = f"{folder}/backups" if folder else "backups"

        # ── 1. Update latest file ────────────────────────────────────────
        sha = await _get_file_sha(latest_path)
        payload = {
            "message": commit_msg,
            "content": content_b64,
        }
        if sha:
            payload["sha"] = sha

        _, status = await _github_request("PUT", f"/contents/{latest_path}", payload)
        if status not in (200, 201):
            logger.error(f"GitHub backup: failed to update {latest_path} (status {status})")
            return False

        logger.info(f"GitHub backup: {latest_path} updated ({len(content_bytes):,} bytes)")

        # ── 2. Add timestamped copy in backups/ ───────────────────────────
        stem = remote_name.rsplit(".", 1)[0]
        backup_path = f"{backups_dir}/{stem}_{timestamp}.data"
        backup_payload = {
            "message": commit_msg,
            "content": content_b64,
        }
        await _github_request("PUT", f"/contents/{backup_path}", backup_payload)

        # ── 3. Prune old backups (keep MAX_HISTORY) ───────────────────────
        asyncio.create_task(_prune_old_backups(backups_dir, stem))

        return True

    except Exception as e:
        logger.error(f"GitHub backup failed: {e}")
        return False


async def _prune_old_backups(backups_dir: str, stem: str):
    """Delete oldest backups in a given folder if more than MAX_HISTORY exist."""
    try:
        data, status = await _github_request("GET", f"/contents/{backups_dir}")
        if status != 200 or not isinstance(data, list):
            return

        # Sort by name (timestamp is in filename so alphabetical = chronological)
        files = sorted(
            [f for f in data if f["name"].startswith(f"{stem}_")],
            key=lambda x: x["name"]
        )

        # Delete oldest ones beyond MAX_HISTORY
        to_delete = files[:-MAX_HISTORY] if len(files) > MAX_HISTORY else []
        for f in to_delete:
            await _github_request("DELETE", f"/contents/{backups_dir}/{f['name']}", {
                "message": f"Remove old backup {f['name']}",
                "sha": f["sha"],
            })
            logger.info(f"GitHub backup: removed old backup {backups_dir}/{f['name']}")

    except Exception as e:
        logger.error(f"GitHub prune error: {e}")
