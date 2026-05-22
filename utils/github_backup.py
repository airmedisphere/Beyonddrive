"""
github_backup.py — Automatic drive.data backup to GitHub.

Every time drive.data is backed up to Telegram, this module
also commits it to a private GitHub repository.

Setup:
  GITHUB_TOKEN = personal access token (repo scope)
  GITHUB_REPO  = username/repo-name  e.g. piyush/beyondbooks-backup

The repo will contain:
  drive.data          ← latest version
  backups/            ← timestamped history (last 10 kept)
    drive_2026-05-22_10-30-00.data
    drive_2026-05-22_09-00-00.data
    ...

Recovery:
  1. Go to github.com/username/repo-name
  2. Download drive.data
  3. Send it to your Telegram storage channel
  4. Update DATABASE_BACKUP_MSG_ID env var
  5. Redeploy
"""

import asyncio
import base64
import json
from datetime import datetime
from utils.logger import Logger
import config

logger = Logger(__name__)

GITHUB_API = "https://api.github.com"
MAX_HISTORY = 10  # Keep last 10 timestamped backups


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


async def backup_to_github(drive_cache_path: str) -> bool:
    """
    Push drive.data to GitHub.
    - Updates drive.data (latest)
    - Adds timestamped copy in backups/
    - Removes oldest backup if > MAX_HISTORY
    Returns True on success.
    """
    if not is_github_enabled():
        return False

    try:
        with open(drive_cache_path, "rb") as f:
            content_bytes = f.read()

        content_b64 = base64.b64encode(content_bytes).decode()
        timestamp   = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        commit_msg  = f"drive.data backup {timestamp} UTC"

        # ── 1. Update drive.data (latest) ────────────────────────────────────
        sha = await _get_file_sha("drive.data")
        payload = {
            "message": commit_msg,
            "content": content_b64,
        }
        if sha:
            payload["sha"] = sha

        _, status = await _github_request("PUT", "/contents/drive.data", payload)
        if status not in (200, 201):
            logger.error(f"GitHub backup: failed to update drive.data (status {status})")
            return False

        logger.info(f"GitHub backup: drive.data updated ({len(content_bytes):,} bytes)")

        # ── 2. Add timestamped copy in backups/ ───────────────────────────────
        backup_path = f"backups/drive_{timestamp}.data"
        backup_payload = {
            "message": commit_msg,
            "content": content_b64,
        }
        await _github_request("PUT", f"/contents/{backup_path}", backup_payload)

        # ── 3. Prune old backups (keep MAX_HISTORY) ───────────────────────────
        asyncio.create_task(_prune_old_backups())

        return True

    except Exception as e:
        logger.error(f"GitHub backup failed: {e}")
        return False


async def _prune_old_backups():
    """Delete oldest backups if more than MAX_HISTORY exist."""
    try:
        data, status = await _github_request("GET", "/contents/backups")
        if status != 200 or not isinstance(data, list):
            return

        # Sort by name (timestamp is in filename so alphabetical = chronological)
        files = sorted(
            [f for f in data if f["name"].startswith("drive_")],
            key=lambda x: x["name"]
        )

        # Delete oldest ones beyond MAX_HISTORY
        to_delete = files[:-MAX_HISTORY] if len(files) > MAX_HISTORY else []
        for f in to_delete:
            await _github_request("DELETE", f"/contents/backups/{f['name']}", {
                "message": f"Remove old backup {f['name']}",
                "sha": f["sha"],
            })
            logger.info(f"GitHub backup: removed old backup {f['name']}")

    except Exception as e:
        logger.error(f"GitHub prune error: {e}")
