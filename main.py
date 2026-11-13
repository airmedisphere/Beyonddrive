
# ============================================================
# ARCANE DRIVE MIGRATION — SECURE DATABASE EXPORT
# Temporary endpoint for migrating Render data to Oracle
# ============================================================

@app.post("/api/admin/export-drive")
async def export_drive_database(request: Request):
    """
    Read-only export of the complete in-memory drive database.

    Includes:
    - All folders and files
    - Original ArcaneDrive IDs
    - Telegram message IDs
    - Source channel references
    - File names, sizes and dates
    - Trashed items
    - Folder authorization metadata
    - Encoded video references
    - Used IDs

    Does not modify or save the database.
    """

    import os
    import hmac
    import json
    from fastapi.responses import Response

    # Require a dedicated export secret.
    expected_secret = os.environ.get(
        "DRIVE_EXPORT_SECRET", ""
    )

    supplied_secret = request.headers.get(
        "X-Export-Secret", ""
    )

    if (
        not expected_secret
        or len(expected_secret) < 32
        or not hmac.compare_digest(
            supplied_secret,
            expected_secret
        )
    ):
        raise HTTPException(
            status_code=403,
            detail="Export access denied"
        )

    drive = check_drive_initialized()

    def serialize(value):
        if value is None or isinstance(
            value, (str, int, float, bool)
        ):
            return value

        if isinstance(value, dict):
            return {
                str(key): serialize(item)
                for key, item in value.items()
            }

        if isinstance(value, (list, tuple, set)):
            return [serialize(item) for item in value]

        if isinstance(value, datetime):
            return value.isoformat()

        if hasattr(value, "__dict__"):
            return {
                key: serialize(item)
                for key, item in vars(value).items()
            }

        raise TypeError(
            f"Unsupported metadata type: {type(value).__name__}"
        )

    # Serialize only the drive database, not Telegram
    # sessions, credentials or unrelated application state.
    snapshot = {
        "format": "arcanedrive-migration-v1",
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "contents": serialize(drive.contents),
        "used_ids": serialize(drive.used_ids),
    }

    root = snapshot["contents"].get("/")

    if not root:
        raise HTTPException(
            status_code=409,
            detail="Drive root is missing"
        )

    def count_items(folder):
        files = 0
        folders = 0

        for item in folder.get("contents", {}).values():
            if item.get("type") == "folder":
                folders += 1
                child_files, child_folders = count_items(item)
                files += child_files
                folders += child_folders
            elif item.get("type") == "file":
                files += 1

        return files, folders

    total_files, total_folders = count_items(root)

    # Prevent accidentally exporting an empty database.
    if total_files == 0:
        raise HTTPException(
            status_code=409,
            detail="Refusing to export an empty drive"
        )

    snapshot["statistics"] = {
        "files": total_files,
        "folders": total_folders,
    }

    payload = json.dumps(
        snapshot,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False
    ).encode("utf-8")

    return Response(
        content=payload,
        media_type="application/json",
        headers={
            "Content-Disposition":
                'attachment; filename="arcanedrive-migration.json"',
            "Cache-Control": "no-store, private",
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
        }
    )
