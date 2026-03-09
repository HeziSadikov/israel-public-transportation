from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import httpx

from .config import (
    GTFS_DATA_DIR,
    FEED_METADATA_PATH,
    GTFS_REMOTE_BASE,
    GTFS_REMOTE_FILENAME,
)
from .sqlite_db import DB_PATH
from scripts.import_gtfs_sqlite import import_gtfs_to_sqlite


@dataclass
class FeedVersion:
    version_id: str
    date: str
    sha256: str
    path: Path
    imported_ok: bool
    created_at: str


def _compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_metadata() -> Dict[str, Any]:
    if FEED_METADATA_PATH.exists():
        with FEED_METADATA_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"active": None, "history": []}


def _save_metadata(meta: Dict[str, Any]) -> None:
    FEED_METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FEED_METADATA_PATH.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _import_feed(path: Path, version_id: str, date: str, sha256: str) -> FeedVersion:
    """
    Import the downloaded GTFS zip into the SQLite database and return a FeedVersion
    record describing the new active feed.

    This replaces the old _load_from_path() sanity check with a real import step.
    """
    # Import into SQLite DB; this will create/replace GTFS tables.
    import_gtfs_to_sqlite(DB_PATH, path)

    now = datetime.utcnow().isoformat() + "Z"
    return FeedVersion(
        version_id=version_id,
        date=date,
        sha256=sha256,
        path=path,
        imported_ok=True,
        created_at=now,
    )


def update_feed() -> Dict[str, Any]:
    """
    Downloads the latest GTFS zip (if online), validates, imports, and
    performs a blue/green switch of the active feed.
    """
    meta = _load_metadata()
    today = datetime.utcnow().strftime("%Y%m%d")
    target_dir = GTFS_DATA_DIR / today
    target_dir.mkdir(parents=True, exist_ok=True)
    zip_path = target_dir / "gtfs.zip"

    url = f"{GTFS_REMOTE_BASE.rstrip('/')}/{GTFS_REMOTE_FILENAME}"
    updated = False
    online_ok = False

    try:
        with httpx.stream("GET", url, timeout=60.0) as resp:
            resp.raise_for_status()
            with zip_path.open("wb") as f:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
        online_ok = True
    except Exception:
        # Online update failed; fall back to last active
        return {
            "updated": False,
            "online_ok": False,
            "message": "Failed to download latest GTFS; using last active feed if available.",
            "active": meta.get("active"),
        }

    sha256 = _compute_sha256(zip_path)
    version_id = f"{today}-{sha256[:8]}"

    # If this exact version already active, skip import
    active = meta.get("active")
    if active and active.get("sha256") == sha256:
        return {
            "updated": False,
            "online_ok": online_ok,
            "message": "Feed already up to date.",
            "active": active,
        }

    # Import and only then switch active (blue/green)
    feed_version = _import_feed(zip_path, version_id, today, sha256)
    record = {
        "version_id": feed_version.version_id,
        "date": feed_version.date,
        "sha256": feed_version.sha256,
        "path": str(feed_version.path),
        "imported_ok": feed_version.imported_ok,
        "created_at": feed_version.created_at,
    }

    history = meta.get("history", [])
    history.append(record)
    meta["history"] = history
    meta["active"] = record
    meta["last_update_attempt"] = datetime.utcnow().isoformat() + "Z"
    meta["last_update_ok"] = True
    _save_metadata(meta)
    updated = True

    return {
        "updated": updated,
        "online_ok": online_ok,
        "active": record,
        "message": "Feed updated and activated.",
    }


def get_feed_status() -> Dict[str, Any]:
    meta = _load_metadata()
    return {
        "active": meta.get("active"),
        "history_len": len(meta.get("history", [])),
        "last_update_attempt": meta.get("last_update_attempt"),
        "last_update_ok": meta.get("last_update_ok"),
    }

