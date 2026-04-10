"""
Download Israel GTFS zip from MOT (or another URL) to a local path.

Used by ``gtfs_updater.update_feed`` and ``ingest_gtfs_postgis --fetch``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import httpx

from .config import GTFS_REMOTE_BASE, GTFS_REMOTE_FILENAME
from .logging_utils import log


def default_gtfs_download_url() -> str:
    return f"{GTFS_REMOTE_BASE.rstrip('/')}/{GTFS_REMOTE_FILENAME}"


def _normalize_etag(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.startswith("W/"):
        s = s[2:].strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    return s or None


def _extract_metadata(headers: httpx.Headers) -> Dict[str, Optional[str]]:
    return {
        "etag": _normalize_etag(headers.get("ETag")),
        "last_modified": headers.get("Last-Modified"),
        "content_length": headers.get("Content-Length"),
    }


def get_remote_gtfs_metadata(
    url: Optional[str] = None,
    *,
    timeout: float = 30.0,
    log_tag: str = "gtfs/download",
) -> Dict[str, Optional[str]]:
    """
    Retrieve GTFS freshness metadata from remote.

    Tries HEAD first. If not supported/fails, falls back to a range GET (0-0).
    Returns metadata keys: etag, last_modified, content_length.
    """
    resolved = url or default_gtfs_download_url()
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        try:
            resp = client.head(resolved)
            resp.raise_for_status()
            return _extract_metadata(resp.headers)
        except Exception as e:
            log(log_tag, f"HEAD metadata probe failed ({e!s}); trying range GET ...")

        resp = client.get(resolved, headers={"Range": "bytes=0-0"})
        resp.raise_for_status()
        return _extract_metadata(resp.headers)


def download_gtfs_zip(
    dest: Path,
    url: Optional[str] = None,
    *,
    log_tag: str = "gtfs/download",
    timeout: float = 300.0,
) -> None:
    """
    Stream-download a GTFS zip to ``dest`` (overwrites if present).
    Creates ``dest.parent`` if needed.
    """
    resolved = url or default_gtfs_download_url()
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    log(log_tag, f"Downloading GTFS from {resolved} ...")
    with httpx.stream("GET", resolved, timeout=timeout) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length") or 0)
        downloaded = 0
        next_log = 5 * 1024 * 1024
        with dest.open("wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded >= next_log:
                    mb = downloaded / (1024 * 1024)
                    if total > 0:
                        pct = downloaded * 100.0 / total
                        log(log_tag, f"Downloaded {mb:.1f} MB ({pct:.1f}%) ...")
                    else:
                        log(log_tag, f"Downloaded {mb:.1f} MB ...")
                    next_log += 5 * 1024 * 1024
        # Emit one final in-stream progress line when the loop ended before
        # crossing the next periodic threshold.
        if downloaded > 0 and downloaded < next_log:
            mb = downloaded / (1024 * 1024)
            if total > 0:
                pct = downloaded * 100.0 / total
                log(log_tag, f"Downloaded {mb:.1f} MB ({pct:.1f}%) ...")
            else:
                log(log_tag, f"Downloaded {mb:.1f} MB ...")
    size_mb = dest.stat().st_size / (1024 * 1024)
    log(log_tag, f"Download complete: {size_mb:.1f} MB saved to {dest}")
