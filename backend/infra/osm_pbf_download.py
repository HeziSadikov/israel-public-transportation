"""
Download an OSM PBF extract (e.g. Geofabrik Israel-and-Palestine) to a local path.

Mirrors the freshness-probe + streaming pattern of ``gtfs_download.py`` so the
detour v3 importer can use the same --fetch / --fetch-if-newer semantics as the
GTFS ingest.
"""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Optional

import httpx

from .logging_utils import log


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


def parse_last_modified(value: Optional[str]) -> Optional[datetime]:
    """Parse an HTTP Last-Modified header into an aware UTC datetime."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_remote_osm_pbf_metadata(
    url: str,
    *,
    timeout: float = 30.0,
    log_tag: str = "osm/download",
) -> Dict[str, Optional[str]]:
    """
    Retrieve OSM PBF freshness metadata from remote.

    Tries HEAD first. If not supported/fails, falls back to a range GET (0-0).
    Returns metadata keys: etag, last_modified, content_length.
    """
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        try:
            resp = client.head(url)
            resp.raise_for_status()
            return _extract_metadata(resp.headers)
        except Exception as e:
            log(log_tag, f"HEAD metadata probe failed ({e!s}); trying range GET ...")

        resp = client.get(url, headers={"Range": "bytes=0-0"})
        resp.raise_for_status()
        return _extract_metadata(resp.headers)


def download_osm_pbf(
    dest: Path,
    url: str,
    *,
    log_tag: str = "osm/download",
    timeout: float = 1800.0,
) -> None:
    """
    Stream-download an OSM PBF extract to ``dest`` (overwrites if present).
    Creates ``dest.parent`` if needed. Long timeout because Israel-and-Palestine
    is ~100 MB and Geofabrik can be slow.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    log(log_tag, f"Downloading OSM PBF from {url} ...")
    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as resp:
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
        if downloaded > 0 and downloaded < next_log:
            mb = downloaded / (1024 * 1024)
            if total > 0:
                pct = downloaded * 100.0 / total
                log(log_tag, f"Downloaded {mb:.1f} MB ({pct:.1f}%) ...")
            else:
                log(log_tag, f"Downloaded {mb:.1f} MB ...")
    size_mb = dest.stat().st_size / (1024 * 1024)
    log(log_tag, f"Download complete: {size_mb:.1f} MB saved to {dest}")
