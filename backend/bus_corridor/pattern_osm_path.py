"""Detour v3 M4: read/write helpers for ``pattern_osm_segments``."""

from __future__ import annotations

from typing import Any, Dict, List

from backend.infra import db_access as db


def load_pattern_osm_path(
    *,
    feed_id: int,
    pattern_id: str,
    conn,
) -> List[Dict[str, Any]]:
    """Ordered segment assignments for ``(feed_id, pattern_id)`` (by ``seq``)."""
    return db.fetch_pattern_osm_segments_path(feed_id=feed_id, pattern_id=str(pattern_id), conn=conn)


def pattern_osm_path_row_count(*, feed_id: int, pattern_id: str, conn) -> int:
    """How many segments are persisted for this pattern path."""
    return db.count_pattern_osm_segments(feed_id=feed_id, pattern_id=str(pattern_id), conn=conn)


def clear_pattern_osm_path(*, feed_id: int, pattern_id: str, conn) -> None:
    """Delete all segments for ``(feed_id, pattern_id)``. Caller commits."""
    db.replace_pattern_osm_segments(feed_id=feed_id, pattern_id=str(pattern_id), rows=[], conn=conn)


__all__ = [
    "clear_pattern_osm_path",
    "load_pattern_osm_path",
    "pattern_osm_path_row_count",
]
