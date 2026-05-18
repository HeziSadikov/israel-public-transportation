"""Detour v3 M5 — aggregate ``pattern_osm_segments`` → bus-corridor evidence tables."""

from __future__ import annotations

from typing import Any, Dict, Optional
from backend.infra import db_access as db


def build_bus_evidence(
    conn,
    *,
    feed_id: Optional[int] = None,
    commit: bool = True,
    exact_trip_counts: bool = False,
) -> Dict[str, Any]:
    """
    Replace ``gtfs_bus_segment_evidence`` and ``gtfs_bus_turn_evidence`` for ``feed_id``
    using ``pattern_osm_segments``. Caller supplies an open Postgres connection.
    """
    fid = int(feed_id) if feed_id is not None else int(db.get_active_feed_id(conn))
    stats = db.rebuild_gtfs_bus_corridor_evidence(
        feed_id=fid,
        conn=conn,
        commit_between_steps=commit,
        exact_trip_counts=exact_trip_counts,
    )
    if commit:
        conn.commit()
    return {"feed_id": fid, **stats}


__all__ = ["build_bus_evidence"]
