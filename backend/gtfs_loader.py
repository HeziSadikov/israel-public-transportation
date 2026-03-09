from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Iterable
import json
import sqlite3

from .config import FEED_METADATA_PATH, GTFS_CACHE
from .sqlite_db import get_conn


@dataclass
class GTFSFeed:
    version_id: str
    routes: List[Dict]
    agencies: List[Dict]
    trips: List[Dict]
    stop_times: List[Dict]
    stops: List[Dict]
    calendar_dates: List[Dict]
    calendar: List[Dict]
    shapes: List[Dict]


def _get_active_version_id() -> str:
    if FEED_METADATA_PATH.exists():
        with FEED_METADATA_PATH.open("r", encoding="utf-8") as f:
            meta = json.load(f)
            active = meta.get("active")
            if active:
                return active.get("version_id", "dev-local")
    return "dev-local"


def load_active_feed(force_reload: bool = False) -> GTFSFeed:
    """
    Loads GTFS tables from the SQLite database into memory and caches them.
    """
    cache_key = "sqlite-feed"
    if not force_reload and cache_key in GTFS_CACHE:
        return GTFS_CACHE[cache_key]

    conn = get_conn()
    agencies = [dict(r) for r in conn.execute("SELECT * FROM agency").fetchall()]
    routes = [dict(r) for r in conn.execute("SELECT * FROM routes").fetchall()]
    trips = [dict(r) for r in conn.execute("SELECT * FROM trips").fetchall()]
    # stop_times is large; we keep it empty here and access via dedicated helpers.
    stop_times: List[Dict] = []
    stops = [dict(r) for r in conn.execute("SELECT * FROM stops").fetchall()]
    try:
        calendar_dates = [dict(r) for r in conn.execute("SELECT * FROM calendar_dates").fetchall()]
    except sqlite3.OperationalError:
        # Some feeds may not have calendar_dates.txt; treat as empty.
        calendar_dates = []
    try:
        calendar = [dict(r) for r in conn.execute("SELECT * FROM calendar").fetchall()]
    except sqlite3.OperationalError:
        # Similarly, if calendar.txt is missing, treat as empty.
        calendar = []
    try:
        shapes = [dict(r) for r in conn.execute("SELECT * FROM shapes").fetchall()]
    except sqlite3.OperationalError:
        shapes = []

    version_id = _get_active_version_id()

    feed = GTFSFeed(
        version_id=version_id,
        routes=routes,
        agencies=agencies,
        trips=trips,
        stop_times=stop_times,
        stops=stops,
        calendar_dates=calendar_dates,
        calendar=calendar,
        shapes=shapes,
    )
    GTFS_CACHE[cache_key] = feed
    return feed


def get_routes_search_index(force_reload: bool = False) -> List[tuple]:
    """
    Returns a list of (route_dict, normalized_search_str) for fast route search.
    This implementation now reads routes from SQLite instead of routes.txt.
    """
    cache_key = "sqlite-routes-index"
    if not force_reload and cache_key in GTFS_CACHE:
        return GTFS_CACHE[cache_key]

    conn = get_conn()
    routes = [dict(r) for r in conn.execute("SELECT * FROM routes").fetchall()]
    index = []
    for row in routes:
        parts = [
            row.get("route_id") or "",
            row.get("route_short_name") or "",
            row.get("route_long_name") or "",
        ]
        search_str = " ".join(parts).lower()
        index.append((row, search_str))
    GTFS_CACHE[cache_key] = index
    return index

