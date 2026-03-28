"""Canonical dict shape for route preview responses and route_preview_cache pickles."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def build_route_preview_cache_dict(
    pattern_id: Any,
    stops: Optional[List[Any]],
    route_geojson: Any,
    used_osm_snapping: bool,
    feed_version: str,
) -> Dict[str, Any]:
    """
    Single builder for preview payloads stored in Postgres and GRAPH_CACHE preview keys.
    Keeps app.py and precompute_graphs_postgis aligned; optional slimmer wire formats
    can be introduced here without duplicating dict assembly.
    """
    return {
        "pattern_id": pattern_id,
        "stops": stops or [],
        "route_geojson": route_geojson,
        "used_osm_snapping": bool(used_osm_snapping),
        "feed_version": feed_version,
    }
