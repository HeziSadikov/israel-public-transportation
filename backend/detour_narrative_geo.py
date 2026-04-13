"""
Build detour LineString from Hebrew turn-by-turn via geocoding + Valhalla (no GTFS stop anchors).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .config import VALHALLA_URL
from .detour_geo_validation import road_geojson_clear_of_blockage
from .detour_instructions_text import merged_steps_to_geocode_queries
from .geocoding_nominatim import geocode_ordered_waypoints
from .osm_detour import route_waypoints_avoiding_polygon
from .osm_pretty import map_match_coordinates


def try_build_narrative_detour_linestring(
    merged_steps: List[Dict[str, Any]],
    blockage_geojson: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Geocode intersection queries from steps, route on OSM avoiding blockage.
    Returns GeoJSON LineString geometry dict, or None on failure.
    """
    if not VALHALLA_URL or not str(VALHALLA_URL).strip():
        return None
    if not merged_steps:
        return None

    queries = merged_steps_to_geocode_queries(merged_steps)
    if len(queries) < 2:
        return None

    waypoints = geocode_ordered_waypoints(queries)
    if not waypoints or len(waypoints) < 2:
        return None

    osm = route_waypoints_avoiding_polygon(waypoints, blockage_geojson)
    if not osm.success or len(osm.coordinates) < 2:
        return None

    coords = list(osm.coordinates)
    snapped = map_match_coordinates(coords)
    if snapped is not None and len(snapped.coords) >= 2:
        coords = list(snapped.coords)

    road: Dict[str, Any] = {"type": "LineString", "coordinates": coords}
    if not road_geojson_clear_of_blockage(road, blockage_geojson):
        return None
    return road
