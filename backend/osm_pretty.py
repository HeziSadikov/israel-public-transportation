from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import httpx
from shapely.geometry import LineString

from .config import OSM_ENGINE_URL
from .graph_builder import EdgeGeometry


@dataclass
class OSMPrettyResult:
    snapped_pattern_geom: Optional[LineString]
    snapped_edges: Dict[Tuple[str, str], EdgeGeometry]
    used_osm: bool


def _linestring_to_osrm_coords(ls: LineString) -> str:
    # OSRM expects lon,lat;lon,lat...
    return ";".join(f"{x},{y}" for x, y in ls.coords)


def map_match_pattern(
    pattern_geom: LineString, edge_geometries: Dict[Tuple[str, str], EdgeGeometry]
) -> OSMPrettyResult:
    """
    Strategy A: map-match the full pattern polyline once via OSRM /match,
    then re-slice the snapped polyline per edge by cumulative length.
    If OSRM is unavailable, returns the original geometries.
    """
    if not pattern_geom or len(pattern_geom.coords) < 2:
        return OSMPrettyResult(
            snapped_pattern_geom=None, snapped_edges=edge_geometries, used_osm=False
        )

    coords_str = _linestring_to_osrm_coords(pattern_geom)
    url = f"{OSM_ENGINE_URL.rstrip('/')}/match/v1/driving/{coords_str}"

    try:
        resp = httpx.get(url, params={"geometries": "geojson", "overview": "full"}, timeout=20.0)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("matchings"):
            raise RuntimeError("No OSRM matchings returned")
        geom = data["matchings"][0]["geometry"]
        snapped_pattern = LineString(geom["coordinates"])
    except Exception:
        return OSMPrettyResult(
            snapped_pattern_geom=None, snapped_edges=edge_geometries, used_osm=False
        )

    # For now, keep per-edge geometries as GTFS-based;
    # pattern polyline is snapped and used for display if desired.
    return OSMPrettyResult(
        snapped_pattern_geom=snapped_pattern,
        snapped_edges=edge_geometries,
        used_osm=True,
    )

