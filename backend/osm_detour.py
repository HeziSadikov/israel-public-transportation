"""
Route a segment using OSM (Valhalla) while avoiding a polygon.
Used for detours so the path goes around the blocked area on the road network.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from shapely.geometry import shape

from .config import VALHALLA_URL


@dataclass
class OSMDetourResult:
    """Result of routing from A to B avoiding a polygon."""

    coordinates: List[Tuple[float, float]]  # (lon, lat) GeoJSON order
    distance_m: float
    time_s: float
    success: bool


def _decode_polyline(encoded: str, precision: int = 6) -> List[Tuple[float, float]]:
    """Decode Valhalla/Google polyline (lat,lon pairs) to list of (lon, lat) for GeoJSON."""
    factor = 10.0 ** precision
    coords: List[Tuple[float, float]] = []
    idx = 0
    lat = 0.0
    lon = 0.0
    n = len(encoded)
    while idx < n:
        shift = 0
        result = 0
        while True:
            b = ord(encoded[idx]) - 63
            idx += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if result & 1 else result >> 1
        lat += dlat / factor

        shift = 0
        result = 0
        while True:
            b = ord(encoded[idx]) - 63
            idx += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if result & 1 else result >> 1
        lon += dlon / factor
        coords.append((lon, lat))
    return coords


def _polygon_to_exclude_rings(blockage_geojson: Dict[str, Any]) -> Optional[List[List[List[float]]]]:
    """Convert GeoJSON geometry to Valhalla exclude_polygons format: [[[lon, lat], ...], ...]."""
    geom = shape(blockage_geojson)
    if geom.is_empty:
        return None
    rings: List[List[List[float]]] = []
    if geom.geom_type == "Polygon":
        # Exterior ring only (Valhalla closes the ring)
        ext = geom.exterior
        if ext and len(ext.coords) >= 3:
            rings.append([[float(x), float(y)] for x, y in ext.coords])
    elif geom.geom_type == "MultiPolygon":
        for poly in geom.geoms:
            ext = poly.exterior
            if ext and len(ext.coords) >= 3:
                rings.append([[float(x), float(y)] for x, y in ext.coords])
    elif geom.geom_type == "Point":
        # Buffer point to a small polygon so roads through it are excluded
        buf = geom.buffer(1e-4)  # ~10 m
        if buf.exterior and len(buf.exterior.coords) >= 3:
            rings.append([[float(x), float(y)] for x, y in buf.exterior.coords])
    elif geom.geom_type == "LineString":
        # Buffer line to a polygon
        buf = geom.buffer(1e-4)
        if buf.exterior and len(buf.exterior.coords) >= 3:
            rings.append([[float(x), float(y)] for x, y in buf.exterior.coords])
    return rings if rings else None


def route_avoiding_polygon(
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    blockage_geojson: Dict[str, Any],
    costing: str = "bus",
    timeout_s: float = 15.0,
) -> OSMDetourResult:
    """
    Call Valhalla route API with exclude_polygons so the path goes around the blockage.
    Returns coordinates (lon, lat), distance_m, time_s. On failure returns success=False.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
        )

    exclude = _polygon_to_exclude_rings(blockage_geojson)
    body: Dict[str, Any] = {
        "locations": [
            {"lat": from_lat, "lon": from_lon},
            {"lat": to_lat, "lon": to_lon},
        ],
        "costing": costing,
        "units": "kilometers",
    }
    if exclude:
        body["exclude_polygons"] = exclude

    url = f"{VALHALLA_URL.rstrip('/')}/route"
    try:
        resp = httpx.post(url, json=body, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
        )

    trip = data.get("trip") or {}
    legs = trip.get("legs") or []
    if not legs:
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
        )

    all_coords: List[Tuple[float, float]] = []
    total_km = 0.0
    total_time_s = 0.0
    for leg in legs:
        summary = leg.get("summary") or {}
        total_km += float(summary.get("length", 0))
        total_time_s += float(summary.get("time", 0))
        enc = leg.get("shape", "")
        if enc:
            all_coords.extend(_decode_polyline(enc))

    if not all_coords:
        return OSMDetourResult(
            coordinates=[],
            distance_m=total_km * 1000.0,
            time_s=total_time_s,
            success=total_km > 0,
        )

    return OSMDetourResult(
        coordinates=all_coords,
        distance_m=total_km * 1000.0,
        time_s=total_time_s,
        success=True,
    )
