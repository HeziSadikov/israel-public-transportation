"""
Route a segment using OSM (Valhalla) while avoiding a polygon.
Used for detours so the path goes around the blocked area on the road network.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from shapely.geometry import shape

from backend.infra.config import VALHALLA_URL


@dataclass
class OSMDetourResult:
    """Result of routing from A to B avoiding a polygon."""

    coordinates: List[Tuple[float, float]]  # (lon, lat) GeoJSON order
    distance_m: float
    time_s: float
    success: bool
    # Street-oriented steps from Valhalla maneuvers (instruction_en / street).
    turn_by_turn: Optional[List[Dict[str, Any]]] = None


@dataclass
class OSMFeasibilityResult:
    success: bool
    reason_code: str
    road_geometry_geojson: Optional[Dict[str, Any]]
    road_distance_m: Optional[float]
    road_time_s: Optional[float]
    gtfs_distance_m: Optional[float]
    distance_ratio: Optional[float]
    time_ratio: Optional[float]
    turn_by_turn: Optional[List[Dict[str, Any]]]


def _maneuvers_to_turn_steps(legs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    for leg in legs:
        for m in leg.get("maneuvers") or []:
            txt = (
                m.get("instruction")
                or m.get("verbal_succinct_transition_instruction")
                or m.get("verbal_pre_transition_instruction")
                or ""
            )
            streets = m.get("street_names") or []
            street = streets[0] if streets else None
            row: Dict[str, Any] = {}
            if isinstance(txt, str) and txt.strip():
                row["instruction_en"] = txt.strip()
            if isinstance(street, str) and street.strip():
                row["street"] = street.strip()
            toward = m.get("toward")
            if isinstance(toward, str) and toward.strip():
                row["toward_street"] = toward.strip()
            if row:
                steps.append(row)
    return steps


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
        # Buffer point to a modest polygon so roads through it are excluded
        buf = geom.buffer(3e-4)  # ~30 m
        if buf.exterior and len(buf.exterior.coords) >= 3:
            rings.append([[float(x), float(y)] for x, y in buf.exterior.coords])
    elif geom.geom_type == "LineString":
        # Buffer line to a polygon (wider so nearby parallel lanes are excluded)
        buf = geom.buffer(3e-4)
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
            turn_by_turn=None,
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
            turn_by_turn=None,
        )

    trip = data.get("trip") or {}
    legs = trip.get("legs") or []
    if not legs:
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
            turn_by_turn=None,
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
            turn_by_turn=_maneuvers_to_turn_steps(legs) if legs else None,
        )

    tbt = _maneuvers_to_turn_steps(legs)
    return OSMDetourResult(
        coordinates=all_coords,
        distance_m=total_km * 1000.0,
        time_s=total_time_s,
        success=True,
        turn_by_turn=tbt or None,
    )


def route_waypoints_avoiding_polygon(
    waypoints_lonlat: List[Tuple[float, float]],
    blockage_geojson: Dict[str, Any],
    costing: str = "bus",
    timeout_s: float = 45.0,
) -> OSMDetourResult:
    """
    Route through an ordered list of (lon, lat) waypoints (intersections only — no stop anchors).
    Uses Valhalla /route with multiple locations and optional exclude_polygons.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
            turn_by_turn=None,
        )
    if len(waypoints_lonlat) < 2:
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
            turn_by_turn=None,
        )

    exclude = _polygon_to_exclude_rings(blockage_geojson)
    body: Dict[str, Any] = {
        "locations": [{"lat": lat, "lon": lon} for lon, lat in waypoints_lonlat],
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
            turn_by_turn=None,
        )

    trip = data.get("trip") or {}
    legs = trip.get("legs") or []
    if not legs:
        return OSMDetourResult(
            coordinates=[],
            distance_m=0.0,
            time_s=0.0,
            success=False,
            turn_by_turn=None,
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
            turn_by_turn=_maneuvers_to_turn_steps(legs) if legs else None,
        )

    tbt = _maneuvers_to_turn_steps(legs)
    return OSMDetourResult(
        coordinates=all_coords,
        distance_m=total_km * 1000.0,
        time_s=total_time_s,
        success=True,
        turn_by_turn=tbt or None,
    )


def evaluate_road_feasibility_for_candidate(
    *,
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    blockage_geojson: Dict[str, Any],
    gtfs_distance_m: Optional[float] = None,
    gtfs_time_s: Optional[float] = None,
    max_distance_ratio: float = 2.8,
    max_time_ratio: float = 3.5,
    timeout_s: float = 20.0,
) -> OSMFeasibilityResult:
    road = route_avoiding_polygon(
        from_lon=from_lon,
        from_lat=from_lat,
        to_lon=to_lon,
        to_lat=to_lat,
        blockage_geojson=blockage_geojson,
        timeout_s=timeout_s,
    )
    if not road.success or len(road.coordinates) < 2:
        return OSMFeasibilityResult(
            success=False,
            reason_code="road_route_unavailable",
            road_geometry_geojson=None,
            road_distance_m=None,
            road_time_s=None,
            gtfs_distance_m=gtfs_distance_m,
            distance_ratio=None,
            time_ratio=None,
            turn_by_turn=None,
        )
    distance_ratio: Optional[float] = None
    if gtfs_distance_m and gtfs_distance_m > 1.0:
        distance_ratio = float(road.distance_m) / float(gtfs_distance_m)
    time_ratio: Optional[float] = None
    if gtfs_time_s and gtfs_time_s > 1.0:
        time_ratio = float(road.time_s) / float(gtfs_time_s)
    if distance_ratio is not None and distance_ratio > max_distance_ratio:
        return OSMFeasibilityResult(
            success=False,
            reason_code="road_distance_ratio_too_high",
            road_geometry_geojson=None,
            road_distance_m=road.distance_m,
            road_time_s=road.time_s,
            gtfs_distance_m=gtfs_distance_m,
            distance_ratio=distance_ratio,
            time_ratio=time_ratio,
            turn_by_turn=road.turn_by_turn,
        )
    if time_ratio is not None and time_ratio > max_time_ratio:
        return OSMFeasibilityResult(
            success=False,
            reason_code="road_time_ratio_too_high",
            road_geometry_geojson=None,
            road_distance_m=road.distance_m,
            road_time_s=road.time_s,
            gtfs_distance_m=gtfs_distance_m,
            distance_ratio=distance_ratio,
            time_ratio=time_ratio,
            turn_by_turn=road.turn_by_turn,
        )
    geometry = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": list(road.coordinates)},
                "properties": {"kind": "osm_detour", "hybrid_candidate": True},
            }
        ],
    }
    return OSMFeasibilityResult(
        success=True,
        reason_code="road_validated",
        road_geometry_geojson=geometry,
        road_distance_m=road.distance_m,
        road_time_s=road.time_s,
        gtfs_distance_m=gtfs_distance_m,
        distance_ratio=distance_ratio,
        time_ratio=time_ratio,
        turn_by_turn=road.turn_by_turn,
    )
