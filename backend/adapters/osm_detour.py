"""
Route a segment using OSM (Valhalla) while avoiding a polygon.
Used for detours so the path goes around the blocked area on the road network.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from shapely.geometry import shape

from backend.infra.config import (
    VALHALLA_CIRCUIT_COOLDOWN_S,
    VALHALLA_CIRCUIT_FAIL_THRESHOLD,
    VALHALLA_COUNTRY_CROSSING_PENALTY,
    VALHALLA_HEADING_SNAP_RADIUS_M,
    VALHALLA_HEADING_TOLERANCE_DEG,
    VALHALLA_LOCATION_RADIUS_M,
    VALHALLA_MANEUVER_PENALTY_S,
    VALHALLA_PRIVATE_ACCESS_PENALTY,
    VALHALLA_SERVICE_FACTOR,
    VALHALLA_SERVICE_PENALTY_S,
    VALHALLA_TRACE_ATTRIBUTES_ENABLED,
    VALHALLA_URL,
    VALHALLA_USE_LIVING_STREETS,
    VALHALLA_USE_TRACKS,
)


# ---------------------------------------------------------------------------
# Circuit breaker (C1)
# ---------------------------------------------------------------------------

class _CircuitBreaker:
    """Count consecutive Valhalla transport errors; open the circuit when threshold is reached."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._failures = 0
        self._open_until: float = 0.0

    def is_open(self) -> bool:
        with self._lock:
            return time.monotonic() < self._open_until

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._open_until = 0.0

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= VALHALLA_CIRCUIT_FAIL_THRESHOLD:
                self._open_until = time.monotonic() + float(VALHALLA_CIRCUIT_COOLDOWN_S)

    def reset(self) -> None:
        with self._lock:
            self._failures = 0
            self._open_until = 0.0


_breaker = _CircuitBreaker()


@dataclass
class OSMDetourResult:
    """Result of routing from A to B avoiding a polygon."""

    coordinates: List[Tuple[float, float]]  # (lon, lat) GeoJSON order
    distance_m: float
    time_s: float
    success: bool
    # Street-oriented steps from Valhalla maneuvers (instruction_en / street).
    turn_by_turn: Optional[List[Dict[str, Any]]] = None
    # Edge attribution from /trace_attributes (list of dicts with way_id, road_class, etc.)
    edge_attributes: Optional[List[Dict[str, Any]]] = None


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
            # Valhalla proto: kUturnRight=12, kUturnLeft=13 (see directions.proto Maneuver.Type).
            mt = m.get("type")
            if isinstance(mt, (int, float)) and not isinstance(mt, bool):
                try:
                    row["maneuver_type"] = int(mt)
                except (TypeError, ValueError):
                    pass
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


def _break_point(
    lon: float,
    lat: float,
    *,
    radius_m: int,
    heading_deg: Optional[float] = None,
    heading_tolerance_deg: Optional[int] = None,
) -> Dict[str, Any]:
    loc: Dict[str, Any] = {"lat": lat, "lon": lon, "type": "break"}
    if radius_m > 0:
        loc["radius"] = int(radius_m)
    if heading_deg is not None:
        loc["heading"] = int(round(float(heading_deg))) % 360
        tol = int(heading_tolerance_deg) if heading_tolerance_deg is not None else int(VALHALLA_HEADING_TOLERANCE_DEG)
        loc["heading_tolerance"] = max(1, tol)
    return loc


def _valhalla_break_locations(
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    *,
    exit_heading_deg: Optional[float] = None,
    rejoin_heading_deg: Optional[float] = None,
    heading_tolerance_deg: Optional[int] = None,
    radius_exit_m: Optional[int] = None,
    radius_rejoin_m: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Endpoints for /route with optional snap radius and optional preferred headings (divided roads)."""
    r_def = int(VALHALLA_LOCATION_RADIUS_M)
    re = int(radius_exit_m) if radius_exit_m is not None else r_def
    rr = int(radius_rejoin_m) if radius_rejoin_m is not None else r_def
    tol = heading_tolerance_deg
    a = _break_point(
        from_lon, from_lat,
        radius_m=max(0, re),
        heading_deg=exit_heading_deg,
        heading_tolerance_deg=tol,
    )
    b = _break_point(
        to_lon, to_lat,
        radius_m=max(0, rr),
        heading_deg=rejoin_heading_deg,
        heading_tolerance_deg=tol,
    )
    return [a, b]


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


def _valhalla_costing_options(costing: str) -> Dict[str, Any]:
    """Detour-friendly costing tweaks shared by all /route calls."""
    c = (costing or "auto").strip().lower() or "auto"
    opts: Dict[str, Any] = {
        "maneuver_penalty": int(VALHALLA_MANEUVER_PENALTY_S),
        "service_penalty": int(VALHALLA_SERVICE_PENALTY_S),
    }
    if c == "bus":
        opts["use_living_streets"] = float(VALHALLA_USE_LIVING_STREETS)
        opts["use_tracks"] = float(VALHALLA_USE_TRACKS)
        opts["service_factor"] = float(VALHALLA_SERVICE_FACTOR)
        opts["private_access_penalty"] = float(VALHALLA_PRIVATE_ACCESS_PENALTY)
        opts["country_crossing_penalty"] = float(VALHALLA_COUNTRY_CROSSING_PENALTY)
    return {c: opts}


def route_avoiding_polygon(
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    blockage_geojson: Dict[str, Any],
    costing: str = "bus",
    timeout_s: float = 15.0,
    *,
    exit_heading_deg: Optional[float] = None,
    rejoin_heading_deg: Optional[float] = None,
) -> OSMDetourResult:
    """
    Call Valhalla route API with exclude_polygons so the path goes around the blockage.
    Returns coordinates (lon, lat), distance_m, time_s. On failure returns success=False.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return OSMDetourResult(coordinates=[], distance_m=0.0, time_s=0.0, success=False)
    if _breaker.is_open():
        return OSMDetourResult(
            coordinates=[], distance_m=0.0, time_s=0.0, success=False,
            turn_by_turn=None,
        )

    exclude = _polygon_to_exclude_rings(blockage_geojson)
    body: Dict[str, Any] = {
        "locations": _valhalla_break_locations(
            from_lon, from_lat, to_lon, to_lat,
            exit_heading_deg=exit_heading_deg,
            rejoin_heading_deg=rejoin_heading_deg,
        ),
        "costing": costing,
        "costing_options": _valhalla_costing_options(costing),
        "units": "kilometers",
    }
    if exclude:
        body["exclude_polygons"] = exclude

    url = f"{VALHALLA_URL.rstrip('/')}/route"
    try:
        resp = httpx.post(url, json=body, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()
        _breaker.record_success()
    except Exception:
        _breaker.record_failure()
        return OSMDetourResult(coordinates=[], distance_m=0.0, time_s=0.0, success=False)

    trip = data.get("trip") or {}
    legs = trip.get("legs") or []
    if not legs:
        return OSMDetourResult(coordinates=[], distance_m=0.0, time_s=0.0, success=False)

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


def route_avoiding_polygon_alternates(
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    blockage_geojson: Dict[str, Any],
    costing: str = "bus",
    alternate_count: int = 2,
    timeout_s: float = 25.0,
) -> List[OSMDetourResult]:
    results, _debug = route_avoiding_polygon_alternates_debug(
        from_lon=from_lon,
        from_lat=from_lat,
        to_lon=to_lon,
        to_lat=to_lat,
        blockage_geojson=blockage_geojson,
        costing=costing,
        alternate_count=alternate_count,
        timeout_s=timeout_s,
    )
    return results


def route_avoiding_polygon_alternates_debug(
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    blockage_geojson: Dict[str, Any],
    costing: str = "bus",
    alternate_count: int = 2,
    timeout_s: float = 25.0,
    *,
    exit_heading_deg: Optional[float] = None,
    rejoin_heading_deg: Optional[float] = None,
) -> Tuple[List[OSMDetourResult], Dict[str, Any]]:
    """
    Request multiple route alternatives from Valhalla (when supported).
    Returns successful OSMDetourResult list plus compact diagnostics metadata.
    """
    out: List[OSMDetourResult] = []
    debug: Dict[str, Any] = {
        "valhalla_url_set": bool(VALHALLA_URL and VALHALLA_URL.strip()),
        "request_timeout_s": timeout_s,
        "alternates_requested": max(0, int(alternate_count)),
        "exclude_polygon_count": 0,
        "http_status": None,
        "error_type": None,
        "error_message": None,
        "primary_legs_count": 0,
        "alternates_count": 0,
        "fallback_attempted": False,
        "fallback_success": False,
    }
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        r = route_avoiding_polygon(
            from_lon,
            from_lat,
            to_lon,
            to_lat,
            blockage_geojson,
            costing=costing,
            timeout_s=timeout_s,
            exit_heading_deg=exit_heading_deg,
            rejoin_heading_deg=rejoin_heading_deg,
        )
        debug["fallback_attempted"] = True
        debug["fallback_success"] = bool(r.success)
        debug["error_type"] = "valhalla_url_missing"
        debug["error_message"] = "VALHALLA_URL is empty"
        return ([r] if r.success else []), debug

    if _breaker.is_open():
        debug["error_type"] = "valhalla_circuit_open"
        debug["error_message"] = "Circuit breaker open; Valhalla calls suspended temporarily"
        return [], debug

    exclude = _polygon_to_exclude_rings(blockage_geojson)
    debug["exclude_polygon_count"] = len(exclude) if exclude else 0
    alts_req = max(0, int(alternate_count))
    heading_tol = int(VALHALLA_HEADING_TOLERANCE_DEG)
    tight_r = int(VALHALLA_HEADING_SNAP_RADIUS_M)
    loose_r = int(VALHALLA_LOCATION_RADIUS_M)
    h_exit, h_rejoin = exit_heading_deg, rejoin_heading_deg

    location_batches: List[Tuple[str, List[Dict[str, Any]]]] = []
    if h_exit is not None and h_rejoin is not None:
        tr = tight_r if tight_r > 0 else loose_r
        location_batches.append(
            (
                "heading_tight",
                _valhalla_break_locations(
                    from_lon,
                    from_lat,
                    to_lon,
                    to_lat,
                    exit_heading_deg=h_exit,
                    rejoin_heading_deg=h_rejoin,
                    heading_tolerance_deg=heading_tol,
                    radius_exit_m=tr,
                    radius_rejoin_m=tr,
                ),
            )
        )
        location_batches.append(
            (
                "heading_loose",
                _valhalla_break_locations(
                    from_lon,
                    from_lat,
                    to_lon,
                    to_lat,
                    exit_heading_deg=h_exit,
                    rejoin_heading_deg=h_rejoin,
                    heading_tolerance_deg=heading_tol,
                    radius_exit_m=loose_r,
                    radius_rejoin_m=loose_r,
                ),
            )
        )
    location_batches.append(
        ("plain", _valhalla_break_locations(from_lon, from_lat, to_lon, to_lat)),
    )

    url = f"{VALHALLA_URL.rstrip('/')}/route"
    data: Optional[Dict[str, Any]] = None
    try:
        for loc_batch_name, locations in location_batches:
            base: Dict[str, Any] = {
                "locations": locations,
                "costing": costing,
                "costing_options": _valhalla_costing_options(costing),
                "units": "kilometers",
                "alternates": alts_req,
            }
            if exclude:
                base["exclude_polygons"] = exclude

            attempts: List[Tuple[str, Dict[str, Any]]] = [("primary", dict(base))]
            if alts_req != 0:
                b0 = dict(base)
                b0["alternates"] = 0
                attempts.append(("alternates_0", b0))
            if (costing or "").lower() == "bus":
                ba = dict(base)
                ba["alternates"] = 0
                ba["costing"] = "auto"
                ba["costing_options"] = _valhalla_costing_options("auto")
                attempts.append(("costing_auto_alternates_0", ba))

            for attempt_name, body in attempts:
                resp = httpx.post(url, json=body, timeout=timeout_s)
                debug["http_status"] = resp.status_code
                if resp.status_code < 400:
                    data = resp.json()
                    debug["valhalla_location_batch"] = loc_batch_name
                    debug["valhalla_attempt_used"] = attempt_name
                    debug["error_type"] = None
                    debug["error_message"] = None
                    debug.pop("valhalla_error_json", None)
                    _breaker.record_success()
                    break
                err_txt = (resp.text or "")[:2000]
                debug["error_type"] = "valhalla_http_error"
                debug["error_message"] = err_txt
                err_json: Dict[str, Any] = {}
                try:
                    j = resp.json()
                    if isinstance(j, dict):
                        err_json = j
                        debug["valhalla_error_json"] = j
                except Exception:
                    pass
                ec = err_json.get("error_code") if isinstance(err_json, dict) else None
                if ec != 442:
                    return out, debug
            if data is not None:
                break
        if data is None:
            return out, debug
    except Exception as e:
        _breaker.record_failure()
        debug["error_type"] = type(e).__name__
        debug["error_message"] = str(e)
        return out, debug

    trip = data.get("trip") or {}
    legs = trip.get("legs") or []
    debug["primary_legs_count"] = len(legs)
    if legs:
        one = _trip_legs_to_result(legs)
        if one.success:
            out.append(one)
    alts = data.get("alternates") or []
    debug["alternates_count"] = len(alts) if isinstance(alts, list) else 0
    for alt in alts:
        if not isinstance(alt, dict):
            continue
        t = alt.get("trip") or alt
        legs_a = t.get("legs") or []
        if not legs_a:
            continue
        r = _trip_legs_to_result(legs_a)
        if r.success:
            out.append(r)
    if not out:
        debug["fallback_attempted"] = True
        r = route_avoiding_polygon(
            from_lon,
            from_lat,
            to_lon,
            to_lat,
            blockage_geojson,
            costing=costing,
            timeout_s=timeout_s,
            exit_heading_deg=exit_heading_deg,
            rejoin_heading_deg=rejoin_heading_deg,
        )
        if r.success:
            out.append(r)
        debug["fallback_success"] = bool(r.success)
    return out, debug


def _normalize_trace_attributes_edge(edge: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valhalla may return flat keys like way_id/length or dotted keys (edge.way_id) or nested edge{}.
    Downstream code expects way_id, length (km), road_class at top level.
    """
    e = dict(edge)
    if e.get("way_id") is None:
        for k in ("edge.way_id",):
            if k in e and e[k] is not None:
                try:
                    e["way_id"] = int(e[k])
                except (TypeError, ValueError):
                    pass
                break
        if e.get("way_id") is None:
            inner = e.get("edge")
            if isinstance(inner, dict) and inner.get("way_id") is not None:
                try:
                    e["way_id"] = int(inner["way_id"])
                except (TypeError, ValueError):
                    pass
    if e.get("length") is None:
        for k in ("edge.length",):
            if k in e and e[k] is not None:
                try:
                    e["length"] = float(e[k])
                except (TypeError, ValueError):
                    pass
                break
        if e.get("length") is None:
            inner = e.get("edge")
            if isinstance(inner, dict) and inner.get("length") is not None:
                try:
                    e["length"] = float(inner["length"])
                except (TypeError, ValueError):
                    pass
    if e.get("road_class") is None:
        for k in ("edge.road_class",):
            if k in e and e[k] is not None:
                e["road_class"] = e[k]
                break
        if e.get("road_class") is None:
            inner = e.get("edge")
            if isinstance(inner, dict) and inner.get("road_class") is not None:
                e["road_class"] = inner["road_class"]
    return e


def match_route_attributes(
    coordinates_lonlat: List[Tuple[float, float]],
    costing: str = "bus",
    timeout_s: float = 10.0,
) -> Optional[List[Dict[str, Any]]]:
    """
    Call Valhalla /trace_attributes to get per-edge OSM way IDs and road attributes for a polyline.
    Returns list of edge dicts with keys: way_id, length, road_class, speed, access_restriction,
    use, surface, tunnel, bridge, toll.  Returns None on failure or when disabled/unreachable.
    """
    if not VALHALLA_TRACE_ATTRIBUTES_ENABLED:
        return None
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return None
    if _breaker.is_open():
        return None
    if len(coordinates_lonlat) < 2:
        return None
    # /trace_attributes expects lat,lon pairs
    shape_pts = [{"lat": lat, "lon": lon} for lon, lat in coordinates_lonlat]
    body: Dict[str, Any] = {
        "shape": shape_pts,
        "costing": costing,
        "shape_match": "map_snap",
        "filters": {
            "attributes": [
                "edge.way_id",
                "edge.length",
                "edge.speed",
                "edge.road_class",
                "edge.surface",
                "edge.tunnel",
                "edge.bridge",
                "edge.toll",
                "edge.access_restriction",
                "edge.use",
                "node.intersection_type",
                "matched.type",
            ],
            "action": "include",
        },
    }
    url = f"{VALHALLA_URL.rstrip('/')}/trace_attributes"
    try:
        resp = httpx.post(url, json=body, timeout=timeout_s)
        if resp.status_code >= 400:
            return None
        data = resp.json()
        edges = data.get("edges")
        if not isinstance(edges, list):
            return None
        return [_normalize_trace_attributes_edge(x) for x in edges if isinstance(x, dict)]
    except Exception:
        return None


def valhalla_locate(
    points_lonlat: List[Tuple[float, float]],
    costing: str = "bus",
    timeout_s: float = 5.0,
) -> Optional[List[Dict[str, Any]]]:
    """
    Call Valhalla /locate for each point; returns list of location result dicts.
    Each dict has 'input_lon/lat' and Valhalla's 'edges' array with 'way_id' and 'minimum_reachability'.
    Returns None on failure.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return None
    if _breaker.is_open():
        return None
    locs = [{"lat": lat, "lon": lon} for lon, lat in points_lonlat]
    body: Dict[str, Any] = {"locations": locs, "costing": costing}
    url = f"{VALHALLA_URL.rstrip('/')}/locate"
    try:
        resp = httpx.post(url, json=body, timeout=timeout_s)
        if resp.status_code >= 400:
            return None
        data = resp.json()
        if not isinstance(data, list):
            return None
        return data
    except Exception:
        return None


def valhalla_health(timeout_s: float = 5.0) -> Dict[str, Any]:
    """
    Call Valhalla /status. Returns dict with keys: ok, version, tileset_last_modified, error.
    Does NOT raise; safe to call at startup.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return {"ok": False, "error": "VALHALLA_URL not configured"}
    url = f"{VALHALLA_URL.rstrip('/')}/status"
    try:
        resp = httpx.get(url, timeout=timeout_s)
        if resp.status_code >= 400:
            return {"ok": False, "error": f"HTTP {resp.status_code}", "body": (resp.text or "")[:300]}
        data = resp.json()
        return {
            "ok": True,
            "version": data.get("version"),
            "tileset_last_modified": data.get("tileset_last_modified"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _trip_legs_to_result(legs: List[Dict[str, Any]]) -> OSMDetourResult:
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
    r = int(VALHALLA_LOCATION_RADIUS_M)
    locs: List[Dict[str, Any]] = []
    n = len(waypoints_lonlat)
    for i, (lon, lat) in enumerate(waypoints_lonlat):
        loc: Dict[str, Any] = {"lat": lat, "lon": lon}
        if i == 0 or i == n - 1:
            loc["type"] = "break"
        else:
            loc["type"] = "through"
        if r > 0:
            loc["radius"] = r
        locs.append(loc)
    body: Dict[str, Any] = {
        "locations": locs,
        "costing": costing,
        "costing_options": _valhalla_costing_options(costing),
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
