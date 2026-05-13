"""Orchestrate detour v2 pipeline for one trip."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import time
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, cast

from shapely.geometry import LineString, shape

from backend.infra import db_access as db
from backend.infra.config import (
    DETOUR_V2_TIMING_LOG,
    LEGAL_ANCHOR_INDEX_ANCHOR_VERSION,
    LEGAL_ANCHOR_INDEX_ENABLED,
    USE_MATCHED_PHYSICAL_GEOMETRY,
    VALIDATE_DETOUR_CARRIAGEWAY,
)
from backend.infra.logging_utils import log
from backend.adapters.osm_detour import route_avoiding_polygon, route_waypoints_avoiding_polygon, valhalla_locate

from backend.domain.detour_physical.debug_geojson import (
    build_detour_debug_feature_collection,
    coords_from_geojson_linestring,
)
from backend.domain.detour_physical.matched_trip_geometry import MatchedTripPhysical
from backend.infra import pattern_edge_match_repo as pattern_edge_match_repo

from .anchor_selector import enumerate_anchor_candidates, enumerate_intersection_anchor_candidates
from .intersection_finder import find_route_intersections
from . import legal_anchor_runtime
from .bus_feasibility_evaluator import evaluate_candidate
from .detour_validator import validate_detour_carriageway
from .candidate_decoder import decode_valhalla_candidate
from .corridor_builder import affected_shape_subline, incident_exclusion_polygon_for_stage, corridor_stages_order
from .detour_ranker import rank_candidates
from .incident_projector import edge_ban_way_ids, project_incident_polygon
from .models import AnchorPair, DetourComputeOutput, DetourComputeStatus, FeasibilityResult, RankedCandidate
from .tier_classifier import classify_tier
from .policy import DetourPolicyConfig, get_default_policy
from .road_candidate_generator import generate_candidates_with_debug
from .service_stitching import compute_stitching
from .trip_impact_analyzer import analyze_trip_impact


def _make_relaxed_policy(pol: DetourPolicyConfig) -> DetourPolicyConfig:
    """Return a copy of *pol* with GTFS-evidence hard-reject and coincide cap disabled.

    Used in the pass-2 fallback so the engine always surfaces *something*
    when strict mode rejects every Valhalla candidate.
    """
    relaxed_vehicle = dataclasses.replace(pol.vehicle, require_gtfs_way_evidence=False)
    relaxed_service = dataclasses.replace(pol.service, max_route_coincide_fraction=1.0)
    return dataclasses.replace(pol, vehicle=relaxed_vehicle, service=relaxed_service)


def _load_trip_context(trip_id: str) -> Tuple[Optional[str], Optional[str], Optional[LineString]]:
    """Return (route_id, shape_id, shape_line)."""
    try:
        row = db.get_trip_route_shape(trip_id)
    except Exception:
        row = None
    if not row:
        return None, None, None
    rid = str(row.get("route_id") or "")
    sid = row.get("shape_id")
    sid_s = str(sid) if sid is not None else None
    line = None
    if sid_s:
        try:
            line = db.get_shape_line(sid_s)
        except Exception:
            line = None
    return rid, sid_s, line


def _line_fraction_inside_blockage(
    coords: list[tuple[float, float]],
    blockage_geojson: Dict[str, Any],
) -> float:
    """Share of path length (approx, planar degrees) inside incident geometry."""
    if len(coords) < 2:
        return 0.0
    try:
        line = LineString(coords)
        blk = shape(blockage_geojson)
    except Exception:
        return 0.0
    if line.is_empty or line.length <= 0 or blk.is_empty:
        return 0.0
    inter = line.intersection(blk)
    if inter.is_empty:
        return 0.0
    try:
        ilen = float(inter.length)
    except Exception:
        return 0.0
    return min(1.0, ilen / float(line.length))


def _coincides_with_route_fraction(
    detour_coords: list[tuple[float, float]],
    route_line: LineString,
    exit_dist_m: float,
    rejoin_dist_m: float,
    total_m: float,
    *,
    tolerance_m: float = 30.0,
    sample_step_m: float = 20.0,
) -> float:
    """Fraction of detour length that lies within *tolerance_m* of the original route slice.

    A value close to 1.0 means Valhalla returned the original road segment
    unchanged (no real bypass).  We sample every *sample_step_m* along the
    detour and test proximity to the route sub-line between the exit and rejoin
    anchors.  Returns 0.0 on any geometry error.
    """
    if len(detour_coords) < 2 or total_m <= 0 or route_line.is_empty:
        return 0.0
    try:
        from shapely.ops import substring as _substring

        exit_norm = max(0.0, min(1.0, exit_dist_m / total_m))
        rejoin_norm = max(0.0, min(1.0, rejoin_dist_m / total_m))
        if rejoin_norm <= exit_norm:
            return 0.0
        route_slice = _substring(route_line, exit_norm, rejoin_norm, normalized=True)
        if route_slice is None or route_slice.is_empty:
            return 0.0

        detour_line = LineString(detour_coords)
        det_len = float(detour_line.length)
        if det_len <= 0:
            return 0.0

        # Convert tolerance to degrees (approx; 1 deg ≈ 111 320 m at the equator).
        tol_deg = tolerance_m / 111_320.0
        # Walk detour every sample_step_m worth of degrees.
        step_deg = max(sample_step_m / 111_320.0, det_len / 500.0)
        dist = 0.0
        coinciding = 0.0
        while dist <= det_len + 1e-9:
            pt = detour_line.interpolate(min(dist, det_len))
            if float(route_slice.distance(pt)) <= tol_deg:
                coinciding += step_deg
            dist += step_deg

        return min(1.0, coinciding / det_len)
    except Exception:
        return 0.0


def _bearing_deg(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Initial great-circle bearing in [0,360)."""
    p = math.pi / 180.0
    y = math.sin((lon2 - lon1) * p) * math.cos(lat2 * p)
    x = math.cos(lat1 * p) * math.sin(lat2 * p) - math.sin(lat1 * p) * math.cos(lat2 * p) * math.cos((lon2 - lon1) * p)
    b = math.degrees(math.atan2(y, x))
    return (b + 360.0) % 360.0


def _bearing_delta_deg(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def _line_forward_bearing(line: LineString, dist_m: float, total_m: float, probe_m: float = 45.0) -> Optional[float]:
    if total_m <= 0 or line.is_empty:
        return None
    t0 = max(0.0, min(1.0, dist_m / total_m))
    t1 = max(0.0, min(1.0, (dist_m + probe_m) / total_m))
    if t1 <= t0:
        t1 = min(1.0, t0 + 1e-6)
    p0 = line.interpolate(t0, normalized=True)
    p1 = line.interpolate(t1, normalized=True)
    return _bearing_deg(float(p0.x), float(p0.y), float(p1.x), float(p1.y))


def _polyline_probe_bearing(coords: list[tuple[float, float]], from_start: bool, min_probe_m: float = 45.0) -> Optional[float]:
    """Bearing probed from the start or end of a polyline in the FORWARD travel direction."""
    if len(coords) < 2:
        return None
    if from_start:
        lon0, lat0 = coords[0]
        acc_m = 0.0
        for i in range(1, len(coords)):
            lon1, lat1 = coords[i]
            seg_m = float(LineString([(lon0, lat0), (lon1, lat1)]).length * 111_320.0)
            acc_m += seg_m
            if acc_m >= min_probe_m or i == len(coords) - 1:
                return _bearing_deg(lon0, lat0, lon1, lat1)
            lon0, lat0 = lon1, lat1
    else:
        # Approach bearing at the END — travel direction as the route nears its last point.
        # Walk backward from second-to-last to find a point far enough, then measure
        # bearing FROM that point TO the final point (forward direction).
        final_lon, final_lat = coords[-1]
        acc_m = 0.0
        for i in range(len(coords) - 2, -1, -1):
            lon0, lat0 = coords[i]
            seg_m = float(LineString([(lon0, lat0), (final_lon, final_lat)]).length * 111_320.0)
            acc_m += seg_m
            if acc_m >= min_probe_m or i == 0:
                return _bearing_deg(lon0, lat0, final_lon, final_lat)
    return None


def _backtrack_heading_deltas(
    *,
    line: LineString,
    total_m: float,
    exit_dist_m: float,
    rejoin_dist_m: float,
    route_coords: list[tuple[float, float]],
) -> tuple[Optional[float], Optional[float]]:
    line_exit = _line_forward_bearing(line, exit_dist_m, total_m)
    line_rejoin = _line_forward_bearing(line, rejoin_dist_m, total_m)
    route_start = _polyline_probe_bearing(route_coords, from_start=True)
    route_end = _polyline_probe_bearing(route_coords, from_start=False)
    d_exit = _bearing_delta_deg(route_start, line_exit) if (route_start is not None and line_exit is not None) else None
    d_rejoin = _bearing_delta_deg(route_end, line_rejoin) if (route_end is not None and line_rejoin is not None) else None
    return d_exit, d_rejoin


# Fraction compare tolerance (planar deg²); keeps boundary-only touches from false positives.
_INCIDENT_OVERLAP_FRAC_EPS = 1e-9


def _matched_physical_trusted(mtp: MatchedTripPhysical, pol: DetourPolicyConfig) -> bool:
    """Use persisted matched geometry for impact + anchors only when path and anchor gates pass."""
    pp = pol.physical_path
    if mtp.line is None or mtp.line.is_empty:
        return False
    return mtp.passes_path_thresholds(pp) and mtp.passes_anchor_thresholds(pp)


def _physical_segment_validation_sets(
    mtp: MatchedTripPhysical,
    blocked_start_m: float,
    blocked_end_m: float,
) -> Tuple[set[int], set[int], set[int]]:
    """Derive segment id sets for validator from per-leg summary metadata (best-effort)."""
    blocked_ids: set[int] = set()
    exit_ids: set[int] = set()
    rejoin_ids: set[int] = set()
    for row in mtp.per_pair:
        if not row.get("present"):
            continue
        lo = float(row.get("cum_dist_start_m") or 0)
        hi = float(row.get("cum_dist_end_m") or 0)
        es = row.get("entry_segment_id")
        xs = row.get("exit_segment_id")
        if es is not None and lo <= blocked_end_m and hi >= blocked_start_m:
            blocked_ids.add(int(es))
        if xs is not None and lo <= blocked_end_m and hi >= blocked_start_m:
            blocked_ids.add(int(xs))
        if es is not None and lo < blocked_start_m <= hi:
            exit_ids.add(int(es))
        if xs is not None and lo <= blocked_end_m < hi:
            rejoin_ids.add(int(xs))
    return blocked_ids, exit_ids, rejoin_ids


def _no_safe_detour_aggregate_reason(corridor_debug: list[Dict[str, Any]]) -> str:
    """Primary failure label after narrow → medium → wide."""
    if not corridor_debug:
        return "unknown"
    if any(r.get("corridor_outcome") == "all_rejected_incident_overlap" for r in corridor_debug):
        return "all_candidates_intersect_incident"
    if any(r.get("corridor_outcome") == "all_rejected_backtrack_or_uturn" for r in corridor_debug):
        return "all_candidates_backtrack_or_uturn"
    for r in reversed(corridor_debug):
        cg = (r.get("candidate_generation_reason") or "").strip()
        if cg == "valhalla_http_error":
            return "no_valhalla_path_with_exclusions"
        if cg and cg not in ("", "valhalla_routes_returned"):
            return cg
    return "no_safe_detour"


def _log_stage(trip_id: str, stage: str, **fields: object) -> None:
    parts = [f"trip_id={trip_id}", f"stage={stage}"]
    for k, v in fields.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    log("detours/v2/compute", " ".join(parts))


def _exclude_hash(geojson: Optional[Dict[str, Any]]) -> str:
    """Stable short hash of exclusion polygon for Valhalla call-cache keying."""
    if not geojson:
        return "none"
    try:
        s = json.dumps(geojson, sort_keys=True)
        return hashlib.sha1(s.encode()).hexdigest()[:12]
    except Exception:
        return "err"


_ROAD_CLASS_RANK: Dict[str, int] = {
    "motorway": 0, "motorway_link": 0,
    "trunk": 0, "trunk_link": 0,
    "primary": 0, "primary_link": 0,
    "secondary": 0, "secondary_link": 0,
    "tertiary": 0, "tertiary_link": 0,
    "residential": 1, "unclassified": 1, "road": 1,
    "service": 2, "service_other": 2,
    "living_street": 2, "alley": 2, "driveway": 2, "parking_aisle": 2,
    "track": 2, "footway": 2, "path": 2, "cycleway": 2,
    "pedestrian": 2, "steps": 2, "bridleway": 2,
}


def _validate_anchors_via_locate(
    anchors_list: List[AnchorPair],
    policy: DetourPolicyConfig,
    banned_way_ids: Optional[set[int]] = None,
) -> List[AnchorPair]:
    """Drop unreachable anchors and enrich with road class; filter out service/footway/banned anchors."""
    min_reach = int(getattr(policy.anchor, "min_locate_reachability_nodes", 50))
    # Collect unique points.
    points: List[Tuple[float, float]] = []
    seen: set[tuple[float, float]] = set()
    for ap in anchors_list:
        for pt in [(ap.exit_lon, ap.exit_lat), (ap.rejoin_lon, ap.rejoin_lat)]:
            if pt not in seen:
                seen.add(pt)
                points.append(pt)
    loc_results = None
    try:
        if points:
            loc_results = valhalla_locate(points, costing="bus")
    except Exception:
        loc_results = None

    reach_map: Dict[Tuple[float, float], int] = {}
    road_class_map: Dict[Tuple[float, float], str] = {}
    road_class_rank_map: Dict[Tuple[float, float], int] = {}
    way_id_map: Dict[Tuple[float, float], int] = {}

    if loc_results and isinstance(loc_results, list):
        for i, (lon, lat) in enumerate(points):
            if i >= len(loc_results):
                break
            loc = loc_results[i]
            if not isinstance(loc, dict):
                continue
            edges = loc.get("edges") or []
            best_reach = max((int(e.get("minimum_reachability") or 0) for e in edges if isinstance(e, dict)), default=0)
            key = (round(lon, 7), round(lat, 7))
            reach_map[key] = best_reach
            # Use the best-reachability edge for road class + way ID.
            best_edge = max(
                (e for e in edges if isinstance(e, dict)),
                key=lambda e: int(e.get("minimum_reachability") or 0),
                default=None,
            )
            if best_edge is not None:
                rc = str(best_edge.get("road_class") or "")
                road_class_map[key] = rc
                road_class_rank_map[key] = _ROAD_CLASS_RANK.get(rc.lower(), 1)
                wid = int(best_edge.get("way_id") or 0)
                way_id_map[key] = wid

    validated_ok: List[AnchorPair] = []
    rejected_class2: List[AnchorPair] = []
    for ap in anchors_list:
        exit_key = (round(ap.exit_lon, 7), round(ap.exit_lat, 7))
        rejoin_key = (round(ap.rejoin_lon, 7), round(ap.rejoin_lat, 7))
        # Reachability gate (skip when locate call failed → default to passing).
        if min_reach > 0 and loc_results is not None:
            exit_r = reach_map.get(exit_key, min_reach)
            rejoin_r = reach_map.get(rejoin_key, min_reach)
            if exit_r < min_reach or rejoin_r < min_reach:
                continue
        # Enrich with road class.
        ap.exit_road_class = road_class_map.get(exit_key)
        ap.rejoin_road_class = road_class_map.get(rejoin_key)
        ap.exit_road_class_rank = road_class_rank_map.get(exit_key, 1)
        ap.rejoin_road_class_rank = road_class_rank_map.get(rejoin_key, 1)
        # Drop anchors whose snap way is in the incident ban list.
        exit_wid = way_id_map.get(exit_key, 0)
        rejoin_wid = way_id_map.get(rejoin_key, 0)
        if banned_way_ids and ((exit_wid and exit_wid in banned_way_ids) or (rejoin_wid and rejoin_wid in banned_way_ids)):
            continue
        max_rank = max(ap.exit_road_class_rank, ap.rejoin_road_class_rank)
        if max_rank >= 2:
            rejected_class2.append(ap)
        else:
            validated_ok.append(ap)

    if validated_ok:
        # Sort by best road-class rank first, then keep original ordering within same rank.
        validated_ok.sort(key=lambda a: (max(a.exit_road_class_rank, a.rejoin_road_class_rank), 0))
        return validated_ok
    # Fall back to class-2 anchors if nothing better exists.
    if rejected_class2:
        return rejected_class2
    # Nothing survived: return original list (Valhalla unavailable / all banned).
    return anchors_list


def _ridership_weight_for_stop(
    stop_id: str,
    stop_lonlat: Dict[str, Tuple[float, float]],
    gtfs_way_evidence: Dict[int, Dict[str, Any]],
) -> float:
    """Estimate ridership weight [1.0, ~3.0] from nearby GTFS way evidence route_count."""
    # If we have decoded way evidence, use max route_count as proxy for ridership.
    if not gtfs_way_evidence:
        return 1.0
    max_rc = max(
        (int((v or {}).get("route_count") or 0) for v in gtfs_way_evidence.values()),
        default=0,
    )
    if max_rc <= 0:
        return 1.0
    import math as _math
    return 1.0 + _math.log1p(max_rc) / _math.log1p(25)


def _try_via_stop_route(
    best_selected: RankedCandidate,
    best_anchors: AnchorPair,
    best_stitch,
    blockage_geojson: Dict[str, Any],
    stop_lonlat: Dict[str, Tuple[float, float]],
    policy: DetourPolicyConfig,
    trip_id: str,
) -> Optional[RankedCandidate]:
    """D1: retry routing with skipped stops inserted as through-points (via-stops)."""
    skipped = list(best_stitch.skipped_stop_ids or [])
    if not skipped:
        return None
    corridor_m = float(getattr(policy.service, "via_stop_corridor_m", 60.0))
    time_factor = float(getattr(policy.service, "via_extra_time_factor", 1.25))
    if not best_selected.decoded:
        return None
    geom_coords = None
    if best_selected.decoded.road_segments:
        geom_fc = best_selected.decoded.geometry_geojson
        features = geom_fc.get("features") or [] if isinstance(geom_fc, dict) else []
        for feat in features:
            geom = (feat.get("geometry") or {}) if isinstance(feat, dict) else {}
            if geom.get("type") == "LineString":
                geom_coords = geom.get("coordinates") or []
                break
    if not geom_coords or len(geom_coords) < 2:
        return None
    try:
        detour_line = LineString(geom_coords)
    except Exception:
        return None
    # Find skipped stops within corridor_m of detour path.
    deg_per_m = 1.0 / 111_320.0
    via_pts: List[Tuple[float, float]] = []
    for sid in skipped:
        lonlat = stop_lonlat.get(str(sid))
        if not lonlat:
            continue
        from shapely.geometry import Point
        try:
            pt = Point(lonlat[0], lonlat[1])
            dist_deg = detour_line.distance(pt)
            if dist_deg <= corridor_m * deg_per_m:
                via_pts.append(lonlat)
        except Exception:
            continue
    if not via_pts:
        return None
    waypoints = [(best_anchors.exit_lon, best_anchors.exit_lat)] + via_pts + [(best_anchors.rejoin_lon, best_anchors.rejoin_lat)]
    try:
        via_result = route_waypoints_avoiding_polygon(waypoints, blockage_geojson, costing="bus")
    except Exception:
        return None
    if not via_result.success:
        return None
    if via_result.time_s > best_selected.travel_time_s * time_factor:
        return None
    # Build a new RankedCandidate from the via result.
    from .candidate_decoder import decode_valhalla_candidate
    via_decoded = decode_valhalla_candidate(
        via_result.coordinates, via_result.time_s, match_osm_segments=True
    )
    via_feas = best_selected.feasibility
    breakdown = dict(best_selected.score_breakdown)
    breakdown["via_stops_added"] = float(len(via_pts))
    breakdown["travel_time_s"] = via_result.time_s
    _log_stage(
        trip_id, "via_stop_route",
        via_stops=len(via_pts),
        via_time_s=round(via_result.time_s, 1),
        base_time_s=round(best_selected.travel_time_s, 1),
    )
    from .models import RankedCandidate as RC
    return RC(
        strategy="via_stops",
        total_score=via_result.time_s + (best_selected.total_score - best_selected.travel_time_s),
        travel_time_s=via_result.time_s,
        distance_m=via_result.distance_m,
        decoded=via_decoded,
        feasibility=via_feas,
        rejection_reasons=[],
        score_breakdown=breakdown,
    )


def _gtfs_evidence_distance_fraction(decoded: Any, gtfs_ev: Dict[int, Dict[str, Any]]) -> float:
    total = sum(s.length_m for s in decoded.road_segments) or 1.0
    ok_m = 0.0
    for s in decoded.road_segments:
        if s.synthetic:
            continue
        w = int(s.osm_way_id or 0)
        if w and w in gtfs_ev and float((gtfs_ev[w] or {}).get("confidence_score") or 0.0) > 0.05:
            ok_m += s.length_m
    return ok_m / total


def _tier_status(tier: str) -> str:
    return str(tier or "REVIEW_RECOMMENDED").lower()


def _build_emergency_fallback(
    *,
    anchors: AnchorPair,
    blockage_geojson: Dict[str, Any],
    projection: Any,
    pol: DetourPolicyConfig,
    feed_id: int,
    anchor_shape_line: LineString,
    total_m: float,
    stop_rows: List[Dict[str, Any]],
    stop_lonlat: Dict[str, Tuple[float, float]],
) -> RankedCandidate:
    """Last-resort path: Valhalla auto costing, else straight-line synthetic geometry."""
    stitch_pre = compute_stitching(
        line=anchor_shape_line,
        total_m=total_m,
        stop_rows=stop_rows,
        stop_lonlat=stop_lonlat,
        exit_dist_m=anchors.exit_shape_dist_m,
        rejoin_dist_m=anchors.rejoin_shape_dist_m,
        policy=pol,
        detour_line=None,
    )
    baseline_d = max(0.0, anchors.rejoin_shape_dist_m - anchors.exit_shape_dist_m)
    baseline_t = baseline_d / max(5.0, 8.0)
    r = route_avoiding_polygon(
        anchors.exit_lon,
        anchors.exit_lat,
        anchors.rejoin_lon,
        anchors.rejoin_lat,
        blockage_geojson,
        costing="auto",
        timeout_s=25.0,
        exit_heading_deg=anchors.exit_forward_bearing_deg,
        rejoin_heading_deg=anchors.rejoin_forward_bearing_deg,
    )
    if r.success and len(r.coordinates) >= 2:
        coords = list(r.coordinates)
        t_s = float(r.time_s)
        d_m = float(r.distance_m)
        decoded = decode_valhalla_candidate(coords, t_s, match_osm_segments=True, feed_id=feed_id)
        turn_bt = r.turn_by_turn
    else:
        coords = [(anchors.exit_lon, anchors.exit_lat), (anchors.rejoin_lon, anchors.rejoin_lat)]
        from shapely.geometry import LineString as _LS

        d_m = float(_LS(coords).length * 111_320.0)
        t_s = max(30.0, d_m / 8.0)
        decoded = decode_valhalla_candidate(coords, t_s, match_osm_segments=False, feed_id=feed_id)
        turn_bt = None
    try:
        detour_line = LineString(coords)
    except Exception:
        detour_line = LineString([(anchors.exit_lon, anchors.exit_lat), (anchors.rejoin_lon, anchors.rejoin_lat)])
    stitch_full = compute_stitching(
        line=anchor_shape_line,
        total_m=total_m,
        stop_rows=stop_rows,
        stop_lonlat=stop_lonlat,
        exit_dist_m=anchors.exit_shape_dist_m,
        rejoin_dist_m=anchors.rejoin_shape_dist_m,
        policy=pol,
        detour_line=detour_line,
    )
    way_ids = sorted({int(s.osm_way_id) for s in decoded.road_segments if int(s.osm_way_id or 0) > 0})
    gtfs_ev = db.get_gtfs_bus_way_evidence_bulk(feed_id, way_ids) if way_ids else {}
    bus_ev = db.get_bus_edge_evidence_bulk(way_ids) if way_ids else {}
    olap = _line_fraction_inside_blockage(coords, blockage_geojson)
    cap = float(pol.service.max_incident_overlap_fraction)
    path_bad = olap > cap + _INCIDENT_OVERLAP_FRAC_EPS
    feas = evaluate_candidate(
        decoded=decoded,
        projection=projection,
        policy=pol,
        baseline_blocked_distance_m=baseline_d,
        baseline_blocked_time_s=baseline_t,
        detour_distance_m=d_m,
        detour_time_s=t_s,
        banned_way_ids=edge_ban_way_ids(projection),
        gtfs_way_evidence=gtfs_ev,
        bus_edge_evidence=bus_ev,
        turn_by_turn=turn_bt,
        bearing_delta_exit_deg=None,
        bearing_delta_rejoin_deg=None,
        path_intersects_blockage=path_bad,
        stitch_ok=stitch_pre.stitch_ok,
        invalid_geometry=len(coords) < 2,
        carriageway_reasons=None,
    )
    if not feas.accepted:
        feas = FeasibilityResult(
            accepted=True,
            hard_reject_reasons=[],
            segment_penalty_s=feas.segment_penalty_s + 800.0,
            turn_penalty_s=feas.turn_penalty_s,
            uncertainty_penalty_s=feas.uncertainty_penalty_s + 400.0,
            service_penalty_s=feas.service_penalty_s,
            evidence_bonus_s=feas.evidence_bonus_s,
            notes=list(feas.notes) + ["emergency_force_accept_geometry"],
            sharp_turn_count=feas.sharp_turn_count,
            confidence_score=max(0.08, feas.confidence_score * 0.5),
            warnings=list(feas.warnings) + ["emergency_fallback_unverified"],
        )
    total = (
        t_s
        + feas.segment_penalty_s
        + feas.turn_penalty_s
        + feas.uncertainty_penalty_s
        + feas.service_penalty_s
        + feas.evidence_bonus_s
    )
    passed = ["emergency_geometry"]
    if stitch_pre.stitch_ok:
        passed.append("rejoins_downstream")
    if not path_bad:
        passed.append("avoids_blockage")
    return RankedCandidate(
        strategy="emergency_fallback",
        total_score=total,
        travel_time_s=t_s,
        distance_m=d_m,
        decoded=decoded,
        feasibility=feas,
        rejection_reasons=[],
        score_breakdown={
            "travel_time_s": t_s,
            "corridor": "emergency",
            "anchor_index": -1.0,
            "skipped_stops": float(len(stitch_full.skipped_stop_ids)),
            "exit_shape_dist_m": float(anchors.exit_shape_dist_m),
            "rejoin_shape_dist_m": float(anchors.rejoin_shape_dist_m),
            "total_score": total,
        },
        tier="EMERGENCY_FALLBACK",
        confidence_score=0.12,
        warnings=["emergency_fallback_unverified_for_buses"],
        hard_constraints_passed=passed,
        review_required=True,
    )


def compute_detour_for_trip(
    *,
    trip_id: str,
    blockage_geojson: Dict[str, Any],
    service_date: str,
    policy: Optional[DetourPolicyConfig] = None,
    incident_id: Optional[int] = None,
    _valhalla_cache: Optional[Dict[str, Any]] = None,
    debug_detour: bool = False,
    use_matched_physical: bool = False,
) -> DetourComputeOutput:
    pol = policy or get_default_policy()
    # Per-request Valhalla response cache: key → (cands, cdebug). Shared across call when
    # multiple trips use the same shape (B3).
    vcache: Dict[str, Any] = _valhalla_cache if _valhalla_cache is not None else {}
    deadline_ms = int(getattr(pol.search, "per_trip_deadline_ms", 12000))
    t_start = time.monotonic()
    stage = "load_trip_context"
    try:
        _log_stage(trip_id, stage, service_date=service_date, incident_id=incident_id)
        route_id, shape_id, line = _load_trip_context(trip_id)
        if not route_id or line is None or getattr(line, "is_empty", True):
            _log_stage(trip_id, stage, status="error", error="trip_or_shape_not_found")
            return DetourComputeOutput(
                status="error",
                trip_id=trip_id,
                route_id=route_id or "",
                error="trip_or_shape_not_found",
                policy_version=pol.version,
            )

        stage = "impact_analysis"
        matched_physical = MatchedTripPhysical(line=None)
        physical_line = None
        physical_path_used = False
        physical_fallback_reason: Optional[str] = None
        if USE_MATCHED_PHYSICAL_GEOMETRY or use_matched_physical:
            try:
                matched_physical = pattern_edge_match_repo.get_concatenated_matched_geom_for_trip(
                    trip_id, policy=pol.physical_path
                )
                if _matched_physical_trusted(matched_physical, pol):
                    physical_line = matched_physical.line
                    physical_path_used = True
                else:
                    physical_line = None
                    if matched_physical.line is None:
                        physical_fallback_reason = matched_physical.fallback_reason or "no_matched_line"
                    elif not matched_physical.passes_path_thresholds(pol.physical_path):
                        physical_fallback_reason = "path_coverage_or_quality"
                    elif not matched_physical.passes_anchor_thresholds(pol.physical_path):
                        physical_fallback_reason = "anchor_coverage_gate"
                    else:
                        physical_fallback_reason = "physical_policy"
            except Exception as ex:
                matched_physical = MatchedTripPhysical(line=None, fallback_reason="load_error")
                physical_fallback_reason = str(ex)[:200]
        anchor_shape_line = physical_line if physical_path_used else line
        impact = analyze_trip_impact(
            trip_id=trip_id,
            route_id=route_id,
            shape_id=shape_id,
            shape_line=line,
            blockage_geojson=blockage_geojson,
            physical_shape_line=physical_line,
        )
        if not impact.intersects_blockage or impact.blocked is None:
            _log_stage(trip_id, stage, status="no_impact")
            return DetourComputeOutput(
                status="no_impact",
                trip_id=trip_id,
                route_id=route_id,
                policy_version=pol.version,
            )

        blocked_len_m = int(round(float(impact.blocked.blocked_end_m - impact.blocked.blocked_start_m)))
        _log_stage(trip_id, stage, blocked_len_m=blocked_len_m)

        stage = "load_stops"
        stop_rows = db.get_stop_times_for_trip(trip_id)
        sids = [str(r["stop_id"]) for r in stop_rows]
        stop_lonlat = db.get_stop_lonlat_bulk(sids)
        _log_stage(trip_id, stage, stop_count=len(stop_rows))

        total_m = float(impact.blocked.shape_length_m or 0.0) if impact.blocked else 0.0
        if total_m <= 0:
            from .trip_impact_analyzer import _line_length_m

            total_m = _line_length_m(anchor_shape_line)

        feed_id = db.get_active_feed_id()
        evidence_diag_stats: Dict[str, Any] = {}
        if feed_id is not None:
            try:
                evidence_diag_stats = db.get_detour_evidence_diag_stats(int(feed_id))
            except Exception as _diag_ex:
                evidence_diag_stats = {"evidence_diag_error": str(_diag_ex)[:200]}
        stage = "incident_projection"
        projection = project_incident_polygon(
            blockage_geojson=blockage_geojson,
            feed_id=feed_id,
            db_osm_available=True,
            narrow_buffer_m=float(pol.corridor.narrow_buffer_m),
        )
        banned = edge_ban_way_ids(projection)
        _log_stage(trip_id, stage, edge_bans=len(banned), feed_id=feed_id, **evidence_diag_stats)

        stage = "anchor_selection"
        radii = list(getattr(pol.anchor, "search_radii_m", None) or [400.0, 800.0, 1500.0, 3000.0, 5000.0])
        anchor_seen: set[Tuple[int, int]] = set()
        anchor_candidates: List[AnchorPair] = []

        # Build a (stop_id, shape_dist_m) list for intersection anchor labelling.
        from .anchor_selector import _stops_with_dist_along as _swd_along
        stops_with_dist_list = _swd_along(anchor_shape_line, total_m, stop_rows, stop_lonlat)

        # Attempt intersection-based anchor selection first.
        anchor_shape_coords_for_trace = list(anchor_shape_line.coords)
        try:
            route_intersections = find_route_intersections(
                route_coords_lonlat=anchor_shape_coords_for_trace,
                total_m=total_m,
                blocked_start_m=impact.blocked.blocked_start_m,
                blocked_end_m=impact.blocked.blocked_end_m,
                window_m=float(radii[-1]) if radii else 5000.0,
            )
        except Exception:
            route_intersections = []

        anchor_source_used = "intersection_finder" if route_intersections else "stop_window"
        _log_stage(
            trip_id,
            "anchor_source",
            anchor_source=anchor_source_used,
            intersection_count=len(route_intersections),
        )

        for rad in radii:
            if route_intersections:
                batch = enumerate_intersection_anchor_candidates(
                    intersections=route_intersections,
                    blocked=impact.blocked,
                    stops_with_dist=stops_with_dist_list,
                    policy=pol,
                    max_pairs=pol.anchor.candidate_pairs_k,
                    search_before_window_m=float(rad),
                    search_after_window_m=float(rad),
                    blockage_geojson=blockage_geojson,
                )
                if not batch:
                    # No intersection pair straddles the blockage at this radius;
                    # fall back to stop-window for this radius iteration.
                    batch = enumerate_anchor_candidates(
                        line=anchor_shape_line,
                        blocked=impact.blocked,
                        stop_rows=stop_rows,
                        stop_lonlat=stop_lonlat,
                        policy=pol,
                        max_pairs=pol.anchor.candidate_pairs_k,
                        search_before_window_m=float(rad),
                        search_after_window_m=float(rad),
                        blockage_geojson=blockage_geojson,
                    )
            else:
                batch = enumerate_anchor_candidates(
                    line=anchor_shape_line,
                    blocked=impact.blocked,
                    stop_rows=stop_rows,
                    stop_lonlat=stop_lonlat,
                    policy=pol,
                    max_pairs=pol.anchor.candidate_pairs_k,
                    search_before_window_m=float(rad),
                    search_after_window_m=float(rad),
                    blockage_geojson=blockage_geojson,
                )
            for anc in batch:
                key = (int(round(anc.exit_shape_dist_m)), int(round(anc.rejoin_shape_dist_m)))
                if key in anchor_seen:
                    continue
                anchor_seen.add(key)
                anc.search_radius_m = float(rad)
                anchor_candidates.append(anc)
        if (
            LEGAL_ANCHOR_INDEX_ENABLED
            and physical_path_used
            and impact.blocked is not None
        ):
            try:
                fv = db.get_active_feed_version_key()
                pid = db.get_pattern_id_for_trip(trip_id)
                if pid:
                    idx_rows = db.fetch_pattern_legal_anchor_candidates(
                        fv, pid, anchor_version=LEGAL_ANCHOR_INDEX_ANCHOR_VERSION
                    )
                    if idx_rows:
                        ex_rows, rj_rows = legal_anchor_runtime.load_index_rows(idx_rows)
                        total_for_idx = float(impact.blocked.shape_length_m or 0.0)
                        if total_for_idx <= 0.0:
                            from .trip_impact_analyzer import _line_length_m

                            total_for_idx = _line_length_m(anchor_shape_line)
                        legal_pair = legal_anchor_runtime.select_legal_anchor_pair(
                            ex_rows,
                            rj_rows,
                            blocked_start_m=float(impact.blocked.blocked_start_m),
                            blocked_end_m=float(impact.blocked.blocked_end_m),
                            total_shape_m=total_for_idx,
                            policy=pol,
                        )
                        if legal_pair is not None:
                            legal_pair.search_radius_m = float(radii[0]) if radii else 400.0
                            anchor_candidates.insert(0, legal_pair)
            except Exception:
                pass
        anchor_candidates = _validate_anchors_via_locate(anchor_candidates, pol, banned_way_ids=banned)
        if not anchor_candidates:
            anchor_candidates = enumerate_anchor_candidates(
                line=anchor_shape_line,
                blocked=impact.blocked,
                stop_rows=stop_rows,
                stop_lonlat=stop_lonlat,
                policy=pol,
                max_pairs=1,
                blockage_geojson=blockage_geojson,
            )
        for ap in anchor_candidates:
            exb = _line_forward_bearing(anchor_shape_line, ap.exit_shape_dist_m, total_m)
            rjb = _line_forward_bearing(anchor_shape_line, ap.rejoin_shape_dist_m, total_m)
            ap.exit_forward_bearing_deg = exb
            ap.rejoin_forward_bearing_deg = rjb
            if physical_path_used:
                ap.anchor_geometry_source = "matched_physical"
            else:
                ap.anchor_geometry_source = "gtfs_shape"

        anchors = anchor_candidates[0]
        _log_stage(
            trip_id,
            stage,
            anchor_candidate_count=len(anchor_candidates),
            anchor_source=anchor_source_used,
            exit_stop_id=anchors.exit_stop_id,
            rejoin_stop_id=anchors.rejoin_stop_id,
        )

        corridor_debug: list[Dict[str, Any]] = []
        all_rank_inputs: List[
            Tuple[str, float, float, Any, Dict[str, Any], Any]
        ] = []
        all_metas: List[Dict[str, Any]] = []
        # Parallel list: raw kwargs passed to evaluate_candidate per entry in all_rank_inputs.
        # Used by pass-2 to re-evaluate the same candidates with relaxed policy.
        eval_param_records: List[Dict[str, Any]] = []
        best_gtfs_ev: Dict[int, Dict[str, Any]] = {}

        val_blocked_seg: set[int] = set()
        val_exit_seg: set[int] = set()
        val_rejoin_seg: set[int] = set()
        if physical_path_used and impact.blocked is not None:
            vb, ve, vrj = _physical_segment_validation_sets(
                matched_physical,
                float(impact.blocked.blocked_start_m),
                float(impact.blocked.blocked_end_m),
            )
            val_blocked_seg, val_exit_seg, val_rejoin_seg = vb, ve, vrj

        concurrency = int(getattr(pol.search, "valhalla_concurrency", 4))
        early_score = float(getattr(pol.search, "early_accept_score", 200.0))
        early_tier_ok = bool(getattr(pol.search, "early_accept_tier_auto_ok", True))

        work_items: List[Tuple[int, AnchorPair, str, Dict[str, Any]]] = []
        for anchor_idx, anc in enumerate(anchor_candidates):
            for corridor_stage in corridor_stages_order():
                excl = incident_exclusion_polygon_for_stage(
                    blockage_geojson,
                    corridor_stage,
                    pol,
                    line=anchor_shape_line,
                    anchors=anc,
                    shape_length_m=total_m,
                )
                work_items.append((anchor_idx, anc, corridor_stage, excl))

        def _run_one(item: Tuple[int, AnchorPair, str, Dict[str, Any]]) -> Tuple[int, AnchorPair, str, list, Dict[str, Any]]:
            anchor_idx, anc, corridor_stage, excl = item
            cache_key = (
                f"{round(anc.exit_lon,6)},{round(anc.exit_lat,6)}"
                f"|{round(anc.rejoin_lon,6)},{round(anc.rejoin_lat,6)}"
                f"|{corridor_stage}|{_exclude_hash(excl)}"
                f"|h={round(anc.exit_forward_bearing_deg or -1.0, 1)},{round(anc.rejoin_forward_bearing_deg or -1.0, 1)}"
            )
            if cache_key in vcache:
                return anchor_idx, anc, corridor_stage, *vcache[cache_key]
            cands, cdebug = generate_candidates_with_debug(
                anc.exit_lon,
                anc.exit_lat,
                anc.rejoin_lon,
                anc.rejoin_lat,
                excl,
                alternate_count=2,
                exit_heading_deg=anc.exit_forward_bearing_deg,
                rejoin_heading_deg=anc.rejoin_forward_bearing_deg,
            )
            vcache[cache_key] = (cands, cdebug)
            return anchor_idx, anc, corridor_stage, cands, cdebug

        # B1: dispatch Valhalla calls in parallel threads.
        futures: List[Future] = []
        executor = ThreadPoolExecutor(max_workers=min(concurrency, len(work_items) or 1))
        try:
            for item in work_items:
                futures.append(executor.submit(_run_one, item))

            stage = "candidate_generation"
            for future in as_completed(futures):
                # C4: per-trip deadline check.
                elapsed_ms = (time.monotonic() - t_start) * 1000.0
                if elapsed_ms > deadline_ms and len(all_rank_inputs) > 0:
                    _log_stage(trip_id, "deadline", elapsed_ms=round(elapsed_ms), outcome="returning_best_so_far")
                    break

                try:
                    anchor_idx, anchors, corridor_stage, cands, cdebug = future.result()
                except Exception as fe:
                    log("detours/v2/compute", f"trip_id={trip_id} stage=candidate_generation future_error={fe!s}")
                    continue

                stitch_pre = compute_stitching(
                    line=anchor_shape_line,
                    total_m=total_m,
                    stop_rows=stop_rows,
                    stop_lonlat=stop_lonlat,
                    exit_dist_m=anchors.exit_shape_dist_m,
                    rejoin_dist_m=anchors.rejoin_shape_dist_m,
                    policy=pol,
                    detour_line=None,
                )
                err_detail = ""
                if isinstance(cdebug.get("valhalla_error_json"), dict):
                    err_detail = str(cdebug.get("valhalla_error_json"))[:800]
                elif cdebug.get("error_message"):
                    err_detail = str(cdebug.get("error_message"))[:800]
                cdebug_row: Dict[str, Any] = {
                    "anchor_index": anchor_idx,
                    "exit_stop_id": anchors.exit_stop_id,
                    "rejoin_stop_id": anchors.rejoin_stop_id,
                    "stitch_ok": stitch_pre.stitch_ok,
                    "skipped_stops": len(stitch_pre.skipped_stop_ids),
                    "search_radius_m": anchors.search_radius_m,
                    "corridor": corridor_stage,
                    "candidate_count": len(cands),
                    "candidate_generation_reason": cdebug.get("candidate_generation_reason"),
                    "error_type": cdebug.get("error_type"),
                    "http_status": cdebug.get("http_status"),
                    "fallback_attempted": cdebug.get("fallback_attempted"),
                    "fallback_success": cdebug.get("fallback_success"),
                    "valhalla_error_detail": err_detail or None,
                    "valhalla_attempt_used": cdebug.get("valhalla_attempt_used"),
                    "valhalla_location_batch": cdebug.get("valhalla_location_batch"),
                    "elapsed_ms": round((time.monotonic() - t_start) * 1000.0),
                }
                corridor_debug.append(cdebug_row)
                _log_stage(
                    trip_id, stage,
                    anchor_index=anchor_idx,
                    exit_stop_id=anchors.exit_stop_id,
                    rejoin_stop_id=anchors.rejoin_stop_id,
                    corridor=corridor_stage,
                    candidate_count=len(cands),
                    reason=cdebug.get("candidate_generation_reason"),
                    error_type=cdebug.get("error_type"),
                    http_status=cdebug.get("http_status"),
                    valhalla_error_detail=err_detail or None,
                    fallback_attempted=cdebug.get("fallback_attempted"),
                    fallback_success=cdebug.get("fallback_success"),
                    valhalla_attempt_used=cdebug.get("valhalla_attempt_used"),
                )
                if not cands:
                    continue
                baseline_d = max(0.0, anchors.rejoin_shape_dist_m - anchors.exit_shape_dist_m)
                baseline_t = baseline_d / max(5.0, 8.0)
                for rc in cands:
                    decode_stage = "candidate_decode"
                    _log_stage(trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy)
                    coords = rc.osm_result_coordinates
                    invalid_geom = len(coords) < 2
                    decoded = decode_valhalla_candidate(
                        coords, rc.time_s, match_osm_segments=True, feed_id=feed_id,
                    )
                    way_ids = sorted({int(s.osm_way_id) for s in decoded.road_segments if int(s.osm_way_id or 0) > 0})
                    gtfs_ev = db.get_gtfs_bus_way_evidence_bulk(feed_id, way_ids) if way_ids else {}
                    bus_ev = db.get_bus_edge_evidence_bulk(way_ids) if way_ids else {}
                    if way_ids:
                        best_gtfs_ev.update(gtfs_ev)
                    hit_ids = list(gtfs_ev.keys())
                    _log_stage(
                        trip_id,
                        decode_stage,
                        anchor_index=anchor_idx,
                        strategy=rc.strategy,
                        ways_total=len(way_ids),
                        ways_with_gtfs_evidence_table_hit=len(gtfs_ev),
                        decoded_osm_way_ids_sample=",".join(str(w) for w in way_ids[:12]),
                        gtfs_evidence_hit_way_ids_sample=",".join(str(w) for w in sorted(hit_ids)[:12]),
                    )
                    d_exit, d_rejoin = _backtrack_heading_deltas(
                        line=anchor_shape_line,
                        total_m=total_m,
                        exit_dist_m=anchors.exit_shape_dist_m,
                        rejoin_dist_m=anchors.rejoin_shape_dist_m,
                        route_coords=coords,
                    )
                    olap = _line_fraction_inside_blockage(coords, blockage_geojson)
                    cap = float(pol.service.max_incident_overlap_fraction)
                    path_bad = olap > cap + _INCIDENT_OVERLAP_FRAC_EPS
                    coincide_frac = _coincides_with_route_fraction(
                        coords,
                        anchor_shape_line,
                        anchors.exit_shape_dist_m,
                        anchors.rejoin_shape_dist_m,
                        total_m,
                    )
                    _log_stage(
                        trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy,
                        blockage_overlap_fraction=round(olap, 6), max_incident_overlap_fraction=cap,
                        route_coincide_fraction=round(coincide_frac, 4),
                    )
                    cv_reasons: List[str] = []
                    if VALIDATE_DETOUR_CARRIAGEWAY:
                        ok_cv, cv_reasons = validate_detour_carriageway(
                            route_coords_lonlat=coords,
                            decoded=decoded,
                            expected_exit_bearing_deg=anchors.exit_forward_bearing_deg,
                            expected_rejoin_bearing_deg=anchors.rejoin_forward_bearing_deg,
                            expected_exit_segment_ids=val_exit_seg if val_exit_seg else None,
                            expected_rejoin_segment_ids=val_rejoin_seg if val_rejoin_seg else None,
                            blocked_segment_ids=val_blocked_seg if val_blocked_seg else None,
                            hard_reject_wrong_entry_exit_segment=pol.physical_path.hard_reject_wrong_entry_exit_segment,
                        )
                        if not ok_cv and debug_detour:
                            try:
                                db.insert_detour_audit_row(
                                    detour_id=str(uuid.uuid4()),
                                    trip_id=trip_id,
                                    route_id=route_id,
                                    validation_status="rejected",
                                    validation_reason=",".join(cv_reasons[:12]),
                                    debug_json={
                                        "trip_id": trip_id,
                                        "reasons": cv_reasons,
                                        "physical_path_used": physical_path_used,
                                    },
                                )
                            except Exception:
                                pass
                    try:
                        detour_line = LineString(coords)
                    except Exception:
                        detour_line = None
                    stitch_full = compute_stitching(
                        line=anchor_shape_line,
                        total_m=total_m,
                        stop_rows=stop_rows,
                        stop_lonlat=stop_lonlat,
                        exit_dist_m=anchors.exit_shape_dist_m,
                        rejoin_dist_m=anchors.rejoin_shape_dist_m,
                        policy=pol,
                        detour_line=detour_line,
                    )
                    skipped_n = len(stitch_full.skipped_stop_ids)
                    feas = evaluate_candidate(
                        decoded=decoded,
                        projection=projection,
                        policy=pol,
                        baseline_blocked_distance_m=baseline_d,
                        baseline_blocked_time_s=baseline_t,
                        detour_distance_m=rc.distance_m,
                        detour_time_s=rc.time_s,
                        banned_way_ids=banned,
                        gtfs_way_evidence=gtfs_ev,
                        bus_edge_evidence=bus_ev,
                        turn_by_turn=rc.turn_by_turn,
                        bearing_delta_exit_deg=d_exit,
                        bearing_delta_rejoin_deg=d_rejoin,
                        path_intersects_blockage=path_bad,
                        stitch_ok=stitch_pre.stitch_ok,
                        invalid_geometry=invalid_geom,
                        carriageway_reasons=cv_reasons if cv_reasons else None,
                        route_coincide_fraction=coincide_frac,
                    )
                    _log_stage(
                        trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy,
                        bearing_delta_exit_deg=round(float(d_exit or 0.0), 2),
                        bearing_delta_rejoin_deg=round(float(d_rejoin or 0.0), 2),
                        blocked_start_m=round(float(impact.blocked.blocked_start_m if impact.blocked else 0.0), 1),
                        blocked_end_m=round(float(impact.blocked.blocked_end_m if impact.blocked else 0.0), 1),
                        exit_shape_dist_m=round(float(anchors.exit_shape_dist_m), 1),
                        rejoin_shape_dist_m=round(float(anchors.rejoin_shape_dist_m), 1),
                        route_coincide_fraction=round(coincide_frac, 4),
                    )
                    if skipped_n > 0:
                        weight = _ridership_weight_for_stop(
                            anchors.exit_stop_id or "", stop_lonlat, gtfs_ev
                        )
                        feas.service_penalty_s += skipped_n * float(pol.service.skipped_stop_penalty_s) * weight
                        feas.notes.append(f"skipped_stops={skipped_n}")
                    extra = {
                        "corridor": corridor_stage,
                        "anchor_index": float(anchor_idx),
                        "skipped_stops": float(skipped_n),
                        "exit_shape_dist_m": float(anchors.exit_shape_dist_m),
                        "rejoin_shape_dist_m": float(anchors.rejoin_shape_dist_m),
                        "search_radius_m": float(anchors.search_radius_m),
                    }
                    all_rank_inputs.append((rc.strategy, rc.time_s, rc.distance_m, feas, extra, decoded))
                    all_metas.append({"anchors": anchors, "stitch_full": stitch_full, "gtfs_ev": gtfs_ev})
                    eval_param_records.append({
                        "projection": projection,
                        "baseline_blocked_distance_m": baseline_d,
                        "baseline_blocked_time_s": baseline_t,
                        "detour_distance_m": rc.distance_m,
                        "detour_time_s": rc.time_s,
                        "banned_way_ids": banned,
                        "gtfs_way_evidence": gtfs_ev,
                        "bus_edge_evidence": bus_ev,
                        "turn_by_turn": rc.turn_by_turn,
                        "bearing_delta_exit_deg": d_exit,
                        "bearing_delta_rejoin_deg": d_rejoin,
                        "path_intersects_blockage": path_bad,
                        "stitch_ok": stitch_pre.stitch_ok,
                        "invalid_geometry": invalid_geom,
                        "carriageway_reasons": cv_reasons if cv_reasons else None,
                        "route_coincide_fraction": coincide_frac,
                    })
                rank_stage = "ranking"
                _log_stage(
                    trip_id, rank_stage,
                    anchor_index=anchor_idx, corridor=corridor_stage,
                    accumulated=len(all_rank_inputs),
                )
        finally:
            executor.shutdown(wait=False)

        def _meta_for_ranked(
            r: RankedCandidate,
            inputs: List[Tuple[str, float, float, Any, Dict[str, Any], Any]],
            metas: List[Dict[str, Any]],
        ) -> Dict[str, Any]:
            for i, tup in enumerate(inputs):
                strat, ts, dm = tup[0], tup[1], tup[2]
                if (
                    strat == r.strategy
                    and abs(float(ts) - float(r.travel_time_s)) < 2.0
                    and abs(float(dm) - float(r.distance_m)) < 15.0
                ):
                    return metas[i] if i < len(metas) else {}
            return {}

        ranked_all = rank_candidates(all_rank_inputs, pol)
        discarded: List[RankedCandidate] = [x for x in ranked_all if x.total_score >= float("inf")]
        winners_raw = [x for x in ranked_all if x.total_score < float("inf")]

        valhalla_produced_any = any(r.get("candidate_count", 0) > 0 for r in corridor_debug)
        all_feasibility_rejected = bool(all_rank_inputs) and not winners_raw

        # Pass 2: when strict GTFS / coincide checks rejected everything, re-evaluate
        # the same decoded candidates with a relaxed policy and surface the least-bad
        # one as EMERGENCY_FALLBACK.  This ensures the engine never returns no_safe_detour
        # while Valhalla actually produced candidates.
        used_strict_fallback = False
        pass1_reject_reasons: List[str] = []
        if not winners_raw and valhalla_produced_any and all_feasibility_rejected:
            seen_reasons: set[str] = set()
            for c in discarded:
                for r_reason in (c.rejection_reasons or []):
                    if r_reason not in seen_reasons:
                        seen_reasons.add(r_reason)
                        pass1_reject_reasons.append(r_reason)
            _log_stage(
                trip_id, "strict_pass_rejected_all",
                rejected_count=len(discarded),
                top_reasons=",".join(pass1_reject_reasons[:5]),
                running_pass2=True,
            )
            relaxed_pol = _make_relaxed_policy(pol)
            pass2_rank_inputs: List[Tuple[str, float, float, Any, Dict[str, Any], Any]] = []
            pass2_metas: List[Dict[str, Any]] = []
            for (strat, t_s, d_m, _old_feas, extra, decoded), params, meta in zip(
                all_rank_inputs, eval_param_records, all_metas
            ):
                feas2 = evaluate_candidate(decoded=decoded, policy=relaxed_pol, **params)
                feas2.warnings.append("strict_gtfs_pass_failed_fallback_relaxed")
                pass2_rank_inputs.append((strat, t_s, d_m, feas2, extra, decoded))
                pass2_metas.append(meta)
            ranked_p2 = rank_candidates(pass2_rank_inputs, relaxed_pol)
            p2_winners = [x for x in ranked_p2 if x.total_score < float("inf")]
            if p2_winners:
                winners_raw = p2_winners
                discarded = [x for x in ranked_p2 if x.total_score >= float("inf")]
                all_metas = pass2_metas
                # Remap all_rank_inputs so _meta_for_ranked works correctly.
                all_rank_inputs = pass2_rank_inputs
                used_strict_fallback = True

        if not winners_raw:
            fb = _build_emergency_fallback(
                anchors=anchors,
                blockage_geojson=blockage_geojson,
                projection=projection,
                pol=pol,
                feed_id=feed_id,
                anchor_shape_line=anchor_shape_line,
                total_m=total_m,
                stop_rows=stop_rows,
                stop_lonlat=stop_lonlat,
            )
            winners_enriched: List[RankedCandidate] = [fb]
            best_corridor = "emergency"
            best_anchors = anchors
        else:
            winners_enriched = []
            for r in winners_raw:
                meta = _meta_for_ranked(r, all_rank_inputs, all_metas)
                stitch_f = meta.get("stitch_full")
                gtfs_ev_local = meta.get("gtfs_ev") or {}
                gtfs_frac = _gtfs_evidence_distance_fraction(r.decoded, gtfs_ev_local) if r.decoded else 0.0
                feas = r.feasibility or FeasibilityResult(accepted=True, hard_reject_reasons=[])
                tier, conf, warns, passed, review = classify_tier(
                    feasibility=feas,
                    decoded=r.decoded,
                    stitch=stitch_f,
                    is_emergency_fallback=False,
                    gtfs_evidence_way_fraction=gtfs_frac,
                )
                r.tier = tier
                r.confidence_score = conf
                r.warnings = warns
                r.hard_constraints_passed = passed
                r.review_required = review
                winners_enriched.append(r)
            tier_order = {"AUTO_OK": 0, "REVIEW_RECOMMENDED": 1, "LOW_CONFIDENCE": 2, "EMERGENCY_FALLBACK": 3}
            winners_enriched.sort(key=lambda x: (tier_order.get(str(x.tier), 9), x.total_score, x.distance_m))
            best_corridor = str(winners_enriched[0].score_breakdown.get("corridor") or "")
            best_anchors = (
                _meta_for_ranked(winners_enriched[0], all_rank_inputs, all_metas).get("anchors") or anchors
            )

        top3 = winners_enriched[:3]
        for ri, c in enumerate(top3, start=1):
            c.candidate_rank = ri
        best_selected = top3[0]
        best_ranked = top3
        meta0 = _meta_for_ranked(best_selected, all_rank_inputs, all_metas)
        best_stitch = meta0.get("stitch_full")
        if best_stitch is None:
            dc = coords_from_geojson_linestring(
                best_selected.decoded.geometry_geojson if best_selected.decoded else None
            )
            dl = LineString(dc) if len(dc) >= 2 else None
            best_stitch = compute_stitching(
                line=anchor_shape_line,
                total_m=total_m,
                stop_rows=stop_rows,
                stop_lonlat=stop_lonlat,
                exit_dist_m=best_anchors.exit_shape_dist_m,
                rejoin_dist_m=best_anchors.rejoin_shape_dist_m,
                policy=pol,
                detour_line=dl,
            )
        best_row: Dict[str, Any] = {"anchor_index": 0, "corridor": best_corridor}
        via_candidate = _try_via_stop_route(
            best_selected, best_anchors, best_stitch, blockage_geojson, stop_lonlat, pol, trip_id
        )
        if via_candidate is not None and via_candidate.total_score < best_selected.total_score:
            best_selected = via_candidate
            best_ranked = [via_candidate] + [x for x in best_ranked if x is not via_candidate][:2]
            best_ranked[0].candidate_rank = 1
        # Pass-2 winners are always surfaced as EMERGENCY_FALLBACK regardless of their
        # computed tier, since strict policy was relaxed to find them.
        if used_strict_fallback:
            best_selected.tier = "EMERGENCY_FALLBACK"
            for c in best_ranked:
                c.tier = "EMERGENCY_FALLBACK"
        out_status = _tier_status(str(best_selected.tier))
        _log_stage(
            trip_id, "select_best",
            strategy=best_selected.strategy,
            total_score=round(best_selected.total_score, 3),
            corridor=best_corridor,
            tier=best_selected.tier,
            anchor_index=best_row.get("anchor_index") if isinstance(best_row, dict) else None,
            strict_fallback=used_strict_fallback,
        )
        dbg_ok: Dict[str, Any] = {
            "candidate_generation": corridor_debug,
            "physical_path_used": physical_path_used,
            "physical_fallback_reason": physical_fallback_reason,
            "matched_trip_metadata": {
                "coverage_ratio": matched_physical.coverage_ratio,
                "ambiguous_stop_pairs": matched_physical.ambiguous_stop_pairs,
                "weak_stop_pairs": matched_physical.weak_stop_pairs,
                "fallback_reason": matched_physical.fallback_reason,
            },
            "discarded_count": len(discarded),
        }
        if debug_detour:
            gtfs_coords = [(float(x), float(y)) for x, y in line.coords]
            matched_coords = None
            if matched_physical.line is not None and not matched_physical.line.is_empty:
                matched_coords = [(float(x), float(y)) for x, y in matched_physical.line.coords]
            blocked_matched: Optional[List[tuple[float, float]]] = None
            if physical_line is not None and impact.blocked is not None:
                try:
                    slm = float(impact.blocked.shape_length_m or total_m)
                    sub = affected_shape_subline(
                        physical_line,
                        impact.blocked.blocked_start_m,
                        impact.blocked.blocked_end_m,
                        slm,
                    )
                    blocked_matched = [(float(x), float(y)) for x, y in sub.coords]
                except Exception:
                    blocked_matched = None
            dec_coords = coords_from_geojson_linestring(
                best_selected.decoded.geometry_geojson if best_selected.decoded else None
            )
            dbg_ok["geojson"] = build_detour_debug_feature_collection(
                gtfs_shape_coords_lonlat=gtfs_coords,
                matched_physical_coords_lonlat=matched_coords,
                blocked_span_on_matched_coords_lonlat=blocked_matched,
                raw_valhalla_coords_lonlat=dec_coords,
                decoded_detour_coords_lonlat=dec_coords,
                exit_lon=best_anchors.exit_lon,
                exit_lat=best_anchors.exit_lat,
                rejoin_lon=best_anchors.rejoin_lon,
                rejoin_lat=best_anchors.rejoin_lat,
                blockage_geojson=blockage_geojson,
                extra={"trip_id": trip_id, "debug_detour": True},
            )
        if DETOUR_V2_TIMING_LOG:
            log(
                "detours/v2/compute",
                f"trip_id={trip_id} timing_ms={round((time.monotonic()-t_start)*1000.0)} status={out_status}",
            )
        return DetourComputeOutput(
            status=cast(DetourComputeStatus, out_status),
            trip_id=trip_id,
            route_id=route_id,
            anchors=best_anchors,
            corridor_stage=best_corridor,
            candidates=best_ranked,
            selected=best_selected,
            policy_version=pol.version,
            stitching=best_stitch,
            attempts=corridor_debug,
            debug=dbg_ok,
            discarded=discarded,
        )
    except Exception as e:
        log(
            "detours/v2/compute",
            f"trip_id={trip_id} stage={stage} error_type={type(e).__name__} error={e!s}",
        )
        log("detours/v2/compute", f"trip_id={trip_id} stage={stage} traceback={traceback.format_exc().strip()}")
        raise
