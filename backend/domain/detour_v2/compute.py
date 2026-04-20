"""Orchestrate detour v2 pipeline for one trip."""

from __future__ import annotations

import hashlib
import json
import math
import time
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString, shape

from backend.infra import db_access as db
from backend.infra.config import (
    DETOUR_V2_TIMING_LOG,
    USE_MATCHED_PHYSICAL_GEOMETRY,
    VALIDATE_DETOUR_CARRIAGEWAY,
)
from backend.infra.logging_utils import log
from backend.adapters.osm_detour import route_waypoints_avoiding_polygon, valhalla_locate

from backend.domain.detour_physical.debug_geojson import (
    build_detour_debug_feature_collection,
    coords_from_geojson_linestring,
)
from backend.domain.detour_physical.matched_trip_geometry import MatchedTripPhysical
from backend.infra import pattern_edge_match_repo as pattern_edge_match_repo

from .anchor_selector import enumerate_anchor_candidates
from .bus_feasibility_evaluator import evaluate_candidate
from .detour_validator import validate_detour_carriageway
from .candidate_decoder import decode_valhalla_candidate
from .corridor_builder import affected_shape_subline, incident_exclusion_polygon_for_stage, corridor_stages_order
from .detour_ranker import rank_candidates
from .incident_projector import edge_ban_way_ids, project_incident_polygon
from .models import AnchorPair, DetourComputeOutput, RankedCandidate
from .policy import DetourPolicyConfig, get_default_policy
from .road_candidate_generator import generate_candidates_with_debug
from .service_stitching import compute_stitching
from .trip_impact_analyzer import analyze_trip_impact


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
    if len(coords) < 2:
        return None
    seq = coords if from_start else list(reversed(coords))
    lon0, lat0 = seq[0]
    acc_m = 0.0
    for i in range(1, len(seq)):
        lon1, lat1 = seq[i]
        seg_m = float(LineString([(lon0, lat0), (lon1, lat1)]).length * 111_320.0)
        acc_m += seg_m
        if acc_m >= min_probe_m or i == len(seq) - 1:
            return _bearing_deg(lon0, lat0, lon1, lat1)
        lon0, lat0 = lon1, lat1
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


def _validate_anchors_via_locate(
    anchors_list: List[AnchorPair],
    policy: DetourPolicyConfig,
) -> List[AnchorPair]:
    """Drop anchor pairs whose exit or rejoin is unreachable via Valhalla /locate (A3)."""
    min_reach = int(getattr(policy.anchor, "min_locate_reachability_nodes", 50))
    if min_reach <= 0:
        return anchors_list
    # Collect unique points.
    points: List[Tuple[float, float]] = []
    seen: set[tuple[float, float]] = set()
    for ap in anchors_list:
        for pt in [(ap.exit_lon, ap.exit_lat), (ap.rejoin_lon, ap.rejoin_lat)]:
            if pt not in seen:
                seen.add(pt)
                points.append(pt)
    try:
        loc_results = valhalla_locate(points, costing="bus")
    except Exception:
        loc_results = None
    if not loc_results or not isinstance(loc_results, list):
        return anchors_list
    # Build reachability map by point.
    reach_map: Dict[Tuple[float, float], int] = {}
    for i, (lon, lat) in enumerate(points):
        if i >= len(loc_results):
            break
        loc = loc_results[i]
        if not isinstance(loc, dict):
            continue
        edges = loc.get("edges") or []
        best = max((int(e.get("minimum_reachability") or 0) for e in edges if isinstance(e, dict)), default=0)
        reach_map[(round(lon, 7), round(lat, 7))] = best

    validated: List[AnchorPair] = []
    for ap in anchors_list:
        exit_r = reach_map.get((round(ap.exit_lon, 7), round(ap.exit_lat, 7)), min_reach)
        rejoin_r = reach_map.get((round(ap.rejoin_lon, 7), round(ap.rejoin_lat, 7)), min_reach)
        if exit_r >= min_reach and rejoin_r >= min_reach:
            validated.append(ap)
    return validated if validated else anchors_list


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

        stage = "anchor_selection"
        anchor_candidates = enumerate_anchor_candidates(
            line=anchor_shape_line,
            blocked=impact.blocked,
            stop_rows=stop_rows,
            stop_lonlat=stop_lonlat,
            policy=pol,
            max_pairs=pol.anchor.candidate_pairs_k,
        )
        # A3: validate anchor reachability via /locate (drops unreachable anchor pairs).
        anchor_candidates = _validate_anchors_via_locate(anchor_candidates, pol)

        total_m = float(impact.blocked.shape_length_m or 0.0) if impact.blocked else 0.0
        if total_m <= 0:
            from .trip_impact_analyzer import _line_length_m

            total_m = _line_length_m(anchor_shape_line)
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
            exit_stop_id=anchors.exit_stop_id,
            rejoin_stop_id=anchors.rejoin_stop_id,
        )

        stage = "incident_projection"
        feed_id = db.get_active_feed_id()
        projection = project_incident_polygon(
            blockage_geojson=blockage_geojson,
            feed_id=feed_id,
            db_osm_available=True,
        )
        banned = edge_ban_way_ids(projection)
        _log_stage(trip_id, stage, edge_bans=len(banned), feed_id=feed_id)

        corridor_debug: list[Dict[str, Any]] = []
        best: Optional[tuple[float, str, Any, Any, Any]] = None
        best_row: Optional[Dict[str, Any]] = None
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

        # Build all (anchor_idx, corridor_stage) work items.
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
                if elapsed_ms > deadline_ms and best is not None:
                    _log_stage(trip_id, "deadline", elapsed_ms=round(elapsed_ms), outcome="returning_best_so_far")
                    break

                try:
                    anchor_idx, anchors, corridor_stage, cands, cdebug = future.result()
                except Exception as fe:
                    log("detours/v2/compute", f"trip_id={trip_id} stage=candidate_generation future_error={fe!s}")
                    continue

                # Compute stitching for this anchor pair.
                stitch = compute_stitching(
                    line=anchor_shape_line,
                    total_m=total_m,
                    stop_rows=stop_rows,
                    stop_lonlat=stop_lonlat,
                    exit_dist_m=anchors.exit_shape_dist_m,
                    rejoin_dist_m=anchors.rejoin_shape_dist_m,
                    policy=pol,
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
                    "stitch_ok": stitch.stitch_ok,
                    "skipped_stops": len(stitch.skipped_stop_ids),
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
                rank_inputs = []
                for rc in cands:
                    decode_stage = "candidate_decode"
                    _log_stage(trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy)
                    decoded = decode_valhalla_candidate(
                        rc.osm_result_coordinates, rc.time_s, match_osm_segments=True, feed_id=feed_id,
                    )
                    way_ids = sorted({int(s.osm_way_id) for s in decoded.road_segments if int(s.osm_way_id or 0) > 0})
                    gtfs_ev = db.get_gtfs_bus_way_evidence_bulk(feed_id, way_ids) if way_ids else {}
                    _log_stage(
                        trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy,
                        ways_total=len(way_ids), ways_with_gtfs_evidence=len(gtfs_ev),
                    )
                    d_exit, d_rejoin = _backtrack_heading_deltas(
                        line=anchor_shape_line,
                        total_m=total_m,
                        exit_dist_m=anchors.exit_shape_dist_m,
                        rejoin_dist_m=anchors.rejoin_shape_dist_m,
                        route_coords=rc.osm_result_coordinates,
                    )
                    feas = evaluate_candidate(
                        decoded=decoded, projection=projection, policy=pol,
                        baseline_blocked_distance_m=baseline_d, baseline_blocked_time_s=baseline_t,
                        detour_distance_m=rc.distance_m, detour_time_s=rc.time_s,
                        banned_way_ids=banned, gtfs_way_evidence=gtfs_ev,
                        turn_by_turn=rc.turn_by_turn,
                        bearing_delta_exit_deg=d_exit, bearing_delta_rejoin_deg=d_rejoin,
                    )
                    _log_stage(
                        trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy,
                        bearing_delta_exit_deg=round(float(d_exit or 0.0), 2),
                        bearing_delta_rejoin_deg=round(float(d_rejoin or 0.0), 2),
                    )
                    olap = _line_fraction_inside_blockage(rc.osm_result_coordinates, blockage_geojson)
                    cap = float(pol.service.max_incident_overlap_fraction)
                    _log_stage(
                        trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy,
                        blockage_overlap_fraction=round(olap, 6), max_incident_overlap_fraction=cap,
                    )
                    if olap > cap + _INCIDENT_OVERLAP_FRAC_EPS:
                        feas.hard_reject_reasons.append("detour_path_intersects_incident")
                        feas.accepted = False
                        _log_stage(
                            trip_id, decode_stage, anchor_index=anchor_idx, strategy=rc.strategy,
                            status="overlap_reject",
                            blockage_overlap_fraction=round(olap, 6), max_incident_overlap_fraction=cap,
                        )
                    if VALIDATE_DETOUR_CARRIAGEWAY:
                        ok_cv, cv_reasons = validate_detour_carriageway(
                            route_coords_lonlat=rc.osm_result_coordinates,
                            decoded=decoded,
                            expected_exit_bearing_deg=anchors.exit_forward_bearing_deg,
                            expected_rejoin_bearing_deg=anchors.rejoin_forward_bearing_deg,
                            expected_exit_segment_ids=val_exit_seg if val_exit_seg else None,
                            expected_rejoin_segment_ids=val_rejoin_seg if val_rejoin_seg else None,
                            blocked_segment_ids=val_blocked_seg if val_blocked_seg else None,
                            hard_reject_wrong_entry_exit_segment=pol.physical_path.hard_reject_wrong_entry_exit_segment,
                        )
                        if not ok_cv:
                            for cr in cv_reasons:
                                feas.hard_reject_reasons.append(f"carriageway:{cr}")
                            feas.accepted = False
                            if debug_detour:
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
                    if not stitch.stitch_ok:
                        feas.hard_reject_reasons.append("cannot-stitch-service")
                        feas.accepted = False
                    skipped_n = len(stitch.skipped_stop_ids)
                    if skipped_n > 0:
                        # D2: ridership-weighted skipped-stop penalty.
                        weight = _ridership_weight_for_stop(
                            anchors.exit_stop_id or "", stop_lonlat, gtfs_ev
                        )
                        feas.service_penalty_s += skipped_n * float(pol.service.skipped_stop_penalty_s) * weight
                        feas.notes.append(f"skipped_stops={skipped_n}")
                    rank_inputs.append(
                        (
                            rc.strategy,
                            rc.time_s,
                            rc.distance_m,
                            feas,
                            {
                                "corridor": corridor_stage,
                                "anchor_index": float(anchor_idx),
                                "skipped_stops": float(skipped_n),
                            },
                            decoded,
                        )
                    )

                rank_stage = "ranking"
                ranked = rank_candidates(rank_inputs, pol)
                winners = [x for x in ranked if x.total_score < float("inf")]
                if (
                    not winners and rank_inputs
                    and all("detour_path_intersects_incident" in f.hard_reject_reasons for _, _, _, f, _, _ in rank_inputs)
                ):
                    corridor_debug[-1]["corridor_outcome"] = "all_rejected_incident_overlap"
                elif (
                    not winners and rank_inputs
                    and all(
                        ("anchor_backtrack_heading" in f.hard_reject_reasons or "u_turn_maneuver" in f.hard_reject_reasons)
                        for _, _, _, f, _, _ in rank_inputs
                    )
                ):
                    corridor_debug[-1]["corridor_outcome"] = "all_rejected_backtrack_or_uturn"
                log_kwargs: Dict[str, Any] = dict(
                    anchor_index=anchor_idx, corridor=corridor_stage,
                    ranked_count=len(ranked), winner_count=len(winners),
                )
                if not winners and rank_inputs:
                    fr0 = rank_inputs[0][3].hard_reject_reasons
                    log_kwargs["sample_hard_rejects"] = ",".join(fr0[:12]) if fr0 else "(none)"
                _log_stage(trip_id, rank_stage, **log_kwargs)
                if winners:
                    top = winners[0]
                    if best is None or top.total_score < best[0]:
                        best = (top.total_score, corridor_stage, anchors, ranked, top)
                        best_row = cdebug_row
                        best_gtfs_ev = gtfs_ev
                    # B2: early accept — stop processing remaining futures if score is good enough.
                    if top.total_score <= early_score and corridor_stage == "narrow":
                        _log_stage(trip_id, "early_accept", score=round(top.total_score, 2), corridor=corridor_stage)
                        break
        finally:
            executor.shutdown(wait=False)

        # C3: Anchor rescue — if no winners, widen search window and retry once.
        if best is None and len(anchor_candidates) > 0:
            rescue_k = int(getattr(pol.anchor, "rescue_stops_per_side", 8))
            if rescue_k > pol.anchor.candidate_stops_per_side:
                _log_stage(trip_id, "anchor_rescue", attempt="rescue", rescue_k=rescue_k)
                import dataclasses as _dc
                rescue_pol = _dc.replace(pol, anchor=_dc.replace(pol.anchor, candidate_stops_per_side=rescue_k))
                rescue_candidates = enumerate_anchor_candidates(
                    line=anchor_shape_line,
                    blocked=impact.blocked,
                    stop_rows=stop_rows,
                    stop_lonlat=stop_lonlat,
                    policy=rescue_pol,
                    max_pairs=min(rescue_pol.anchor.candidate_pairs_k, 3),
                )
                rescue_candidates = _validate_anchors_via_locate(rescue_candidates, rescue_pol)
                for ap in rescue_candidates:
                    ap.exit_forward_bearing_deg = _line_forward_bearing(
                        anchor_shape_line, ap.exit_shape_dist_m, total_m
                    )
                    ap.rejoin_forward_bearing_deg = _line_forward_bearing(
                        anchor_shape_line, ap.rejoin_shape_dist_m, total_m
                    )
                    ap.anchor_geometry_source = (
                        "matched_physical" if physical_path_used else "gtfs_shape"
                    )
                for rescue_anc in rescue_candidates:
                    for corridor_stage in corridor_stages_order():
                        excl = incident_exclusion_polygon_for_stage(
                            blockage_geojson,
                            corridor_stage,
                            pol,
                            line=anchor_shape_line,
                            anchors=rescue_anc,
                            shape_length_m=total_m,
                        )
                        cands, cdebug = generate_candidates_with_debug(
                            rescue_anc.exit_lon,
                            rescue_anc.exit_lat,
                            rescue_anc.rejoin_lon,
                            rescue_anc.rejoin_lat,
                            excl,
                            alternate_count=2,
                            exit_heading_deg=rescue_anc.exit_forward_bearing_deg,
                            rejoin_heading_deg=rescue_anc.rejoin_forward_bearing_deg,
                        )
                        corridor_debug.append({
                            "anchor_index": "rescue",
                            "corridor": corridor_stage,
                            "candidate_count": len(cands),
                            "candidate_generation_reason": cdebug.get("candidate_generation_reason"),
                            "elapsed_ms": round((time.monotonic() - t_start) * 1000.0),
                        })
                        if not cands:
                            continue
                        stitch = compute_stitching(
                            line=anchor_shape_line,
                            total_m=total_m,
                            stop_rows=stop_rows,
                            stop_lonlat=stop_lonlat,
                            exit_dist_m=rescue_anc.exit_shape_dist_m,
                            rejoin_dist_m=rescue_anc.rejoin_shape_dist_m,
                            policy=pol,
                        )
                        baseline_d = max(0.0, rescue_anc.rejoin_shape_dist_m - rescue_anc.exit_shape_dist_m)
                        baseline_t = baseline_d / max(5.0, 8.0)
                        rank_inputs = []
                        for rc in cands:
                            decoded = decode_valhalla_candidate(
                                rc.osm_result_coordinates, rc.time_s, match_osm_segments=True, feed_id=feed_id,
                            )
                            way_ids = sorted({int(s.osm_way_id) for s in decoded.road_segments if int(s.osm_way_id or 0) > 0})
                            gtfs_ev = db.get_gtfs_bus_way_evidence_bulk(feed_id, way_ids) if way_ids else {}
                            d_exit, d_rejoin = _backtrack_heading_deltas(
                                line=anchor_shape_line,
                                total_m=total_m,
                                exit_dist_m=rescue_anc.exit_shape_dist_m,
                                rejoin_dist_m=rescue_anc.rejoin_shape_dist_m,
                                route_coords=rc.osm_result_coordinates,
                            )
                            feas = evaluate_candidate(
                                decoded=decoded, projection=projection, policy=pol,
                                baseline_blocked_distance_m=baseline_d, baseline_blocked_time_s=baseline_t,
                                detour_distance_m=rc.distance_m, detour_time_s=rc.time_s,
                                banned_way_ids=banned, gtfs_way_evidence=gtfs_ev,
                                turn_by_turn=rc.turn_by_turn,
                                bearing_delta_exit_deg=d_exit, bearing_delta_rejoin_deg=d_rejoin,
                            )
                            olap = _line_fraction_inside_blockage(rc.osm_result_coordinates, blockage_geojson)
                            cap = float(pol.service.max_incident_overlap_fraction)
                            if olap > cap + _INCIDENT_OVERLAP_FRAC_EPS:
                                feas.hard_reject_reasons.append("detour_path_intersects_incident")
                                feas.accepted = False
                            if VALIDATE_DETOUR_CARRIAGEWAY:
                                ok_cv, cv_reasons = validate_detour_carriageway(
                                    route_coords_lonlat=rc.osm_result_coordinates,
                                    decoded=decoded,
                                    expected_exit_bearing_deg=rescue_anc.exit_forward_bearing_deg,
                                    expected_rejoin_bearing_deg=rescue_anc.rejoin_forward_bearing_deg,
                                    expected_exit_segment_ids=val_exit_seg if val_exit_seg else None,
                                    expected_rejoin_segment_ids=val_rejoin_seg if val_rejoin_seg else None,
                                    blocked_segment_ids=val_blocked_seg if val_blocked_seg else None,
                                    hard_reject_wrong_entry_exit_segment=pol.physical_path.hard_reject_wrong_entry_exit_segment,
                                )
                                if not ok_cv:
                                    for cr in cv_reasons:
                                        feas.hard_reject_reasons.append(f"carriageway:{cr}")
                                    feas.accepted = False
                            if not stitch.stitch_ok:
                                feas.hard_reject_reasons.append("cannot-stitch-service")
                                feas.accepted = False
                            skipped_n = len(stitch.skipped_stop_ids)
                            if skipped_n > 0:
                                weight = _ridership_weight_for_stop("", stop_lonlat, gtfs_ev)
                                feas.service_penalty_s += skipped_n * float(pol.service.skipped_stop_penalty_s) * weight
                            rank_inputs.append((rc.strategy, rc.time_s, rc.distance_m, feas, {"corridor": corridor_stage, "anchor_index": "rescue", "skipped_stops": float(skipped_n)}, decoded))
                        ranked = rank_candidates(rank_inputs, pol)
                        winners = [x for x in ranked if x.total_score < float("inf")]
                        if winners:
                            top = winners[0]
                            if best is None or top.total_score < best[0]:
                                best = (top.total_score, corridor_stage, rescue_anc, ranked, top)
                                best_row = corridor_debug[-1]
                                best_gtfs_ev = gtfs_ev
                            break
                    if best is not None:
                        break

        if best is not None:
            _, best_corridor, best_anchors, best_ranked, best_selected = best
            best_stitch = compute_stitching(
                line=anchor_shape_line,
                total_m=total_m,
                stop_rows=stop_rows,
                stop_lonlat=stop_lonlat,
                exit_dist_m=best_anchors.exit_shape_dist_m,
                rejoin_dist_m=best_anchors.rejoin_shape_dist_m,
                policy=pol,
            )
            # D1: try inserting skipped stops as via points.
            via_candidate = _try_via_stop_route(
                best_selected, best_anchors, best_stitch, blockage_geojson, stop_lonlat, pol, trip_id
            )
            if via_candidate is not None and via_candidate.total_score < best_selected.total_score:
                best_selected = via_candidate
                best_ranked = [via_candidate] + best_ranked
            _log_stage(
                trip_id, "select_best",
                strategy=best_selected.strategy,
                total_score=round(best_selected.total_score, 3),
                corridor=best_corridor,
                anchor_index=best_row.get("anchor_index") if isinstance(best_row, dict) else None,
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
                    f"trip_id={trip_id} timing_ms={round((time.monotonic()-t_start)*1000.0)} status=ok",
                )
            return DetourComputeOutput(
                status="ok",
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
            )

        # C4: check if deadline expired with no result.
        elapsed_ms = (time.monotonic() - t_start) * 1000.0
        final_debug = corridor_debug[-1] if corridor_debug else {}
        aggregate_reason = _no_safe_detour_aggregate_reason(corridor_debug)
        if elapsed_ms > deadline_ms:
            aggregate_reason = "deadline_exceeded"
        gtfs_reject_count = 0
        for row in corridor_debug:
            if row.get("candidate_count") == 0 and row.get("candidate_generation_reason"):
                gtfs_reject_count += 1
        _log_stage(
            trip_id, "result",
            status="no_safe_detour",
            reason=aggregate_reason,
            error_type=final_debug.get("error_type"),
            http_status=final_debug.get("http_status"),
            ways_rejected_no_gtfs=gtfs_reject_count,
            elapsed_ms=round(elapsed_ms),
        )
        dbg_ns: Dict[str, Any] = {
            "candidate_generation_reason": aggregate_reason,
            "valhalla_http_status": final_debug.get("http_status"),
            "ways_rejected_no_gtfs": gtfs_reject_count,
            "candidate_generation": corridor_debug,
            "physical_path_used": physical_path_used,
            "physical_fallback_reason": physical_fallback_reason,
            "matched_trip_metadata": {
                "coverage_ratio": matched_physical.coverage_ratio,
                "ambiguous_stop_pairs": matched_physical.ambiguous_stop_pairs,
                "weak_stop_pairs": matched_physical.weak_stop_pairs,
                "fallback_reason": matched_physical.fallback_reason,
            },
        }
        if debug_detour:
            gtfs_coords = [(float(x), float(y)) for x, y in line.coords]
            matched_coords = None
            if matched_physical.line is not None and not matched_physical.line.is_empty:
                matched_coords = [(float(x), float(y)) for x, y in matched_physical.line.coords]
            dbg_ns["geojson"] = build_detour_debug_feature_collection(
                gtfs_shape_coords_lonlat=gtfs_coords,
                matched_physical_coords_lonlat=matched_coords,
                exit_lon=anchors.exit_lon,
                exit_lat=anchors.exit_lat,
                rejoin_lon=anchors.rejoin_lon,
                rejoin_lat=anchors.rejoin_lat,
                blockage_geojson=blockage_geojson,
                extra={"trip_id": trip_id, "reason": aggregate_reason},
            )
        return DetourComputeOutput(
            status="no_safe_detour",
            trip_id=trip_id,
            route_id=route_id,
            anchors=anchors,
            candidates=[],
            policy_version=pol.version,
            stitching=None,
            attempts=corridor_debug,
            debug=dbg_ns,
        )
    except Exception as e:
        log(
            "detours/v2/compute",
            f"trip_id={trip_id} stage={stage} error_type={type(e).__name__} error={e!s}",
        )
        log("detours/v2/compute", f"trip_id={trip_id} stage={stage} traceback={traceback.format_exc().strip()}")
        raise
