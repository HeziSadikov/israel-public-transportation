"""Detour v3 — bus-corridor A* around blockage using ``pattern_osm_segments``."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from shapely.geometry import LineString

from backend.domain.detour_v2.models import (
    AnchorPair,
    DecodedCandidate,
    DetourComputeOutput,
    FeasibilityResult,
    RankedCandidate,
    RoadSegmentRef,
)
from backend.domain.detour_v2.policy import DetourPolicyConfig, get_default_policy
from backend.domain.detour_v2.serialize import format_detour_ai_log_line
from backend.domain.detour_v2.trip_impact_analyzer import analyze_trip_impact
from backend.infra import db_access as db
from backend.infra.config import (
    DETOUR_V3_COST_MODE,
    DETOUR_V3_ENABLED,
    DETOUR_V3_IMPORT_RUN_ID,
)
from backend.infra.logging_utils import log
from backend.routing.anchors import (
    SegmentAnchorPair,
    anchor_pairs_for_pattern_blocks,
    resolve_anchor_nodes,
)
from backend.routing.astar import astar_shortest_path
from backend.routing.blockers import polygon_geojson_feature_to_wkt, project_polygon_to_bans
from backend.routing.costs import (
    BusCorridorCostMode,
    DEFAULT_ROUTING_COST_PROFILE,
    bus_corridor_segment_enter_penalty_m,
)
from backend.routing.road_graph_loader import RoadGraph, load_road_graph

from .debug_payload import build_debug_payload
from .scoring import score_v3_path

POLICY_VERSION_V3 = "detour_v3_m5"


def _connect():
    import psycopg2
    from psycopg2.extras import DictCursor

    return psycopg2.connect(db.DB_URL, cursor_factory=DictCursor)


def _load_trip_context(trip_id: str, conn) -> tuple[Optional[str], Optional[str], Optional[LineString]]:
    """Return (route_id, shape_id, shape_line)."""
    row = db.get_trip_route_shape(trip_id, conn=conn)
    if not row:
        return None, None, None
    rid = str(row.get("route_id") or "")
    sid = row.get("shape_id")
    sid_s = str(sid) if sid is not None else None
    line = None
    if sid_s:
        try:
            line = db.get_shape_line(sid_s, conn=conn)
        except Exception:
            line = None
    return rid, sid_s, line if isinstance(line, LineString) else None


def _decoded_from_path(graph: RoadGraph, seg_ids: List[int]) -> DecodedCandidate:
    refs: List[RoadSegmentRef] = []
    for i, sid in enumerate(seg_ids):
        s = graph.segments[sid]
        refs.append(
            RoadSegmentRef(
                segment_id=sid,
                osm_way_id=0,
                from_node_id=s.from_node_id,
                to_node_id=s.to_node_id,
                sequence_index=i,
                length_m=float(s.length_m),
                travel_time_s=float(s.length_m) / 12.0,
                highway=s.highway,
                access=None,
                bus=None,
                psv=None,
                service=None,
                synthetic=False,
            )
        )
    return DecodedCandidate(road_segments=refs, turns=[], geometry_geojson={"type": "LineString", "coordinates": []})


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
    conn=None,
) -> DetourComputeOutput:
    """M5 orchestrator — same kw-only surface as detour_v2.compute_detour_for_trip for HTTP wiring."""
    del _valhalla_cache, use_matched_physical, service_date, incident_id
    pol = policy or get_default_policy()
    t0 = time.monotonic()
    close = False
    if conn is None:
        conn = _connect()
        close = True
    notes: List[str] = []
    attempts: List[Dict[str, Any]] = []

    try:
        if not DETOUR_V3_ENABLED:
            return DetourComputeOutput(
                status="error",
                trip_id=trip_id,
                route_id="",
                policy_version=POLICY_VERSION_V3,
                error="detour_v3_disabled",
            )

        route_id, shape_id, shape_line = _load_trip_context(str(trip_id), conn)
        if not route_id or shape_line is None or getattr(shape_line, "is_empty", True):
            return DetourComputeOutput(
                status="error",
                trip_id=str(trip_id),
                route_id=route_id or "",
                policy_version=POLICY_VERSION_V3,
                error="trip_or_shape_not_found",
            )

        impact = analyze_trip_impact(
            trip_id=str(trip_id),
            route_id=route_id,
            shape_id=shape_id,
            shape_line=shape_line,
            blockage_geojson=blockage_geojson,
            physical_shape_line=None,
        )

        if not impact.intersects_blockage or impact.blocked is None:
            return DetourComputeOutput(
                status="no_impact",
                trip_id=str(trip_id),
                route_id=route_id,
                policy_version=POLICY_VERSION_V3,
            )

        feed_id = db.get_active_feed_id(conn)
        polygon_wkt = polygon_geojson_feature_to_wkt(blockage_geojson)
        bans = project_polygon_to_bans(conn, polygon_wkt, segment_import_run_id=DETOUR_V3_IMPORT_RUN_ID)

        pid = db.resolve_pattern_id_for_trip(str(trip_id), conn)
        if not pid:
            return DetourComputeOutput(
                status="error",
                trip_id=str(trip_id),
                route_id=route_id,
                policy_version=POLICY_VERSION_V3,
                error="pattern_not_found",
            )

        pos_rows = db.fetch_pattern_osm_segments_path(feed_id=int(feed_id), pattern_id=str(pid), conn=conn)
        pattern_seg_ids = [int(r["segment_id"]) for r in pos_rows]
        if len(pattern_seg_ids) < 2:
            return DetourComputeOutput(
                status="error",
                trip_id=str(trip_id),
                route_id=route_id,
                policy_version=POLICY_VERSION_V3,
                error="pattern_osm_path_missing",
                debug=(
                    {"detour_v3": build_debug_payload(
                        pattern_segment_ids=pattern_seg_ids,
                        banned_segment_count=len(bans.banned_segment_ids),
                        banned_turn_pair_count=len(bans.banned_turn_pairs),
                        attempts=attempts,
                        notes=["empty_or_short_pattern_osm_segments"],
                    )}
                    if debug_detour
                    else None
                ),
            )

        graph = load_road_graph(conn, segment_import_run_id=DETOUR_V3_IMPORT_RUN_ID)
        seg_nodes = {sid: (s.from_node_id, s.to_node_id) for sid, s in graph.segments.items()}

        raw_anchors = anchor_pairs_for_pattern_blocks(pattern_seg_ids, set(bans.banned_segment_ids))
        anchors = resolve_anchor_nodes(raw_anchors, seg_nodes)
        if not anchors:
            notes.append("no_interior_anchor_pairs")
            return DetourComputeOutput(
                status="no_safe_detour",
                trip_id=str(trip_id),
                route_id=route_id,
                policy_version=POLICY_VERSION_V3,
                attempts=attempts,
                corridor_stage="anchors",
                debug=(
                    {"detour_v3": build_debug_payload(
                        pattern_segment_ids=pattern_seg_ids,
                        banned_segment_count=len(bans.banned_segment_ids),
                        banned_turn_pair_count=len(bans.banned_turn_pairs),
                        attempts=attempts,
                        notes=notes + ["anchors_empty"],
                    )}
                    if debug_detour
                    else None
                ),
            )

        observed = db.fetch_gtfs_bus_observed_segment_ids(feed_id=int(feed_id), conn=conn)
        cost_mode_raw = str(DETOUR_V3_COST_MODE or "bus_corridor_plus_connectors").strip().lower()
        if cost_mode_raw == "strict_bus_corridor":
            cost_mode: BusCorridorCostMode = "strict_bus_corridor"
        else:
            cost_mode = "bus_corridor_plus_connectors"
        if cost_mode == "strict_bus_corridor" and not observed:
            notes.append("strict_mode_degraded_no_evidence_rows")
            cost_mode = "bus_corridor_plus_connectors"

        def enter_penalty(seg_id: int) -> float:
            s = graph.segments.get(seg_id)
            hw = s.highway if s is not None else None
            return bus_corridor_segment_enter_penalty_m(
                seg_id,
                hw,
                observed_segment_ids=observed,
                mode=cost_mode,
            )

        best: Optional[tuple[float, RankedCandidate, SegmentAnchorPair, List[int]]] = None

        ban_seg = bans.banned_segment_ids
        ban_turn = bans.banned_turn_pairs

        for ap in anchors:
            st = graph.segments.get(ap.exit_segment_id)
            gj = graph.segments.get(ap.rejoin_segment_id)
            if not st or not gj:
                attempts.append(
                    {
                        "exit_segment_id": ap.exit_segment_id,
                        "rejoin_segment_id": ap.rejoin_segment_id,
                        "status": "anchor_segment_not_in_graph",
                    },
                )
                continue
            path = astar_shortest_path(
                graph,
                int(ap.exit_node_id),
                int(ap.rejoin_node_id),
                profile=DEFAULT_ROUTING_COST_PROFILE,
                banned_segment_ids=ban_seg,
                banned_turn_pairs=ban_turn,
                segment_enter_extra_cost_m=enter_penalty,
            )
            if not path:
                attempts.append(
                    {
                        "exit_node": ap.exit_node_id,
                        "rejoin_node": ap.rejoin_node_id,
                        "status": "no_path",
                    },
                )
                continue
            if db.detour_path_intersects_polygon_wkt(path, polygon_wkt, conn=conn):
                attempts.append(
                    {
                        "exit_node": ap.exit_node_id,
                        "rejoin_node": ap.rejoin_node_id,
                        "status": "rejected_enters_polygon",
                        "segments": len(path),
                    },
                )
                continue
            dist = sum(float(graph.segments[s].length_m) for s in path)
            scr, breakdown = score_v3_path(
                detour_segment_ids=path,
                pattern_segment_ids=pattern_seg_ids,
                distance_m=dist,
            )
            geom_feat = db.fetch_segment_path_geojson_feature(path, conn=conn)
            decoded = _decoded_from_path(graph, path)
            decoded.geometry_geojson = geom_feat.get("geometry") or decoded.geometry_geojson

            cand = RankedCandidate(
                strategy="v3_own_graph_astar",
                total_score=float(scr),
                travel_time_s=float(dist / 10.0),
                distance_m=float(dist),
                decoded=decoded,
                feasibility=FeasibilityResult(
                    accepted=True,
                    confidence_score=min(1.0, 0.45 + breakdown.get("overlap_ratio", 0.0)),
                ),
                score_breakdown=breakdown,
                tier="REVIEW_RECOMMENDED",
                confidence_score=min(1.0, 0.45 + breakdown.get("overlap_ratio", 0.0)),
                warnings=["v3_own_graph_candidate"],
                hard_constraints_passed=["polygon_clear", "astar_legal_turns"],
                candidate_rank=len(attempts) + 1,
                review_required=True,
            )

            attempts.append(
                {
                    "exit_node": ap.exit_node_id,
                    "rejoin_node": ap.rejoin_node_id,
                    "segment_count": len(path),
                    "distance_m": dist,
                    "score": scr,
                },
            )

            if best is None or scr > best[0]:
                best = (scr, cand, ap, path)

        elapsed_ms = int(round((time.monotonic() - t0) * 1000.0))
        log(
            "detours/v3/compute",
            f"trip_id={trip_id} route_id={route_id} pattern_id={pid} elapsed_ms={elapsed_ms} attempt_count={len(attempts)}",
        )

        if best is None:
            return DetourComputeOutput(
                status="no_safe_detour",
                trip_id=str(trip_id),
                route_id=route_id,
                policy_version=POLICY_VERSION_V3,
                attempts=attempts,
                corridor_stage="routing",
                debug=(
                    {"detour_v3": build_debug_payload(
                        pattern_segment_ids=pattern_seg_ids,
                        banned_segment_count=len(bans.banned_segment_ids),
                        banned_turn_pair_count=len(bans.banned_turn_pairs),
                        attempts=attempts,
                        notes=notes,
                    )}
                    if debug_detour
                    else None
                ),
            )

        _scr, ranked, ap_chosen, path_ids = best
        anchor_pair = AnchorPair(
            exit_lon=0.0,
            exit_lat=0.0,
            rejoin_lon=0.0,
            rejoin_lat=0.0,
            exit_shape_dist_m=float(impact.blocked.blocked_start_m) if impact.blocked else 0.0,
            rejoin_shape_dist_m=float(impact.blocked.blocked_end_m) if impact.blocked else 0.0,
            anchor_geometry_source="pattern_osm_segments",
            anchor_source="segment_anchor",
            exit_osm_segment_id=int(ap_chosen.exit_segment_id),
            rejoin_osm_segment_id=int(ap_chosen.rejoin_segment_id),
        )

        debug_obj = None
        if debug_detour:
            dbg = build_debug_payload(
                pattern_segment_ids=pattern_seg_ids,
                banned_segment_count=len(bans.banned_segment_ids),
                banned_turn_pair_count=len(bans.banned_turn_pairs),
                attempts=attempts,
                notes=notes
                + [
                    f"v3_anchor_exit_seg={ap_chosen.exit_segment_id}",
                    f"v3_anchor_rejoin_seg={ap_chosen.rejoin_segment_id}",
                    f"path_segments={len(path_ids)}",
                ],
            )
            dbg["chosen_path_segment_ids"] = [int(x) for x in path_ids]
            if raw_anchors:
                dbg["blocked_idx_range"] = [raw_anchors[0].blocked_first_idx, raw_anchors[0].blocked_last_idx]
            debug_obj = {"detour_v3": dbg}

        out = DetourComputeOutput(
            status="review_recommended",
            trip_id=str(trip_id),
            route_id=route_id,
            policy_version=f"{POLICY_VERSION_V3}/{pol.version}",
            anchors=anchor_pair,
            candidates=[ranked],
            selected=ranked,
            attempts=attempts,
            debug=debug_obj,
        )
        if debug_detour:
            try:
                log("detours/v3/compute_ai", format_detour_ai_log_line(out))
            except Exception:
                pass
        return out
    finally:
        if close:
            conn.close()


__all__ = ["POLICY_VERSION_V3", "compute_detour_for_trip"]
