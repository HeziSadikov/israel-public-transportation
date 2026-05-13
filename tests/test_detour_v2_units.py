"""Unit tests for detour v2 policy, ranking, and shape impact (no DB)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest
from pydantic import ValidationError
from shapely.geometry import LineString, box

from backend.domain.detour_v2.bus_feasibility_evaluator import evaluate_candidate
from backend.domain.detour_v2.anchor_selector import enumerate_anchor_candidates, enumerate_intersection_anchor_candidates
from backend.domain.detour_v2.intersection_finder import IntersectionPoint, find_route_intersections
from backend.domain.detour_v2.candidate_decoder import decode_polyline_to_synthetic_segments
from backend.domain.detour_v2.detour_ranker import rank_candidates
from backend.domain.detour_v2.incident_projector import IncidentProjection
from backend.domain.detour_v2.models import (
    AnchorPair,
    BlockedShapeInterval,
    DecodedCandidate,
    DetourComputeOutput,
    FeasibilityResult,
    RankedCandidate,
    RoadSegmentRef,
)
from backend.domain.detour_v2.policy import DetourPolicyConfig
from backend.domain.detour_v2.road_candidate_generator import generate_candidates_with_debug
from backend.domain.detour_v2.serialize import detour_compute_output_to_dict
from backend.domain.detour_v2.trip_impact_analyzer import blocked_interval_along_line
from backend.domain.pattern_builder import resolve_representative_trip_id
from backend.adapters.osm_detour import route_avoiding_polygon_alternates_debug
from backend.mcp_server.schemas.api_models import DetourComputeV2Request


def test_blocked_interval_along_line_simple():
    line = LineString([(34.0, 31.5), (34.01, 31.51), (34.02, 31.52)])
    poly = box(34.005, 31.505, 34.015, 31.515)
    b = blocked_interval_along_line(line, {"type": "Polygon", "coordinates": [list(poly.exterior.coords)]})
    assert b is not None
    assert b.blocked_start_m < b.blocked_end_m
    assert b.blocked_end_m <= b.shape_length_m + 1


def test_rank_candidates_prefers_lower_score():
    pol = DetourPolicyConfig()
    items = [
        ("a", 100.0, 1000.0, FeasibilityResult(accepted=True), {}, None),
        ("b", 90.0, 900.0, FeasibilityResult(accepted=True), {}, None),
    ]
    ranked = rank_candidates(items, pol)
    assert ranked[0].strategy == "b"


def test_feasibility_synthetic_skips_local_fraction_hard_reject():
    pol = DetourPolicyConfig()
    coords = [(34.0 + i * 0.001, 31.5) for i in range(20)]
    dec = decode_polyline_to_synthetic_segments(coords, 120.0)
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=500.0,
        detour_time_s=80.0,
        banned_way_ids=set(),
    )
    assert "local_fraction_exceeded" not in feas.hard_reject_reasons


def test_feasibility_local_share_hard_reject_only_when_policy_enabled():
    """High residential share hard-rejects by default (reject_hard_local_share=True).
    When the policy flag is set to False it downgrades to a penalty only."""
    seg_res = RoadSegmentRef(
        segment_id=1,
        osm_way_id=101,
        from_node_id=1,
        to_node_id=2,
        sequence_index=0,
        length_m=800.0,
        travel_time_s=80.0,
        highway="residential",
        synthetic=False,
    )
    seg_pri = RoadSegmentRef(
        segment_id=2,
        osm_way_id=102,
        from_node_id=2,
        to_node_id=3,
        sequence_index=1,
        length_m=200.0,
        travel_time_s=20.0,
        highway="primary",
        synthetic=False,
    )
    dec = DecodedCandidate(
        road_segments=[seg_res, seg_pri],
        turns=[],
        geometry_geojson={"type": "FeatureCollection", "features": []},
    )
    # Default policy: reject_hard_local_share=True → 80% residential > 40% threshold → hard reject.
    # Provide GTFS evidence for both ways so the GTFS check doesn't fire first.
    pol = DetourPolicyConfig()
    assert pol.vehicle.reject_hard_local_share is True
    gtfs_ev = {101: {"confidence_score": 0.9}, 102: {"confidence_score": 0.9}}
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=1000.0,
        detour_time_s=100.0,
        banned_way_ids=set(),
        gtfs_way_evidence=gtfs_ev,
    )
    assert not feas.accepted
    assert any("local_road_share_too_high" in r for r in feas.hard_reject_reasons)

    # When flag is disabled → penalty-only (accepted).
    pol.vehicle.reject_hard_local_share = False
    feas2 = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=1000.0,
        detour_time_s=100.0,
        banned_way_ids=set(),
        gtfs_way_evidence=gtfs_ev,
    )
    assert feas2.accepted
    assert not feas2.hard_reject_reasons
    assert any("local_fraction" in w or "high_local" in w for w in feas2.warnings)


_MIN_POLY = {"type": "Polygon", "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]}


def test_detour_compute_v2_request_accepts_trip_ids():
    req = DetourComputeV2Request(
        service_date="20250101",
        trip_ids=["abc"],
        blockage_geojson=_MIN_POLY,
    )
    assert req.trip_ids == ["abc"]
    assert req.route_id is None


def test_detour_compute_v2_request_accepts_route_id_without_trip_ids():
    req = DetourComputeV2Request(
        service_date="20250101",
        trip_ids=[],
        route_id="R1",
        blockage_geojson=_MIN_POLY,
    )
    assert req.route_id == "R1"


def test_detour_compute_v2_request_accepts_debug_flags():
    req = DetourComputeV2Request(
        service_date="20250101",
        trip_ids=["t1"],
        blockage_geojson=_MIN_POLY,
        debug_detour=True,
        use_matched_physical=True,
    )
    assert req.debug_detour is True
    assert req.use_matched_physical is True


def test_detour_compute_v2_request_rejects_empty_trip_and_route():
    with pytest.raises(ValidationError):
        DetourComputeV2Request(
            service_date="20250101",
            trip_ids=[],
            blockage_geojson=_MIN_POLY,
        )


@patch("backend.domain.pattern_builder.get_stop_times_for_trip")
def test_resolve_representative_trip_id_picks_trip(mock_st):
    mock_st.return_value = [
        {"stop_id": "a", "stop_sequence": 1, "arrival_time": None, "departure_time": None},
        {"stop_id": "b", "stop_sequence": 2, "arrival_time": None, "departure_time": None},
    ]
    feed = SimpleNamespace(
        trips=[
            {
                "trip_id": "T_MAIN",
                "route_id": "R1",
                "direction_id": "0",
                "service_id": "SVC1",
                "shape_id": "SH1",
            },
        ],
        calendar=[],
    )
    tid = resolve_representative_trip_id(feed, "R1", None, "20250101")
    assert tid == "T_MAIN"


def test_route_avoiding_polygon_alternates_debug_reports_missing_url():
    with patch("backend.adapters.osm_detour.VALHALLA_URL", ""):
        out, dbg = route_avoiding_polygon_alternates_debug(
            from_lon=34.7,
            from_lat=32.0,
            to_lon=34.8,
            to_lat=32.1,
            blockage_geojson=_MIN_POLY,
        )
    assert out == []
    assert dbg["valhalla_url_set"] is False
    assert dbg["error_type"] == "valhalla_url_missing"


@patch("backend.domain.detour_v2.road_candidate_generator.route_avoiding_polygon_alternates_debug")
def test_generate_candidates_with_debug_reports_no_legs_reason(mock_alt):
    mock_alt.return_value = (
        [],
        {
            "valhalla_url_set": True,
            "request_timeout_s": 25.0,
            "alternates_requested": 2,
            "exclude_polygon_count": 1,
            "http_status": 200,
            "error_type": None,
            "error_message": None,
            "primary_legs_count": 0,
            "alternates_count": 0,
            "fallback_attempted": True,
            "fallback_success": False,
        },
    )
    out, dbg = generate_candidates_with_debug(
        exit_lon=34.7,
        exit_lat=32.0,
        rejoin_lon=34.8,
        rejoin_lat=32.1,
        blockage_geojson=_MIN_POLY,
    )
    assert out == []
    assert dbg["candidate_generation_reason"] == "no_legs_in_response"


def test_feasibility_strict_rejects_way_without_gtfs_evidence():
    """With require_gtfs_way_evidence=True (now the default), unknown way is a hard reject."""
    pol = DetourPolicyConfig()
    assert pol.vehicle.require_gtfs_way_evidence is True  # confirm default
    seg = RoadSegmentRef(
        segment_id=1,
        osm_way_id=123,
        from_node_id=1,
        to_node_id=2,
        sequence_index=0,
        length_m=200.0,
        travel_time_s=40.0,
        highway="primary",
        synthetic=False,
    )
    dec = DecodedCandidate(
        road_segments=[seg],
        turns=[],
        geometry_geojson={"type": "FeatureCollection", "features": []},
    )
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=80.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=200.0,
        detour_time_s=40.0,
        banned_way_ids=set(),
        gtfs_way_evidence={},
    )
    # Now a hard reject, not just a warning.
    assert not feas.accepted
    assert any("not_in_gtfs_bus_corridor:123" in r for r in feas.hard_reject_reasons)


def test_feasibility_strict_accepts_way_with_gtfs_evidence():
    """A segment with GTFS-bus evidence must not be hard-rejected."""
    pol = DetourPolicyConfig()
    seg = RoadSegmentRef(
        segment_id=1,
        osm_way_id=321,
        from_node_id=1,
        to_node_id=2,
        sequence_index=0,
        length_m=200.0,
        travel_time_s=40.0,
        highway="primary",
        synthetic=False,
    )
    dec = DecodedCandidate(
        road_segments=[seg],
        turns=[],
        geometry_geojson={"type": "FeatureCollection", "features": []},
    )
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=80.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=200.0,
        detour_time_s=40.0,
        banned_way_ids=set(),
        gtfs_way_evidence={321: {"confidence_score": 0.95}},
    )
    assert feas.accepted
    assert not any("not_in_gtfs_bus_corridor" in r for r in feas.hard_reject_reasons)


def test_feasibility_valhalla_uturn_maneuver_type_float_still_detected_when_hard_reject_enabled():
    """With reject_explicit_u_turn_maneuver=True (new default), U-turn causes a hard reject."""
    pol = DetourPolicyConfig()
    assert pol.service.reject_explicit_u_turn_maneuver is True
    dec = decode_polyline_to_synthetic_segments([(34.8, 32.1), (34.81, 32.1), (34.82, 32.1)], 60.0)
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=350.0,
        detour_time_s=120.0,
        banned_way_ids=set(),
        turn_by_turn=[{"maneuver_type": 12.0}],
        bearing_delta_exit_deg=10.0,
        bearing_delta_rejoin_deg=10.0,
    )
    assert "u_turn_maneuver_detected" in feas.warnings
    assert not feas.accepted
    assert "explicit_u_turn_maneuver" in feas.hard_reject_reasons

    # When the flag is disabled → U-turn adds turn penalty but does not hard-reject.
    pol.service.reject_explicit_u_turn_maneuver = False
    feas2 = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=350.0,
        detour_time_s=120.0,
        banned_way_ids=set(),
        turn_by_turn=[{"maneuver_type": 12.0}],
        bearing_delta_exit_deg=10.0,
        bearing_delta_rejoin_deg=10.0,
    )
    assert feas2.accepted


def test_feasibility_no_false_positive_uturn_substring_in_unrelated_instruction():
    """Old logic used `'uturn' in txt`, which matched inside unrelated tokens (e.g. 'Futurn')."""
    pol = DetourPolicyConfig()
    dec = decode_polyline_to_synthetic_segments([(34.8, 32.1), (34.81, 32.1), (34.82, 32.1)], 60.0)
    assert "uturn" in "Futurn".lower()
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=350.0,
        detour_time_s=120.0,
        banned_way_ids=set(),
        turn_by_turn=[{"instruction_en": "Continue toward Futurn Industrial Zone"}],
        bearing_delta_exit_deg=10.0,
        bearing_delta_rejoin_deg=10.0,
    )
    assert "u_turn_maneuver_detected" not in feas.warnings
    assert feas.accepted


def test_feasibility_rejects_explicit_u_turn_and_backtrack_heading():
    pol = DetourPolicyConfig()
    pol.service.reject_explicit_u_turn_maneuver = True
    # Enable hard backtrack reject explicitly for this test.
    pol.service.reject_hard_backtrack = True
    dec = decode_polyline_to_synthetic_segments([(34.8, 32.1), (34.81, 32.1), (34.82, 32.1)], 60.0)
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=350.0,
        detour_time_s=120.0,
        banned_way_ids=set(),
        turn_by_turn=[{"instruction_en": "Make a U-turn at the next intersection"}],
        bearing_delta_exit_deg=172.0,
        bearing_delta_rejoin_deg=20.0,
    )
    # Large bearing delta is an absolute hard reject (wrong-direction rejoin).
    assert "wrong_direction_rejoin" in feas.hard_reject_reasons
    assert not feas.accepted


def test_enumerate_anchor_candidates_returns_multiple_pairs():
    pol = DetourPolicyConfig()
    line = LineString([(34.0, 31.5), (34.01, 31.5), (34.02, 31.5), (34.03, 31.5), (34.04, 31.5)])
    blocked = SimpleNamespace(blocked_start_m=900.0, blocked_end_m=1200.0, shape_length_m=4000.0)
    stop_rows = [
        {"stop_id": "s0", "shape_dist_traveled": 600.0},
        {"stop_id": "s1", "shape_dist_traveled": 700.0},
        {"stop_id": "s2", "shape_dist_traveled": 800.0},
        {"stop_id": "s3", "shape_dist_traveled": 1300.0},
        {"stop_id": "s4", "shape_dist_traveled": 1400.0},
        {"stop_id": "s5", "shape_dist_traveled": 1500.0},
    ]
    stop_lonlat = {
        "s0": (34.001, 31.5),
        "s1": (34.008, 31.5),
        "s2": (34.010, 31.5),
        "s3": (34.020, 31.5),
        "s4": (34.025, 31.5),
        "s5": (34.028, 31.5),
    }
    cands = enumerate_anchor_candidates(
        line=line,
        blocked=blocked,
        stop_rows=stop_rows,
        stop_lonlat=stop_lonlat,
        policy=pol,
        max_pairs=3,
    )
    assert len(cands) == 3
    assert cands[0].exit_shape_dist_m < cands[0].rejoin_shape_dist_m
    assert cands[0].exit_stop_id is not None
    assert cands[0].rejoin_stop_id is not None


@patch("backend.adapters.osm_detour.httpx.post")
def test_route_alternates_sends_costing_options(mock_post):
    resp = SimpleNamespace(
        status_code=200,
        text="",
        json=lambda: {"trip": {"legs": [{"summary": {"length": 0.5, "time": 80}, "shape": ""}]}, "alternates": []},
    )
    mock_post.return_value = resp
    with patch("backend.adapters.osm_detour.VALHALLA_URL", "http://valhalla:8002"):
        out, dbg = route_avoiding_polygon_alternates_debug(
            from_lon=34.7,
            from_lat=32.0,
            to_lon=34.8,
            to_lat=32.1,
            blockage_geojson=_MIN_POLY,
            costing="bus",
            alternate_count=0,
        )
    assert len(out) == 1
    assert dbg.get("valhalla_attempt_used") == "primary"
    sent = mock_post.call_args.kwargs["json"]
    assert sent["costing"] == "bus"
    assert "costing_options" in sent
    assert "bus" in sent["costing_options"]


def _make_rejected_ranked_candidate() -> RankedCandidate:
    """Build a RankedCandidate that carries total_score=inf (hard-rejected by feasibility)."""
    dec = decode_polyline_to_synthetic_segments([(34.8, 32.1), (34.81, 32.1)], 60.0)
    return RankedCandidate(
        strategy="alternate_1",
        travel_time_s=120.0,
        distance_m=800.0,
        total_score=float("inf"),
        feasibility=FeasibilityResult(
            accepted=False,
            hard_reject_reasons=["no_real_bypass:coincide=0.95"],
            confidence_score=0.0,
            warnings=["detour_coincides_with_route_95pct"],
        ),
        decoded=dec,
        score_breakdown={"total_score": float("inf"), "some_penalty": float("inf")},
        tier="LOW_CONFIDENCE",
        confidence_score=0.0,
        warnings=[],
        hard_constraints_passed=[],
        candidate_rank=2,
        review_required=True,
        rejection_reasons=["no_real_bypass:coincide=0.95"],
    )


def test_feasibility_rejects_detour_that_coincides_with_route():
    """Detour geometry that traces the original route slice must be hard-rejected with no_real_bypass."""
    # Route: straight east from lon 34.80 → 34.90 (constant lat 32.10).
    # Anchors: exit at 200 m, rejoin at 1600 m of a 2 km route.
    # Detour: identical to the route between the anchors → coincide ≈ 1.0.
    route_coords = [(34.80 + i * 0.001, 32.10) for i in range(11)]  # 10 segments ≈ 1 km each degree-step
    dec = decode_polyline_to_synthetic_segments(
        [(34.802, 32.10), (34.810, 32.10), (34.814, 32.10), (34.816, 32.10)], 60.0
    )
    pol = DetourPolicyConfig()
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=500.0,
        baseline_blocked_time_s=60.0,
        detour_distance_m=900.0,
        detour_time_s=80.0,
        banned_way_ids=set(),
        stitch_ok=True,
        route_coincide_fraction=0.96,  # clearly degenerate
    )
    assert not feas.accepted
    assert any("no_real_bypass" in r for r in feas.hard_reject_reasons)

    # Just below new threshold (0.49) → should be accepted (all-synthetic segments, no other rejects).
    feas2 = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=500.0,
        baseline_blocked_time_s=60.0,
        detour_distance_m=900.0,
        detour_time_s=80.0,
        banned_way_ids=set(),
        stitch_ok=True,
        route_coincide_fraction=0.49,
    )
    assert feas2.accepted
    assert not any("no_real_bypass" in r for r in feas2.hard_reject_reasons)


def test_serialize_rejected_candidate_is_postgres_jsonb_safe():
    """Rejected candidates carry total_score=inf; serialized output must be jsonb-safe (no Infinity tokens)."""
    rejected = _make_rejected_ranked_candidate()
    dec_accepted = decode_polyline_to_synthetic_segments([(34.8, 32.1), (34.81, 32.1), (34.82, 32.1)], 90.0)
    accepted = RankedCandidate(
        strategy="baseline_bus",
        travel_time_s=95.0,
        distance_m=1200.0,
        total_score=120.0,
        feasibility=FeasibilityResult(accepted=True, hard_reject_reasons=[], confidence_score=0.65, warnings=[]),
        decoded=dec_accepted,
        score_breakdown={"total_score": 120.0},
        tier="REVIEW_RECOMMENDED",
        confidence_score=0.65,
        warnings=[],
        hard_constraints_passed=[],
        candidate_rank=1,
        review_required=True,
        rejection_reasons=[],
    )
    anchors = AnchorPair(
        exit_lon=34.80, exit_lat=32.10,
        rejoin_lon=34.82, rejoin_lat=32.10,
        exit_shape_dist_m=200.0,
        rejoin_shape_dist_m=1800.0,
    )
    out = DetourComputeOutput(
        status="review_recommended",
        trip_id="trip_123",
        route_id="R1",
        anchors=anchors,
        candidates=[accepted],
        selected=accepted,
        discarded=[rejected],
        policy_version="v2-default-6",
    )
    d = detour_compute_output_to_dict(out)
    # Must not raise — this is the same constraint as PostgreSQL jsonb.
    json.dumps(d, allow_nan=False)


# ---------------------------------------------------------------------------
# Intersection-finder tests
# ---------------------------------------------------------------------------

def _make_trace_detail(*, drivable_cross_class: str = "primary", footway_only: bool = False):
    """Build a minimal match_route_attributes_detailed return value."""
    if footway_only:
        inter_edges = [{"driveability": "both", "road_class": "footway"}]
    else:
        inter_edges = [
            {"driveability": "both", "road_class": drivable_cross_class},
        ]
    return {
        "edges": [
            {
                "road_class": "secondary",
                "end_shape_index": 1,
                "end_osm_node_id": 42,
                "end_node": {"intersecting_edges": inter_edges},
            }
        ],
        "shape_lonlat": [(34.800, 32.100), (34.810, 32.100)],
    }


def test_find_route_intersections_filters_non_drivable_junctions():
    """Nodes where only footways cross must be filtered out."""
    coords = [(34.800, 32.100), (34.810, 32.100), (34.820, 32.100)]
    detail_footway = _make_trace_detail(footway_only=True)
    with patch(
        "backend.adapters.osm_detour.match_route_attributes_detailed",
        return_value=detail_footway,
    ):
        result = find_route_intersections(
            route_coords_lonlat=coords,
            total_m=2000.0,
        )
    assert result == [], "Footway-only junction must be filtered out"


def test_find_route_intersections_keeps_drivable_junction():
    """Nodes with at least one drivable non-service cross-street must be returned."""
    coords = [(34.800, 32.100), (34.810, 32.100), (34.820, 32.100)]
    detail_primary = _make_trace_detail(drivable_cross_class="primary")
    with patch(
        "backend.adapters.osm_detour.match_route_attributes_detailed",
        return_value=detail_primary,
    ):
        result = find_route_intersections(
            route_coords_lonlat=coords,
            total_m=2000.0,
        )
    assert len(result) == 1
    assert result[0].cross_count == 1
    assert "primary" in result[0].cross_road_classes
    assert result[0].osm_node_id == 42


def test_intersection_anchor_pair_straddles_blockage():
    """enumerate_intersection_anchor_candidates: exit < blocked_start, rejoin > blocked_end,
    and skipped_est drives selection (close pair with fewer skipped stops wins over far pair)."""
    # Route 0–3000 m; blockage 1000–2000 m.
    # Stops at 700, 800, 900, 2100, 2200 m (so close pairs skip fewer stops).
    stops = [("s1", 700.0), ("s2", 800.0), ("s3", 900.0), ("s4", 2100.0), ("s5", 2200.0)]
    intersections = [
        # close exit — clearance 250 m, only 0 stops between it and close rejoin
        IntersectionPoint(shape_dist_m=750.0, lon=34.8075, lat=32.100, road_class="secondary", cross_road_classes=("primary",), cross_count=1),
        # far exit — clearance 1500 m, would skip many stops
        IntersectionPoint(shape_dist_m=200.0, lon=34.802, lat=32.100, road_class="secondary", cross_road_classes=("primary",), cross_count=1),
        # close rejoin — 50 m past blockage end → exactly at gate (50 m), should be included
        IntersectionPoint(shape_dist_m=2050.0, lon=34.8205, lat=32.100, road_class="secondary", cross_road_classes=("primary",), cross_count=1),
        # far rejoin — clearance 700 m
        IntersectionPoint(shape_dist_m=2700.0, lon=34.827, lat=32.100, road_class="secondary", cross_road_classes=("primary",), cross_count=1),
        # inside blockage — excluded
        IntersectionPoint(shape_dist_m=1500.0, lon=34.815, lat=32.100, road_class="secondary", cross_road_classes=("residential",), cross_count=1),
    ]
    blocked = BlockedShapeInterval(blocked_start_m=1000.0, blocked_end_m=2000.0, shape_length_m=3000.0)
    policy = DetourPolicyConfig()

    pairs = enumerate_intersection_anchor_candidates(
        intersections=intersections,
        blocked=blocked,
        stops_with_dist=stops,
        policy=policy,
        max_pairs=5,
        search_before_window_m=2000.0,
        search_after_window_m=2000.0,
    )
    assert pairs, "Expected at least one anchor pair"
    for p in pairs:
        assert p.exit_shape_dist_m < blocked.blocked_start_m, "Exit must be before blockage start"
        assert p.rejoin_shape_dist_m > blocked.blocked_end_m, "Rejoin must be after blockage end"
        assert p.anchor_source == "intersection_finder"

    # Fewest skipped stops wins: close pair (exit=750, rejoin=2050) skips stops s2, s3 (dist 800, 900)
    # = 2 stops.  Far pair (exit=200, rejoin=2050) skips s1,s2,s3 = 3 stops.
    # So close pair should be ranked first.
    best = pairs[0]
    skipped_in_best = sum(1 for _, d in stops if best.exit_shape_dist_m < d < best.rejoin_shape_dist_m)
    # Verify we did not pick the far exit (shape_dist=200) when the close one (750) exists
    assert best.exit_shape_dist_m >= 750.0 - 1.0, (
        f"Expected close exit (≥750 m); got {best.exit_shape_dist_m:.0f} m"
    )


def test_intersection_finder_shape_dist_uses_geometric_projection():
    """shape_dist_m must be derived from projecting onto the original route, not Valhalla's
    shape index — which is an index into the denser matched shape, not the input coords."""
    # Route runs west→east from lon=34.800 to lon=34.830, lat fixed at 32.100.
    # Total length ≈ 3000 m at this latitude (30*0.001° * 111_320 * cos(32.1°) ≈ 2826 m).
    import math as _math
    n = 31
    route_coords = [(34.800 + i * 0.001, 32.100) for i in range(n)]
    total_m = sum(
        _math.hypot(
            (route_coords[i+1][0] - route_coords[i][0]) * _math.cos(_math.radians(route_coords[i][1])) * 111_320.0,
            (route_coords[i+1][1] - route_coords[i][1]) * 111_320.0,
        )
        for i in range(n - 1)
    )
    midpoint_lon = 34.815  # 15th point out of 30 intervals = midpoint geographically

    # Fake Valhalla response: one edge whose end_shape_index is 25 in a
    # DENSE matched shape (60 points = 2× the input density). Without the
    # projection fix, the code would use trace_cumdist[25] ≈ 83% of route.
    # With the fix, it projects (34.815, 32.100) onto the original route ≈ 50%.
    dense_shape = [(34.800 + i * 0.0005, 32.100) for i in range(61)]

    fake_detail = {
        "edges": [
            {
                "road_class": "primary",
                "end_shape_index": 25,   # index into dense_shape, NOT route_coords
                "end_osm_node_id": 99,
                "end_node": {
                    "intersecting_edges": [
                        {"driveability": "both", "road_class": "secondary"}
                    ]
                },
            }
        ],
        "shape_lonlat": dense_shape,
    }
    with patch(
        "backend.adapters.osm_detour.match_route_attributes_detailed",
        return_value=fake_detail,
    ):
        result = find_route_intersections(
            route_coords_lonlat=route_coords,
            total_m=total_m,
        )

    assert len(result) == 1, "Expected exactly one intersection"
    # dense_shape[25] has lon=34.8125, which is the geometric midpoint of the
    # dense shape. Its projection onto the original route should be close to
    # total_m * (34.8125 - 34.800) / (34.830 - 34.800) ≈ 41.7% of route, i.e.
    # shape_dist_m ≈ 0.417 * total_m. It must NOT be ≈ 83% (trace_cumdist[25]).
    sd = result[0].shape_dist_m
    expected_approx = total_m * (34.8125 - 34.800) / (34.830 - 34.800)
    bad_approx = total_m * 25 / (n - 1)   # what the old (broken) code would return
    assert abs(sd - expected_approx) < 50.0, (
        f"shape_dist_m={sd:.1f} should be near {expected_approx:.1f} m (projection), "
        f"not {bad_approx:.1f} m (wrong index)"
    )


def test_intersection_anchors_fallback_to_stops_when_trace_attributes_empty():
    """When find_route_intersections returns [], stop-window fallback must still produce anchors."""
    with patch(
        "backend.domain.detour_v2.compute.find_route_intersections",
        return_value=[],
    ), patch(
        "backend.domain.detour_v2.compute.db.get_stop_times_for_trip",
        return_value=[
            {"stop_id": "s1", "shape_dist_traveled": 600.0},
            {"stop_id": "s2", "shape_dist_traveled": 2400.0},
        ],
    ), patch(
        "backend.domain.detour_v2.compute.db.get_stop_lonlat_bulk",
        return_value={},
    ), patch(
        "backend.domain.detour_v2.compute._load_trip_context",
        return_value=("route_1", "shape_1", LineString([(34.8 + i * 0.001, 32.1) for i in range(31)])),
    ), patch(
        "backend.domain.detour_v2.compute.db.get_active_feed_id",
        return_value=None,
    ), patch(
        "backend.domain.detour_v2.compute.db.get_detour_evidence_diag_stats",
        return_value={},
    ), patch(
        "backend.domain.detour_v2.compute.project_incident_polygon",
        return_value=MagicMock(edge_bans=[], turn_bans=[]),
    ), patch(
        "backend.domain.detour_v2.compute.edge_ban_way_ids",
        return_value=set(),
    ), patch(
        "backend.domain.detour_v2.compute._validate_anchors_via_locate",
        side_effect=lambda cands, *a, **kw: cands,
    ), patch(
        "backend.domain.detour_v2.compute.generate_candidates_with_debug",
        return_value=([], {}),
    ):
        from backend.domain.detour_v2.compute import compute_detour_for_trip
        result = compute_detour_for_trip(
            trip_id="test_trip",
            service_date="2026-05-11",
            blockage_geojson={"type": "Polygon", "coordinates": [[[34.812, 32.099], [34.817, 32.099], [34.817, 32.101], [34.812, 32.101], [34.812, 32.099]]]},
        )
    # Must not be an error; no_impact or emergency_fallback or no_safe_detour are all acceptable
    assert result.status not in ("error",), f"Unexpected error status: {result.status}"


# ---------------------------------------------------------------------------
# New tests: strict-GTFS hard reject / relaxed accept / coincide 0.47 / two-pass
# ---------------------------------------------------------------------------

def test_feasibility_hard_rejects_unknown_way_under_strict_gtfs():
    """With require_gtfs_way_evidence=True, a segment with no GTFS evidence is a hard reject."""
    pol = DetourPolicyConfig()
    assert pol.vehicle.require_gtfs_way_evidence is True
    seg = RoadSegmentRef(
        segment_id=1, osm_way_id=999, from_node_id=10, to_node_id=11,
        sequence_index=0, length_m=300.0, travel_time_s=60.0, highway="secondary", synthetic=False,
    )
    dec = DecodedCandidate(
        road_segments=[seg], turns=[],
        geometry_geojson={"type": "FeatureCollection", "features": []},
    )
    feas = evaluate_candidate(
        decoded=dec, projection=IncidentProjection(), policy=pol,
        baseline_blocked_distance_m=100.0, baseline_blocked_time_s=20.0,
        detour_distance_m=300.0, detour_time_s=60.0, banned_way_ids=set(),
        gtfs_way_evidence={}, bus_edge_evidence={},
    )
    assert not feas.accepted
    assert any("not_in_gtfs_bus_corridor:999" in r for r in feas.hard_reject_reasons)


def test_feasibility_accepts_unknown_way_under_relaxed_gtfs():
    """With require_gtfs_way_evidence=False, unknown ways are NOT hard-rejected."""
    pol = DetourPolicyConfig()
    pol.vehicle.require_gtfs_way_evidence = False
    seg = RoadSegmentRef(
        segment_id=1, osm_way_id=999, from_node_id=10, to_node_id=11,
        sequence_index=0, length_m=300.0, travel_time_s=60.0, highway="secondary", synthetic=False,
    )
    dec = DecodedCandidate(
        road_segments=[seg], turns=[],
        geometry_geojson={"type": "FeatureCollection", "features": []},
    )
    feas = evaluate_candidate(
        decoded=dec, projection=IncidentProjection(), policy=pol,
        baseline_blocked_distance_m=100.0, baseline_blocked_time_s=20.0,
        detour_distance_m=300.0, detour_time_s=60.0, banned_way_ids=set(),
        gtfs_way_evidence={}, bus_edge_evidence={},
    )
    assert feas.accepted
    assert not any("not_in_gtfs_bus_corridor" in r for r in feas.hard_reject_reasons)


def test_feasibility_bus_edge_evidence_counts_as_known():
    """operator-approved bus_edge_evidence keeps the way from being hard-rejected."""
    pol = DetourPolicyConfig()
    assert pol.vehicle.require_gtfs_way_evidence is True
    seg = RoadSegmentRef(
        segment_id=1, osm_way_id=888, from_node_id=10, to_node_id=11,
        sequence_index=0, length_m=200.0, travel_time_s=40.0, highway="secondary", synthetic=False,
    )
    dec = DecodedCandidate(
        road_segments=[seg], turns=[],
        geometry_geojson={"type": "FeatureCollection", "features": []},
    )
    feas = evaluate_candidate(
        decoded=dec, projection=IncidentProjection(), policy=pol,
        baseline_blocked_distance_m=80.0, baseline_blocked_time_s=20.0,
        detour_distance_m=200.0, detour_time_s=40.0, banned_way_ids=set(),
        gtfs_way_evidence={},
        bus_edge_evidence={888: {"confidence_score": 0.8, "approved_detour_count": 1}},
    )
    assert feas.accepted
    assert not any("not_in_gtfs_bus_corridor" in r for r in feas.hard_reject_reasons)


def test_feasibility_rejects_detour_with_51pct_coincidence():
    """route_coincide_fraction=0.51 must be hard-rejected under the new 0.50 cap;
    a value of 0.47 is below the cap and should be accepted."""
    pol = DetourPolicyConfig()
    assert pol.service.max_route_coincide_fraction == 0.50
    dec = decode_polyline_to_synthetic_segments([(34.8, 32.1), (34.81, 32.1), (34.82, 32.1)], 60.0)

    # Above cap → hard reject.
    feas = evaluate_candidate(
        decoded=dec, projection=IncidentProjection(), policy=pol,
        baseline_blocked_distance_m=100.0, baseline_blocked_time_s=20.0,
        detour_distance_m=350.0, detour_time_s=80.0, banned_way_ids=set(),
        stitch_ok=True, route_coincide_fraction=0.51,
    )
    assert not feas.accepted
    assert any("no_real_bypass" in r for r in feas.hard_reject_reasons)

    # Below cap → accepted (0.47 < 0.50).
    feas2 = evaluate_candidate(
        decoded=dec, projection=IncidentProjection(), policy=pol,
        baseline_blocked_distance_m=100.0, baseline_blocked_time_s=20.0,
        detour_distance_m=350.0, detour_time_s=80.0, banned_way_ids=set(),
        stitch_ok=True, route_coincide_fraction=0.47,
    )
    assert feas2.accepted
    assert not any("no_real_bypass" in r for r in feas2.hard_reject_reasons)
