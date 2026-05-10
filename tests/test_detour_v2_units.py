"""Unit tests for detour v2 policy, ranking, and shape impact (no DB)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from shapely.geometry import LineString, box

from backend.domain.detour_v2.bus_feasibility_evaluator import evaluate_candidate
from backend.domain.detour_v2.anchor_selector import enumerate_anchor_candidates
from backend.domain.detour_v2.candidate_decoder import decode_polyline_to_synthetic_segments
from backend.domain.detour_v2.detour_ranker import rank_candidates
from backend.domain.detour_v2.incident_projector import IncidentProjection
from backend.domain.detour_v2.models import DecodedCandidate, FeasibilityResult, RoadSegmentRef
from backend.domain.detour_v2.policy import DetourPolicyConfig
from backend.domain.detour_v2.road_candidate_generator import generate_candidates_with_debug
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
    """High residential share must not hard-reject unless vehicle.reject_hard_local_share is True."""
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
    pol = DetourPolicyConfig()
    feas = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=1000.0,
        detour_time_s=100.0,
        banned_way_ids=set(),
    )
    assert feas.accepted
    assert "local_fraction_exceeded" not in feas.hard_reject_reasons

    pol.vehicle.reject_hard_local_share = True
    feas2 = evaluate_candidate(
        decoded=dec,
        projection=IncidentProjection(),
        policy=pol,
        baseline_blocked_distance_m=100.0,
        baseline_blocked_time_s=20.0,
        detour_distance_m=1000.0,
        detour_time_s=100.0,
        banned_way_ids=set(),
    )
    # Local share is scored (penalty + warnings), not an absolute hard reject.
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
    pol = DetourPolicyConfig()
    pol.vehicle.require_gtfs_way_evidence = True  # enable strict mode explicitly
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
    assert any(w.startswith("not_in_gtfs_bus_corridor_123") for w in feas.warnings)
    assert feas.accepted


def test_feasibility_strict_accepts_way_with_gtfs_evidence():
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
    assert not any(r.startswith("not_in_gtfs_bus_corridor_321") for r in feas.hard_reject_reasons)


def test_feasibility_valhalla_uturn_maneuver_type_float_still_detected_when_hard_reject_enabled():
    pol = DetourPolicyConfig()
    pol.service.reject_explicit_u_turn_maneuver = True
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
    assert feas.turn_penalty_s > 0


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
