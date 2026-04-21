"""Unit tests for legal anchor index (Valhalla edge walk + scoring)."""

from __future__ import annotations

from backend.domain.detour_physical.legal_anchor_index import (
    LegalAnchorOsmCaches,
    _is_corridor_continuation,
    merge_and_rank_records,
    score_legal_exit_intersection,
)
from backend.domain.detour_v2.legal_anchor_runtime import select_legal_anchor_pair
from backend.domain.detour_v2.policy import DetourPolicyConfig, get_default_policy


def test_continuation_detected_by_heading():
    nxt = {"begin_heading": 90.0}
    inter = {"begin_heading": 90.0, "to_edge_name_consistency": False}
    assert _is_corridor_continuation(inter, nxt) is True
    inter2 = {"begin_heading": 200.0, "to_edge_name_consistency": False}
    assert _is_corridor_continuation(inter2, nxt) is False


def test_to_edge_name_consistency_implies_continuation():
    inter = {"to_edge_name_consistency": True, "begin_heading": 0.0}
    assert _is_corridor_continuation(inter, {"begin_heading": 180.0}) is True


def test_score_rejects_non_drivable():
    inter = {"begin_heading": 10.0, "road_class": "primary", "driveability": "backward", "use": "road"}
    assert score_legal_exit_intersection(inter, next_edge={"begin_heading": 200.0}, incoming_way_id=1, end_osm_node_id=2, conn=None) is None


def test_merge_and_rank_assigns_roles():
    ex = [{"shape_dist_m": 10.0, "score": 2.0, "lon": 34.0, "lat": 32.0, "osm_node_id": 1}]
    rj = [{"shape_dist_m": 500.0, "score": 2.0, "lon": 34.1, "lat": 32.0, "osm_node_id": 2}]
    a, b = merge_and_rank_records(ex, rj, per_role_limit=5)
    assert len(a) == 1 and a[0]["role"] == "exit" and a[0]["rank_in_role"] == 0
    assert len(b) == 1 and b[0]["role"] == "rejoin"


def test_select_legal_anchor_pair_smoke():
    pol = get_default_policy()
    ex_rows = [
        {
            "role": "exit",
            "shape_dist_m": 50.0,
            "lon": 34.0,
            "lat": 32.0,
            "score": 3.0,
        }
    ]
    rj_rows = [
        {
            "role": "rejoin",
            "shape_dist_m": 400.0,
            "lon": 34.2,
            "lat": 32.0,
            "score": 3.0,
        }
    ]
    pair = select_legal_anchor_pair(
        ex_rows,
        rj_rows,
        blocked_start_m=100.0,
        blocked_end_m=200.0,
        total_shape_m=800.0,
        policy=pol,
    )
    assert pair is not None
    assert pair.exit_shape_dist_m == 50.0
    assert pair.rejoin_shape_dist_m == 400.0
    assert pair.anchor_source == "legal_index"


def test_policy_loader_has_anchor_windows():
    cfg = DetourPolicyConfig()
    assert cfg.anchor.search_before_window_m > 0


def test_score_turn_rejection_matches_cache():
    caches = LegalAnchorOsmCaches(
        outgoing_by_node={1: [{"osm_way_id": 100, "heading_start_deg": 90.0}]},
        forbidden_tos={(50, 1): frozenset({100})},
    )
    inter = {
        "begin_heading": 90.0,
        "road_class": "primary",
        "driveability": "forward",
        "use": "road",
        "to_edge_name_consistency": False,
    }
    assert (
        score_legal_exit_intersection(
            inter,
            next_edge={"begin_heading": 0.0},
            incoming_way_id=50,
            end_osm_node_id=1,
            caches=caches,
            conn=None,
        )
        is None
    )


def test_score_cache_allows_when_not_restricted():
    caches = LegalAnchorOsmCaches(
        outgoing_by_node={1: [{"osm_way_id": 100, "heading_start_deg": 90.0}]},
        forbidden_tos={(50, 1): frozenset({999})},
    )
    inter = {
        "begin_heading": 90.0,
        "road_class": "primary",
        "driveability": "forward",
        "use": "road",
        "to_edge_name_consistency": False,
    }
    sc = score_legal_exit_intersection(
        inter,
        next_edge={"begin_heading": 0.0},
        incoming_way_id=50,
        end_osm_node_id=1,
        caches=caches,
        conn=None,
    )
    assert sc is not None and sc > 0


def test_merge_rank_unchanged_with_cache_build_path():
    ex = [{"shape_dist_m": 10.0, "score": 2.0, "lon": 34.0, "lat": 32.0, "osm_node_id": 1}]
    rj = [{"shape_dist_m": 500.0, "score": 2.0, "lon": 34.1, "lat": 32.0, "osm_node_id": 2}]
    a, b = merge_and_rank_records(ex, rj, per_role_limit=24)
    assert len(a) == 1 and len(b) == 1
