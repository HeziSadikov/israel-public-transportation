"""Pure tests for v3 anchors + corridor cost helpers."""

from __future__ import annotations

from backend.routing.anchors import anchor_pairs_for_pattern_blocks, iter_blocked_ranges
from backend.routing.costs import bus_corridor_segment_enter_penalty_m


def test_iter_blocked_ranges_splits_islands():
    pat = [1, 2, 3, 4, 5]
    banned = {2, 3, 5}
    assert iter_blocked_ranges(pat, banned) == [(1, 2), (4, 4)]


def test_anchor_pairs_skips_touching_route_ends():
    pat = [10, 11, 99, 100, 101, 102]
    banned = {99, 100}
    anchors = anchor_pairs_for_pattern_blocks(pat, banned)
    assert len(anchors) == 1
    assert anchors[0].exit_segment_id == 11
    assert anchors[0].rejoin_segment_id == 101


def test_bus_corridor_strict_bans_unknown_segment():
    assert bus_corridor_segment_enter_penalty_m(
        7,
        "primary",
        observed_segment_ids={1, 2},
        mode="strict_bus_corridor",
    ) >= 1e17


def test_bus_corridor_plus_prefers_observed_over_connector():
    p_obs = bus_corridor_segment_enter_penalty_m(
        1,
        "service",
        observed_segment_ids={1},
        mode="bus_corridor_plus_connectors",
    )
    p_conn = bus_corridor_segment_enter_penalty_m(
        99,
        "secondary",
        observed_segment_ids={1},
        mode="bus_corridor_plus_connectors",
    )
    assert p_obs == 0.0
    assert p_conn > 0.0
