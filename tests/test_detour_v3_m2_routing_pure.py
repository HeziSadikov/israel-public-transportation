"""Pure-unit tests for M2 routing helpers (no DB required)."""

from __future__ import annotations

from backend.routing.astar import astar_shortest_path
from backend.routing.costs import (
    DEFAULT_ROUTING_COST_PROFILE,
    move_cost_m,
    normalize_heading_delta_deg,
)
from backend.routing.road_graph_loader import RoadGraph, RoadSegment, TurnMove


def test_normalize_heading_delta_deg_wrap_and_sign():
    assert normalize_heading_delta_deg(0.0, 90.0) == 90.0
    assert abs(normalize_heading_delta_deg(180.0, 0.0) - (-180.0)) < 1e-9


def test_move_cost_penalizes_sharp_turn():
    p = DEFAULT_ROUTING_COST_PROFILE
    assert move_cost_m(100.0, None, profile=p) == 100.0
    steep = float(p.sharp_turn_threshold_deg) + 5.0
    assert move_cost_m(100.0, steep, profile=p) > 100.0


def test_astar_straight_line_three_segments():
    """
    Nodes 10 --seg1--> 20 --seg2--> 30 --seg3--> 40
    Directed graph aligned with successors.
    """
    segs = {
        1: RoadSegment(
            segment_id=1,
            from_node_id=10,
            to_node_id=20,
            length_m=10.0,
            highway=None,
        ),
        2: RoadSegment(
            segment_id=2,
            from_node_id=20,
            to_node_id=30,
            length_m=20.0,
            highway=None,
        ),
        3: RoadSegment(
            segment_id=3,
            from_node_id=30,
            to_node_id=40,
            length_m=15.0,
            highway=None,
        ),
    }
    successors = {
        1: (TurnMove(2, None),),
        2: (TurnMove(3, None),),
        3: tuple(),
    }
    geom = {
        10: (34.0, 31.9),
        20: (34.01, 31.901),
        30: (34.02, 31.902),
        40: (34.03, 31.903),
    }
    g = RoadGraph(segments=segs, node_geom=geom, successors=successors)
    path = astar_shortest_path(g, 10, 40)
    assert path == [1, 2, 3]


def test_astar_disconnected_returns_none():
    segs = {
        11: RoadSegment(11, 1, 2, 1.0, None),
        22: RoadSegment(22, 99, 100, 2.0, None),
    }
    successors = {11: (), 22: ()}
    g = RoadGraph(segments=segs, node_geom={1: (0.0, 0.0), 100: (1.0, 1.0)}, successors=successors)
    assert astar_shortest_path(g, 1, 100) is None


def test_astar_empty_start_raises_no_start_deg():
    g = RoadGraph(segments={}, node_geom={}, successors={})
    assert astar_shortest_path(g, 1, 2) is None


def test_astar_banned_middle_segment_unreachable():
    """Chain 10→20→30→40 ; ban middle segment with no bypass."""
    segs = {
        1: RoadSegment(1, 10, 20, 10.0, None),
        2: RoadSegment(2, 20, 30, 10.0, None),
        3: RoadSegment(3, 30, 40, 10.0, None),
    }
    successors = {1: (TurnMove(2, None),), 2: (TurnMove(3, None),), 3: ()}
    g = RoadGraph(
        segments=segs,
        node_geom={10: (0.0, 0.0), 20: (0.1, 0.0), 30: (0.2, 0.0), 40: (0.3, 0.0)},
        successors=successors,
    )
    assert astar_shortest_path(g, 10, 40, banned_segment_ids={2}) is None


def test_astar_banned_segment_parallel_detour():
    """
           ┌→ 4 → 5 ┐ via hub node 50
    10 → 20          → 30 → 40
           └→ 2 (straight, banned)

    Ban segment 2; expect 1-4-5-6.
    """
    segs = {
        1: RoadSegment(1, 10, 20, 10.0, None),
        2: RoadSegment(2, 20, 30, 50.0, None),
        4: RoadSegment(4, 20, 50, 12.0, None),
        5: RoadSegment(5, 50, 30, 12.0, None),
        6: RoadSegment(6, 30, 40, 10.0, None),
    }
    successors = {
        1: (TurnMove(2, None), TurnMove(4, None)),
        2: (TurnMove(6, None),),
        4: (TurnMove(5, None),),
        5: (TurnMove(6, None),),
        6: (),
    }
    g = RoadGraph(
        segments=segs,
        node_geom={
            10: (0.0, 0.0),
            20: (1.0, 0.0),
            30: (3.0, 0.0),
            40: (4.0, 0.0),
            50: (2.0, 0.1),
        },
        successors=successors,
    )
    path = astar_shortest_path(g, 10, 40, banned_segment_ids=frozenset({2}))
    assert path == [1, 4, 5, 6]


def test_astar_banned_turn_skips_specific_move():
    """Same topology as parallel detour but ban move (1,2) instead of banning seg 2 globally."""
    segs = {
        1: RoadSegment(1, 10, 20, 10.0, None),
        2: RoadSegment(2, 20, 30, 50.0, None),
        4: RoadSegment(4, 20, 50, 12.0, None),
        5: RoadSegment(5, 50, 30, 12.0, None),
        6: RoadSegment(6, 30, 40, 10.0, None),
    }
    successors = {
        1: (TurnMove(2, None), TurnMove(4, None)),
        2: (TurnMove(6, None),),
        4: (TurnMove(5, None),),
        5: (TurnMove(6, None),),
        6: (),
    }
    g = RoadGraph(
        segments=segs,
        node_geom={
            10: (0.0, 0.0),
            20: (1.0, 0.0),
            30: (3.0, 0.0),
            40: (4.0, 0.0),
            50: (2.0, 0.1),
        },
        successors=successors,
    )
    path = astar_shortest_path(g, 10, 40, banned_turn_pairs={(1, 2)})
    assert path == [1, 4, 5, 6]


def test_polygon_geojson_polygon_to_wkt():
    from backend.routing.blockers import polygon_geojson_feature_to_wkt

    gj = {
        "type": "Polygon",
        "coordinates": [
            [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
        ],
    }
    wkt_out = polygon_geojson_feature_to_wkt(gj)
    assert str(wkt_out).upper().startswith("POLYGON")
