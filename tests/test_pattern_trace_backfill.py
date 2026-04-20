"""Unit tests for pattern-level trace split (no Valhalla)."""

from __future__ import annotations

from typing import List
from unittest.mock import patch

from shapely.geometry import LineString, Point

from backend.domain.detour_physical.edge_match_models import EdgeMatchResult, EdgeMatchScore
from backend.domain.detour_physical.edge_matcher import trace_pattern_split_to_legs
from backend.domain.detour_physical.pattern_trace_split import (
    cumulative_stop_meters_along_shape,
    ensure_linestring,
    extract_edges_for_shape_fractions,
    slice_shape_between_stop_indices,
)


def _edge(way: int, length_km: float) -> dict:
    return {"way_id": way, "length": length_km, "road_class": "primary"}


def test_ensure_linestring_coerces_point():
    g = ensure_linestring(Point(34.0, 31.5))
    assert g.geom_type == "LineString"
    assert len(g.coords) == 2


def test_slice_shape_never_returns_point():
    """Degenerate distance slice must still be a LineString for PostGIS."""
    line = LineString([(34.0, 31.5), (34.2, 31.52)])
    stops = [
        {"shape_dist_traveled": 100.0},
        {"shape_dist_traveled": 100.0},
    ]
    sl = slice_shape_between_stop_indices(line, stops, 0, 1)
    assert sl.geom_type == "LineString"


def test_extract_edges_for_shape_fractions_mid_slice():
    edges = [_edge(1, 0.1), _edge(2, 0.1), _edge(3, 0.1)]
    sub = extract_edges_for_shape_fractions(edges, 1.0 / 3.0, 2.0 / 3.0)
    assert len(sub) >= 1
    assert sub[0]["way_id"] == 2


def test_cumulative_stop_meters_monotonic():
    line = LineString([(34.0, 31.5), (34.1, 31.6)])
    stops = [
        {"shape_dist_traveled": 0.0},
        {"shape_dist_traveled": 50.0},
        {"shape_dist_traveled": 40.0},
    ]
    cum, s_use = cumulative_stop_meters_along_shape(line, stops)
    assert cum[0] <= cum[1] <= cum[2]
    assert s_use > 0


def test_trace_pattern_split_full_shape_mock():
    """Single full trace: leg fractions map to edge subsequences."""
    line = LineString([(34.0, 31.5), (34.2, 31.52)])
    stops = [
        {"stop_id": "a", "stop_sequence": 1, "shape_dist_traveled": 0.0},
        {"stop_id": "b", "stop_sequence": 2, "shape_dist_traveled": 500.0},
        {"stop_id": "c", "stop_sequence": 3, "shape_dist_traveled": 1000.0},
    ]
    long_edges = [_edge(10, 0.05) for _ in range(20)]

    def fake_match(line_in: LineString, **kwargs):
        return EdgeMatchResult(
            success=True,
            edge_records=long_edges,
            score=EdgeMatchScore(coverage_ratio=0.9, total=1.0),
            is_ambiguous=False,
            notes=["ok"],
        )

    with patch(
        "backend.domain.detour_physical.edge_matcher.match_full_shape_to_osm_edges",
        side_effect=fake_match,
    ):
        per_leg, notes = trace_pattern_split_to_legs(
            line,
            stops,
            full_trace_max_km=100.0,
            chunk_legs=5,
            chunk_overlap=1,
        )
    assert "full_shape_trace" in notes
    assert len(per_leg) == 2
    assert per_leg[0] and per_leg[1]
    assert all(e.get("way_id") == 10 for e in per_leg[0])


def test_trace_pattern_split_chunks_when_long_mock():
    line = LineString([(34.0 + i * 0.01, 31.5) for i in range(50)])
    n_stops = 25
    stops = [
        {"stop_id": str(i), "stop_sequence": i + 1, "shape_dist_traveled": float(i * 100)}
        for i in range(n_stops)
    ]
    chunk_edges = [_edge(99, 0.02) for _ in range(15)]

    def fake_match(line_in: LineString, **kwargs):
        return EdgeMatchResult(
            success=True,
            edge_records=chunk_edges,
            score=EdgeMatchScore(coverage_ratio=0.85, total=1.0),
            is_ambiguous=False,
            notes=["ok"],
        )

    with patch(
        "backend.domain.detour_physical.edge_matcher.match_full_shape_to_osm_edges",
        side_effect=fake_match,
    ):
        per_leg, notes = trace_pattern_split_to_legs(
            line,
            stops,
            full_trace_max_km=0.001,
            chunk_legs=8,
            chunk_overlap=1,
        )
    assert "chunked_trace" in notes


def test_trace_pattern_split_skips_unneeded_chunks_for_resume():
    """Only Valhalla-call chunks that cover legs still missing from DB."""
    line = LineString([(34.0 + i * 0.01, 31.5) for i in range(50)])
    n_stops = 25
    stops = [
        {"stop_id": str(i), "stop_sequence": i + 1, "shape_dist_traveled": float(i * 100)}
        for i in range(n_stops)
    ]
    chunk_edges = [_edge(99, 0.02) for _ in range(15)]
    calls: List[int] = []

    def fake_match(line_in: LineString, **kwargs):
        calls.append(1)
        return EdgeMatchResult(
            success=True,
            edge_records=chunk_edges,
            score=EdgeMatchScore(coverage_ratio=0.85, total=1.0),
            is_ambiguous=False,
            notes=["ok"],
        )

    with patch(
        "backend.domain.detour_physical.edge_matcher.match_full_shape_to_osm_edges",
        side_effect=fake_match,
    ):
        trace_pattern_split_to_legs(
            line,
            stops,
            full_trace_max_km=0.001,
            chunk_legs=8,
            chunk_overlap=1,
            only_leg_indices={20},
        )
    assert len(calls) == 1

    calls.clear()
    with patch(
        "backend.domain.detour_physical.edge_matcher.match_full_shape_to_osm_edges",
        side_effect=fake_match,
    ):
        trace_pattern_split_to_legs(
            line,
            stops,
            full_trace_max_km=0.001,
            chunk_legs=8,
            chunk_overlap=1,
        )
    assert len(calls) >= 2
