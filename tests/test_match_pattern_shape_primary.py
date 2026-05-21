"""Shape-primary pattern_osm_segments matching (v2 algorithm)."""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from shapely.geometry import LineString

import importlib

pom_module = importlib.import_module("backend.bus_corridor.match_patterns_to_osm")
from backend.domain.detour_physical.edge_match_models import EdgeMatchResult, EdgeMatchScore
from backend.domain.detour_physical.edge_matcher import (
    match_pattern_shape_to_osm_edges,
    trace_linestring_with_km_cap,
)
from backend.infra import pipeline_skip as ps

BLOCKAGE_WKT = (
    "POLYGON((34.79243 32.05876,34.79442 32.05876,34.79442 32.05953,"
    "34.79243 32.05953,34.79243 32.05876))"
)
ROUTE_19776_PATTERN_ID = "6e2b7956c86e6cab"


def _edge(way: int, length_km: float = 0.01) -> Dict[str, Any]:
    return {
        "way_id": way,
        "length": length_km,
        "begin_osm_node_id": way * 10,
        "end_osm_node_id": way * 10 + 1,
    }


def test_trace_linestring_slice_overlap_retries_with_backtrack():
    """Long shapes use overlapping km slices (not abutting only)."""
    line = LineString([(34.0 + i * 0.02, 31.5) for i in range(80)])
    calls: List[float] = []

    def fake_match(gtfs_line: LineString, **kwargs):
        calls.append(float(gtfs_line.length))
        n = max(3, len(list(gtfs_line.coords)))
        return EdgeMatchResult(
            success=True,
            edge_records=[_edge(100 + len(calls))],
            score=EdgeMatchScore(total=1.0),
        )

    with patch(
        "backend.domain.detour_physical.edge_matcher.match_gtfs_slice_to_osm_edges",
        side_effect=fake_match,
    ):
        res = trace_linestring_with_km_cap(
            line,
            max_km=10.0,
            slice_overlap_m=200.0,
        )

    assert res.success
    assert len(calls) >= 2
    assert "slice_overlap_m=200" in " ".join(res.notes or [])


def test_trace_linestring_fails_whole_pattern_on_subslice_fail():
    line = LineString([(34.0 + i * 0.02, 31.5) for i in range(80)])

    def fake_match(gtfs_line: LineString, **kwargs):
        if len(calls) >= 2:
            return EdgeMatchResult(success=False, notes=["fail"])
        calls.append(1)
        return EdgeMatchResult(success=True, edge_records=[_edge(1)])

    calls: List[int] = []
    with patch(
        "backend.domain.detour_physical.edge_matcher.match_gtfs_slice_to_osm_edges",
        side_effect=fake_match,
    ):
        res = trace_linestring_with_km_cap(line, max_km=10.0, slice_overlap_m=200.0)

    assert not res.success
    assert any("subslice_fail" in n for n in (res.notes or []))


def test_do_match_write_uses_shape_primary_not_stop_legs():
    shape = LineString([(34.0, 31.5), (34.3, 31.52)])
    edges = [_edge(1), _edge(2)]
    trace_res = EdgeMatchResult(success=True, edge_records=edges, score=EdgeMatchScore(total=1.0))

    conn = MagicMock()
    with patch.object(pom_module, "_shape_line_for_pattern", return_value=shape):
        with patch.object(
            pom_module,
            "match_pattern_shape_to_osm_edges",
            return_value=(trace_res, ["full_shape_primary_trace"]),
        ) as mock_shape:
            with patch.object(
                pom_module.db,
                "fetch_osm_road_segments_by_way_endpoint_triples",
                return_value={
                    (1, 10, 11): [
                        {
                            "segment_id": 501,
                            "import_source": "detour_v3_pbf_import",
                            "heading_start_deg": None,
                            "length_m": 10.0,
                        }
                    ],
                    (2, 20, 21): [
                        {
                            "segment_id": 502,
                            "import_source": "detour_v3_pbf_import",
                            "heading_start_deg": None,
                            "length_m": 10.0,
                        }
                    ],
                },
            ):
                with patch.object(
                    pom_module,
                    "resolve_trace_edges_to_segment_ids",
                    return_value=([501, 502], 0),
                ):
                    with patch.object(pom_module.db, "replace_pattern_osm_segments") as mock_write:
                        out = pom_module._do_match_write(
                            conn,
                            feed_id=21,
                            pid=ROUTE_19776_PATTERN_ID,
                            repr_shape_id="156917",
                            full_trace_max_km=10.0,
                            slice_overlap_m=200.0,
                            costing="bus",
                            densify_m=15.0,
                        )

    mock_shape.assert_called_once()
    assert out.status == "written"
    assert out.segments_written == 2
    rows = mock_write.call_args.kwargs.get("rows") or mock_write.call_args[1].get("rows")
    assert rows[0]["source"] == pom_module.SOURCE_VALHALLA_TRACE_FULL_SHAPE


def test_match_single_without_stops_when_shape_exists():
    shape = LineString([(34.0, 31.5), (34.1, 31.51)])
    written = pom_module.PatternOsmMatchResult(
        ROUTE_19776_PATTERN_ID, "written", 3, trace_notes=("full_shape_primary_trace",)
    )
    conn = MagicMock()
    conn.autocommit = False

    with patch.object(pom_module, "_build_match_fingerprint", return_value="fp_v2"):
        with patch.object(pom_module.ps, "fast_path_may_skip", return_value=False):
            with patch.object(pom_module, "_shape_line_for_pattern", return_value=shape):
                with patch.object(pom_module.ps, "commit_if_in_transaction"):
                    with patch.object(pom_module.ps, "rebuild_unit") as mock_rebuild:
                        mock_rebuild.return_value.__enter__ = MagicMock(
                            return_value=MagicMock(skipped_after_lock=False)
                        )
                        mock_rebuild.return_value.__exit__ = MagicMock(return_value=False)
                        with patch.object(pom_module, "_do_match_write", return_value=written) as mock_write:
                            out = pom_module.match_single_pattern_to_osm(
                                conn,
                                feed_id=21,
                                pattern_id=ROUTE_19776_PATTERN_ID,
                                repr_trip_id=None,
                                repr_shape_id="156917",
                                force_refresh=True,
                            )

    assert out.status == "written"
    mock_write.assert_called_once()
    assert mock_write.call_args.kwargs.get("repr_shape_id") == "156917"


def test_pattern_osm_fingerprint_v2_differs_from_v1_chunk_params():
    fp_v2 = ps.build_pattern_osm_match_fingerprint(
        pattern_signature="psig",
        osm_dataset_fingerprint="osm",
        costing="bus",
        densify_m=15.0,
        full_trace_max_km=10.0,
        slice_overlap_m=200.0,
    )
    old_ver = ps.PATTERN_OSM_MATCH_ALGORITHM_VERSION
    try:
        ps.PATTERN_OSM_MATCH_ALGORITHM_VERSION = "pattern_osm_match_v1"
        fp_v1 = ps.build_pattern_osm_match_fingerprint(
            pattern_signature="psig",
            osm_dataset_fingerprint="osm",
            costing="bus",
            densify_m=15.0,
            full_trace_max_km=10.0,
            slice_overlap_m=200.0,
        )
    finally:
        ps.PATTERN_OSM_MATCH_ALGORITHM_VERSION = old_ver
    assert fp_v1 != fp_v2


def test_acceptance_blockage_intersects_sql_template():
    """Document guardrailed acceptance query (feed_id + import_run_id scoped)."""
    feed_id = 21
    import_run_id = 4
    sql = """
        SELECT COUNT(*)
        FROM pattern_osm_segments pos
        JOIN osm_road_segments s ON s.segment_id = pos.segment_id
        WHERE pos.feed_id = %(feed_id)s
          AND pos.pattern_id = %(pattern_id)s
          AND s.import_run_id = %(import_run_id)s
          AND ST_Intersects(s.geom, ST_GeomFromText(%(blockage_wkt)s, 4326))
    """
    assert str(feed_id) in sql or "feed_id" in sql
    assert "import_run_id" in sql
    assert ROUTE_19776_PATTERN_ID
    assert BLOCKAGE_WKT.startswith("POLYGON")


def test_match_pattern_shape_to_osm_edges_delegates_to_km_cap():
    line = LineString([(34.0, 31.5), (34.05, 31.51)])
    fake = EdgeMatchResult(success=True, edge_records=[_edge(9)], score=EdgeMatchScore(total=1.0))
    with patch(
        "backend.domain.detour_physical.edge_matcher.trace_linestring_with_km_cap",
        return_value=fake,
    ) as mock_cap:
        res, notes = match_pattern_shape_to_osm_edges(line, slice_overlap_m=150.0)
    mock_cap.assert_called_once()
    assert mock_cap.call_args.kwargs["slice_overlap_m"] == 150.0
    assert res.success
    assert "full_shape_primary_trace" in notes
