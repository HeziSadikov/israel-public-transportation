from __future__ import annotations

from types import SimpleNamespace

import networkx as nx
import pytest
from shapely.geometry import LineString

from backend.config import parse_bool_env
from backend.detour_graph import (
    DetourGraph,
    DetourGraphBuildError,
    PATTERN_DATA_MISSING,
    POSTGIS_UNAVAILABLE,
    build_detour_graph,
)
from backend.detour_service import DetourComputeError, DetourComputeInput, compute_detour
from backend.graph_builder import EdgeGeometry
import backend.graph_builder as graph_builder_mod


def _tiny_graph(u: str = "a", v: str = "b", sid_u: str = "S1", sid_v: str = "S2") -> tuple[nx.DiGraph, dict]:
    g = nx.DiGraph()
    g.add_node(u, stop_id=sid_u, lat=32.0, lon=34.8, pattern_id="p1")
    g.add_node(v, stop_id=sid_v, lat=32.01, lon=34.81, pattern_id="p1")
    g.add_edge(u, v, travel_time_s=60.0, distance_m=1000.0, weight=60.0)
    ge = {(u, v): EdgeGeometry(from_stop_id=sid_u, to_stop_id=sid_v, linestring=LineString([(34.8, 32.0), (34.81, 32.01)]))}
    return g, ge


def test_parse_bool_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLAG_X", "YeS")
    assert parse_bool_env("FLAG_X", False) is True
    monkeypatch.setenv("FLAG_X", "0")
    assert parse_bool_env("FLAG_X", True) is False
    monkeypatch.delenv("FLAG_X", raising=False)
    assert parse_bool_env("FLAG_X", True) is True


def test_build_detour_graph_postgis_unavailable_when_fallback_off(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.detour_graph as dg

    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    monkeypatch.setattr(dg, "find_routes_in_polygon", lambda **_: [])

    with pytest.raises(DetourGraphBuildError) as exc:
        build_detour_graph(
            feed=object(),
            date_ymd="20260101",
            blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
            primary_route_id="R1",
            primary_direction_id="0",
        )
    assert exc.value.code == POSTGIS_UNAVAILABLE


def test_build_detour_graph_fallback_on_logs_warning(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    import backend.detour_graph as dg

    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", True)
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    monkeypatch.setattr(dg, "find_routes_in_polygon", lambda **_: [])

    class FakePatternBuilder:
        def __init__(self, _feed):
            pass

        def build_patterns_for_route(self, **_kwargs):
            return [
                SimpleNamespace(
                    pattern_id="p1",
                    route_id="R1",
                    direction_id="0",
                    stop_ids=["S1", "S2"],
                    frequency=10,
                    representative_trip_id="T1",
                    representative_shape_id=None,
                )
            ]

        def pick_most_frequent_pattern(self, pats):
            return pats[0]

    class FakeGraphBuilder:
        def __init__(self, _feed):
            pass

        def build_graph_for_pattern(self, _pattern):
            g, ge = _tiny_graph()
            return SimpleNamespace(graph=g, edge_geometries=ge)

    monkeypatch.setattr(dg, "PatternBuilder", FakePatternBuilder)
    monkeypatch.setattr(dg, "GraphBuilder", FakeGraphBuilder)

    caplog.set_level("WARNING")
    res = build_detour_graph(
        feed=object(),
        date_ymd="20260101",
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
        primary_route_id="R1",
        primary_direction_id="0",
    )
    assert isinstance(res, DetourGraph)
    recs = [r for r in caplog.records if r.message == "detour_graph_feed_fallback"]
    assert recs, "expected fallback warning log record"
    rec = recs[0]
    assert getattr(rec, "route_id", None) == "R1"
    assert getattr(rec, "date", None) == "20260101"
    assert getattr(rec, "reason_code", None) == POSTGIS_UNAVAILABLE
    assert getattr(rec, "fallback_used", None) is True


def _input_for_service() -> DetourComputeInput:
    g, ge = _tiny_graph("c1", "c2", "S1", "S2")
    return DetourComputeInput(
        route_id="R1",
        direction_id="0",
        date_str="20260101",
        start_stop_id="S1",
        end_stop_id="S2",
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
        cache_graph=g,
        cache_edge_geometries=ge,
        used_shape=True,
        used_osm_snapping=False,
        feed_version="f1",
        feed=None,
        start_sec=3600,
        end_sec=7200,
    )


def test_compute_detour_maps_postgis_unavailable_to_503(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.detour_service as ds

    monkeypatch.setattr(ds, "astar_route", lambda **_: ["c1", "c2"])
    monkeypatch.setattr(
        ds,
        "build_detour_graph",
        lambda **_: (_ for _ in ()).throw(DetourGraphBuildError(POSTGIS_UNAVAILABLE, "db")),
    )
    with pytest.raises(DetourComputeError) as exc:
        compute_detour(_input_for_service())
    assert exc.value.status_code == 503


def test_compute_detour_maps_pattern_missing_to_409(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.detour_service as ds

    monkeypatch.setattr(ds, "astar_route", lambda **_: ["c1", "c2"])
    monkeypatch.setattr(
        ds,
        "build_detour_graph",
        lambda **_: (_ for _ in ()).throw(DetourGraphBuildError(PATTERN_DATA_MISSING, "missing")),
    )
    with pytest.raises(DetourComputeError) as exc:
        compute_detour(_input_for_service())
    assert exc.value.status_code == 409


def test_compute_detour_threads_window_uses_resolver_and_detour_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.detour_service as ds

    inp = _input_for_service()
    detour_g, detour_ge = _tiny_graph("d1", "d2", "S1", "S2")
    captured: dict = {"build": None, "astar_calls": []}

    class FakeDetourGraph:
        def __init__(self):
            self.graph = detour_g
            self.edge_geometries = detour_ge

        def resolve_endpoint(self, stop_id: str, *, prefer_primary: bool = True):
            return "d1" if stop_id == "S1" else "d2"

    def fake_build_detour_graph(**kwargs):
        captured["build"] = kwargs
        return FakeDetourGraph()

    def fake_astar_route(**kwargs):
        captured["astar_calls"].append(kwargs)
        return ["c1", "c2"] if kwargs["graph"] is inp.cache_graph else ["d1", "d2"]

    monkeypatch.setattr(ds, "build_detour_graph", fake_build_detour_graph)
    monkeypatch.setattr(ds, "compute_blocked_edges", lambda **_: ({("d1", "d2")}, {"type": "FeatureCollection", "features": []}))
    monkeypatch.setattr(ds, "collect_path_geojson", lambda **_: {"type": "FeatureCollection", "features": []})
    monkeypatch.setattr(ds, "astar_route", fake_astar_route)

    res = compute_detour(inp)
    assert res.blocked_edges_count == 1
    assert captured["build"]["start_sec"] == 3600
    assert captured["build"]["end_sec"] == 7200
    assert len(captured["astar_calls"]) == 2
    # baseline call
    assert captured["astar_calls"][0]["blocked_edges"] == set()
    # detour call: blocked set comes only from detour graph computation
    assert captured["astar_calls"][1]["blocked_edges"] == {("d1", "d2")}


def test_build_detour_graph_postgis_uses_precomputed_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Smoke/regression:
    When PostGIS is available, `build_detour_graph` must assemble the local graph from
    precomputed `pattern_nodes` / `pattern_edges` instead of rebuilding per-pattern graphs.
    """
    import backend.detour_graph as dg

    # If legacy on-demand per-pattern graph building is called, we want the test to fail.
    def _fail_build_graph_for_pattern(*_args, **_kwargs):
        raise AssertionError("build_graph_for_pattern_from_postgis must not be called")

    monkeypatch.setattr(graph_builder_mod, "build_graph_for_pattern_from_postgis", _fail_build_graph_for_pattern)

    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)

    # Candidate routes returned by AOI lookup (keep it small).
    monkeypatch.setattr(
        dg, "find_routes_in_polygon", lambda **_kwargs: [{"route_id": "R2", "direction_id": "0"}]
    )

    # PostGIS available + Top-K selection returns primary and secondary patterns.
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        dg.db_access,
        "get_top_patterns_for_routes",
        lambda **_kwargs: {
            ("R1", "0"): [SimpleNamespace(pattern_id="pat_R1_0")],
            ("R2", "0"): [SimpleNamespace(pattern_id="pat_R2_0")],
        },
    )

    # Provide a minimal precomputed ride graph for two patterns.
    from shapely.geometry import LineString

    def _fake_get_pattern_nodes_bulk(pattern_ids: list[str], *_args, **_kwargs):
        out = []
        for pid in pattern_ids:
            # Two stops per pattern: S1 -> S2
            n1 = f"{pid}:S1:0"
            n2 = f"{pid}:S2:1"
            out.extend(
                [
                    {
                        "node_id": n1,
                        "pattern_id": pid,
                        "route_id": "R1",
                        "direction_id": "0",
                        "stop_id": "S1",
                        "stop_sequence": 0,
                        "lat": 32.0,
                        "lon": 34.8,
                        "out_heading_deg": 90.0,
                        "frequency": 10,
                    },
                    {
                        "node_id": n2,
                        "pattern_id": pid,
                        "route_id": "R1",
                        "direction_id": "0",
                        "stop_id": "S2",
                        "stop_sequence": 1,
                        "lat": 32.0005,
                        "lon": 34.8005,
                        "out_heading_deg": 90.0,
                        "frequency": 10,
                    },
                ]
            )
        return out

    def _fake_get_pattern_edges_bulk(pattern_ids: list[str], *_args, **_kwargs):
        out = []
        ls = LineString([(34.8, 32.0), (34.8005, 32.0005)])
        for pid in pattern_ids:
            out.append(
                {
                    "pattern_id": pid,
                    "from_node_id": f"{pid}:S1:0",
                    "to_node_id": f"{pid}:S2:1",
                    "from_stop_id": "S1",
                    "to_stop_id": "S2",
                    "travel_time_s": 60.0,
                    "distance_m": 1000.0,
                    "linestring": ls,
                }
            )
        return out

    monkeypatch.setattr(dg.db_access, "get_pattern_nodes_bulk", _fake_get_pattern_nodes_bulk)
    monkeypatch.setattr(dg.db_access, "get_pattern_edges_bulk", _fake_get_pattern_edges_bulk)

    res = dg.build_detour_graph(
        feed=object(),
        date_ymd="20260101",
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
        primary_route_id="R1",
        primary_direction_id="0",
        start_sec=None,
        end_sec=None,
    )

    assert res.primary_pattern_id == "pat_R1_0"
    # Both patterns should be present (rides + potential transfers).
    assert "pat_R1_0:S1:0" in res.graph.nodes
    assert "pat_R1_0:S2:1" in res.graph.nodes
    assert ("pat_R1_0:S1:0", "pat_R1_0:S2:1") in res.edge_geometries


def test_build_detour_graph_threads_topk_and_window(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.detour_graph as dg

    captured: dict = {}
    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)
    monkeypatch.setattr(dg, "DETOUR_TOP_K_PATTERNS", 2)
    monkeypatch.setattr(dg, "find_routes_in_polygon", lambda **_kwargs: [{"route_id": "R2", "direction_id": "1"}])
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_args, **_kwargs: 1)

    def _fake_selector(**kwargs):
        captured["kwargs"] = kwargs
        return {("R1", "0"): [SimpleNamespace(pattern_id="pat_R1_0")]}

    monkeypatch.setattr(dg.db_access, "get_top_patterns_for_routes", _fake_selector)
    monkeypatch.setattr(dg.db_access, "get_pattern_nodes_bulk", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(dg.db_access, "get_pattern_edges_bulk", lambda *_args, **_kwargs: [])

    dg.build_detour_graph(
        feed=object(),
        date_ymd="20260101",
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
        primary_route_id="R1",
        primary_direction_id="0",
        start_sec=3600,
        end_sec=7200,
    )

    selector_args = captured["kwargs"]
    assert selector_args["k_per_route_dir"] == 2
    assert selector_args["start_sec"] == 3600
    assert selector_args["end_sec"] == 7200
    assert selector_args["direction_filter_by_route"] == {"R1": "0"}
