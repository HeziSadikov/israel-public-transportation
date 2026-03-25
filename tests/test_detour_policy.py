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
