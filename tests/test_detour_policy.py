from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import networkx as nx
import pytest
from shapely.geometry import LineString, shape

from backend.infra.config import parse_bool_env
from backend.domain.detour_graph import (
    DetourGraph,
    DetourGraphBuildError,
    PATTERN_DATA_MISSING,
    POSTGIS_UNAVAILABLE,
    build_detour_graph,
)
from backend.domain.detour_service import (
    DetourComputeError,
    DetourComputeInput,
    DetourComputeResult,
    compute_detour,
    compute_detour_with_strategies,
)
from backend.domain.router_core import astar_route, dijkstra_best_route, dfs_best_route
from backend.domain.graph_builder import EdgeGeometry
from backend.domain.routing_policy import RoutingPolicy, default_by_area_routing_policy
from backend.adapters.osm_detour import OSMFeasibilityResult
import backend.domain.graph_builder as graph_builder_mod


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
    import backend.domain.detour_graph as dg

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
    assert exc.value.code == PATTERN_DATA_MISSING


def test_build_detour_graph_no_feed_fallback_when_spatial_selector_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.domain.detour_graph as dg

    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", True)
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_a, **_k: 1)
    monkeypatch.setattr(dg, "find_routes_in_polygon", lambda **_: [])
    monkeypatch.setattr(
        dg.db_access,
        "get_detour_patterns_for_routes",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("selector failed")),
    )
    with pytest.raises(DetourGraphBuildError) as exc:
        build_detour_graph(
            feed=object(),
            date_ymd="20260101",
            blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
            primary_route_id="R1",
            primary_direction_id="0",
        )
    assert exc.value.code == PATTERN_DATA_MISSING
    assert "spatial pattern index" in str(exc.value)


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
    import backend.domain.detour_service as ds

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
    import backend.domain.detour_service as ds

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
    import backend.domain.detour_service as ds

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


def test_compute_detour_instructions_only_when_astar_fails_with_hebrew_text(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.domain.detour_service as ds

    inp = _input_for_service()
    inp.instructions_text_he = "פנה שמאלה, ישר"
    detour_g, detour_ge = _tiny_graph("d1", "d2", "S1", "S2")

    class FakeDetourGraph:
        def __init__(self) -> None:
            self.graph = detour_g
            self.edge_geometries = detour_ge

        def resolve_endpoint(self, stop_id: str, *, prefer_primary: bool = True):
            return "d1" if stop_id == "S1" else "d2"

    calls = {"n": 0}

    def fake_astar_route(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return ["c1", "c2"]
        raise RuntimeError("no detour path")

    monkeypatch.setattr(ds, "build_detour_graph", lambda **_kwargs: FakeDetourGraph())
    monkeypatch.setattr(
        ds,
        "compute_blocked_edges",
        lambda **_: (set(), {"type": "FeatureCollection", "features": []}),
    )
    monkeypatch.setattr(ds, "astar_route", fake_astar_route)

    res = compute_detour(inp)
    assert res.instructions_only is True
    assert res.path_geojson.get("features") == []
    assert res.turn_by_turn and [s["instruction_he"] for s in res.turn_by_turn] == ["פנה שמאלה", "ישר"]
    assert res.from_override is False
    assert res.reason_code == "instructions_only_fallback"
    assert res.strategy_used == "instructions_only"


def test_build_detour_graph_postgis_uses_precomputed_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Smoke/regression:
    When PostGIS is available, `build_detour_graph` must assemble the local graph from
    precomputed `pattern_nodes` / `pattern_edges` instead of rebuilding per-pattern graphs.
    """
    import backend.domain.detour_graph as dg

    # If legacy on-demand per-pattern graph building is called, we want the test to fail.
    def _fail_build_graph_for_pattern(*_args, **_kwargs):
        raise AssertionError("build_graph_for_pattern_from_postgis must not be called")

    monkeypatch.setattr(graph_builder_mod, "build_graph_for_pattern_from_postgis", _fail_build_graph_for_pattern)

    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)

    # Candidate routes returned by AOI lookup (keep it small).
    monkeypatch.setattr(
        dg, "find_routes_in_polygon", lambda **_kwargs: [{"route_id": "R2", "direction_id": "0"}]
    )

    # PostGIS available + spatial selection returns primary and secondary patterns.
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        dg.db_access,
        "get_detour_patterns_for_routes",
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


def test_build_detour_graph_threads_spatial_selector_and_window(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.domain.detour_graph as dg

    captured: dict = {}
    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)
    monkeypatch.setattr(dg, "DETOUR_TOP_K_PATTERNS_SPATIAL", 2)
    monkeypatch.setattr(dg, "DETOUR_SPATIAL_MIN_OVERLAP_M", 9)
    monkeypatch.setattr(dg, "find_routes_in_polygon", lambda **_kwargs: [{"route_id": "R2", "direction_id": "1"}])
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_args, **_kwargs: 1)

    def _fake_selector(**kwargs):
        captured["kwargs"] = kwargs
        return {("R1", "0"): [SimpleNamespace(pattern_id="pat_R1_0")]}

    monkeypatch.setattr(dg.db_access, "get_detour_patterns_for_routes", _fake_selector)
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
    assert selector_args["min_overlap_m"] == 9.0
    assert selector_args["aoi_geojson"]["type"] in {"Polygon", "MultiPolygon"}


def test_build_detour_graph_fails_when_spatial_selector_returns_no_patterns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import backend.domain.detour_graph as dg

    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)
    monkeypatch.setattr(dg, "find_routes_in_polygon", lambda **_kwargs: [{"route_id": "R2", "direction_id": "1"}])
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(dg.db_access, "get_detour_patterns_for_routes", lambda **_kwargs: {})

    with pytest.raises(DetourGraphBuildError) as exc:
        dg.build_detour_graph(
            feed=object(),
            date_ymd="20260101",
            blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
            primary_route_id="R1",
            primary_direction_id="0",
            start_sec=3600,
            end_sec=7200,
        )
    assert exc.value.code == PATTERN_DATA_MISSING
    assert "AOI-overlapping patterns" in str(exc.value)


def test_build_detour_graph_aoi_unions_replaced_segment_corridor(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.domain.detour_graph as dg

    seen: list = []

    def fake_find(*, polygon_geojson, **kwargs):
        seen.append(polygon_geojson)
        return [{"route_id": "RX", "direction_id": "0"}]

    monkeypatch.setattr(dg, "find_routes_in_polygon", fake_find)
    monkeypatch.setattr(dg, "DETOUR_ALLOW_FEED_FALLBACK", False)
    monkeypatch.setattr(dg.db_access, "get_active_feed_id", lambda *_a, **_k: 1)

    def _fake_patterns(**kwargs):
        return {("R1", "0"): [SimpleNamespace(pattern_id="pat_R1")]}

    monkeypatch.setattr(dg.db_access, "get_detour_patterns_for_routes", _fake_patterns)
    monkeypatch.setattr(dg.db_access, "get_pattern_nodes_bulk", lambda *_a, **_k: [])
    monkeypatch.setattr(dg.db_access, "get_pattern_edges_bulk", lambda *_a, **_k: [])

    blockage = {"type": "Point", "coordinates": [34.8, 32.0]}
    replaced = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[35.5, 32.5], [35.6, 32.6]],
                },
                "properties": {},
            }
        ],
    }
    dg.build_detour_graph(
        feed=object(),
        date_ymd="20260101",
        blockage_geojson=blockage,
        primary_route_id="R1",
        primary_direction_id="0",
        replaced_segment_geojson=replaced,
    )
    assert len(seen) == 1
    aoi = shape(seen[0])
    assert aoi.area > 0.0001


def test_default_by_area_routing_policy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BY_AREA_ROUTING_PER_EDGE_PENALTY_S", "21")
    monkeypatch.setenv("BY_AREA_ROUTING_TRANSFER_PENALTY_S", "430")
    monkeypatch.setenv("BY_AREA_ROUTING_TRANSFER_DISTANCE_PENALTY_PER_M_S", "0.5")
    monkeypatch.setenv("BY_AREA_ROUTING_PATTERN_SWITCH_PENALTY_S", "140")
    pol = default_by_area_routing_policy()
    assert pol.per_edge_penalty_s == 21.0
    assert pol.transfer_penalty_s == 430.0
    assert pol.transfer_distance_penalty_per_m_s == 0.5
    assert pol.pattern_switch_penalty_s == 140.0


def test_astar_route_weighted_prefers_neater_path() -> None:
    g = nx.DiGraph()
    g.add_node("a", lat=32.0, lon=34.8, pattern_id="p1", frequency=0)
    g.add_node("b", lat=32.001, lon=34.801, pattern_id="p1", frequency=0)
    g.add_node("c", lat=32.002, lon=34.802, pattern_id="p2", frequency=0)
    g.add_node("d", lat=32.003, lon=34.803, pattern_id="p1", frequency=0)

    # Direct branch has lower base travel_time but requires long transfer + pattern switch.
    g.add_edge("a", "c", travel_time_s=30.0, distance_m=250.0, is_transfer=True)
    g.add_edge("c", "d", travel_time_s=20.0, distance_m=50.0, is_transfer=False)
    # Neater branch stays on same pattern with no transfer.
    g.add_edge("a", "b", travel_time_s=40.0, distance_m=100.0, is_transfer=False)
    g.add_edge("b", "d", travel_time_s=30.0, distance_m=100.0, is_transfer=False)

    pol = RoutingPolicy(
        per_edge_penalty_s=0.0,
        transfer_penalty_s=100.0,
        transfer_distance_penalty_per_m_s=0.4,
        pattern_switch_penalty_s=100.0,
        frequency_discount_coef=0.0,
        frequency_cap=60.0,
        heuristic_max_speed_m_s=22.22,
    )
    path = astar_route(
        graph=g,
        edge_geometries={},
        start_node_id="a",
        end_node_id="d",
        blocked_edges=set(),
        policy=pol,
    )
    assert path == ["a", "b", "d"]


def test_dijkstra_best_route_matches_astar_on_weighted_graph() -> None:
    g = nx.DiGraph()
    g.add_node("a", lat=32.0, lon=34.8, pattern_id="p1", frequency=0)
    g.add_node("b", lat=32.001, lon=34.801, pattern_id="p1", frequency=0)
    g.add_node("c", lat=32.002, lon=34.802, pattern_id="p2", frequency=0)
    g.add_node("d", lat=32.003, lon=34.803, pattern_id="p1", frequency=0)

    g.add_edge("a", "c", travel_time_s=30.0, distance_m=250.0, is_transfer=True)
    g.add_edge("c", "d", travel_time_s=20.0, distance_m=50.0, is_transfer=False)
    g.add_edge("a", "b", travel_time_s=40.0, distance_m=100.0, is_transfer=False)
    g.add_edge("b", "d", travel_time_s=30.0, distance_m=100.0, is_transfer=False)

    pol = RoutingPolicy(
        per_edge_penalty_s=0.0,
        transfer_penalty_s=100.0,
        transfer_distance_penalty_per_m_s=0.4,
        pattern_switch_penalty_s=100.0,
        frequency_discount_coef=0.0,
        frequency_cap=60.0,
        heuristic_max_speed_m_s=22.22,
    )
    expected = astar_route(
        graph=g,
        edge_geometries={},
        start_node_id="a",
        end_node_id="d",
        blocked_edges=set(),
        policy=pol,
    )
    dijkstra_path = dijkstra_best_route(
        graph=g,
        edge_geometries={},
        start_node_id="a",
        end_node_id="d",
        blocked_edges=set(),
        policy=pol,
    )
    assert dijkstra_path == expected == ["a", "b", "d"]


def test_dijkstra_and_astar_agree_when_direct_edge_blocked() -> None:
    g = nx.DiGraph()
    g.add_node("a", lat=32.0, lon=34.8, pattern_id="p1", frequency=0)
    g.add_node("b", lat=32.001, lon=34.801, pattern_id="p1", frequency=0)
    g.add_node("c", lat=32.002, lon=34.802, pattern_id="p2", frequency=0)
    g.add_node("d", lat=32.003, lon=34.803, pattern_id="p1", frequency=0)

    g.add_edge("a", "c", travel_time_s=30.0, distance_m=250.0, is_transfer=True)
    g.add_edge("c", "d", travel_time_s=20.0, distance_m=50.0, is_transfer=False)
    g.add_edge("a", "b", travel_time_s=40.0, distance_m=100.0, is_transfer=False)
    g.add_edge("b", "d", travel_time_s=30.0, distance_m=100.0, is_transfer=False)

    pol = RoutingPolicy(
        per_edge_penalty_s=0.0,
        transfer_penalty_s=100.0,
        transfer_distance_penalty_per_m_s=0.4,
        pattern_switch_penalty_s=100.0,
        frequency_discount_coef=0.0,
        frequency_cap=60.0,
        heuristic_max_speed_m_s=22.22,
    )
    blocked = {("a", "c")}
    expected = astar_route(
        graph=g,
        edge_geometries={},
        start_node_id="a",
        end_node_id="d",
        blocked_edges=blocked,
        policy=pol,
    )
    dijkstra_path = dijkstra_best_route(
        graph=g,
        edge_geometries={},
        start_node_id="a",
        end_node_id="d",
        blocked_edges=blocked,
        policy=pol,
    )
    assert expected == ["a", "b", "d"]
    assert dijkstra_path == expected


def test_compute_detour_dijkstra_raises_error_when_no_path_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """When dijkstra_best_route returns None (no path found), compute_detour
    should raise DetourComputeError."""
    import backend.domain.detour_service as ds
    from backend.domain.detour_service import DetourComputeError

    inp = replace(_input_for_service(), routing_engine="dijkstra")
    detour_g, detour_ge = _tiny_graph("d1", "d2", "S1", "S2")

    class FakeDetourGraph:
        def __init__(self) -> None:
            self.graph = detour_g
            self.edge_geometries = detour_ge

        def resolve_endpoint(self, stop_id: str, *, prefer_primary: bool = True):
            return "d1" if stop_id == "S1" else "d2"

    def fake_dijkstra_best_route(**_kwargs):
        return None

    monkeypatch.setattr(ds, "build_detour_graph", lambda **_kwargs: FakeDetourGraph())
    monkeypatch.setattr(
        ds,
        "compute_blocked_edges",
        lambda **_: (set(), {"type": "FeatureCollection", "features": []}),
    )
    monkeypatch.setattr(ds, "collect_path_geojson", lambda **_: {"type": "FeatureCollection", "features": []})
    monkeypatch.setattr(ds, "dijkstra_best_route", fake_dijkstra_best_route)

    with pytest.raises(DetourComputeError):
        compute_detour(inp)


def test_compute_route_detour_by_area_cache_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.mcp_server.transport import http as app_mod

    expected = {
        "route_id": "R1",
        "direction_id": "0",
        "pattern_id": "p1",
        "blocked_edges_count": 1,
        "stop_before": "S1",
        "stop_after": "S2",
        "detour_stop_path": ["S1", "S2"],
        "detour_geojson": {"type": "FeatureCollection", "features": []},
        "replaced_segment_geojson": {"type": "FeatureCollection", "features": []},
        "used_transfers": False,
        "error": None,
    }

    monkeypatch.setattr(app_mod.db_access_module, "get_trip_time_bounds_pg", lambda: {})
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig1")
    monkeypatch.setattr(
        app_mod.db_access_module,
        "get_cached_detour_by_area_pg",
        lambda **_kwargs: expected,
    )

    class _FailPatternBuilder:
        def __init__(self, _feed):
            raise AssertionError("PatternBuilder should not run on cache hit")

    monkeypatch.setattr(app_mod, "PatternBuilder", _FailPatternBuilder)
    res = app_mod._compute_route_detour_by_area(
        feed=object(),
        date_str="20260101",
        start_sec=3600,
        end_sec=7200,
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
        blockage_hash="abc",
        cache_mode="route",
        policy_profile="weighted-v1",
        route_id="R1",
        direction_id="0",
        transfer_radius_m=200.0,
        use_osm_detour=False,
    )
    assert res.route_id == "R1"
    assert res.detour_stop_path == ["S1", "S2"]


def test_compute_route_detour_by_area_blocks_path_intersecting_polygon(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.mcp_server.transport import http as app_mod

    # Cache row exists but is invalid (intersects blockage), so compute path must proceed.
    cached_invalid = {
        "route_id": "R1",
        "direction_id": "0",
        "pattern_id": "p1",
        "blocked_edges_count": 1,
        "stop_before": "S1",
        "stop_after": "S2",
        "detour_stop_path": ["S1", "S2"],
        "detour_geojson": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": [[34.8, 32.0], [34.81, 32.01]]},
                    "properties": {},
                }
            ],
        },
        "replaced_segment_geojson": {"type": "FeatureCollection", "features": []},
        "used_transfers": False,
        "error": None,
    }

    monkeypatch.setattr(app_mod.db_access_module, "get_trip_time_bounds_pg", lambda: {})
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig1")
    monkeypatch.setattr(app_mod.db_access_module, "get_cached_detour_by_area_pg", lambda **_kwargs: cached_invalid)
    monkeypatch.setattr(app_mod.db_access_module, "save_detour_by_area_pg", lambda **_kwargs: None)

    class FakePatternBuilder:
        def __init__(self, _feed):
            pass

        def build_patterns_for_route(self, **_kwargs):
            return [SimpleNamespace(pattern_id="p1", direction_id="0", stop_ids=["S1", "S2"])]

        def pick_most_frequent_pattern(self, pats):
            return pats[0]

    class FakeGraphBuilder:
        def __init__(self, _feed):
            pass

        def build_graph_for_pattern(self, _pattern):
            g, ge = _tiny_graph("p1:S1:0", "p1:S2:1", "S1", "S2")
            return SimpleNamespace(graph=g, edge_geometries=ge)

    detour_g = nx.DiGraph()
    detour_g.add_node("p1:S1:0", stop_id="S1", lat=32.0, lon=34.8, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:S2:1", stop_id="S2", lat=32.01, lon=34.81, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:S3:9", stop_id="S3", lat=32.0, lon=34.83, pattern_id="p1", frequency=0)
    # Short direct edge intersects blockage polygon and should be auto-blocked by strict check.
    detour_g.add_edge("p1:S1:0", "p1:S2:1", travel_time_s=10.0, distance_m=200.0, is_transfer=False)
    detour_g.add_edge("p1:S1:0", "p1:S3:9", travel_time_s=20.0, distance_m=1000.0, is_transfer=False)
    detour_g.add_edge("p1:S3:9", "p1:S2:1", travel_time_s=20.0, distance_m=1000.0, is_transfer=False)
    detour_ge = {
        ("p1:S1:0", "p1:S2:1"): EdgeGeometry(from_stop_id="S1", to_stop_id="S2", linestring=LineString([(34.8, 32.0), (34.81, 32.01)])),
        ("p1:S1:0", "p1:S3:9"): EdgeGeometry(from_stop_id="S1", to_stop_id="S3", linestring=LineString([(34.8, 32.0), (34.83, 32.0)])),
        ("p1:S3:9", "p1:S2:1"): EdgeGeometry(from_stop_id="S3", to_stop_id="S2", linestring=LineString([(34.83, 32.0), (34.81, 32.01)])),
    }

    class FakeDetourGraph:
        def __init__(self):
            self.graph = detour_g
            self.edge_geometries = detour_ge

    monkeypatch.setattr(app_mod, "PatternBuilder", FakePatternBuilder)
    monkeypatch.setattr(app_mod, "GraphBuilder", FakeGraphBuilder)
    monkeypatch.setattr(app_mod, "build_detour_graph", lambda **_kwargs: FakeDetourGraph())
    calls = {"n": 0}

    def _fake_compute_blocked_edges(**_kwargs):
        calls["n"] += 1
        # First call is on the primary pattern graph (route is affected).
        if calls["n"] == 1:
            return {("p1:S1:0", "p1:S2:1")}, {"type": "FeatureCollection", "features": []}
        # Second call is on detour graph: intentionally miss the true blocker.
        return set(), {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(app_mod, "compute_blocked_edges", _fake_compute_blocked_edges)

    blockage = {
        "type": "Polygon",
        "coordinates": [[[34.804, 32.004], [34.806, 32.004], [34.806, 32.006], [34.804, 32.006], [34.804, 32.004]]],
    }
    res = app_mod._compute_route_detour_by_area(
        feed=object(),
        date_str="20260101",
        start_sec=3600,
        end_sec=7200,
        blockage_geojson=blockage,
        blockage_hash="h1",
        cache_mode="route",
        policy_profile="weighted-v1",
        route_id="R1",
        direction_id="0",
        transfer_radius_m=200.0,
        use_osm_detour=False,
    )
    assert res.error is None
    assert res.detour_stop_path == ["S1", "S3", "S2"]


def test_compute_route_detour_by_area_manual_road_before_automatic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Submitted detour_road_geojson is applied before GTFS/OSM and is not rejected for clipping the blockage."""
    from backend.mcp_server.transport import http as app_mod

    monkeypatch.setattr(app_mod.db_access_module, "get_trip_time_bounds_pg", lambda: {})
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig1")
    monkeypatch.setattr(app_mod.db_access_module, "get_cached_detour_by_area_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod.db_access_module, "get_detour_street_override_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod.db_access_module, "save_detour_by_area_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod, "VALHALLA_URL", "http://valhalla")
    monkeypatch.setattr(app_mod.db_access_module, "save_detour_street_override_pg", lambda **_kwargs: None)

    class FakePatternBuilder:
        def __init__(self, _feed):
            pass

        def build_patterns_for_route(self, **_kwargs):
            return [SimpleNamespace(pattern_id="p1", direction_id="0", stop_ids=["S1", "S2"])]

        def pick_most_frequent_pattern(self, pats):
            return pats[0]

    class FakeGraphBuilder:
        def __init__(self, _feed):
            pass

        def build_graph_for_pattern(self, _pattern):
            g, ge = _tiny_graph("p1:S1:0", "p1:S2:1", "S1", "S2")
            return SimpleNamespace(graph=g, edge_geometries=ge)

    detour_g = nx.DiGraph()
    detour_g.add_node("p1:S1:0", stop_id="S1", lat=32.0, lon=34.8, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:S2:1", stop_id="S2", lat=32.01, lon=34.81, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:S3:9", stop_id="S3", lat=32.0, lon=34.83, pattern_id="p1", frequency=0)
    detour_g.add_edge("p1:S1:0", "p1:S2:1", travel_time_s=10.0, distance_m=200.0, is_transfer=False)
    detour_g.add_edge("p1:S1:0", "p1:S3:9", travel_time_s=20.0, distance_m=1000.0, is_transfer=False)
    detour_g.add_edge("p1:S3:9", "p1:S2:1", travel_time_s=20.0, distance_m=1000.0, is_transfer=False)
    detour_ge = {
        ("p1:S1:0", "p1:S2:1"): EdgeGeometry(from_stop_id="S1", to_stop_id="S2", linestring=LineString([(34.8, 32.0), (34.81, 32.01)])),
        ("p1:S1:0", "p1:S3:9"): EdgeGeometry(from_stop_id="S1", to_stop_id="S3", linestring=LineString([(34.8, 32.0), (34.83, 32.0)])),
        ("p1:S3:9", "p1:S2:1"): EdgeGeometry(from_stop_id="S3", to_stop_id="S2", linestring=LineString([(34.83, 32.0), (34.81, 32.01)])),
    }

    class FakeDetourGraph:
        def __init__(self):
            self.graph = detour_g
            self.edge_geometries = detour_ge

    monkeypatch.setattr(app_mod, "PatternBuilder", FakePatternBuilder)
    monkeypatch.setattr(app_mod, "GraphBuilder", FakeGraphBuilder)
    monkeypatch.setattr(app_mod, "build_detour_graph", lambda **_kwargs: FakeDetourGraph())

    calls = {"n": 0}

    def _fake_compute_blocked_edges(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {("p1:S1:0", "p1:S2:1")}, {"type": "FeatureCollection", "features": []}
        return set(), {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(app_mod, "compute_blocked_edges", _fake_compute_blocked_edges)

    blockage = {
        "type": "Polygon",
        "coordinates": [[[34.804, 32.004], [34.806, 32.004], [34.806, 32.006], [34.804, 32.006], [34.804, 32.004]]],
    }
    # Line passes through the blockage interior; previously rejected by road_geojson_clear_of_blockage.
    manual_road = {
        "type": "LineString",
        "coordinates": [[34.8045, 32.0045], [34.8055, 32.0055]],
    }

    class _Feed:
        trips = [{"route_id": "R1", "direction_id": "0", "service_id": "SVC", "trip_id": "t1"}]

    monkeypatch.setattr(app_mod, "ServiceCalendar", lambda _feed: SimpleNamespace(active_service_ids_for_date=lambda _d: {"SVC"}))

    res = app_mod._compute_route_detour_by_area(
        feed=_Feed(),
        date_str="20260101",
        start_sec=3600,
        end_sec=7200,
        blockage_geojson=blockage,
        blockage_hash="h1",
        cache_mode="route",
        policy_profile="weighted-v1",
        route_id="R1",
        direction_id="0",
        transfer_radius_m=200.0,
        use_osm_detour=False,
        apply_request_street_override=True,
        street_override_road=manual_road,
        street_override_remember=False,
    )
    assert res.error is None
    assert res.detour_stop_path == ["S1", "S2"]
    assert res.detour_geojson and res.detour_geojson.get("features")
    assert res.detour_geojson["features"][0].get("properties", {}).get("kind") == "street_override"
    assert res.from_override is False
    assert res.reason_code == "request_override_used"
    assert res.strategy_used == "street_override"


def test_compute_route_detour_by_area_prefers_best_hybrid_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.mcp_server.transport import http as app_mod

    monkeypatch.setattr(app_mod.db_access_module, "get_trip_time_bounds_pg", lambda: {})
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig1")
    monkeypatch.setattr(app_mod.db_access_module, "get_cached_detour_by_area_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod.db_access_module, "get_detour_street_override_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod.db_access_module, "save_detour_by_area_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod, "VALHALLA_URL", "http://valhalla")

    class FakePatternBuilder:
        def __init__(self, _feed):
            pass

        def build_patterns_for_route(self, **_kwargs):
            return [SimpleNamespace(pattern_id="p1", direction_id="0", stop_ids=["S1", "S2"])]

        def pick_most_frequent_pattern(self, pats):
            return pats[0]

    class FakeGraphBuilder:
        def __init__(self, _feed):
            pass

        def build_graph_for_pattern(self, _pattern):
            g, ge = _tiny_graph("p1:S1:0", "p1:S2:1", "S1", "S2")
            return SimpleNamespace(graph=g, edge_geometries=ge)

    detour_g = nx.DiGraph()
    detour_g.add_node("p1:S1:0", stop_id="S1", lat=32.0, lon=34.8, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:Sx:1", stop_id="Sx", lat=32.01, lon=34.81, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:Sy:2", stop_id="Sy", lat=32.02, lon=34.82, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:S2:3", stop_id="S2", lat=32.03, lon=34.83, pattern_id="p1", frequency=0)
    detour_g.add_node("p1:S3:9", stop_id="S3", lat=32.005, lon=34.805, pattern_id="p1", frequency=0)
    detour_g.add_edge("p1:S1:0", "p1:Sx:1", travel_time_s=1000.0, distance_m=2000.0, is_transfer=False)
    detour_g.add_edge("p1:Sx:1", "p1:Sy:2", travel_time_s=1000.0, distance_m=2000.0, is_transfer=False)
    detour_g.add_edge("p1:Sy:2", "p1:S2:3", travel_time_s=1000.0, distance_m=2000.0, is_transfer=False)
    detour_g.add_edge("p1:S1:0", "p1:S3:9", travel_time_s=100.0, distance_m=500.0, is_transfer=False)
    detour_g.add_edge("p1:S3:9", "p1:S2:3", travel_time_s=100.0, distance_m=500.0, is_transfer=False)
    detour_ge = {
        ("p1:S1:0", "p1:Sx:1"): EdgeGeometry(from_stop_id="S1", to_stop_id="Sx", linestring=LineString([(34.8, 32.0), (34.81, 32.01)])),
        ("p1:Sx:1", "p1:Sy:2"): EdgeGeometry(from_stop_id="Sx", to_stop_id="Sy", linestring=LineString([(34.81, 32.01), (34.82, 32.02)])),
        ("p1:Sy:2", "p1:S2:3"): EdgeGeometry(from_stop_id="Sy", to_stop_id="S2", linestring=LineString([(34.82, 32.02), (34.83, 32.03)])),
        ("p1:S1:0", "p1:S3:9"): EdgeGeometry(from_stop_id="S1", to_stop_id="S3", linestring=LineString([(34.8, 32.0), (34.805, 32.005)])),
        ("p1:S3:9", "p1:S2:3"): EdgeGeometry(from_stop_id="S3", to_stop_id="S2", linestring=LineString([(34.805, 32.005), (34.83, 32.03)])),
    }

    class FakeDetourGraph:
        def __init__(self):
            self.graph = detour_g
            self.edge_geometries = detour_ge

    monkeypatch.setattr(app_mod, "PatternBuilder", FakePatternBuilder)
    monkeypatch.setattr(app_mod, "GraphBuilder", FakeGraphBuilder)
    monkeypatch.setattr(app_mod, "build_detour_graph", lambda **_kwargs: FakeDetourGraph())
    monkeypatch.setattr(
        app_mod,
        "compute_blocked_edges",
        lambda **_kwargs: ({("p1:S1:0", "p1:S2:1")}, {"type": "FeatureCollection", "features": []}),
    )
    calls = {"n": 0}

    def _fake_astar(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return ["p1:S1:0", "p1:Sx:1", "p1:Sy:2", "p1:S2:3"]
        return ["p1:S1:0", "p1:S3:9", "p1:S2:3"]

    monkeypatch.setattr(app_mod, "astar_route", _fake_astar)

    def _road_eval(**kwargs):
        gtfs_dist = float(kwargs.get("gtfs_distance_m") or 0.0)
        ratio = 1.8 if gtfs_dist > 3000 else 1.1
        return OSMFeasibilityResult(
            success=True,
            reason_code="road_validated",
            road_geometry_geojson={
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[34.8, 32.0], [34.83, 32.03]]}, "properties": {"kind": "osm_detour"}}
                ],
            },
            road_distance_m=gtfs_dist * ratio,
            road_time_s=1000.0,
            gtfs_distance_m=gtfs_dist,
            distance_ratio=ratio,
            time_ratio=1.2,
            turn_by_turn=None,
        )

    monkeypatch.setattr(app_mod, "evaluate_road_feasibility_for_candidate", _road_eval)

    class _Feed:
        trips = [{"route_id": "R1", "direction_id": "0", "service_id": "SVC", "trip_id": "t1"}]
        stops = [
            {"stop_id": "S1", "stop_lat": 32.0, "stop_lon": 34.8},
            {"stop_id": "S2", "stop_lat": 32.03, "stop_lon": 34.83},
        ]

    monkeypatch.setattr(app_mod, "ServiceCalendar", lambda _feed: SimpleNamespace(active_service_ids_for_date=lambda _d: {"SVC"}))

    res = app_mod._compute_route_detour_by_area(
        feed=_Feed(),
        date_str="20260101",
        start_sec=3600,
        end_sec=7200,
        blockage_geojson={"type": "Point", "coordinates": [35.5, 33.0]},
        blockage_hash="h1",
        cache_mode="route",
        policy_profile="weighted-v1",
        route_id="R1",
        direction_id="0",
        transfer_radius_m=200.0,
        use_osm_detour=True,
    )
    assert res.error is None
    assert res.strategy_used == "gtfs_road_hybrid"
    assert res.reason_code == "hybrid_road_validated"
    assert res.detour_stop_path == ["S1", "S3", "S2"]


def test_compute_route_detour_by_area_road_rejects_falls_back_gtfs(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.mcp_server.transport import http as app_mod

    monkeypatch.setattr(app_mod.db_access_module, "get_trip_time_bounds_pg", lambda: {})
    monkeypatch.setattr(app_mod.db_access_module, "get_active_feed_id", lambda: 1)
    monkeypatch.setattr(app_mod, "_resolve_route_sig_hash", lambda *_args, **_kwargs: "sig1")
    monkeypatch.setattr(app_mod.db_access_module, "get_cached_detour_by_area_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod.db_access_module, "get_detour_street_override_pg", lambda **_kwargs: None)
    monkeypatch.setattr(app_mod.db_access_module, "save_detour_by_area_pg", lambda **_kwargs: None)

    class FakePatternBuilder:
        def __init__(self, _feed):
            pass

        def build_patterns_for_route(self, **_kwargs):
            return [SimpleNamespace(pattern_id="p1", direction_id="0", stop_ids=["S1", "S2"])]

        def pick_most_frequent_pattern(self, pats):
            return pats[0]

    class FakeGraphBuilder:
        def __init__(self, _feed):
            pass

        def build_graph_for_pattern(self, _pattern):
            g, ge = _tiny_graph("p1:S1:0", "p1:S2:1", "S1", "S2")
            return SimpleNamespace(graph=g, edge_geometries=ge)

    detour_g, detour_ge = _tiny_graph("p1:S1:0", "p1:S2:1", "S1", "S2")

    class FakeDetourGraph:
        def __init__(self):
            self.graph = detour_g
            self.edge_geometries = detour_ge

    monkeypatch.setattr(app_mod, "PatternBuilder", FakePatternBuilder)
    monkeypatch.setattr(app_mod, "GraphBuilder", FakeGraphBuilder)
    monkeypatch.setattr(app_mod, "build_detour_graph", lambda **_kwargs: FakeDetourGraph())
    monkeypatch.setattr(
        app_mod,
        "compute_blocked_edges",
        lambda **_kwargs: ({("p1:S1:0", "p1:S2:1")}, {"type": "FeatureCollection", "features": []}),
    )
    monkeypatch.setattr(app_mod, "astar_route", lambda **_kwargs: ["p1:S1:0", "p1:S2:1"])
    monkeypatch.setattr(
        app_mod,
        "evaluate_road_feasibility_for_candidate",
        lambda **_kwargs: OSMFeasibilityResult(
            success=False,
            reason_code="road_route_unavailable",
            road_geometry_geojson=None,
            road_distance_m=None,
            road_time_s=None,
            gtfs_distance_m=1000.0,
            distance_ratio=None,
            time_ratio=None,
            turn_by_turn=None,
        ),
    )

    class _Feed:
        trips = [{"route_id": "R1", "direction_id": "0", "service_id": "SVC", "trip_id": "t1"}]
        stops = [
            {"stop_id": "S1", "stop_lat": 32.0, "stop_lon": 34.8},
            {"stop_id": "S2", "stop_lat": 32.01, "stop_lon": 34.81},
        ]

    monkeypatch.setattr(app_mod, "ServiceCalendar", lambda _feed: SimpleNamespace(active_service_ids_for_date=lambda _d: {"SVC"}))

    res = app_mod._compute_route_detour_by_area(
        feed=_Feed(),
        date_str="20260101",
        start_sec=3600,
        end_sec=7200,
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
        blockage_hash="h1",
        cache_mode="route",
        policy_profile="weighted-v1",
        route_id="R1",
        direction_id="0",
        transfer_radius_m=200.0,
        use_osm_detour=True,
    )
    assert res.error is None
    assert res.strategy_used == "gtfs_multiroute"
    assert res.reason_code == "gtfs_only_fallback"


def test_hybrid_osm_detour_segment_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.mcp_server.transport import http as app_mod

    class _OK:
        success = True
        coordinates = [(34.8, 32.0), (34.82, 32.0)]
        distance_m = 1000.0
        time_s = 120.0

    monkeypatch.setattr(app_mod, "VALHALLA_URL", "http://valhalla")
    monkeypatch.setattr(app_mod, "route_avoiding_polygon", lambda *_args, **_kwargs: _OK())
    monkeypatch.setattr(app_mod, "map_match_coordinates", lambda _coords: None)

    out = app_mod._hybrid_osm_detour_segment(
        blockage_geojson={
            "type": "Polygon",
            "coordinates": [[[34.804, 32.004], [34.806, 32.004], [34.806, 32.006], [34.804, 32.006], [34.804, 32.004]]],
        },
        segment_line=LineString([(34.8, 32.0), (34.81, 32.01)]),
        fallback_from_pt=(34.8, 32.0),
        fallback_to_pt=(34.81, 32.01),
    )
    assert out is not None
    assert out["detour_coords"][0] == (34.8, 32.0)


def test_detour_endpoint_hybrid_fallback_to_gtfs(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.mcp_server.transport import http as app_mod
    from fastapi import HTTPException

    req = app_mod.DetourRequest(
        route_id="R1",
        direction_id="0",
        date="20260101",
        start_stop_id="S1",
        end_stop_id="S2",
        blockage_geojson={"type": "Point", "coordinates": [34.8, 32.0]},
    )

    monkeypatch.setattr(app_mod, "HYBRID_DETOUR_ENABLED", True)
    monkeypatch.setattr(app_mod, "VALHALLA_URL", "http://valhalla")
    monkeypatch.setattr(
        app_mod,
        "detour_osm",
        lambda _req: (_ for _ in ()).throw(HTTPException(status_code=409, detail="no osm")),
    )

    expected = SimpleNamespace(
        blocked_edges_count=0,
        stop_path=["S1", "S2"],
        path_geojson={"type": "FeatureCollection", "features": []},
        blocked_edges_geojson={"type": "FeatureCollection", "features": []},
        total_travel_time_s=100.0,
        total_distance_m=1000.0,
        baseline_travel_time_s=100.0,
        baseline_distance_m=1000.0,
        detour_delay_s=0.0,
        detour_extra_distance_m=0.0,
        used_shape=True,
        used_osm_snapping=False,
        feed_version="f1",
    )
    g, ge = _tiny_graph("c1", "c2", "S1", "S2")
    monkeypatch.setattr(app_mod, "GRAPH_CACHE", {"f1|R1|0|20260101|gtfs": {"graph": g, "edge_geometries": ge, "used_shape": True, "used_osm_snapping": False}})
    monkeypatch.setattr(app_mod, "_graph_cache_keys_for_lookup", lambda *_args, **_kwargs: ["f1|R1|0|20260101|gtfs"])
    monkeypatch.setattr(app_mod, "load_active_feed", lambda: object())
    monkeypatch.setattr(app_mod, "compute_detour_with_strategies", lambda *_args, **_kwargs: expected)

    res = app_mod.detour(req)
    assert res.stop_path == ["S1", "S2"]


def test_compute_detour_with_strategies_prefers_osm_success() -> None:
    inp = _input_for_service()

    def _osm(_inp: DetourComputeInput) -> DetourComputeResult:
        return DetourComputeResult(
            blocked_edges_count=1,
            stop_path=["S1", "S2"],
            path_geojson={"type": "FeatureCollection", "features": []},
            blocked_edges_geojson={"type": "FeatureCollection", "features": []},
            total_travel_time_s=10.0,
            total_distance_m=100.0,
            baseline_travel_time_s=20.0,
            baseline_distance_m=120.0,
            detour_delay_s=-10.0,
            detour_extra_distance_m=-20.0,
            used_shape=True,
            used_osm_snapping=False,
            feed_version="f1",
            reason_code="osm_hybrid_path_found",
            strategy_used="osm_hybrid",
            confidence=0.9,
        )

    res = compute_detour_with_strategies(inp, osm_strategy=_osm, prefer_osm=True)
    assert res.strategy_used == "osm_hybrid"
    assert res.reason_code == "osm_hybrid_path_found"
    assert isinstance(res.diagnostics, dict)
    assert res.diagnostics.get("coordinator") == "detour_service"


def test_compute_detour_with_strategies_falls_back_to_gtfs(monkeypatch: pytest.MonkeyPatch) -> None:
    inp = _input_for_service()

    expected = DetourComputeResult(
        blocked_edges_count=0,
        stop_path=["S1", "S2"],
        path_geojson={"type": "FeatureCollection", "features": []},
        blocked_edges_geojson={"type": "FeatureCollection", "features": []},
        total_travel_time_s=10.0,
        total_distance_m=100.0,
        baseline_travel_time_s=10.0,
        baseline_distance_m=100.0,
        detour_delay_s=0.0,
        detour_extra_distance_m=0.0,
        used_shape=True,
        used_osm_snapping=False,
        feed_version="f1",
        reason_code="gtfs_path_found",
        strategy_used="gtfs_graph",
        confidence=0.8,
    )
    monkeypatch.setattr("backend.domain.detour_service.compute_detour", lambda _inp: expected)

    res = compute_detour_with_strategies(inp, osm_strategy=lambda _inp: None, prefer_osm=True)
    assert res.strategy_used == "gtfs_graph"
    assert res.reason_code == "gtfs_path_found"
    assert isinstance(res.diagnostics, dict)
    assert res.diagnostics.get("coordinator") == "detour_service"
