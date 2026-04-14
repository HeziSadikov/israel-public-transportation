"""Unit tests for virtual junction nodes on detour ride edges."""

from __future__ import annotations

import networkx as nx
from shapely.geometry import LineString, mapping, shape

from backend.detour_junctions import (
    JunctionBuildConfig,
    apply_corridor_junctions,
    replace_ride_edge_with_junction_chain,
    snap_geocode_points_to_edges,
)
from backend.domain.graph_builder import EdgeGeometry
from backend.domain.router_core import compute_blocked_edges


def _sample_edge_graph() -> tuple[nx.DiGraph, dict]:
    g = nx.DiGraph()
    g.add_node(
        "p1:s1:0",
        pattern_id="p1",
        route_id="r1",
        direction_id=0,
        stop_id="s1",
        stop_sequence=0,
        lat=32.0,
        lon=34.8,
        out_heading_deg=90.0,
        frequency=10.0,
    )
    g.add_node(
        "p1:s2:1",
        pattern_id="p1",
        route_id="r1",
        direction_id=0,
        stop_id="s2",
        stop_sequence=1,
        lat=32.001,
        lon=34.801,
        out_heading_deg=90.0,
        frequency=10.0,
    )
    ls = LineString([(34.8, 32.0), (34.8005, 32.0005), (34.801, 32.001)])
    eg: dict = {
        ("p1:s1:0", "p1:s2:1"): EdgeGeometry(from_stop_id="s1", to_stop_id="s2", linestring=ls),
    }
    g.add_edge(
        "p1:s1:0",
        "p1:s2:1",
        weight=120.0,
        travel_time_s=120.0,
        distance_m=400.0,
    )
    return g, eg


def test_replace_preserves_total_time_and_distance() -> None:
    g, edge_geoms = _sample_edge_graph()
    ok = replace_ride_edge_with_junction_chain(
        g,
        edge_geoms,
        "p1:s1:0",
        "p1:s2:1",
        [(34.8005, 32.0005)],
        junction_reason="test",
        min_end_m=1.0,
    )
    assert ok
    path = ["p1:s1:0", "p1:s2:1"]
    # find jx node
    mids = [n for n in g.nodes() if str(n).startswith("jx:")]
    assert len(mids) == 1
    j = mids[0]
    tt = (
        float(g["p1:s1:0"][j]["travel_time_s"])
        + float(g[j]["p1:s2:1"]["travel_time_s"])
    )
    dm = float(g["p1:s1:0"][j]["distance_m"]) + float(g[j]["p1:s2:1"]["distance_m"])
    assert abs(tt - 120.0) < 1e-6
    assert abs(dm - 400.0) < 0.02


def test_blocked_edges_after_split() -> None:
    g, edge_geoms = _sample_edge_graph()
    replace_ride_edge_with_junction_chain(
        g,
        edge_geoms,
        "p1:s1:0",
        "p1:s2:1",
        [(34.8005, 32.0005)],
        junction_reason="test",
        min_end_m=1.0,
    )
    mids = [n for n in g.nodes() if str(n).startswith("jx:")]
    j = mids[0]
    poly = mapping(shape({"type": "Polygon", "coordinates": [[[34.8004, 32.0004], [34.8006, 32.0004], [34.8006, 32.0006], [34.8004, 32.0006], [34.8004, 32.0004]]]}))
    blocked, _ = compute_blocked_edges(edge_geometries=edge_geoms, blockage_geojson=poly)
    assert (g.has_edge("p1:s1:0", j) and (("p1:s1:0", j) in blocked)) or (
        g.has_edge(j, "p1:s2:1") and ((j, "p1:s2:1") in blocked)
    )


def test_snap_geocode_finds_edge() -> None:
    g, edge_geoms = _sample_edge_graph()
    pts = snap_geocode_points_to_edges(g, edge_geoms, [(34.8005, 32.0005)], max_snap_m=500.0)
    assert len(pts) == 1
    plon, plat, u, v = pts[0]
    assert (u, v) == ("p1:s1:0", "p1:s2:1")
    assert abs(plon - 34.8005) < 1e-3


def test_apply_corridor_junctions_two_patterns() -> None:
    g = nx.DiGraph()
    for nid, lat, lon in [
        ("pA:a:0", 32.0, 34.8),
        ("pA:b:1", 32.002, 34.802),
        ("pB:c:0", 32.0, 34.8),
        ("pB:d:1", 32.002, 34.802),
    ]:
        g.add_node(
            nid,
            pattern_id=nid.split(":")[0],
            route_id="rA" if nid.startswith("pA") else "rB",
            direction_id=0,
            stop_id=nid.split(":")[1],
            stop_sequence=int(nid.split(":")[2]),
            lat=lat,
            lon=lon,
            out_heading_deg=45.0,
            frequency=5.0,
        )
    # Crossing diagonals: same center (34.801, 32.001)
    la = LineString([(34.8, 32.0), (34.802, 32.002)])
    lb = LineString([(34.8, 32.002), (34.802, 32.0)])
    eg = {
        ("pA:a:0", "pA:b:1"): EdgeGeometry(from_stop_id="a", to_stop_id="b", linestring=la),
        ("pB:c:0", "pB:d:1"): EdgeGeometry(from_stop_id="c", to_stop_id="d", linestring=lb),
    }
    for u, v, tt in [
        ("pA:a:0", "pA:b:1", 100.0),
        ("pB:c:0", "pB:d:1", 100.0),
    ]:
        g.add_edge(u, v, weight=tt, travel_time_s=tt, distance_m=500.0)

    aoi = mapping(LineString([(34.79, 31.99), (34.81, 31.99), (34.81, 32.01), (34.79, 32.01), (34.79, 31.99)]).convex_hull)
    cfg = JunctionBuildConfig(crossing_enabled=True, near_miss_m=40.0, dedupe_cluster_m=20.0)
    n = apply_corridor_junctions(g, eg, aoi, cfg)
    assert n >= 1
    jx = [n for n in g.nodes if str(n).startswith("jx:")]
    assert len(jx) >= 1
