"""Orchestration for POST /detour: baseline path, local detour graph, A*, response assembly."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from .detour_graph import DetourGraphParams, build_detour_graph, default_detour_graph_params
from .graph_builder import EdgeGeometry
from .router_core import astar_route, collect_path_geojson, compute_blocked_edges
from .routing_policy import RoutingPolicy, default_routing_policy


class DetourComputeError(Exception):
    """Maps to HTTP errors from the /detour handler."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


@dataclass
class DetourComputeInput:
    route_id: str
    direction_id: Optional[str]
    date_str: str
    start_stop_id: str
    end_stop_id: str
    blockage_geojson: Dict[str, Any]
    # Cached single-route graph from /graph/build
    cache_graph: nx.DiGraph
    cache_edge_geometries: Dict[Tuple[str, str], EdgeGeometry]
    used_shape: bool
    used_osm_snapping: bool
    feed_version: str
    feed: Any  # Optional GTFSFeed; None when PostGIS-only
    start_sec: Optional[int] = None
    end_sec: Optional[int] = None
    detour_params: Optional[DetourGraphParams] = None
    routing_policy: Optional[RoutingPolicy] = None


@dataclass
class DetourComputeResult:
    blocked_edges_count: int
    stop_path: List[str]
    path_geojson: Dict[str, Any]
    blocked_edges_geojson: Dict[str, Any]
    total_travel_time_s: Optional[float]
    total_distance_m: Optional[float]
    baseline_travel_time_s: Optional[float]
    baseline_distance_m: Optional[float]
    detour_delay_s: Optional[float]
    detour_extra_distance_m: Optional[float]
    used_shape: bool
    used_osm_snapping: bool
    feed_version: str


def _resolve_node_on_graph(graph: nx.DiGraph, stop_id: str) -> Optional[str]:
    return next(
        (n for n in graph.nodes() if graph.nodes[n].get("stop_id") == stop_id),
        None,
    )


def _path_metrics(graph: nx.DiGraph, path_nodes: List[str]) -> Tuple[float, float]:
    tt = 0.0
    dist = 0.0
    for i in range(len(path_nodes) - 1):
        u, v = path_nodes[i], path_nodes[i + 1]
        edge_data = graph.get_edge_data(u, v, default={})
        tt += float(edge_data.get("travel_time_s", 0.0))
        dist += float(edge_data.get("distance_m", 0.0))
    return tt, dist


def compute_detour(inp: DetourComputeInput) -> DetourComputeResult:
    policy = inp.routing_policy or default_routing_policy()
    dparams = inp.detour_params or default_detour_graph_params()

    start_node = _resolve_node_on_graph(inp.cache_graph, inp.start_stop_id)
    end_node = _resolve_node_on_graph(inp.cache_graph, inp.end_stop_id)
    if not start_node or not end_node:
        raise DetourComputeError(
            400,
            "start_stop_id or end_stop_id not found in graph.",
        )

    baseline_travel_time_s: Optional[float] = None
    baseline_distance_m: Optional[float] = None
    try:
        baseline_path = astar_route(
            graph=inp.cache_graph,
            edge_geometries=inp.cache_edge_geometries,
            start_node_id=start_node,
            end_node_id=end_node,
            blocked_edges=set(),
            policy=policy,
        )
        baseline_travel_time_s, baseline_distance_m = _path_metrics(inp.cache_graph, baseline_path)
    except Exception:
        baseline_travel_time_s = None
        baseline_distance_m = None

    detour_graph_res = build_detour_graph(
        feed=inp.feed,
        date_ymd=inp.date_str,
        blockage_geojson=inp.blockage_geojson,
        primary_route_id=inp.route_id,
        primary_direction_id=inp.direction_id,
        start_sec=inp.start_sec,
        end_sec=inp.end_sec,
        params=dparams,
    )

    detour_blocked, detour_blocked_geojson = compute_blocked_edges(
        edge_geometries=detour_graph_res.edge_geometries,
        blockage_geojson=inp.blockage_geojson,
    )
    # Route only on the detour graph's edge set; blocked keys are (u,v) in that graph.
    blocked_for_routing = detour_blocked

    detour_start = detour_graph_res.resolve_endpoint(inp.start_stop_id, prefer_primary=True)
    detour_end = detour_graph_res.resolve_endpoint(inp.end_stop_id, prefer_primary=True)
    if not detour_start or not detour_end:
        raise DetourComputeError(
            400,
            "start_stop_id or end_stop_id not in detour graph.",
        )

    try:
        path_nodes = astar_route(
            graph=detour_graph_res.graph,
            edge_geometries=detour_graph_res.edge_geometries,
            start_node_id=detour_start,
            end_node_id=detour_end,
            blocked_edges=blocked_for_routing,
            policy=policy,
        )
    except Exception:
        raise DetourComputeError(
            409,
            "No detour path found on GTFS routes; adjust blockage or parameters.",
        )

    stop_path = [detour_graph_res.graph.nodes[n]["stop_id"] for n in path_nodes]
    path_geojson = collect_path_geojson(
        edge_geometries=detour_graph_res.edge_geometries,
        path=path_nodes,
    )
    total_travel_time_s, total_distance_m = _path_metrics(detour_graph_res.graph, path_nodes)

    detour_delay_s: Optional[float] = None
    detour_extra_distance_m: Optional[float] = None
    if baseline_travel_time_s is not None and baseline_distance_m is not None:
        detour_delay_s = total_travel_time_s - baseline_travel_time_s
        detour_extra_distance_m = total_distance_m - baseline_distance_m

    return DetourComputeResult(
        blocked_edges_count=len(detour_blocked),
        stop_path=stop_path,
        path_geojson=path_geojson,
        blocked_edges_geojson=detour_blocked_geojson,
        total_travel_time_s=total_travel_time_s,
        total_distance_m=total_distance_m,
        baseline_travel_time_s=baseline_travel_time_s,
        baseline_distance_m=baseline_distance_m,
        detour_delay_s=detour_delay_s,
        detour_extra_distance_m=detour_extra_distance_m,
        used_shape=inp.used_shape,
        used_osm_snapping=inp.used_osm_snapping,
        feed_version=inp.feed_version,
    )
