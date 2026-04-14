from __future__ import annotations

import heapq
import logging
import os
import time
from math import radians, cos, sin, asin, sqrt
from typing import Dict, List, Tuple, Optional, Any

import networkx as nx
from shapely.geometry import mapping, LineString, shape
from shapely.strtree import STRtree

from backend.domain.graph_builder import GraphBuildResult, EdgeGeometry
from .routing_policy import RoutingPolicy, default_routing_policy

logger = logging.getLogger(__name__)


def routing_edge_weight(
    graph: nx.DiGraph,
    u: str,
    v: str,
    attrs: Dict[str, Any],
    policy: RoutingPolicy,
    blocked_edges: set[Tuple[str, str]],
) -> float:
    """
    Policy-weighted travel cost for one directed edge (seconds), matching `astar_route`.
    Blocked edges are impassable (infinite cost).
    """
    if (u, v) in blocked_edges:
        return float("inf")

    base = float(attrs.get("travel_time_s", attrs.get("weight", 1.0)))

    per_edge_penalty = policy.per_edge_penalty_s

    transfer_penalty = 0.0
    if attrs.get("is_transfer"):
        transfer_penalty = policy.transfer_penalty_s
        transfer_dist_m = float(attrs.get("distance_m", 0.0) or 0.0)
        transfer_penalty += transfer_dist_m * policy.transfer_distance_penalty_per_m_s

    pattern_switch_penalty = 0.0
    try:
        pu = graph.nodes[u].get("pattern_id")
        pv = graph.nodes[v].get("pattern_id")
        if pu is not None and pv is not None and pu != pv:
            pattern_switch_penalty = policy.pattern_switch_penalty_s
    except Exception:
        pattern_switch_penalty = 0.0

    freq_factor = 1.0
    try:
        f1 = float(graph.nodes[u].get("frequency") or 0.0)
        f2 = float(graph.nodes[v].get("frequency") or 0.0)
        freq = max(f1, f2)
        if freq > 0:
            freq = min(freq, policy.frequency_cap)
            freq_factor = 1.0 / (1.0 + policy.frequency_discount_coef * freq)
    except Exception:
        freq_factor = 1.0

    return (base + per_edge_penalty + transfer_penalty + pattern_switch_penalty) * freq_factor


def astar_route(
    graph: nx.DiGraph,
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry],
    start_node_id: str,
    end_node_id: str,
    blocked_edges: Optional[set[Tuple[str, str]]] = None,
    policy: Optional[RoutingPolicy] = None,
) -> List[str]:
    """
    A* routing where edge weights are interpreted as travel time in seconds.
    Any edge in blocked_edges is treated as impassable (infinite cost), so the
    path never uses that road/segment; the router only uses edges that are
    possible to go through.

    `start_node_id` and `end_node_id` are graph node identifiers (pattern-stop
    node ids), not raw GTFS stop_ids.
    """
    blocked_edges = blocked_edges or set()
    pol = policy or default_routing_policy()

    def heuristic(n1: str, n2: str) -> float:
        # Use straight-line distance divided by an upper-bound speed
        # to get an admissible estimate of travel time.
        lat1 = graph.nodes[n1]["lat"]
        lon1 = graph.nodes[n1]["lon"]
        lat2 = graph.nodes[n2]["lat"]
        lon2 = graph.nodes[n2]["lon"]
        R = 6371000.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        dist_m = R * c

        return dist_m / pol.heuristic_max_speed_m_s

    return nx.astar_path(
        graph,
        start_node_id,
        end_node_id,
        heuristic=heuristic,
        weight=lambda u, v, attrs: routing_edge_weight(graph, u, v, attrs, pol, blocked_edges),
    )


def _path_policy_cost(
    graph: nx.DiGraph,
    path: List[str],
    policy: RoutingPolicy,
    blocked_edges: set[Tuple[str, str]],
) -> float:
    """Sum of ``routing_edge_weight`` along *path*."""
    total = 0.0
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        attrs = graph.get_edge_data(u, v, default={})
        w = routing_edge_weight(graph, u, v, attrs, policy, blocked_edges)
        total += w
    return total


def dijkstra_best_route(
    graph: nx.DiGraph,
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry],
    start_node_id: str,
    end_node_id: str,
    blocked_edges: Optional[set[Tuple[str, str]]] = None,
    policy: Optional[RoutingPolicy] = None,
    max_time_s: Optional[float] = None,
) -> Optional[List[str]]:
    """
    Dijkstra's algorithm with a priority queue and permanent visited set.

    Each node is settled at most once at its minimum-cost path, making this
    O(E log V) regardless of graph density. Finds the optimal path or returns
    ``None`` when no path exists or the timeout fires before the goal is settled.

    ``max_time_s`` overrides the ``DETOUR_DFS_TIMEOUT_S`` env variable.
    """
    blocked_edges = blocked_edges or set()
    pol = policy or default_routing_policy()

    if start_node_id == end_node_id:
        return [start_node_id]

    timeout_s = max_time_s if max_time_s is not None else float(os.getenv("DETOUR_DFS_TIMEOUT_S", "3.0"))
    deadline = time.monotonic() + timeout_s

    dist: Dict[str, float] = {start_node_id: 0.0}
    prev: Dict[str, Optional[str]] = {start_node_id: None}
    settled: set[str] = set()
    # Priority queue entries: (cost_so_far, node_id)
    pq: List[Tuple[float, str]] = [(0.0, start_node_id)]

    while pq:
        if time.monotonic() > deadline:
            logger.info("dijkstra_best_route: timeout after settling %d nodes", len(settled))
            return None

        cost_u, u = heapq.heappop(pq)
        if u in settled:
            continue
        settled.add(u)

        if u == end_node_id:
            path: List[str] = []
            cur: Optional[str] = u
            while cur is not None:
                path.append(cur)
                cur = prev[cur]
            path.reverse()
            n_transfers = 0
            patterns_used: set[str] = set()
            for i in range(len(path) - 1):
                ed = graph.get_edge_data(path[i], path[i + 1], default={})
                if ed.get("is_transfer"):
                    n_transfers += 1
                pid = graph.nodes[path[i]].get("pattern_id")
                if pid:
                    patterns_used.add(pid)
            pid_end = graph.nodes[path[-1]].get("pattern_id")
            if pid_end:
                patterns_used.add(pid_end)
            logger.info(
                "dijkstra_best_route: found path (hops=%d, cost=%.1f, settled=%d, transfers=%d, patterns=%d)",
                len(path) - 1,
                cost_u,
                len(settled),
                n_transfers,
                len(patterns_used),
            )
            return path

        for v in graph.successors(u):
            if v in settled:
                continue
            attrs = graph.get_edge_data(u, v, default={})
            w = routing_edge_weight(graph, u, v, attrs, pol, blocked_edges)
            if w == float("inf"):
                continue
            new_cost = cost_u + w
            if new_cost < dist.get(v, float("inf")):
                dist[v] = new_cost
                prev[v] = u
                heapq.heappush(pq, (new_cost, v))

    logger.info("dijkstra_best_route: no path found (settled=%d)", len(settled))
    return None


# Backward-compatible alias for callers still referencing the old name.
dfs_best_route = dijkstra_best_route


def collect_path_geojson(
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry],
    path: List[str],
) -> Dict:
    features: List[Dict] = []
    for i in range(len(path) - 1):
        u = path[i]
        v = path[i + 1]
        eg = edge_geometries.get((u, v))
        if not eg:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(eg.linestring),
                "properties": {"from_stop_id": eg.from_stop_id, "to_stop_id": eg.to_stop_id},
            }
        )
    return {"type": "FeatureCollection", "features": features}


def compute_blocked_edges(
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry],
    blockage_geojson: Dict,
) -> Tuple[set[Tuple[str, str]], Dict]:
    from backend.infra import db_access

    # Prefer PostGIS for intersection (SQL-heavy path).
    blocked_sql = db_access.get_blocked_edge_keys_pg(
        edge_geometries, blockage_geojson
    )
    if blocked_sql is not None:
        blocked = blocked_sql
        blocked_feats = []
        for (u, v) in blocked:
            eg = edge_geometries.get((u, v))
            if eg and eg.linestring:
                from_id = getattr(eg, "from_stop_id", u)
                to_id = getattr(eg, "to_stop_id", v)
                blocked_feats.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(eg.linestring),
                        "properties": {"from_stop_id": from_id, "to_stop_id": to_id},
                    }
                )
        return blocked, {"type": "FeatureCollection", "features": blocked_feats}

    # Fallback: STRtree in Python.
    geom = shape(blockage_geojson)
    try:
        geom = geom.buffer(1e-3)
    except Exception:
        pass
    if geom.is_empty:
        return set(), {"type": "FeatureCollection", "features": []}

    lines: List[LineString] = []
    index_to_key: Dict[int, Tuple[str, str]] = {}
    for key, eg in edge_geometries.items():
        idx = len(lines)
        lines.append(eg.linestring)
        index_to_key[idx] = key
    if not lines:
        return set(), {"type": "FeatureCollection", "features": []}
    tree = STRtree(lines)

    blocked: set[Tuple[str, str]] = set()
    blocked_feats = []
    try:
        indices = tree.query(geom, predicate="intersects")
    except TypeError:
        indices = tree.query(geom)
    try:
        iter_indices = indices.flat
    except AttributeError:
        iter_indices = indices if isinstance(indices, (list, tuple)) else [indices]
    for idx in iter_indices:
        idx = int(idx)
        key = index_to_key.get(idx)
        if not key:
            continue
        u, v = key
        ls = lines[idx]
        if not ls.intersects(geom):
            continue
        blocked.add((u, v))
        eg = edge_geometries.get((u, v))
        from_id = eg.from_stop_id if eg else u
        to_id = eg.to_stop_id if eg else v
        blocked_feats.append(
            {
                "type": "Feature",
                "geometry": mapping(ls),
                "properties": {"from_stop_id": from_id, "to_stop_id": to_id},
            }
        )
    return blocked, {"type": "FeatureCollection", "features": blocked_feats}
