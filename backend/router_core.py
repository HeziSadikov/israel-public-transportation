from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Any

import networkx as nx
from shapely.geometry import mapping, LineString, shape
from shapely.strtree import STRtree

from .graph_builder import GraphBuildResult, EdgeGeometry
from .routing_policy import RoutingPolicy, default_routing_policy


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

    def weight(u: str, v: str, attrs: Dict[str, Any]) -> float:
        # Never route through a blocked edge: treat as impassable.
        if (u, v) in blocked_edges:
            return float("inf")

        # Base edge weight: scheduled travel time in seconds (fallback to generic weight).
        base = float(attrs.get("travel_time_s", attrs.get("weight", 1.0)))

        per_edge_penalty = pol.per_edge_penalty_s

        transfer_penalty = 0.0
        if attrs.get("is_transfer"):
            transfer_penalty = pol.transfer_penalty_s

        freq_factor = 1.0
        try:
            f1 = float(graph.nodes[u].get("frequency") or 0.0)
            f2 = float(graph.nodes[v].get("frequency") or 0.0)
            freq = max(f1, f2)
            if freq > 0:
                freq = min(freq, pol.frequency_cap)
                freq_factor = 1.0 / (1.0 + pol.frequency_discount_coef * freq)
        except Exception:
            freq_factor = 1.0

        return (base + per_edge_penalty + transfer_penalty) * freq_factor

    def heuristic(n1: str, n2: str) -> float:
        # Use straight-line distance divided by an upper-bound speed
        # to get an admissible estimate of travel time.
        lat1 = graph.nodes[n1]["lat"]
        lon1 = graph.nodes[n1]["lon"]
        lat2 = graph.nodes[n2]["lat"]
        lon2 = graph.nodes[n2]["lon"]
        from math import radians, cos, sin, asin, sqrt

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
        weight=lambda u, v, attrs: weight(u, v, attrs),
    )


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
    from . import db_access

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

