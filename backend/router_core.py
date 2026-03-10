from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Any

import networkx as nx
from shapely.geometry import mapping, LineString, shape
from shapely.strtree import STRtree

from .graph_builder import GraphBuildResult, EdgeGeometry


def astar_route(
    graph: nx.DiGraph,
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry],
    start_stop_id: str,
    end_stop_id: str,
    blocked_edges: Optional[set[Tuple[str, str]]] = None,
) -> List[str]:
    """
    A* routing where edge weights are interpreted as travel time in seconds.
    Any edge in blocked_edges is treated as impassable (infinite cost), so the
    path never uses that road/segment; the router only uses edges that are
    possible to go through.
    """
    blocked_edges = blocked_edges or set()

    def weight(u: str, v: str, attrs: Dict[str, Any]) -> float:
        # Never route through a blocked edge: treat as impassable.
        if (u, v) in blocked_edges:
            return float("inf")

        # Base edge weight: scheduled travel time in seconds (fallback to generic weight).
        base = float(attrs.get("travel_time_s", attrs.get("weight", 1.0)))

        # Soft per-edge penalty as a proxy for dwell/boarding/etc.
        per_edge_penalty = 10.0

        # Additional penalty for transfer edges so we strongly prefer staying
        # on the same vehicle when possible.
        transfer_penalty = 0.0
        if attrs.get("is_transfer"):
            transfer_penalty = 240.0  # ~4 minutes equivalent

        # Frequency-based bias: prefer more frequent patterns when the graph
        # carries per-node frequency metadata (trips per day). This is a
        # modest discount that never dominates absolute time.
        freq_factor = 1.0
        try:
            f1 = float(graph.nodes[u].get("frequency") or 0.0)
            f2 = float(graph.nodes[v].get("frequency") or 0.0)
            freq = max(f1, f2)
            if freq > 0:
                import math

                # Clamp very high frequencies and apply a gentle discount.
                freq = min(freq, 60.0)
                freq_factor = 1.0 / (1.0 + 0.01 * freq)
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

        # Assume a conservative max speed of 80 km/h (~22.22 m/s) so the
        # heuristic never overestimates true travel time.
        max_speed_m_s = 22.22
        return dist_m / max_speed_m_s

    return nx.astar_path(
        graph,
        start_stop_id,
        end_stop_id,
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
    geom = shape(blockage_geojson)
    # Buffer the blockage so corridor edges that run along or through the drawn
    # area are reliably detected (~110 m at mid-latitudes); avoids detour path
    # still crossing the blocked area (e.g. line 16 Holon–Rishon).
    try:
        geom = geom.buffer(1e-3)
    except Exception:
        pass
    if geom.is_empty:
        return set(), {"type": "FeatureCollection", "features": []}

    # Build a spatial index. Shapely 2 STRtree.query(geom) returns indices.
    # Use predicate="intersects" so we only get edges that actually intersect the polygon.
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
    blocked_feats: List[Dict] = []
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
    blocked_fc = {"type": "FeatureCollection", "features": blocked_feats}
    return blocked, blocked_fc

