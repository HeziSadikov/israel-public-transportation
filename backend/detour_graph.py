from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set

import networkx as nx
from shapely.geometry import LineString, shape, mapping

from .gtfs_loader import GTFSFeed
from .pattern_builder import PatternBuilder, RoutePattern
from .graph_builder import (
    GraphBuilder,
    EdgeGeometry,
    haversine_meters,
    angle_difference_deg,
)
from .service_calendar import ServiceCalendar
from .area_search import find_routes_in_polygon

# Allow transfers between pattern-stop nodes whose outgoing headings are within this many degrees.
TRANSFER_HEADING_TOLERANCE_DEG = 90.0
# Buffer (degrees) for AOI when selecting candidate routes; ~0.003 ≈ few hundred meters at mid-lat.
AOI_BUFFER_DEG = 0.003


@dataclass
class DetourGraph:
    graph: nx.DiGraph
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry]
    primary_pattern_id: Optional[str] = None  # for resolving stop_before/stop_after to node_ids


def _build_route_pattern(
    patterns_builder: PatternBuilder,
    route_id: str,
    direction_id: Optional[str],
    yyyymmdd: str,
) -> Optional[RoutePattern]:
    patterns = patterns_builder.build_patterns_for_route(
        route_id=route_id,
        direction_id=direction_id,
        yyyymmdd=yyyymmdd,
        max_trips=None,
    )
    if not patterns:
        return None
    return patterns_builder.pick_most_frequent_pattern(patterns)


def _merge_graphs(
    base_graph: nx.DiGraph,
    base_edges: Dict[Tuple[str, str], EdgeGeometry],
    add_graph: nx.DiGraph,
    add_edges: Dict[Tuple[str, str], EdgeGeometry],
) -> None:
    """Add all nodes and edges from add_graph; no coalescing (pattern-stop nodes are unique)."""
    for nid, data in add_graph.nodes(data=True):
        if nid not in base_graph:
            base_graph.add_node(nid, **data)
    for u, v, data in add_graph.edges(data=True):
        if not base_graph.has_edge(u, v):
            base_graph.add_edge(u, v, **data)
        else:
            existing = base_graph.get_edge_data(u, v) or {}
            if float(data.get("weight", 1e9)) < float(existing.get("weight", 1e9)):
                base_graph[u][v].update(data)
    for key, eg in add_edges.items():
        if key not in base_edges:
            base_edges[key] = eg


def _add_transfer_edges(
    g: nx.DiGraph,
    max_transfer_m: float = 120.0,
    walk_speed_m_s: float = 1.3,
    fixed_penalty_s: float = 60.0,
    heading_tolerance_deg: float = TRANSFER_HEADING_TOLERANCE_DEG,
) -> None:
    """
    Add transfer edges only between pattern-stop nodes that are:
    - within max_transfer_m, and
    - heading-compatible (out_heading_deg within tolerance) so we do not
      connect northbound to southbound / wrong כיוון נסיעה.
    """
    if not g.nodes:
        return

    cell_deg = 0.001
    buckets: Dict[Tuple[int, int], List[str]] = {}
    for nid, data in g.nodes(data=True):
        lat = float(data.get("lat"))
        lon = float(data.get("lon"))
        ci = int(lat / cell_deg)
        cj = int(lon / cell_deg)
        buckets.setdefault((ci, cj), []).append(nid)

    for nid, data in g.nodes(data=True):
        lat1 = float(data.get("lat"))
        lon1 = float(data.get("lon"))
        h1 = data.get("out_heading_deg")
        if h1 is None:
            h1 = 0.0  # allow transfer from terminal stops
        ci = int(lat1 / cell_deg)
        cj = int(lon1 / cell_deg)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                cell_nodes = buckets.get((ci + di, cj + dj))
                if not cell_nodes:
                    continue
                for nid2 in cell_nodes:
                    if nid2 == nid:
                        continue
                    d2 = g.nodes[nid2]
                    lat2 = float(d2.get("lat"))
                    lon2 = float(d2.get("lon"))
                    h2 = d2.get("out_heading_deg")
                    if h2 is None:
                        h2 = 0.0
                    dist_m = haversine_meters(lat1, lon1, lat2, lon2)
                    if dist_m <= 0 or dist_m > max_transfer_m:
                        continue
                    if angle_difference_deg(h1, h2) > heading_tolerance_deg:
                        continue
                    if not g.has_edge(nid, nid2):
                        travel_time_s = dist_m / walk_speed_m_s + fixed_penalty_s
                        g.add_edge(
                            nid,
                            nid2,
                            weight=travel_time_s,
                            travel_time_s=travel_time_s,
                            distance_m=dist_m,
                            is_transfer=True,
                        )
                    if not g.has_edge(nid2, nid):
                        travel_time_s = dist_m / walk_speed_m_s + fixed_penalty_s
                        g.add_edge(
                            nid2,
                            nid,
                            weight=travel_time_s,
                            travel_time_s=travel_time_s,
                            distance_m=dist_m,
                            is_transfer=True,
                        )


def build_detour_graph(
    feed: GTFSFeed,
    date_ymd: str,
    blockage_geojson: Dict,
    primary_route_id: str,
    primary_direction_id: Optional[str],
    transfer_radius_m: float = 200.0,
    start_sec: Optional[int] = None,
    end_sec: Optional[int] = None,
    aoi_buffer_deg: float = AOI_BUFFER_DEG,
) -> DetourGraph:
    """
    Build a direction-aware detour graph.

    - Candidate routes: routes whose shapes intersect a **buffered AOI** around the blockage
      (so nearby parallel corridors and routes that go around the blockage are included).
    - Blocking still uses the raw blockage geometry (caller uses blockage_geojson for
      compute_blocked_edges). Here we only use the buffer to select which routes go in the graph.
    - Nodes are pattern-stops; ride edges same-pattern only; transfers heading-compatible.
    """
    patterns_builder = PatternBuilder(feed)
    graph_builder = GraphBuilder(feed)

    # Two geometries: raw blockage for blocking; buffered AOI for candidate route selection.
    blockage_geom = shape(blockage_geojson)
    try:
        aoi_geom = blockage_geom.buffer(aoi_buffer_deg)
    except Exception:
        aoi_geom = blockage_geom
    aoi_geojson = mapping(aoi_geom)

    day_start = start_sec if start_sec is not None else 0
    day_end = end_sec if end_sec is not None else 27 * 3600
    routes_raw = find_routes_in_polygon(
        feed=feed,
        polygon_geojson=aoi_geojson,
        yyyymmdd=date_ymd,
        start_sec=day_start,
        end_sec=day_end,
    )
    route_ids: Set[str] = {r["route_id"] for r in routes_raw}
    route_ids.add(primary_route_id)

    g = nx.DiGraph()
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry] = {}
    primary_pattern_id: Optional[str] = None

    primary_pattern = _build_route_pattern(
        patterns_builder, primary_route_id, primary_direction_id, date_ymd
    )
    if primary_pattern is not None:
        primary_pattern_id = primary_pattern.pattern_id
        res = graph_builder.build_graph_for_pattern(primary_pattern)
        _merge_graphs(g, edge_geoms, res.graph, res.edge_geometries)

    for rid in route_ids:
        if rid == primary_route_id:
            continue
        pat = _build_route_pattern(patterns_builder, rid, None, date_ymd)
        if pat is None:
            continue
        res = graph_builder.build_graph_for_pattern(pat)
        _merge_graphs(g, edge_geoms, res.graph, res.edge_geometries)

    _add_transfer_edges(
        g,
        max_transfer_m=transfer_radius_m,
        heading_tolerance_deg=TRANSFER_HEADING_TOLERANCE_DEG,
    )

    for u, v in g.edges():
        if (u, v) in edge_geoms:
            continue
        nu = g.nodes.get(u, {})
        nv = g.nodes.get(v, {})
        lat1 = float(nu.get("lat", 0))
        lon1 = float(nu.get("lon", 0))
        lat2 = float(nv.get("lat", 0))
        lon2 = float(nv.get("lon", 0))
        sid_u = nu.get("stop_id", u)
        sid_v = nv.get("stop_id", v)
        line = LineString([(lon1, lat1), (lon2, lat2)])
        edge_geoms[(u, v)] = EdgeGeometry(from_stop_id=sid_u, to_stop_id=sid_v, linestring=line)

    return DetourGraph(
        graph=g,
        edge_geometries=edge_geoms,
        primary_pattern_id=primary_pattern_id,
    )

