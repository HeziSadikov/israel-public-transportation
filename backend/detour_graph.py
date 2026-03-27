from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Tuple, Optional, Set

import networkx as nx
from shapely.geometry import LineString, shape, mapping

from .pattern_builder import PatternBuilder, RoutePattern
from .graph_builder import (
    GraphBuilder,
    EdgeGeometry,
    haversine_meters,
    angle_difference_deg,
)
from .area_search import find_routes_in_polygon
from . import db_access
from .config import DETOUR_ALLOW_FEED_FALLBACK

if TYPE_CHECKING:
    from .gtfs_loader import GTFSFeed


logger = logging.getLogger(__name__)

POSTGIS_UNAVAILABLE = "postgis_unavailable"
PATTERN_DATA_MISSING = "pattern_data_missing"


class DetourGraphBuildError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _env_float(key: str, default: str) -> float:
    return float(os.getenv(key, default))


# Legacy aliases (same env keys as DetourGraphParams).
AOI_BUFFER_DEG = _env_float("DETOUR_AOI_BUFFER_DEG", "0.01")
TRANSFER_HEADING_TOLERANCE_DEG = _env_float("DETOUR_TRANSFER_HEADING_TOLERANCE_DEG", "90")


@dataclass(frozen=True)
class DetourGraphParams:
    """Explicit search-space tuning for detour graph construction (AOI, transfers)."""

    aoi_buffer_deg: float
    transfer_heading_tolerance_deg: float
    transfer_radius_m: float
    transfer_walk_speed_m_s: float
    transfer_fixed_penalty_s: float


def default_detour_graph_params() -> DetourGraphParams:
    return DetourGraphParams(
        aoi_buffer_deg=_env_float("DETOUR_AOI_BUFFER_DEG", "0.01"),
        transfer_heading_tolerance_deg=_env_float("DETOUR_TRANSFER_HEADING_TOLERANCE_DEG", "90"),
        transfer_radius_m=_env_float("DETOUR_TRANSFER_RADIUS_M", "200"),
        transfer_walk_speed_m_s=_env_float("DETOUR_TRANSFER_WALK_SPEED_M_S", "1.3"),
        transfer_fixed_penalty_s=_env_float("DETOUR_TRANSFER_FIXED_PENALTY_S", "60"),
    )


def _build_nodes_by_stop_id(
    g: nx.DiGraph,
    primary_pattern_id: Optional[str],
) -> Dict[str, List[str]]:
    """Map GTFS stop_id -> pattern-stop node ids, sorted: primary pattern first, then by node id."""

    by_stop: Dict[str, List[str]] = {}
    for nid in g.nodes():
        sid = g.nodes[nid].get("stop_id")
        if sid is None:
            continue
        by_stop.setdefault(str(sid), []).append(nid)

    def sort_key(n: str) -> Tuple[int, str]:
        pid = g.nodes[n].get("pattern_id")
        primary_first = 0 if (primary_pattern_id and pid == primary_pattern_id) else 1
        return (primary_first, n)

    for sid, nids in by_stop.items():
        by_stop[sid] = sorted(nids, key=sort_key)
    return by_stop


@dataclass
class DetourGraph:
    graph: nx.DiGraph
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry]
    primary_pattern_id: Optional[str] = None
    nodes_by_stop_id: Dict[str, List[str]] = field(default_factory=dict)

    def resolve_endpoint(self, stop_id: str, *, prefer_primary: bool = True) -> Optional[str]:
        """
        Resolve a physical stop_id to one pattern-stop node.

        If prefer_primary is True, prefer a node on primary_pattern_id (deterministic: lexicographically
        smallest node id among primary-pattern candidates). Otherwise choose the lexicographically
        smallest node id among all candidates.
        """
        nids = self.nodes_by_stop_id.get(stop_id)
        if not nids:
            return None
        if prefer_primary:
            return nids[0]
        return min(nids)


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
    max_transfer_m: float,
    walk_speed_m_s: float,
    fixed_penalty_s: float,
    heading_tolerance_deg: float,
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


def _merge_from_postgis_bulk(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    metas: List[db_access.PatternMeta],
    date_ymd: str,
) -> None:
    # Backward-compatible alias: date_ymd is metadata-only for this precomputed
    # local ride-network build in Phase 1.
    _merge_from_postgis_precomputed(g, edge_geoms, metas)


def _merge_from_postgis_precomputed(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    metas: List[db_access.PatternMeta],
) -> None:
    """Merge precomputed ride-network subgraph for these patterns."""
    if not metas:
        return

    pattern_ids = [m.pattern_id for m in metas if m.pattern_id]
    if not pattern_ids:
        return

    nodes = db_access.get_pattern_nodes_bulk(pattern_ids)
    edges = db_access.get_pattern_edges_bulk(pattern_ids)

    # Nodes
    for nd in nodes:
        nid = nd["node_id"]
        if nid in g:
            continue
        g.add_node(
            nid,
            pattern_id=nd["pattern_id"],
            route_id=nd["route_id"],
            direction_id=nd["direction_id"],
            stop_id=nd["stop_id"],
            stop_sequence=nd["stop_sequence"],
            stop_name=None,
            lat=float(nd["lat"]),
            lon=float(nd["lon"]),
            out_heading_deg=nd["out_heading_deg"],
            frequency=nd["frequency"],
        )

    # Ride edges
    for ed in edges:
        u = ed["from_node_id"]
        v = ed["to_node_id"]
        if u not in g or v not in g:
            # Defensive: should not happen, but avoid malformed inserts breaking request.
            continue
        travel_time_s = ed.get("travel_time_s")
        distance_m = ed.get("distance_m")
        if travel_time_s is None:
            continue
        weight = float(travel_time_s)
        g.add_edge(
            u,
            v,
            weight=weight,
            travel_time_s=weight,
            distance_m=0.0 if distance_m is None else float(distance_m),
        )

        linestring = ed.get("linestring")
        if linestring is not None:
            edge_geoms[(u, v)] = EdgeGeometry(
                from_stop_id=ed["from_stop_id"],
                to_stop_id=ed["to_stop_id"],
                linestring=linestring,
            )


def build_detour_graph(
    feed: Optional[GTFSFeed],
    date_ymd: str,
    blockage_geojson: Dict,
    primary_route_id: str,
    primary_direction_id: Optional[str],
    start_sec: Optional[int] = None,
    end_sec: Optional[int] = None,
    params: Optional[DetourGraphParams] = None,
) -> DetourGraph:
    """
    Build a direction-aware detour graph.

    - Candidate routes: routes whose shapes intersect a **buffered AOI** around the blockage
      (so nearby parallel corridors and routes that go around the blockage are included).
      Broad time windows include more routes and increase build cost.
    - Blocking still uses the raw blockage geometry (caller uses blockage_geojson for
      compute_blocked_edges). Here we only use the buffer to select which routes go in the graph.
    - Nodes are pattern-stops; ride edges same-pattern only; transfers heading-compatible.
    - When PostGIS can resolve the primary pattern, all merged routes use PostGIS precomputed
      ride-network tables (`pattern_nodes` + `pattern_edges`) to assemble the local graph
      (no per-pattern GraphBuilder work on the detour hot path). Feed + GraphBuilder is only used
      when PostGIS pattern lookup fails but an in-memory feed is available.
    """
    p = params or default_detour_graph_params()

    blockage_geom = shape(blockage_geojson)
    try:
        aoi_geom = blockage_geom.buffer(p.aoi_buffer_deg)
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

    use_postgis = False
    primary_meta = None
    postgis_error: Optional[Exception] = None
    try:
        db_access.get_active_feed_id()
        dir_param = str(primary_direction_id) if primary_direction_id is not None else None
        primary_meta = db_access.get_pattern_for_route(
            primary_route_id, dir_param, date_ymd
        )
        use_postgis = primary_meta is not None
    except (RuntimeError, Exception) as exc:
        postgis_error = exc
    if feed is None and primary_meta is not None:
        use_postgis = True

    if use_postgis and primary_meta is not None:
        primary_pattern_id = primary_meta.pattern_id
        metas = [primary_meta]
        for rid in route_ids:
            if rid == primary_route_id:
                continue
            meta = db_access.get_pattern_for_route(rid, None, date_ymd)
            if meta is not None:
                metas.append(meta)
        _merge_from_postgis_precomputed(g, edge_geoms, metas)
    else:
        if postgis_error is not None and not (DETOUR_ALLOW_FEED_FALLBACK and feed is not None):
            raise DetourGraphBuildError(
                POSTGIS_UNAVAILABLE,
                "PostGIS is unavailable for detour graph build.",
            ) from postgis_error
        if primary_meta is None and not (DETOUR_ALLOW_FEED_FALLBACK and feed is not None):
            raise DetourGraphBuildError(
                PATTERN_DATA_MISSING,
                "Primary route pattern data is missing in PostGIS for detour graph build.",
            )
        if feed is None:
            idx = _build_nodes_by_stop_id(g, None)
            return DetourGraph(
                graph=g,
                edge_geometries=edge_geoms,
                primary_pattern_id=None,
                nodes_by_stop_id=idx,
            )
        reason_code = POSTGIS_UNAVAILABLE if postgis_error is not None else PATTERN_DATA_MISSING
        logger.warning(
            "detour_graph_feed_fallback",
            extra={
                "route_id": primary_route_id,
                "date": date_ymd,
                "reason_code": reason_code,
                "fallback_used": True,
            },
        )
        patterns_builder = PatternBuilder(feed)
        graph_builder = GraphBuilder(feed)

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
        max_transfer_m=p.transfer_radius_m,
        walk_speed_m_s=p.transfer_walk_speed_m_s,
        fixed_penalty_s=p.transfer_fixed_penalty_s,
        heading_tolerance_deg=p.transfer_heading_tolerance_deg,
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

    idx = _build_nodes_by_stop_id(g, primary_pattern_id)
    return DetourGraph(
        graph=g,
        edge_geometries=edge_geoms,
        primary_pattern_id=primary_pattern_id,
        nodes_by_stop_id=idx,
    )
