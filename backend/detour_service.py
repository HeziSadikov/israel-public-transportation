"""Orchestration for POST /detour: baseline path, local detour graph, A*, response assembly."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import networkx as nx

logger = logging.getLogger(__name__)

from . import db_access
from .detour_geo_validation import (
    road_geojson_clear_of_blockage,
    road_geojson_has_routable_geometry,
)
from .detour_instructions_text import instructions_text_he_to_steps, merged_steps_to_geocode_queries
from .geocoding_nominatim import geocode_query_best_effort
from .detour_graph import (
    DetourGraphParams,
    DetourGraphBuildError,
    PATTERN_DATA_MISSING,
    POSTGIS_UNAVAILABLE,
    build_detour_graph,
    default_detour_graph_params,
)
from .graph_builder import EdgeGeometry
from .router_core import astar_route, collect_path_geojson, compute_blocked_edges, dijkstra_best_route, dfs_best_route
from .routing_policy import RoutingPolicy, default_routing_policy

STRATEGY_GTFS_GRAPH = "gtfs_graph"
STRATEGY_STREET_OVERRIDE = "street_override"
STRATEGY_INSTRUCTIONS_ONLY = "instructions_only"
REASON_GRAPH_UNAVAILABLE = "graph_unavailable"
REASON_ENDPOINT_NOT_FOUND = "endpoint_not_found"
REASON_UNAFFECTED = "route_not_affected"
REASON_GTFS_PATH_FOUND = "gtfs_path_found"
REASON_STORED_OVERRIDE_USED = "stored_override_used"
REASON_REQUEST_OVERRIDE_USED = "request_override_used"
REASON_INSTRUCTIONS_ONLY = "instructions_only_fallback"
REASON_NO_DETOUR_PATH = "no_detour_path"
REASON_INVALID_GEOMETRY = "invalid_geometry"


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
    feed_id: Optional[int] = None
    route_sig_hash: Optional[str] = None
    detour_road_geojson: Optional[Dict[str, Any]] = None
    turn_by_turn: Optional[List[Dict[str, Any]]] = None
    instructions_text_he: Optional[str] = None
    remember_override: bool = False
    routing_engine: Literal["astar", "dfs", "dijkstra"] = "astar"


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
    turn_by_turn: Optional[List[Dict[str, Any]]] = None
    from_override: bool = False
    instructions_only: bool = False
    reason_code: Optional[str] = None
    strategy_used: Optional[str] = None
    confidence: Optional[float] = None
    diagnostics: Optional[Dict[str, Any]] = None


DetourStrategyCallable = Callable[[DetourComputeInput], Optional[DetourComputeResult]]


EMPTY_PATH_GEOJSON: Dict[str, Any] = {"type": "FeatureCollection", "features": []}


def _manual_steps_from_input(inp: DetourComputeInput) -> List[Dict[str, Any]]:
    if inp.turn_by_turn:
        return list(inp.turn_by_turn)
    if inp.instructions_text_he and str(inp.instructions_text_he).strip():
        return instructions_text_he_to_steps(inp.instructions_text_he)
    return []


def _junction_geocode_waypoints(inp: DetourComputeInput) -> Optional[List[Tuple[float, float]]]:
    """Geocode instruction steps to (lon, lat) for snapping onto corridor edges as junctions."""
    steps = _manual_steps_from_input(inp)
    if not steps:
        return None
    queries = merged_steps_to_geocode_queries(steps)
    if not queries:
        return None
    out: List[Tuple[float, float]] = []
    prior: Optional[Tuple[float, float]] = None
    for q in queries:
        pt = geocode_query_best_effort(q, prior_lonlat=prior)
        if pt is not None:
            out.append(pt)
            prior = pt
    return out if out else None


def _result_instructions_only(
    inp: DetourComputeInput,
    detour_blocked_geojson: Dict[str, Any],
    blocked_count: int,
    steps: List[Dict[str, Any]],
    baseline_travel_time_s: Optional[float],
    baseline_distance_m: Optional[float],
    *,
    from_override: bool,
) -> DetourComputeResult:
    stop_path = [inp.start_stop_id, inp.end_stop_id]
    return DetourComputeResult(
        blocked_edges_count=blocked_count,
        stop_path=stop_path,
        path_geojson=EMPTY_PATH_GEOJSON,
        blocked_edges_geojson=detour_blocked_geojson,
        total_travel_time_s=None,
        total_distance_m=None,
        baseline_travel_time_s=baseline_travel_time_s,
        baseline_distance_m=baseline_distance_m,
        detour_delay_s=None,
        detour_extra_distance_m=None,
        used_shape=inp.used_shape,
        used_osm_snapping=inp.used_osm_snapping,
        feed_version=inp.feed_version,
        turn_by_turn=steps,
        from_override=from_override,
        instructions_only=True,
        reason_code=REASON_INSTRUCTIONS_ONLY,
        strategy_used=STRATEGY_INSTRUCTIONS_ONLY,
        confidence=0.3,
        diagnostics={"fallback": "instructions_only", "from_override": from_override},
    )


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


def _result_from_street_override(
    inp: DetourComputeInput,
    detour_blocked_geojson: Dict[str, Any],
    blocked_count: int,
    road: Dict[str, Any],
    steps: Optional[List[Dict[str, Any]]],
    baseline_travel_time_s: Optional[float],
    baseline_distance_m: Optional[float],
    *,
    from_override: bool,
) -> DetourComputeResult:
    from .detour_geo_validation import (
        approximate_line_length_m,
        extract_line_coords_from_road_geojson,
        road_geojson_to_path_feature_collection,
    )

    path_geojson = road_geojson_to_path_feature_collection(road)
    coords = extract_line_coords_from_road_geojson(road)
    total_distance_m = approximate_line_length_m(coords)
    total_travel_time_s = total_distance_m / 5.0 if total_distance_m > 0 else None
    detour_delay_s = None
    detour_extra_distance_m = None
    if baseline_travel_time_s is not None and baseline_distance_m is not None and total_travel_time_s is not None:
        detour_delay_s = total_travel_time_s - baseline_travel_time_s
        detour_extra_distance_m = total_distance_m - baseline_distance_m
    stop_path = [inp.start_stop_id, inp.end_stop_id]
    return DetourComputeResult(
        blocked_edges_count=blocked_count,
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
        turn_by_turn=steps,
        from_override=from_override,
        instructions_only=False,
        reason_code=REASON_STORED_OVERRIDE_USED if from_override else REASON_REQUEST_OVERRIDE_USED,
        strategy_used=STRATEGY_STREET_OVERRIDE,
        confidence=0.95 if from_override else 0.85,
        diagnostics={"override_source": "stored" if from_override else "request"},
    )


def _path_geojson_clear_of_blockage(path_geojson: Dict[str, Any], blockage_geojson: Dict[str, Any]) -> bool:
    # Reuse shared road validation by validating each line feature independently.
    if not isinstance(path_geojson, dict):
        return False
    if path_geojson.get("type") != "FeatureCollection":
        return False
    features = path_geojson.get("features") or []
    if not isinstance(features, list):
        return False
    line_geometries = []
    for feat in features:
        if not isinstance(feat, dict):
            continue
        geom = feat.get("geometry")
        if isinstance(geom, dict) and geom.get("type") in ("LineString", "MultiLineString"):
            line_geometries.append(geom)
    if not line_geometries:
        return True
    for geom in line_geometries:
        if not road_geojson_clear_of_blockage(geom, blockage_geojson):
            return False
    return True


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

    try:
        detour_graph_res = build_detour_graph(
            feed=inp.feed,
            date_ymd=inp.date_str,
            blockage_geojson=inp.blockage_geojson,
            primary_route_id=inp.route_id,
            primary_direction_id=inp.direction_id,
            start_sec=inp.start_sec,
            end_sec=inp.end_sec,
            params=dparams,
            junction_geocode_waypoints_lonlat=_junction_geocode_waypoints(inp),
        )
    except DetourGraphBuildError as e:
        if e.code == POSTGIS_UNAVAILABLE:
            raise DetourComputeError(
                503,
                "PostGIS is unavailable for detour computation.",
            ) from e
        if e.code == PATTERN_DATA_MISSING:
            raise DetourComputeError(
                409,
                "Required pattern data is missing for detour computation.",
            ) from e
        raise DetourComputeError(
            409,
            "Detour graph build failed.",
        ) from e

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

    blockage_hash = db_access.hash_geojson_canonical(inp.blockage_geojson)
    route_sig = inp.route_sig_hash or ""
    if inp.feed_id is not None:
        okey = db_access.build_street_override_key(
            "point",
            inp.route_id,
            inp.direction_id,
            blockage_hash,
            inp.start_stop_id,
            inp.end_stop_id,
            route_sig,
        )
        try:
            stored = db_access.get_detour_street_override_pg(inp.feed_id, okey)
        except Exception:
            stored = None
        if stored:
            sroad = stored["road_geojson"]
            st_steps = stored.get("turn_by_turn") or []
            if road_geojson_has_routable_geometry(sroad) and road_geojson_clear_of_blockage(
                sroad, inp.blockage_geojson
            ):
                res = _result_from_street_override(
                    inp,
                    detour_blocked_geojson,
                    len(detour_blocked),
                    sroad,
                    st_steps,
                    baseline_travel_time_s,
                    baseline_distance_m,
                    from_override=True,
                )
                if not _path_geojson_clear_of_blockage(res.path_geojson, inp.blockage_geojson):
                    res.reason_code = REASON_INVALID_GEOMETRY
                    res.diagnostics = {"override_source": "stored", "validation": "blocked"}
                else:
                    return res
            if st_steps and not road_geojson_has_routable_geometry(sroad):
                return _result_instructions_only(
                    inp,
                    detour_blocked_geojson,
                    len(detour_blocked),
                    st_steps,
                    baseline_travel_time_s,
                    baseline_distance_m,
                    from_override=True,
                )

    def _route_detour_path() -> List[str]:
        if inp.routing_engine in ("dfs", "dijkstra"):
            dijkstra_path = dijkstra_best_route(
                graph=detour_graph_res.graph,
                edge_geometries=detour_graph_res.edge_geometries,
                start_node_id=detour_start,
                end_node_id=detour_end,
                blocked_edges=blocked_for_routing,
                policy=policy,
            )
            if dijkstra_path is not None:
                return dijkstra_path
            raise nx.NetworkXNoPath("Dijkstra found no path within limits")
        return astar_route(
            graph=detour_graph_res.graph,
            edge_geometries=detour_graph_res.edge_geometries,
            start_node_id=detour_start,
            end_node_id=detour_end,
            blocked_edges=blocked_for_routing,
            policy=policy,
        )

    try:
        path_nodes = _route_detour_path()
    except Exception:
        manual_road = inp.detour_road_geojson
        steps = _manual_steps_from_input(inp)
        if manual_road and road_geojson_has_routable_geometry(manual_road) and road_geojson_clear_of_blockage(
            manual_road, inp.blockage_geojson
        ):
            res = _result_from_street_override(
                inp,
                detour_blocked_geojson,
                len(detour_blocked),
                manual_road,
                steps,
                baseline_travel_time_s,
                baseline_distance_m,
                from_override=False,
            )
            if inp.remember_override and inp.feed_id is not None:
                try:
                    db_access.save_detour_street_override_pg(
                        inp.feed_id,
                        db_access.build_street_override_key(
                            "point",
                            inp.route_id,
                            inp.direction_id,
                            blockage_hash,
                            inp.start_stop_id,
                            inp.end_stop_id,
                            route_sig,
                        ),
                        "point",
                        inp.route_id,
                        inp.direction_id,
                        blockage_hash,
                        inp.start_stop_id,
                        inp.end_stop_id,
                        route_sig,
                        manual_road,
                        steps,
                    )
                except Exception:
                    pass
            return res
        if steps:
            empty_road = {"type": "LineString", "coordinates": []}
            res = _result_instructions_only(
                inp,
                detour_blocked_geojson,
                len(detour_blocked),
                steps,
                baseline_travel_time_s,
                baseline_distance_m,
                from_override=False,
            )
            if inp.remember_override and inp.feed_id is not None:
                try:
                    db_access.save_detour_street_override_pg(
                        inp.feed_id,
                        db_access.build_street_override_key(
                            "point",
                            inp.route_id,
                            inp.direction_id,
                            blockage_hash,
                            inp.start_stop_id,
                            inp.end_stop_id,
                            route_sig,
                        ),
                        "point",
                        inp.route_id,
                        inp.direction_id,
                        blockage_hash,
                        inp.start_stop_id,
                        inp.end_stop_id,
                        route_sig,
                        empty_road,
                        steps,
                    )
                except Exception:
                    pass
            return res
        raise DetourComputeError(
            409,
            "No detour path found on GTFS routes; adjust blockage or parameters.",
        )

    stop_path = [
        detour_graph_res.graph.nodes[n]["stop_id"]
        for n in path_nodes
        if detour_graph_res.graph.nodes[n].get("stop_id") is not None
    ]
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

    res = DetourComputeResult(
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
        turn_by_turn=None,
        from_override=False,
        instructions_only=False,
        reason_code=REASON_GTFS_PATH_FOUND,
        strategy_used=STRATEGY_GTFS_GRAPH,
        confidence=0.8,
        diagnostics={"blocked_edges": len(detour_blocked), "routing_engine": inp.routing_engine},
    )
    if not _path_geojson_clear_of_blockage(res.path_geojson, inp.blockage_geojson):
        raise DetourComputeError(
            409,
            "Computed detour still intersects blockage geometry.",
        )
    return res


def compute_detour_with_strategies(
    inp: DetourComputeInput,
    *,
    osm_strategy: Optional[DetourStrategyCallable] = None,
    prefer_osm: bool = True,
) -> DetourComputeResult:
    attempts: List[Dict[str, Any]] = []

    def _record_attempt(strategy: str, outcome: str, reason: Optional[str] = None) -> None:
        entry: Dict[str, Any] = {"strategy": strategy, "outcome": outcome}
        if reason:
            entry["reason"] = reason
        attempts.append(entry)

    def _run_osm() -> Optional[DetourComputeResult]:
        if osm_strategy is None:
            return None
        try:
            res = osm_strategy(inp)
        except Exception as exc:  # pragma: no cover - defensive integration boundary
            _record_attempt("osm_hybrid", "error", str(exc))
            return None
        if res is None:
            _record_attempt("osm_hybrid", "miss", "no_route")
            return None
        if not res.strategy_used:
            res.strategy_used = "osm_hybrid"
        if not res.reason_code:
            res.reason_code = "osm_hybrid_path_found"
        if res.confidence is None:
            res.confidence = 0.9
        d = dict(res.diagnostics or {})
        d["attempts"] = list(attempts)
        d["coordinator"] = "detour_service"
        res.diagnostics = d
        _record_attempt("osm_hybrid", "success")
        return res

    def _run_gtfs() -> DetourComputeResult:
        res = compute_detour(inp)
        d = dict(res.diagnostics or {})
        d["attempts"] = list(attempts)
        d["coordinator"] = "detour_service"
        res.diagnostics = d
        _record_attempt("gtfs_graph", "success")
        return res

    if prefer_osm:
        osm_res = _run_osm()
        if osm_res is not None:
            return osm_res
        return _run_gtfs()

    try:
        return _run_gtfs()
    except Exception as exc:
        _record_attempt("gtfs_graph", "error", str(exc))
        osm_res = _run_osm()
        if osm_res is not None:
            return osm_res
        raise
