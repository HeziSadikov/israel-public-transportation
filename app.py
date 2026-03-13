from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple, Optional
import hashlib
import pickle

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from backend.gtfs_updater import update_feed, get_feed_status
from backend.feed_postgis import load_active_feed
from backend.pattern_builder import PatternBuilder, RoutePattern
from backend.graph_builder import (
    GraphBuilder,
    build_graph_for_pattern_from_postgis,
    pattern_stop_node_id,
    parse_pattern_stop_node_id,
)
from backend.osm_pretty import map_match_pattern
from backend.router_core import astar_route, collect_path_geojson, compute_blocked_edges
from backend.detour_graph import build_detour_graph
from backend.osm_detour import route_avoiding_polygon
from backend.config import VALHALLA_URL
from backend.api_models import (
    RouteSearchRequest,
    RouteInfo,
    FeedUpdateResponse,
    FeedStatusResponse,
    GraphBuildRequest,
    GraphBuildResponse,
    GraphStopsResponse,
    GraphStopsResponseStop,
    DetourRequest,
    DetourResponse,
    AreaRoutesQuery,
    AreaRoutesResponse,
    AreaRouteResult,
    StopInBounds,
    StopRoutesRequest,
    StopRoutesResponse,
    StopRouteResult,
    DetourByAreaRequest,
    DetourByAreaResponse,
    DetourByAreaRouteResult,
    DetourByAreaMode,
    GeocodeResult,
)
from backend.config import GRAPH_CACHE, GRAPH_CACHE_DIR
from backend.area_search import find_routes_in_polygon
from backend import db_access as db_access_module
from backend.service_calendar import ServiceCalendar


app = FastAPI(title="Israel GTFS Detour Router")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@app.get("/geocode", response_model=list[GeocodeResult])
def geocode(q: str, limit: int = 5):
    """Geocode an address or place name via Nominatim. Use for address search in the UI."""
    q = (q or "").strip()
    if not q or len(q) < 2:
        return []
    try:
        import httpx
        with httpx.Client(timeout=10.0, headers={"User-Agent": "IsraelGTFSDetourRouter/1.0"}) as client:
            resp = client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": q, "format": "json", "limit": min(limit, 10)},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Geocoding failed: {e}")
    results = []
    for item in data:
        lat = item.get("lat")
        lon = item.get("lon")
        if lat is None or lon is None:
            continue
        try:
            results.append(
                GeocodeResult(
                    display_name=item.get("display_name") or "",
                    lat=float(lat),
                    lon=float(lon),
                    place_id=int(item["place_id"]) if item.get("place_id") else None,
                )
            )
        except (TypeError, ValueError):
            continue
    return results


@app.get("/stops/in-bounds", response_model=list[StopInBounds])
def stops_in_bounds(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    limit: int = 500,
):
    """Return stops whose coordinates fall inside the given bounding box (e.g. map view)."""
    if min_lat >= max_lat or min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="Invalid bounds: min must be less than max")
    # PostGIS-backed lookup: active feed's stops table.
    try:
        stops = db_access_module.get_stops_in_bounds_pg(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not load stops from PostGIS: {e}")
    return [StopInBounds(**s) for s in stops]


@app.post("/routes/search", response_model=list[RouteInfo])
def routes_search(req: RouteSearchRequest):
    q = req.q.strip()
    if not q:
        return []
    try:
        rows = db_access_module.search_routes_pg(q, req.limit)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not search routes in PostGIS: {e}")
    results: list[RouteInfo] = []
    for row in rows:
        route_type_val = None
        if row.get("route_type") is not None and str(row.get("route_type", "")).strip():
            try:
                route_type_val = int(row["route_type"])
            except (ValueError, KeyError, TypeError):
                route_type_val = None
        results.append(
            RouteInfo(
                route_id=row.get("route_id") or "",
                route_short_name=row.get("route_short_name"),
                route_long_name=row.get("route_long_name"),
                agency_id=row.get("agency_id"),
                agency_name=row.get("agency_name"),
                route_type=route_type_val,
            )
        )
    return results


@app.post("/feed/update", response_model=FeedUpdateResponse)
def feed_update():
    result = update_feed()
    return FeedUpdateResponse(
        updated=result["updated"],
        online_ok=result["online_ok"],
        message=result["message"],
        active=result.get("active"),
    )


@app.get("/feed/status", response_model=FeedStatusResponse)
def feed_status():
    status = get_feed_status()
    return FeedStatusResponse(**status)


def _graph_cache_key(
    feed_version: str,
    route_id: str,
    direction_id: str | None,
    date: str,
    pretty_osm: bool,
) -> str:
    return "|".join(
        [
            feed_version,
            route_id,
            direction_id or "",
            date,
            "osm" if pretty_osm else "gtfs",
        ]
    )


def _graph_cache_keys_for_lookup(
    route_id: str,
    direction_id: str | None,
    date_str: str,
) -> list[str]:
    """Return candidate cache keys to try (PostGIS first, then feed version if loaded)."""
    keys = []
    try:
        feed_id = db_access_module.get_active_feed_id()
        keys.append(
            _graph_cache_key(f"postgis-{feed_id}", route_id, direction_id, date_str, False)
        )
        keys.append(
            _graph_cache_key(f"postgis-{feed_id}", route_id, direction_id, date_str, True)
        )
    except (RuntimeError, Exception):
        pass
    try:
        feed = load_active_feed()
        keys.append(
            _graph_cache_key(feed.version_id, route_id, direction_id, date_str, False)
        )
        keys.append(
            _graph_cache_key(feed.version_id, route_id, direction_id, date_str, True)
        )
    except Exception:
        pass
    return keys


def _graph_cache_file_path(cache_key: str, feed_version: str):
    """
    Map an in-memory graph cache key to a safe on-disk file path.
    We hash the key because it contains characters that are not valid in file names.
    """
    digest = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
    version_dir = GRAPH_CACHE_DIR / feed_version
    version_dir.mkdir(parents=True, exist_ok=True)
    return version_dir / f"{digest}.pkl"


@app.post("/graph/build", response_model=GraphBuildResponse)
def graph_build(req: GraphBuildRequest):
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")

    # Prefer PostGIS when an active feed is available and we have a pattern for this route.
    try:
        feed_id = db_access_module.get_active_feed_id()
        dir_param = str(req.direction_id) if req.direction_id is not None else None
        meta = db_access_module.get_pattern_for_route(
            req.route_id, dir_param, date_str
        )
        if meta is not None:
            feed_version = f"postgis-{feed_id}"
            key = _graph_cache_key(
                feed_version=feed_version,
                route_id=req.route_id,
                direction_id=req.direction_id,
                date=date_str,
                pretty_osm=req.pretty_osm,
            )
            cached = GRAPH_CACHE.get(key)
            if cached is not None:
                pat = cached["pattern"]
                return GraphBuildResponse(
                    pattern_id=pat.pattern_id,
                    stop_count=len(pat.stop_ids),
                    edge_count=len(cached["edge_geometries"]),
                    used_shape=cached["used_shape"],
                    used_osm_snapping=cached.get("used_osm_snapping", False),
                    example_stop_ids=pat.stop_ids[:5],
                    feed_version=feed_version,
                )
            # Try PostGIS route_graph_cache keyed by per-route signature.
            try:
                sig_hash = db_access_module.compute_route_signature(req.route_id, req.direction_id)
                cached_blob = db_access_module.get_cached_route_graph_pg(
                    feed_id=feed_id,
                    route_id=req.route_id,
                    direction_id=req.direction_id,
                    date_ymd=date_str,
                    pretty_osm=req.pretty_osm,
                    route_sig_hash=sig_hash,
                )
            except Exception:
                sig_hash = None
                cached_blob = None

            if cached_blob:
                try:
                    cached = pickle.loads(cached_blob)
                    GRAPH_CACHE[key] = cached
                    pat: RoutePattern = cached["pattern"]
                    edge_geoms = cached["edge_geometries"]
                    used_shape = cached["used_shape"]
                    used_osm = cached.get("used_osm_snapping", False)
                    return GraphBuildResponse(
                        pattern_id=pat.pattern_id,
                        stop_count=len(pat.stop_ids),
                        edge_count=len(edge_geoms),
                        used_shape=used_shape,
                        used_osm_snapping=used_osm,
                        example_stop_ids=pat.stop_ids[:5],
                        feed_version=feed_version,
                    )
                except Exception:
                    # Fall through to rebuild on cache decode errors.
                    pass

            build_result = build_graph_for_pattern_from_postgis(meta, date_str)
            edge_geoms = build_result.edge_geometries
            used_osm = False
            snapped_pattern_geom = None
            if req.pretty_osm:
                try:
                    osm_res = map_match_pattern(
                        pattern_geom=_merge_edge_geometries(edge_geoms),
                        edge_geometries=edge_geoms,
                    )
                    used_osm = osm_res.used_osm
                    snapped_pattern_geom = osm_res.snapped_pattern_geom
                    edge_geoms = osm_res.snapped_edges
                except Exception:
                    pass
            cache_entry = {
                "graph": build_result.graph,
                "edge_geometries": edge_geoms,
                "pattern": build_result.pattern,
                "used_shape": build_result.used_shape,
                "used_osm_snapping": used_osm,
                "snapped_pattern_geom": snapped_pattern_geom,
                "date": date_str,
            }
            GRAPH_CACHE[key] = cache_entry
            # Persist to PostGIS graph cache keyed by route signature.
            if sig_hash:
                try:
                    db_access_module.save_route_graph_pg(
                        feed_id=feed_id,
                        route_id=req.route_id,
                        direction_id=req.direction_id,
                        date_ymd=date_str,
                        pretty_osm=req.pretty_osm,
                        route_sig_hash=sig_hash,
                        graph_blob=pickle.dumps(cache_entry),
                    )
                except Exception:
                    # Cache write failures should not break the request.
                    pass

            pat = build_result.pattern
            return GraphBuildResponse(
                pattern_id=pat.pattern_id,
                stop_count=len(pat.stop_ids),
                edge_count=len(edge_geoms),
                used_shape=build_result.used_shape,
                used_osm_snapping=used_osm,
                example_stop_ids=pat.stop_ids[:5],
                feed_version=feed_version,
            )
    except (RuntimeError, Exception):
        pass

    feed = None  # SQLite/GTFS loader removed; always build from PostGIS paths now.
    feed_version = "postgis-active"
    key = _graph_cache_key(
        feed_version=feed_version,
        route_id=req.route_id,
        direction_id=req.direction_id,
        date=date_str,
        pretty_osm=req.pretty_osm,
    )

    # 1) Try in-memory cache (skip both pattern building and graph building)
    cached = GRAPH_CACHE.get(key)
    if cached is not None:
        pat: RoutePattern = cached["pattern"]
        edge_geoms = cached["edge_geometries"]
        used_shape = cached["used_shape"]
        used_osm = cached.get("used_osm_snapping", False)
        return GraphBuildResponse(
            pattern_id=pat.pattern_id,
            stop_count=len(pat.stop_ids),
            edge_count=len(edge_geoms),
            used_shape=used_shape,
            used_osm_snapping=used_osm,
            example_stop_ids=pat.stop_ids[:5],
            feed_version=feed_version,
        )

    # 2) Build from scratch and persist to memory + PostGIS cache
    patterns_builder = PatternBuilder(feed)
    patterns = patterns_builder.build_patterns_for_route(
        route_id=req.route_id,
        direction_id=req.direction_id,
        yyyymmdd=date_str,
        max_trips=req.max_trips,
    )
    if not patterns:
        raise HTTPException(status_code=404, detail="No patterns found for route/date")

    chosen = patterns_builder.pick_most_frequent_pattern(patterns)
    assert chosen is not None
    graph_builder = GraphBuilder(feed)
    build_result = graph_builder.build_graph_for_pattern(chosen)

    used_osm = False
    snapped_pattern_geom = None

    if req.pretty_osm:
        osm_res = map_match_pattern(
            pattern_geom=_merge_edge_geometries(build_result.edge_geometries),
            edge_geometries=build_result.edge_geometries,
        )
        used_osm = osm_res.used_osm
        snapped_pattern_geom = osm_res.snapped_pattern_geom
        edge_geoms = osm_res.snapped_edges
    else:
        edge_geoms = build_result.edge_geometries

    cache_entry = {
        "graph": build_result.graph,
        "edge_geometries": edge_geoms,
        "pattern": chosen,
        "used_shape": build_result.used_shape,
        "used_osm_snapping": used_osm,
        "snapped_pattern_geom": snapped_pattern_geom,
        "date": date_str,
    }
    GRAPH_CACHE[key] = cache_entry
    try:
        save_route_graph_blob(
            feed_version=feed_version,
            route_id=req.route_id,
            direction_id=req.direction_id or "",
            date_ymd=date_str,
            pretty_osm=req.pretty_osm,
            blob=pickle.dumps(cache_entry),
        )
    except Exception:
        # DB cache failures should not break the request.
        pass

    return GraphBuildResponse(
        pattern_id=chosen.pattern_id,
        stop_count=len(chosen.stop_ids),
        edge_count=len(edge_geoms),
        used_shape=build_result.used_shape,
        used_osm_snapping=used_osm,
        example_stop_ids=chosen.stop_ids[:5],
        feed_version=feed_version,
    )


def _merge_edge_geometries(edge_geometries: Dict[Tuple[str, str], any]):
    # Concatenate all edges in order of from->to; this is a heuristic
    if not edge_geometries:
        return None
    # Assume edges follow pattern order; just link them in insertion order
    from shapely.geometry import LineString

    coords = []
    for eg in edge_geometries.values():
        if not coords:
            coords.extend(list(eg.linestring.coords))
        else:
            coords.extend(list(eg.linestring.coords)[1:])
    return LineString(coords)


@app.get("/graph/stops", response_model=GraphStopsResponse)
def graph_stops(
    route_id: str,
    direction_id: str | None = None,
    pattern_id: str | None = None,
    date: str | None = None,
):
    """
    Return ordered stops for the pattern used to build the graph.

    Priority:
      1. If a matching pattern is present in GRAPH_CACHE (from /graph/build),
         use that (avoids date mismatches and extra GTFS scans).
      2. Otherwise, fall back to rebuilding patterns for the given date.
    Stops are resolved from feed when available, else from PostGIS via db_access.
    """
    try:
        feed = load_active_feed()
    except Exception:
        feed = None

    # 1) Try to reuse an existing cached graph pattern first.
    cached_pattern: RoutePattern | None = None
    if pattern_id:
        for entry in GRAPH_CACHE.values():
            pat: RoutePattern = entry.get("pattern")
            if pat and pat.route_id == route_id and pat.pattern_id == pattern_id:
                cached_pattern = pat
                break
    else:
        for entry in GRAPH_CACHE.values():
            pat: RoutePattern = entry.get("pattern")
            if pat and pat.route_id == route_id:
                cached_pattern = pat
                break

    if cached_pattern is not None:
        chosen = cached_pattern
    else:
        if feed is None:
            raise HTTPException(
                status_code=503,
                detail="No cached pattern and GTFS feed not loaded; call /graph/build first or load feed.",
            )
        date_str = date or datetime.utcnow().strftime("%Y%m%d")
        patterns_builder = PatternBuilder(feed)
        patterns = patterns_builder.build_patterns_for_route(
            route_id=route_id,
            direction_id=direction_id,
            yyyymmdd=date_str,
            max_trips=None,
        )
        if not patterns:
            raise HTTPException(status_code=404, detail="No patterns found for route/date")

        if pattern_id and pattern_id in patterns:
            chosen = patterns[pattern_id]
        else:
            chosen = patterns_builder.pick_most_frequent_pattern(patterns)
            assert chosen is not None

    res_stops: list[GraphStopsResponseStop] = []
    if feed is not None:
        stops_by_id = {s["stop_id"]: s for s in feed.stops}
        for idx, sid in enumerate(chosen.stop_ids):
            st = stops_by_id.get(sid)
            if not st:
                continue
            res_stops.append(
                GraphStopsResponseStop(
                    stop_id=sid,
                    name=st.get("stop_name"),
                    lat=float(st["stop_lat"]),
                    lon=float(st["stop_lon"]),
                    sequence=idx,
                )
            )
    else:
        try:
            stops_meta = db_access_module.get_pattern_stops(chosen.pattern_id)
            for idx, s in enumerate(stops_meta):
                res_stops.append(
                    GraphStopsResponseStop(
                        stop_id=s.stop_id,
                        name=s.name,
                        lat=s.lat,
                        lon=s.lon,
                        sequence=idx,
                    )
                )
        except (RuntimeError, Exception):
            raise HTTPException(
                status_code=503,
                detail="Could not load stop details from PostGIS.",
            )

    return GraphStopsResponse(pattern_id=chosen.pattern_id, stops=res_stops)


@app.post("/detour", response_model=DetourResponse)
def detour(req: DetourRequest):
    blockage_geojson = _ensure_geometry(req.blockage_geojson)
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")

    # Simple debug log so we can see each detour request without retyping.
    print(
        f"[detour] route_id={req.route_id!r}, direction_id={req.direction_id!r}, "
        f"date={date_str}, start_stop_id={req.start_stop_id!r}, "
        f"end_stop_id={req.end_stop_id!r}",
        flush=True,
    )

    # Graph must have been built earlier with /graph/build.
    # Try both PostGIS and feed-based cache keys.
    candidate_keys = _graph_cache_keys_for_lookup(
        req.route_id, req.direction_id, date_str
    )
    cache = None
    cache_key_used = None
    for key in candidate_keys:
        if key in GRAPH_CACHE:
            cache = GRAPH_CACHE[key]
            cache_key_used = key
            break
    if cache is None:
        raise HTTPException(
            status_code=400,
            detail="Graph not built yet; call /graph/build first.",
        )
    feed_version = cache_key_used.split("|")[0] if cache_key_used else ""
    try:
        feed = load_active_feed()
    except Exception:
        feed = None

    graph = cache["graph"]
    edge_geometries = cache["edge_geometries"]
    used_shape = cache["used_shape"]
    used_osm_snapping = cache["used_osm_snapping"]

    # Graph uses pattern-stop nodes; resolve physical stop_ids to node_ids.
    start_node = next(
        (n for n in graph.nodes() if graph.nodes[n].get("stop_id") == req.start_stop_id),
        None,
    )
    end_node = next(
        (n for n in graph.nodes() if graph.nodes[n].get("stop_id") == req.end_stop_id),
        None,
    )
    if not start_node or not end_node:
        raise HTTPException(
            status_code=400,
            detail="start_stop_id or end_stop_id not found in graph.",
        )

    baseline_travel_time_s: Optional[float] = None
    baseline_distance_m: Optional[float] = None
    baseline_path: list[str] = []
    try:
        baseline_path = astar_route(
            graph=graph,
            edge_geometries=edge_geometries,
            start_stop_id=start_node,
            end_stop_id=end_node,
            blocked_edges=set(),
        )
        bt = 0.0
        bd = 0.0
        for i in range(len(baseline_path) - 1):
            u = baseline_path[i]
            v = baseline_path[i + 1]
            edge_data = graph.get_edge_data(u, v, default={})
            bt += float(edge_data.get("travel_time_s", 0.0))
            bd += float(edge_data.get("distance_m", 0.0))
        baseline_travel_time_s = bt
        baseline_distance_m = bd
    except Exception:
        baseline_travel_time_s = None
        baseline_distance_m = None

    # Primary-route blocked edges (for union with detour graph blocked set).
    blocked_edges, blocked_edges_geojson = compute_blocked_edges(
        edge_geometries=edge_geometries, blockage_geojson=blockage_geojson
    )

    path: list[str]
    path_geojson: Dict
    total_travel_time_s: float
    total_distance_m: float
    used_osm_detour = False

    # Always build a multi-route GTFS detour graph (selected route + routes in buffered AOI + transfers).
    # Uses buffered AOI for candidate routes so nearby parallel corridors are included.
    detour_graph_res = build_detour_graph(
        feed=feed,
        date_ymd=date_str,
        blockage_geojson=blockage_geojson,
        primary_route_id=req.route_id,
        primary_direction_id=req.direction_id,
        transfer_radius_m=200.0,
        start_sec=None,
        end_sec=None,
    )
    detour_blocked, detour_blocked_geojson = compute_blocked_edges(
        edge_geometries=detour_graph_res.edge_geometries,
        blockage_geojson=blockage_geojson,
    )
    blocked_for_routing = blocked_edges | detour_blocked

    g = detour_graph_res.graph
    primary_pid = getattr(detour_graph_res, "primary_pattern_id", None)

    def _node_matches_stop(n, stop_id):
        return g.nodes[n].get("stop_id") == stop_id

    def _prefer_primary(n):
        if not primary_pid:
            return True
        return g.nodes[n].get("pattern_id") == primary_pid

    detour_start = next(
        (n for n in g.nodes() if _node_matches_stop(n, req.start_stop_id)),
        None,
    )
    detour_end = next(
        (n for n in g.nodes() if _node_matches_stop(n, req.end_stop_id)),
        None,
    )
    if detour_start and primary_pid and g.nodes[detour_start].get("pattern_id") != primary_pid:
        alt = next((n for n in g.nodes() if _node_matches_stop(n, req.start_stop_id) and _prefer_primary(n)), None)
        if alt:
            detour_start = alt
    if detour_end and primary_pid and g.nodes[detour_end].get("pattern_id") != primary_pid:
        alt = next((n for n in g.nodes() if _node_matches_stop(n, req.end_stop_id) and _prefer_primary(n)), None)
        if alt:
            detour_end = alt

    if not detour_start or not detour_end:
        raise HTTPException(
            status_code=400,
            detail="start_stop_id or end_stop_id not in detour graph.",
        )

    # Route on the multi-route graph (GTFS only); do not route on the single-route cache.
    try:
        path_nodes = astar_route(
            graph=detour_graph_res.graph,
            edge_geometries=detour_graph_res.edge_geometries,
            start_stop_id=detour_start,
            end_stop_id=detour_end,
            blocked_edges=blocked_for_routing,
        )
    except Exception:
        # Detour must be computable on the GTFS/PostGIS graph; we do not fall back
        # to a separate road-graph router here so that the chosen path always comes
        # from the same graph that enforces the blockage.
        raise HTTPException(
            status_code=409,
            detail="No detour path found on GTFS routes; adjust blockage or parameters.",
        )

    path = [detour_graph_res.graph.nodes[n]["stop_id"] for n in path_nodes]
    path_geojson = collect_path_geojson(
        edge_geometries=detour_graph_res.edge_geometries,
        path=path_nodes,
    )
    total_travel_time_s = 0.0
    total_distance_m = 0.0
    for i in range(len(path_nodes) - 1):
        u, v = path_nodes[i], path_nodes[i + 1]
        edge_data = detour_graph_res.graph.get_edge_data(u, v, default={})
        total_travel_time_s += float(edge_data.get("travel_time_s", 0.0))
        total_distance_m += float(edge_data.get("distance_m", 0.0))
    blocked_edges = detour_blocked
    blocked_edges_geojson = detour_blocked_geojson

    detour_delay_s: Optional[float] = None
    detour_extra_distance_m: Optional[float] = None
    if baseline_travel_time_s is not None and baseline_distance_m is not None:
        detour_delay_s = total_travel_time_s - baseline_travel_time_s
        detour_extra_distance_m = total_distance_m - baseline_distance_m

    print(
        f"[detour] result route_id={req.route_id!r}, direction_id={req.direction_id!r}, "
        f"blocked_edges={len(blocked_edges)}, "
        f"stop_path_len={len(path)}, "
        f"total_travel_time_s={total_travel_time_s:.1f}, "
        f"baseline_travel_time_s={baseline_travel_time_s if baseline_travel_time_s is not None else 'N/A'}",
        flush=True,
    )

    return DetourResponse(
        blocked_edges_count=len(blocked_edges),
        stop_path=path,
        path_geojson=path_geojson,
        blocked_edges_geojson=blocked_edges_geojson,
        total_travel_time_s=total_travel_time_s,
        total_distance_m=total_distance_m,
        baseline_travel_time_s=baseline_travel_time_s,
        baseline_distance_m=baseline_distance_m,
        detour_delay_s=detour_delay_s,
        detour_extra_distance_m=detour_extra_distance_m,
        used_shape=used_shape,
        used_osm_snapping=used_osm_snapping,
        feed_version=feed_version,
    )


@app.post("/detour/osm", response_model=DetourResponse)
def detour_osm(req: DetourRequest):
    """
    Experimental: compute the detour segment on the OSM/Valhalla road graph.

    Flow:
    - Use the cached single-route GTFS graph to:
      - compute the baseline stop-to-stop path
      - detect which baseline edges are blocked by the polygon
      - locate the last unblocked stop before the blockage and the first after
    - Ask Valhalla /route with exclude_polygons so the road-level path between
      those two stops avoids the blockage.
    - Splice: GTFS edges before + OSM detour polyline + GTFS edges after.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        raise HTTPException(
            status_code=503,
            detail="VALHALLA_URL is not configured; OSM-based detours are unavailable.",
        )

    blockage_geojson = _ensure_geometry(req.blockage_geojson)
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")

    # Graph must have been built earlier with /graph/build.
    candidate_keys = _graph_cache_keys_for_lookup(
        req.route_id, req.direction_id, date_str
    )
    cache = None
    for key in candidate_keys:
        if key in GRAPH_CACHE:
            cache = GRAPH_CACHE[key]
            break
    if cache is None:
        raise HTTPException(
            status_code=400,
            detail="Graph not built yet; call /graph/build first.",
        )

    graph = cache["graph"]
    edge_geometries = cache["edge_geometries"]
    used_shape = cache["used_shape"]
    used_osm_snapping = cache["used_osm_snapping"]
    feed_version = cache.get("date", "")

    # Resolve physical stop_ids to node_ids on the single-route graph.
    start_node = next(
        (n for n in graph.nodes() if graph.nodes[n].get("stop_id") == req.start_stop_id),
        None,
    )
    end_node = next(
        (n for n in graph.nodes() if graph.nodes[n].get("stop_id") == req.end_stop_id),
        None,
    )
    if not start_node or not end_node:
        raise HTTPException(
            status_code=400,
            detail="start_stop_id or end_stop_id not found in graph.",
        )

    # Baseline path on the GTFS graph (no blocking yet).
    try:
        baseline_path = astar_route(
            graph=graph,
            edge_geometries=edge_geometries,
            start_stop_id=start_node,
            end_stop_id=end_node,
            blocked_edges=set(),
        )
    except Exception:
        raise HTTPException(
            status_code=409,
            detail="No baseline path found between the selected stops.",
        )

    # Identify which edges of the baseline path are blocked by the polygon.
    blocked_edges, blocked_edges_geojson = compute_blocked_edges(
        edge_geometries=edge_geometries, blockage_geojson=blockage_geojson
    )
    if not blocked_edges:
        # Nothing on the baseline path is actually blocked; return the baseline GTFS path.
        path = [graph.nodes[n]["stop_id"] for n in baseline_path]
        path_geojson = collect_path_geojson(
            edge_geometries=edge_geometries,
            path=baseline_path,
        )
        total_travel_time_s = 0.0
        total_distance_m = 0.0
        for i in range(len(baseline_path) - 1):
            u, v = baseline_path[i], baseline_path[i + 1]
            ed = graph.get_edge_data(u, v, default={})
            total_travel_time_s += float(ed.get("travel_time_s", 0.0))
            total_distance_m += float(ed.get("distance_m", 0.0))
        return DetourResponse(
            blocked_edges_count=0,
            stop_path=path,
            path_geojson=path_geojson,
            blocked_edges_geojson=blocked_edges_geojson,
            total_travel_time_s=total_travel_time_s,
            total_distance_m=total_distance_m,
            baseline_travel_time_s=total_travel_time_s,
            baseline_distance_m=total_distance_m,
            detour_delay_s=0.0,
            detour_extra_distance_m=0.0,
            used_shape=used_shape,
            used_osm_snapping=used_osm_snapping,
            feed_version=str(feed_version),
        )

    # Find first and last indices on the baseline path that correspond to blocked edges.
    i_first: Optional[int] = None
    i_last: Optional[int] = None
    for i in range(len(baseline_path) - 1):
        uv = (baseline_path[i], baseline_path[i + 1])
        if uv in blocked_edges:
            if i_first is None:
                i_first = i
            i_last = i
    if i_first is None or i_last is None:
        raise HTTPException(
            status_code=409,
            detail="Could not locate a blocked span on the baseline path.",
        )

    # Stops immediately before and after the blocked span.
    stop_before = baseline_path[i_first]
    stop_after = baseline_path[i_last + 1]
    na = graph.nodes.get(stop_before, {})
    nb = graph.nodes.get(stop_after, {})
    lon_a, lat_a = na.get("lon"), na.get("lat")
    lon_b, lat_b = nb.get("lon"), nb.get("lat")
    if None in (lon_a, lat_a, lon_b, lat_b):
        raise HTTPException(
            status_code=500,
            detail="Could not resolve coordinates for entry/exit stops of the blocked span.",
        )

    # Ask Valhalla to compute a detour on the bus/road graph that avoids the blockage polygon.
    osm = route_avoiding_polygon(
        float(lon_a),
        float(lat_a),
        float(lon_b),
        float(lat_b),
        blockage_geojson,
    )
    if not osm.success or not osm.coordinates:
        raise HTTPException(
            status_code=409,
            detail="Valhalla could not compute a detour around the blockage.",
        )

    from shapely.geometry import mapping

    # Splice GTFS edges before + OSM detour segment + GTFS edges after into one FeatureCollection.
    features = []
    # GTFS before
    for i in range(i_first):
        u, v = baseline_path[i], baseline_path[i + 1]
        eg = edge_geometries.get((u, v))
        if eg:
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(eg.linestring),
                    "properties": {
                        "from_stop_id": eg.from_stop_id,
                        "to_stop_id": eg.to_stop_id,
                    },
                }
            )
    # OSM detour
    features.append(
        {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": list(osm.coordinates)},
            "properties": {"kind": "osm_detour"},
        }
    )
    # GTFS after
    for i in range(i_last + 1, len(baseline_path) - 1):
        u, v = baseline_path[i], baseline_path[i + 1]
        eg = edge_geometries.get((u, v))
        if eg:
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(eg.linestring),
                    "properties": {
                        "from_stop_id": eg.from_stop_id,
                        "to_stop_id": eg.to_stop_id,
                    },
                }
            )

    path_geojson = {"type": "FeatureCollection", "features": features}

    # Accumulate baseline and detour times/distances.
    baseline_travel_time_s = 0.0
    baseline_distance_m = 0.0
    for i in range(len(baseline_path) - 1):
        u, v = baseline_path[i], baseline_path[i + 1]
        ed = graph.get_edge_data(u, v, default={})
        baseline_travel_time_s += float(ed.get("travel_time_s", 0.0))
        baseline_distance_m += float(ed.get("distance_m", 0.0))

    total_travel_time_s = 0.0
    total_distance_m = 0.0
    for i in range(i_first):
        u, v = baseline_path[i], baseline_path[i + 1]
        ed = graph.get_edge_data(u, v, default={})
        total_travel_time_s += float(ed.get("travel_time_s", 0.0))
        total_distance_m += float(ed.get("distance_m", 0.0))
    total_travel_time_s += osm.time_s
    total_distance_m += osm.distance_m
    for i in range(i_last + 1, len(baseline_path) - 1):
        u, v = baseline_path[i], baseline_path[i + 1]
        ed = graph.get_edge_data(u, v, default={})
        total_travel_time_s += float(ed.get("travel_time_s", 0.0))
        total_distance_m += float(ed.get("distance_m", 0.0))

    # stop_path excludes the internal pattern-stop ids; return physical stop_ids.
    path_nodes_osm = baseline_path[: i_first + 1] + baseline_path[i_last + 1 :]
    stop_path = [graph.nodes[n]["stop_id"] for n in path_nodes_osm]

    detour_delay_s = total_travel_time_s - baseline_travel_time_s
    detour_extra_distance_m = total_distance_m - baseline_distance_m

    return DetourResponse(
        blocked_edges_count=len(blocked_edges),
        stop_path=stop_path,
        path_geojson=path_geojson,
        blocked_edges_geojson=blocked_edges_geojson,
        total_travel_time_s=total_travel_time_s,
        total_distance_m=total_distance_m,
        baseline_travel_time_s=baseline_travel_time_s,
        baseline_distance_m=baseline_distance_m,
        detour_delay_s=detour_delay_s,
        detour_extra_distance_m=detour_extra_distance_m,
        used_shape=used_shape,
        used_osm_snapping=used_osm_snapping,
        feed_version=str(feed_version),
    )


@app.get("/graph/geojson")
def graph_geojson(
    route_id: str,
    direction_id: str | None = None,
    pattern_id: str | None = None,
    date: str | None = None,
    pretty_osm: bool = False,
):
    """
    Optional endpoint returning full route pattern polyline and stops for display.
    """
    date_str = date or datetime.utcnow().strftime("%Y%m%d")
    candidate_keys = _graph_cache_keys_for_lookup(route_id, direction_id, date_str)
    want_osm = "osm" if pretty_osm else "gtfs"
    cache = None
    fallback_cache = None
    for key in candidate_keys:
        if key in GRAPH_CACHE:
            if want_osm in key:
                cache = GRAPH_CACHE[key]
                break
            if fallback_cache is None:
                fallback_cache = GRAPH_CACHE[key]
    if cache is None:
        cache = fallback_cache
    if not cache:
        raise HTTPException(
            status_code=400,
            detail="Graph not built yet; call /graph/build first.",
        )

    from shapely.geometry import mapping, Point

    pattern = cache["pattern"]
    edge_geometries = cache["edge_geometries"]
    snapped_pattern_geom = cache.get("snapped_pattern_geom")
    graph = cache.get("graph")

    # Build stop features from graph nodes (works for both PostGIS and feed-backed cache).
    stop_features = []
    if graph is not None:
        for sid in pattern.stop_ids:
            node = next(
                (n for n, d in graph.nodes(data=True) if d.get("stop_id") == sid),
                None,
            )
            if node is None:
                continue
            d = graph.nodes[node]
            lat, lon = d.get("lat"), d.get("lon")
            if lat is None or lon is None:
                continue
            pt = Point(float(lon), float(lat))
            stop_features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(pt),
                    "properties": {"stop_id": sid, "name": d.get("stop_name")},
                }
            )
    else:
        try:
            feed = load_active_feed()
            stops_by_id = {s["stop_id"]: s for s in feed.stops}
            for sid in pattern.stop_ids:
                st = stops_by_id.get(sid)
                if not st:
                    continue
                pt = Point(float(st["stop_lon"]), float(st["stop_lat"]))
                stop_features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(pt),
                        "properties": {"stop_id": sid, "name": st.get("stop_name")},
                    }
                )
        except Exception:
            pass

    edge_features = []
    for (u, v), eg in edge_geometries.items():
        edge_features.append(
            {
                "type": "Feature",
                "geometry": mapping(eg.linestring),
                "properties": {"from_stop_id": eg.from_stop_id, "to_stop_id": eg.to_stop_id},
            }
        )

    features = stop_features + edge_features

    if snapped_pattern_geom is not None:
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(snapped_pattern_geom),
                "properties": {"kind": "pattern_snapped"},
            }
        )

    return {"type": "FeatureCollection", "features": features}


def _parse_hhmm_to_seconds(value: str) -> int:
    """
    Parses HH:MM or HH:MM:SS into seconds since 00:00. Falls back to 0 on
    invalid values.
    """
    if not value:
        return 0
    parts = value.split(":")
    try:
        if len(parts) == 2:
            h, m = map(int, parts)
            s = 0
        elif len(parts) == 3:
            h, m, s = map(int, parts)
        else:
            return 0
        return h * 3600 + m * 60 + s
    except ValueError:
        return 0


def _build_replaced_segment_geojson(
    pattern_id: str,
    stop_ids: list[str],
    i_first: int,
    i_last: int,
    edge_geometries: Dict[Tuple[str, str], any],
) -> Dict:
    from shapely.geometry import mapping

    features: list[Dict] = []
    for i in range(i_first, i_last + 1):
        node_u = pattern_stop_node_id(pattern_id, stop_ids[i], i)
        node_v = pattern_stop_node_id(pattern_id, stop_ids[i + 1], i + 1)
        eg = edge_geometries.get((node_u, node_v))
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


def _compute_route_detour_by_area(
    feed,
    date_str: str,
    start_sec: int,
    end_sec: int,
    blockage_geojson: Dict,
    route_id: str,
    direction_id: Optional[str],
    transfer_radius_m: float,
) -> DetourByAreaRouteResult:
    # Respect time window: if the route has no active trips in the window,
    # treat it as unaffected for this request.
    # Use PostGIS trip_time_bounds to quickly filter routes with no active trips
    # in the requested window.
    try:
        bounds = db_access_module.get_trip_time_bounds_pg()
    except Exception:
        bounds = {}
    if bounds:
        svc_cal = ServiceCalendar(feed)
        active_services = svc_cal.active_service_ids_for_date(date_str)
        has_trip_in_window = False
        for trip in feed.trips:
            if trip.get("route_id") != route_id:
                continue
            if direction_id is not None and str(trip.get("direction_id")) != str(direction_id):
                continue
            if trip.get("service_id") not in active_services:
                continue
            tb = bounds.get(trip.get("trip_id"))
            if not tb:
                continue
            lo, hi = tb
            if hi >= start_sec and lo <= end_sec:
                has_trip_in_window = True
                break
        if not has_trip_in_window:
            return DetourByAreaRouteResult(
                route_id=route_id,
                direction_id=direction_id,
                pattern_id=None,
                blocked_edges_count=0,
                detour_stop_path=[],
                detour_geojson={"type": "FeatureCollection", "features": []},
                replaced_segment_geojson={"type": "FeatureCollection", "features": []},
                used_transfers=False,
                error=None,
            )

    patterns_builder = PatternBuilder(feed)
    patterns = patterns_builder.build_patterns_for_route(
        route_id=route_id,
        direction_id=direction_id,
        yyyymmdd=date_str,
        max_trips=None,
    )
    chosen = patterns_builder.pick_most_frequent_pattern(patterns) if patterns else None
    if chosen is None:
        return DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=direction_id,
            pattern_id=None,
            blocked_edges_count=0,
            error="No patterns found for route/date",
        )

    graph_builder = GraphBuilder(feed)
    build_result = graph_builder.build_graph_for_pattern(chosen)

    blocked_edges, _ = compute_blocked_edges(
        edge_geometries=build_result.edge_geometries,
        blockage_geojson=blockage_geojson,
    )
    if not blocked_edges:
        # Route not affected by this blockage.
        return DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=0,
            detour_stop_path=[],
            detour_geojson={"type": "FeatureCollection", "features": []},
            replaced_segment_geojson={"type": "FeatureCollection", "features": []},
            used_transfers=False,
            error=None,
        )

    # blocked_edges are (node_id, node_id) for pattern-stop graph. Find span on primary pattern.
    stop_ids = chosen.stop_ids
    pid = chosen.pattern_id
    seqs: List[int] = []
    for (u, v) in blocked_edges:
        pat_u, _, seq_u = parse_pattern_stop_node_id(u)
        pat_v, _, seq_v = parse_pattern_stop_node_id(v)
        if pat_u == pid:
            seqs.append(seq_u)
        if pat_v == pid:
            seqs.append(seq_v)
    if not seqs:
        return DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=len(blocked_edges),
            error="Could not map blocked edges to pattern chain",
        )
    i_first = min(seqs)
    i_last = max(seqs)
    if i_last >= len(stop_ids) - 1:
        i_last = len(stop_ids) - 2
    stop_before = stop_ids[i_first]
    stop_after = stop_ids[i_last + 1]

    start_node = pattern_stop_node_id(pid, stop_before, i_first)
    end_node = pattern_stop_node_id(pid, stop_after, i_last + 1)

    detour_graph_res = build_detour_graph(
        feed=feed,
        date_ymd=date_str,
        blockage_geojson=blockage_geojson,
        primary_route_id=route_id,
        primary_direction_id=direction_id,
        transfer_radius_m=transfer_radius_m,
        start_sec=start_sec,
        end_sec=end_sec,
    )
    detour_blocked, _ = compute_blocked_edges(
        edge_geometries=detour_graph_res.edge_geometries,
        blockage_geojson=blockage_geojson,
    )
    blocked_for_routing = blocked_edges | detour_blocked
    try:
        detour_path_nodes = astar_route(
            graph=detour_graph_res.graph,
            edge_geometries=detour_graph_res.edge_geometries,
            start_stop_id=start_node,
            end_stop_id=end_node,
            blocked_edges=blocked_for_routing,
        )
    except Exception:
        return DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=len(blocked_edges),
            error="No detour path found between entry/exit stops",
        )

    detour_geojson = collect_path_geojson(
        edge_geometries=detour_graph_res.edge_geometries,
        path=detour_path_nodes,
    )
    replaced_segment_geojson = _build_replaced_segment_geojson(
        pattern_id=pid,
        stop_ids=stop_ids,
        i_first=i_first,
        i_last=i_last,
        edge_geometries=build_result.edge_geometries,
    )

    detour_stop_path = [detour_graph_res.graph.nodes[n]["stop_id"] for n in detour_path_nodes]

    used_transfers = False
    for u, v in zip(detour_path_nodes, detour_path_nodes[1:]):
        data = detour_graph_res.graph.get_edge_data(u, v, default={})
        if data.get("is_transfer"):
            used_transfers = True
            break

    return DetourByAreaRouteResult(
        route_id=route_id,
        direction_id=chosen.direction_id,
        pattern_id=chosen.pattern_id,
        blocked_edges_count=len(blocked_edges),
        stop_before=stop_before,
        stop_after=stop_after,
        detour_stop_path=detour_stop_path,
        detour_geojson=detour_geojson,
        replaced_segment_geojson=replaced_segment_geojson,
        used_transfers=used_transfers,
        error=None,
    )


def _ensure_geometry(geojson: dict) -> dict:
    """If the payload is a GeoJSON Feature, return its geometry; otherwise return as-is."""
    if isinstance(geojson, dict) and geojson.get("type") == "Feature":
        return geojson.get("geometry") or {}
    return geojson


def _normalize_area_geometry(geojson: dict) -> dict:
    """
    Accept GeoJSON Geometry or Feature; convert Point/LineString to Polygon via buffer
    so that area search can find routes that pass through the drawn shape.
    """
    from shapely.geometry import shape, mapping

    geom = _ensure_geometry(geojson)
    if not isinstance(geom, dict) or not geom.get("coordinates"):
        raise HTTPException(status_code=400, detail="polygon_geojson must be a GeoJSON Geometry or Feature")
    g = shape(geom)
    if g.is_empty:
        raise HTTPException(status_code=400, detail="polygon_geojson is empty")
    if g.geom_type == "Point":
        g = g.buffer(0.001)  # ~100 m
    elif g.geom_type == "LineString":
        g = g.buffer(0.00035)  # ~35 m
    elif g.geom_type not in ("Polygon", "MultiPolygon"):
        raise HTTPException(
            status_code=400,
            detail="polygon_geojson must be Polygon, LineString, or Point",
        )
    return mapping(g)


@app.post("/area/routes", response_model=AreaRoutesResponse)
def area_routes(req: AreaRoutesQuery):
    """
    Given a polygon and time window on a specific service date, return the list
    of routes whose shapes intersect that polygon and have at least one trip
    running during that window. Accepts Polygon, LineString, or Point (buffered).
    """
    if not req.polygon_geojson:
        raise HTTPException(status_code=400, detail="polygon_geojson is required")

    if not (req.date and len(req.date) == 8 and req.date.isdigit()):
        raise HTTPException(status_code=400, detail="date must be YYYYMMDD (e.g. 20250303)")

    start_sec = _parse_hhmm_to_seconds(req.start_time)
    end_sec = _parse_hhmm_to_seconds(req.end_time)
    if end_sec < start_sec:
        raise HTTPException(
            status_code=400, detail="end_time must be greater than or equal to start_time"
        )

    try:
        polygon_geojson = _normalize_area_geometry(req.polygon_geojson)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid polygon_geojson: {e}")

    try:
        routes_raw = find_routes_in_polygon(
            feed=None,
            polygon_geojson=polygon_geojson,
            yyyymmdd=req.date,
            start_sec=start_sec,
            end_sec=end_sec,
        )
    except HTTPException:
        raise
    except Exception as e:
        # Log full traceback to help diagnose encoding / PostGIS issues.
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Area search failed: {e}")

    def _fmt_time(sec: Optional[int]) -> Optional[str]:
        if sec is None:
            return None
        if sec < 0:
            sec = 0
        h = sec // 3600
        m = (sec % 3600) // 60
        return f"{h:02d}:{m:02d}"

    results: list[AreaRouteResult] = []
    for r in routes_raw:
        dir_id_val = r.get("direction_id")
        dir_id_str = str(dir_id_val) if dir_id_val is not None else None
        results.append(
            AreaRouteResult(
                route_id=r["route_id"],
                direction_id=dir_id_str,
                route_short_name=r.get("route_short_name"),
                route_long_name=r.get("route_long_name"),
                agency_id=r.get("agency_id"),
                agency_name=r.get("agency_name"),
                first_time=_fmt_time(r.get("first_time_s")),
                last_time=_fmt_time(r.get("last_time_s")),
            )
        )

    # Sort by route_short_name then route_id for a stable, human-friendly list.
    results.sort(
        key=lambda x: (
            (x.route_short_name or x.route_id or ""),
            (x.route_id or ""),
            (x.direction_id or ""),
        )
    )

    if req.max_results and len(results) > req.max_results:
        results = results[: req.max_results]

    return AreaRoutesResponse(routes=results)


@app.post("/detours/by-area", response_model=DetourByAreaResponse)
def detours_by_area(req: DetourByAreaRequest):
    """
    Compute detours for routes affected by a blockage polygon.

    mode='route': detour a single selected route:
      - detect blocked segment on its main pattern
      - automatically choose entry/exit stops
      - compute a multi-route detour between them

    mode='all': compute detours for all routes whose shapes intersect the polygon
      within the given date/time window (up to max_routes).
    """
    if not req.blockage_geojson:
        raise HTTPException(status_code=400, detail="blockage_geojson is required")

    if not (req.date and len(req.date) == 8 and req.date.isdigit()):
        raise HTTPException(status_code=400, detail="date must be YYYYMMDD (e.g. 20250303)")

    start_sec = _parse_hhmm_to_seconds(req.start_time)
    end_sec = _parse_hhmm_to_seconds(req.end_time)
    if end_sec < start_sec:
        raise HTTPException(
            status_code=400, detail="end_time must be greater than or equal to start_time"
        )

    try:
        polygon_geojson = _normalize_area_geometry(req.blockage_geojson)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid blockage_geojson: {e}")

    try:
        feed = load_active_feed()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"GTFS feed could not be loaded. Place israel-public-transportation.zip in the project root or run /feed/update. ({e})",
        )

    print(
        f"[detours/by-area] mode={req.mode}, date={req.date}, "
        f"time_window={req.start_time}-{req.end_time}, "
        f"max_routes={req.max_routes}, transfer_radius_m={req.transfer_radius_m}",
        flush=True,
    )

    if req.mode == DetourByAreaMode.route:
        if not req.route_id:
            raise HTTPException(status_code=400, detail="route_id is required when mode='route'")
        result = _compute_route_detour_by_area(
            feed=feed,
            date_str=req.date,
            start_sec=start_sec,
            end_sec=end_sec,
            blockage_geojson=polygon_geojson,
            route_id=req.route_id,
            direction_id=req.direction_id,
            transfer_radius_m=req.transfer_radius_m,
        )
        print(
            f"[detours/by-area] single route_id={result.route_id!r}, "
            f"direction_id={result.direction_id!r}, "
            f"blocked_edges={result.blocked_edges_count}, "
            f"used_transfers={result.used_transfers}, "
            f"error={result.error!r}",
            flush=True,
        )
        return DetourByAreaResponse(mode=req.mode, result=result, feed_version=feed.version_id)

    # mode == 'all'
    # Find all routes in area/time window (capped by max_routes) and compute per-route detours.
    try:
        routes_raw = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd=req.date,
            start_sec=start_sec,
            end_sec=end_sec,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Area search failed: {e}")

    # Deduplicate by (route_id, direction_id) and cap results.
    seen: set[tuple[str, Optional[str]]] = set()
    candidates: list[tuple[str, Optional[str]]] = []
    for r in routes_raw:
        key = (r["route_id"], r.get("direction_id"))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(key)
        if len(candidates) >= req.max_routes:
            break

    results: list[DetourByAreaRouteResult] = []
    for route_id, direction_id in candidates:
        res = _compute_route_detour_by_area(
            feed=feed,
            date_str=req.date,
            start_sec=start_sec,
            end_sec=end_sec,
            blockage_geojson=polygon_geojson,
            route_id=route_id,
            direction_id=direction_id,
            transfer_radius_m=req.transfer_radius_m,
        )
        results.append(res)

    print(
        f"[detours/by-area] computed {len(results)} route detours "
        f"for blockage on date={req.date}, "
        f"time_window={req.start_time}-{req.end_time}",
        flush=True,
    )

    return DetourByAreaResponse(mode=req.mode, results=results, feed_version=feed.version_id)


@app.post("/stop/routes", response_model=StopRoutesResponse)
def stop_routes(req: StopRoutesRequest):
    """Return routes that serve the given stop on the given date within the time window."""
    start_sec = _parse_hhmm_to_seconds(req.start_time)
    end_sec = _parse_hhmm_to_seconds(req.end_time)
    if end_sec < start_sec:
        raise HTTPException(
            status_code=400,
            detail="end_time must be greater than or equal to start_time",
        )
    try:
        routes_raw = db_access_module.get_routes_serving_stop_pg(
            stop_id=req.stop_id,
            yyyymmdd=req.date,
            start_sec=start_sec,
            end_sec=end_sec,
            max_results=req.max_results,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stop routes failed: {e}")
    return StopRoutesResponse(
        stop_id=req.stop_id,
        routes=[StopRouteResult(**r) for r in routes_raw],
    )

