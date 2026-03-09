from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple, Optional
import hashlib
import pickle

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from backend.gtfs_loader import load_active_feed, get_routes_search_index
from backend.gtfs_updater import update_feed, get_feed_status
from backend.pattern_builder import PatternBuilder, RoutePattern
from backend.graph_builder import (
    GraphBuilder,
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
from backend.sqlite_db import (
    search_routes,
    stops_in_bounds_sqlite,
    get_route_graph_blob,
    save_route_graph_blob,
    get_route_graph_v2,
    get_route_graphs_v2_for_route,
    get_pattern_record,
    get_pattern_stops,
    get_trip_time_bounds_from_db,
)
from backend.area_search import find_routes_in_polygon
from backend.stop_services import get_stops_in_bounds, get_routes_serving_stop
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
    # Prefer SQLite if available; fall back to in-memory feed.
    try:
        stops = stops_in_bounds_sqlite(min_lat, max_lat, min_lon, max_lon, limit)
        return [StopInBounds(**s) for s in stops]
    except FileNotFoundError:
        feed = load_active_feed()
        stops = get_stops_in_bounds(feed, min_lat, max_lat, min_lon, max_lon, limit=limit)
        return [StopInBounds(**s) for s in stops]


@app.post("/routes/search", response_model=list[RouteInfo])
def routes_search(req: RouteSearchRequest):
    q = req.q.strip().lower()
    if not q:
        return []
    # Prefer SQLite-backed search; fall back to in-memory index if DB missing.
    try:
        rows = search_routes(q, req.limit)
        return [
            RouteInfo(
                route_id=row.get("route_id") or "",
                route_short_name=row.get("route_short_name"),
                route_long_name=row.get("route_long_name"),
                agency_id=row.get("agency_id"),
                agency_name=row.get("agency_name"),
                route_type=int(row["route_type"]) if row.get("route_type") not in (None, "") else None,
            )
            for row in rows
        ]
    except FileNotFoundError:
        # Fallback to existing in-memory index if SQLite DB not present.
        feed = load_active_feed()
        agency_name_by_id = {a.get("agency_id"): a.get("agency_name") for a in getattr(feed, "agencies", [])}
        try:
            index = get_routes_search_index()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Could not load routes: {e}") from e

        results: list[RouteInfo] = []
        for row, search_str in index:
            if len(results) >= req.limit:
                break
            # For numeric-only queries, prefer exact route_id/short_name matches.
            if q.isdigit():
                rid = (row.get("route_id") or "").lower()
                sn = (row.get("route_short_name") or "").lower()
                if rid != q or sn != q:
                    continue
            else:
                if q not in search_str:
                    continue
            route_type_val = None
            if row.get("route_type") is not None and str(row.get("route_type", "")).strip():
                try:
                    route_type_val = int(row["route_type"])
                except (ValueError, KeyError, TypeError):
                    pass
            agency_id = row.get("agency_id")
            results.append(
                RouteInfo(
                    route_id=row.get("route_id") or "",
                    route_short_name=row.get("route_short_name"),
                    route_long_name=row.get("route_long_name"),
                    agency_id=agency_id,
                    agency_name=agency_name_by_id.get(agency_id),
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
    feed = load_active_feed()
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")
    feed_version = feed.version_id
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

    # 2) Try compact v2 SQLite-backed cache (fast path)
    requested_direction = req.direction_id or ""
    v2_row = get_route_graph_v2(
        feed_version=feed_version,
        route_id=req.route_id,
        direction_id=requested_direction,
        date_ymd=date_str,
        pretty_osm=req.pretty_osm,
    )

    # If no exact direction match and no direction was specified by the caller,
    # fall back to any precomputed direction for this route/date and pick the
    # most frequent pattern.
    if v2_row is None and not req.direction_id:
        candidates = get_route_graphs_v2_for_route(
            feed_version=feed_version,
            route_id=req.route_id,
            date_ymd=date_str,
            pretty_osm=req.pretty_osm,
        )
        if len(candidates) == 1:
            v2_row = candidates[0]
        elif len(candidates) > 1:
            best = None
            best_freq = -1
            for row in candidates:
                pid = row.get("pattern_id") or ""
                dir_id = row.get("direction_id") or ""
                meta = get_pattern_record(
                    feed_version=feed_version,
                    route_id=req.route_id,
                    direction_id=dir_id,
                    date_ymd=date_str,
                    pattern_id=pid,
                )
                freq = int(meta.get("frequency") or 0) if meta else 0
                if freq > best_freq:
                    best_freq = freq
                    best = row
            if best is not None:
                v2_row = best

    if v2_row is not None:
        # Reconstruct lightweight cache entry so /graph/geojson and /detour
        # can operate without a full rebuild.
        pattern_id = v2_row.get("pattern_id") or ""
        edge_count = int(v2_row.get("edge_count") or 0)
        dir_id = v2_row.get("direction_id") or ""

        # Look up pattern metadata and ordered stops.
        pat_meta = get_pattern_record(
            feed_version=feed_version,
            route_id=req.route_id,
            direction_id=dir_id,
            date_ymd=date_str,
            pattern_id=pattern_id,
        )
        if pat_meta is None:
            # If metadata missing, fall back to slow path.
            raise HTTPException(status_code=500, detail="Pattern metadata missing in v2 cache.")

        used_shape = bool(pat_meta.get("used_shape"))
        frequency = int(pat_meta.get("frequency") or 0)
        repr_trip_id = pat_meta.get("repr_trip_id")
        repr_shape_id = pat_meta.get("repr_shape_id")

        stops_meta = get_pattern_stops(feed_version=feed_version, pattern_id=pattern_id)
        stop_ids = [s["stop_id"] for s in stops_meta]

        # Rebuild a minimal RoutePattern object.
        pattern = RoutePattern(
            pattern_id=pattern_id,
            route_id=req.route_id,
            direction_id=dir_id or None,
            stop_ids=stop_ids,
            frequency=frequency,
            representative_trip_id=repr_trip_id or "",
            representative_shape_id=repr_shape_id,
        )

        # Decode adjacency arrays from blobs.
        from array import array

        u_arr = array("I")
        v_arr = array("I")
        w_arr = array("f")
        u_blob = v2_row.get("u_idx_blob") or b""
        v_blob = v2_row.get("v_idx_blob") or b""
        w_blob = v2_row.get("w_s_blob") or b""
        u_arr.frombytes(u_blob)
        v_arr.frombytes(v_blob)
        w_arr.frombytes(w_blob)

        # Build a minimal NetworkX graph so existing detour logic keeps working.
        import networkx as nx
        from shapely.geometry import LineString

        g = nx.DiGraph()
        # Add nodes with coordinates.
        for s in stops_meta:
            sid = s["stop_id"]
            g.add_node(
                sid,
                stop_id=sid,
                stop_name=s.get("name"),
                lat=float(s["lat"]),
                lon=float(s["lon"]),
            )

        # Map stop indices to ids.
        idx_to_sid = [s["stop_id"] for s in stops_meta]

        edge_geoms: Dict[Tuple[str, str], any] = {}
        for i in range(min(len(u_arr), len(v_arr), len(w_arr))):
            ui = u_arr[i]
            vi = v_arr[i]
            if ui >= len(idx_to_sid) or vi >= len(idx_to_sid):
                continue
            a = idx_to_sid[ui]
            b = idx_to_sid[vi]
            w = float(w_arr[i])

            sa = next((s for s in stops_meta if s["stop_id"] == a), None)
            sb = next((s for s in stops_meta if s["stop_id"] == b), None)
            if not sa or not sb:
                continue
            lat1, lon1 = float(sa["lat"]), float(sa["lon"])
            lat2, lon2 = float(sb["lat"]), float(sb["lon"])

            # Simple straight-line geometry between stops (adequate for blockage UI
            # when combined with pattern_polyline for display).
            geom = LineString([(lon1, lat1), (lon2, lat2)])
            edge_geoms[(a, b)] = type("EdgeGeometry", (), {"from_stop_id": a, "to_stop_id": b, "linestring": geom})()

            g.add_edge(
                a,
                b,
                weight=w,
                travel_time_s=w,
                distance_m=0.0,
            )

        # Optional snapped pattern geometry. Since v2 no longer stores a full
        # pattern polyline, we recompute OSM-snapped geometry on demand when
        # pretty_osm is requested so the route sticks to the actual roads.
        snapped_geom = None
        used_osm_snapping = False
        if req.pretty_osm:
            try:
                osm_res = map_match_pattern(
                    pattern_geom=_merge_edge_geometries(edge_geoms),
                    edge_geometries=edge_geoms,
                )
                used_osm_snapping = osm_res.used_osm
                snapped_geom = osm_res.snapped_pattern_geom
                # Replace edge geometries with snapped versions for display.
                edge_geoms = osm_res.snapped_edges
            except Exception:
                # If OSRM/map matching fails, we silently fall back to straight
                # segments between stops.
                used_osm_snapping = False
                snapped_geom = None

        cache_entry = {
            "graph": g,
            "edge_geometries": edge_geoms,
            "pattern": pattern,
            "used_shape": used_shape,
            "used_osm_snapping": used_osm_snapping,
            "snapped_pattern_geom": snapped_geom,
            "date": date_str,
        }
        GRAPH_CACHE[key] = cache_entry

        example_stops = stop_ids[:5]
        return GraphBuildResponse(
            pattern_id=pattern_id,
            stop_count=len(stop_ids),
            edge_count=edge_count,
            used_shape=used_shape,
            used_osm_snapping=bool(v2_row.get("pattern_polyline_osm")),
            example_stop_ids=example_stops,
            feed_version=feed_version,
        )

    # 3) Try legacy SQLite-backed pickled cache
    try:
        blob = get_route_graph_blob(
            feed_version=feed_version,
            route_id=req.route_id,
            direction_id=req.direction_id or "",
            date_ymd=date_str,
            pretty_osm=req.pretty_osm,
        )
    except FileNotFoundError:
        blob = None
    if blob:
        try:
            cached = pickle.loads(blob)
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
            # If DB cache is corrupt or incompatible, ignore and rebuild.
            pass

    # 4) Build from scratch and persist to memory + disk
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
    """
    feed = load_active_feed()

    # 1) Try to reuse an existing cached graph pattern first.
    cached_pattern: RoutePattern | None = None
    if pattern_id:
        for entry in GRAPH_CACHE.values():
            pat: RoutePattern = entry.get("pattern")
            if pat and pat.route_id == route_id and pat.pattern_id == pattern_id:
                cached_pattern = pat
                break
    else:
        # If no pattern_id was given, try to find any cached pattern for this route.
        for entry in GRAPH_CACHE.values():
            pat: RoutePattern = entry.get("pattern")
            if pat and pat.route_id == route_id:
                cached_pattern = pat
                break

    if cached_pattern is not None:
        chosen = cached_pattern
    else:
        # 2) Fallback: rebuild patterns for the given date.
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

    stops_by_id = {s["stop_id"]: s for s in feed.stops}
    res_stops: list[GraphStopsResponseStop] = []
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

    return GraphStopsResponse(pattern_id=chosen.pattern_id, stops=res_stops)


@app.post("/detour", response_model=DetourResponse)
def detour(req: DetourRequest):
    blockage_geojson = _ensure_geometry(req.blockage_geojson)
    feed = load_active_feed()
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")

    # Graph must have been built earlier with /graph/build
    # We try both pretty_osm true/false keys in case of mismatch.
    for pretty in (True, False):
        key = _graph_cache_key(
            feed_version=feed.version_id,
            route_id=req.route_id,
            direction_id=req.direction_id,
            date=date_str,
            pretty_osm=pretty,
        )
        if key in GRAPH_CACHE:
            cache = GRAPH_CACHE[key]
            break
    else:
        raise HTTPException(
            status_code=400,
            detail="Graph not built yet; call /graph/build first.",
        )

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
    except Exception:
        # No path on multi-route GTFS graph. Optionally try OSM (Valhalla) as fallback.
        if (
            VALHALLA_URL
            and blocked_edges
            and baseline_path
            and baseline_travel_time_s is not None
        ):
            try:
                i_first: Optional[int] = None
                i_last: Optional[int] = None
                for i in range(len(baseline_path) - 1):
                    uv = (baseline_path[i], baseline_path[i + 1])
                    if uv in blocked_edges:
                        if i_first is None:
                            i_first = i
                        i_last = i
                if i_first is not None and i_last is not None:
                    stop_before = baseline_path[i_first]
                    stop_after = baseline_path[i_last + 1]
                    na = graph.nodes.get(stop_before, {})
                    nb = graph.nodes.get(stop_after, {})
                    lon_a, lat_a = na.get("lon"), na.get("lat")
                    lon_b, lat_b = nb.get("lon"), nb.get("lat")
                    if None not in (lon_a, lat_a, lon_b, lat_b):
                        osm = route_avoiding_polygon(
                            float(lon_a), float(lat_a),
                            float(lon_b), float(lat_b),
                            blockage_geojson,
                        )
                        if osm.success and osm.coordinates:
                            from shapely.geometry import mapping
                            path_nodes_osm = (
                                baseline_path[: i_first + 1] + baseline_path[i_last + 1 :]
                            )
                            path = [graph.nodes[n]["stop_id"] for n in path_nodes_osm]
                            features = []
                            for i in range(i_first):
                                u, v = baseline_path[i], baseline_path[i + 1]
                                eg = edge_geometries.get((u, v))
                                if eg:
                                    features.append({
                                        "type": "Feature",
                                        "geometry": mapping(eg.linestring),
                                        "properties": {"from_stop_id": eg.from_stop_id, "to_stop_id": eg.to_stop_id},
                                    })
                            features.append({
                                "type": "Feature",
                                "geometry": {"type": "LineString", "coordinates": list(osm.coordinates)},
                                "properties": {"kind": "osm_detour"},
                            })
                            for i in range(i_last + 1, len(baseline_path) - 1):
                                u, v = baseline_path[i], baseline_path[i + 1]
                                eg = edge_geometries.get((u, v))
                                if eg:
                                    features.append({
                                        "type": "Feature",
                                        "geometry": mapping(eg.linestring),
                                        "properties": {"from_stop_id": eg.from_stop_id, "to_stop_id": eg.to_stop_id},
                                    })
                            path_geojson = {"type": "FeatureCollection", "features": features}
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
                            used_osm_detour = True
            except Exception:
                pass
        if not used_osm_detour:
            raise HTTPException(
                status_code=409,
                detail="No detour path found on GTFS routes; try OSM/Valhalla or adjust blockage.",
            )

    detour_delay_s: Optional[float] = None
    detour_extra_distance_m: Optional[float] = None
    if baseline_travel_time_s is not None and baseline_distance_m is not None:
        detour_delay_s = total_travel_time_s - baseline_travel_time_s
        detour_extra_distance_m = total_distance_m - baseline_distance_m

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
        feed_version=feed.version_id,
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
    feed = load_active_feed()
    date_str = date or datetime.utcnow().strftime("%Y%m%d")

    key = _graph_cache_key(
        feed_version=feed.version_id,
        route_id=route_id,
        direction_id=direction_id,
        date=date_str,
        pretty_osm=pretty_osm,
    )
    cache = GRAPH_CACHE.get(key)
    if not cache:
        raise HTTPException(
            status_code=400,
            detail="Graph not built yet; call /graph/build first.",
        )

    from shapely.geometry import mapping, Point

    pattern = cache["pattern"]
    edge_geometries = cache["edge_geometries"]
    snapped_pattern_geom = cache.get("snapped_pattern_geom")

    stops_by_id = {s["stop_id"]: s for s in feed.stops}
    stop_features = []
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
    bounds = get_trip_time_bounds_from_db()
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
        feed = load_active_feed()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"GTFS feed could not be loaded. Place israel-public-transportation.zip in the project root or run /feed/update. ({e})",
        )

    try:
        routes_raw = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            yyyymmdd=req.date,
            start_sec=start_sec,
            end_sec=end_sec,
        )
    except HTTPException:
        raise
    except Exception as e:
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
        results.append(
            AreaRouteResult(
                route_id=r["route_id"],
                direction_id=r.get("direction_id"),
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
    feed = load_active_feed()
    try:
        routes_raw = get_routes_serving_stop(
            feed=feed,
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

