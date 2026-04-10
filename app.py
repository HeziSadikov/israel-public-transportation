from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import hashlib
import logging.config
import pickle
import time

from fastapi import FastAPI, HTTPException, Response
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
from backend.osm_pretty import map_match_pattern, map_match_coordinates
from backend.router_core import astar_route, collect_path_geojson, compute_blocked_edges
from backend.detour_graph import build_detour_graph, default_detour_graph_params
from backend.detour_service import compute_detour, DetourComputeInput, DetourComputeError
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
    StopSearchResult,
    DetourByAreaRequest,
    DetourByAreaResponse,
    DetourByAreaRouteResult,
    DetourByAreaMode,
    GeocodeResult,
)
from backend.config import (
    GRAPH_CACHE,
    GRAPH_CACHE_DIR,
    GRAPH_WARMUP_ENABLED,
    GRAPH_WARMUP_LOG_PROGRESS,
    GRAPH_WARMUP_TIMEOUT_S,
    GRAPH_WARMUP_PROFILES,
    GRAPH_WARMUP_PREVIEWS_ENABLED,
    GRAPH_WARMUP_PREVIEW_VERIFY_SIG,
    GRAPH_WARMUP_PREVIEW_MAX_ROUTES,
)
from backend.route_preview_payload import build_route_preview_cache_dict
from backend.area_search import find_routes_in_polygon
from backend import db_access as db_access_module
from backend.logging_utils import log
from backend.service_calendar import ServiceCalendar, resolve_service_profile
from backend.uvicorn_logging import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)

app = FastAPI(title="Israel GTFS Detour Router")
GRAPH_WARMUP_STATUS: Dict[str, object] = {
    "last_started_at": None,
    "last_finished_at": None,
    "last_duration_s": None,
    "loaded_gtfs": 0,
    "loaded_osm": 0,
    "loaded_previews_gtfs": 0,
    "loaded_previews_osm": 0,
    "previews_skipped_stale": 0,
    "errors": 0,
}


def _resolve_route_sig_hash(
    feed_id: int,
    route_id: str,
    direction_id: str | None,
) -> Optional[str]:
    """
    Get route signature cheaply from route_signatures table when available,
    fallback to on-demand compute.
    """
    try:
        sig = db_access_module.get_route_signature(feed_id, route_id, direction_id)
        if sig:
            return sig
    except Exception:
        pass
    try:
        return db_access_module.compute_route_signature(route_id, direction_id)
    except Exception:
        return None


def _warmup_route_previews(
    feed_id: int,
    feed_version: str,
    profile_keys: List[str],
    started: float,
    timeout_s: int,
) -> Tuple[int, int, int]:
    """Bulk-load route_preview_cache rows into GRAPH_CACHE. Returns (gtfs_count, osm_count, stale_skips)."""
    if not GRAPH_WARMUP_PREVIEWS_ENABLED:
        return 0, 0, 0
    loaded_gtfs = 0
    loaded_osm = 0
    skipped_stale = 0
    max_n = int(GRAPH_WARMUP_PREVIEW_MAX_ROUTES or 0)
    total_loaded = 0
    stop_all = False
    # Avoid a DB round-trip per preview row when signature verification is enabled.
    # We bulk-load route signatures once, then only compute missing keys on demand.
    live_sig_by_route_dir: Dict[Tuple[str, Optional[str]], Optional[str]] = {}
    if GRAPH_WARMUP_PREVIEW_VERIFY_SIG:
        t0 = time.time()
        try:
            sigs = db_access_module.get_route_signatures_bulk(feed_id)
            live_sig_by_route_dir.update(sigs)
            log(
                "graph/cache/warmup",
                f"phase=preview_sig_bulk_load done rows={len(sigs)} elapsed_s={round(time.time() - t0, 3)}",
            )
        except Exception as e:
            log("graph/cache/warmup", f"phase=preview_sig_bulk_load error={e!s}")
            GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
    for pretty_osm in (False, True):
        if stop_all or time.time() - started > timeout_s:
            break
        for profile_key in profile_keys:
            if stop_all or time.time() - started > timeout_s:
                break
            bulk_t0 = time.time()
            log(
                "graph/cache/warmup",
                f"phase=preview_bulk_fetch start profile={profile_key} pretty_osm={pretty_osm}",
            )
            try:
                bulk = db_access_module.get_cached_previews_bulk(
                    feed_id, profile_key, pretty_osm
                )
            except Exception:
                GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
                log(
                    "graph/cache/warmup",
                    f"phase=preview_bulk_fetch error profile={profile_key} pretty_osm={pretty_osm} elapsed_s={round(time.time() - bulk_t0, 3)}",
                )
                continue
            log(
                "graph/cache/warmup",
                f"phase=preview_bulk_fetch done profile={profile_key} pretty_osm={pretty_osm} rows={len(bulk)} elapsed_s={round(time.time() - bulk_t0, 3)}",
            )
            items_seen = 0
            for (route_id, direction_id), (sig_db, _pat_id, blob) in bulk.items():
                items_seen += 1
                if GRAPH_WARMUP_LOG_PROGRESS and items_seen % 500 == 0:
                    elapsed_s = round(time.time() - started, 3)
                    log(
                        "graph/cache/warmup",
                        f"phase=preview_hydrate progress profile={profile_key} pretty_osm={pretty_osm} seen={items_seen} loaded_gtfs={loaded_gtfs} loaded_osm={loaded_osm} skipped_stale={skipped_stale} elapsed_s={elapsed_s}",
                    )
                if time.time() - started > timeout_s:
                    stop_all = True
                    break
                if max_n > 0 and total_loaded >= max_n:
                    stop_all = True
                    break
                if GRAPH_WARMUP_PREVIEW_VERIFY_SIG:
                    key = (str(route_id), None if direction_id is None else str(direction_id))
                    if key not in live_sig_by_route_dir:
                        live_sig_by_route_dir[key] = _resolve_route_sig_hash(
                            feed_id, route_id, direction_id
                        )
                    live = live_sig_by_route_dir.get(key)
                    if not live or live != sig_db:
                        skipped_stale += 1
                        continue
                try:
                    payload = pickle.loads(blob)
                except Exception:
                    GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
                    continue
                if not isinstance(payload, dict) or payload.get("route_geojson") is None:
                    GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
                    continue
                mem_key = _preview_mem_key(
                    feed_version, route_id, direction_id, profile_key, pretty_osm
                )
                GRAPH_CACHE[mem_key] = payload
                total_loaded += 1
                if pretty_osm:
                    loaded_osm += 1
                else:
                    loaded_gtfs += 1
            log(
                "graph/cache/warmup",
                f"phase=preview_hydrate done profile={profile_key} pretty_osm={pretty_osm} seen={items_seen} loaded_gtfs={loaded_gtfs} loaded_osm={loaded_osm} skipped_stale={skipped_stale}",
            )
    return loaded_gtfs, loaded_osm, skipped_stale


def _build_preview_payload_from_cache_entry(cache: Dict) -> Dict[str, object]:
    """
    Build route preview payload (geojson + ordered stops) from a cache entry.
    """
    if cache.get("preview_geojson") is not None and cache.get("preview_stops") is not None:
        return {
            "route_geojson": cache.get("preview_geojson"),
            "stops": cache.get("preview_stops"),
        }
    from shapely.geometry import mapping, Point

    pattern = cache["pattern"]
    edge_geometries = cache["edge_geometries"]
    snapped_pattern_geom = cache.get("snapped_pattern_geom")
    graph = cache.get("graph")

    stop_features = []
    stops_list = []
    try:
        feed = load_active_feed()
        stops_by_id = {s["stop_id"]: s for s in feed.stops}
    except Exception:
        stops_by_id = {}
    by_stop: Dict[str, Dict] | None = None

    def _graph_stop_map() -> Dict[str, Dict]:
        nonlocal by_stop
        if by_stop is None:
            by_stop = {}
            if graph is not None:
                for _nid, node_data in graph.nodes(data=True):
                    sid = node_data.get("stop_id")
                    if sid is None:
                        continue
                    key = str(sid)
                    if key not in by_stop:
                        by_stop[key] = node_data
        return by_stop

    for idx, sid in enumerate(pattern.stop_ids):
        st = stops_by_id.get(sid)
        if st:
            lat, lon = float(st["stop_lat"]), float(st["stop_lon"])
            name = st.get("stop_name")
        else:
            d = _graph_stop_map().get(str(sid))
            if not d:
                continue
            lat, lon = d.get("lat"), d.get("lon")
            if lat is None or lon is None:
                continue
            name = d.get("stop_name")
        pt = Point(float(lon), float(lat))
        stop_features.append(
            {
                "type": "Feature",
                "geometry": mapping(pt),
                "properties": {"stop_id": sid, "name": name},
            }
        )
        stops_list.append(
            {
                "stop_id": str(sid),
                "name": name,
                "lat": float(lat),
                "lon": float(lon),
                "sequence": idx,
            }
        )

    # One line feature instead of per-edge mapping() — avoids multi-minute Shapely on long routes.
    line_features: List[Dict] = []
    if snapped_pattern_geom is not None:
        line_features.append(
            {
                "type": "Feature",
                "geometry": mapping(snapped_pattern_geom),
                "properties": {"kind": "pattern_snapped"},
            }
        )
    else:
        merged = _merge_edge_geometries(edge_geometries)
        if merged is not None and not getattr(merged, "is_empty", False):
            line_features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(merged),
                    "properties": {"kind": "route_merged"},
                }
            )
        else:
            for (_u, _v), eg in edge_geometries.items():
                line_features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(eg.linestring),
                        "properties": {"from_stop_id": eg.from_stop_id, "to_stop_id": eg.to_stop_id},
                    }
                )

    features = stop_features + line_features
    route_geojson = {"type": "FeatureCollection", "features": features}
    cache["preview_geojson"] = route_geojson
    cache["preview_stops"] = stops_list
    return {"route_geojson": route_geojson, "stops": stops_list}

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


def _run_graph_cache_warmup(
    profiles: Optional[List[str]] = None,
    *,
    include_previews: bool = True,
) -> Dict[str, object]:
    started = time.time()
    GRAPH_WARMUP_STATUS["last_started_at"] = datetime.utcnow().isoformat() + "Z"
    GRAPH_WARMUP_STATUS["errors"] = 0
    GRAPH_WARMUP_STATUS["loaded_previews_gtfs"] = 0
    GRAPH_WARMUP_STATUS["loaded_previews_osm"] = 0
    GRAPH_WARMUP_STATUS["previews_skipped_stale"] = 0
    loaded_gtfs = 0
    loaded_osm = 0
    profile_keys = profiles or list(GRAPH_WARMUP_PROFILES)
    log(
        "graph/cache/warmup",
        f"start profiles={profile_keys}, timeout_s={GRAPH_WARMUP_TIMEOUT_S}, "
        f"include_previews={include_previews}",
    )
    try:
        phase_t0 = time.time()
        log("graph/cache/warmup", "phase=resolve_active_feed start")
        feed_id = db_access_module.get_active_feed_id()
        feed_version = f"postgis-{feed_id}"
        log(
            "graph/cache/warmup",
            f"phase=resolve_active_feed done feed_id={feed_id} elapsed_s={round(time.time() - phase_t0, 3)}",
        )
        phase_t0 = time.time()
        log("graph/cache/warmup", "phase=load_route_direction_pairs start")
        pairs = db_access_module.get_route_direction_pairs(feed_id)
        log(
            "graph/cache/warmup",
            f"phase=load_route_direction_pairs done pairs={len(pairs)} elapsed_s={round(time.time() - phase_t0, 3)}",
        )
        phase_t0 = time.time()
        log("graph/cache/warmup", "phase=load_cached_graphs_bulk start pretty_osm=False")
        cached_gtfs = db_access_module.get_cached_graphs_bulk(feed_id, False)
        log(
            "graph/cache/warmup",
            f"phase=load_cached_graphs_bulk done pretty_osm=False rows={len(cached_gtfs)} elapsed_s={round(time.time() - phase_t0, 3)}",
        )
        phase_t0 = time.time()
        log("graph/cache/warmup", "phase=load_cached_graphs_bulk start pretty_osm=True")
        cached_osm = db_access_module.get_cached_graphs_bulk(feed_id, True)
        log(
            "graph/cache/warmup",
            f"phase=load_cached_graphs_bulk done pretty_osm=True rows={len(cached_osm)} elapsed_s={round(time.time() - phase_t0, 3)}",
        )
        timeout_s = max(1, int(GRAPH_WARMUP_TIMEOUT_S))
        log(
            "graph/cache/warmup",
            f"phase=hydrate_memory_graph_cache start timeout_s={timeout_s} profiles={len(profile_keys)}",
        )
        last_progress_log = 0
        for route_id, direction_id in pairs:
            if time.time() - started > timeout_s:
                log("graph/cache/warmup", "phase=hydrate_memory_graph_cache timeout_reached")
                break
            last_progress_log += 1
            if GRAPH_WARMUP_LOG_PROGRESS and last_progress_log >= 250:
                last_progress_log = 0
                elapsed_s = round(time.time() - started, 3)
                log(
                    "graph/cache/warmup",
                    f"phase=hydrate_memory_graph_cache progress elapsed_s={elapsed_s} loaded_gtfs={loaded_gtfs} loaded_osm={loaded_osm}",
                )
            row_gtfs = cached_gtfs.get((route_id, direction_id))
            if row_gtfs:
                try:
                    entry = pickle.loads(row_gtfs[1])
                    for p in profile_keys:
                        key = _graph_cache_key(feed_version, route_id, direction_id, p, False)
                        GRAPH_CACHE[key] = entry
                    loaded_gtfs += 1
                except Exception:
                    GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
            row_osm = cached_osm.get((route_id, direction_id))
            if row_osm:
                try:
                    entry = pickle.loads(row_osm[1])
                    for p in profile_keys:
                        key = _graph_cache_key(feed_version, route_id, direction_id, p, True)
                        GRAPH_CACHE[key] = entry
                    loaded_osm += 1
                except Exception:
                    GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
        log(
            "graph/cache/warmup",
            f"phase=hydrate_memory_graph_cache done loaded_gtfs={loaded_gtfs} loaded_osm={loaded_osm}",
        )
    except Exception as e:
        log("graph/cache/warmup", f"fatal_error={e!s}")
        GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
    preview_gtfs = 0
    preview_osm = 0
    preview_skip = 0
    if include_previews:
        try:
            phase_t0 = time.time()
            log("graph/cache/warmup", "phase=preview_warmup_prepare start")
            feed_id_pv = db_access_module.get_active_feed_id()
            feed_version_pv = f"postgis-{feed_id_pv}"
            timeout_s = max(1, int(GRAPH_WARMUP_TIMEOUT_S))
            log(
                "graph/cache/warmup",
                f"phase=preview_warmup_prepare done feed_id={feed_id_pv} elapsed_s={round(time.time() - phase_t0, 3)}",
            )
            if time.time() - started <= timeout_s:
                log("graph/cache/warmup", "phase=preview_warmup_load start")
                preview_gtfs, preview_osm, preview_skip = _warmup_route_previews(
                    feed_id_pv,
                    feed_version_pv,
                    profile_keys,
                    started,
                    timeout_s,
                )
                log(
                    "graph/cache/warmup",
                    f"phase=preview_warmup_load done loaded_previews_gtfs={preview_gtfs} loaded_previews_osm={preview_osm} previews_skipped_stale={preview_skip}",
                )
            else:
                log("graph/cache/warmup", "phase=preview_warmup_load skipped timeout_reached_before_start")
        except Exception as e:
            log("graph/cache/warmup", f"preview_warmup_error={e!s}")
            GRAPH_WARMUP_STATUS["errors"] = int(GRAPH_WARMUP_STATUS["errors"] or 0) + 1
    GRAPH_WARMUP_STATUS["loaded_previews_gtfs"] = preview_gtfs
    GRAPH_WARMUP_STATUS["loaded_previews_osm"] = preview_osm
    GRAPH_WARMUP_STATUS["previews_skipped_stale"] = preview_skip
    finished = time.time()
    GRAPH_WARMUP_STATUS["last_finished_at"] = datetime.utcnow().isoformat() + "Z"
    GRAPH_WARMUP_STATUS["last_duration_s"] = round(finished - started, 3)
    GRAPH_WARMUP_STATUS["loaded_gtfs"] = loaded_gtfs
    GRAPH_WARMUP_STATUS["loaded_osm"] = loaded_osm
    log(
        "graph/cache/warmup",
        f"done duration_s={GRAPH_WARMUP_STATUS['last_duration_s']}, "
        f"loaded_gtfs={loaded_gtfs}, loaded_osm={loaded_osm}, "
        f"loaded_previews_gtfs={preview_gtfs}, loaded_previews_osm={preview_osm}, "
        f"previews_skipped_stale={preview_skip}, errors={GRAPH_WARMUP_STATUS['errors']}",
    )
    return dict(GRAPH_WARMUP_STATUS)


@app.on_event("startup")
def startup_graph_warmup():
    if not GRAPH_WARMUP_ENABLED:
        log("graph/cache/warmup", "startup skipped (GRAPH_WARMUP_ENABLED=false)")
        return
    log("graph/cache/warmup", "startup trigger")
    _run_graph_cache_warmup()


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


@app.get("/stops/search", response_model=list[StopSearchResult])
def stops_search(
    q: str,
    limit: int = 20,
):
    """Search stops by name/code/id in the active feed."""
    q = (q or "").strip()
    if len(q) < 2:
        return []
    try:
        rows = db_access_module.search_stops_pg(q=q, limit=max(1, min(limit, 100)))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not search stops in PostGIS: {e}")
    return [StopSearchResult(**r) for r in rows]


@app.post("/routes/search", response_model=list[RouteInfo])
def routes_search(req: RouteSearchRequest):
    q = req.q.strip()
    if not q:
        return []
    window = _parse_window_range(
        req.start_date, req.start_time, req.end_date, req.end_time, allow_empty=True
    )
    try:
        rows = db_access_module.search_routes_pg_range(
            q,
            req.limit,
            start_date_ymd=window[0] if window else None,
            start_sec=window[1] if window else None,
            end_date_ymd=window[2] if window else None,
            end_sec=window[3] if window else None,
        )
    except Exception as e:
        log("routes/search", f"error q={q!r} reason={e!s}")
        raise HTTPException(status_code=503, detail=f"Could not search routes in PostGIS: {e}")
    log(
        "routes/search",
        f"q={q!r} limit={req.limit} rows={len(rows)}"
        + (f" window={window[0]} {req.start_time}-{window[2]} {req.end_time}" if window else ""),
    )
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
                trip_count=row.get("trip_count"),
                last_stop_name=row.get("last_stop_name"),
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
    cal_min: Optional[int] = None
    cal_max: Optional[int] = None
    coverage_note: Optional[str] = None
    try:
        cal_min, cal_max = db_access_module.get_active_feed_calendar_span()
    except Exception:
        cal_min, cal_max = None, None
    if cal_min is not None and cal_max is not None:
        today_utc_ymd = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
        if today_utc_ymd < cal_min or today_utc_ymd > cal_max:

            def _ymd_disp(x: int) -> str:
                s = f"{x:08d}"
                return f"{s[:4]}-{s[4:6]}-{s[6:8]}"

            coverage_note = (
                f"UTC date {_ymd_disp(today_utc_ymd)} is outside the loaded GTFS calendar "
                f"({_ymd_disp(cal_min)} to {_ymd_disp(cal_max)}). "
                "Place a newer israel-public-transportation.zip in the project root and call "
                "POST /feed/update, or pick service dates in that range for area search and graphs."
            )
    return FeedStatusResponse(
        **status,
        calendar_min_ymd=cal_min,
        calendar_max_ymd=cal_max,
        calendar_coverage_note=coverage_note,
    )


def _graph_cache_key(
    feed_version: str,
    route_id: str,
    direction_id: str | None,
    profile_key: str,
    pretty_osm: bool,
) -> str:
    return "|".join(
        [
            feed_version,
            route_id,
            direction_id or "",
            f"profile:{profile_key}",
            "osm" if pretty_osm else "gtfs",
        ]
    )


def _preview_mem_key(
    feed_version: str,
    route_id: str,
    direction_id: str | None,
    profile_key: str,
    pretty_osm: bool,
) -> str:
    return "|".join(
        [
            feed_version,
            route_id,
            direction_id or "",
            f"profile:{profile_key}",
            "osm" if pretty_osm else "gtfs",
            "preview",
        ]
    )


def _legacy_graph_cache_key(
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
    profile_key = resolve_service_profile(date_str)
    keys = []
    try:
        feed_id = db_access_module.get_active_feed_id()
        keys.append(
            _graph_cache_key(f"postgis-{feed_id}", route_id, direction_id, profile_key, False)
        )
        keys.append(
            _graph_cache_key(f"postgis-{feed_id}", route_id, direction_id, profile_key, True)
        )
        # Transition compatibility for older date-fragmented keys.
        keys.append(
            _legacy_graph_cache_key(f"postgis-{feed_id}", route_id, direction_id, date_str, False)
        )
        keys.append(
            _legacy_graph_cache_key(f"postgis-{feed_id}", route_id, direction_id, date_str, True)
        )
    except (RuntimeError, Exception):
        pass
    try:
        feed = load_active_feed()
        keys.append(
            _graph_cache_key(feed.version_id, route_id, direction_id, profile_key, False)
        )
        keys.append(
            _graph_cache_key(feed.version_id, route_id, direction_id, profile_key, True)
        )
        keys.append(
            _legacy_graph_cache_key(feed.version_id, route_id, direction_id, date_str, False)
        )
        keys.append(
            _legacy_graph_cache_key(feed.version_id, route_id, direction_id, date_str, True)
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
def graph_build(req: GraphBuildRequest, response: Response):
    t0 = time.perf_counter()
    cache_hit = "none"
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")
    profile_key = resolve_service_profile(date_str)

    # Prefer PostGIS when an active feed is available and we have a pattern for this route.
    try:
        feed_id = db_access_module.get_active_feed_id()
        dir_param = str(req.direction_id) if req.direction_id is not None else None
        meta = db_access_module.get_pattern_for_route(
            req.route_id, dir_param, date_str
        )
        if meta is None:
            for fallback_dir in (None, "0", "1"):
                if fallback_dir == dir_param:
                    continue
                meta = db_access_module.get_pattern_for_route(
                    req.route_id, fallback_dir, date_str
                )
                if meta is not None:
                    break
        if meta is None:
            raise HTTPException(
                status_code=404,
                detail="No pattern found for this route/date. Run POST /feed/update to build patterns.",
            )
        feed_version = f"postgis-{feed_id}"
        key = _graph_cache_key(
            feed_version=feed_version,
            route_id=req.route_id,
            direction_id=req.direction_id,
            profile_key=profile_key,
            pretty_osm=req.pretty_osm,
        )
        cached = GRAPH_CACHE.get(key)
        if cached is not None:
            cache_hit = "memory"
            pat = cached["pattern"]
            elapsed = (time.perf_counter() - t0) * 1000.0
            response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
            response.headers["X-Cache-Hit"] = cache_hit
            response.headers["X-Graph-Cache-Hit"] = cache_hit
            log(
                "graph/build",
                f"route_id={req.route_id} dir={req.direction_id} hit={cache_hit} elapsed_ms={elapsed:.1f}",
            )
            return GraphBuildResponse(
                pattern_id=pat.pattern_id,
                stop_count=len(pat.stop_ids),
                edge_count=len(cached["edge_geometries"]),
                used_shape=cached["used_shape"],
                used_osm_snapping=cached.get("used_osm_snapping", False),
                example_stop_ids=pat.stop_ids[:5],
                feed_version=feed_version,
            )
        # Try PostGIS route_graph_cache keyed by per-route signature (date-agnostic).
        try:
            sig_hash = db_access_module.compute_route_signature(req.route_id, req.direction_id)
            cached_blob = db_access_module.get_cached_route_graph_pg(
                feed_id=feed_id,
                route_id=req.route_id,
                direction_id=req.direction_id,
                pretty_osm=req.pretty_osm,
                route_sig_hash=sig_hash,
            )
        except Exception:
            sig_hash = None
            cached_blob = None

        if cached_blob:
            try:
                cache_hit = "db"
                cached = pickle.loads(cached_blob)
                GRAPH_CACHE[key] = cached
                # Backward-compatible alias key (date-shaped) during transition.
                GRAPH_CACHE[
                    _legacy_graph_cache_key(
                        feed_version=feed_version,
                        route_id=req.route_id,
                        direction_id=req.direction_id,
                        date=date_str,
                        pretty_osm=req.pretty_osm,
                    )
                ] = cached
                pat = cached["pattern"]
                edge_geoms = cached["edge_geometries"]
                used_shape = cached["used_shape"]
                used_osm = cached.get("used_osm_snapping", False)
                elapsed = (time.perf_counter() - t0) * 1000.0
                response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
                response.headers["X-Cache-Hit"] = cache_hit
                response.headers["X-Graph-Cache-Hit"] = cache_hit
                log(
                    "graph/build",
                    f"route_id={req.route_id} dir={req.direction_id} hit={cache_hit} elapsed_ms={elapsed:.1f}",
                )
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
            "preview_geojson": None,
            "preview_stops": None,
        }
        GRAPH_CACHE[key] = cache_entry
        GRAPH_CACHE[
            _legacy_graph_cache_key(
                feed_version=feed_version,
                route_id=req.route_id,
                direction_id=req.direction_id,
                date=date_str,
                pretty_osm=req.pretty_osm,
            )
        ] = cache_entry
        if sig_hash:
            try:
                db_access_module.save_route_graph_pg(
                    feed_id=feed_id,
                    route_id=req.route_id,
                    direction_id=req.direction_id,
                    pretty_osm=req.pretty_osm,
                    route_sig_hash=sig_hash,
                    graph_blob=pickle.dumps(cache_entry),
                    date_ymd=date_str,
                )
            except Exception:
                pass

        pat = build_result.pattern
        elapsed = (time.perf_counter() - t0) * 1000.0
        response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
        response.headers["X-Cache-Hit"] = "built"
        response.headers["X-Graph-Cache-Hit"] = "built"
        log(
            "graph/build",
            f"route_id={req.route_id} dir={req.direction_id} hit=built elapsed_ms={elapsed:.1f}",
        )
        return GraphBuildResponse(
            pattern_id=pat.pattern_id,
            stop_count=len(pat.stop_ids),
            edge_count=len(edge_geoms),
            used_shape=build_result.used_shape,
            used_osm_snapping=used_osm,
            example_stop_ids=pat.stop_ids[:5],
            feed_version=feed_version,
        )
    except HTTPException:
        raise
    except (RuntimeError, Exception) as e:
        elapsed = (time.perf_counter() - t0) * 1000.0
        response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
        response.headers["X-Cache-Hit"] = cache_hit
        response.headers["X-Graph-Cache-Hit"] = cache_hit
        log(
            "graph/build",
            f"error route_id={req.route_id} dir={req.direction_id} hit={cache_hit} "
            f"elapsed_ms={elapsed:.1f} reason={e!s}",
        )
        raise HTTPException(
            status_code=500,
            detail=f"Graph build failed: {e!s}",
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
    response: Response,
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
    t0 = time.perf_counter()
    cache_hit = "none"
    try:
        feed = load_active_feed()
    except Exception:
        feed = None

    # 1) Try to reuse an existing cached graph pattern first.
    cached_pattern: RoutePattern | None = None
    cached_entry = None
    if pattern_id:
        for entry in GRAPH_CACHE.values():
            pat: RoutePattern = entry.get("pattern")
            if pat and pat.route_id == route_id and pat.pattern_id == pattern_id:
                cached_pattern = pat
                cached_entry = entry
                break
    else:
        for entry in GRAPH_CACHE.values():
            pat: RoutePattern = entry.get("pattern")
            if pat and pat.route_id == route_id:
                cached_pattern = pat
                cached_entry = entry
                break

    if cached_pattern is not None:
        cache_hit = "memory_pattern"
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

    if cached_entry is not None and cached_entry.get("preview_stops") is not None:
        preview_stops = cached_entry.get("preview_stops") or []
        out = GraphStopsResponse(
            pattern_id=chosen.pattern_id,
            stops=[GraphStopsResponseStop(**s) for s in preview_stops],
        )
        elapsed = (time.perf_counter() - t0) * 1000.0
        response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
        response.headers["X-Cache-Hit"] = "preview"
        log(
            "graph/stops",
            f"route_id={route_id} dir={direction_id} hit=preview elapsed_ms={elapsed:.1f}",
        )
        return out

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

    elapsed = (time.perf_counter() - t0) * 1000.0
    response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
    response.headers["X-Cache-Hit"] = cache_hit
    log(
        "graph/stops",
        f"route_id={route_id} dir={direction_id} hit={cache_hit} elapsed_ms={elapsed:.1f}",
    )
    return GraphStopsResponse(pattern_id=chosen.pattern_id, stops=res_stops)


@app.post("/detour", response_model=DetourResponse)
def detour(req: DetourRequest):
    blockage_geojson = _ensure_geometry(req.blockage_geojson)
    date_str = req.date or datetime.utcnow().strftime("%Y%m%d")

    log(
        "detour",
        f"route_id={req.route_id!r}, direction_id={req.direction_id!r}, "
        f"date={date_str}, start_stop_id={req.start_stop_id!r}, "
        f"end_stop_id={req.end_stop_id!r}",
    )

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
        log(
            "detour",
            f"error route_id={req.route_id!r}, direction_id={req.direction_id!r}, "
            "reason=graph_cache_miss",
        )
        raise HTTPException(
            status_code=400,
            detail="Graph not built yet; call /graph/build first.",
        )
    feed_version = cache_key_used.split("|")[0] if cache_key_used else ""
    try:
        feed = load_active_feed()
    except Exception:
        feed = None

    start_sec: Optional[int] = None
    end_sec: Optional[int] = None
    st_in = (req.start_time or "").strip()
    et_in = (req.end_time or "").strip()
    if st_in or et_in:
        start_sec = _parse_hhmm_to_seconds(st_in) if st_in else 0
        end_sec = _parse_hhmm_to_seconds(et_in) if et_in else 27 * 3600

    try:
        result = compute_detour(
            DetourComputeInput(
                route_id=req.route_id,
                direction_id=req.direction_id,
                date_str=date_str,
                start_stop_id=req.start_stop_id,
                end_stop_id=req.end_stop_id,
                blockage_geojson=blockage_geojson,
                cache_graph=cache["graph"],
                cache_edge_geometries=cache["edge_geometries"],
                used_shape=cache["used_shape"],
                used_osm_snapping=cache["used_osm_snapping"],
                feed_version=feed_version,
                feed=feed,
                start_sec=start_sec,
                end_sec=end_sec,
            )
        )
    except DetourComputeError as e:
        log(
            "detour",
            f"error route_id={req.route_id!r}, direction_id={req.direction_id!r}, "
            f"status_code={e.status_code} reason={e.detail!r}",
        )
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    log(
        "detour",
        f"result route_id={req.route_id!r}, direction_id={req.direction_id!r}, "
        f"blocked_edges={result.blocked_edges_count}, "
        f"stop_path_len={len(result.stop_path)}, "
        f"total_travel_time_s={result.total_travel_time_s:.1f}, "
        f"baseline_travel_time_s={result.baseline_travel_time_s if result.baseline_travel_time_s is not None else 'N/A'}",
    )

    return DetourResponse(
        blocked_edges_count=result.blocked_edges_count,
        stop_path=result.stop_path,
        path_geojson=result.path_geojson,
        blocked_edges_geojson=result.blocked_edges_geojson,
        total_travel_time_s=result.total_travel_time_s,
        total_distance_m=result.total_distance_m,
        baseline_travel_time_s=result.baseline_travel_time_s,
        baseline_distance_m=result.baseline_distance_m,
        detour_delay_s=result.detour_delay_s,
        detour_extra_distance_m=result.detour_extra_distance_m,
        used_shape=result.used_shape,
        used_osm_snapping=result.used_osm_snapping,
        feed_version=result.feed_version,
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
            start_node_id=start_node,
            end_node_id=end_node,
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
    # OSM detour: optionally snap to OSRM road graph so the line matches the rest of the map
    detour_coords = list(osm.coordinates)
    snapped = map_match_coordinates(detour_coords)
    if snapped is not None and len(snapped.coords) >= 2:
        detour_coords = list(snapped.coords)
    features.append(
        {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": detour_coords},
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
    response: Response,
    route_id: str,
    direction_id: str | None = None,
    pattern_id: str | None = None,
    date: str | None = None,
    pretty_osm: bool = False,
):
    """
    Optional endpoint returning full route pattern polyline and stops for display.
    """
    t0 = time.perf_counter()
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

    if cache.get("preview_geojson") is not None:
        elapsed = (time.perf_counter() - t0) * 1000.0
        response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
        response.headers["X-Cache-Hit"] = "preview"
        log(
            "graph/geojson",
            f"route_id={route_id} dir={direction_id} hit=preview elapsed_ms={elapsed:.1f}",
        )
        return cache.get("preview_geojson")

    from shapely.geometry import mapping, Point

    pattern = cache["pattern"]
    edge_geometries = cache["edge_geometries"]
    snapped_pattern_geom = cache.get("snapped_pattern_geom")
    graph = cache.get("graph")

    # Build stop features from graph nodes (works for both PostGIS and feed-backed cache).
    stop_features = []
    preview_stops = []
    if graph is not None:
        by_stop: Dict[str, Dict] = {}
        for _nid, node_data in graph.nodes(data=True):
            sid = node_data.get("stop_id")
            if sid is None:
                continue
            k = str(sid)
            if k not in by_stop:
                by_stop[k] = node_data
        for sid in pattern.stop_ids:
            d = by_stop.get(str(sid))
            if d is None:
                continue
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
            preview_stops.append(
                {
                    "stop_id": str(sid),
                    "name": d.get("stop_name"),
                    "lat": float(lat),
                    "lon": float(lon),
                    "sequence": len(preview_stops),
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

    out = {"type": "FeatureCollection", "features": features}
    cache["preview_geojson"] = out
    if preview_stops:
        cache["preview_stops"] = preview_stops
    elapsed = (time.perf_counter() - t0) * 1000.0
    response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
    response.headers["X-Cache-Hit"] = "assembled"
    log(
        "graph/geojson",
        f"route_id={route_id} dir={direction_id} hit=assembled elapsed_ms={elapsed:.1f}",
    )
    return out


@app.get("/graph/preview")
def graph_preview(
    response: Response,
    route_id: str,
    direction_id: str | None = None,
    date: str | None = None,
    pretty_osm: bool = False,
):
    """
    Single-call selection payload for instant map preview:
    pattern_id + route_geojson + ordered stops.
    """
    t0 = time.perf_counter()
    date_str = date or datetime.utcnow().strftime("%Y%m%d")
    profile_key = resolve_service_profile(date_str)
    try:
        feed_id = db_access_module.get_active_feed_id()
    except Exception as e:
        log("graph/preview", f"error route_id={route_id} dir={direction_id} reason=active_feed_lookup_failed error={e!s}")
        raise HTTPException(status_code=503, detail=f"Could not resolve active feed: {e!s}")
    feed_version = f"postgis-{feed_id}"
    sig_hash = _resolve_route_sig_hash(feed_id, route_id, direction_id)
    if not sig_hash:
        log("graph/preview", f"error route_id={route_id} dir={direction_id} reason=route_signature_missing")
        raise HTTPException(status_code=409, detail="Could not resolve route signature for preview cache.")

    preview_mem_key = _preview_mem_key(
        feed_version, route_id, direction_id, profile_key, pretty_osm
    )
    mem_preview = GRAPH_CACHE.get(preview_mem_key)
    if isinstance(mem_preview, dict) and mem_preview.get("route_geojson") is not None:
        elapsed = (time.perf_counter() - t0) * 1000.0
        response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
        response.headers["X-Cache-Hit"] = "memory"
        response.headers["X-Graph-Cache-Hit"] = "none"
        log(
            "graph/preview",
            f"route_id={route_id} dir={direction_id} preview_hit=memory graph_hit=none elapsed_ms={elapsed:.1f}",
        )
        return mem_preview

    db_preview = db_access_module.get_cached_route_preview_pg(
        feed_id=feed_id,
        route_id=route_id,
        direction_id=direction_id,
        profile_key=profile_key,
        pretty_osm=pretty_osm,
        route_sig_hash=sig_hash,
    )
    if db_preview and db_preview.get("preview_blob"):
        try:
            payload = pickle.loads(db_preview["preview_blob"])
            if isinstance(payload, dict) and payload.get("route_geojson") is not None:
                GRAPH_CACHE[preview_mem_key] = payload
                elapsed = (time.perf_counter() - t0) * 1000.0
                response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
                response.headers["X-Cache-Hit"] = "postgres"
                response.headers["X-Graph-Cache-Hit"] = "none"
                log(
                    "graph/preview",
                    f"route_id={route_id} dir={direction_id} preview_hit=postgres graph_hit=none elapsed_ms={elapsed:.1f}",
                )
                return payload
        except Exception:
            pass

    # Fallback: load/build graph, assemble preview, then persist preview cache.
    graph_cache_hit = "none"
    candidate_keys = _graph_cache_keys_for_lookup(route_id, direction_id, date_str)
    want_osm = "osm" if pretty_osm else "gtfs"
    cache = None
    fallback_cache = None
    for key in candidate_keys:
        if key in GRAPH_CACHE:
            if want_osm in key:
                cache = GRAPH_CACHE[key]
                graph_cache_hit = "memory"
                break
            if fallback_cache is None:
                fallback_cache = GRAPH_CACHE[key]
    if cache is None and fallback_cache is not None:
        cache = fallback_cache
        graph_cache_hit = "memory_fallback"

    if cache is None:
        cached_blob = db_access_module.get_cached_route_graph_pg(
            feed_id=feed_id,
            route_id=route_id,
            direction_id=direction_id,
            pretty_osm=pretty_osm,
            route_sig_hash=sig_hash,
        )
        if cached_blob:
            try:
                cache = pickle.loads(cached_blob)
                graph_cache_hit = "postgres"
                # hydrate canonical graph cache key
                gk = _graph_cache_key(feed_version, route_id, direction_id, profile_key, pretty_osm)
                GRAPH_CACHE[gk] = cache
            except Exception:
                cache = None

    if cache is None:
        # Last resort build path.
        _ = graph_build(
            GraphBuildRequest(
                route_id=route_id,
                direction_id=direction_id,
                date=date_str,
                pretty_osm=pretty_osm,
            ),
            response,
        )
        graph_cache_hit = "built"
        # resolve again from in-memory graph cache
        for key in _graph_cache_keys_for_lookup(route_id, direction_id, date_str):
            if key in GRAPH_CACHE and (want_osm in key or cache is None):
                cache = GRAPH_CACHE[key]
                if want_osm in key:
                    break

    if not cache:
        log(
            "graph/preview",
            f"error route_id={route_id} dir={direction_id} reason=preview_cache_miss_after_fallback "
            f"graph_hit={graph_cache_hit}",
        )
        raise HTTPException(status_code=400, detail="Preview cache miss and graph fallback failed.")

    preview = _build_preview_payload_from_cache_entry(cache)
    pat = cache["pattern"]
    payload = build_route_preview_cache_dict(
        pat.pattern_id,
        preview["stops"],
        preview["route_geojson"],
        bool(cache.get("used_osm_snapping", False)),
        feed_version,
    )
    GRAPH_CACHE[preview_mem_key] = payload
    try:
        db_access_module.save_route_preview_pg(
            feed_id=feed_id,
            route_id=route_id,
            direction_id=direction_id,
            profile_key=profile_key,
            pretty_osm=pretty_osm,
            route_sig_hash=sig_hash,
            pattern_id=str(pat.pattern_id),
            preview_blob=pickle.dumps(payload),
        )
    except Exception:
        pass

    elapsed = (time.perf_counter() - t0) * 1000.0
    response.headers["X-Elapsed-Ms"] = f"{elapsed:.1f}"
    response.headers["X-Cache-Hit"] = "built_fallback"
    response.headers["X-Graph-Cache-Hit"] = graph_cache_hit
    log(
        "graph/preview",
        f"route_id={route_id} dir={direction_id} preview_hit=built_fallback graph_hit={graph_cache_hit} elapsed_ms={elapsed:.1f}",
    )
    return payload


@app.get("/graph/cache/status")
def graph_cache_status():
    return {
        "entries": len(GRAPH_CACHE),
        "profiles": list(GRAPH_WARMUP_PROFILES),
        "warmup": dict(GRAPH_WARMUP_STATUS),
    }


@app.post("/graph/cache/warmup")
def graph_cache_warmup(
    profiles: Optional[str] = None,
    include_previews: bool = True,
):
    selected = None
    if profiles:
        selected = [p.strip() for p in str(profiles).split(",") if p.strip()]
    log(
        "graph/cache/warmup",
        f"manual trigger profiles={selected} include_previews={include_previews}",
    )
    res = _run_graph_cache_warmup(selected, include_previews=include_previews)
    return {
        "ok": True,
        "entries": len(GRAPH_CACHE),
        "warmup": res,
    }


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


def _parse_window_range(
    start_date: Optional[str],
    start_time: Optional[str],
    end_date: Optional[str],
    end_time: Optional[str],
    *,
    allow_empty: bool = False,
) -> Optional[tuple[str, int, str, int]]:
    start_date = (start_date or "").strip()
    start_time = (start_time or "").strip()
    end_date = (end_date or "").strip()
    end_time = (end_time or "").strip()
    if allow_empty and not start_date and not start_time and not end_date and not end_time:
        return None
    if not (start_date and start_time and end_date and end_time):
        raise HTTPException(
            status_code=400,
            detail="start_date/start_time/end_date/end_time are required together",
        )
    if not (len(start_date) == 8 and start_date.isdigit()):
        raise HTTPException(status_code=400, detail="start_date must be YYYYMMDD")
    if not (len(end_date) == 8 and end_date.isdigit()):
        raise HTTPException(status_code=400, detail="end_date must be YYYYMMDD")
    start_sec = _parse_hhmm_to_seconds(start_time)
    end_sec = _parse_hhmm_to_seconds(end_time)
    if start_sec < 0 or end_sec < 0:
        raise HTTPException(status_code=400, detail="time values must be non-negative")
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d").replace(
            hour=start_sec // 3600,
            minute=(start_sec % 3600) // 60,
            second=start_sec % 60,
        )
        end_dt = datetime.strptime(end_date, "%Y%m%d").replace(
            hour=end_sec // 3600,
            minute=(end_sec % 3600) // 60,
            second=end_sec % 60,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date/time values")
    if end_dt < start_dt:
        raise HTTPException(
            status_code=400, detail="end date/time must be greater than or equal to start date/time"
        )
    return start_date, start_sec, end_date, end_sec


def _segment_line_from_edges(
    pattern_id: str,
    stop_ids: List[str],
    i_first: int,
    i_last: int,
    edge_geometries: Dict[Tuple[str, str], any],
):
    """Build a single LineString for the route segment from edge i_first-1 through i_last (inclusive)."""
    from shapely.geometry import LineString
    from shapely.ops import linemerge

    parts = []
    for i in range(i_first - 1, i_last + 1):
        if i + 1 >= len(stop_ids):
            break
        node_u = pattern_stop_node_id(pattern_id, stop_ids[i], i)
        node_v = pattern_stop_node_id(pattern_id, stop_ids[i + 1], i + 1)
        eg = edge_geometries.get((node_u, node_v))
        if eg and eg.linestring and len(eg.linestring.coords) >= 2:
            parts.append(eg.linestring)
    if not parts:
        return None
    merged = linemerge(parts) if len(parts) > 1 else parts[0]
    if merged.geom_type == "LineString":
        return merged
    if merged.geom_type == "MultiLineString" and len(merged.geoms) > 0:
        return merged.geoms[0]
    return None


def _entry_exit_points_from_geometry(
    segment_line,
    blockage_polygon,
) -> Optional[Tuple[Tuple[float, float], Tuple[float, float]]]:
    """
    Get (from_pt, to_pt) where the route line crosses the blockage:
    from_pt = last point before entering, to_pt = first point after leaving (lon, lat).
    """
    if not segment_line or segment_line.is_empty or segment_line.length == 0:
        return None
    try:
        outside = segment_line.difference(blockage_polygon)
    except Exception:
        return None
    if outside.is_empty:
        return None
    from shapely.geometry import LineString

    coords_start = list(segment_line.coords)
    if not coords_start:
        return None
    first_route_pt = coords_start[0]
    last_route_pt = coords_start[-1]

    if outside.geom_type == "LineString":
        return (outside.coords[0], outside.coords[-1])
    if outside.geom_type != "MultiLineString" or len(outside.geoms) < 2:
        return None
    # Identify which segment is "before" (contains route start) and "after" (contains route end).
    dist_to_start = [
        (part, (part.coords[0][0] - first_route_pt[0]) ** 2 + (part.coords[0][1] - first_route_pt[1]) ** 2)
        for part in outside.geoms if part.coords
    ]
    dist_to_end = [
        (part, (part.coords[-1][0] - last_route_pt[0]) ** 2 + (part.coords[-1][1] - last_route_pt[1]) ** 2)
        for part in outside.geoms if part.coords
    ]
    if not dist_to_start or not dist_to_end:
        return None
    before = min(dist_to_start, key=lambda x: x[1])[0]
    after = min(dist_to_end, key=lambda x: x[1])[0]
    from_pt = before.coords[-1]
    to_pt = after.coords[0]
    return (from_pt, to_pt)


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
    use_osm_detour: bool,
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

    # Optional: use Valhalla OSM detour instead of GTFS multi-route graph.
    if use_osm_detour and VALHALLA_URL:
        from shapely.geometry import shape as _shape

        blockage_geom = _ensure_geometry(blockage_geojson)
        g_block = _shape(blockage_geom)

        # Prefer geometry-based entry/exit: where the route line crosses the blockage (not stops).
        segment_line = _segment_line_from_edges(
            pid, stop_ids, i_first, i_last, build_result.edge_geometries
        )
        from_pt = to_pt = None
        if segment_line:
            pts = _entry_exit_points_from_geometry(segment_line, g_block)
            if pts:
                from_pt, to_pt = pts

        # Fallback: use boundary stops (no walking to "first stop outside").
        if from_pt is None or to_pt is None:
            stops_by_id = {s.get("stop_id"): s for s in getattr(feed, "stops", [])}
            sb = stops_by_id.get(stop_before)
            sa = stops_by_id.get(stop_after)
            if sb and sa:
                try:
                    from_pt = (float(sb.get("stop_lon")), float(sb.get("stop_lat")))
                    to_pt = (float(sa.get("stop_lon")), float(sa.get("stop_lat")))
                except (TypeError, ValueError):
                    pass

        if from_pt is not None and to_pt is not None:
            try:
                lon_b, lat_b = from_pt[0], from_pt[1]
                lon_a, lat_a = to_pt[0], to_pt[1]
                osm = route_avoiding_polygon(
                    lon_b,
                    lat_b,
                    lon_a,
                    lat_a,
                    blockage_geojson,
                )
                if osm.success and osm.coordinates:
                    detour_coords = list(osm.coordinates)
                    snapped = map_match_coordinates(detour_coords)
                    if snapped is not None and len(snapped.coords) >= 2:
                        detour_coords = list(snapped.coords)
                    detour_geojson = {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "LineString",
                                    "coordinates": detour_coords,
                                },
                                "properties": {"kind": "osm_detour"},
                            }
                        ],
                    }
                    replaced_segment_geojson = _build_replaced_segment_geojson(
                        pattern_id=pid,
                        stop_ids=stop_ids,
                        i_first=i_first,
                        i_last=i_last,
                        edge_geometries=build_result.edge_geometries,
                    )
                    detour_stop_path = [stop_before, stop_after]
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
                        used_transfers=False,
                        error=None,
                    )
            except Exception:
                pass

    detour_params = replace(
        default_detour_graph_params(),
        transfer_radius_m=transfer_radius_m,
    )
    detour_graph_res = build_detour_graph(
        feed=feed,
        date_ymd=date_str,
        blockage_geojson=blockage_geojson,
        primary_route_id=route_id,
        primary_direction_id=direction_id,
        start_sec=start_sec,
        end_sec=end_sec,
        params=detour_params,
    )
    detour_blocked, _ = compute_blocked_edges(
        edge_geometries=detour_graph_res.edge_geometries,
        blockage_geojson=blockage_geojson,
    )
    # Impassable edges are defined only on the detour graph we search.
    blocked_for_routing = detour_blocked
    try:
        detour_path_nodes = astar_route(
            graph=detour_graph_res.graph,
            edge_geometries=detour_graph_res.edge_geometries,
            start_node_id=start_node,
            end_node_id=end_node,
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
    Given a polygon and datetime range, return the list
    of routes whose shapes intersect that polygon and have at least one trip
    running during that window. Accepts Polygon, LineString, or Point (buffered).
    """
    if not req.polygon_geojson:
        raise HTTPException(status_code=400, detail="polygon_geojson is required")

    start_date_ymd, start_sec, end_date_ymd, end_sec = _parse_window_range(
        req.start_date, req.start_time, req.end_date, req.end_time
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
            start_date_ymd=start_date_ymd,
            start_sec=start_sec,
            end_date_ymd=end_date_ymd,
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
                trip_count=r.get("trip_count"),
                last_stop_name=r.get("last_stop_name"),
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

    calendar_hint: Optional[str] = None
    if not results:
        try:
            cal_min, cal_max = db_access_module.get_active_feed_calendar_span()
        except Exception:
            cal_min, cal_max = None, None
        if cal_min is not None and cal_max is not None:
            rs = int(start_date_ymd)
            re = int(end_date_ymd)
            overlaps = not (re < cal_min or rs > cal_max)
            if not overlaps:

                def _ymd_disp(x: int) -> str:
                    s = f"{x:08d}"
                    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"

                calendar_hint = (
                    "The selected dates are outside the loaded GTFS calendar "
                    f"({_ymd_disp(cal_min)} to {_ymd_disp(cal_max)}). "
                    "Update the feed or pick dates in that range. "
                    "Area search only includes trips on scheduled service days."
                )

    log(
        "area/routes",
        f"window={start_date_ymd} {req.start_time}-{end_date_ymd} {req.end_time} routes={len(results)}",
    )
    return AreaRoutesResponse(routes=results, calendar_hint=calendar_hint)


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

    start_date_ymd, start_sec, end_date_ymd, end_sec = _parse_window_range(
        req.start_date, req.start_time, req.end_date, req.end_time
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

    log(
        "detours/by-area",
        f"mode={req.mode}, start_date={start_date_ymd}, end_date={end_date_ymd}, "
        f"time_window={req.start_time}-{req.end_time}, "
        f"max_routes={req.max_routes}, transfer_radius_m={req.transfer_radius_m}",
    )

    if req.mode == DetourByAreaMode.route:
        if not req.route_id:
            raise HTTPException(status_code=400, detail="route_id is required when mode='route'")
        result = _compute_route_detour_by_area(
            feed=feed,
            date_str=start_date_ymd,
            start_sec=start_sec,
            end_sec=end_sec,
            blockage_geojson=polygon_geojson,
            route_id=req.route_id,
            direction_id=req.direction_id,
            transfer_radius_m=req.transfer_radius_m,
            use_osm_detour=req.use_osm_detour,
        )
        log(
            "detours/by-area",
            f"single route_id={result.route_id!r}, "
            f"direction_id={result.direction_id!r}, "
            f"blocked_edges={result.blocked_edges_count}, "
            f"used_transfers={result.used_transfers}, "
            f"error={result.error!r}",
        )
        return DetourByAreaResponse(mode=req.mode, result=result, feed_version=feed.version_id)

    # mode == 'all'
    # Find all routes in area/time window (capped by max_routes) and compute per-route detours.
    try:
        routes_raw = find_routes_in_polygon(
            feed=feed,
            polygon_geojson=polygon_geojson,
            start_date_ymd=start_date_ymd,
            start_sec=start_sec,
            end_date_ymd=end_date_ymd,
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
            date_str=start_date_ymd,
            start_sec=start_sec,
            end_sec=end_sec,
            blockage_geojson=polygon_geojson,
            route_id=route_id,
            direction_id=direction_id,
            transfer_radius_m=req.transfer_radius_m,
            use_osm_detour=req.use_osm_detour,
        )
        results.append(res)

    log(
        "detours/by-area",
        f"computed {len(results)} route detours "
        f"for blockage on start_date={start_date_ymd}, end_date={end_date_ymd}, "
        f"time_window={req.start_time}-{req.end_time}",
    )

    return DetourByAreaResponse(mode=req.mode, results=results, feed_version=feed.version_id)


@app.post("/stop/routes", response_model=StopRoutesResponse)
def stop_routes(req: StopRoutesRequest):
    """Return routes that serve the given stop within the selected datetime range."""
    start_date_ymd, start_sec, end_date_ymd, end_sec = _parse_window_range(
        req.start_date, req.start_time, req.end_date, req.end_time
    )
    try:
        routes_raw = db_access_module.get_routes_serving_stop_pg_range(
            stop_id=req.stop_id,
            start_date_ymd=start_date_ymd,
            start_sec=start_sec,
            end_date_ymd=end_date_ymd,
            end_sec=end_sec,
            max_results=req.max_results,
        )
    except db_access_module.StopRoutesQueryTimeoutError as e:
        raise HTTPException(
            status_code=504,
            detail=f"{e}. Search took too long on the backend; please try again shortly.",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stop routes failed: {e}")
    return StopRoutesResponse(
        stop_id=req.stop_id,
        routes=[StopRouteResult(**r) for r in routes_raw],
    )

