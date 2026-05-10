from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional
import hashlib
import logging.config
import pickle
import time
import traceback

import httpx
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware

from backend.infra.gtfs_updater import update_feed, get_feed_status
from backend.infra.feed_postgis import load_active_feed
from backend.domain.pattern_builder import (
    PatternBuilder,
    RoutePattern,
    resolve_most_frequent_route_pattern,
    resolve_representative_trip_id,
)
from backend.domain.graph_builder import (
    GraphBuilder,
    build_graph_for_pattern_from_postgis,
    pattern_stop_node_id,
    parse_pattern_stop_node_id,
)
from backend.adapters.osm_pretty import map_match_pattern, map_match_coordinates
from backend.domain.router_core import (
    astar_route,
    collect_path_geojson,
    compute_blocked_edges,
    dijkstra_best_route,
    dfs_best_route,
    routing_edge_weight,
)
from backend.domain.detour_graph import build_detour_graph, default_detour_graph_params
from backend.domain.routing_policy import default_by_area_routing_policy
from backend.adapters.osm_detour import route_avoiding_polygon, evaluate_road_feasibility_for_candidate
from backend.domain.detour_geo_validation import (
    road_geojson_clear_of_blockage,
    road_geojson_has_routable_geometry,
    geometry_inflation_within_thresholds,
)
from backend.domain.detour_instructions_text import instructions_text_he_to_steps
from backend.domain.detour_narrative_geo import try_build_narrative_detour_linestring
from backend.adapters.geocoding_nominatim import nominatim_search_raw
from backend.domain.detour_street_override_response import (
    build_detour_response_from_road_override,
    build_detour_response_instructions_only,
)
from backend.infra.config import (
    VALHALLA_URL,
    HYBRID_DETOUR_ENABLED,
    DETOUR_V2_ENABLED,
    DETOUR_V2_AI_LOG,
    DETOUR_V2_DEBUG,
    DETOUR_ENGINE,
)
from backend.mcp_server.schemas.api_models import (
    RouteSearchRequest,
    RouteInfo,
    FeedUpdateResponse,
    FeedStatusResponse,
    GraphBuildRequest,
    GraphBuildResponse,
    GraphStopsResponse,
    GraphStopsResponseStop,
    AreaRoutesQuery,
    AreaRoutesResponse,
    AreaRouteResult,
    StopInBounds,
    StopRoutesRequest,
    StopRoutesResponse,
    StopRouteResult,
    StopSearchResult,
    DetourByAreaRouteResult,
    DetourTurnStep,
    GeocodeResult,
    IncidentCreateRequest,
    IncidentCreateResponse,
    DetourComputeV2Request,
    DetourComputeV2Response,
    BusEdgeConstraintRequest,
    BusTurnConstraintRequest,
    ConstraintCreateResponse,
    DetourApproveV2Request,
    DetourApproveV2Response,
    DetourV2DetailResponse,
)
from backend.infra.config import (
    GOVMAP_TILE_UPSTREAM_TEMPLATE,
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
from backend.domain.route_preview_payload import build_route_preview_cache_dict
from backend.domain.area_search import find_routes_in_polygon
from backend.infra import db_access as db_access_module
from backend.infra.logging_utils import log
from backend.domain.service_calendar import ServiceCalendar, resolve_service_profile
from backend.infra.uvicorn_logging import LOGGING_CONFIG
from backend.domain.detour_v2.compute import compute_detour_for_trip
from backend.domain.detour_v2.serialize import detour_compute_output_to_dict, format_detour_ai_log_line
from backend.domain.detour_v2.policy import get_default_policy
from backend.domain.detour_v2.incident_projector import project_incident_polygon
from backend.domain.detour_v2 import detour_memory as detour_memory_v2
from backend.adapters.osm_detour import valhalla_health

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


def create_app() -> FastAPI:
    """Compatibility app factory used by transport wrappers."""
    return app


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


@app.get("/api/v1/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@app.get("/api/v1/govmap-tiles/{z}/{x}/{y}")
def govmap_tile_proxy(z: int, x: int, y: int):
    """
    Same-origin raster tile relay for MapLibre when VITE_GOVMAP_USE_PROXY=1.
    Set GOVMAP_TILE_UPSTREAM_TEMPLATE with {z}, {x}, {y} placeholders (and query params as needed).
    """
    tpl = GOVMAP_TILE_UPSTREAM_TEMPLATE
    if not tpl:
        raise HTTPException(
            status_code=503,
            detail="GovMap tile proxy is not configured (set GOVMAP_TILE_UPSTREAM_TEMPLATE).",
        )
    url = tpl.replace("{z}", str(z)).replace("{x}", str(x)).replace("{y}", str(y))
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            r = client.get(url)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Tile upstream error: {exc}") from exc
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Upstream tile HTTP {r.status_code}")
    ct = r.headers.get("content-type", "image/png")
    return Response(
        content=r.content,
        media_type=ct,
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/api/v1/routes/tiles/{z}/{x}/{y}.mvt")
def routes_vector_tile(
    z: int,
    x: int,
    y: int,
    scope: str = "all",
    render_mode: str = "balanced",
    start_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_date: Optional[str] = None,
    end_time: Optional[str] = None,
):
    """
    Route vector tiles for rendering an all-routes background layer.

    scope='all': all route-direction geometries in the active feed.
    scope='time_window': only route-direction geometries active in the supplied window.
    """
    if z < 0 or z > 22:
        raise HTTPException(status_code=400, detail="z must be between 0 and 22")
    if x < 0 or y < 0:
        raise HTTPException(status_code=400, detail="x/y must be non-negative")

    normalized_scope = (scope or "all").strip().lower()
    if normalized_scope not in ("all", "time_window"):
        raise HTTPException(status_code=400, detail="scope must be one of: all, time_window")
    normalized_render_mode = (render_mode or "balanced").strip().lower()
    if normalized_render_mode not in ("always_visible", "balanced"):
        raise HTTPException(status_code=400, detail="render_mode must be one of: always_visible, balanced")

    start_date_ymd: Optional[str] = None
    start_sec: Optional[int] = None
    end_date_ymd: Optional[str] = None
    end_sec: Optional[int] = None
    if normalized_scope == "time_window":
        start_date_ymd, start_sec, end_date_ymd, end_sec = _parse_window_range(
            start_date, start_time, end_date, end_time
        )

    try:
        tile = db_access_module.get_routes_vector_tile_mvt(
            z=z,
            x=x,
            y=y,
            scope=normalized_scope,
            render_mode=normalized_render_mode,
            start_date_ymd=start_date_ymd,
            start_sec=start_sec,
            end_date_ymd=end_date_ymd,
            end_sec=end_sec,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not build route vector tile: {e}")

    # Keep tile caching short so zoom/pan reflects backend tile changes quickly.
    cache_control = "public, max-age=90, must-revalidate"
    if normalized_scope == "time_window":
        cache_control = "public, max-age=30, must-revalidate"
    log(
        "routes/tiles",
        f"scope={normalized_scope} render_mode={normalized_render_mode} z={z} x={x} y={y} bytes={len(tile)}",
    )
    return Response(
        content=tile,
        media_type="application/vnd.mapbox-vector-tile",
        headers={
            "Cache-Control": cache_control,
        },
    )


@app.get("/api/v1/routes/coverage/{z}/{x}/{y}.mvt")
def routes_coverage_tile(
    z: int,
    x: int,
    y: int,
):
    """Generalized low-zoom route-presence tiles (feed-wide context)."""
    if z < 0 or z > 22:
        raise HTTPException(status_code=400, detail="z must be between 0 and 22")
    if x < 0 or y < 0:
        raise HTTPException(status_code=400, detail="x/y must be non-negative")
    try:
        tile = db_access_module.get_routes_coverage_tile_mvt(z=z, x=x, y=y)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not build route coverage tile: {e}")
    log("routes/coverage", f"z={z} x={x} y={y} bytes={len(tile)}")
    return Response(
        content=tile,
        media_type="application/vnd.mapbox-vector-tile",
        headers={"Cache-Control": "public, max-age=180, must-revalidate"},
    )


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
def startup_detour_v2_schema():
    """Create detour v2 / incident tables when missing (older local DBs)."""
    try:
        db_access_module.ensure_detour_v2_support_schema()
        log("db/schema", "detour v2 support tables ensured (if missing)")
    except Exception as e:
        log("db/schema", f"ensure detour v2 support schema failed (persist may error until fixed): {e}")
    try:
        db_access_module.ensure_pattern_physical_layer_schema()
        log("db/schema", "pattern physical layer tables ensured (if missing)")
    except Exception as e:
        log("db/schema", f"ensure pattern physical layer schema failed: {e}")
    try:
        db_access_module.ensure_pattern_legal_anchor_schema()
        log("db/schema", "pattern legal anchor tables ensured (if missing)")
    except Exception as e:
        log("db/schema", f"ensure pattern legal anchor schema failed: {e}")


@app.on_event("startup")
def startup_detour_v2_health():
    """Log Valhalla health at startup (C2). Does not crash on failure."""
    try:
        health = valhalla_health(timeout_s=5.0)
        if health.get("ok"):
            log(
                "valhalla/health",
                f"ok=true version={health.get('version')} tileset_last_modified={health.get('tileset_last_modified')}",
            )
        else:
            log("valhalla/health", f"ok=false error={health.get('error')}")
    except Exception as e:
        log("valhalla/health", f"health check exception: {e}")


@app.on_event("startup")
def startup_graph_warmup():
    if not GRAPH_WARMUP_ENABLED:
        log("graph/cache/warmup", "startup skipped (GRAPH_WARMUP_ENABLED=false)")
        return
    log("graph/cache/warmup", "startup trigger")
    _run_graph_cache_warmup()


@app.get("/api/v1/geocode", response_model=list[GeocodeResult])
def geocode(q: str, limit: int = 5):
    """Geocode an address or place name via Nominatim. Use for address search in the UI."""
    q = (q or "").strip()
    if not q or len(q) < 2:
        return []
    try:
        data = nominatim_search_raw(q, limit=min(limit, 10), countrycodes="il")
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


@app.get("/api/v1/stops/in-bounds", response_model=list[StopInBounds])
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


@app.get("/api/v1/stops/search", response_model=list[StopSearchResult])
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


@app.post("/api/v1/routes/search", response_model=list[RouteInfo])
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


@app.post("/api/v1/feed/update", response_model=FeedUpdateResponse)
def feed_update():
    result = update_feed()
    return FeedUpdateResponse(
        updated=result["updated"],
        online_ok=result["online_ok"],
        message=result["message"],
        active=result.get("active"),
    )


@app.get("/api/v1/feed/status", response_model=FeedStatusResponse)
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


@app.post("/api/v1/graph/build", response_model=GraphBuildResponse)
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


@app.get("/api/v1/graph/stops", response_model=GraphStopsResponse)
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


@app.get("/api/v1/graph/geojson")
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


@app.get("/api/v1/graph/preview")
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


@app.get("/api/v1/graph/cache/status")
def graph_cache_status():
    return {
        "entries": len(GRAPH_CACHE),
        "profiles": list(GRAPH_WARMUP_PROFILES),
        "warmup": dict(GRAPH_WARMUP_STATUS),
    }


@app.post("/api/v1/graph/cache/warmup")
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


def _hybrid_osm_detour_segment(
    *,
    blockage_geojson: Dict,
    segment_line=None,
    fallback_from_pt: Optional[Tuple[float, float]] = None,
    fallback_to_pt: Optional[Tuple[float, float]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Compute an intersection-aware bypass segment on OSM and return detour segment payload.
    Returns None when no valid bypass could be built.
    """
    if not VALHALLA_URL or not VALHALLA_URL.strip():
        return None
    from shapely.geometry import shape as _shape, LineString

    try:
        g_block = _shape(_ensure_geometry(blockage_geojson))
    except Exception:
        return None
    if g_block.is_empty:
        return None

    from_pt = to_pt = None
    if segment_line is not None:
        pts = _entry_exit_points_from_geometry(segment_line, g_block)
        if pts:
            from_pt, to_pt = pts
    if (from_pt is None or to_pt is None) and fallback_from_pt and fallback_to_pt:
        from_pt, to_pt = fallback_from_pt, fallback_to_pt
    if from_pt is None or to_pt is None:
        return None

    osm = route_avoiding_polygon(
        float(from_pt[0]),
        float(from_pt[1]),
        float(to_pt[0]),
        float(to_pt[1]),
        blockage_geojson,
    )
    if not osm.success or not osm.coordinates:
        return None
    detour_coords = list(osm.coordinates)
    snapped = map_match_coordinates(detour_coords)
    if snapped is not None and len(snapped.coords) >= 2:
        detour_coords = list(snapped.coords)
    if len(detour_coords) < 2:
        return None

    try:
        ls = LineString(detour_coords)
        inter = ls.intersection(g_block)
        if (not inter.is_empty) and (
            float(getattr(inter, "length", 0.0) or 0.0) > 0.0
            or float(getattr(inter, "area", 0.0) or 0.0) > 0.0
        ):
            return None
    except Exception:
        return None

    return {
        "detour_coords": detour_coords,
        "distance_m": float(osm.distance_m),
        "time_s": float(osm.time_s),
        "entry_pt": (float(from_pt[0]), float(from_pt[1])),
        "exit_pt": (float(to_pt[0]), float(to_pt[1])),
        "turn_by_turn": getattr(osm, "turn_by_turn", None) or [],
    }


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


def _detour_by_area_from_street_road(
    *,
    route_id: str,
    direction_id: Optional[str],
    pattern_id: str,
    blocked_edges_count: int,
    stop_before: str,
    stop_after: str,
    road_geojson: Dict[str, Any],
    turn_steps: Optional[List[Dict[str, Any]]],
    replaced_segment_geojson: Dict[str, Any],
    from_override: bool,
) -> DetourByAreaRouteResult:
    from backend.domain.detour_geo_validation import road_geojson_to_path_feature_collection

    detour_geojson = road_geojson_to_path_feature_collection(road_geojson)
    tbt: Optional[List[DetourTurnStep]] = None
    if turn_steps:
        parsed: List[DetourTurnStep] = []
        for x in turn_steps:
            try:
                parsed.append(DetourTurnStep.model_validate(x))
            except Exception:
                continue
        tbt = parsed or None
    return DetourByAreaRouteResult(
        route_id=route_id,
        direction_id=direction_id,
        pattern_id=pattern_id,
        blocked_edges_count=blocked_edges_count,
        stop_before=stop_before,
        stop_after=stop_after,
        detour_stop_path=[stop_before, stop_after],
        detour_geojson=detour_geojson,
        replaced_segment_geojson=replaced_segment_geojson,
        used_transfers=False,
        error=None,
        turn_by_turn=tbt,
        from_override=from_override,
        instructions_only=False,
        reason_code="stored_override_used" if from_override else "request_override_used",
        strategy_used="street_override",
        confidence=0.95 if from_override else 0.85,
        diagnostics={"override_source": "stored" if from_override else "request"},
    )


def _detour_by_area_instructions_only(
    *,
    route_id: str,
    direction_id: Optional[str],
    pattern_id: str,
    blocked_edges_count: int,
    stop_before: str,
    stop_after: str,
    turn_steps: Optional[List[Dict[str, Any]]],
    replaced_segment_geojson: Dict[str, Any],
    from_override: bool,
) -> DetourByAreaRouteResult:
    tbt: Optional[List[DetourTurnStep]] = None
    if turn_steps:
        parsed: List[DetourTurnStep] = []
        for x in turn_steps:
            try:
                parsed.append(DetourTurnStep.model_validate(x))
            except Exception:
                continue
        tbt = parsed or None
    empty_fc: Dict[str, Any] = {"type": "FeatureCollection", "features": []}
    return DetourByAreaRouteResult(
        route_id=route_id,
        direction_id=direction_id,
        pattern_id=pattern_id,
        blocked_edges_count=blocked_edges_count,
        stop_before=stop_before,
        stop_after=stop_after,
        detour_stop_path=[stop_before, stop_after],
        detour_geojson=empty_fc,
        replaced_segment_geojson=replaced_segment_geojson,
        used_transfers=False,
        error=None,
        turn_by_turn=tbt,
        from_override=from_override,
        instructions_only=True,
        reason_code="instructions_only_fallback",
        strategy_used="instructions_only",
        confidence=0.3,
        diagnostics={"override_source": "stored" if from_override else "request"},
    )


def _compute_route_detour_by_area(
    feed,
    date_str: str,
    start_sec: int,
    end_sec: int,
    blockage_geojson: Dict,
    blockage_hash: str,
    cache_mode: str,
    policy_profile: str,
    route_id: str,
    direction_id: Optional[str],
    transfer_radius_m: float,
    use_osm_detour: bool,
    apply_request_street_override: bool = False,
    street_override_road: Optional[Dict[str, Any]] = None,
    street_override_turns: Optional[List[Dict[str, Any]]] = None,
    street_override_remember: bool = False,
    override_stop_before: Optional[str] = None,
    override_stop_after: Optional[str] = None,
    street_instructions_text_he: Optional[str] = None,
    prefer_osm_detour: bool = False,
    routing_engine: str = "astar",
) -> DetourByAreaRouteResult:
    from shapely.geometry import shape as _shape

    _re = (routing_engine or "astar").strip().lower()
    if _re not in ("astar", "dfs"):
        _re = "astar"
    cache_policy_profile = f"{policy_profile}|re={_re}"

    def _geom_violates_blockage(route_geom, blocked_geom) -> bool:
        """True when route geometry actually goes through blockage interior."""
        try:
            if route_geom.crosses(blocked_geom):
                return True
        except Exception:
            pass
        try:
            if route_geom.within(blocked_geom) or blocked_geom.contains(route_geom):
                return True
        except Exception:
            pass
        try:
            inter = route_geom.intersection(blocked_geom)
            if not inter.is_empty:
                # If intersection has measurable length/area, it's a true overlap, not a boundary touch.
                if float(getattr(inter, "length", 0.0) or 0.0) > 0.0:
                    return True
                if float(getattr(inter, "area", 0.0) or 0.0) > 0.0:
                    return True
        except Exception:
            pass
        return False

    def _geojson_intersects_blockage(features_geojson: Optional[Dict], blockage_geom_json: Dict) -> bool:
        if not isinstance(features_geojson, dict):
            return False
        try:
            blocked_geom = _shape(blockage_geom_json)
            if blocked_geom.is_empty:
                return False
        except Exception:
            return False
        if features_geojson.get("type") == "FeatureCollection":
            features = features_geojson.get("features") or []
            for feat in features:
                geom_json = (feat or {}).get("geometry")
                if not geom_json:
                    continue
                try:
                    if _geom_violates_blockage(_shape(geom_json), blocked_geom):
                        return True
                except Exception:
                    continue
            return False
        if features_geojson.get("type") == "Feature":
            geom_json = features_geojson.get("geometry")
            if not geom_json:
                return False
            try:
                return bool(_geom_violates_blockage(_shape(geom_json), blocked_geom))
            except Exception:
                return False
        try:
            return bool(_geom_violates_blockage(_shape(features_geojson), blocked_geom))
        except Exception:
            return False

    def _path_edges_intersecting_blockage(
        path_nodes: List[str],
        blockage_geom_json: Dict,
        edge_geometries: Dict[tuple[str, str], Any],
    ) -> set[tuple[str, str]]:
        offenders: set[tuple[str, str]] = set()
        try:
            blocked_geom = _shape(blockage_geom_json)
            if blocked_geom.is_empty:
                return offenders
        except Exception:
            return offenders
        for u, v in zip(path_nodes, path_nodes[1:]):
            eg = edge_geometries.get((u, v))
            if not eg or not getattr(eg, "linestring", None):
                continue
            try:
                if _geom_violates_blockage(eg.linestring, blocked_geom):
                    offenders.add((u, v))
            except Exception:
                continue
        return offenders

    def _with_meta(
        res: DetourByAreaRouteResult,
        *,
        reason_code: str,
        strategy_used: str,
        confidence: float,
        diagnostics: Optional[Dict[str, Any]] = None,
    ) -> DetourByAreaRouteResult:
        res.reason_code = reason_code
        res.strategy_used = strategy_used
        res.confidence = confidence
        diag = dict(diagnostics or {})
        diag.setdefault("detour_engine", DETOUR_ENGINE)
        res.diagnostics = diag
        return res

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
            res = DetourByAreaRouteResult(
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
            return _with_meta(
                res,
                reason_code="route_not_in_window",
                strategy_used="window_filter",
                confidence=1.0,
            )

    feed_id: Optional[int] = None
    try:
        feed_id = db_access_module.get_active_feed_id()
    except Exception:
        feed_id = None
    route_sig_hash = _resolve_route_sig_hash(feed_id, route_id, direction_id) if feed_id is not None else None
    route_sig_hash = route_sig_hash or ""
    if route_sig_hash:
        try:
            cached = db_access_module.get_cached_detour_by_area_pg(
                feed_id=feed_id,
                mode=cache_mode,
                route_id=route_id,
                direction_id=direction_id,
                date_ymd=date_str,
                start_sec=start_sec,
                end_sec=end_sec,
                transfer_radius_m=transfer_radius_m,
                use_osm_detour=use_osm_detour,
                policy_profile=cache_policy_profile,
                blockage_hash=blockage_hash,
                route_sig_hash=route_sig_hash,
            )
        except Exception:
            cached = None
        if cached is not None:
            cached_geo = cached.get("detour_geojson")
            if _geojson_intersects_blockage(cached_geo, blockage_geojson):
                log(
                    "detours/by-area",
                    f"cache=invalid_intersects_blockage mode={cache_mode} route_id={route_id!r} direction_id={direction_id!r}",
                )
            else:
                log(
                    "detours/by-area",
                    f"cache=hit mode={cache_mode} route_id={route_id!r} direction_id={direction_id!r}",
                )
                return DetourByAreaRouteResult.model_validate(cached)
    log(
        "detours/by-area",
        f"cache=miss mode={cache_mode} route_id={route_id!r} direction_id={direction_id!r}",
    )

    chosen = resolve_most_frequent_route_pattern(
        feed, route_id, direction_id, date_str
    )
    if chosen is None:
        res = DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=direction_id,
            pattern_id=None,
            blocked_edges_count=0,
            error="No patterns found for route/date",
        )
        return _with_meta(
            res,
            reason_code="pattern_missing",
            strategy_used="pattern_selection",
            confidence=1.0,
        )

    graph_builder = GraphBuilder(feed)
    build_result = graph_builder.build_graph_for_pattern(chosen)

    blocked_edges, _ = compute_blocked_edges(
        edge_geometries=build_result.edge_geometries,
        blockage_geojson=blockage_geojson,
    )
    if not blocked_edges:
        # Route not affected by this blockage.
        res = DetourByAreaRouteResult(
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
        return _with_meta(
            res,
            reason_code="route_not_affected",
            strategy_used="impact_detection",
            confidence=1.0,
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
        res = DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=len(blocked_edges),
            error="Could not map blocked edges to pattern chain",
        )
        return _with_meta(
            res,
            reason_code="blocked_span_unresolved",
            strategy_used="impact_detection",
            confidence=0.0,
        )
    i_first = min(seqs)
    i_last = max(seqs)
    if i_last >= len(stop_ids) - 1:
        i_last = len(stop_ids) - 2
    stop_before = stop_ids[i_first]
    stop_after = stop_ids[i_last + 1]

    start_node = pattern_stop_node_id(pid, stop_before, i_first)
    end_node = pattern_stop_node_id(pid, stop_after, i_last + 1)

    replaced_segment_geojson = _build_replaced_segment_geojson(
        pattern_id=pid,
        stop_ids=stop_ids,
        i_first=i_first,
        i_last=i_last,
        edge_geometries=build_result.edge_geometries,
    )

    def _merged_area_manual_steps() -> List[Dict[str, Any]]:
        if street_override_turns:
            return list(street_override_turns)
        if street_instructions_text_he and str(street_instructions_text_he).strip():
            return instructions_text_he_to_steps(street_instructions_text_he)
        return []

    def _override_endpoints_match() -> bool:
        if override_stop_before is not None and override_stop_before != stop_before:
            return False
        if override_stop_after is not None and override_stop_after != stop_after:
            return False
        return True

    stored_road_result: Optional[DetourByAreaRouteResult] = None
    stored_instructions_result: Optional[DetourByAreaRouteResult] = None
    if feed_id is not None:
        okey_ba = db_access_module.build_street_override_key(
            "by_area",
            route_id,
            direction_id,
            blockage_hash,
            stop_before,
            stop_after,
            route_sig_hash,
        )
        try:
            stored_ba = db_access_module.get_detour_street_override_pg(feed_id, okey_ba)
        except Exception:
            stored_ba = None
        if stored_ba:
            sroad = stored_ba["road_geojson"]
            st_steps = stored_ba.get("turn_by_turn") or []
            if road_geojson_has_routable_geometry(sroad) and road_geojson_clear_of_blockage(
                sroad, blockage_geojson
            ):
                stored_road_result = _detour_by_area_from_street_road(
                    route_id=route_id,
                    direction_id=chosen.direction_id,
                    pattern_id=chosen.pattern_id,
                    blocked_edges_count=len(blocked_edges),
                    stop_before=stop_before,
                    stop_after=stop_after,
                    road_geojson=sroad,
                    turn_steps=st_steps,
                    replaced_segment_geojson=replaced_segment_geojson,
                    from_override=True,
                )
            if st_steps and not road_geojson_has_routable_geometry(sroad):
                stored_instructions_result = _detour_by_area_instructions_only(
                    route_id=route_id,
                    direction_id=chosen.direction_id,
                    pattern_id=chosen.pattern_id,
                    blocked_edges_count=len(blocked_edges),
                    stop_before=stop_before,
                    stop_after=stop_after,
                    turn_steps=st_steps,
                    replaced_segment_geojson=replaced_segment_geojson,
                    from_override=True,
                )

    @dataclass
    class _GTFSCandidate:
        path_nodes: List[str]
        stop_path: List[str]
        detour_geojson: Dict[str, Any]
        used_transfers: bool
        gtfs_cost: float
        gtfs_distance_m: float
        gtfs_time_s: float

    def _extract_gtfs_candidates(max_candidates: int = 3) -> List[_GTFSCandidate]:
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
            replaced_segment_geojson=replaced_segment_geojson,
        )
        detour_blocked, _ = compute_blocked_edges(
            edge_geometries=detour_graph_res.edge_geometries,
            blockage_geojson=blockage_geojson,
        )
        blocked_for_routing = set(detour_blocked)
        area_pol = default_by_area_routing_policy()

        def _path_cost(path_nodes: List[str]) -> float:
            c = 0.0
            for u, v in zip(path_nodes, path_nodes[1:]):
                attrs = detour_graph_res.graph.get_edge_data(u, v, default={})
                c += routing_edge_weight(
                    detour_graph_res.graph,
                    u,
                    v,
                    attrs,
                    area_pol,
                    blocked_edges=set(),
                )
            return c

        def _make_candidate(path_nodes: List[str]) -> _GTFSCandidate:
            detour_geojson = collect_path_geojson(
                edge_geometries=detour_graph_res.edge_geometries,
                path=path_nodes,
            )
            stop_path_local = [
                detour_graph_res.graph.nodes[n]["stop_id"]
                for n in path_nodes
                if detour_graph_res.graph.nodes[n].get("stop_id") is not None
            ]
            used_transfers_local = False
            total_dist = 0.0
            total_time = 0.0
            for u, v in zip(path_nodes, path_nodes[1:]):
                data = detour_graph_res.graph.get_edge_data(u, v, default={})
                if data.get("is_transfer"):
                    used_transfers_local = True
                total_dist += float(data.get("distance_m", 0.0))
                total_time += float(data.get("travel_time_s", 0.0))
            return _GTFSCandidate(
                path_nodes=path_nodes,
                stop_path=stop_path_local,
                detour_geojson=detour_geojson,
                used_transfers=used_transfers_local,
                gtfs_cost=_path_cost(path_nodes),
                gtfs_distance_m=total_dist,
                gtfs_time_s=total_time,
            )

        candidates: List[_GTFSCandidate] = []
        seen_paths: set[Tuple[str, ...]] = set()
        for _attempt in range(max_candidates * 4):
            try:
                if _re in ("dfs", "dijkstra"):
                    candidate_path = dijkstra_best_route(
                        graph=detour_graph_res.graph,
                        edge_geometries=detour_graph_res.edge_geometries,
                        start_node_id=start_node,
                        end_node_id=end_node,
                        blocked_edges=blocked_for_routing,
                        policy=area_pol,
                        max_time_s=3.0,
                    )
                else:
                    candidate_path = astar_route(
                        graph=detour_graph_res.graph,
                        edge_geometries=detour_graph_res.edge_geometries,
                        start_node_id=start_node,
                        end_node_id=end_node,
                        blocked_edges=blocked_for_routing,
                        policy=area_pol,
                    )
            except Exception:
                candidate_path = None
            if not candidate_path:
                break
            key = tuple(candidate_path)
            if key in seen_paths:
                break
            extra_blocked = _path_edges_intersecting_blockage(
                candidate_path, blockage_geojson, detour_graph_res.edge_geometries
            )
            if not extra_blocked:
                seen_paths.add(key)
                cand = _make_candidate(candidate_path)
                if not _geojson_intersects_blockage(cand.detour_geojson, blockage_geojson):
                    candidates.append(cand)
                    if len(candidates) >= max_candidates:
                        break
                # Perturb for alternative candidates by blocking the longest traversed edge.
                removable: Optional[Tuple[str, str]] = None
                max_dist = -1.0
                for u, v in zip(candidate_path, candidate_path[1:]):
                    if (u, v) in blocked_for_routing:
                        continue
                    d = float(detour_graph_res.graph.get_edge_data(u, v, default={}).get("distance_m", 0.0))
                    if d > max_dist:
                        max_dist = d
                        removable = (u, v)
                if removable is None:
                    break
                blocked_for_routing.add(removable)
                continue
            blocked_for_routing = blocked_for_routing.union(extra_blocked)
        return candidates

    merged_steps = _merged_area_manual_steps()

    # Request body includes detour_road_geojson: apply it before automatic OSM/GTFS detours.
    # Operator-drawn paths are trusted; do not reject for clipping the blockage polygon.
    if (
        apply_request_street_override
        and street_override_road
        and road_geojson_has_routable_geometry(street_override_road)
    ):
        if not _override_endpoints_match():
            return _with_meta(DetourByAreaRouteResult(
                route_id=route_id,
                direction_id=chosen.direction_id,
                pattern_id=chosen.pattern_id,
                blocked_edges_count=len(blocked_edges),
                stop_before=stop_before,
                stop_after=stop_after,
                error=(
                    "stop_before/stop_after do not match the blockage segment for this route. "
                    "Clear the stop overrides or fix direction."
                ),
            ), reason_code="override_endpoints_mismatch", strategy_used="street_override", confidence=0.0)
        res = _detour_by_area_from_street_road(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=len(blocked_edges),
            stop_before=stop_before,
            stop_after=stop_after,
            road_geojson=street_override_road,
            turn_steps=merged_steps or None,
            replaced_segment_geojson=replaced_segment_geojson,
            from_override=False,
        )
        if street_override_remember and feed_id is not None:
            try:
                db_access_module.save_detour_street_override_pg(
                    feed_id,
                    db_access_module.build_street_override_key(
                        "by_area",
                        route_id,
                        direction_id,
                        blockage_hash,
                        stop_before,
                        stop_after,
                        route_sig_hash,
                    ),
                    "by_area",
                    route_id,
                    direction_id,
                    blockage_hash,
                    stop_before,
                    stop_after,
                    route_sig_hash,
                    street_override_road,
                    merged_steps,
                )
            except Exception:
                pass
        if route_sig_hash:
            try:
                payload = res.model_dump() if hasattr(res, "model_dump") else res.dict()
                db_access_module.save_detour_by_area_pg(
                    feed_id=feed_id,
                    mode=cache_mode,
                    route_id=route_id,
                    direction_id=direction_id,
                    date_ymd=date_str,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    transfer_radius_m=transfer_radius_m,
                    use_osm_detour=use_osm_detour,
                    policy_profile=cache_policy_profile,
                    blockage_hash=blockage_hash,
                    route_sig_hash=route_sig_hash,
                    result_json=payload,
                )
            except Exception:
                pass
        return res

    def _persist_result(res: DetourByAreaRouteResult) -> None:
        if not route_sig_hash:
            return
        try:
            payload = res.model_dump() if hasattr(res, "model_dump") else res.dict()
            db_access_module.save_detour_by_area_pg(
                feed_id=feed_id,
                mode=cache_mode,
                route_id=route_id,
                direction_id=direction_id,
                date_ymd=date_str,
                start_sec=start_sec,
                end_sec=end_sec,
                transfer_radius_m=transfer_radius_m,
                use_osm_detour=use_osm_detour,
                policy_profile=cache_policy_profile,
                blockage_hash=blockage_hash,
                route_sig_hash=route_sig_hash,
                result_json=payload,
            )
        except Exception:
            pass

    def _from_gtfs_candidate(
        cand: _GTFSCandidate,
        *,
        reason_code: str,
        strategy_used: str,
        confidence: float,
        diagnostics: Optional[Dict[str, Any]] = None,
    ) -> DetourByAreaRouteResult:
        res = DetourByAreaRouteResult(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=len(blocked_edges),
            stop_before=stop_before,
            stop_after=stop_after,
            detour_stop_path=cand.stop_path,
            detour_geojson=cand.detour_geojson,
            replaced_segment_geojson=replaced_segment_geojson,
            used_transfers=cand.used_transfers,
            error=None,
        )
        return _with_meta(
            res,
            reason_code=reason_code,
            strategy_used=strategy_used,
            confidence=confidence,
            diagnostics=diagnostics,
        )

    def _try_hybrid_from_gtfs_candidates(cands: List[_GTFSCandidate]) -> Optional[DetourByAreaRouteResult]:
        if not (use_osm_detour and VALHALLA_URL and cands):
            return None
        stops_by_id = {s.get("stop_id"): s for s in getattr(feed, "stops", [])}
        best: Optional[Tuple[float, DetourByAreaRouteResult]] = None
        for cand in cands:
            if not cand.stop_path:
                continue
            entry_id = cand.stop_path[0]
            exit_id = cand.stop_path[-1]
            s_from = stops_by_id.get(entry_id)
            s_to = stops_by_id.get(exit_id)
            if not s_from or not s_to:
                continue
            try:
                from_lon = float(s_from.get("stop_lon"))
                from_lat = float(s_from.get("stop_lat"))
                to_lon = float(s_to.get("stop_lon"))
                to_lat = float(s_to.get("stop_lat"))
            except Exception:
                continue
            feas = evaluate_road_feasibility_for_candidate(
                from_lon=from_lon,
                from_lat=from_lat,
                to_lon=to_lon,
                to_lat=to_lat,
                blockage_geojson=blockage_geojson,
                gtfs_distance_m=cand.gtfs_distance_m,
                gtfs_time_s=cand.gtfs_time_s,
            )
            if not feas.success or not feas.road_geometry_geojson:
                continue
            if not geometry_inflation_within_thresholds(
                baseline_distance_m=cand.gtfs_distance_m,
                candidate_distance_m=feas.road_distance_m,
                max_ratio=3.0,
            ):
                continue
            ratio_penalty = 0.0
            if feas.distance_ratio is not None and feas.distance_ratio > 1.0:
                ratio_penalty += (feas.distance_ratio - 1.0) * 700.0
            if feas.time_ratio is not None and feas.time_ratio > 1.0:
                ratio_penalty += (feas.time_ratio - 1.0) * 450.0
            transfer_penalty = 120.0 if cand.used_transfers else 0.0
            hybrid_score = cand.gtfs_cost + ratio_penalty + transfer_penalty
            confidence = 0.95
            if feas.distance_ratio is not None:
                confidence -= min(max(feas.distance_ratio - 1.0, 0.0) * 0.15, 0.45)
            confidence = max(confidence, 0.35)
            road_res = _from_gtfs_candidate(
                cand,
                reason_code="hybrid_road_validated",
                strategy_used="gtfs_road_hybrid",
                confidence=confidence,
                diagnostics={
                    "hybrid_score": hybrid_score,
                    "gtfs_cost": cand.gtfs_cost,
                    "road_distance_m": feas.road_distance_m,
                    "road_time_s": feas.road_time_s,
                    "distance_ratio": feas.distance_ratio,
                    "time_ratio": feas.time_ratio,
                },
            )
            road_res.detour_geojson = feas.road_geometry_geojson
            if feas.turn_by_turn:
                parsed_tbt: List[DetourTurnStep] = []
                for step in feas.turn_by_turn:
                    try:
                        parsed_tbt.append(DetourTurnStep.model_validate(step))
                    except Exception:
                        continue
                road_res.turn_by_turn = parsed_tbt or None
            if best is None or hybrid_score < best[0]:
                best = (hybrid_score, road_res)
        return best[1] if best is not None else None

    gtfs_candidates = _extract_gtfs_candidates(max_candidates=3)
    hybrid_res = _try_hybrid_from_gtfs_candidates(gtfs_candidates)
    if hybrid_res is not None:
        _persist_result(hybrid_res)
        return hybrid_res
    if gtfs_candidates:
        gtfs_best = min(gtfs_candidates, key=lambda c: c.gtfs_cost)
        reason = "gtfs_only_fallback" if use_osm_detour else "gtfs_path_found"
        gtfs_res = _from_gtfs_candidate(
            gtfs_best,
            reason_code=reason,
            strategy_used="gtfs_multiroute",
            confidence=0.72 if use_osm_detour else 0.8,
            diagnostics={
                "candidate_count": len(gtfs_candidates),
                "gtfs_cost": gtfs_best.gtfs_cost,
                "gtfs_distance_m": gtfs_best.gtfs_distance_m,
            },
        )
        _persist_result(gtfs_res)
        return gtfs_res
    if stored_road_result is not None:
        _persist_result(stored_road_result)
        return stored_road_result
    if stored_instructions_result is not None:
        _persist_result(stored_instructions_result)
        return stored_instructions_result

    if (
        apply_request_street_override
        and merged_steps
        and _override_endpoints_match()
    ):
        narrative_road = try_build_narrative_detour_linestring(merged_steps, blockage_geojson)
        if narrative_road:
            res = _detour_by_area_from_street_road(
                route_id=route_id,
                direction_id=chosen.direction_id,
                pattern_id=chosen.pattern_id,
                blocked_edges_count=len(blocked_edges),
                stop_before=stop_before,
                stop_after=stop_after,
                road_geojson=narrative_road,
                turn_steps=merged_steps or None,
                replaced_segment_geojson=replaced_segment_geojson,
                from_override=False,
            )
            if street_override_remember and feed_id is not None:
                try:
                    db_access_module.save_detour_street_override_pg(
                        feed_id,
                        db_access_module.build_street_override_key(
                            "by_area",
                            route_id,
                            direction_id,
                            blockage_hash,
                            stop_before,
                            stop_after,
                            route_sig_hash,
                        ),
                        "by_area",
                        route_id,
                        direction_id,
                        blockage_hash,
                        stop_before,
                        stop_after,
                        route_sig_hash,
                        narrative_road,
                        merged_steps,
                    )
                except Exception:
                    pass
            if route_sig_hash:
                try:
                    payload = res.model_dump() if hasattr(res, "model_dump") else res.dict()
                    db_access_module.save_detour_by_area_pg(
                        feed_id=feed_id,
                        mode=cache_mode,
                        route_id=route_id,
                        direction_id=direction_id,
                        date_ymd=date_str,
                        start_sec=start_sec,
                        end_sec=end_sec,
                        transfer_radius_m=transfer_radius_m,
                        use_osm_detour=use_osm_detour,
                        policy_profile=cache_policy_profile,
                        blockage_hash=blockage_hash,
                        route_sig_hash=route_sig_hash,
                        result_json=payload,
                    )
                except Exception:
                    pass
            return res
    if apply_request_street_override and merged_steps and _override_endpoints_match():
        empty_road = {"type": "LineString", "coordinates": []}
        res = _detour_by_area_instructions_only(
            route_id=route_id,
            direction_id=chosen.direction_id,
            pattern_id=chosen.pattern_id,
            blocked_edges_count=len(blocked_edges),
            stop_before=stop_before,
            stop_after=stop_after,
            turn_steps=merged_steps,
            replaced_segment_geojson=replaced_segment_geojson,
            from_override=False,
        )
        if street_override_remember and feed_id is not None:
            try:
                db_access_module.save_detour_street_override_pg(
                    feed_id,
                    db_access_module.build_street_override_key(
                        "by_area",
                        route_id,
                        direction_id,
                        blockage_hash,
                        stop_before,
                        stop_after,
                        route_sig_hash,
                    ),
                    "by_area",
                    route_id,
                    direction_id,
                    blockage_hash,
                    stop_before,
                    stop_after,
                    route_sig_hash,
                    empty_road,
                    merged_steps,
                )
            except Exception:
                pass
        if route_sig_hash:
            try:
                payload = res.model_dump() if hasattr(res, "model_dump") else res.dict()
                db_access_module.save_detour_by_area_pg(
                    feed_id=feed_id,
                    mode=cache_mode,
                    route_id=route_id,
                    direction_id=direction_id,
                    date_ymd=date_str,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    transfer_radius_m=transfer_radius_m,
                    use_osm_detour=use_osm_detour,
                    policy_profile=cache_policy_profile,
                    blockage_hash=blockage_hash,
                    route_sig_hash=route_sig_hash,
                    result_json=payload,
                )
            except Exception:
                pass
        return res
    res = DetourByAreaRouteResult(
        route_id=route_id,
        direction_id=chosen.direction_id,
        pattern_id=chosen.pattern_id,
        blocked_edges_count=len(blocked_edges),
        stop_before=stop_before,
        stop_after=stop_after,
        error="No blockage-safe detour path found between entry/exit stops",
    )
    return _with_meta(
        res,
        reason_code="no_detour_path",
        strategy_used="strategy_pipeline",
        confidence=0.0,
        diagnostics={"prefer_osm_detour": prefer_osm_detour, "use_osm_detour": use_osm_detour},
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


@app.post("/api/v1/area/routes", response_model=AreaRoutesResponse)
def area_routes(req: AreaRoutesQuery):
    """
    Given a polygon and datetime range, return the list
    of routes whose shapes intersect that polygon and have at least one trip
    running during that window. Accepts Polygon, LineString, or Point (buffered).
    """
    if not req.polygon_geojson:
        raise HTTPException(status_code=400, detail="polygon_geojson is required")

    request_t0 = time.perf_counter()
    start_date_ymd, start_sec, end_date_ymd, end_sec = _parse_window_range(
        req.start_date, req.start_time, req.end_date, req.end_time
    )
    single_day = start_date_ymd == end_date_ymd
    try:
        feed_id = db_access_module.get_active_feed_id()
    except Exception as e:
        feed_id = "error"
        log("area/routes", f"phase=feed_lookup_error error_type={type(e).__name__} error={e!s}")

    log(
        "area/routes",
        f"phase=request start_date={start_date_ymd} end_date={end_date_ymd} "
        f"start_time={req.start_time} end_time={req.end_time} max_results={req.max_results} "
        f"single_day={str(single_day).lower()} feed_id={feed_id} "
        f"time_semantics_mode={req.time_semantics_mode}",
    )

    try:
        polygon_geojson = _normalize_area_geometry(req.polygon_geojson)
    except HTTPException as e:
        log("area/routes", f"phase=bad_geometry status_code={e.status_code}")
        raise
    except Exception as e:
        log("area/routes", f"phase=bad_geometry error_type={type(e).__name__} error={e!s}")
        raise HTTPException(status_code=400, detail=f"Invalid polygon_geojson: {e}")

    geom_type = str(polygon_geojson.get("type") or "unknown")
    bbox_txt = "n/a"
    try:
        from shapely.geometry import shape as shapely_shape

        g = shapely_shape(polygon_geojson)
        minx, miny, maxx, maxy = g.bounds
        bbox_txt = f"{minx:.5f},{miny:.5f},{maxx:.5f},{maxy:.5f}"
    except Exception as e:
        log("area/routes", f"phase=geometry_bounds_error error_type={type(e).__name__} error={e!s}")
    log("area/routes", f"phase=geometry geom_type={geom_type} bbox={bbox_txt}")

    query_t0 = time.perf_counter()
    try:
        routes_raw = find_routes_in_polygon(
            feed=None,
            polygon_geojson=polygon_geojson,
            start_date_ymd=start_date_ymd,
            start_sec=start_sec,
            end_date_ymd=end_date_ymd,
            end_sec=end_sec,
            time_semantics_mode=req.time_semantics_mode,
        )
        query_elapsed_ms = int((time.perf_counter() - query_t0) * 1000)
        log("area/routes", f"phase=query_ok elapsed_ms={query_elapsed_ms} route_rows={len(routes_raw)}")
    except HTTPException as e:
        query_elapsed_ms = int((time.perf_counter() - query_t0) * 1000)
        log("area/routes", f"phase=query_http_error status_code={e.status_code} elapsed_ms={query_elapsed_ms}")
        raise
    except Exception as e:
        query_elapsed_ms = int((time.perf_counter() - query_t0) * 1000)
        if type(e).__name__ == "QueryCanceled":
            log(
                "area/routes",
                f"phase=query_timeout elapsed_ms={query_elapsed_ms} "
                f"time_semantics_mode={req.time_semantics_mode}",
            )
            raise HTTPException(
                status_code=503,
                detail=(
                    "Area pass-through query timed out for selected mode "
                    f"'{req.time_semantics_mode}'. Try a smaller area/window or increase "
                    "AREA_ROUTES_STATEMENT_TIMEOUT_MS."
                ),
            )
        log(
            "area/routes",
            f"phase=query_error elapsed_ms={query_elapsed_ms} error_type={type(e).__name__} error={e!s}",
        )
        # Log full traceback to help diagnose encoding / PostGIS issues.
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
                time_match_confidence=r.get("time_match_confidence"),
                time_match_note=r.get("time_match_note"),
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

    total_elapsed_ms = int((time.perf_counter() - request_t0) * 1000)
    log(
        "area/routes",
        f"phase=done window={start_date_ymd} {req.start_time}-{end_date_ymd} {req.end_time} "
        f"routes={len(results)} elapsed_ms={total_elapsed_ms} "
        f"calendar_hint={'set' if calendar_hint else 'none'}",
    )
    return AreaRoutesResponse(routes=results, calendar_hint=calendar_hint)


@app.post("/api/v1/stop/routes", response_model=StopRoutesResponse)
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


@app.post("/api/v1/incidents", response_model=IncidentCreateResponse)
def post_incident(req: IncidentCreateRequest):
    """Create an incident from a polygon; preview affected routes and derived OSM edge bans."""
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled (DETOUR_V2_ENABLED=0).")
    try:
        polygon_geojson = _normalize_area_geometry(req.polygon_geojson)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid polygon_geojson: {e}")
    start_date_ymd, start_sec, end_date_ymd, end_sec = _parse_window_range(
        req.start_date, req.start_time, req.end_date, req.end_time
    )
    try:
        feed = load_active_feed()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"GTFS feed could not be loaded: {e}")
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
    feed_id = db_access_module.get_active_feed_id()
    proj = project_incident_polygon(
        blockage_geojson=polygon_geojson,
        feed_id=feed_id,
        db_osm_available=True,
    )
    try:
        incident_id = db_access_module.insert_incident(
            polygon_geojson,
            req.incident_type,
            req.description,
            None,
            None,
            req.created_by,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save incident: {e}")
    pol = get_default_policy()
    log(
        "detours/v2/incident",
        f"incident_id={incident_id} affected_routes={len(routes_raw)} edge_bans={len(proj.edge_bans)} policy={pol.version}",
    )
    return IncidentCreateResponse(
        incident_id=incident_id,
        affected_route_count=len(routes_raw),
        derived_edge_ban_count=len(proj.edge_bans),
        policy_version=pol.version,
    )


@app.post("/api/v1/detours/compute", response_model=DetourComputeV2Response)
def post_detours_compute_v2(req: DetourComputeV2Request):
    """Compute road-graph detours for one or more trips (detour v2 engine)."""
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled (DETOUR_V2_ENABLED=0).")
    if not req.blockage_geojson:
        raise HTTPException(status_code=400, detail="blockage_geojson is required")
    stripped_trip_ids = [t.strip() for t in req.trip_ids if t and str(t).strip()]
    trip_ids_to_compute: list[str]
    source: str
    if stripped_trip_ids:
        trip_ids_to_compute = stripped_trip_ids
        source = "trip_ids"
    else:
        route = (req.route_id or "").strip()
        try:
            feed = load_active_feed()
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"GTFS feed could not be loaded. ({e})",
            )
        tid = resolve_representative_trip_id(feed, route, req.direction_id, req.service_date)
        if not tid:
            raise HTTPException(
                status_code=400,
                detail="No patterns found for route/date/direction",
            )
        trip_ids_to_compute = [tid]
        source = "route_id"
    pol = get_default_policy()
    log(
        "detours/v2/compute",
        " ".join(
            [
                f"service_date={req.service_date}",
                f"incident_id={req.incident_id}",
                f"persist={req.persist}",
                f"trip_count={len(trip_ids_to_compute)}",
                f"source={source}",
                f"route_id={req.route_id or ''}",
                f"direction_id={req.direction_id if req.direction_id is not None else ''}",
            ]
        ),
    )
    results: list[dict] = []
    detour_request_ids: list[int] = []
    feed_id = db_access_module.get_active_feed_id()
    # B3: shared Valhalla response cache for trips on the same shape within this request.
    valhalla_cache: Dict[str, Any] = {}
    use_geojson = bool(getattr(req, "detour_debug", False)) or bool(getattr(req, "debug_detour", False))
    use_ai_log = (
        bool(getattr(req, "detour_debug", False))
        or bool(getattr(req, "log_ai_summary", False))
        or DETOUR_V2_DEBUG
        or DETOUR_V2_AI_LOG
    )
    for trip_id in trip_ids_to_compute:
        t0 = time.time()
        rid: Optional[int] = None
        out = None
        payload: Dict[str, Any]
        try:
            out = compute_detour_for_trip(
                trip_id=trip_id,
                blockage_geojson=req.blockage_geojson,
                service_date=req.service_date,
                incident_id=req.incident_id,
                _valhalla_cache=valhalla_cache,
                debug_detour=use_geojson,
                use_matched_physical=bool(getattr(req, "use_matched_physical", False)),
            )
            if use_ai_log:
                try:
                    log("detours/v2/compute_ai", format_detour_ai_log_line(out))
                except Exception as ai_log_exc:
                    log(
                        "detours/v2/compute_ai",
                        f"trip_id={trip_id} error=ai_log_failed err={ai_log_exc!s}",
                    )
            payload = detour_compute_output_to_dict(out)
        except Exception as e:
            payload = {
                "status": "error",
                "trip_id": trip_id,
                "route_id": "",
                "policy_version": pol.version,
                "error": "compute_exception",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "candidates": [],
            }
            log(
                "detours/v2/compute",
                " ".join(
                    [
                        f"trip_id={trip_id}",
                        f"service_date={req.service_date}",
                        f"incident_id={req.incident_id}",
                        f"source={source}",
                        f"route_id={req.route_id or ''}",
                        f"direction_id={req.direction_id if req.direction_id is not None else ''}",
                        f"error_type={type(e).__name__}",
                        f"error={e!s}",
                    ]
                ),
            )
            log("detours/v2/compute", f"trip_id={trip_id} traceback={traceback.format_exc().strip()}")
        elapsed_ms = int(round((time.time() - t0) * 1000.0))
        results.append(payload)
        selected_strategy = ""
        selected = payload.get("selected")
        if isinstance(selected, dict):
            selected_strategy = str(selected.get("strategy") or "")
        if req.persist:
            try:
                route_id_for_persist = ""
                status_for_persist = str(payload.get("status") or "error")
                if out is not None:
                    route_id_for_persist = out.route_id
                else:
                    route_id_for_persist = str(payload.get("route_id") or req.route_id or "")
                rid = detour_memory_v2.save_detour_request(
                    feed_id=feed_id,
                    trip_id=trip_id,
                    route_id=route_id_for_persist,
                    service_date=req.service_date,
                    incident_id=req.incident_id,
                    status=status_for_persist,
                    payload_json=payload,
                )
                if out is not None:
                    detour_memory_v2.save_candidates(rid, out.candidates, discarded=out.discarded)
                detour_request_ids.append(rid)
                log("detours/v2/compute", f"trip_id={trip_id} persist_ok=true request_id={rid}")
            except Exception as e:
                log(
                    "detours/v2/compute",
                    f"persist_error trip_id={trip_id} error_type={type(e).__name__} err={e!s}",
                )
                log("detours/v2/compute", f"persist_error trip_id={trip_id} traceback={traceback.format_exc().strip()}")
        log(
            "detours/v2/compute",
            " ".join(
                [
                    f"trip_id={trip_id}",
                    f"route_id={payload.get('route_id') or ''}",
                    f"status={payload.get('status') or ''}",
                    f"error={payload.get('error') or ''}",
                    f"candidates={len(payload.get('candidates') or [])}",
                    f"selected_strategy={selected_strategy}",
                    f"request_id={rid if rid is not None else ''}",
                    f"elapsed_ms={elapsed_ms}",
                ]
            ),
        )
    return DetourComputeV2Response(
        results=results,
        detour_request_ids=detour_request_ids,
        policy_version=pol.version,
    )


@app.get("/api/v1/detours/policy")
def get_detour_policy():
    """Return the live DetourPolicyConfig as JSON (F1 - debug endpoint)."""
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled.")
    return get_default_policy().to_dict()


@app.post("/api/v1/constraints/edge", response_model=ConstraintCreateResponse)
def post_bus_edge_constraint(req: BusEdgeConstraintRequest):
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled.")
    try:
        cid = db_access_module.insert_bus_edge_constraint(
            osm_way_id=req.osm_way_id,
            direction=req.direction,
            constraint_type=req.constraint_type,
            severity=req.severity,
            reason_code=req.reason_code,
            notes=req.notes,
            created_by=req.created_by,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return ConstraintCreateResponse(id=cid)


@app.post("/api/v1/constraints/turn", response_model=ConstraintCreateResponse)
def post_bus_turn_constraint(req: BusTurnConstraintRequest):
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled.")
    try:
        cid = db_access_module.insert_bus_turn_constraint(
            from_way_id=req.from_way_id,
            via_node_id=req.via_node_id,
            to_way_id=req.to_way_id,
            constraint_type=req.constraint_type,
            severity=req.severity,
            reason_code=req.reason_code,
            notes=req.notes,
            created_by=req.created_by,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return ConstraintCreateResponse(id=cid)


@app.post("/api/v1/detours/{detour_request_id}/approve", response_model=DetourApproveV2Response)
def post_detour_approve_v2(detour_request_id: int, req: DetourApproveV2Request):
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled.")
    full = db_access_module.get_detour_request_full(detour_request_id)
    if not full:
        log("detours/v2/approve", f"detour_request_id={detour_request_id} not_found=true")
        raise HTTPException(status_code=404, detail="detour request not found")
    req_row = full["request"]
    cands = full.get("candidates") or []
    chosen = None
    want_rank = getattr(req, "candidate_rank", None)
    if want_rank is not None:
        try:
            wr = int(want_rank)
        except Exception:
            wr = None
        if wr is not None:
            for c in cands:
                try:
                    if int(c.get("candidate_rank") or -1) == wr:
                        chosen = c
                        break
                except Exception:
                    continue
    if chosen is None:
        for c in cands:
            if c.get("accepted"):
                chosen = c
                break
    if chosen is None and cands:
        chosen = min(cands, key=lambda x: float(x.get("score") or 1e30))
    if chosen is None:
        log("detours/v2/approve", f"detour_request_id={detour_request_id} no_candidates=true")
        raise HTTPException(status_code=400, detail="no candidates to approve")
    import json as _json

    road_seq = chosen.get("road_sequence_json")
    if isinstance(road_seq, str):
        try:
            road_seq = _json.loads(road_seq)
        except Exception:
            road_seq = []
    elif road_seq is None:
        road_seq = []
    geom = chosen.get("geometry_json")
    if isinstance(geom, str):
        try:
            geom = _json.loads(geom)
        except Exception:
            geom = {}
    feed_id = int(req_row["feed_id"])
    route_id = str(req_row["route_id"])
    trip_id = str(req_row["trip_id"])
    sig = f"{trip_id}:{req_row.get('service_date')}"
    inc = req_row.get("incident_id")
    inc_sig = str(inc) if inc is not None else "none"
    try:
        aid = db_access_module.insert_approved_detour(
            feed_id=feed_id,
            route_id=route_id,
            trip_pattern_key=sig,
            incident_signature=inc_sig,
            geometry_json=geom if isinstance(geom, dict) else {},
            road_sequence_json=road_seq,
            turn_sequence_json=chosen.get("turn_sequence_json") or [],
            approved_by=req.approved_by,
        )
    except Exception as e:
        log(
            "detours/v2/approve",
            f"detour_request_id={detour_request_id} write_failed=true error_type={type(e).__name__} err={e!s}",
        )
        log("detours/v2/approve", f"detour_request_id={detour_request_id} traceback={traceback.format_exc().strip()}")
        raise HTTPException(status_code=500, detail=str(e))
    for seg in road_seq:
        if not isinstance(seg, dict):
            continue
        wid = seg.get("osm_way_id")
        if wid:
            try:
                detour_memory_v2.bump_edge_evidence(int(wid), seg.get("direction"))
            except Exception:
                pass
    turn_seq = chosen.get("turn_sequence_json") or []
    if isinstance(turn_seq, str):
        try:
            turn_seq = _json.loads(turn_seq)
        except Exception:
            turn_seq = []
    seg_way_by_id: Dict[int, int] = {}
    for seg in road_seq:
        if not isinstance(seg, dict):
            continue
        try:
            sid = int(seg.get("segment_id") or 0)
            wid = int(seg.get("osm_way_id") or 0)
        except Exception:
            continue
        if sid and wid:
            seg_way_by_id[sid] = wid
    for tr in turn_seq:
        if not isinstance(tr, dict):
            continue
        try:
            vn = int(tr.get("via_node_id") or 0)
            fs = int(tr.get("from_segment_id") or 0)
            ts = int(tr.get("to_segment_id") or 0)
        except Exception:
            continue
        fw_w = int(tr.get("from_way_id") or 0) or seg_way_by_id.get(fs, 0)
        tw_w = int(tr.get("to_way_id") or 0) or seg_way_by_id.get(ts, 0)
        if fw_w and vn and tw_w:
            try:
                db_access_module.bump_bus_turn_evidence(fw_w, vn, tw_w)
            except Exception:
                pass
    log(
        "detours/v2/approve",
        f"detour_request_id={detour_request_id} approved_detour_id={aid} route_id={route_id} trip_id={trip_id}",
    )
    return DetourApproveV2Response(approved_detour_id=aid)


@app.get("/api/v1/detours/{detour_request_id}", response_model=DetourV2DetailResponse)
def get_detour_v2_detail(detour_request_id: int):
    if not DETOUR_V2_ENABLED:
        raise HTTPException(status_code=503, detail="Detour v2 API is disabled.")
    full = db_access_module.get_detour_request_full(detour_request_id)
    if not full:
        log("detours/v2/detail", f"detour_request_id={detour_request_id} not_found=true")
        raise HTTPException(status_code=404, detail="detour request not found")
    return DetourV2DetailResponse(request=dict(full["request"]), candidates=full.get("candidates") or [])

