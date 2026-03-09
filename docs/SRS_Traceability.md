# SRS Traceability — Israel GTFS Detour Router

This document maps the **Israel GTFS Detour Router SRS** (`docs/Israel_GTFS_Detour_Router_SRS.md`) functional and non-functional requirements to the current implementation.

---

## 1. Functional requirements (Israel SRS)

| Req | Description | Implementation |
|-----|-------------|----------------|
| **FR-1** | Download latest GTFS zip from configured MoT URL when requested | `backend/gtfs_updater.py`: download + `POST /feed/update`; `app.py` feed update handler |
| **FR-2** | Validate core GTFS tables on import; activate only after successful validation | `backend/gtfs_importer.py` (or equivalent) validation; activation in `gtfs_updater` |
| **FR-3** | Maintain active feed_version and allow querying current status | `GET /feed/status`; `backend/config.py` `FEED_METADATA_PATH`; feed version in responses |
| **FR-4** | Search routes by route_id and/or route_short_name | `POST /routes/search`; `backend/gtfs_loader.py` routes index |
| **FR-5** | Display selected route corridor on map using GTFS shapes when available | `GET /graph/geojson`; `app.py` graph build + edge geometry from shapes/straight-line |
| **FR-6** | List ordered stops for selected route pattern | `GET /graph/stops`; `backend/sqlite_db.py` `get_pattern_stops` |
| **FR-7** | Given drawn geometry, date, time window → routes active in window whose corridor intersects area | `POST /area/routes`; `backend/area_search.py` (SQL path + shape_bbox/trip_time_bounds; fallback in-memory) |
| **FR-8** | Line/point input converted to buffered polygon for area search | `backend/area_search.py`: buffer applied to line/point before intersection |
| **FR-9** | Compute detour for selected route from blockage only (no manual start/end stop) | `POST /detours/by-area` mode `route`; `app.py` detour-by-area logic determines stop_before/stop_after from blockage |
| **FR-10** | Compute detours for all affected routes in time window (capped by max_routes) | `POST /detours/by-area` mode `all`; `DetourByAreaRequest.max_routes` |
| **FR-11** | Detours avoid corridor edges that intersect buffered blockage | `backend/router_core.py` / `detour_graph.py`: blocked edges removed/penalized in graph |
| **FR-12** | Detours on GTFS stop-to-stop edges; corridor switching via transfer edges (transfer_radius_m) | `backend/detour_graph.py` `_add_transfer_edges`; `DetourByAreaRequest.transfer_radius_m` |
| **FR-13** | Per route: identify blocked span, compute alternate path between stop_before and stop_after | `app.py`: blocked span detection; `router_core` / `detour_graph` shortest path between those stops |
| **FR-14** | Response: GeoJSON + route_id, direction_id, blocked_edges_count, stop_before/stop_after, used_transfers, errors | `backend/api_models.py` `DetourByAreaRouteResult`, `DetourByAreaResponse` (includes feed_version) |
| **FR-15** | Cache/reuse computed graphs/patterns (in-memory and/or SQLite) | `app.py`: `GRAPH_CACHE`; SQLite `route_graphs_v2`; `area_search.py` trip_time_bounds + shapes caches |
| **FR-16** | Precompute graphs for date/feed_version to minimize first-request latency | `scripts/precompute_graphs.py`; same cache consumed by `POST /graph/build` |

---

## 2. Non-functional requirements (Israel SRS)

| Req | Description | Implementation |
|-----|-------------|----------------|
| **NFR-1** | Area search 5–15 s for typical AOI/time window | Area search uses SQL + shape_bbox/trip_time_bounds and optional STRtree; acceptable for typical AOI |
| **NFR-2** | Route-only detour 2–10 s for typical urban routes | Graph cache + precompute; detour on cached graph |
| **NFR-3** | Use spatial indexing (e.g. STRtree/R-tree) to avoid O(E) intersection | `backend/router_core.py`: `STRtree` for edge/blockage intersection; area_search uses shape_bbox in SQL |
| **NFR-4** | Avoid full-feed scans; prefer indexed SQLite and caches | `backend/sqlite_db.py`: indexes on feed_version, route_id, date_ymd, shape_id; trip_time_bounds, shape_bbox tables |
| **NFR-5** | If feed download fails, keep using last active feed | Feed activation only on success; status API reports last state |
| **NFR-6** | If OSM services unavailable, degrade gracefully (GTFS-only geometry) | OSRM/Valhalla optional; graph and detour work without them |
| **NFR-7** | No sensitive user data; only transit geometry and identifiers | No auth; request/response models contain only transit data |
| **NFR-8** | CORS and API exposure configurable | FastAPI CORS middleware; deployment configurable |
| **NFR-9** | Code modular (feed, patterns, graph, routing, area search, UI) | `backend`: gtfs_loader, pattern_builder, graph_builder, router_core, area_search, detour_graph, osm_detour, api_models, config |
| **NFR-10** | Logging for detour decisions (blocked span, entry/exit, transfers, failures) | **Partial**: structured logging for detour decisions not yet added; can be added in `app.py` and `router_core`/`detour_graph` |

---

## 3. API alignment (Israel SRS §4.2)

| Method | Path | Implemented |
|--------|------|-------------|
| GET | /health | ✓ |
| POST | /routes/search | ✓ |
| POST | /feed/update | ✓ |
| GET | /feed/status | ✓ |
| POST | /graph/build | ✓ |
| GET | /graph/stops | ✓ |
| GET | /graph/geojson | ✓ |
| POST | /area/routes | ✓ |
| POST | /detours/by-area | ✓ (returns feed_version) |

---

## 4. Relationship to main SRS

The project uses two requirement sources:

- **`docs/SRS.md`** — Primary SRS (route search, graph build, area search, detour, optional Valhalla/OSRM).
- **`docs/Israel_GTFS_Detour_Router_SRS.md`** — MoT-focused SRS (same product; FR-1–FR-16, NFRs, acceptance criteria, detour rules).

Implementation satisfies both: corridor-only detours, transfer edges, polygon-only detour API, caching, and precompute. Valhalla/OSRM remain optional per both documents.
