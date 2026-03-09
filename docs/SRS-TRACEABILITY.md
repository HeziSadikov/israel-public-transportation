# SRS Traceability: Requirements → Implementation

This document maps **docs/SRS.md** (Software Requirements Specification) to the codebase so the project can be maintained and rebuilt according to the SRS.

---

## 1. GTFS Feed Management (FR-1)

| ID | Requirement | Implementation |
|----|-------------|-----------------|
| FR-1.1 | Load active GTFS from configured path | `backend/gtfs_loader.py` – `load_active_feed()`; `backend/config.py` – paths; `app.py` – all endpoints use loaded feed |
| FR-1.2 | Update feed from remote URL, activate after successful import | `backend/gtfs_updater.py` – `update_feed()`; `app.py` – `POST /feed/update` |
| FR-1.3 | Expose feed status via API | `app.py` – `GET /feed/status`; `backend/gtfs_updater.py` – `get_feed_status()` |

---

## 2. Route Search and Selection (FR-2)

| ID | Requirement | Implementation |
|----|-------------|-----------------|
| FR-2.1 | Search routes by ID or name; list with route_id, short/long name, agency | `app.py` – `POST /routes/search`; `backend/sqlite_db.py` – `search_routes()` or in-memory index in `gtfs_loader.py` |
| FR-2.2 | Select route from search or “lines in area”; single source of truth | `frontend/src/App.tsx` – `handleSelectRoute()`, used by main search list and area-overlay table rows |

---

## 3. Graph Build and Visualization (FR-3)

| ID | Requirement | Implementation |
|----|-------------|-----------------|
| FR-3.1 | Build directed graph for route/date, most frequent pattern | `app.py` – `POST /graph/build`; `backend/pattern_builder.py`, `backend/graph_builder.py`; cache in `app.py` / `GRAPH_CACHE` |
| FR-3.2 | Edge geometry from GTFS shapes; fallback straight-line | `backend/graph_builder.py` – shape slicing, `_slice_by_cumulative_distances`, haversine fallback |
| FR-3.3 | Optional OSRM snap for display only | `backend/osm_pretty.py` – `map_match_pattern()`; `app.py` – graph build when `pretty_osm=true` |
| FR-3.4 | Frontend: route polyline, stops, optional snapped geometry on map | `frontend/src/App.tsx` – `MapWithDrawing`, `routePolylines`, `stops` → `<Polyline>`, `<Marker>`; `routeGeojson` from `GET /graph/geojson` |

---

## 4. Blockage Definition and Area Search (FR-4)

| ID | Requirement | Implementation |
|----|-------------|-----------------|
| FR-4.1 | Draw blockage (point/line/polygon/rectangle); store as GeoJSON | `frontend/src/App.tsx` – Leaflet.draw in `MapEffects`, `onBlockageChange(geometry)`; state `blockageGeojson` |
| FR-4.2 | Clear or cancel drawn blockage | `frontend/src/App.tsx` – `mapRef.current.clearBlockage()`, `cancelDrawing()`, `undoLastPoint()`; `backend` not required |
| FR-4.3 | Area search: polygon + date + time window → routes with shapes intersecting and trips in window | `app.py` – `POST /area/routes`; `backend/area_search.py` – `find_routes_in_polygon()`; SQL + `shape_bbox`, `trip_time_bounds` |
| FR-4.4 | Area results in resizable floating table over map; rows selectable for route | `frontend/src/App.tsx` – `.area-overlay` over map, `.area-overlay-table`, row `onClick` → `handleSelectRoute()`; `frontend/src/app.css` – `.area-overlay`, `resize: both` |

---

## 5. Detour Computation (FR-5)

| ID | Requirement | Implementation |
|----|-------------|-----------------|
| FR-5.1 | Compute detour for blockage: selected route only or all affected | `frontend/src/App.tsx` – `handleDetourByArea()`, `detourMode` (“route” \| “all”); `app.py` – `POST /detours/by-area` |
| FR-5.2 | Determine edges intersecting blockage; alternative path avoiding them | `backend/router_core.py` – `compute_blocked_edges()` (STRtree, buffer); `backend/detour_graph.py` – `build_detour_graph()`; A* in `router_core.py` |
| FR-5.3 | Valhalla exclude_polygons for road-network detour when configured | `backend/osm_detour.py` – `route_avoiding_polygon()`; `app.py` – `/detour` and `_compute_route_detour_by_area` use Valhalla when `VALHALLA_URL` set |
| FR-5.4 | Fallback to GTFS multi-route detour when Valhalla unavailable | `app.py` – `build_detour_graph()` + A* when Valhalla not set or request fails; `backend/detour_graph.py` |
| FR-5.5 | Frontend: display detour path (and optionally replaced segment), summary | `frontend/src/App.tsx` – `detour.path_geojson` in `routePolylines`; detour result section (blocked edges, stops in path, affected routes) |

---

## 6. API and Integration (FR-6)

| ID | Requirement | Implementation |
|----|-------------|-----------------|
| FR-6.1 | REST APIs: health, route search, feed update/status, graph build/stops/geojson, detour, area routes, detours by area | `app.py` – all listed endpoints |
| FR-6.2 | Frontend calls APIs over HTTP/JSON; errors → user-visible messages | `frontend/src/App.tsx` – axios calls, `setMessage()` on catch, timeout 30s/120s |
| FR-6.3 | CORS enabled for frontend origin | `app.py` – `CORSMiddleware(allow_origins=["*"], ...)` |

---

## 7. Non-Functional Requirements

| NFR | Requirement | Implementation |
|-----|-------------|-----------------|
| 5.1 Performance | Cache/DB for graph and area search; timeouts and loading state | `GRAPH_CACHE`, SQLite `trip_time_bounds`/`shape_bbox`; area_search SQL path; frontend `loading`/`areaLoading`; axios timeout |
| 5.2 Security | No auth; local/trusted use | No auth in app; config paths server-side |
| 5.3 Usability | Map does not jump; polygon persists; Compute detour enabled when valid | Stable `center`; no `fitBounds` on stops when blockage present; `blockageGeojson` rendered via `<GeoJSON>`; button `disabled={loading \|\| !blockageGeojson}` and route check only in “route” mode |
| 5.4 Maintainability | Backend modules; frontend app + map component; shared route selection | Backend: `backend/*.py`; Frontend: `App.tsx` + `MapWithDrawing`, `handleSelectRoute` used in both search and area table |

---

## 8. Second SRS (Word)

The file **docs/Israel_GTFS_Detour_Router_SRS.docx** could not be read automatically (binary format). To align the project with both SRS documents:

1. Export the Word SRS to **docs/Israel_GTFS_Detour_Router_SRS.md** (or paste its content into a .md file), or  
2. Paste the key requirements or differences here.

Then this traceability and the codebase can be updated to satisfy the second SRS as well.
