Software Requirements Specification (SRS)

Israel GTFS Detour Router — Web Prototype

Version: 1.0

Date: 2026-03-05

Document Owner: Project Team


This document specifies functional and non-functional requirements for a web-based system that ingests Israel Ministry of Transport (MoT) GTFS feeds and produces bus-corridor-constrained detours around user-drawn blockages on a map.


# 1. Introduction

## 1.1 Purpose

Define requirements for a prototype that can: (1) discover affected routes in a drawn area and time window, and (2) propose detours that stay on existing GTFS bus corridors, optionally using OSM-based geometry for display.

## 1.2 Scope

- Backend: FastAPI service with SQLite-backed GTFS storage and precomputed/cached graph artifacts.
- Frontend: React + Leaflet app that visualizes routes, allows drawing blockages, and requests detours.
- Data: Israel MoT GTFS schedule feed (routes, trips, stops, stop_times, shapes, calendar, calendar_dates).
- Optional: Road-network map-matching for prettier geometry (OSRM/Valhalla), without changing allowed bus corridors.
## 1.3 Definitions and acronyms

- GTFS: General Transit Feed Specification.
- MoT: Ministry of Transport (Israel).
- AOI: Area of Interest (blockage polygon bbox + buffer).
- Pattern: A route/direction stop-sequence variant derived from trips/stop_times.
- Corridor: A sequence of stop-to-stop edges that exist in GTFS service.
- Transfer edge: A synthetic connector between nearby stops (e.g., ≤120 m) to enable corridor switching.
## 1.4 References

- GTFS Schedule Reference (routes.txt, trips.txt, stops.txt, stop_times.txt, shapes.txt, calendar.txt, calendar_dates.txt).
- OpenStreetMap (basemap tiles) and optional OSRM/Valhalla services for map-matching/road detour geometry.
# 2. Overall Description

## 2.1 Product perspective

The system is a local/desktop-deployed web app composed of a Python backend and a React frontend. It operates on GTFS feeds stored in SQLite and produces corridor-constrained detours by routing on a directed graph built from GTFS stop sequences.

## 2.2 Product functions (high-level)

- Download and activate latest GTFS feed (with validation and versioning).
- Search routes by short name / id and display route geometry on a map.
- Draw a blockage (polygon/line/point) on the map and find routes active in a time window that intersect the blockage.
- Compute detours for: (a) a selected route, or (b) all affected routes in the selected time window.
- Ensure detours stay on existing GTFS corridors; allow switching corridors via nearby-stop transfer edges.
- Cache/precompute graphs for performance; reuse cached artifacts across requests.
## 2.3 User classes

- Transit operations staff: need quick corridor-safe detours for blockages.
- Analysts/devs: validate GTFS data and experiment with routing/corridor constraints.
## 2.4 Operating environment

- Backend: Python 3.11+, FastAPI, SQLite, Shapely, NetworkX (prototype).
- Frontend: React + Vite + Leaflet + Leaflet.draw.
- Local optional services: OSRM/Valhalla container for road geometry assistance.
## 2.5 Constraints

- Detours must remain on GTFS-derived corridors (no free-form street routing for the bus path).
- GTFS data quality may vary (missing shapes, missing shape_dist_traveled, inconsistent times).
- Performance must be acceptable for interactive use on a consumer PC.
## 2.6 Assumptions

- Israel MoT GTFS feed is available via HTTPS and can be stored locally.
- Stops are sufficiently dense to form corridor graphs in urban areas; transfers may be required to bypass blockages.
- Time parsing supports extended GTFS times (e.g., 24:xx–27:xx) as needed.
# 3. System Features and Functional Requirements

Functional requirements are labeled FR-#. The implementation may include additional helper endpoints/tools for debugging, but FRs define the expected behavior.

## 3.1 GTFS feed management

- FR-1: The system shall download the latest GTFS zip from the configured MoT base URL when requested.
- FR-2: The system shall validate core GTFS tables on import and activate a feed version only after successful validation.
- FR-3: The system shall maintain an active feed_version identifier and allow querying current status.
## 3.2 Route discovery and display

- FR-4: The system shall support searching routes by route_id and/or route_short_name.
- FR-5: The system shall display the selected route corridor on a map using GTFS shapes when available.
- FR-6: The system shall list ordered stops for the selected route pattern used for display.
## 3.3 Area-based route search (impact discovery)

- FR-7: Given a drawn geometry (polygon/line/point), date, and time window, the system shall return routes whose service is active in that window and whose corridor geometry intersects the drawn area.
- FR-8: For line/point input, the system shall convert to a buffered polygon for area search.
## 3.4 Detour computation by area

- FR-9: The system shall compute a detour for a selected route based only on the drawn blockage geometry (no manual start/end stop selection).
- FR-10: The system shall compute detours for all affected routes in the time window (capped by max_routes).
- FR-11: Detours shall avoid corridors (edges) that intersect the buffered blockage geometry.
- FR-12: Detours shall remain on existing GTFS stop-to-stop corridor edges, with optional corridor switching through transfer edges between nearby stops (within transfer_radius_m).
- FR-13: For each detoured route, the system shall identify the blocked span on the route’s baseline corridor and compute an alternate path between stop_before and stop_after.
- FR-14: The response shall include detour geometry (GeoJSON) and metadata: route_id, direction_id, blocked_edges_count, stop_before/stop_after, used_transfers, and errors when detour is impossible.
## 3.5 Caching and performance features

- FR-15: The system shall cache/reuse computed graphs/patterns for repeated requests (in-memory and/or SQLite).
- FR-16: The system shall support precomputing graphs for a date/feed_version to minimize first-request latency.
# 4. External Interface Requirements

## 4.1 User interface (Frontend)

- Map view with OpenStreetMap tiles.
- Drawing tools: polygon/rectangle/polyline/marker to define blockage.
- Controls: route search/select, area search, detour mode (route-only vs all), date/time window, transfer radius (optional).
- Results: highlighted detour polyline(s), affected routes list, summary metrics.
## 4.2 API (Backend)

Endpoints may evolve; the SRS defines the expected semantics.

| Method | Path | Purpose | Notes |
| --- | --- | --- | --- |
| GET | /health | Service health check |  |
| POST | /routes/search | Search routes by query | Returns route_id/short/long name, operator info when available. |
| POST | /feed/update | Download & activate latest feed | Validates tables and updates feed_version. |
| GET | /feed/status | Report active feed version/status |  |
| POST | /graph/build | Build or load route corridor graph | Used for display/debug; should not reset the map view unexpectedly. |
| GET | /graph/stops | Ordered stops for current route pattern |  |
| GET | /graph/geojson | Route corridor GeoJSON for display | Optional helper; can include snapped geometry if enabled. |
| POST | /area/routes | Find routes intersecting drawn area | Filtered by date/time; line/point buffered into polygon. |
| POST | /detours/by-area | Polygon-only detours (route/all) | Primary detour API: route-only or all affected routes. Returns feed_version. |

## 4.3 External services

- MoT GTFS hosting (HTTPS) for feed downloads.
- OpenStreetMap tile servers for basemap rendering (frontend).
- Optional OSRM/Valhalla for map-matching/road detour geometry (display assist only).
# 5. Non-Functional Requirements

## 5.1 Performance

- NFR-1: Area search should return results within 5–15 seconds for typical AOIs and time windows on a consumer PC.
- NFR-2: Route-only detour should return within 2–10 seconds for typical urban routes; complex AOIs may take longer.
- NFR-3: The system shall use spatial indexing (e.g., STRtree/R-tree) to avoid O(E) intersection tests on large graphs.
- NFR-4: The system shall avoid unnecessary full-feed scans in hot paths; prefer indexed SQLite queries and caches.
## 5.2 Reliability and recovery

- NFR-5: If feed download fails, the system shall continue using the last active feed.
- NFR-6: If optional OSM services are unavailable, the system shall degrade gracefully and continue with GTFS-only geometry.
## 5.3 Security and privacy

- NFR-7: The system shall not expose sensitive user data; requests/responses contain only transit geometry and identifiers.
- NFR-8: CORS and API exposure should be configurable for deployment context.
## 5.4 Maintainability

- NFR-9: Code should be modular (feed, patterns, graph, routing, area search, UI).
- NFR-10: Logging shall allow tracing detour decisions: blocked span, entry/exit stops, transfer usage, and failure causes.
# 6. Data Requirements

## 6.1 Source GTFS tables

- agency.txt, routes.txt, trips.txt, stops.txt, stop_times.txt, shapes.txt, calendar.txt, calendar_dates.txt.
## 6.2 Derived/auxiliary data

- trip_time_bounds: (trip_id -> [min_dep_sec, max_dep_sec]) for fast time-window filtering.
- shape bounding boxes (shape_id -> min/max lat/lon) for quick AOI filtering.
- patterns: pattern_id, route_id, direction_id, stop_ids, frequency, representative_trip_id/shape_id.
- graph caches: per feed_version/date/route/direction artifacts (v2 recommended as compact arrays).
## 6.3 Geometry formats

- All geometry returned to frontend as GeoJSON (FeatureCollection).
- Blocking uses Shapely geometries internally; optional buffering applied for user expectation alignment.
# 7. Detour Rules and Routing Policy

## 7.1 Baseline corridor and blocked span detection

- Compute baseline corridor for each route using the selected dominant pattern (by frequency) for the date/time window.
- Determine blocked edges where corridor geometry intersects buffered blockage polygon.
- Choose stop_before/stop_after as outermost endpoints of the blocked span.
## 7.2 Multi-route detour graph construction

- Include primary route corridor plus additional corridors that intersect the AOI and are active in the time window.
- Add transfer edges between nearby stops within transfer_radius_m with a fixed + walking-time penalty.
- Do not introduce street-based driving edges; only GTFS corridor edges are used for bus movement.
## 7.3 Path selection

- Run shortest path (A* or Dijkstra) on the detour graph with blocked edges removed/penalized.
- Prefer fewer transfers by incorporating transfer penalties; optionally enforce max transfers in future iterations.
# 8. Acceptance Criteria

1. Drawing a polygon and searching /area/routes returns routes intersecting the area during the selected time window.
2. In route-only mode, selecting a route and drawing a blocking polygon computes a detour without asking for start/end stops.
3. Detour geometry does not intersect the buffered polygon (within the configured tolerance).
4. In all-routes mode, the system returns a capped list of affected routes and detours where possible.
5. If no detour exists, the response includes a clear error per route (e.g., no alternative corridor).
# 9. Future Enhancements

- Expose transfer_radius_m and transfer penalty in UI (city presets).
- Improve geometry precision: slice shapes between stops using shape_dist_traveled where available.
- Persist spatial indexes and compact graph structures to reduce cold-start time.
- Add user controls: 'Fit to blockage', 'Fit to route', and preserve map view during background graph builds.

---

## 10. Related project documents

- **Project SRS:** [SRS.md](SRS.md) — Primary product SRS (route search, graph, area search, detour, optional Valhalla/OSRM).
- **Traceability:** [SRS_Traceability.md](SRS_Traceability.md) — Maps this document’s FR-1–FR-16 and NFRs to the codebase.
