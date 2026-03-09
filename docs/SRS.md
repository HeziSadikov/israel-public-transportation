# Software Requirements Specification (SRS)
## Israel GTFS Detour Router

**Version:** 1.0  
**Date:** 2026-03-01

---

## 1. Introduction

### 1.1 Purpose

This document specifies the software requirements for the **Israel GTFS Detour Router**, a web application that builds a directed bus/transit network from Israeli GTFS data and computes detour routes when the user defines a blockage (point, line, or polygon) on a map. The system supports both GTFS-only detours and, when configured, road-network detours via Valhalla so that paths avoid the blocked area.

### 1.2 Scope

- **Product name:** Israel GTFS Detour Router (prototype).
- **In scope:**
  - Loading and versioning of Israel GTFS feeds (local zip or remote download).
  - Route search by ID or name.
  - Building a directed graph from GTFS route patterns (stops, shapes, stop_times).
  - Visualizing routes and stops on an interactive map.
  - Drawing blockage geometry (point, line, polygon, rectangle) on the map.
  - Finding lines that pass through a drawn area within a date/time window.
  - Computing detours for a selected route or for all affected routes, using the drawn blockage.
  - Optional OSRM map-matching for display geometry; optional Valhalla routing for road-network detours.
- **Out of scope (for this SRS):**
  - Mobile apps, offline PWA, or non-web clients.
  - Real-time vehicle positions or live disruption feeds.
  - User accounts, authentication, or persistence of user-defined blockages.
  - Official certification or integration with MOT systems.

### 1.3 Definitions and Acronyms

| Term | Definition |
|------|------------|
| GTFS | General Transit Feed Specification (CSV-based transit data). |
| OSRM | Open Source Routing Machine; used for map-matching geometry to roads. |
| Valhalla | Open-source routing engine; used for routing around polygons (exclude_polygons). |
| Pattern | A unique stop sequence for a route/direction on a given date; identified by `pattern_id`. |
| Blockage | User-drawn geometry (point/line/polygon) representing an area to avoid. |
| Detour | Alternative path that avoids blocked edges; may use GTFS-only or OSM road network. |

### 1.4 References

- [GTFS Static Reference](https://gtfs.org/schedule/reference/)
- Project `README.md` and `RUN-BACKEND.md`
- Backend modules: `backend/pattern_builder.py`, `backend/graph_builder.py`, `backend/router_core.py`, `backend/area_search.py`, `backend/osm_detour.py`
- **Related:** [Israel GTFS Detour Router SRS (MoT-focused)](Israel_GTFS_Detour_Router_SRS.md) — FR-1–FR-16, NFRs, acceptance criteria. [Traceability](SRS_Traceability.md) maps those requirements to this implementation.

---

## 2. Overall Description

### 2.1 Product Perspective

The system is a standalone web application:

- **Backend:** Python (FastAPI), runs on a configurable port (default 8000). Reads GTFS from local zip, unpacked files, or SQLite; optionally uses OSRM (map-matching) and Valhalla (detour routing).
- **Frontend:** React (Vite) + Leaflet; runs on a dev server (e.g. port 5173) and communicates with the backend via HTTP/JSON.
- **Data:** Israel public transportation GTFS (routes, trips, stops, stop_times, shapes, calendar, calendar_dates). Optional SQLite DB with derived tables (`trip_time_bounds`, `shape_bbox`, graph cache) for performance.

### 2.2 User Characteristics

- **Primary users:** Transport planners, analysts, or developers who need to:
  - Inspect which lines serve an area.
  - See how a route would detour around a blockage (e.g. closure, event).
- **Assumptions:** Users have a modern browser, network access to the backend, and optionally local OSRM/Valhalla for advanced features.

### 2.3 Constraints

- GTFS data must conform to GTFS static (UTF-8, optional BOM).
- Backend expects Israel GTFS (e.g. from MOT); service calendar and 28-hour time (e.g. 24:00–27:59) are supported.
- OSRM and Valhalla are optional; the system must operate without them with reduced functionality (no OSM snapping, no road-network detour).

---

## 3. System Features (Functional Requirements)

### 3.1 GTFS Feed Management

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1.1 | The system shall load the active GTFS feed from a configured path (e.g. `data/gtfs/` or project root zip). | High |
| FR-1.2 | The system shall support updating the feed from a configurable remote URL and activate it only after successful import. | Medium |
| FR-1.3 | The system shall expose feed status (active version, last update attempt, success/failure) via an API. | Medium |

### 3.2 Route Search and Selection

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-2.1 | The user shall be able to search routes by ID or name (or part thereof) and receive a list of matching routes with `route_id`, short/long name, agency. | High |
| FR-2.2 | The user shall be able to select a route from search results or from “lines in area” results; selection shall be consistent (single source of truth). | High |

### 3.3 Graph Build and Visualization

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-3.1 | The system shall build a directed graph for the selected route (and optional direction) for a given date, using the most frequent stop pattern. | High |
| FR-3.2 | The system shall support edge geometry from GTFS shapes when available, with fallback to straight-line segments between stops. | High |
| FR-3.3 | The system shall optionally use OSRM to snap the pattern geometry to the road network for display only (no change to allowed edges). | Low |
| FR-3.4 | The frontend shall display the route pattern (polyline), stops (markers), and optional snapped geometry on a map. | High |

### 3.4 Blockage Definition and Area Search

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-4.1 | The user shall be able to draw a blockage on the map (point, line, polygon, or rectangle) and the system shall store it as GeoJSON geometry. | High |
| FR-4.2 | The user shall be able to clear or cancel the drawn blockage. | High |
| FR-4.3 | Given a polygon (or buffered point/line), a date (YYYYMMDD), and a time window, the system shall return a list of routes whose shapes intersect the polygon and have at least one trip in the time window. | High |
| FR-4.4 | Area search results shall be presented in a detailed, resizable floating table over the map (not in the sidebar); rows shall be selectable to set the current route. | High |

### 3.5 Detour Computation

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-5.1 | The user shall be able to compute a detour for the current blockage, either for the selected route only or for all routes affected in the time window. | High |
| FR-5.2 | The system shall determine which edges of the route(s) intersect the blockage and compute an alternative path that avoids those edges. | High |
| FR-5.3 | When Valhalla is configured, the system shall use Valhalla’s exclude_polygons to route the blocked segment on the road network so the path goes around the blockage. | Medium |
| FR-5.4 | When Valhalla is not available or the request fails, the system shall fall back to a GTFS-based detour (multi-route graph with transfer edges) so a path is still returned. | High |
| FR-5.5 | The frontend shall display the detour path (and optionally the replaced segment) on the map and show a short summary (e.g. blocked edges count, stops in path). | High |

### 3.6 API and Integration

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-6.1 | The backend shall expose REST APIs for: health, route search, feed update/status, graph build, graph stops, graph GeoJSON, detour (start/end stops + blockage), area routes, detours by area. | High |
| FR-6.2 | The frontend shall call these APIs over HTTP (JSON) and handle errors with user-visible messages. | High |
| FR-6.3 | CORS shall be enabled so the frontend (different origin) can call the backend. | High |

---

## 4. External Interface Requirements

### 4.1 User Interfaces

- **Sidebar:** Search box, route list (from search), “Build graph” button, “Pretty with OSM” option, date and time inputs for area/detour, “Find lines in drawn area” button, blockage clear/cancel/undo, detour mode (selected route / all routes), “Compute detour” button, message area, detour result summary.
- **Map:** Leaflet map with tile layer, draw controls (marker, polyline, polygon, rectangle), route polyline, stops, blockage layer, detour path. A floating, resizable “Lines in area” table overlay when area search has results.

### 4.2 Hardware Interfaces

- None specified; standard server and client hardware sufficient for development and small-scale use.

### 4.3 Software Interfaces

- **GTFS:** Local zip or directory of CSV files; optional SQLite DB (e.g. `data/gtfs.db`) with `stop_times.dep_sec`, `trip_time_bounds`, `shape_bbox`.
- **OSRM:** HTTP API (e.g. `http://localhost:5000`) for `/match` (map-matching). Optional.
- **Valhalla:** HTTP API (e.g. `http://localhost:8002`) for `/route` with `exclude_polygons`. Optional.

### 4.4 Communications

- Frontend–backend: HTTP/HTTPS, JSON request/response. Backend base URL configurable (e.g. env `VITE_API_BASE`, default `http://127.0.0.1:8000`).

---

## 5. Non-Functional Requirements

### 5.1 Performance

- Route search and graph build (with cache or precomputed DB) should complete within a few seconds for typical Israeli route sizes.
- Area search should use SQL and spatial prefiltering (e.g. `trip_time_bounds`, `shape_bbox`) to avoid full table scans where possible.
- Detour computation may take longer when Valhalla or multi-route graph is used; timeouts (e.g. 120 s) and user feedback (loading state) are required.

### 5.2 Safety and Security

- No authentication in scope; backend is intended for local or trusted network use.
- No sensitive data stored in the frontend beyond session state; GTFS and cache paths are server-side configuration.

### 5.3 Usability

- Map must not jump or clear the user’s drawn blockage when selecting a route or building a graph; polygon must remain visible.
- “Compute detour” shall be enabled when a blockage exists and (in “selected route” mode) a route is selected; no redundant “select route first” when the route is already selected.

### 5.4 Maintainability

- Backend organized in modules (gtfs_loader, pattern_builder, graph_builder, router_core, area_search, osm_detour, api_models, config).
- Frontend: single main app component and a map-with-drawing component; shared route selection handler for search and area results.

---

## 6. Appendices

### 6.1 API Endpoint Summary

| Method | Path | Purpose |
|--------|------|---------|
| GET | /health | Health check |
| POST | /routes/search | Search routes by query |
| POST | /feed/update | Update GTFS from remote |
| GET | /feed/status | Feed status |
| POST | /graph/build | Build graph for route/date |
| GET | /graph/stops | Get stops for pattern |
| GET | /graph/geojson | Get route GeoJSON for map |
| POST | /detour | Detour between start/end stops with blockage |
| POST | /area/routes | Lines in polygon + date/time |
| POST | /detours/by-area | Detour by blockage (mode: route | all) |
| GET | /stops/in-bounds | Stops in bounding box (optional) |
| POST | /stop/routes | Routes serving a stop (optional) |

### 6.2 Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-03-01 | Initial SRS. |
