## Israel GTFS Detour Router (Prototype)

Backend + frontend prototype that:

**Requirements:** The project is built and maintained according to the Software Requirements Specification (SRS). See **`docs/SRS.md`** for the full specification and **`docs/SRS-TRACEABILITY.md`** for the mapping from requirements to implementation. A second SRS exists as **`docs/Israel_GTFS_Detour_Router_SRS.docx`**; to incorporate it, export that document to Markdown or paste its requirements so the codebase can be aligned to both.

- **Builds a directed bus network graph strictly from Israel GTFS** (stop→stop edges from route patterns).
- **Computes detour routes** when the user draws a blockage geometry (point/line/polygon).
- **With Valhalla**: detour uses the **road network** (OSM) to route *around* the blocked area so the path does not pass through it.
- **Without Valhalla**: detour stays on existing GTFS corridors (alternative bus segments; may still pass near the blocked area).
- **Optionally snaps geometry to OSM** via a local OSRM instance for prettier display and more realistic blockage intersection.

### Project layout

- **Python backend (FastAPI)** – in `app.py` and `backend/`
  - `gtfs_updater.py`: downloads & versions GTFS feeds, blue/green activation.
  - `gtfs_loader.py`: loads active GTFS feed (UTF‑8 BOM aware).
  - `service_calendar.py`: service_ids per date, 28‑hour time parsing helper.
  - `pattern_builder.py`: builds route patterns (pattern_id = hash(route_id + direction_id + ordered stop_ids)).
  - `graph_builder.py`: builds directed GTFS graph, edge weights, and geometries (shapes.txt aware).
  - `osm_pretty.py`: optional OSRM map-matching for pretty geometry (no change to allowed edges).
  - `osm_detour.py`: optional Valhalla routing with `exclude_polygons` so the detour goes around the blockage on the road network.
  - `router_core.py`: A* routing with haversine heuristic + blockage edge handling.
  - `api_models.py`: Pydantic models for requests/responses.
  - `config.py`: paths, GTFS remote URL, OSM engine URL, simple in‑memory caches.
- **React + Vite + Leaflet frontend** – in `frontend/`
  - Route search, graph build, stop selection, blockage drawing, and detour visualization.
- **OSRM docker-compose** – `docker-compose.osrm.yml`
- **Demo script** – `scripts/demo.py`

---

## Backend setup

### 1. Python environment

From the project root:

```bash
pip install -r requirements.txt
```

Python 3.11+ is recommended.

### 2. Local GTFS data

Place `israel-public-transportation.zip` in the project root as:

- `./israel-public-transportation.zip`

On first run the backend will:

- Use the **active feed** from `data/gtfs/feed_version.json` if present.
- Otherwise fall back to the local `israel-public-transportation.zip` (or unpacked text files in the root) as a **dev-local** feed.

All GTFS text files are read with **UTF‑8 with BOM** (`utf-8-sig`).

The GTFS import (e.g. `python -m scripts.import_gtfs_sqlite --db data/gtfs.db` or the updater’s import step) creates:

- **`stop_times.dep_sec`** and **`trip_time_bounds`** — so “Find lines in drawn area” can use a fast SQL query instead of scanning all stop_times.
- **`shape_bbox`** — bounding box per shape for a spatial prefilter before building LineStrings.

Re-run the import (or a full feed update) to create these tables if you have an older DB.

### 3. Run the backend

From the project root:

```bash
python -m run_uvicorn
```

(Uses `uvicorn_logging.json` for consistent timestamps in the reload parent. If you prefer `py -m uvicorn app:app ...`, either add `--log-config uvicorn_logging.json`, set `UVICORN_LOG_CONFIG` to that file’s path in your shell, or run `.\dev.ps1 app:app --reload --port 8000` / `dev.bat …`.)

The FastAPI app exposes:

1. `GET /health`
2. `POST /routes/search`
3. `POST /feed/update`
4. `GET /feed/status`
5. `POST /graph/build`
6. `GET /graph/stops`
7. `POST /detour`
8. `GET /graph/geojson` (optional helper for full pattern display)

### 4. PostgreSQL/PostGIS (recommended backend)

The app can use **PostgreSQL with PostGIS** as the primary data layer for area search, graph building, and detours. When `DATABASE_URL` is set and an active feed exists in PostGIS, `/area/routes`, `/graph/build`, and detour logic use it; otherwise the legacy SQLite/in-memory path is used.

**Configuration**

- Set `DATABASE_URL` to your Postgres connection string, e.g.  
  `postgresql://user:pass@localhost:5432/israel_gtfs`  
  (PowerShell: `$env:DATABASE_URL="postgresql://user:pass@localhost:5432/israel_gtfs"`)

**Docker (Postgres + PostGIS)**

```bash
docker compose up -d
```

Then create the schema (once, as superuser for `CREATE EXTENSION postgis`):

```bash
docker compose exec postgis psql -U postgres -d israel_gtfs -f /backend/db_postgis_schema.sql
```

**Ingest and patterns**

From the project root, with `DATABASE_URL` set (or pass `--database-url`):

```bash
# All-in-one: patterns (auto date) + graph/preview precompute. Add --with-ingest to run GTFS ingest first.
# Ingest / patterns / graphs each skip their own work when unchanged (--ingest-force / --force-patterns override patterns+ingest).
# Add --ingest-fetch to download only if missing, --ingest-fetch-if-newer to refresh only when remote changed,
# or --ingest-fetch-always to always re-download.
python -m scripts.precompute_all_postgis --workers 4
python -m scripts.precompute_all_postgis --with-ingest --workers 4
python -m scripts.precompute_all_postgis --with-ingest --ingest-fetch-if-newer --workers 4
python -m scripts.precompute_all_postgis --with-ingest --ingest-fetch-always --workers 4
```

```bash
# Ingest (defaults: repo-root israel-public-transportation.zip, DATABASE_URL, MOT source URL).
# Skips reload if the active feed's stored SHA-256 matches the zip; use --force to ingest anyway.
# --fetch downloads from --source-url only if zip is missing.
# --fetch-if-newer probes remote metadata (etag/last-modified/content-length) and downloads when changed
# (or when metadata is unavailable/inconclusive, for safety).
# During --fetch-if-newer, ingest prints metadata probe/decision logs, and downloader prints MB/% progress.
# --fetch-always always downloads first.
python -m backend.scripts.ingest_gtfs_postgis
python -m backend.scripts.ingest_gtfs_postgis --fetch
python -m backend.scripts.ingest_gtfs_postgis --fetch-if-newer
# Override paths / DB if needed:
python -m backend.scripts.ingest_gtfs_postgis --gtfs-zip ./other.zip --database-url "postgresql://user:pass@localhost:5432/israel_gtfs"

# Precompute route patterns (--date optional; skips if patterns already match active feed zip checksum; --force to rebuild).
# Adds feed_versions.patterns_built_checksum via ALTER if your DB predates that column.
# Pattern + graph precompute CLIs log to stderr with timestamps (app.action); ingest continues to use stdout prints.
python -m backend.scripts.build_patterns_postgis
python -m backend.scripts.build_patterns_postgis --date 20260308 --database-url "postgresql://user:pass@localhost:5432/israel_gtfs"

# Precompute route graph cache incrementally (signature-gated; skips unchanged routes)
python -m scripts.precompute_graphs_postgis --workers 4
```

The same script writes **`route_preview_cache`** rows for each `--profiles` value (GTFS geometry always; OSM-pretty lines when map-matching succeeds). **Fast `/graph/preview`** depends on those preview rows (and optional in-memory preview warmup), not only on `route_graph_cache`. Use the **same profile list** as `GRAPH_WARMUP_PROFILES` so the UI’s resolved service profile (weekday / Friday / Shabbat, etc.) matches precomputed keys. After any feed or pattern change that rotates signatures, re-run precompute (or accept occasional `built_fallback` until the next run).

After ingest, the new feed is marked active. Run the backend with `DATABASE_URL` set; `/graph/build` and area/detour endpoints will use PostGIS.

Warmup behavior:
- Startup warmup is enabled by default and hydrates in-memory **graph** cache from PostGIS, then bulk-loads **`route_preview_cache`** into `GRAPH_CACHE` preview keys (same key shape as `/graph/preview`) until `GRAPH_WARMUP_TIMEOUT_S` is reached.
- Configure with:
  - `GRAPH_WARMUP_ENABLED=true|false`
  - `GRAPH_WARMUP_TIMEOUT_S=300`
  - `GRAPH_WARMUP_PROFILES=weekday,friday,saturday,sunday`
  - `GRAPH_WARMUP_PREVIEWS_ENABLED=true|false` (skip preview memory hydration)
  - `GRAPH_WARMUP_PREVIEW_VERIFY_SIG=true|false` (skip rows whose `route_sig_hash` no longer matches the live route)
  - `GRAPH_WARMUP_PREVIEW_MAX_ROUTES=0` (optional cap on preview rows loaded per warmup; `0` = unlimited within the timeout)
- Operational endpoints:
  - `GET /graph/cache/status` (includes `warmup.loaded_previews_gtfs`, `loaded_previews_osm`, `previews_skipped_stale`)
  - `POST /graph/cache/warmup?profiles=weekday,friday` — add `include_previews=false` to warm graphs only

Detour-by-area weighted routing + cache:
- `/detours/by-area` now uses a dedicated weighted routing policy (separate from `/detour` defaults).
- Route-level detour answers are cached in `detour_by_area_cache` (PostGIS) with key fields:
  - feed/version context
  - mode + route_id + direction_id
  - date/time window + transfer radius + `use_osm_detour`
  - policy profile + normalized blockage geometry hash
- Cache entries are guarded by `route_sig_hash`, so route changes automatically invalidate stale rows.
- New by-area policy env knobs:
  - `BY_AREA_ROUTING_POLICY_PROFILE=weighted-v1` (cache-key profile token)
  - `BY_AREA_ROUTING_PER_EDGE_PENALTY_S=20`
  - `BY_AREA_ROUTING_TRANSFER_PENALTY_S=420`
  - `BY_AREA_ROUTING_TRANSFER_DISTANCE_PENALTY_PER_M_S=0.30`
  - `BY_AREA_ROUTING_PATTERN_SWITCH_PENALTY_S=120`
  - `BY_AREA_ROUTING_FREQ_DISCOUNT_COEF=0.0`
  - `BY_AREA_ROUTING_FREQ_CAP=60`
  - `BY_AREA_ROUTING_HEURISTIC_MAX_SPEED_M_S=22.22`
- Optional global routing knobs (all A* paths):
  - `ROUTING_TRANSFER_DISTANCE_PENALTY_PER_M_S=0`
  - `ROUTING_PATTERN_SWITCH_PENALTY_S=0`

Hybrid intersection-aware detours:
- `POST /detour` now attempts hybrid OSM bypass first (when enabled), then falls back to GTFS detour.
- `POST /detours/by-area` (`use_osm_detour=true`) uses the same shared hybrid bypass segment builder.
- Hybrid behavior:
  - derives entry/exit near blockage from route geometry crossing points
  - routes around polygon on road graph (Valhalla), then optional map-match polish
  - rejects bypass segments that still overlap blockage interior
- Config:
  - `HYBRID_DETOUR_ENABLED=true|false` (default true)
  - `VALHALLA_URL=http://localhost:8002` (required for OSM hybrid routing)
- Logs:
  - `/detour`: `hybrid=hit` or `hybrid=fallback_gtfs`
  - `/detours/by-area`: cache profile includes `|hybrid-osm-v1` when OSM hybrid is requested

**Optional later optimization:** if `built_fallback` remains costly after precompute + preview warmup, a slimmer stored preview (for example one merged centerline instead of per-edge GeoJSON features) can shrink pickles and client parse time; `backend/route_preview_payload.py` is the single place to evolve the persisted dict shape alongside the API.

Benchmark helper:
- Compare first-call vs warm-cache `/graph/build` latency:
  - `python -m scripts.benchmark_graph_latency --route-id 36594 --direction-id 1 --date 20260327 --runs 10`
- Compare single-call route preview latency (`/graph/preview`):
  - `python -m scripts.benchmark_graph_latency --endpoint preview --route-id 36594 --direction-id 1 --date 20260327 --runs 10`
  - Optional prewarm before timing:
    - `python -m scripts.benchmark_graph_latency --route-id 36594 --direction-id 1 --date 20260327 --runs 10 --trigger-warmup`
  - `python -m scripts.benchmark_graph_latency --endpoint preview --route-id 36594 --direction-id 1 --date 20260327 --runs 10 --trigger-warmup`

For realistic latency checks, avoid `--reload`:
- `py -m uvicorn app:app --port 8000 --log-config uvicorn_logging.json`

**Manage / CI**

- Schema: `backend/db_postgis_schema.sql`
- Ingest: `backend/scripts/ingest_gtfs_postgis.py`
- Patterns: `backend/scripts/build_patterns_postgis.py`
- Data access: `backend/db_access.py` (used by area search, graph builder, detour graph)

### GTFS update behavior

- Remote base: `https://gtfs.mot.gov.il/gtfsfiles/` (configurable via `GTFS_REMOTE_BASE`).
- Dataset file: `israel-public-transportation.zip` (configurable via `GTFS_REMOTE_FILENAME`).
- `backend/gtfs_download.py`: shared HTTPS streaming download used by `gtfs_updater` and by `ingest_gtfs_postgis --fetch` / `--fetch-always`.
- `backend/gtfs_updater.py`:
  - Downloads the daily feed into `./data/gtfs/YYYYMMDD/gtfs.zip`.
  - Computes SHA‑256 and builds `version_id = YYYYMMDD-<sha8>`.
  - Tries a basic import to validate tables.
  - **Only after a successful import** marks it as `active` (blue/green).
  - Maintains `feed_version.json` with:
    - `active` (version, date, sha256, path, timestamps)
    - `history` of previous versions
    - `last_update_attempt` / `last_update_ok`
  - If download fails, `/feed/update` returns `online_ok=false` and the backend continues to use the last active/local feed.

### Service calendar and 28‑hour time

- `service_calendar.ServiceCalendar`:
  - Computes active `service_id`s for a given `YYYYMMDD`:
    - Uses `calendar.txt` (if present) for base weekly service.
    - Applies `calendar_dates.txt` (`exception_type` 1/2) for additions/removals.
- `parse_gtfs_time_to_seconds`:
  - Parses **extended GTFS times** like `24:xx`–`27:xx` into seconds since start of the service day (supports Israel’s 04:00–03:59 service-day model).

---

## Graph and routing logic

### Route patterns

`backend/pattern_builder.py`:

- For a given **route_id**, optional **direction_id**, and **date (YYYYMMDD)**:
  - Filters trips whose `service_id` is active that date.
  - Builds ordered stop sequences from `stop_times.txt` per `trip_id`.
  - Groups by **pattern key**: `route_id + direction_id + ordered stop_ids`.
  - `pattern_id = sha256(pattern_key)[:16]`.
  - Tracks count (frequency).
  - For each pattern, stores:
    - `pattern_id`
    - `route_id`, `direction_id`
    - `stop_ids` (ordered)
    - `frequency`
    - `representative_trip_id`
    - `representative_shape_id` (if present on trips)
- The **most frequent pattern** is chosen as default for `/graph/build` and `/graph/stops`.

### Graph builder

`backend/graph_builder.py`:

- Nodes: `stops` (from `stops.txt`, lat/lon, name).
- Directed edges: consecutive stops in the selected pattern.
- Edge weights:
  - Currently uses **haversine meters** between stop coordinates.
  - (Ready to prefer `shape_dist_traveled` deltas if needed.)
- Geometry:
  - If `shapes.txt` exists and the representative pattern has a `shape_id`:
    - Builds a `LineString` for the shape.
    - Uses `stop_times.shape_dist_traveled` when available to **slice** the shape between stops.
    - Otherwise **projects stops onto the shape** and slices between projections.
  - If no shapes are available (e.g. Israel Railways with missing shapes):
    - Fallback geometry is a straight `LineString` between stop coordinates.

The result is:

- `GraphBuildResult.graph`: `networkx.DiGraph`.
- `edge_geometries`: map `(from_stop_id, to_stop_id)` → `shapely.LineString`.
- `used_shape`: flag.

### OSM “pretty” geometry (OSRM)

`backend/osm_pretty.py`:

- Uses **OSRM** (local container) to map-match the full pattern polyline:
  - Strategy A: `/match` the entire pattern polyline once.
  - Stores `snapped_pattern_geom`.
  - At present, **per-edge geometries remain the GTFS-based lines**, but the snapped pattern is available for display and blockage intersection.
- If OSRM is unreachable or returns no matchings, the module:
  - Returns `used_osm=False`.
  - Keeps pure GTFS geometries.

**Important**: OSM snapping is **only for geometry**:

- Allowed edges remain exactly the GTFS stop→stop edges.
- Detours are always computed on the GTFS graph.

### Router and blockage

`backend/router_core.py`:

- **A\*** shortest path:
  - Graph: `networkx.DiGraph` built from GTFS stop→stop edges.
  - Weight: edge `weight` (meters) unless edge is blocked (then `inf`).
  - Heuristic: **haversine distance** from current node to goal node.
- **Blocked edges**:
  - For each edge geometry and a given blockage `GeoJSON` geometry:
    - If `edge.linestring.intersects(blockage_geom)`, the edge is blocked.
  - Returns:
    - `blocked_edges` set.
    - `blocked_edges_geojson` (`FeatureCollection`) for visualization.
- **Detour behavior**:
  - If **`VALHALLA_URL`** is set: the segment between “last stop before blockage” and “first stop after blockage” is routed via **Valhalla** with `exclude_polygons` set to the blockage, so the path goes around it on the road network. The rest of the path stays on GTFS edges.
  - If Valhalla is not set or the request fails: **A\*** finds a path on the GTFS graph avoiding blocked edges (may still pass near the blocked area).
- Path geometry:
  - Builds a `FeatureCollection` of `LineString` features along the final stop path (GTFS edges plus, when used, one OSM detour segment with `properties.kind: "osm_detour"`).

---

## API endpoints

### 1) `GET /health`

Simple health check: returns `{status: "ok", time: ...}`.

### 2) `POST /routes/search`

Body:

```json
{ "q": "100", "limit": 20 }
```

Returns:

```json
[
  {
    "route_id": "12345",
    "route_short_name": "100",
    "route_long_name": "Some corridor",
    "agency_id": "15",
    "route_type": 3
  }
]
```

### 3) `POST /feed/update`

- Downloads latest GTFS zip (if online).
- Validates tables and computes hash.
- Imports and, if successful, switches active feed.
- On failure, continues using previous active feed (or dev-local).

Returns (example):

```json
{
  "updated": true,
  "online_ok": true,
  "message": "Feed updated and activated.",
  "active": {
    "version_id": "20260227-abcdef01",
    "date": "20260227",
    "sha256": "...",
    "path": "data/gtfs/20260227/gtfs.zip",
    "imported_ok": true,
    "created_at": "2026-02-27T12:34:56Z"
  }
}
```

### 4) `GET /feed/status`

Returns:

- `active` feed record.
- `history_len`.
- `last_update_attempt`.
- `last_update_ok`.

### 5) `POST /graph/build`

Body:

```json
{
  "route_id": "12345",
  "direction_id": "0",
  "date": "20260227",
  "max_trips": 50,
  "pretty_osm": true
}
```

Behavior:

- Builds patterns for the date and route (and optional direction).
- Chooses the **most frequent pattern** by default.
- Builds GTFS graph and geometries.
- If `pretty_osm = true`, tries OSM map-matching once and stores snapped pattern geometry.
- Caches the result in memory under key:

  - `(feed_version, route_id, direction_id, date, pattern_id, pretty_osm)`.

Returns:

```json
{
  "pattern_id": "abcd1234ef567890",
  "stop_count": 42,
  "edge_count": 41,
  "used_shape": true,
  "used_osm_snapping": true,
  "example_stop_ids": ["10001", "10002", "10003"],
  "feed_version": "20260227-abcdef01"
}
```

### 6) `GET /graph/stops`

Query:

- `route_id` (required)
- `direction_id` (optional)
- `pattern_id` (optional)
- `date` (optional, default = today)

If `pattern_id` is omitted, picks the most frequent pattern.

Returns ordered stops in the pattern:

```json
{
  "pattern_id": "abcd1234ef567890",
  "stops": [
    { "stop_id": "10001", "name": "Stop A", "lat": 31.1, "lon": 35.1, "sequence": 0 },
    { "stop_id": "10002", "name": "Stop B", "lat": 31.2, "lon": 35.2, "sequence": 1 }
  ]
}
```

### 7) `POST /detour`

Body:

```json
{
  "route_id": "12345",
  "direction_id": "0",
  "pattern_id": "abcd1234ef567890",
  "date": "20260227",
  "start_stop_id": "10001",
  "end_stop_id": "10010",
  "blockage_geojson": {
    "type": "Polygon",
    "coordinates": [[[35.2, 31.2], [35.21, 31.2], [35.21, 31.21], [35.2, 31.21], [35.2, 31.2]]]
  }
}
```

Behavior:

- Looks up a previously built graph from the cache; if none:
  - Returns `400` with `"Graph not built yet; call /graph/build first."`
- Computes blocked edges by intersecting blockage geometry with per-edge geometry:
  - Prefers snapped geometries if OSM was used during `/graph/build`.
- **If `VALHALLA_URL` is set** and there are blocked edges on the baseline path: requests a route from “last stop before blockage” to “first stop after blockage” from Valhalla with `exclude_polygons` so the path goes *around* the blockage on the road network. Time/distance for that segment come from Valhalla.
- **Otherwise**: runs A\* shortest path on the GTFS graph avoiding blocked edges (path may still pass near the blocked area).
  - If no path: returns `409` with `"No detour path found."`

Returns:

```json
{
  "blocked_edges_count": 3,
  "stop_path": ["10001", "10002", "10005", "10007", "10010"],
  "path_geojson": { "type": "FeatureCollection", "features": [/* snapped or GTFS edges */] },
  "blocked_edges_geojson": { "type": "FeatureCollection", "features": [/* blocked edges */] },
  "used_shape": true,
  "used_osm_snapping": true,
  "feed_version": "20260227-abcdef01"
}
```

### 8) `GET /graph/geojson`

Optional helper returning full route pattern polyline and stops, using either:

- GTFS geometry only, or
- Snapped pattern geometry (if OSRM is enabled and `pretty_osm=true` was used during `/graph/build`).

---

## Frontend (React + Vite + Leaflet)

### 1. Install dependencies

From `./frontend`:

```bash
npm install
```

### 2. Run dev server

From `./frontend`:

```bash
npm run dev
```

Default Vite dev server: `http://localhost:5173`.

The frontend assumes the backend is reachable at `http://localhost:8000` (`API_BASE` in `App.tsx`).

### Features

- **Search route** (`/routes/search`) and select a `route_id`.
- **Build graph**:
  - Choose whether to enable **“Pretty with OSM”** (requires OSRM).
  - Triggers `/graph/build`, `/graph/stops`, and `/graph/geojson`.
- **Visualize**:
  - Shows stops as markers.
  - Shows pattern polyline (GTFS or snapped).
- **Select start/end stops** from ordered lists.
- **Draw blockage**:
  - Uses Leaflet.draw (marker/polyline/polygon/rectangle).
  - Sends the drawn geometry’s GeoJSON to `/detour`.
- **Compute detour**:
  - Calls `/detour`.
  - Renders GTFS/snapped detour polylines in green.
  - Shows basic summary (blocked edges count, stop path length).

---

## OSM / OSRM setup

### 1. Prepare OSM extract

1. Download an Israel `.osm.pbf` extract (e.g. from Geofabrik).
2. Place the file as:

   - `./osm/israel.osm.pbf`

### 2. Preprocess and run OSRM

From the project root, using the provided compose file:

```bash
# Extract
docker compose -f docker-compose.osrm.yml run --rm osrm \
  osrm-extract -p /opt/car.lua /data/israel.osm.pbf

# Partition
docker compose -f docker-compose.osrm.yml run --rm osrm \
  osrm-partition /data/israel.osrm

# Customize
docker compose -f docker-compose.osrm.yml run --rm osrm \
  osrm-customize /data/israel.osrm

# Run the routing engine
docker compose -f docker-compose.osrm.yml up
```

This exposes OSRM at:

- `http://localhost:5000`

The backend uses this URL by default (`OSM_ENGINE_URL` in `backend/config.py`), via the **/match** API:

- `GET /match/v1/driving/{lon,lat;...}` with `geometries=geojson&overview=full`

If the OSRM engine is **not running**:

- The map-matching step fails gracefully.
- The backend falls back to **pure GTFS geometry** (still functional).

### 3. Valhalla (detour around blockage)

To make the **detour** route *around* the blocked area on the road network (instead of only choosing other GTFS segments), run [Valhalla](https://github.com/valhalla/valhalla) and set:

```bash
export VALHALLA_URL=http://localhost:8002
```

(Valhalla’s default port is 8002; use your own URL if different.)

Valhalla supports `exclude_polygons` in the route request, so the segment between the last stop before the blockage and the first stop after is computed on the road graph while avoiding the drawn polygon. If `VALHALLA_URL` is not set or the request fails, the backend falls back to A* on the GTFS graph (detour may still pass near the blocked area).

---

## Demo script

`scripts/demo.py` is a simple end‑to‑end exercise.

### 1. Make sure backend is running

From project root:

```bash
python -m run_uvicorn
```

### 2. Run demo

From project root:

```bash
python scripts/demo.py
```

Optional: override API base URL:

```bash
API_BASE="http://localhost:8000" python scripts/demo.py
```

Demo steps:

1. Calls `/feed/update` (handles offline failure gracefully).
2. Calls `/routes/search` to pick a sample route.
3. Calls `/graph/build` for that route.
4. Calls `/graph/stops` and chooses:
   - First stop as **start**.
   - Last stop as **end**.
   - Two consecutive middle stops to create a **LineString blockage**.
5. Calls `/detour` with that blockage.
6. Prints a short summary:
   - `blocked_edges_count`
   - `stop_path` length

---

## Quick start summary

1. **Install backend deps**:

   ```bash
   pip install -r requirements.txt
   ```

2. **Place GTFS file**:

   - `./israel-public-transportation.zip`

3. **Run backend**:

   ```bash
   python -m run_uvicorn
   ```

4. **(Optional) Run OSRM for OSM “pretty” geometry**:

   - Place `./osm/israel.osm.pbf`.
   - Run the `osrm-extract`, `osrm-partition`, `osrm-customize`, then:

   ```bash
   docker compose -f docker-compose.osrm.yml up
   ```

5. **Run frontend**:

   ```bash
   cd frontend
   npm install
   npm run dev
   ```

   Then open the URL printed by Vite (default `http://localhost:5173`).

6. **Use the app**:

   - Search for a route.
   - Build graph (optionally “Pretty with OSM”).
   - Select start/end stops.
   - Draw a blockage.
   - Compute detour and inspect the new path.

