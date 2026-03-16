# Refactor: Less Python, More SQL

Goal: make the app faster by doing more work in PostgreSQL/PostGIS and less in Python.

## Current state

| Area | Where | Heavy work |
|------|--------|------------|
| **Blocked edges** | `router_core.compute_blocked_edges` | Build list of LineStrings, STRtree in Python, intersect each with polygon |
| **Routes serving stop** | `db_access.get_routes_serving_stop_pg` | One query for stop_times, then Python loop: active_services, time window, aggregate |
| **Graph build** | `graph_builder` | Pattern stops + shape from DB, then Python: nodes, edges, shape slicing, travel times |
| **Detour graph** | `detour_graph.build_detour_graph` | One SQL for routes in polygon, then per-route: get pattern, build graph in Python, merge; transfer edges in Python (grid + haversine) |
| **Pattern discovery** | `pattern_builder` | N × get_stop_times_for_trip, group by stop sequence in Python (when not using precomputed `patterns` table) |
| **Feed load** | `feed_postgis.load_active_feed` | Several SELECTs, build big in-memory feed object |

Already SQL-heavy: `/stops/in-bounds`, `/routes/search`, `/area/routes` (get_routes_in_polygon), pattern lookup, pattern_stops, shape line, trip time bounds, graph cache.

---

## Phase 1 (quick wins)

1. **Blocked-edge detection in PostGIS**  
   - Add `db_access.get_blocked_edge_keys_pg(edge_geometries, blockage_geojson)` that sends edge WKT + polygon WKT in one query, returns set of (node_u, node_v) that intersect.  
   - Use in `router_core.compute_blocked_edges`: call DB instead of STRtree in Python.  
   - Keeps same API; only the intersection runs in SQL.

2. **Routes serving stop in one query**  
   - Single SQL: active services for date, join stop_times/trips, filter by time window using parsed seconds in SQL (e.g. `split_part` for GTFS time strings), aggregate min/max per route/direction, join routes/agencies.  
   - Replace Python loop in `get_routes_serving_stop_pg` with one `db_access` call.

---

## Phase 2 (medium)

3. **Pattern edge geometries in PostGIS**  
   - Table or materialized view: `pattern_edges(feed_id, pattern_id, from_seq, to_seq, geom)` built when patterns are built (or on demand).  
   - Blocked-edge query then uses `pattern_edges` + polygon only (no passing WKT from Python).  
   - Requires pattern build / ingest to write edge geometries.

4. **Precomputed transfer edges**  
   - Table `transfer_edges(feed_id, from_pattern_id, from_seq, to_pattern_id, to_seq, distance_m, ...)` for stops within radius and heading tolerance.  
   - Built once per feed/area or on demand; `build_detour_graph` reads from DB instead of grid + haversine in Python.

---

## Phase 3 (larger)

5. **Graph as tables**  
   - Tables: `graph_nodes`, `graph_edges` (pattern_id, from/to node keys, travel_time_s, geom).  
   - A* stays in Python but loads one query (edges for relevant patterns) instead of building graph from pattern + shape in Python.  
   - Cache layer can still use graph_blob for speed; cold path uses SQL.

6. **Avoid full feed load**  
   - Replace `load_active_feed()` with targeted queries (e.g. “stops for these stop_ids”, “routes for these route_ids”).  
   - Callers already use patterns/pattern_stops from DB; reduce remaining “full feed” usage to minimal lookups.

---

## Implementation order

- **Phase 1** ✅ Implemented: blocked-edge detection in PostGIS (`get_blocked_edge_keys_pg` + `compute_blocked_edges`), and routes-serving-stop as a single SQL query (`get_routes_serving_stop_pg`).
- **Phase 2/3** can follow incrementally without breaking existing behavior.
