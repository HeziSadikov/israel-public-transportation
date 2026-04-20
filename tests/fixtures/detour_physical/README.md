# Detour physical-layer regression fixtures

Each fixture describes a scenario where GTFS geometry is ambiguous relative to OSM (divided roads, parallel service roads, shape drift). Use these for golden tests once the matcher and validator are populated with real feed data.

## Schema (`fixture.schema.json`)

- `id`: stable string id
- `description`: human-readable case
- `route_id`, `direction_id`, `service_date`: GTFS selectors
- `blockage_geojson`: GeoJSON geometry (polygon) in WGS84
- `expect`: object with optional:
  - `blocked_gtfs_edge_keys`: list of logical edge keys `{ "from_stop_sequence": int, "to_stop_sequence": int }` (when pattern known)
  - `entry_segment_id`, `rejoin_segment_id`: expected OSM segment ids after matching (nullable until backfill exists)
  - `detour_valid`: `true` | `false` | `null` (unknown)

## Files

- `example_divided_road.json` — placeholder coordinates; replace with real Israel examples when recording failures.
