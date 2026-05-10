# Golden trips for `/api/v1/area/routes` pass-through modes

These tests assert **known** `trip_id` outcomes against your loaded GTFS in Postgres. Trip IDs are feed-specific, so they are not hardcoded in the main test suite.

## How to run

1. Pick real `trip_id` values from your DB (same `feed_versions.active` feed the app uses).

2. For each golden case, define:
   - `trip_id`, `date_ymd` (service day inside calendar)
   - `polygon_wkt` (4326) that the trip’s shape should intersect
   - `start_sec` / `end_sec` (seconds since midnight) — window where you expect pass-through to match or not
   - `time_semantics_mode`: `pass_through_precise` or `pass_through_stop_proxy`
   - `expect`: `include_route` (route for that trip must appear) or `exclude_route` (that route must not appear — best for routes with a single trip in the AOI)

3. Copy `area_routes_golden_trips.example.json` to `area_routes_golden_local.json` (gitignored) and fill `cases`.

4. Run:

```bash
set AREA_GOLDEN_TRIPS_FILE=tests/fixtures/area_routes_golden_local.json
pytest tests/test_area_routes_golden_trips.py -v
```

Optional: `AREA_ROUTES_STATEMENT_TIMEOUT_MS=0` if pass-through queries need more than 10s on large feeds.

## CI

Without `AREA_GOLDEN_TRIPS_FILE`, golden tests **skip** (no failure). In CI you can inject a trimmed DB + JSON secret if you want strict gates.
