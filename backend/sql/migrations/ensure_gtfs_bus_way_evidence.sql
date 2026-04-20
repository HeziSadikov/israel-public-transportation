-- GTFS -> OSM bus-way evidence table for strict detour-v2 compatibility checks.
-- Safe to re-run (idempotent).

CREATE TABLE IF NOT EXISTS gtfs_bus_way_evidence (
    feed_id              INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    osm_way_id           BIGINT NOT NULL,
    direction            TEXT NOT NULL DEFAULT '',
    trip_count           INT NOT NULL DEFAULT 0,
    route_count          INT NOT NULL DEFAULT 0,
    sample_trip_ids_json JSONB,
    confidence_score     DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    last_computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feed_id, osm_way_id, direction)
);

CREATE INDEX IF NOT EXISTS idx_gtfs_bus_way_evidence_feed_way
    ON gtfs_bus_way_evidence(feed_id, osm_way_id);
