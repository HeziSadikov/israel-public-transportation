-- Legal divergence / rejoin anchor index per GTFS pattern (Valhalla trace + PostGIS legality).
-- Idempotent; safe to re-run.

CREATE TABLE IF NOT EXISTS pattern_legal_anchor_candidate (
    id                  BIGSERIAL PRIMARY KEY,
    feed_version        TEXT NOT NULL,
    pattern_id          TEXT NOT NULL,
    role                TEXT NOT NULL CHECK (role IN ('exit', 'rejoin')),
    rank_in_role        INT NOT NULL,
    shape_dist_m        DOUBLE PRECISION NOT NULL,
    lon                 DOUBLE PRECISION NOT NULL,
    lat                 DOUBLE PRECISION NOT NULL,
    osm_node_id         BIGINT,
    incoming_way_id     BIGINT,
    score               DOUBLE PRECISION,
    trace_meta          JSONB,
    anchor_version      TEXT NOT NULL DEFAULT '1',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (feed_version, pattern_id, role, rank_in_role)
);

CREATE INDEX IF NOT EXISTS idx_pattern_legal_anchor_lookup
    ON pattern_legal_anchor_candidate (feed_version, pattern_id);
