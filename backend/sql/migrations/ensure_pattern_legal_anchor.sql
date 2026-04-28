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
    UNIQUE (feed_version, pattern_id, anchor_version, role, rank_in_role)
);

CREATE INDEX IF NOT EXISTS idx_pattern_legal_anchor_lookup
    ON pattern_legal_anchor_candidate (feed_version, pattern_id);

CREATE INDEX IF NOT EXISTS idx_pattern_legal_anchor_lookup_versioned
    ON pattern_legal_anchor_candidate (feed_version, pattern_id, anchor_version);

-- Skip/resume: terminal outcome per pattern + anchor_version (no_shape, trace_failed, ok, …).
CREATE TABLE IF NOT EXISTS pattern_legal_anchor_pattern_status (
    feed_version   TEXT NOT NULL,
    pattern_id     TEXT NOT NULL,
    anchor_version TEXT NOT NULL,
    outcome        TEXT NOT NULL,
    row_count      INT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feed_version, pattern_id, anchor_version)
);

CREATE INDEX IF NOT EXISTS idx_pattern_legal_anchor_status_lookup
    ON pattern_legal_anchor_pattern_status (feed_version, pattern_id);

-- Cross-run Valhalla trace reuse (forward / reverse) keyed by representative shape.
CREATE TABLE IF NOT EXISTS pattern_trace_valhalla_cache (
    feed_version    TEXT NOT NULL,
    repr_shape_id   TEXT NOT NULL,
    direction       TEXT NOT NULL,
    trace_version   TEXT NOT NULL,
    edges_json      JSONB NOT NULL,
    shape_lonlat_json JSONB,
    total_m         DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feed_version, repr_shape_id, direction, trace_version)
);

-- Upgrade existing DBs: replace old uniqueness (without anchor_version) with versioned uniqueness.
ALTER TABLE pattern_legal_anchor_candidate
    DROP CONSTRAINT IF EXISTS pattern_legal_anchor_candidate_feed_version_pattern_id_role_rank_in_role_key;

DO $legal_anchor_unique$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        WHERE t.relname = 'pattern_legal_anchor_candidate'
          AND c.conname = 'pattern_legal_anchor_candidate_feed_version_pattern_id_anchor_version_role_rank_in_role_key'
    ) THEN
        ALTER TABLE pattern_legal_anchor_candidate
            ADD CONSTRAINT pattern_legal_anchor_candidate_feed_version_pattern_id_anchor_version_role_rank_in_role_key
            UNIQUE (feed_version, pattern_id, anchor_version, role, rank_in_role);
    END IF;
END;
$legal_anchor_unique$;
