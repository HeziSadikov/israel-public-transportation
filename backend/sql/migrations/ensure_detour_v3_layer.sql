-- Idempotent migration: Detour v3 bus-corridor road graph layer.
-- Safe to re-run; uses IF NOT EXISTS and ADD COLUMN IF NOT EXISTS.
--
-- See backend/sql/schema/db_postgis_schema.sql for the canonical schema; this
-- migration brings existing databases up to the v3 layer additions:
--   - osm_import_runs (audit trail)
--   - osm_nodes / osm_ways / osm_way_nodes (raw OSM parse tables)
--   - osm_segment_turns (precomputed legal turns; populated in M2)
--   - pattern_osm_segments / gtfs_bus_segment_evidence / gtfs_bus_turn_evidence (M4/M5)
--   - osm_road_segments / osm_turn_restrictions: headings, length, provenance.
--     The length_m / heading_start_deg / heading_end_deg columns are also
--     declared by ensure_pattern_physical_layer.sql; this file uses
--     ADD COLUMN IF NOT EXISTS for forward / backward compatibility.
--
-- Provenance rule: every row written by the v3 PBF importer sets
--   import_run_id  = current osm_import_runs.id
--   import_source  = 'detour_v3_pbf_import'
-- Legacy rows (NULL/NULL) are preserved by --reset-osm-import.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- Audit trail: one row per OSM PBF import run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS osm_import_runs (
    id                BIGSERIAL PRIMARY KEY,
    pbf_path          TEXT,
    pbf_url           TEXT,
    pbf_size_bytes    BIGINT,
    pbf_modified_at   TIMESTAMPTZ,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'running',
    stats_json        JSONB
);

CREATE INDEX IF NOT EXISTS idx_osm_import_runs_started_at
    ON osm_import_runs(started_at DESC);

-- ---------------------------------------------------------------------------
-- Raw OSM parse tables (v3-owned; safe to truncate on reset).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS osm_nodes (
    node_id BIGINT PRIMARY KEY,
    geom    GEOMETRY(Point, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_osm_nodes_geom
    ON osm_nodes USING GIST (geom);

CREATE TABLE IF NOT EXISTS osm_ways (
    way_id     BIGINT PRIMARY KEY,
    highway    TEXT,
    name       TEXT,
    oneway     TEXT,
    access     TEXT,
    bus        TEXT,
    psv        TEXT,
    service    TEXT,
    junction   TEXT,
    maxwidth   DOUBLE PRECISION,
    maxheight  DOUBLE PRECISION,
    tags_json  JSONB
);

CREATE INDEX IF NOT EXISTS idx_osm_ways_highway
    ON osm_ways(highway);

CREATE TABLE IF NOT EXISTS osm_way_nodes (
    way_id  BIGINT NOT NULL,
    seq     INT NOT NULL,
    node_id BIGINT NOT NULL,
    PRIMARY KEY (way_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_osm_way_nodes_node
    ON osm_way_nodes(node_id);

-- ---------------------------------------------------------------------------
-- Precomputed legal turn table (populated in M2).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS osm_segment_turns (
    from_segment_id    BIGINT NOT NULL,
    via_node_id        BIGINT NOT NULL,
    to_segment_id      BIGINT NOT NULL,
    turn_angle_deg     DOUBLE PRECISION,
    is_forbidden       BOOLEAN NOT NULL,
    is_only_restricted BOOLEAN NOT NULL,
    restriction_id     BIGINT,
    is_u_turn          BOOLEAN NOT NULL,
    PRIMARY KEY (from_segment_id, to_segment_id)
);

CREATE INDEX IF NOT EXISTS idx_osm_segment_turns_via
    ON osm_segment_turns(via_node_id);

CREATE INDEX IF NOT EXISTS idx_osm_segment_turns_from
    ON osm_segment_turns(from_segment_id);

-- ---------------------------------------------------------------------------
-- Bus-corridor evidence layer (populated in M4 / M5).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pattern_osm_segments (
    feed_id     INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    pattern_id  TEXT NOT NULL,
    seq         INT NOT NULL,
    segment_id  BIGINT NOT NULL,
    confidence  DOUBLE PRECISION,
    source      TEXT,
    PRIMARY KEY (feed_id, pattern_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_pattern_osm_segments_feed_segment
    ON pattern_osm_segments(feed_id, segment_id);

CREATE TABLE IF NOT EXISTS gtfs_bus_segment_evidence (
    feed_id          INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    segment_id       BIGINT NOT NULL,
    trip_count       INT NOT NULL DEFAULT 0,
    route_count      INT NOT NULL DEFAULT 0,
    pattern_count    INT NOT NULL DEFAULT 0,
    confidence_score DOUBLE PRECISION,
    PRIMARY KEY (feed_id, segment_id)
);

CREATE TABLE IF NOT EXISTS gtfs_bus_turn_evidence (
    feed_id          INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    from_segment_id  BIGINT NOT NULL,
    to_segment_id    BIGINT NOT NULL,
    trip_count       INT NOT NULL DEFAULT 0,
    route_count      INT NOT NULL DEFAULT 0,
    pattern_count    INT NOT NULL DEFAULT 0,
    confidence_score DOUBLE PRECISION,
    PRIMARY KEY (feed_id, from_segment_id, to_segment_id)
);

ALTER TABLE gtfs_bus_turn_evidence
    ADD COLUMN IF NOT EXISTS pattern_count INT NOT NULL DEFAULT 0;

-- ---------------------------------------------------------------------------
-- Shared-table ALTERs: extend osm_road_segments and osm_turn_restrictions.
-- Existing rows have NULL provenance and are treated as LEGACY: they are
-- preserved by --reset-osm-import. Only v3-written rows are deletable.
-- ---------------------------------------------------------------------------

-- osm_road_segments and osm_turn_restrictions are declared in
-- db_postgis_schema.sql. They predate this migration on most DBs, so we ADD
-- COLUMN IF NOT EXISTS to keep this script idempotent.

-- length_m / heading_start_deg / heading_end_deg are also added by
-- ensure_pattern_physical_layer.sql; the IF NOT EXISTS guards make this
-- idempotent for either ordering.
ALTER TABLE osm_road_segments
    ADD COLUMN IF NOT EXISTS length_m          DOUBLE PRECISION;
ALTER TABLE osm_road_segments
    ADD COLUMN IF NOT EXISTS heading_start_deg DOUBLE PRECISION;
ALTER TABLE osm_road_segments
    ADD COLUMN IF NOT EXISTS heading_end_deg   DOUBLE PRECISION;
ALTER TABLE osm_road_segments
    ADD COLUMN IF NOT EXISTS import_run_id     BIGINT REFERENCES osm_import_runs(id) ON DELETE SET NULL;
ALTER TABLE osm_road_segments
    ADD COLUMN IF NOT EXISTS import_source     TEXT;

ALTER TABLE osm_turn_restrictions
    ADD COLUMN IF NOT EXISTS via_way_id      BIGINT;
ALTER TABLE osm_turn_restrictions
    ADD COLUMN IF NOT EXISTS applies_to_bus  BOOLEAN;
ALTER TABLE osm_turn_restrictions
    ADD COLUMN IF NOT EXISTS except_bus      BOOLEAN;
ALTER TABLE osm_turn_restrictions
    ADD COLUMN IF NOT EXISTS except_psv      BOOLEAN;
ALTER TABLE osm_turn_restrictions
    ADD COLUMN IF NOT EXISTS import_run_id   BIGINT REFERENCES osm_import_runs(id) ON DELETE SET NULL;
ALTER TABLE osm_turn_restrictions
    ADD COLUMN IF NOT EXISTS import_source   TEXT;

-- Partial indexes keep legacy rows out of v3 hot paths without touching them.
CREATE INDEX IF NOT EXISTS idx_osm_road_segments_import_run
    ON osm_road_segments(import_run_id) WHERE import_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_osm_turn_restrictions_import_run
    ON osm_turn_restrictions(import_run_id) WHERE import_run_id IS NOT NULL;
