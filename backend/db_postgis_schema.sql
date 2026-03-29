-- PostgreSQL/PostGIS schema for Israel GTFS Detour Router
-- This file defines core GTFS tables and derived routing helpers.

CREATE EXTENSION IF NOT EXISTS postgis;

-- Feed versions (blue/green style activation)
CREATE TABLE IF NOT EXISTS feed_versions (
    id              SERIAL PRIMARY KEY,
    source_url      TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum        TEXT,
    patterns_built_checksum TEXT,
    active          BOOLEAN NOT NULL DEFAULT FALSE
);

-- Existing DBs from before patterns_built_checksum: safe to re-run (Postgres 11+).
ALTER TABLE feed_versions ADD COLUMN IF NOT EXISTS patterns_built_checksum TEXT;

-- Core GTFS tables (one schema per project; feed_id references feed_versions)

CREATE TABLE IF NOT EXISTS agencies (
    feed_id     INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    agency_id   TEXT NOT NULL,
    name        TEXT,
    url         TEXT,
    timezone    TEXT,
    lang        TEXT,
    phone       TEXT,
    PRIMARY KEY (feed_id, agency_id)
);

CREATE TABLE IF NOT EXISTS routes (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    route_id        TEXT NOT NULL,
    agency_id       TEXT,
    short_name      TEXT,
    long_name       TEXT,
    route_type      INT,
    route_color     TEXT,
    route_text_color TEXT,
    PRIMARY KEY (feed_id, route_id)
);

CREATE TABLE IF NOT EXISTS stops (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    stop_id         TEXT NOT NULL,
    name            TEXT,
    description     TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    geom            GEOGRAPHY(POINT, 4326),
    zone_id         TEXT,
    parent_station  TEXT,
    PRIMARY KEY (feed_id, stop_id)
);

CREATE INDEX IF NOT EXISTS idx_stops_geom
    ON stops USING GIST (geom);

CREATE TABLE IF NOT EXISTS trips (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    trip_id         TEXT NOT NULL,
    route_id        TEXT NOT NULL,
    service_id      TEXT NOT NULL,
    direction_id    INT,
    shape_id        TEXT,
    headsign        TEXT,
    block_id        TEXT,
    PRIMARY KEY (feed_id, trip_id)
);

CREATE INDEX IF NOT EXISTS idx_trips_route_dir_svc
    ON trips(feed_id, route_id, direction_id, service_id);

CREATE TABLE IF NOT EXISTS stop_times (
    feed_id             INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    id                  BIGSERIAL PRIMARY KEY,
    trip_id             TEXT NOT NULL,
    arrival_time        TEXT,
    departure_time      TEXT,
    stop_id             TEXT NOT NULL,
    stop_sequence       INT NOT NULL,
    pickup_type         INT,
    drop_off_type       INT,
    shape_dist_traveled DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_stop_times_trip_seq
    ON stop_times(feed_id, trip_id, stop_sequence);

CREATE TABLE IF NOT EXISTS calendar (
    feed_id     INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    service_id  TEXT NOT NULL,
    monday      INT,
    tuesday     INT,
    wednesday   INT,
    thursday    INT,
    friday      INT,
    saturday    INT,
    sunday      INT,
    start_date  INT,
    end_date    INT,
    PRIMARY KEY (feed_id, service_id)
);

CREATE TABLE IF NOT EXISTS calendar_dates (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    service_id      TEXT NOT NULL,
    date            INT NOT NULL,
    exception_type  INT NOT NULL,
    PRIMARY KEY (feed_id, service_id, date)
);

-- Shapes as points (raw GTFS)
CREATE TABLE IF NOT EXISTS shapes (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    shape_id        TEXT NOT NULL,
    seq             INT NOT NULL,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    dist_traveled   DOUBLE PRECISION,
    geom            GEOGRAPHY(POINT, 4326),
    PRIMARY KEY (feed_id, shape_id, seq)
);

-- Derived shapes as linestrings for fast spatial search
CREATE TABLE IF NOT EXISTS shapes_lines (
    feed_id     INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    shape_id    TEXT NOT NULL,
    geom        GEOMETRY(LineString, 4326),
    PRIMARY KEY (feed_id, shape_id)
);

CREATE INDEX IF NOT EXISTS idx_shapes_lines_geom
    ON shapes_lines USING GIST (geom);

-- Optional per-route shape summary (representative pattern / date-snapped)
CREATE TABLE IF NOT EXISTS route_shapes (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    route_id        TEXT NOT NULL,
    direction_id    INT,
    date_ymd        INT,
    geom            GEOMETRY(LineString, 4326),
    PRIMARY KEY (feed_id, route_id, direction_id, date_ymd)
);

CREATE INDEX IF NOT EXISTS idx_route_shapes_geom
    ON route_shapes USING GIST (geom);

-- Routing helper: patterns
CREATE TABLE IF NOT EXISTS patterns (
    feed_id         INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    pattern_id      TEXT NOT NULL,
    route_id        TEXT NOT NULL,
    direction_id    INT,
    repr_trip_id    TEXT,
    repr_shape_id   TEXT,
    stop_ids        TEXT[],          -- ordered stop_id chain
    frequency       INT,             -- trips per day
    used_shape      BOOLEAN,
    PRIMARY KEY (feed_id, pattern_id)
);

-- Explicit ordered stop chain per pattern (preferred for joins)
CREATE TABLE IF NOT EXISTS pattern_stops (
    feed_id     INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    pattern_id  TEXT NOT NULL,
    seq         INT NOT NULL,
    stop_id     TEXT NOT NULL,
    PRIMARY KEY (feed_id, pattern_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_pattern_stops_pattern_seq
    ON pattern_stops(feed_id, pattern_id, seq);

-- Trip time bounds for fast time-window filters
CREATE TABLE IF NOT EXISTS trip_time_bounds (
    feed_id     INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    trip_id     TEXT NOT NULL,
    first_sec   INT NOT NULL,
    last_sec    INT NOT NULL,
    PRIMARY KEY (feed_id, trip_id)
);

-- Per-route signatures to detect changes between feeds
CREATE TABLE IF NOT EXISTS route_signatures (
    feed_id      INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    route_id     TEXT NOT NULL,
    direction_id INT,
    sig_hash     TEXT NOT NULL,
    PRIMARY KEY (feed_id, route_id, direction_id)
);

-- Optional cache for precomputed route graphs keyed by route signature
CREATE TABLE IF NOT EXISTS route_graph_cache (
    feed_id        INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    route_id       TEXT NOT NULL,
    direction_id   INT,
    -- date_ymd is now metadata-only (\"last built for\"), not part of the logical key.
    date_ymd       INT,
    pretty_osm     BOOLEAN NOT NULL,
    route_sig_hash TEXT NOT NULL,
    graph_blob     BYTEA NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feed_id, route_id, direction_id, pretty_osm)
);

-- Optional cache for lightweight route preview payloads (selection-time rendering).
CREATE TABLE IF NOT EXISTS route_preview_cache (
    feed_id        INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    route_id       TEXT NOT NULL,
    direction_id   INT,
    profile_key    TEXT NOT NULL,
    pretty_osm     BOOLEAN NOT NULL,
    route_sig_hash TEXT NOT NULL,
    pattern_id     TEXT,
    preview_blob   BYTEA NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feed_id, route_id, direction_id, profile_key, pretty_osm)
);

CREATE INDEX IF NOT EXISTS idx_route_preview_cache_feed_profile
    ON route_preview_cache(feed_id, profile_key, pretty_osm);

-- ---------------------------------------------------------------------------
-- Precomputed directional ride network (pattern_nodes / pattern_edges)
-- ---------------------------------------------------------------------------

-- Each node is a pattern-stop occurrence (pattern-stop node), unique per
-- (pattern_id, stop_id, stop_sequence). This matches router graph semantics:
-- node_id = f"{pattern_id}:{stop_id}:{stop_sequence}" from pattern_stop_node_id().
CREATE TABLE IF NOT EXISTS pattern_nodes (
    feed_id           INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    node_id           TEXT NOT NULL,
    pattern_id       TEXT NOT NULL,
    route_id         TEXT NOT NULL,
    direction_id     INT,
    stop_id          TEXT NOT NULL,
    stop_sequence    INT NOT NULL,
    lat              DOUBLE PRECISION NOT NULL,
    lon              DOUBLE PRECISION NOT NULL,
    out_heading_deg  DOUBLE PRECISION,
    frequency        INT,
    geom              GEOGRAPHY(POINT, 4326),
    PRIMARY KEY (feed_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_pattern_nodes_feed_pattern
    ON pattern_nodes(feed_id, pattern_id);

CREATE INDEX IF NOT EXISTS idx_pattern_nodes_geom
    ON pattern_nodes USING GIST (geom);

CREATE TABLE IF NOT EXISTS pattern_edges (
    feed_id           INT REFERENCES feed_versions(id) ON DELETE CASCADE,
    pattern_id       TEXT NOT NULL,
    from_node_id     TEXT NOT NULL,
    to_node_id       TEXT NOT NULL,
    from_stop_id     TEXT NOT NULL,
    to_stop_id       TEXT NOT NULL,
    travel_time_s    DOUBLE PRECISION,
    distance_m       DOUBLE PRECISION,
    geom              GEOMETRY(LineString, 4326),
    PRIMARY KEY (feed_id, pattern_id, from_node_id, to_node_id)
);

CREATE INDEX IF NOT EXISTS idx_pattern_edges_feed_pattern
    ON pattern_edges(feed_id, pattern_id);

CREATE INDEX IF NOT EXISTS idx_pattern_edges_geom
    ON pattern_edges USING GIST (geom);

-- ---------------------------------------------------------------------------
-- Upgrade: UNIQUE keys for ON CONFLICT upserts (fixes PostgreSQL 42P10)
--
-- Older databases may have route_* tables with only a surrogate PK (e.g. id) or
-- no composite key matching the Python upserts. CREATE TABLE IF NOT EXISTS does
-- not alter existing tables. We add UNIQUE on the logical upsert columns when
-- missing (including when another PK already exists on id).
-- Re-run this file (or db_migration_route_cache_pk.sql) after pulling schema changes.
-- ---------------------------------------------------------------------------

DO $upgrade_route_cache_pk$
BEGIN
    -- route_signatures: ON CONFLICT (feed_id, route_id, direction_id)
    IF to_regclass('public.route_signatures') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON c.conrelid = t.oid
           JOIN pg_namespace n ON t.relnamespace = n.oid
           WHERE n.nspname = 'public'
             AND t.relname = 'route_signatures'
             AND c.contype IN ('p', 'u')
             AND (
                 SELECT coalesce(array_agg(a.attname ORDER BY a.attname), ARRAY[]::name[])
                 FROM unnest(c.conkey) AS ck(attnum)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
             ) = ARRAY['direction_id', 'feed_id', 'route_id']::name[]
       )
    THEN
        DELETE FROM route_signatures a
        USING route_signatures b
        WHERE a.ctid < b.ctid
          AND a.feed_id = b.feed_id
          AND a.route_id = b.route_id
          AND COALESCE(a.direction_id, -1) = COALESCE(b.direction_id, -1);
        ALTER TABLE route_signatures
            ADD CONSTRAINT route_signatures_upsert_uq
            UNIQUE (feed_id, route_id, direction_id);
    END IF;

    -- route_graph_cache: ON CONFLICT (feed_id, route_id, direction_id, pretty_osm)
    IF to_regclass('public.route_graph_cache') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON c.conrelid = t.oid
           JOIN pg_namespace n ON t.relnamespace = n.oid
           WHERE n.nspname = 'public'
             AND t.relname = 'route_graph_cache'
             AND c.contype IN ('p', 'u')
             AND (
                 SELECT coalesce(array_agg(a.attname ORDER BY a.attname), ARRAY[]::name[])
                 FROM unnest(c.conkey) AS ck(attnum)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
             ) = ARRAY['direction_id', 'feed_id', 'pretty_osm', 'route_id']::name[]
       )
    THEN
        DELETE FROM route_graph_cache a
        USING route_graph_cache b
        WHERE a.ctid < b.ctid
          AND a.feed_id = b.feed_id
          AND a.route_id = b.route_id
          AND COALESCE(a.direction_id, -1) = COALESCE(b.direction_id, -1)
          AND a.pretty_osm = b.pretty_osm;
        ALTER TABLE route_graph_cache
            ADD CONSTRAINT route_graph_cache_upsert_uq
            UNIQUE (feed_id, route_id, direction_id, pretty_osm);
    END IF;

    -- route_preview_cache: ON CONFLICT (feed_id, route_id, direction_id, profile_key, pretty_osm)
    IF to_regclass('public.route_preview_cache') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_constraint c
           JOIN pg_class t ON c.conrelid = t.oid
           JOIN pg_namespace n ON t.relnamespace = n.oid
           WHERE n.nspname = 'public'
             AND t.relname = 'route_preview_cache'
             AND c.contype IN ('p', 'u')
             AND (
                 SELECT coalesce(array_agg(a.attname ORDER BY a.attname), ARRAY[]::name[])
                 FROM unnest(c.conkey) AS ck(attnum)
                 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
             ) = ARRAY['direction_id', 'feed_id', 'pretty_osm', 'profile_key', 'route_id']::name[]
       )
    THEN
        DELETE FROM route_preview_cache a
        USING route_preview_cache b
        WHERE a.ctid < b.ctid
          AND a.feed_id = b.feed_id
          AND a.route_id = b.route_id
          AND COALESCE(a.direction_id, -1) = COALESCE(b.direction_id, -1)
          AND a.profile_key = b.profile_key
          AND a.pretty_osm = b.pretty_osm;
        ALTER TABLE route_preview_cache
            ADD CONSTRAINT route_preview_cache_upsert_uq
            UNIQUE (feed_id, route_id, direction_id, profile_key, pretty_osm);
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
    WHEN unique_violation THEN
        RAISE NOTICE
            'route cache upsert key upgrade: unique_violation (duplicates remain?). TRUNCATE route_graph_cache, route_preview_cache, route_signatures and retry.';
END
$upgrade_route_cache_pk$;

