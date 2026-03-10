-- PostgreSQL/PostGIS schema for Israel GTFS Detour Router
-- This file defines core GTFS tables and derived routing helpers.

CREATE EXTENSION IF NOT EXISTS postgis;

-- Feed versions (blue/green style activation)
CREATE TABLE IF NOT EXISTS feed_versions (
    id              SERIAL PRIMARY KEY,
    source_url      TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum        TEXT,
    active          BOOLEAN NOT NULL DEFAULT FALSE
);

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

