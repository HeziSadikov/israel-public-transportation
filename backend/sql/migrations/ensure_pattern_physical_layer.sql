-- Physical layer: GTFS pattern edges ↔ directed OSM segment chains (idempotent).

ALTER TABLE osm_road_segments ADD COLUMN IF NOT EXISTS length_m DOUBLE PRECISION;
ALTER TABLE osm_road_segments ADD COLUMN IF NOT EXISTS heading_start_deg DOUBLE PRECISION;
ALTER TABLE osm_road_segments ADD COLUMN IF NOT EXISTS heading_end_deg DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS pattern_edge (
    pattern_edge_id     BIGSERIAL PRIMARY KEY,
    feed_version        TEXT NOT NULL,
    pattern_id          TEXT NOT NULL,
    route_id            TEXT NOT NULL,
    direction_id        INT,
    from_stop_id        TEXT NOT NULL,
    to_stop_id          TEXT NOT NULL,
    from_stop_sequence  INT NOT NULL,
    to_stop_sequence    INT NOT NULL,
    representative_trip_id TEXT,
    representative_shape_id  TEXT,
    gtfs_geom           GEOMETRY(LINESTRING, 4326),
    gtfs_length_m       DOUBLE PRECISION,
    UNIQUE (feed_version, pattern_id, from_stop_sequence, to_stop_sequence)
);

CREATE INDEX IF NOT EXISTS idx_pattern_edge_feed_pattern ON pattern_edge (feed_version, pattern_id);

CREATE TABLE IF NOT EXISTS pattern_edge_match (
    pattern_edge_match_id BIGSERIAL PRIMARY KEY,
    pattern_edge_id       BIGINT NOT NULL REFERENCES pattern_edge(pattern_edge_id) ON DELETE CASCADE,
    ordinal               INT NOT NULL,
    segment_id            BIGINT NOT NULL REFERENCES osm_road_segments(segment_id) ON DELETE CASCADE,
    segment_forward       BOOLEAN NOT NULL,
    offset_mean_m         DOUBLE PRECISION,
    heading_error_deg     DOUBLE PRECISION,
    UNIQUE (pattern_edge_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_pattern_edge_match_edge ON pattern_edge_match (pattern_edge_id);

CREATE TABLE IF NOT EXISTS pattern_edge_match_summary (
    pattern_edge_id       BIGINT PRIMARY KEY REFERENCES pattern_edge(pattern_edge_id) ON DELETE CASCADE,
    matched_geom          GEOMETRY(LINESTRING, 4326),
    entry_segment_id      BIGINT,
    exit_segment_id       BIGINT,
    confidence            DOUBLE PRECISION,
    coverage_ratio        DOUBLE PRECISION,
    mean_offset_m         DOUBLE PRECISION,
    mean_heading_error_deg DOUBLE PRECISION,
    is_ambiguous          BOOLEAN NOT NULL DEFAULT FALSE,
    match_version         TEXT
);

CREATE INDEX IF NOT EXISTS idx_pattern_edge_match_summary_geom ON pattern_edge_match_summary USING GIST (matched_geom);

CREATE TABLE IF NOT EXISTS detour_audit (
    detour_id             UUID PRIMARY KEY,
    request_hash          TEXT,
    route_id              TEXT,
    direction_id          INT,
    entry_segment_id      BIGINT,
    rejoin_segment_id     BIGINT,
    raw_valhalla_geom     GEOMETRY(LINESTRING, 4326),
    decoded_geom          GEOMETRY(LINESTRING, 4326),
    validation_status     TEXT,
    validation_reason     TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_detour_audit_created ON detour_audit (created_at);

ALTER TABLE detour_audit ADD COLUMN IF NOT EXISTS trip_id TEXT;
ALTER TABLE detour_audit ADD COLUMN IF NOT EXISTS debug_json JSONB;
