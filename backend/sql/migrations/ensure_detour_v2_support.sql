-- Idempotent upgrade: incident + detour v2 persistence (see db_postgis_schema.sql).
-- Safe to re-run; uses IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS incidents (
    id              SERIAL PRIMARY KEY,
    polygon_geom    GEOMETRY(Geometry, 4326) NOT NULL,
    incident_type   TEXT,
    description     TEXT,
    start_time      TIMESTAMPTZ,
    end_time        TIMESTAMPTZ,
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS incident_constraints (
    id               BIGSERIAL PRIMARY KEY,
    incident_id      INT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    constraint_type  TEXT NOT NULL,
    osm_way_id_from  BIGINT,
    osm_way_id_to    BIGINT,
    via_node_id      BIGINT,
    direction        TEXT,
    confidence       DOUBLE PRECISION,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bus_edge_constraints (
    id               BIGSERIAL PRIMARY KEY,
    osm_way_id       BIGINT NOT NULL,
    direction        TEXT,
    constraint_type  TEXT NOT NULL,
    severity         DOUBLE PRECISION,
    time_window_json JSONB,
    reason_code      TEXT,
    notes            TEXT,
    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_by       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bus_turn_constraints (
    id               BIGSERIAL PRIMARY KEY,
    from_way_id      BIGINT NOT NULL,
    via_node_id      BIGINT NOT NULL,
    to_way_id        BIGINT NOT NULL,
    constraint_type  TEXT NOT NULL,
    severity         DOUBLE PRECISION,
    time_window_json JSONB,
    reason_code      TEXT,
    notes            TEXT,
    active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_by       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bus_edge_evidence (
    osm_way_id       BIGINT NOT NULL,
    direction        TEXT NOT NULL DEFAULT '',
    approved_detour_count INT NOT NULL DEFAULT 0,
    successful_trace_count INT NOT NULL DEFAULT 0,
    manual_reject_count INT NOT NULL DEFAULT 0,
    confidence_score DOUBLE PRECISION,
    last_seen_at     TIMESTAMPTZ,
    PRIMARY KEY (osm_way_id, direction)
);

CREATE TABLE IF NOT EXISTS bus_turn_evidence (
    from_way_id      BIGINT NOT NULL,
    via_node_id      BIGINT NOT NULL,
    to_way_id        BIGINT NOT NULL,
    approved_detour_count INT NOT NULL DEFAULT 0,
    successful_trace_count INT NOT NULL DEFAULT 0,
    manual_reject_count INT NOT NULL DEFAULT 0,
    confidence_score DOUBLE PRECISION,
    last_seen_at     TIMESTAMPTZ,
    PRIMARY KEY (from_way_id, via_node_id, to_way_id)
);

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

CREATE INDEX IF NOT EXISTS idx_gtfs_bus_way_evidence_feed_way ON gtfs_bus_way_evidence (feed_id, osm_way_id);

CREATE TABLE IF NOT EXISTS detour_requests (
    id               BIGSERIAL PRIMARY KEY,
    feed_id          INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    trip_id          TEXT NOT NULL,
    route_id         TEXT NOT NULL,
    service_date     TEXT NOT NULL,
    incident_id      INT REFERENCES incidents(id) ON DELETE SET NULL,
    status           TEXT NOT NULL,
    payload_json     JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS detour_candidates (
    id                   BIGSERIAL PRIMARY KEY,
    detour_request_id    BIGINT NOT NULL REFERENCES detour_requests(id) ON DELETE CASCADE,
    candidate_rank       INT NOT NULL,
    strategy             TEXT,
    geometry_json        JSONB,
    road_sequence_json   JSONB,
    turn_sequence_json   JSONB,
    travel_time_s        DOUBLE PRECISION,
    distance_m           DOUBLE PRECISION,
    score                DOUBLE PRECISION,
    accepted             BOOLEAN NOT NULL DEFAULT FALSE,
    rejection_reasons_json JSONB,
    score_breakdown_json JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_detour_candidates_request ON detour_candidates (detour_request_id);

CREATE TABLE IF NOT EXISTS approved_detours (
    id                   BIGSERIAL PRIMARY KEY,
    feed_id              INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    route_id             TEXT NOT NULL,
    trip_pattern_key     TEXT NOT NULL,
    incident_signature   TEXT NOT NULL,
    geometry_json        JSONB NOT NULL,
    road_sequence_json   JSONB,
    turn_sequence_json   JSONB,
    approved_by          TEXT,
    approved_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
