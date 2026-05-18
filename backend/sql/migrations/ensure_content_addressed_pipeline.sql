-- Content-addressed pipeline: first-class skip state and pattern content signatures.
-- Idempotent; safe to re-run on GTFS-only DBs (no osm_import_runs) and full PostGIS DBs.

-- ---------------------------------------------------------------------------
-- GTFS / feed / pattern / graph registry (unconditional)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS feed_pipeline_stages (
    feed_id           INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    stage_name        TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    outcome           TEXT NOT NULL CHECK (outcome IN ('running', 'succeeded', 'failed')),
    completed_at      TIMESTAMPTZ,
    stats_json        JSONB,
    PRIMARY KEY (feed_id, stage_name)
);

CREATE TABLE IF NOT EXISTS pattern_signatures (
    feed_id     INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    pattern_id  TEXT NOT NULL,
    sig_hash    TEXT NOT NULL,
    PRIMARY KEY (feed_id, pattern_id)
);

CREATE INDEX IF NOT EXISTS idx_pattern_signatures_feed
    ON pattern_signatures (feed_id);

CREATE TABLE IF NOT EXISTS pattern_osm_match_status (
    feed_id           INT NOT NULL REFERENCES feed_versions(id) ON DELETE CASCADE,
    pattern_id        TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    outcome           TEXT NOT NULL CHECK (outcome IN ('running', 'succeeded', 'failed')),
    completed_at      TIMESTAMPTZ,
    stats_json        JSONB,
    PRIMARY KEY (feed_id, pattern_id)
);

CREATE TABLE IF NOT EXISTS pattern_edge_match_status (
    feed_id           INT NOT NULL,
    pattern_id        TEXT NOT NULL,
    match_version     TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    outcome           TEXT NOT NULL CHECK (outcome IN ('running', 'succeeded', 'failed')),
    completed_at      TIMESTAMPTZ,
    stats_json        JSONB,
    PRIMARY KEY (feed_id, pattern_id, match_version)
);

-- ---------------------------------------------------------------------------
-- OSM import registry (only when osm_import_runs exists; dynamic SQL for FK parse)
-- ---------------------------------------------------------------------------

DO $osm_pipeline$
BEGIN
    IF to_regclass('public.osm_import_runs') IS NOT NULL THEN
        EXECUTE $create_osm_stages$
            CREATE TABLE IF NOT EXISTS osm_pipeline_stages (
                osm_import_run_id BIGINT NOT NULL REFERENCES osm_import_runs(id) ON DELETE CASCADE,
                stage_name        TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                outcome           TEXT NOT NULL CHECK (outcome IN ('running', 'succeeded', 'failed')),
                completed_at      TIMESTAMPTZ,
                stats_json        JSONB,
                PRIMARY KEY (osm_import_run_id, stage_name)
            );
        $create_osm_stages$;

        EXECUTE $add_fp$
            ALTER TABLE osm_import_runs
            ADD COLUMN IF NOT EXISTS osm_dataset_fingerprint TEXT;
        $add_fp$;

        IF to_regclass('public.idx_osm_import_runs_dataset_fp') IS NULL THEN
            EXECUTE $idx$
                CREATE INDEX idx_osm_import_runs_dataset_fp
                ON osm_import_runs (osm_dataset_fingerprint)
                WHERE status IN ('success', 'verify_only');
            $idx$;
        END IF;
    END IF;
END;
$osm_pipeline$;

-- ---------------------------------------------------------------------------
-- Legal anchors pipeline columns (only when parent table exists)
-- ---------------------------------------------------------------------------

DO $legal_anchor_cols$
BEGIN
    IF to_regclass('public.pattern_legal_anchor_pattern_status') IS NOT NULL THEN
        EXECUTE $add_ifp$
            ALTER TABLE pattern_legal_anchor_pattern_status
            ADD COLUMN IF NOT EXISTS input_fingerprint TEXT;
        $add_ifp$;

        EXECUTE $add_po$
            ALTER TABLE pattern_legal_anchor_pattern_status
            ADD COLUMN IF NOT EXISTS pipeline_outcome TEXT;
        $add_po$;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE t.relname = 'pattern_legal_anchor_pattern_status'
              AND c.conname = 'pattern_legal_anchor_pattern_status_pipeline_outcome_check'
        ) THEN
            EXECUTE $chk$
                ALTER TABLE pattern_legal_anchor_pattern_status
                ADD CONSTRAINT pattern_legal_anchor_pattern_status_pipeline_outcome_check
                CHECK (
                    pipeline_outcome IS NULL
                    OR pipeline_outcome IN ('running', 'succeeded', 'failed')
                );
            $chk$;
        END IF;
    END IF;
END;
$legal_anchor_cols$;
