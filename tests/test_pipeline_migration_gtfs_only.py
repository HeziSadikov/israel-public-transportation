"""Apply content-addressed pipeline migration on GTFS-only DB (no osm_import_runs)."""

from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from backend.infra.db_access import DB_URL
from backend.infra.pipeline_skip import (
    _pipeline_migration_diagnostics,
    ensure_pipeline_schema,
    relation_exists,
)


def _connect():
    dsn = os.environ.get("DATABASE_URL", DB_URL)
    try:
        return psycopg2.connect(dsn, connect_timeout=10)
    except psycopg2.Error as e:
        pytest.skip(f"Postgres unreachable: {e}")


@pytest.mark.slow
def test_migration_succeeds_without_osm_import_runs():
    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS feed_versions (
                    id SERIAL PRIMARY KEY,
                    source_url TEXT,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    checksum TEXT,
                    patterns_built_checksum TEXT,
                    active BOOLEAN NOT NULL DEFAULT FALSE
                )
                """
            )
        ensure_pipeline_schema(conn)
        assert relation_exists(conn, "public.feed_pipeline_stages")
        assert relation_exists(conn, "public.pattern_signatures")
        assert relation_exists(conn, "public.pattern_osm_match_status")
        assert relation_exists(conn, "public.pattern_edge_match_status")
        assert not relation_exists(conn, "public.osm_import_runs")
        assert not relation_exists(conn, "public.osm_pipeline_stages")
        diag = _pipeline_migration_diagnostics(conn)
        assert "feed_pipeline_stages=yes" in diag
        assert "osm_import_runs=no" in diag
        assert "osm_pipeline_stages=skipped" in diag
    finally:
        conn.close()


@pytest.mark.slow
def test_migration_creates_osm_pipeline_stages_when_parent_exists():
    """Re-run after osm_import_runs exists (minimal stub; not a dummy for planner use)."""
    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS feed_versions (
                    id SERIAL PRIMARY KEY,
                    source_url TEXT,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    checksum TEXT,
                    patterns_built_checksum TEXT,
                    active BOOLEAN NOT NULL DEFAULT FALSE
                )
                """
            )
            cur.execute("DROP TABLE IF EXISTS osm_pipeline_stages CASCADE")
            cur.execute("DROP TABLE IF EXISTS osm_import_runs CASCADE")
            cur.execute(
                """
                CREATE TABLE osm_import_runs (
                    id BIGSERIAL PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'success'
                )
                """
            )
        ensure_pipeline_schema(conn)
        assert relation_exists(conn, "public.osm_pipeline_stages")
        diag = _pipeline_migration_diagnostics(conn)
        assert "osm_pipeline_stages=yes" in diag
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS osm_pipeline_stages CASCADE")
            cur.execute("DROP TABLE IF EXISTS osm_import_runs CASCADE")
    finally:
        conn.close()
