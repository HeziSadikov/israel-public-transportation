"""Session/transaction hygiene between ensure_pipeline_schema and pattern DDL helpers."""

from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from backend.infra.db_access import DB_URL
from backend.infra.pipeline_skip import commit_if_in_transaction, ensure_pipeline_schema
from backend.scripts.build_patterns_postgis import _ensure_patterns_built_checksum_column


def _connect():
    dsn = os.environ.get("DATABASE_URL", DB_URL)
    try:
        return psycopg2.connect(dsn, connect_timeout=10)
    except psycopg2.Error as e:
        pytest.skip(f"Postgres unreachable: {e}")


@pytest.mark.slow
def test_ensure_pipeline_schema_then_patterns_checksum_column_no_autocommit_toggle():
    conn = _connect()
    try:
        conn.autocommit = True
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
        conn.autocommit = False
        ensure_pipeline_schema(conn)
        _ensure_patterns_built_checksum_column(conn)
        commit_if_in_transaction(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'feed_versions'
                  AND column_name = 'patterns_built_checksum'
                """
            )
            assert cur.fetchone() is not None
    finally:
        conn.close()


def test_commit_if_in_transaction_clears_open_txn():
    conn = _connect()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        assert conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION
        commit_if_in_transaction(conn)
        assert conn.status == psycopg2.extensions.STATUS_READY
        conn.autocommit = True
        conn.autocommit = False
    finally:
        conn.close()
