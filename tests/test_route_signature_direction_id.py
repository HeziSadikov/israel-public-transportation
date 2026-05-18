"""route_signatures must accept trips with NULL GTFS direction_id (stored as -1)."""

from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from backend.infra.db_access import (
    DB_URL,
    direction_id_db_value,
    direction_id_from_db,
    get_route_signatures_bulk,
    upsert_route_signature,
)


def test_direction_id_sentinel_roundtrip():
    assert direction_id_db_value(None) == -1
    assert direction_id_db_value("2") == 2
    assert direction_id_from_db(-1) is None
    assert direction_id_from_db(2) == "2"


@pytest.mark.slow
def test_upsert_route_signature_null_direction():
    conn = psycopg2.connect(os.environ.get("DATABASE_URL", DB_URL), connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM feed_versions WHERE active = TRUE LIMIT 1")
            row = cur.fetchone()
            if row is None:
                pytest.skip("no active feed_versions row")
            feed_id = int(row[0])
        route_id = "test-null-direction-id-route"
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM route_signatures WHERE feed_id = %s AND route_id = %s",
                (feed_id, route_id),
            )
        conn.commit()
        upsert_route_signature(feed_id, route_id, None, "a" * 64, conn=conn)
        conn.commit()
        sigs = get_route_signatures_bulk(feed_id, conn=conn)
        assert (route_id, None) in sigs
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT direction_id FROM route_signatures
                WHERE feed_id = %s AND route_id = %s
                """,
                (feed_id, route_id),
            )
            db_dir = cur.fetchone()[0]
        assert int(db_dir) == -1
    finally:
        conn.close()
