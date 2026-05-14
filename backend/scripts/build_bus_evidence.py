"""Rebuild gtfs_bus_segment_evidence / gtfs_bus_turn_evidence from ``pattern_osm_segments``."""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from backend.bus_corridor.build_bus_evidence import build_bus_evidence
from backend.infra import db_access as db
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.osm_import_db import ensure_detour_v3_layer


def _connect(database_url: str):
    import psycopg2
    from psycopg2.extras import DictCursor

    return psycopg2.connect(database_url, cursor_factory=DictCursor)


def main(argv: Optional[list[str]] = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(
        description="Aggregate pattern_osm_segments into gtfs_bus_segment_evidence and gtfs_bus_turn_evidence."
    )
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument(
        "--feed-id",
        type=int,
        default=None,
        help="Feed version id (default: active feed from feed_versions).",
    )
    args = ap.parse_args(argv)

    db_url = args.database_url or db.DB_URL
    conn = _connect(db_url)
    try:
        db.ensure_pattern_physical_layer_schema(conn=conn)
        ensure_detour_v3_layer(conn)
        out = build_bus_evidence(conn, feed_id=args.feed_id, commit=True)
        log("build_bus_evidence", f"done {out!r}")
        return 0
    except Exception as e:
        conn.rollback()
        log("build_bus_evidence", f"error={e!s}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
