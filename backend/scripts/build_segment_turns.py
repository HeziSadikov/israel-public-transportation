"""
Rebuild ``osm_segment_turns`` (M2) from ``osm_road_segments`` + ``osm_turn_restrictions``.

Usage::

    python -m backend.scripts.build_segment_turns
    python -m backend.scripts.build_segment_turns --database-url postgresql://...
    python -m backend.scripts.build_segment_turns --import-run-id 42

Requires directed segments already populated (``import_osm_pbf --with-segments``).
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

import psycopg2
from psycopg2.extras import DictCursor

from backend.infra import db_access as db
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.infra.osm_import_db import ensure_detour_v3_layer
from backend.osm_import.build_turn_table import build_segment_turns


def _connect(database_url: str):
    return psycopg2.connect(database_url, cursor_factory=DictCursor)


def main(argv: Optional[list[str]] = None) -> int:
    ensure_cli_action_logging()
    ap = argparse.ArgumentParser(description="Rebuild osm_segment_turns (legal turn table, M2).")
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument(
        "--import-run-id",
        type=int,
        default=None,
        help="Only rebuild adjacency from segments with this import_run_id (both legs must match).",
    )
    args = ap.parse_args(argv)

    db_url = args.database_url or db.DB_URL
    conn = _connect(db_url)
    try:
        ensure_detour_v3_layer(conn)
        stats = build_segment_turns(conn, segment_import_run_id=args.import_run_id)
        log(
            "build_segment_turns",
            f"done turns_inserted={stats.turns_inserted} legal={stats.legal_turns} forbidden={stats.forbidden_turns}",
        )
        return 0
    except Exception as e:
        conn.rollback()
        log("build_segment_turns", f"error={e!s}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
