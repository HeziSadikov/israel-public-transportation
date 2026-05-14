"""
DB helpers for detour v3 OSM PBF import: schema migration, run lifecycle,
provenance-scoped reset.

The schema migration is ``backend/sql/migrations/ensure_detour_v3_layer.sql``.

Provenance contract for shared tables (``osm_road_segments`` /
``osm_turn_restrictions``):

* legacy rows have ``import_run_id IS NULL`` and ``import_source IS NULL``.
* v3-written rows must set ``import_run_id`` to the current
  ``osm_import_runs.id`` and ``import_source = IMPORT_SOURCE_V3``.

``reset_v3_osm_import`` only deletes rows that satisfy the provenance
predicate; legacy rows are preserved. To wipe legacy rows too, the caller
must pass ``dangerously_truncate_shared=True`` (CLI requires
``--dangerously-truncate-shared-osm-tables`` and an explicit confirmation).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from psycopg2.extras import Json

from .logging_utils import log


IMPORT_SOURCE_V3 = "detour_v3_pbf_import"

# v3-owned tables: safe to TRUNCATE in --reset-osm-import (no legacy data).
V3_ONLY_OSM_TABLES: tuple[str, ...] = (
    "osm_segment_turns",
    "osm_way_nodes",
    "osm_ways",
    "osm_nodes",
)

# Shared tables: ALTERed by v3, may contain legacy rows. Reset deletes only
# rows that carry v3 provenance.
SHARED_OSM_TABLES: tuple[str, ...] = (
    "osm_road_segments",
    "osm_turn_restrictions",
)


_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "sql"
    / "migrations"
    / "ensure_detour_v3_layer.sql"
)


def ensure_detour_v3_layer(conn) -> None:
    """
    Apply ``ensure_detour_v3_layer.sql`` against ``conn``.

    Idempotent: the migration uses ``CREATE TABLE IF NOT EXISTS`` /
    ``ADD COLUMN IF NOT EXISTS``. Caller owns the transaction; this function
    commits.
    """
    if not _MIGRATION_PATH.exists():
        raise FileNotFoundError(f"Detour v3 migration not found: {_MIGRATION_PATH}")
    sql = _MIGRATION_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log("osm-import-db", f"applied migration {_MIGRATION_PATH.name}")


def start_osm_import_run(
    conn,
    *,
    pbf_path: Optional[str],
    pbf_url: Optional[str],
    pbf_size_bytes: Optional[int],
    pbf_modified_at: Optional[datetime],
    status: str = "running",
) -> int:
    """
    Insert a row into ``osm_import_runs`` and return its ``id``.

    Caller owns the transaction; this function commits so the run id is
    visible immediately (importer crashes still leave an audit trail).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO osm_import_runs
              (pbf_path, pbf_url, pbf_size_bytes, pbf_modified_at, started_at, status)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            RETURNING id
            """,
            (pbf_path, pbf_url, pbf_size_bytes, pbf_modified_at, status),
        )
        row = cur.fetchone()
    conn.commit()
    try:
        run_id = int(row["id"])  # type: ignore[index]
    except Exception:
        run_id = int(row[0])
    log("osm-import-db", f"started osm_import_runs id={run_id} status={status}")
    return run_id


def finish_osm_import_run(
    conn,
    run_id: int,
    *,
    status: str,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Update the import run with ``finished_at = NOW()``, final status and stats.
    Caller may keep the transaction or commit; we commit here so partial
    success is recorded even if subsequent steps abort.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE osm_import_runs
            SET finished_at = NOW(),
                status = %s,
                stats_json = %s
            WHERE id = %s
            """,
            (status, Json(stats or {}), run_id),
        )
    conn.commit()
    log("osm-import-db", f"finished osm_import_runs id={run_id} status={status}")


def reset_v3_osm_import(
    conn,
    *,
    dangerously_truncate_shared: bool = False,
) -> Dict[str, Any]:
    """
    Provenance-scoped reset of detour v3 OSM import data.

    * v3-only tables (``osm_nodes``, ``osm_ways``, ``osm_way_nodes``,
      ``osm_segment_turns``): always ``TRUNCATE ... RESTART IDENTITY CASCADE``.
    * shared tables (``osm_road_segments``, ``osm_turn_restrictions``):
      ``DELETE WHERE import_run_id IS NOT NULL OR import_source = 'detour_v3_pbf_import'``
      so legacy rows (NULL/NULL) are preserved.

    When ``dangerously_truncate_shared`` is True, shared tables are
    ``TRUNCATE``'d in full (including legacy rows). The CLI requires an
    explicit confirmation flag to enable that path.

    Returns a stats dict with per-table counts.
    """
    stats: Dict[str, Any] = {
        "v3_only_truncated": list(V3_ONLY_OSM_TABLES),
        "shared_deleted": {},
        "shared_legacy_kept": {},
        "dangerous_full_truncate": bool(dangerously_truncate_shared),
    }

    with conn.cursor() as cur:
        if V3_ONLY_OSM_TABLES:
            cur.execute(
                "TRUNCATE TABLE "
                + ", ".join(V3_ONLY_OSM_TABLES)
                + " RESTART IDENTITY CASCADE"
            )

        for table in SHARED_OSM_TABLES:
            if dangerously_truncate_shared:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row = cur.fetchone()
                pre_count = int(row[0]) if row else 0
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                stats["shared_deleted"][table] = pre_count
                stats["shared_legacy_kept"][table] = 0
                log(
                    "osm-import-db",
                    f"DANGEROUS full truncate {table} rows_removed={pre_count}",
                )
                continue

            cur.execute(
                f"""
                DELETE FROM {table}
                WHERE import_run_id IS NOT NULL
                   OR import_source = %s
                """,
                (IMPORT_SOURCE_V3,),
            )
            deleted = cur.rowcount if cur.rowcount is not None else 0
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE import_run_id IS NULL
                  AND import_source IS NULL
                """
            )
            row = cur.fetchone()
            legacy_kept = int(row[0]) if row else 0
            stats["shared_deleted"][table] = int(deleted)
            stats["shared_legacy_kept"][table] = legacy_kept
            log(
                "osm-import-db",
                f"reset {table} deleted={deleted} legacy_kept={legacy_kept}",
            )

    conn.commit()
    return stats


def get_recent_import_runs(conn, limit: int = 10) -> List[Dict[str, Any]]:
    """Read the most recent ``osm_import_runs`` rows for status display."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, pbf_path, pbf_url, pbf_size_bytes, pbf_modified_at,
                   started_at, finished_at, status, stats_json
            FROM osm_import_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            stats_val = r["stats_json"]
        except Exception:
            stats_val = r[8]
        if isinstance(stats_val, str):
            try:
                stats_val = json.loads(stats_val)
            except Exception:
                pass
        out.append(
            {
                "id": _row_get(r, "id", 0),
                "pbf_path": _row_get(r, "pbf_path", 1),
                "pbf_url": _row_get(r, "pbf_url", 2),
                "pbf_size_bytes": _row_get(r, "pbf_size_bytes", 3),
                "pbf_modified_at": _iso_or_none(_row_get(r, "pbf_modified_at", 4)),
                "started_at": _iso_or_none(_row_get(r, "started_at", 5)),
                "finished_at": _iso_or_none(_row_get(r, "finished_at", 6)),
                "status": _row_get(r, "status", 7),
                "stats_json": stats_val,
            }
        )
    return out


def _row_get(row: Any, key: str, idx: int) -> Any:
    try:
        return row[key]
    except Exception:
        try:
            return row[idx]
        except Exception:
            return None


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


__all__ = [
    "IMPORT_SOURCE_V3",
    "V3_ONLY_OSM_TABLES",
    "SHARED_OSM_TABLES",
    "ensure_detour_v3_layer",
    "start_osm_import_run",
    "finish_osm_import_run",
    "reset_v3_osm_import",
    "get_recent_import_runs",
]
