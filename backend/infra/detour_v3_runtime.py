"""Startup helpers: active feed with v3 data, OSM import cohort for routing."""

from __future__ import annotations

from typing import Optional

from backend.infra.logging_utils import log


def resolve_detour_v3_import_run_id(conn) -> Optional[int]:
    """
    OSM segment cohort for v3 graph load / incident projection.

    Uses ``DETOUR_V3_IMPORT_RUN_ID`` when set; otherwise latest ``osm_import_runs`` row
    with ``status = 'success'``.
    """
    from backend.infra.config import DETOUR_V3_IMPORT_RUN_ID

    if DETOUR_V3_IMPORT_RUN_ID is not None:
        return int(DETOUR_V3_IMPORT_RUN_ID)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM osm_import_runs
            WHERE status = 'success'
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return int(row["id"] if hasattr(row, "keys") else row[0])


def _row_int(row, key: str = "id") -> int:
    return int(row[key] if hasattr(row, "keys") else row[0])


def ensure_v3_ready_feed_active(conn) -> int:
    """
    Point ``feed_versions.active`` at the feed with the most ``pattern_osm_segments`` rows.

    If the current active feed already has the highest count, leave it unchanged.
    Also marks stale ``osm_import_runs.status = 'running'`` older than 2 hours as ``failed``.
    Returns the active ``feed_id``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE osm_import_runs
            SET status = 'failed'
            WHERE status = 'running'
              AND started_at < NOW() - INTERVAL '2 hours'
            """
        )
        stale = cur.rowcount

        cur.execute(
            """
            SELECT feed_id, COUNT(*)::bigint AS n
            FROM pattern_osm_segments
            GROUP BY feed_id
            ORDER BY n DESC NULLS LAST
            LIMIT 1
            """
        )
        best = cur.fetchone()
        if not best or _row_int(best, "n") <= 0:
            cur.execute("SELECT id FROM feed_versions WHERE active = TRUE LIMIT 1")
            active = cur.fetchone()
            if active:
                fid = _row_int(active)
                log("db/feed", f"v3_feed_active feed_id={fid} (no pattern_osm_segments; kept current active)")
                return fid
            cur.execute("SELECT id FROM feed_versions ORDER BY fetched_at DESC NULLS LAST LIMIT 1")
            latest = cur.fetchone()
            if not latest:
                raise RuntimeError("No rows in feed_versions")
            fid = _row_int(latest)
            log("db/feed", f"v3_feed_active feed_id={fid} (fallback latest feed_versions.id)")
            return fid

        best_fid = _row_int(best, "feed_id")
        best_n = _row_int(best, "n")

        cur.execute("SELECT id FROM feed_versions WHERE active = TRUE ORDER BY fetched_at DESC LIMIT 1")
        active_row = cur.fetchone()
        current_fid = _row_int(active_row) if active_row else None

        if current_fid == best_fid:
            log(
                "db/feed",
                f"v3_feed_active feed_id={best_fid} pattern_osm_segments={best_n:,} (unchanged)",
            )
            return best_fid

        cur.execute("UPDATE feed_versions SET active = FALSE")
        cur.execute("UPDATE feed_versions SET active = TRUE WHERE id = %s", (best_fid,))
        log(
            "db/feed",
            f"v3_feed_active switched feed_id {current_fid} -> {best_fid} "
            f"pattern_osm_segments={best_n:,} stale_import_runs_cleared={stale}",
        )
        return best_fid


def bootstrap_detour_v3_runtime(conn) -> None:
    """Called once at API startup before graph warmup."""
    feed_id = ensure_v3_ready_feed_active(conn)
    run_id = resolve_detour_v3_import_run_id(conn)
    conn.commit()
    log(
        "detour_v3/bootstrap",
        f"feed_id={feed_id} osm_import_run_id={run_id} "
        f"engine_default=v3_when_DETOUR_ENGINE_set",
    )


__all__ = [
    "bootstrap_detour_v3_runtime",
    "ensure_v3_ready_feed_active",
    "resolve_detour_v3_import_run_id",
]
