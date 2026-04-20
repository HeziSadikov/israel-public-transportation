"""
Read-only status of pattern_edge / pattern_edge_match_summary for the active feed.

Does not call Valhalla or modify the database.

  python -m backend.scripts.status_pattern_edge_matches
  python -m backend.scripts.status_pattern_edge_matches --match-version edge_matcher_v2_pattern_trace
"""

from __future__ import annotations

import argparse
import sys

from backend.infra import db_access as db


def _connect(database_url: str):
    import psycopg2
    from psycopg2.extras import DictCursor

    return psycopg2.connect(database_url, cursor_factory=DictCursor)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Report pattern physical-layer completion from Postgres (read-only)."
    )
    ap.add_argument("--database-url", default=None, help="Postgres URL (default DATABASE_URL)")
    ap.add_argument(
        "--match-version",
        default="edge_matcher_v2_pattern_trace",
        help="match_version to compare on pattern_edge_match_summary (default: backfill default).",
    )
    args = ap.parse_args(argv)

    database_url = args.database_url or db.DB_URL
    mv = str(args.match_version)

    conn = _connect(database_url)
    try:
        feed_id = db.get_active_feed_id(conn)
        fv = db.get_active_feed_version_key(conn)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int AS c FROM patterns WHERE feed_id = %s", (feed_id,))
            patterns_in_feed = int(cur.fetchone()["c"])

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT pattern_id)::int AS c
                FROM pattern_edge
                WHERE feed_version = %s
                """,
                (fv,),
            )
            patterns_with_edges = int(cur.fetchone()["c"])

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int AS c FROM pattern_edge WHERE feed_version = %s
                """,
                (fv,),
            )
            leg_rows = int(cur.fetchone()["c"])

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE s.pattern_edge_id IS NULL)::int AS no_summary,
                  COUNT(*) FILTER (
                    WHERE s.pattern_edge_id IS NOT NULL
                    AND s.match_version IS NOT DISTINCT FROM %s
                    AND s.is_ambiguous = FALSE
                  )::int AS ok_clean,
                  COUNT(*) FILTER (
                    WHERE s.pattern_edge_id IS NOT NULL
                    AND s.match_version IS NOT DISTINCT FROM %s
                    AND s.is_ambiguous IS TRUE
                  )::int AS ok_ambiguous,
                  COUNT(*) FILTER (
                    WHERE s.pattern_edge_id IS NOT NULL
                    AND (s.match_version IS DISTINCT FROM %s OR s.match_version IS NULL)
                  )::int AS wrong_or_null_version
                FROM pattern_edge pe
                LEFT JOIN pattern_edge_match_summary s ON s.pattern_edge_id = pe.pattern_edge_id
                WHERE pe.feed_version = %s
                """,
                (mv, mv, mv, fv),
            )
            row = cur.fetchone()
            no_summary = int(row["no_summary"])
            ok_clean = int(row["ok_clean"])
            ok_ambiguous = int(row["ok_ambiguous"])
            wrong_mv = int(row["wrong_or_null_version"])

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int AS c
                FROM patterns p
                WHERE p.feed_id = %s
                  AND NOT EXISTS (
                    SELECT 1 FROM pattern_edge pe
                    WHERE pe.feed_version = %s AND pe.pattern_id = p.pattern_id
                  )
                """,
                (feed_id, fv),
            )
            patterns_no_physical_rows = int(cur.fetchone()["c"])

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int AS c
                FROM (
                  SELECT pe.pattern_id
                  FROM pattern_edge pe
                  LEFT JOIN pattern_edge_match_summary s ON s.pattern_edge_id = pe.pattern_edge_id
                  WHERE pe.feed_version = %s
                  GROUP BY pe.pattern_id
                  HAVING COUNT(*) = SUM(
                    CASE
                      WHEN s.match_version IS NOT DISTINCT FROM %s AND COALESCE(s.is_ambiguous, FALSE) = FALSE
                      THEN 1 ELSE 0
                    END
                  )
                ) t
                """,
                (fv, mv),
            )
            patterns_all_legs_clean = int(cur.fetchone()["c"])

    finally:
        conn.close()

    print(f"active_feed_id={feed_id}")
    print(f"feed_version_key={fv[:24]}...")
    print(f"match_version_checked={mv!r}")
    print("")
    print("GTFS patterns table:")
    print(f"  patterns_total={patterns_in_feed}")
    print(f"  patterns_with_no_pattern_edge_rows={patterns_no_physical_rows}")
    print(f"  patterns_with_at_least_one_pattern_edge_row={patterns_with_edges}")
    print("")
    print("pattern_edge rows (legs) for this feed_version:")
    print(f"  leg_rows_total={leg_rows}")
    print(f"  legs_no_summary_row={no_summary}")
    print(f"  legs_summary_match_version_ok_and_not_ambiguous={ok_clean}")
    print(f"  legs_summary_match_version_but_ambiguous={ok_ambiguous}")
    print(f"  legs_summary_wrong_or_null_match_version={wrong_mv}")
    print("")
    print("Per-pattern completion (only patterns that already have pattern_edge rows):")
    print(
        f"  patterns_where_every_leg_is_{mv}_and_is_ambiguous_false={patterns_all_legs_clean}"
    )
    print("")
    print(
        "Backfill will skip a pattern when every expected leg is 'clean' (same logic as "
        "list_pattern_edge_pairs with accept_ambiguous=false). "
        "Patterns with only GTFS rows and no pattern_edge are not counted in the last line."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
