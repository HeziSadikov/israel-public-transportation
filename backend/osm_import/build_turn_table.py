"""
Detour M2 — precomputed legal turn table.

Materializes ``osm_segment_turns`` from:

* directed adjacent pairs at each junction (
  ``osm_road_segments.to_node_id = next.from_node_id``),
* ``turn_angle_deg`` and ``is_u_turn`` heuristic from headings,
* OSM restrictions with ``via_node_id > 0``:

  * ``no_*`` (and ``no_entry`` / ``no_exit``) — forbid maneuvers matching
    ``from_way → to_way`` at ``via``.
  * ``only_*`` — forbid every maneuver from segments on ``from_way`` ending
    at ``via``, except exits onto ``to_way``.

Restrictions with ``via_node_id = 0`` (via-way only) are unchanged by SQL
until M4+ refinement.

Truncate + rebuild whenever ``osm_road_segments`` changes (
``reset_v3_osm_import`` already truncates this table).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from psycopg2.extensions import cursor as Cursor

from backend.infra.logging_utils import log


_INSERT_ADJACENCY_TEMPLATE = """
INSERT INTO osm_segment_turns (
  from_segment_id,
  via_node_id,
  to_segment_id,
  turn_angle_deg,
  is_forbidden,
  is_only_restricted,
  restriction_id,
  is_u_turn
)
SELECT
  i.segment_id AS from_segment_id,
  i.to_node_id AS via_node_id,
  o.segment_id AS to_segment_id,
  CASE
    WHEN i.heading_end_deg IS NULL OR o.heading_start_deg IS NULL THEN NULL
    ELSE (
      MOD(
        (o.heading_start_deg::numeric - i.heading_end_deg::numeric + 540),
        360
      ) - 180
    )::double precision
  END AS turn_angle_deg,
  FALSE AS is_forbidden,
  FALSE AS is_only_restricted,
  NULL::BIGINT AS restriction_id,
  CASE
    WHEN i.heading_end_deg IS NOT NULL AND o.heading_start_deg IS NOT NULL
        AND ABS(
              (
                MOD(
                  (
                    o.heading_start_deg::numeric - i.heading_end_deg::numeric + 540
                  ),
                  360
                ) - 180
              )
            ) >= 145
      THEN TRUE
    ELSE FALSE
  END AS is_u_turn
FROM osm_road_segments i
JOIN osm_road_segments o
  ON i.to_node_id = o.from_node_id
 AND i.segment_id <> o.segment_id
{segment_filter};
"""


_UPDATE_NO_RESTRICTIONS = """
UPDATE osm_segment_turns AS t
SET
  is_forbidden = TRUE,
  restriction_id = r.id,
  is_only_restricted = FALSE
FROM osm_turn_restrictions AS r,
     osm_road_segments AS fi,
     osm_road_segments AS toe
WHERE fi.segment_id = t.from_segment_id
  AND toe.segment_id = t.to_segment_id
  AND fi.osm_way_id = r.from_way_id
  AND fi.to_node_id = r.via_node_id
  AND toe.from_node_id = r.via_node_id
  AND toe.osm_way_id = r.to_way_id
  AND r.via_node_id IS NOT NULL AND r.via_node_id <> 0
  AND (
    r.restriction_type LIKE 'no_%%'
    OR r.restriction_type IN ('no_entry', 'no_exit')
  )
  AND r.restriction_type NOT LIKE 'only_%%'
"""


_UPDATE_ONLY_RESTRICTIONS = """
UPDATE osm_segment_turns AS t
SET
  is_forbidden = TRUE,
  restriction_id = r.id,
  is_only_restricted = TRUE
FROM osm_turn_restrictions AS r,
     osm_road_segments AS fi,
     osm_road_segments AS toe
WHERE fi.segment_id = t.from_segment_id
  AND toe.segment_id = t.to_segment_id
  AND fi.osm_way_id = r.from_way_id
  AND fi.to_node_id = r.via_node_id
  AND toe.from_node_id = r.via_node_id
  AND r.via_node_id IS NOT NULL AND r.via_node_id <> 0
  AND r.restriction_type LIKE 'only_%%'
  AND toe.osm_way_id <> r.to_way_id
"""


_COUNT_LEGAL_STATS = """
SELECT
  SUM(CASE WHEN is_forbidden THEN 1 ELSE 0 END)::BIGINT AS n_forbidden,
  SUM(CASE WHEN NOT is_forbidden THEN 1 ELSE 0 END)::BIGINT AS n_legal
FROM osm_segment_turns
"""


@dataclass
class SegmentTurnStats:
    turns_inserted: int = 0
    no_restrictions_applied: int = 0
    only_restrictions_applied: int = 0
    forbidden_turns: int = 0
    legal_turns: int = 0
    phase_elapsed_s: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "turns_inserted": self.turns_inserted,
            "no_restrictions_applied": self.no_restrictions_applied,
            "only_restrictions_applied": self.only_restrictions_applied,
            "forbidden_turns": self.forbidden_turns,
            "legal_turns": self.legal_turns,
            "phase_elapsed_s": dict(self.phase_elapsed_s),
        }


def build_segment_turns(
    conn,
    *,
    segment_import_run_id: Optional[int] = None,
) -> SegmentTurnStats:
    """
    Rebuild ``osm_segment_turns`` from ``osm_road_segments`` + restrictions.

    * ``segment_import_run_id`` — when set, only pairs where **both** segments
      have this ``import_run_id``. ``None`` = entire graph currently in table.
    """
    stats = SegmentTurnStats()
    cur: Cursor

    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("TRUNCATE osm_segment_turns")
        stats.phase_elapsed_s["truncate"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        seg_filter = ""
        params: tuple = ()
        if segment_import_run_id is not None:
            seg_filter = "WHERE i.import_run_id = %s AND o.import_run_id = %s"
            params = (segment_import_run_id, segment_import_run_id)

        sql_ins = _INSERT_ADJACENCY_TEMPLATE.format(segment_filter=seg_filter)
        if params:
            cur.execute(sql_ins, params)
        else:
            cur.execute(sql_ins)

        stats.turns_inserted = cur.rowcount if cur.rowcount is not None else 0
        stats.phase_elapsed_s["insert_adjacency"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        cur.execute(_UPDATE_NO_RESTRICTIONS)
        stats.no_restrictions_applied = cur.rowcount if cur.rowcount is not None else 0
        stats.phase_elapsed_s["apply_no"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        cur.execute(_UPDATE_ONLY_RESTRICTIONS)
        stats.only_restrictions_applied = cur.rowcount if cur.rowcount is not None else 0
        stats.phase_elapsed_s["apply_only"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        cur.execute(_COUNT_LEGAL_STATS)
        row = cur.fetchone()
        if row:
            try:
                stats.forbidden_turns = int(row[0] or 0)
                stats.legal_turns = int(row[1] or 0)
            except Exception:
                stats.forbidden_turns = int(row["n_forbidden"] or 0)
                stats.legal_turns = int(row["n_legal"] or 0)
        stats.phase_elapsed_s["count"] = time.perf_counter() - t0

    conn.commit()
    log(
        "build-turn-table",
        (
            "done "
            f"turns_inserted={stats.turns_inserted} "
            f"no_updates={stats.no_restrictions_applied} "
            f"only_updates={stats.only_restrictions_applied} "
            f"forbidden_turns={stats.forbidden_turns} legal_turns={stats.legal_turns}"
        ),
    )
    return stats


__all__ = ["build_segment_turns", "SegmentTurnStats"]
