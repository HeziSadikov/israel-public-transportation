"""
Detour v3: directed segment builder.

Reads:
  * ``osm_ways`` / ``osm_way_nodes`` / ``osm_nodes`` (M1B output)
  * ``osm_turn_restrictions`` (M1B, for via-node split markers)

Writes:
  * ``osm_road_segments`` (one row per directed traversal of each piece,
    with ``length_m``, ``heading_start_deg``, ``heading_end_deg``, full tag
    set, ``tags_json``, and v3 provenance
    (``import_run_id`` + ``import_source``)). The ``heading_*_deg`` columns
    are shared with the physical layer (see
    ``ensure_pattern_physical_layer.sql``).

After writing segments, the run also validates that each v3-provenance
``osm_turn_restrictions`` row resolves against the freshly written segments
(``from_way_id`` and ``to_way_id`` both have at least one segment in this
run, and the via-node still exists in ``osm_nodes``). The counts are
reported back to the orchestrator and stored in
``osm_import_runs.stats_json``.

This module does **not** materialize segment-to-segment turns
(``osm_segment_turns``). That is **M2**.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from backend.infra.logging_utils import log
from backend.infra.osm_import_db import IMPORT_SOURCE_V3
from backend.osm_import.way_splitter import (
    PIECES_TMP,
    create_pieces_temp_table,
    create_split_nodes_temp_table,
)


@dataclass
class SegmentBuildStats:
    pieces: int = 0
    segments_forward: int = 0
    segments_backward: int = 0
    segments_total: int = 0
    restrictions_total: int = 0
    restrictions_resolved: int = 0
    restrictions_unresolved: int = 0
    phase_elapsed_s: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pieces": self.pieces,
            "segments_forward": self.segments_forward,
            "segments_backward": self.segments_backward,
            "segments_total": self.segments_total,
            "restrictions_total": self.restrictions_total,
            "restrictions_resolved": self.restrictions_resolved,
            "restrictions_unresolved": self.restrictions_unresolved,
            "phase_elapsed_s": dict(self.phase_elapsed_s),
        }


def build_directed_segments(
    conn,
    *,
    import_run_id: int,
) -> SegmentBuildStats:
    """
    Run the full M1C pipeline against ``conn``: split nodes → pieces →
    directed ``osm_road_segments`` rows → restriction validation.

    Caller owns the transaction. We use a single connection-scoped transaction
    for the splitter + insert so segment writes appear atomically, then commit
    once. Temp tables are auto-dropped on COMMIT.
    """
    import time

    stats = SegmentBuildStats()

    with conn.cursor() as cur:
        # ----- phase: split nodes -----
        t0 = time.perf_counter()
        create_split_nodes_temp_table(cur)
        stats.phase_elapsed_s["split_nodes"] = time.perf_counter() - t0

        # ----- phase: pieces -----
        t0 = time.perf_counter()
        stats.pieces = create_pieces_temp_table(cur)
        stats.phase_elapsed_s["pieces"] = time.perf_counter() - t0

        # ----- phase: insert directed segments (forward + backward) -----
        t0 = time.perf_counter()
        # oneway interpretation:
        #   forward emission  when oneway IN ('yes','true','1') OR oneway IS NULL
        #                     OR junction IN ('roundabout','circular') with no
        #                     explicit oneway override.
        #   backward emission when oneway IN ('-1','reverse') OR oneway IS NULL
        #                     (and NOT a roundabout-implied one-way).
        # NOTE: ``oneway=no``, ``oneway=false``, ``oneway=0`` collapse to NULL
        # logic below (bidirectional).
        cur.execute(
            """
            INSERT INTO osm_road_segments
              (osm_way_id, from_node_id, to_node_id, geom, direction,
               highway, name, oneway, access, bus, psv,
               service, junction, tags_json,
               length_m, heading_start_deg, heading_end_deg,
               import_run_id, import_source)
            SELECT
                p.way_id,
                p.from_node_id,
                p.to_node_id,
                p.geom,
                'forward',
                ow.highway,
                ow.name,
                ow.oneway,
                ow.access,
                ow.bus,
                ow.psv,
                ow.service,
                ow.junction,
                ow.tags_json,
                p.length_m,
                p.heading_start_deg,
                p.heading_end_deg,
                %s,
                %s
            FROM tmp_v3_way_pieces p
            JOIN osm_ways ow ON ow.way_id = p.way_id
            WHERE NOT (
                LOWER(COALESCE(ow.oneway, '')) IN ('-1', 'reverse')
            )
            """,
            (import_run_id, IMPORT_SOURCE_V3),
        )
        stats.segments_forward = cur.rowcount if cur.rowcount is not None else 0

        cur.execute(
            """
            INSERT INTO osm_road_segments
              (osm_way_id, from_node_id, to_node_id, geom, direction,
               highway, name, oneway, access, bus, psv,
               service, junction, tags_json,
               length_m, heading_start_deg, heading_end_deg,
               import_run_id, import_source)
            SELECT
                p.way_id,
                p.to_node_id,
                p.from_node_id,
                ST_Reverse(p.geom),
                'backward',
                ow.highway,
                ow.name,
                ow.oneway,
                ow.access,
                ow.bus,
                ow.psv,
                ow.service,
                ow.junction,
                ow.tags_json,
                p.length_m,
                -- For the reversed direction, swap heading endpoints and
                -- add 180 deg (mod 360).
                CASE WHEN p.heading_end_deg IS NULL THEN NULL
                     ELSE MOD(p.heading_end_deg::numeric + 180, 360)::double precision
                END,
                CASE WHEN p.heading_start_deg IS NULL THEN NULL
                     ELSE MOD(p.heading_start_deg::numeric + 180, 360)::double precision
                END,
                %s,
                %s
            FROM tmp_v3_way_pieces p
            JOIN osm_ways ow ON ow.way_id = p.way_id
            WHERE
                -- Bidirectional ways and explicit oneway=-1/reverse get a backward edge.
                LOWER(COALESCE(ow.oneway, '')) NOT IN ('yes', 'true', '1')
                AND NOT (
                    LOWER(COALESCE(ow.junction, '')) IN ('roundabout', 'circular')
                    AND COALESCE(ow.oneway, '') = ''
                )
            """,
            (import_run_id, IMPORT_SOURCE_V3),
        )
        stats.segments_backward = cur.rowcount if cur.rowcount is not None else 0

        stats.segments_total = stats.segments_forward + stats.segments_backward
        stats.phase_elapsed_s["insert_directed_segments"] = time.perf_counter() - t0

        # ----- phase: validate restriction resolution -----
        t0 = time.perf_counter()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM osm_turn_restrictions
            WHERE import_run_id = %s
            """,
            (import_run_id,),
        )
        row = cur.fetchone()
        stats.restrictions_total = int(row[0]) if row else 0

        # A restriction "resolves" iff:
        #   - we have at least one segment from this run for from_way_id,
        #   - we have at least one segment from this run for to_way_id,
        #   - and either via_node_id (> 0) is a node we know about,
        #     or via_way_id is a way we have segments for.
        cur.execute(
            """
            WITH r AS (
                SELECT id, from_way_id, to_way_id, via_node_id, via_way_id
                FROM osm_turn_restrictions
                WHERE import_run_id = %s
            ),
            run_segments AS (
                SELECT DISTINCT osm_way_id
                FROM osm_road_segments
                WHERE import_run_id = %s
            ),
            run_nodes AS (
                SELECT DISTINCT from_node_id AS node_id FROM osm_road_segments
                WHERE import_run_id = %s
                UNION
                SELECT DISTINCT to_node_id FROM osm_road_segments
                WHERE import_run_id = %s
            ),
            resolved AS (
                SELECT r.id
                FROM r
                JOIN run_segments sf ON sf.osm_way_id = r.from_way_id
                JOIN run_segments st ON st.osm_way_id = r.to_way_id
                WHERE
                    (
                        r.via_node_id IS NOT NULL AND r.via_node_id > 0
                        AND EXISTS (SELECT 1 FROM run_nodes rn WHERE rn.node_id = r.via_node_id)
                    )
                    OR (
                        r.via_way_id IS NOT NULL
                        AND EXISTS (SELECT 1 FROM run_segments rv WHERE rv.osm_way_id = r.via_way_id)
                    )
            )
            SELECT COUNT(*) FROM resolved
            """,
            (import_run_id, import_run_id, import_run_id, import_run_id),
        )
        row = cur.fetchone()
        stats.restrictions_resolved = int(row[0]) if row else 0
        stats.restrictions_unresolved = max(
            0, stats.restrictions_total - stats.restrictions_resolved
        )
        stats.phase_elapsed_s["validate_restrictions"] = time.perf_counter() - t0

    conn.commit()
    log(
        "build-directed-segments",
        (
            f"done pieces={stats.pieces} "
            f"forward={stats.segments_forward} backward={stats.segments_backward} "
            f"total={stats.segments_total} "
            f"restr_total={stats.restrictions_total} "
            f"restr_resolved={stats.restrictions_resolved} "
            f"restr_unresolved={stats.restrictions_unresolved}"
        ),
    )
    return stats


__all__ = [
    "SegmentBuildStats",
    "build_directed_segments",
]
