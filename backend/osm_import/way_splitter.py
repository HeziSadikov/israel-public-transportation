"""
Detour v3 way splitter.

A raw OSM way may traverse many nodes. For routing we need one directed edge
per non-branching run between *split nodes*:

* every node shared by 2+ ways (intersection),
* every terminal node (first / last node of a way),
* every node used as a ``via`` node in a v3-provenance turn restriction.

This module materializes the split-node set as a temp table and exposes the
SQL fragments that :mod:`build_directed_segments` joins against to emit one
piece (un-directed geometry) per (way_id, from_seq, to_seq) span.

Direction (forward/backward emission) and provenance writes live in
:mod:`build_directed_segments` — this module is geometry-only.
"""

from __future__ import annotations

from backend.infra.logging_utils import log


# Temp table created at the start of a build_directed_segments run.
SPLIT_NODES_TMP = "tmp_v3_split_nodes"

# Temp table that materializes one row per (way_id, from_seq, to_seq) piece
# with its assembled LineString and endpoint metadata. We compute it as a
# temp table (instead of inline CTE) because the orchestrator needs to query
# it multiple times for piece counts, geometry verification, and inserts.
PIECES_TMP = "tmp_v3_way_pieces"


def create_split_nodes_temp_table(cur) -> int:
    """
    Build ``tmp_v3_split_nodes(node_id BIGINT PRIMARY KEY)`` for the current
    session. Returns the row count.

    Inputs (read-only):
      * ``osm_way_nodes`` — written by the PBF importer in M1B.
      * ``osm_turn_restrictions`` — but only v3-provenance rows; legacy rows
        do not contribute via-nodes here so we don't import constraints we
        cannot reason about.
    """
    cur.execute(f"DROP TABLE IF EXISTS {SPLIT_NODES_TMP}")
    cur.execute(
        f"""
        CREATE TEMP TABLE {SPLIT_NODES_TMP} (
            node_id BIGINT PRIMARY KEY
        ) ON COMMIT DROP
        """
    )

    # 1. Intersection nodes (degree >= 2 across ways).
    cur.execute(
        f"""
        INSERT INTO {SPLIT_NODES_TMP} (node_id)
        SELECT node_id
        FROM (
            SELECT node_id, COUNT(DISTINCT way_id) AS deg
            FROM osm_way_nodes
            GROUP BY node_id
        ) x
        WHERE deg >= 2
        ON CONFLICT (node_id) DO NOTHING
        """
    )

    # 2. Terminal nodes (first / last seq per way).
    cur.execute(
        f"""
        INSERT INTO {SPLIT_NODES_TMP} (node_id)
        SELECT wn.node_id
        FROM osm_way_nodes wn
        JOIN (
            SELECT way_id, MIN(seq) AS s_min, MAX(seq) AS s_max
            FROM osm_way_nodes
            GROUP BY way_id
        ) bounds USING (way_id)
        WHERE wn.seq = bounds.s_min OR wn.seq = bounds.s_max
        ON CONFLICT (node_id) DO NOTHING
        """
    )

    # 3. Via-nodes from v3-provenance turn restrictions (skip legacy / sentinel 0).
    cur.execute(
        f"""
        INSERT INTO {SPLIT_NODES_TMP} (node_id)
        SELECT DISTINCT via_node_id
        FROM osm_turn_restrictions
        WHERE import_source = 'detour_v3_pbf_import'
          AND via_node_id IS NOT NULL
          AND via_node_id > 0
        ON CONFLICT (node_id) DO NOTHING
        """
    )

    cur.execute(f"SELECT COUNT(*) FROM {SPLIT_NODES_TMP}")
    row = cur.fetchone()
    n = int(row[0]) if row else 0
    log("way-splitter", f"split nodes materialized count={n} table={SPLIT_NODES_TMP}")
    return n


def create_pieces_temp_table(cur) -> int:
    """
    Build ``tmp_v3_way_pieces`` with one row per piece:

      way_id, from_seq, to_seq, from_node_id, to_node_id, geom (LineString 4326)

    The piece spans the rows of ``osm_way_nodes`` where seq is in
    ``[from_seq, to_seq]`` and both endpoints are split nodes. Each row in
    :data:`SPLIT_NODES_TMP` along a way becomes the boundary between two
    pieces. Returns piece count.
    """
    cur.execute(f"DROP TABLE IF EXISTS {PIECES_TMP}")
    cur.execute(
        f"""
        CREATE TEMP TABLE {PIECES_TMP} (
            way_id            BIGINT NOT NULL,
            from_seq          INT NOT NULL,
            to_seq            INT NOT NULL,
            from_node_id      BIGINT NOT NULL,
            to_node_id        BIGINT NOT NULL,
            geom              GEOMETRY(LINESTRING, 4326) NOT NULL,
            length_m          DOUBLE PRECISION,
            heading_start_deg DOUBLE PRECISION,
            heading_end_deg   DOUBLE PRECISION,
            PRIMARY KEY (way_id, from_seq, to_seq)
        ) ON COMMIT DROP
        """
    )

    # Build (way_id, from_seq, to_seq, from_node_id, to_node_id) by pairing
    # consecutive split-node entries within each way. Then assemble the
    # LineString from the full osm_way_nodes range between those seqs.
    cur.execute(
        f"""
        WITH splits_in_way AS (
            SELECT wn.way_id, wn.seq, wn.node_id
            FROM osm_way_nodes wn
            JOIN {SPLIT_NODES_TMP} s USING (node_id)
        ),
        piece_bounds AS (
            SELECT
                way_id,
                seq AS from_seq,
                node_id AS from_node_id,
                LEAD(seq) OVER (PARTITION BY way_id ORDER BY seq) AS to_seq,
                LEAD(node_id) OVER (PARTITION BY way_id ORDER BY seq) AS to_node_id
            FROM splits_in_way
        ),
        labeled_rows AS (
            SELECT
                pb.way_id, pb.from_seq, pb.to_seq,
                pb.from_node_id, pb.to_node_id,
                wn.seq, n.geom AS pt
            FROM piece_bounds pb
            JOIN osm_way_nodes wn
              ON wn.way_id = pb.way_id
             AND wn.seq BETWEEN pb.from_seq AND pb.to_seq
            JOIN osm_nodes n ON n.node_id = wn.node_id
            WHERE pb.to_seq IS NOT NULL
              AND pb.to_seq > pb.from_seq
        ),
        assembled AS (
            SELECT
                way_id, from_seq, to_seq, from_node_id, to_node_id,
                ST_MakeLine(pt ORDER BY seq) AS geom
            FROM labeled_rows
            GROUP BY way_id, from_seq, to_seq, from_node_id, to_node_id
        )
        INSERT INTO {PIECES_TMP}
            (way_id, from_seq, to_seq, from_node_id, to_node_id, geom,
             length_m, heading_start_deg, heading_end_deg)
        SELECT
            way_id, from_seq, to_seq, from_node_id, to_node_id, geom,
            ST_Length(geom::geography) AS length_m,
            CASE
              WHEN ST_NPoints(geom) >= 2 THEN
                DEGREES(ST_Azimuth(ST_PointN(geom, 1), ST_PointN(geom, 2)))
              ELSE NULL
            END AS heading_start_deg,
            CASE
              WHEN ST_NPoints(geom) >= 2 THEN
                DEGREES(ST_Azimuth(
                    ST_PointN(geom, ST_NPoints(geom) - 1),
                    ST_PointN(geom, ST_NPoints(geom))
                ))
              ELSE NULL
            END AS heading_end_deg
        FROM assembled
        WHERE ST_NPoints(geom) >= 2
          AND ST_IsValid(geom)
        """
    )

    cur.execute(f"SELECT COUNT(*) FROM {PIECES_TMP}")
    row = cur.fetchone()
    n = int(row[0]) if row else 0
    log("way-splitter", f"pieces materialized count={n} table={PIECES_TMP}")
    return n


__all__ = [
    "SPLIT_NODES_TMP",
    "PIECES_TMP",
    "create_split_nodes_temp_table",
    "create_pieces_temp_table",
]
