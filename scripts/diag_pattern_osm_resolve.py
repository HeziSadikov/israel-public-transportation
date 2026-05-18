"""One-off: compare Valhalla trace triple vs osm_road_segments (run from repo root)."""
from __future__ import annotations

import os
import sys

import psycopg2
from psycopg2.extras import DictCursor

from backend.bus_corridor.trace_segment_resolve import (
    dedupe_consecutive_trace_edges,
    flatten_per_leg_trace_edges,
    resolve_trace_edges_to_segment_ids,
    trace_edge_has_osm_endpoint_ids,
    trace_edge_resolution_triple,
)
from backend.domain.detour_physical.edge_matcher import (
    match_full_shape_to_osm_edges,
    trace_pattern_split_to_legs,
)
from backend.infra import db_access as db
from backend.infra.db_access import get_shape_lines_bulk
from backend.infra.osm_import_db import IMPORT_SOURCE_V3


def main() -> int:
    os.environ.setdefault("VALHALLA_URL", "http://127.0.0.1:8002")
    os.environ.setdefault("VALHALLA_TRACE_ATTRIBUTES_ENABLED", "1")
    conn = psycopg2.connect(
        os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/israel_gtfs"),
        cursor_factory=DictCursor,
    )
    fid = 21
    want_pid = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
    with conn.cursor() as cur:
        if want_pid:
            cur.execute(
                """
                SELECT pattern_id, repr_shape_id
                FROM patterns
                WHERE feed_id = %s AND pattern_id = %s
                """,
                (fid, want_pid),
            )
        else:
            cur.execute(
                """
                SELECT pattern_id, repr_shape_id
                FROM patterns
                WHERE feed_id = %s AND repr_shape_id IS NOT NULL AND repr_shape_id != ''
                LIMIT 1
                """,
                (fid,),
            )
        row = cur.fetchone()
    if not row:
        print("no pattern with shape")
        return 1
    pid = row["pattern_id"]
    shape_id = row["repr_shape_id"]
    line = get_shape_lines_bulk(fid, [str(shape_id)], conn=conn).get(str(shape_id))
    print(f"pattern_id={pid} shape_id={shape_id} line_empty={line is None or line.is_empty}")
    res = match_full_shape_to_osm_edges(line, costing="bus")
    edges = res.edge_records if res and res.success else []
    print(f"valhalla_edges={len(edges)}")
    if not edges:
        return 1
    e = edges[0]
    print(
        "edge0",
        {
            k: e.get(k)
            for k in (
                "way_id",
                "begin_osm_node_id",
                "end_osm_node_id",
                "begin_node_id",
                "end_node_id",
            )
        },
    )
    t = trace_edge_resolution_triple(e, 0)
    print(f"triple={t}")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM osm_road_segments WHERE osm_way_id = %s",
            (t[0],),
        )
        print(f"segments_with_way_id={cur.fetchone()['n']}")
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM osm_road_segments
            WHERE osm_way_id = %s AND from_node_id = %s AND to_node_id = %s
            """,
            t,
        )
        print(f"segments_exact_triple={cur.fetchone()['n']}")
        cur.execute(
            """
            SELECT segment_id, from_node_id, to_node_id, import_source
            FROM osm_road_segments
            WHERE osm_way_id = %s
            LIMIT 3
            """,
            (t[0],),
        )
        for r in cur.fetchall():
            print("sample_segment", dict(r))
    # Same path as match_patterns_to_osm (per-leg trace)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT repr_trip_id FROM patterns WHERE feed_id = %s AND pattern_id = %s",
            (fid, pid),
        )
        repr_trip_id = cur.fetchone()["repr_trip_id"]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT stop_id, stop_sequence, arrival_time, departure_time
            FROM stop_times
            WHERE feed_id = %s AND trip_id = %s
            ORDER BY stop_sequence
            """,
            (fid, repr_trip_id),
        )
        stops = [dict(r) for r in cur.fetchall()]
    per_leg, notes = trace_pattern_split_to_legs(line, stops, costing="bus")
    flat = flatten_per_leg_trace_edges(per_leg)
    print(f"per_leg_trace notes={notes} flat_edges={len(flat) if flat else 0}")
    if flat:
        flat = dedupe_consecutive_trace_edges(flat)
        triples = [trace_edge_resolution_triple(e, i) for i, e in enumerate(flat[:5])]
        print("first5_triples", triples)
        triple_map = db.fetch_osm_road_segments_by_way_endpoint_triples(
            [trace_edge_resolution_triple(e, i) for i, e in enumerate(flat)],
            conn=conn,
        )
        way_ids = sorted(
            {
                int(e.get("way_id") or 0)
                for e in flat
                if not trace_edge_has_osm_endpoint_ids(e) and int(e.get("way_id") or 0) > 0
            }
        )
        way_map = db.fetch_osm_road_segments_by_way_ids(way_ids, conn=conn) if way_ids else {}
        ids, unresolved = resolve_trace_edges_to_segment_ids(
            flat,
            triple_map,
            v3_import_source=IMPORT_SOURCE_V3,
            way_to_rows=way_map,
        )
        print(f"resolve ids={None if ids is None else len(ids)} unresolved_hint={unresolved}")
        if flat:
            e0 = flat[0]
            t0 = trace_edge_resolution_triple(e0, 0)
            c0 = triple_map.get(t0) or []
            print(f"edge0 way_id raw={e0.get('way_id')!r} triple={t0} candidates={len(c0)}")
            if c0:
                from backend.bus_corridor.trace_segment_resolve import pick_segment_for_trace_edge

                sid = pick_segment_for_trace_edge(
                    e0, 0, c0, v3_import_source=IMPORT_SOURCE_V3
                )
                print(f"pick_segment edge0 -> {sid}")
            # find first edge that fails
            for i, e in enumerate(flat[:20]):
                t = trace_edge_resolution_triple(e, i)
                c = triple_map.get(t) or []
                if not c:
                    w = int(e.get("way_id") or 0)
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT COUNT(*) n,
                                   COUNT(*) FILTER (WHERE from_node_id > 0) n_real
                            FROM osm_road_segments WHERE osm_way_id = %s
                            """,
                            (w,),
                        )
                        stats = dict(cur.fetchone())
                    print(
                        f"first_missing_at i={i} triple={t} way={w} "
                        f"db_segments={stats}"
                    )
                    break
        # count how many flat edges fail triple but have way rows
        miss = hit = 0
        for i, e in enumerate(flat):
            t = trace_edge_resolution_triple(e, i)
            if triple_map.get(t):
                hit += 1
            else:
                w = int(e.get("way_id") or 0)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) n FROM osm_road_segments WHERE osm_way_id=%s",
                        (w,),
                    )
                    if int(cur.fetchone()["n"] or 0) > 0:
                        miss += 1
        print(f"triple_hit={hit} way_only_possible={miss} total={len(flat)}")
        no_db: set[int] = set()
        for e in flat:
            w = int(e.get("way_id") or 0)
            if w <= 0:
                no_db.add(w)
                continue
            t = trace_edge_resolution_triple(e, 0)
            if triple_map.get(t):
                continue
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM osm_road_segments WHERE osm_way_id=%s LIMIT 1",
                    (w,),
                )
                if not cur.fetchone():
                    no_db.add(w)
        print(f"distinct_ways_with_no_db_segment={len(no_db)} sample={sorted(no_db)[:8]}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
