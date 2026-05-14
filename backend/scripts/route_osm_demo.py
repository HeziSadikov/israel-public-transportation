"""
M3 demo: A* shortest path between OSM junction node ids with optional polygon
avoidance overlay (blocked segments + via-node pivots inside polygon).

Requires ``osm_road_segments``, ``osm_segment_turns``, ``osm_nodes``. PostGIS.

Examples::

    python -m backend.scripts.route_osm_demo \\
      --from-node N1 --to-node N2

    python -m backend.scripts.route_osm_demo \\
      --from-node N1 --to-node N2 --avoid-wkt-file scratch/blockage.txt

    python -m backend.scripts.route_osm_demo \\
      --from-node N1 --to-node N2 --avoid-geojson-file scratch/poly.json

The WKT file is raw text ``POLYGON((lon lat, ... ))`` / ``MULTIPOLYGON``.
The GeoJSON file may be a Feature, Polygon geometry, or single-feature FeatureCollection.

"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import psycopg2

from backend.infra import db_access as db
from backend.infra.logging_utils import ensure_cli_action_logging, log
from backend.routing.astar import astar_shortest_path
from backend.routing.blockers import (
    load_polygon_wkt_from_geojson_file,
    project_polygon_to_bans,
)
from backend.routing.road_graph_loader import load_road_graph


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="route_osm_demo",
        description=(
            "Detour-v3 segment-level A* between junction node IDs "
            "(``osm_road_segments`` + ``osm_segment_turns``). "
            "Optional polygon blockage overlay (M3)."
        ),
    )
    p.add_argument("--from-node", type=int, required=True, dest="start_node")
    p.add_argument("--to-node", type=int, required=True, dest="goal_node")
    p.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL / db_access.DB_URL).",
    )
    p.add_argument(
        "--segment-import-run-id",
        type=int,
        default=None,
        help=(
            "Restrict routing to segments / turn cohort with this import run id "
            "(match ``import_osm_pbf --with-segments/--with-turns``)."
        ),
    )

    grp = p.add_mutually_exclusive_group()
    grp.add_argument(
        "--avoid-wkt-file",
        metavar="PATH",
        help="Plain-text OGC polygon WKT in lon/lat (SRID 4326 semantics).",
    )
    grp.add_argument(
        "--avoid-geojson-file",
        metavar="PATH",
        help="GeoJSON Polygon / MultiPolygon (Feature or FeatureCollection okay).",
    )

    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    ensure_cli_action_logging()
    args = _parse_args(argv)
    dsn = args.database_url or db.DB_URL

    polygon_wkt: Optional[str] = None
    if args.avoid_wkt_file:
        polygon_wkt = Path(args.avoid_wkt_file).read_text(encoding="utf-8").strip()
        if not polygon_wkt:
            sys.stderr.write("[route_osm_demo] avoidance WKT file is empty.\n")
            return 2
    elif args.avoid_geojson_file:
        polygon_wkt = load_polygon_wkt_from_geojson_file(args.avoid_geojson_file)

    conn = psycopg2.connect(dsn)
    try:
        graph = load_road_graph(
            conn, segment_import_run_id=args.segment_import_run_id
        )

        bans = None
        if polygon_wkt is not None:
            bans = project_polygon_to_bans(
                conn,
                polygon_wkt,
                segment_import_run_id=args.segment_import_run_id,
            )
            path = astar_shortest_path(
                graph,
                args.start_node,
                args.goal_node,
                banned_segment_ids=bans.banned_segment_ids,
                banned_turn_pairs=bans.banned_turn_pairs,
            )
        else:
            path = astar_shortest_path(graph, args.start_node, args.goal_node)

        meters = None
        if path:
            meters = sum(graph.segments[sid].length_m for sid in path)

        payload = {
            "ok": path is not None,
            "start_node": args.start_node,
            "goal_node": args.goal_node,
            "avoid_polygon_used": polygon_wkt is not None,
            "path_segment_ids": path,
            "estimated_length_m": meters,
            "segments_loaded": len(graph.segments),
        }
        if bans is not None:
            payload["bans_summary"] = bans.to_dict()

        print(json.dumps(payload, indent=2), flush=True)

        if path is None:
            log(
                "route-demo",
                f"no route start={args.start_node} goal={args.goal_node} "
                f"segments_loaded={len(graph.segments)} avoid={polygon_wkt is not None}",
            )
            return 4

        log(
            "route-demo",
            (
                f"route hops={len(path)} "
                + (
                    f"est_len_m={meters:.1f}"
                    if meters is not None
                    else "est_len_m=null"
                )
                + f" avoid={'yes' if polygon_wkt else 'no'}"
            ),
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
