"""
Detour M3 — polygon-based segment + turn overlays for avoidance routing.

``project_polygon_to_bans`` derives two ban sets against PostGIS:

* **Segment bans** — every ``segment_id`` whose ``osm_road_segments.geom``
  intersects the polygon (``ST_Intersects``, SRID 4326).
* **Turn bans** — every ``(from_segment_id, to_segment_id)`` row in
  ``osm_segment_turns`` whose ``via_node`` lies inside or on the polygon
  boundary (``.via_node.geom`` intersects polygon). These catch maneuvers
  whose pivot lies in the blockage even when both adjoining segment
  geometries only graze the polygon.

Legal routing is unchanged in the database; bans are consumed at runtime by
:class:`~astar.astar_shortest_path` keyword arguments ``banned_segment_ids``
and ``banned_turn_pairs``.

Input geometry:

* Prefer OGC WKT (``POLYGON((... ))`` / ``MULTIPOLYGON(...)``) in CRS 4326,
  matching stored ``geom`` columns.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Set

from psycopg2.extensions import connection as PGConnection

from backend.routing.astar import astar_shortest_path
from backend.routing.costs import (
    DEFAULT_ROUTING_COST_PROFILE,
    RoutingCostProfile,
)
from backend.routing.road_graph_loader import RoadGraph, load_road_graph


@dataclass(frozen=True, slots=True)
class BlockageBans:
    """Runtime overlay for avoidance routing."""

    banned_segment_ids: frozenset[int]
    banned_turn_pairs: frozenset[tuple[int, int]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "banned_segment_ids_count": len(self.banned_segment_ids),
            "banned_turn_pairs_count": len(self.banned_turn_pairs),
        }


def project_polygon_to_bans(
    conn: PGConnection,
    polygon_wkt: str,
    *,
    segment_import_run_id: Optional[int] = None,
    include_via_node_turn_bans: bool = True,
) -> BlockageBans:
    """
    Compute ban sets from a polygon geometry (WKT, lon/lat 4326).

    * ``segment_import_run_id`` — when provided, restricts segment selection
      and turn legs to cohort rows with that provenance.
    """
    banned_seg: Set[int] = set()
    banned_turn: Set[tuple[int, int]] = set()

    run_filter_clause = ""
    run_params_seg: tuple = ()
    run_params_turn: tuple = ()
    if segment_import_run_id is not None:
        run_filter_clause = " AND import_run_id = %s "
        run_params_seg = (segment_import_run_id,)
        run_params_turn = (segment_import_run_id, segment_import_run_id)

    sql_segments = (
        """
        SELECT s.segment_id
        FROM osm_road_segments s,
             (SELECT ST_SetSRID(ST_GeomFromText(%s, 4326), 4326) AS geom) AS poly
        WHERE ST_Intersects(s.geom, poly.geom)
        """
        + run_filter_clause
    )
    sql_via_turns = (
        """
        SELECT t.from_segment_id, t.to_segment_id
        FROM osm_segment_turns t,
             osm_nodes n,
             (SELECT ST_SetSRID(ST_GeomFromText(%s, 4326), 4326) AS geom) AS poly
        WHERE n.node_id = t.via_node_id
          AND ST_Intersects(n.geom, poly.geom)
        """
        + (
            ""
            if segment_import_run_id is None
            else """
          AND EXISTS (
            SELECT 1 FROM osm_road_segments sf
            WHERE sf.segment_id = t.from_segment_id
              AND sf.import_run_id = %s
          )
          AND EXISTS (
            SELECT 1 FROM osm_road_segments st
            WHERE st.segment_id = t.to_segment_id
              AND st.import_run_id = %s
          )
        """
        )
    )

    with conn.cursor() as cur:
        cur.execute(sql_segments, (polygon_wkt,) + run_params_seg)
        for row in cur.fetchall() or []:
            try:
                banned_seg.add(int(row[0]))
            except Exception:
                banned_seg.add(int(row["segment_id"]))

        if include_via_node_turn_bans:
            tp = (polygon_wkt,) + run_params_turn
            cur.execute(sql_via_turns, tp)
            for row in cur.fetchall() or []:
                try:
                    f = int(row[0])
                    t = int(row[1])
                except Exception:
                    f = int(row["from_segment_id"])
                    t = int(row["to_segment_id"])
                banned_turn.add((f, t))

    return BlockageBans(
        banned_segment_ids=frozenset(banned_seg),
        banned_turn_pairs=frozenset(banned_turn),
    )


def polygon_geojson_feature_to_wkt(geojson: Dict[str, Any]) -> str:
    """
    Convert a GeoJSON ``Feature`` or raw ``Polygon`` / ``MultiPolygon``
    geometry dict to WKT (lon/lat) using Shapely.
    """
    from shapely.geometry import shape

    if not isinstance(geojson, dict):
        raise TypeError("geojson must be a dict")

    geom = geojson.get("geometry", geojson)
    if not isinstance(geom, dict):
        raise ValueError("GeoJSON geometry missing or invalid")
    t = geom.get("type")
    if t not in {"Polygon", "MultiPolygon"}:
        raise ValueError(f"unsupported geometry type for blocker: {t!r}")

    poly = shape(geom)
    if not poly.is_valid:
        poly = poly.buffer(0)
    return poly.wkt


def load_polygon_wkt_from_geojson_file(path: Path | str) -> str:
    data = Path(path).read_text(encoding="utf-8")
    obj = json.loads(data)
    if obj.get("type") == "FeatureCollection":
        feats = obj.get("features") or []
        if not feats:
            raise ValueError("empty FeatureCollection")
        obj = feats[0]
    return polygon_geojson_feature_to_wkt(obj)


def route_segments_avoid_polygon(
    conn: PGConnection,
    start_node_id: int,
    goal_node_id: int,
    polygon_wkt: str,
    *,
    segment_import_run_id: Optional[int] = None,
    profile: RoutingCostProfile = DEFAULT_ROUTING_COST_PROFILE,
    include_via_node_turn_bans: bool = True,
    graph: Optional[RoadGraph] = None,
) -> tuple[Optional[list[int]], BlockageBans]:
    """
    Convenience: bans + segment-level A* in one call.

    If ``graph`` is provided it is reused; otherwise loads via
    :func:`~road_graph_loader.load_road_graph` with matching
    ``segment_import_run_id``.
    """
    bans = project_polygon_to_bans(
        conn,
        polygon_wkt,
        segment_import_run_id=segment_import_run_id,
        include_via_node_turn_bans=include_via_node_turn_bans,
    )
    g = graph or load_road_graph(
        conn, segment_import_run_id=segment_import_run_id
    )
    path = astar_shortest_path(
        g,
        start_node_id,
        goal_node_id,
        profile=profile,
        banned_segment_ids=bans.banned_segment_ids,
        banned_turn_pairs=bans.banned_turn_pairs,
    )
    return path, bans


__all__ = [
    "BlockageBans",
    "project_polygon_to_bans",
    "polygon_geojson_feature_to_wkt",
    "load_polygon_wkt_from_geojson_file",
    "route_segments_avoid_polygon",
]
