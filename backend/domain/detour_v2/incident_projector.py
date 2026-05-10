"""Project incident polygon to temporary edge/turn bans."""

from __future__ import annotations

from typing import Any, Dict, List

from shapely.geometry import mapping, shape

from .models import IncidentEdgeBan, IncidentProjection, IncidentTurnBan


def project_incident_polygon(
    *,
    blockage_geojson: Dict[str, Any],
    feed_id: int,
    db_osm_available: bool = True,
    narrow_buffer_m: float = 150.0,
) -> IncidentProjection:
    """
    When OSM segments exist in DB, intersect polygon with segments → edge bans.
    Turn bans: restrictions whose via_node lies on a segment intersecting the polygon
    (optionally buffered in meters via narrow_buffer_m for nearby junctions).
    """
    del feed_id
    if not db_osm_available:
        return IncidentProjection(edge_bans=[], turn_bans=[], segments_intersecting_polygon=0)
    try:
        from backend.infra import db_access as db

        rows = db.osm_segments_intersecting_polygon(0, blockage_geojson)
    except Exception:
        rows = []
    bans: List[IncidentEdgeBan] = []
    for r in rows:
        try:
            wid = int(r.get("osm_way_id") or 0)
            if wid:
                bans.append(
                    IncidentEdgeBan(
                        osm_way_id=wid,
                        direction=r.get("direction"),
                    )
                )
        except Exception:
            continue

    turn_bans: List[IncidentTurnBan] = []
    try:
        from backend.infra import db_access as db

        try:
            g = shape(blockage_geojson)
            if not g.is_empty and narrow_buffer_m > 0:
                deg = max(float(narrow_buffer_m), 1.0) / 111_000.0
                g2 = g.buffer(deg)
                gj = mapping(g2)
            else:
                gj = blockage_geojson
        except Exception:
            gj = blockage_geojson
        node_ids = db.osm_segment_nodes_intersecting_polygon(gj)
        tr_rows = db.fetch_osm_turn_restrictions_by_via_nodes(node_ids) if node_ids else []
        for tr in tr_rows:
            try:
                turn_bans.append(
                    IncidentTurnBan(
                        from_way_id=int(tr.get("from_way_id") or 0),
                        via_node_id=int(tr.get("via_node_id") or 0),
                        to_way_id=int(tr.get("to_way_id") or 0),
                    )
                )
            except Exception:
                continue
    except Exception:
        pass

    return IncidentProjection(
        edge_bans=bans,
        turn_bans=turn_bans,
        segments_intersecting_polygon=len(bans),
    )


def edge_ban_way_ids(projection: IncidentProjection) -> set[int]:
    return {b.osm_way_id for b in projection.edge_bans}
