"""Project incident polygon to temporary edge/turn bans."""

from __future__ import annotations

from typing import Any, Dict, List

from .models import IncidentEdgeBan, IncidentProjection, IncidentTurnBan


def project_incident_polygon(
    *,
    blockage_geojson: Dict[str, Any],
    feed_id: int,
    db_osm_available: bool = True,
) -> IncidentProjection:
    """
    When OSM segments exist in DB, intersect polygon with segments → edge bans.
    Turn bans require junction detection — v1 leaves empty unless DB provides them.
    """
    if not db_osm_available:
        return IncidentProjection(edge_bans=[], turn_bans=[], segments_intersecting_polygon=0)
    try:
        from backend.infra import db_access as db

        rows = db.osm_segments_intersecting_polygon(feed_id, blockage_geojson)
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
    return IncidentProjection(
        edge_bans=bans,
        turn_bans=[],  # populated when turn extractor exists
        segments_intersecting_polygon=len(bans),
    )


def edge_ban_way_ids(projection: IncidentProjection) -> set[int]:
    return {b.osm_way_id for b in projection.edge_bans}
