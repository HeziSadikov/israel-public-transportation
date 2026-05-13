"""Decode route coordinates into RoadSegmentRef / TurnRef (synthetic when OSM DB empty)."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from backend.infra import db_access as db
from backend.adapters.osm_detour import match_route_attributes
from .models import DecodedCandidate, RoadSegmentRef, TurnRef

# Valhalla /trace_attributes road_class → OSM highway approximation
_ROAD_CLASS_TO_HIGHWAY: Dict[str, str] = {
    "motorway": "motorway",
    "trunk": "trunk",
    "primary": "primary",
    "secondary": "secondary",
    "tertiary": "tertiary",
    "residential": "residential",
    "service_other": "service",
    "unclassified": "unclassified",
    "living_street": "living_street",
    "alley": "service",
    "parking_aisle": "service",
    "driveway": "service",
    "footway": "footway",
    "path": "path",
    "cycleway": "cycleway",
    "track": "track",
}


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371000.0
    p = math.pi / 180.0
    a = (
        0.5
        - math.cos((lat2 - lat1) * p) / 2
        + math.cos(lat1 * p) * math.cos(lat2 * p) * (1 - math.cos((lon2 - lon1) * p)) / 2
    )
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def decode_polyline_to_synthetic_segments(
    coordinates: List[tuple[float, float]],
    travel_time_s: float,
) -> DecodedCandidate:
    """
    Build synthetic segment refs along the polyline when `osm_road_segments` is not populated.
    One segment per coordinate step (merged visually for map from full LineString).
    """
    if len(coordinates) < 2:
        return DecodedCandidate(road_segments=[], turns=[], geometry_geojson=_fc([]))

    hops: List[float] = []
    for i in range(len(coordinates) - 1):
        lon1, lat1 = coordinates[i]
        lon2, lat2 = coordinates[i + 1]
        hops.append(_haversine_m(lon1, lat1, lon2, lat2))

    total_len = sum(hops) or 1.0
    chunk_target_m = 120.0
    segments: List[RoadSegmentRef] = []
    turns: List[TurnRef] = []
    buf_m = 0.0
    buf_t = 0.0
    for i, d in enumerate(hops):
        buf_m += d
        buf_t += travel_time_s * (d / total_len)
        last = i == len(hops) - 1
        if buf_m >= chunk_target_m or last:
            sid = 1_000_000 + len(segments)
            segments.append(
                RoadSegmentRef(
                    segment_id=sid,
                    osm_way_id=0,
                    from_node_id=len(segments),
                    to_node_id=len(segments) + 1,
                    sequence_index=len(segments),
                    length_m=max(buf_m, 0.1),
                    travel_time_s=max(buf_t, 0.0),
                    highway="unknown",
                    synthetic=True,
                )
            )
            if len(segments) > 1:
                turns.append(
                    TurnRef(
                        from_segment_id=segments[-2].segment_id,
                        to_segment_id=segments[-1].segment_id,
                        via_node_id=len(segments) - 1,
                        sequence_index=len(turns),
                        turn_angle=None,
                        turn_type="continue",
                    )
                )
            buf_m = 0.0
            buf_t = 0.0

    return DecodedCandidate(
        road_segments=segments,
        turns=turns,
        geometry_geojson=_fc(coordinates),
    )


def _fc(coords: List[tuple[float, float]]) -> Dict[str, Any]:
    if not coords:
        return {"type": "FeatureCollection", "features": []}
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [list(c) for c in coords]},
                "properties": {"kind": "detour_v2_candidate"},
            }
        ],
    }


def decode_valhalla_candidate(
    coordinates: List[tuple[float, float]],
    travel_time_s: float,
    *,
    match_osm_segments: bool = False,
    feed_id: Optional[int] = None,
) -> DecodedCandidate:
    # --- Phase A1: Try /trace_attributes first for exact OSM way IDs ---
    if match_osm_segments and len(coordinates) >= 2:
        try:
            edges = match_route_attributes(coordinates, costing="bus")
        except Exception:
            edges = None
        if edges:
            decoded = _decode_from_trace_edges(coordinates, edges, travel_time_s)
            if decoded is not None:
                return decoded

    # --- Fallback: proximity-based matching via PostGIS ---
    if match_osm_segments:
        try:
            rows = db.get_candidate_osm_segments_for_polyline(coordinates)
        except Exception:
            rows = []
        if rows:
            total_len = sum(float(r.get("length_m") or 0.0) for r in rows) or 1.0
            segs: List[RoadSegmentRef] = []
            turns: List[TurnRef] = []
            for i, r in enumerate(rows):
                seg_id = int(r.get("segment_id") or (2_000_000 + i))
                segs.append(
                    RoadSegmentRef(
                        segment_id=seg_id,
                        osm_way_id=int(r.get("osm_way_id") or 0),
                        from_node_id=int(r.get("from_node_id") or 0),
                        to_node_id=int(r.get("to_node_id") or 0),
                        sequence_index=i,
                        length_m=float(r.get("length_m") or 0.0),
                        travel_time_s=max(0.0, travel_time_s * (float(r.get("length_m") or 0.0) / total_len)),
                        highway=(r.get("highway") or None),
                        access=(r.get("access") or None),
                        bus=(r.get("bus") or None),
                        psv=(r.get("psv") or None),
                        service=(r.get("service") or None),
                        synthetic=False,
                    )
                )
                if i > 0:
                    turns.append(
                        TurnRef(
                            from_segment_id=segs[i - 1].segment_id,
                            to_segment_id=seg_id,
                            via_node_id=int(r.get("from_node_id") or i),
                            sequence_index=i - 1,
                            turn_angle=None,
                            turn_type="continue",
                        )
                    )
            return DecodedCandidate(
                road_segments=segs,
                turns=turns,
                geometry_geojson=_fc(coordinates),
            )
    return decode_polyline_to_synthetic_segments(coordinates, travel_time_s)


def _decode_from_trace_edges(
    coordinates: List[tuple[float, float]],
    edges: List[Dict[str, Any]],
    travel_time_s: float,
) -> Optional[DecodedCandidate]:
    """Build DecodedCandidate from /trace_attributes edge list.

    Uses real OSM node IDs (begin_osm_node_id / end_osm_node_id) from the Valhalla
    response so that TurnRef.via_node_id matches real OSM node ids used by
    IncidentTurnBan and bus_turn_evidence triplets.
    """
    if not edges:
        return None
    total_len_km = sum(float(e.get("length") or 0.0) for e in edges)
    if total_len_km <= 0:
        return None
    total_len_m = total_len_km * 1000.0
    segs: List[RoadSegmentRef] = []
    turns: List[TurnRef] = []
    # Seed the first begin-node from edge[0].begin_osm_node_id (0 when absent).
    prev_end_node: int = int(edges[0].get("begin_osm_node_id") or 0)
    for i, edge in enumerate(edges):
        way_id = int(edge.get("way_id") or 0)
        length_m = float(edge.get("length") or 0.0) * 1000.0
        t_frac = length_m / total_len_m if total_len_m > 0 else (1.0 / len(edges))
        road_class = str(edge.get("road_class") or "")
        highway = _ROAD_CLASS_TO_HIGHWAY.get(road_class, road_class or "unknown")
        use = str(edge.get("use") or "")
        access_restriction = bool(edge.get("access_restriction") or False)
        bus_val = "yes" if use in {"road", "bus"} else None
        access_val = "no" if access_restriction and not bus_val else None
        # Real OSM node IDs — 0 if /trace_attributes didn't return them.
        begin_node = int(edge.get("begin_osm_node_id") or prev_end_node)
        end_node = int(edge.get("end_osm_node_id") or 0)
        seg_id = 3_000_000 + i
        segs.append(
            RoadSegmentRef(
                segment_id=seg_id,
                osm_way_id=way_id,
                from_node_id=begin_node,
                to_node_id=end_node,
                sequence_index=i,
                length_m=max(length_m, 0.1),
                travel_time_s=max(0.0, travel_time_s * t_frac),
                highway=highway,
                access=access_val,
                bus=bus_val,
                psv=None,
                service=None,
                synthetic=False,
            )
        )
        if i > 0:
            # via_node_id is the junction between previous edge's end and this edge's begin.
            via = begin_node if begin_node != 0 else prev_end_node
            turns.append(
                TurnRef(
                    from_segment_id=segs[i - 1].segment_id,
                    to_segment_id=seg_id,
                    via_node_id=via,
                    sequence_index=i - 1,
                    turn_angle=None,
                    turn_type="continue",
                )
            )
        prev_end_node = end_node if end_node != 0 else begin_node
    return DecodedCandidate(
        road_segments=segs,
        turns=turns,
        geometry_geojson=_fc(coordinates),
    )
