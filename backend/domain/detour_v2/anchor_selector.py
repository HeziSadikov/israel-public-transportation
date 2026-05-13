"""Choose exit/rejoin anchors along trip shape."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString, Point, shape as shp_shape

from backend.infra.config import ANCHOR_STOP_MAX_PROJECTION_M

from .intersection_finder import IntersectionPoint
from .models import AnchorPair, BlockedShapeInterval
from .policy import DetourPolicyConfig
from .trip_impact_analyzer import _line_length_m


# Road-class rank used when scoring intersection anchors. Lower = better.
_ROAD_CLASS_RANK: Dict[str, int] = {
    "motorway": 0,
    "trunk": 0,
    "primary": 0,
    "secondary": 0,
    "tertiary": 0,
    "unclassified": 1,
    "residential": 1,
    "service": 2,
    "service_other": 2,
    "alley": 2,
    "driveway": 2,
    "parking_aisle": 2,
    "footway": 3,
    "path": 3,
    "cycleway": 3,
    "pedestrian": 3,
    "steps": 3,
    "track": 3,
    "living_street": 2,
}


def _point_on_line_at_m(line: LineString, dist_m: float, total_m: float) -> Tuple[float, float]:
    if total_m <= 0:
        c = line.coords[0]
        return (float(c[0]), float(c[1]))
    t = max(0.0, min(1.0, dist_m / total_m))
    p = line.interpolate(t, normalized=True)
    return (float(p.x), float(p.y))


def _lonlat_stop_or_shape(
    stop_id: Optional[str],
    dist_m: float,
    total_m: float,
    line: LineString,
    stop_lonlat: Dict[str, Tuple[float, float]],
) -> Tuple[float, float]:
    """Prefer curb stop position when it projects near the shape (meters), else shape point."""
    sx, sy = _point_on_line_at_m(line, dist_m, total_m)
    if not stop_id:
        return sx, sy
    ll = stop_lonlat.get(str(stop_id))
    if not ll:
        return sx, sy
    slon, slat = ll
    try:
        p = Point(slon, slat)
        d_proj = float(line.project(p))
        closest = line.interpolate(d_proj)
        dist_deg = float(p.distance(closest))
        dist_m_approx = dist_deg * 111_320.0
        if dist_m_approx <= float(ANCHOR_STOP_MAX_PROJECTION_M):
            return slon, slat
    except Exception:
        pass
    return sx, sy


def _stops_with_dist_along(
    line: LineString,
    total_m: float,
    stop_rows: List[Dict[str, Any]],
    stop_lonlat: Dict[str, Tuple[float, float]],
) -> List[Tuple[str, float]]:
    """Assign each stop a distance (m) along shape: prefer shape_dist_traveled else project onto line."""
    out: List[Tuple[str, float]] = []
    for row in stop_rows:
        sid = str(row.get("stop_id") or "")
        sdt = row.get("shape_dist_traveled")
        if sdt is not None:
            try:
                out.append((sid, float(sdt)))
                continue
            except Exception:
                pass
        ll = stop_lonlat.get(sid)
        if not ll:
            continue
        lon, lat = ll
        from shapely.geometry import Point

        try:
            d = float(line.project(Point(lon, lat)) / max(line.length, 1e-15) * total_m)
            out.append((sid, d))
        except Exception:
            continue
    out.sort(key=lambda x: x[1])
    return out


def select_anchors(
    *,
    line: LineString,
    blocked: BlockedShapeInterval,
    stop_rows: List[Dict[str, Any]],
    stop_lonlat: Dict[str, Tuple[float, float]],
    policy: DetourPolicyConfig,
) -> AnchorPair:
    """
    Select a single primary pair; for richer optimization use enumerate_anchor_candidates().
    """
    cands = enumerate_anchor_candidates(
        line=line,
        blocked=blocked,
        stop_rows=stop_rows,
        stop_lonlat=stop_lonlat,
        policy=policy,
        max_pairs=1,
    )
    if cands:
        return cands[0]
    total_m = blocked.shape_length_m or _line_length_m(line)
    if total_m <= 0:
        total_m = _line_length_m(line)
    ex_lon, ex_lat = _point_on_line_at_m(line, blocked.blocked_start_m, total_m)
    rj_lon, rj_lat = _point_on_line_at_m(line, blocked.blocked_end_m, total_m)
    return AnchorPair(
        exit_lon=ex_lon,
        exit_lat=ex_lat,
        rejoin_lon=rj_lon,
        rejoin_lat=rj_lat,
        exit_shape_dist_m=blocked.blocked_start_m,
        rejoin_shape_dist_m=blocked.blocked_end_m,
        anchor_quality_note="fallback_blocked_interval",
    )


def enumerate_anchor_candidates(
    *,
    line: LineString,
    blocked: BlockedShapeInterval,
    stop_rows: List[Dict[str, Any]],
    stop_lonlat: Dict[str, Tuple[float, float]],
    policy: DetourPolicyConfig,
    max_pairs: Optional[int] = None,
    search_before_window_m: Optional[float] = None,
    search_after_window_m: Optional[float] = None,
    blockage_geojson: Optional[Dict[str, Any]] = None,
) -> List[AnchorPair]:
    """Generate bounded exit/rejoin candidates ordered by service-preserving heuristics."""
    ap = policy.anchor
    total_m = blocked.shape_length_m or _line_length_m(line)
    if total_m <= 0:
        total_m = _line_length_m(line)
    stops = _stops_with_dist_along(line, total_m, stop_rows, stop_lonlat)
    bs, be = blocked.blocked_start_m, blocked.blocked_end_m
    gap = ap.min_anchor_gap_m

    before_w = float(search_before_window_m if search_before_window_m is not None else ap.search_before_window_m)
    after_w = float(search_after_window_m if search_after_window_m is not None else ap.search_after_window_m)

    exit_lo = max(0.0, bs - before_w)
    exit_hi = max(0.0, bs - gap)
    rejoin_lo = min(total_m, be + gap)
    rejoin_hi = min(total_m, be + after_w)

    poly = None
    if blockage_geojson:
        try:
            poly = shp_shape(blockage_geojson)
            if poly.is_empty:
                poly = None
        except Exception:
            poly = None

    max_side = max(1, int(ap.candidate_stops_per_side))
    k = max(1, int(max_pairs if max_pairs is not None else ap.candidate_pairs_k))
    exits = [(sid, d) for sid, d in stops if exit_lo <= d <= exit_hi]
    rejoins = [(sid, d) for sid, d in stops if rejoin_lo <= d <= rejoin_hi]
    exits.sort(key=lambda x: abs(bs - x[1]))
    rejoins.sort(key=lambda x: abs(x[1] - be))
    exits = exits[:max_side]
    rejoins = rejoins[:max_side]
    if not exits:
        exits = [(None, max(0.0, (exit_lo + exit_hi) / 2.0))]
    if not rejoins:
        rejoins = [(None, min(total_m, (rejoin_lo + rejoin_hi) / 2.0))]

    stop_dists = [d for _, d in stops]
    scored: List[Tuple[Tuple[float, float, float, float], AnchorPair]] = []
    for e_sid, e_d in exits:
        for r_sid, r_d in rejoins:
            if e_d >= r_d:
                continue
            skipped_est = sum(1 for d in stop_dists if e_d < d < r_d)
            interp_pen = float((e_sid is None) + (r_sid is None))
            shoulder_m = max(0.0, bs - e_d) + max(0.0, r_d - be)
            span_m = max(1.0, r_d - e_d)
            ex_lon, ex_lat = _lonlat_stop_or_shape(e_sid, e_d, total_m, line, stop_lonlat)
            rj_lon, rj_lat = _lonlat_stop_or_shape(r_sid, r_d, total_m, line, stop_lonlat)
            note = None
            if e_sid is None or r_sid is None:
                note = "interpolated_shape_anchor"
            cand = AnchorPair(
                exit_lon=ex_lon,
                exit_lat=ex_lat,
                rejoin_lon=rj_lon,
                rejoin_lat=rj_lat,
                exit_stop_id=str(e_sid) if e_sid is not None else None,
                rejoin_stop_id=str(r_sid) if r_sid is not None else None,
                exit_shape_dist_m=e_d,
                rejoin_shape_dist_m=r_d,
                anchor_quality_note=note,
            )
            if poly is not None:
                try:
                    if poly.contains(Point(cand.exit_lon, cand.exit_lat)) or poly.contains(
                        Point(cand.rejoin_lon, cand.rejoin_lat)
                    ):
                        continue
                except Exception:
                    pass
            scored.append(((interp_pen, float(skipped_est), shoulder_m, span_m), cand))

    if not scored:
        ex_d = max(0.0, bs - gap * 2.0)
        rj_d = min(total_m, be + gap * 2.0)
        ex_lon, ex_lat = _point_on_line_at_m(line, ex_d, total_m)
        rj_lon, rj_lat = _point_on_line_at_m(line, rj_d, total_m)
        return [
            AnchorPair(
                exit_lon=ex_lon,
                exit_lat=ex_lat,
                rejoin_lon=rj_lon,
                rejoin_lat=rj_lat,
                exit_shape_dist_m=ex_d,
                rejoin_shape_dist_m=rj_d,
                anchor_quality_note="interpolated_shape_anchor",
            )
        ]

    scored.sort(key=lambda x: x[0])
    return [cand for _, cand in scored[:k]]


# ---------------------------------------------------------------------------
# Intersection-based anchor enumeration (Option A architecture)
# ---------------------------------------------------------------------------

def _nearest_stop_id_by_shape_dist(
    shape_dist_m: float,
    stops_with_dist: List[Tuple[str, float]],
) -> Optional[str]:
    """Return the stop_id whose shape distance is closest to *shape_dist_m*.

    Used only for display / service-stitching labelling; the actual anchor
    lat/lon comes from the intersection, not the stop.
    """
    if not stops_with_dist:
        return None
    best_sid, best_delta = None, float("inf")
    for sid, d in stops_with_dist:
        delta = abs(d - shape_dist_m)
        if delta < best_delta:
            best_delta = delta
            best_sid = sid
    return best_sid


def enumerate_intersection_anchor_candidates(
    *,
    intersections: List[IntersectionPoint],
    blocked: BlockedShapeInterval,
    stops_with_dist: List[Tuple[str, float]],
    policy: DetourPolicyConfig,
    max_pairs: Optional[int] = None,
    search_before_window_m: Optional[float] = None,
    search_after_window_m: Optional[float] = None,
    blockage_geojson: Optional[Dict[str, Any]] = None,
) -> List[AnchorPair]:
    """Generate exit/rejoin anchor pairs using OSM intersection nodes.

    Each intersection is at a real road junction, so Valhalla has a legal
    route that can diverge from the current road.  Falls back to an empty list
    when no intersections straddle the blockage (caller should then fall back
    to ``enumerate_anchor_candidates``).
    """
    ap = policy.anchor
    total_m = blocked.shape_length_m
    bs, be = blocked.blocked_start_m, blocked.blocked_end_m
    gap = ap.min_anchor_gap_m

    before_w = float(
        search_before_window_m if search_before_window_m is not None else ap.search_before_window_m
    )
    after_w = float(
        search_after_window_m if search_after_window_m is not None else ap.search_after_window_m
    )

    exit_lo = max(0.0, bs - before_w)
    exit_hi = max(0.0, bs - gap)
    rejoin_lo = min(total_m, be + gap)
    rejoin_hi = min(total_m, be + after_w)

    poly = None
    if blockage_geojson:
        try:
            poly = shp_shape(blockage_geojson)
            if poly.is_empty:
                poly = None
        except Exception:
            poly = None

    # Minimum clearance gate: discard intersections within 50 m of the blockage
    # edge so Valhalla has room to manoeuvre.  Everything else is kept and ranked
    # by fewest skipped stops first.
    MIN_CLEARANCE_M = 50.0
    exits = [
        ix for ix in intersections
        if exit_lo <= ix.shape_dist_m <= exit_hi
        and (bs - ix.shape_dist_m) >= MIN_CLEARANCE_M
    ]
    rejoins = [
        ix for ix in intersections
        if rejoin_lo <= ix.shape_dist_m <= rejoin_hi
        and (ix.shape_dist_m - be) >= MIN_CLEARANCE_M
    ]

    if not exits or not rejoins:
        return []

    k = max(1, int(max_pairs if max_pairs is not None else ap.candidate_pairs_k))
    max_side = max(1, int(ap.candidate_stops_per_side))
    exits = exits[:max_side]
    rejoins = rejoins[:max_side]

    stop_dists_all = [d for _, d in stops_with_dist]

    # Clearance used only as a tiebreaker when skipped_est is equal.
    blocked_len = max(50.0, be - bs)
    target_clear = min(400.0, max(150.0, 0.5 * blocked_len))

    def _cross_rank(ix: IntersectionPoint) -> int:
        """Lowest (best) road-class rank among the drivable cross-streets."""
        if not ix.cross_road_classes:
            return 99
        return min(_ROAD_CLASS_RANK.get(rc, 1) for rc in ix.cross_road_classes)

    def _route_rank(ix: IntersectionPoint) -> int:
        return _ROAD_CLASS_RANK.get(ix.road_class, 1)

    scored: List[Tuple[Tuple, AnchorPair]] = []
    for e_ix in exits:
        for r_ix in rejoins:
            if e_ix.shape_dist_m >= r_ix.shape_dist_m:
                continue

            # Skip if either point is inside the blockage polygon.
            if poly is not None:
                try:
                    if poly.contains(Point(e_ix.lon, e_ix.lat)) or poly.contains(
                        Point(r_ix.lon, r_ix.lat)
                    ):
                        continue
                except Exception:
                    pass

            e_route_rank = _route_rank(e_ix)
            r_route_rank = _route_rank(r_ix)
            e_cross_rank = _cross_rank(e_ix)
            r_cross_rank = _cross_rank(r_ix)

            # Number of stops between exit and rejoin (skipped estimate).
            skipped_est = sum(1 for d in stop_dists_all if e_ix.shape_dist_m < d < r_ix.shape_dist_m)

            # Clearance penalty used only as tiebreaker when skipped_est ties.
            exit_clear = bs - e_ix.shape_dist_m
            rejoin_clear = r_ix.shape_dist_m - be
            clearance_pen = (
                max(0.0, target_clear - exit_clear)
                + max(0.0, target_clear - rejoin_clear)
            )

            # Lower score = better anchor.
            # Priority: route road class → cross road class → fewest skipped stops → clearance tiebreaker.
            score = (
                e_route_rank + r_route_rank,
                e_cross_rank + r_cross_rank,
                float(skipped_est),
                clearance_pen,
            )

            e_stop_id = _nearest_stop_id_by_shape_dist(e_ix.shape_dist_m, stops_with_dist)
            r_stop_id = _nearest_stop_id_by_shape_dist(r_ix.shape_dist_m, stops_with_dist)

            best_e_cross = min(e_ix.cross_road_classes, key=lambda rc: _ROAD_CLASS_RANK.get(rc, 1), default=None)
            best_r_cross = min(r_ix.cross_road_classes, key=lambda rc: _ROAD_CLASS_RANK.get(rc, 1), default=None)
            cand = AnchorPair(
                exit_lon=e_ix.lon,
                exit_lat=e_ix.lat,
                rejoin_lon=r_ix.lon,
                rejoin_lat=r_ix.lat,
                exit_stop_id=e_stop_id,
                rejoin_stop_id=r_stop_id,
                exit_shape_dist_m=e_ix.shape_dist_m,
                rejoin_shape_dist_m=r_ix.shape_dist_m,
                anchor_quality_note="intersection_anchor",
                anchor_source="intersection_finder",
                exit_osm_segment_id=e_ix.osm_node_id if e_ix.osm_node_id else None,
                rejoin_osm_segment_id=r_ix.osm_node_id if r_ix.osm_node_id else None,
                exit_road_class=e_ix.road_class or None,
                rejoin_road_class=r_ix.road_class or None,
                exit_road_class_rank=e_route_rank,
                rejoin_road_class_rank=r_route_rank,
                exit_cross_road_class=best_e_cross,
                rejoin_cross_road_class=best_r_cross,
            )
            scored.append((score, cand))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0])
    return [cand for _, cand in scored[:k]]
