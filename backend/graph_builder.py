from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Sequence, TYPE_CHECKING

import networkx as nx
from shapely.geometry import LineString

from .gtfs_loader import GTFSFeed
from .sqlite_db import get_stop_times_for_trip
from .pattern_builder import RoutePattern
from .service_calendar import parse_gtfs_time_to_seconds

if TYPE_CHECKING:
    from .db_access import PatternMeta


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import radians, cos, sin, asin, sqrt

    R = 6371000.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Bearing from point 1 to point 2 in degrees (0=North, 90=East)."""
    from math import radians, degrees, atan2, sin, cos

    rlat1, rlon1 = radians(lat1), radians(lon1)
    rlat2, rlon2 = radians(lat2), radians(lon2)
    dlon = rlon2 - rlon1
    x = sin(dlon) * cos(rlat2)
    y = cos(rlat1) * sin(rlat2) - sin(rlat1) * cos(rlat2) * cos(dlon)
    b = degrees(atan2(x, y))
    return (b + 360.0) % 360.0


def angle_difference_deg(h1: float, h2: float) -> float:
    """Smallest angle between two headings in [0, 360)."""
    d = abs(h1 - h2)
    return min(d, 360.0 - d)


def pattern_stop_node_id(pattern_id: str, stop_id: str, stop_sequence: int) -> str:
    """Unique node id for a pattern-stop (direction-aware)."""
    return f"{pattern_id}:{stop_id}:{stop_sequence}"


def parse_pattern_stop_node_id(node_id: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Parse node_id into (pattern_id, stop_id, stop_sequence). Returns (None, None, None) if invalid."""
    if not node_id or ":" not in node_id:
        return None, None, None
    parts = node_id.split(":", 2)
    if len(parts) != 3:
        return None, None, None
    try:
        seq = int(parts[2])
        return parts[0], parts[1], seq
    except ValueError:
        return None, None, None


@dataclass
class EdgeGeometry:
    from_stop_id: str
    to_stop_id: str
    linestring: LineString


@dataclass
class GraphBuildResult:
    graph: nx.DiGraph
    edge_geometries: Dict[Tuple[str, str], EdgeGeometry]
    pattern: RoutePattern
    used_shape: bool


class GraphBuilder:
    def __init__(self, feed: GTFSFeed):
        self.feed = feed
        self.stops_by_id: Dict[str, Dict] = {s["stop_id"]: s for s in feed.stops}

        self.shapes_by_id: Dict[str, List[Dict]] = {}
        for row in feed.shapes:
            sid = row["shape_id"]
            self.shapes_by_id.setdefault(sid, []).append(row)
        for sid, pts in self.shapes_by_id.items():
            pts.sort(key=lambda r: int(r["shape_pt_sequence"]))

        # Lazily populated per-trip stop_times index to avoid loading the
        # entire stop_times.txt into memory up front.
        self.stop_times_by_trip: Dict[str, List[Dict]] = {}

        # Precompute per-edge scheduled travel times (in seconds) for the
        # representative trip of each pattern when requested.
        # We keep this logic in a helper method so it can gracefully fall back
        # when GTFS times are missing or malformed.

    def _load_stop_times_for_trip(self, trip_id: str) -> List[Dict]:
        """
        Ensure stop_times for the given trip_id are loaded into memory.

        This scans stop_times.txt once per unseen trip_id and caches the
        resulting list, sorted by stop_sequence.
        """
        if trip_id in self.stop_times_by_trip:
            return self.stop_times_by_trip[trip_id]

        # Load stop_times for this trip directly from SQLite, ordered by stop_sequence.
        sts: List[Dict] = get_stop_times_for_trip(trip_id)
        self.stop_times_by_trip[trip_id] = sts
        return sts

    def _edge_travel_time_seconds(
        self, pattern: RoutePattern, from_sid: str, to_sid: str
    ) -> Optional[float]:
        """
        Returns scheduled travel time in seconds between two consecutive stops
        for the pattern's representative trip, if available.

        Uses departure_time at from_sid and arrival_time at to_sid.
        Falls back to arrival/arrival or departure/departure if one is missing.
        """
        sts = self._load_stop_times_for_trip(pattern.representative_trip_id)
        if not sts:
            return None

        by_stop: Dict[str, Dict] = {st["stop_id"]: st for st in sts}
        st_from = by_stop.get(from_sid)
        st_to = by_stop.get(to_sid)
        if not st_from or not st_to:
            return None

        def _pick_time(row: Dict, pref_field: str, alt_field: str) -> Optional[int]:
            t = (row.get(pref_field) or "").strip()
            if t:
                return parse_gtfs_time_to_seconds(t)
            t_alt = (row.get(alt_field) or "").strip()
            if t_alt:
                return parse_gtfs_time_to_seconds(t_alt)
            return None

        t_from = _pick_time(st_from, "departure_time", "arrival_time")
        t_to = _pick_time(st_to, "arrival_time", "departure_time")
        if t_from is None or t_to is None:
            return None

        dt = t_to - t_from
        # Guard against malformed data where times go backwards or are zero.
        if dt <= 0:
            return None
        return float(dt)

    def build_graph_for_pattern(self, pattern: RoutePattern) -> GraphBuildResult:
        """
        Build a direction-aware graph: nodes are pattern-stops
        (pattern_id, stop_id, stop_sequence) so the same physical stop appears
        as different nodes per pattern/direction. Each node has out_heading_deg
        for transfer compatibility.
        """
        g = nx.DiGraph()
        pid = pattern.pattern_id
        stop_ids = pattern.stop_ids
        n_stops = len(stop_ids)

        # First pass: add nodes with lat/lon; we'll set out_heading_deg in the edge loop.
        for seq, sid in enumerate(stop_ids):
            stop = self.stops_by_id.get(sid)
            if not stop:
                continue
            node_id = pattern_stop_node_id(pid, sid, seq)
            g.add_node(
                node_id,
                pattern_id=pid,
                route_id=pattern.route_id,
                direction_id=pattern.direction_id,
                stop_id=sid,
                stop_sequence=seq,
                stop_name=stop.get("stop_name"),
                lat=float(stop.get("stop_lat")),
                lon=float(stop.get("stop_lon")),
                out_heading_deg=None,  # set below for non-last stops
                # Trips per day / pattern frequency; used to slightly prefer
                # more frequent lines in A* weighting without dominating time.
                frequency=pattern.frequency,
            )

        edge_geoms: Dict[Tuple[str, str], EdgeGeometry] = {}

        shape_line: Optional[LineString] = None
        shape_dists: Dict[str, float] = {}
        used_shape = False

        shape_pts: Optional[List[Tuple[float, float]]] = None
        shape_cum_m: Optional[List[float]] = None
        if pattern.representative_shape_id and pattern.representative_shape_id in self.shapes_by_id:
            pts = self.shapes_by_id[pattern.representative_shape_id]
            coords = [(float(p["shape_pt_lon"]), float(p["shape_pt_lat"])) for p in pts]
            if len(coords) >= 2:
                shape_line = LineString(coords)
                shape_pts, shape_cum_m = _line_to_pts_and_cum_m(shape_line)

        sts = self._load_stop_times_for_trip(pattern.representative_trip_id)
        for st in sts:
            sid = st["stop_id"]
            if st.get("shape_dist_traveled"):
                shape_dists[sid] = float(st["shape_dist_traveled"])

        for i in range(n_stops - 1):
            a = stop_ids[i]
            b = stop_ids[i + 1]
            stop_a = self.stops_by_id.get(a)
            stop_b = self.stops_by_id.get(b)
            if not stop_a or not stop_b:
                continue

            lat1, lon1 = float(stop_a["stop_lat"]), float(stop_a["stop_lon"])
            lat2, lon2 = float(stop_b["stop_lat"]), float(stop_b["stop_lon"])
            out_heading = bearing_deg(lat1, lon1, lat2, lon2)

            node_a = pattern_stop_node_id(pid, a, i)
            node_b = pattern_stop_node_id(pid, b, i + 1)
            if g.has_node(node_a):
                g.nodes[node_a]["out_heading_deg"] = out_heading

            if a in shape_dists and b in shape_dists:
                start_d, end_d = shape_dists[a], shape_dists[b]
                if end_d > start_d:
                    distance_m = end_d - start_d
                else:
                    distance_m = haversine_meters(lat1, lon1, lat2, lon2)
            else:
                distance_m = haversine_meters(lat1, lon1, lat2, lon2)

            travel_time_s = self._edge_travel_time_seconds(pattern, a, b)
            if travel_time_s is None:
                travel_time_s = distance_m / 8.33 if distance_m > 0 else 1.0

            geom = self._build_edge_geom(
                a, b, lat1, lon1, lat2, lon2,
                shape_line, shape_dists, shape_pts, shape_cum_m,
            )
            edge_geoms[(node_a, node_b)] = EdgeGeometry(from_stop_id=a, to_stop_id=b, linestring=geom)

            g.add_edge(
                node_a,
                node_b,
                weight=travel_time_s,
                travel_time_s=travel_time_s,
                distance_m=distance_m,
            )

        # Last stop: use incoming bearing as out_heading for transfer logic.
        if n_stops >= 2 and g.nodes:
            last_sid = stop_ids[n_stops - 1]
            prev_sid = stop_ids[n_stops - 2]
            stop_prev = self.stops_by_id.get(prev_sid)
            stop_last = self.stops_by_id.get(last_sid)
            if stop_prev and stop_last:
                lat_prev = float(stop_prev["stop_lat"])
                lon_prev = float(stop_prev["stop_lon"])
                lat_last = float(stop_last["stop_lat"])
                lon_last = float(stop_last["stop_lon"])
                incoming = bearing_deg(lat_prev, lon_prev, lat_last, lon_last)
                last_node = pattern_stop_node_id(pid, last_sid, n_stops - 1)
                if g.has_node(last_node):
                    g.nodes[last_node]["out_heading_deg"] = incoming

        return GraphBuildResult(
            graph=g,
            edge_geometries=edge_geoms,
            pattern=pattern,
            used_shape=shape_line is not None,
        )

    def _build_edge_geom(
        self,
        from_sid: str,
        to_sid: str,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        shape_line: Optional[LineString],
        shape_dists: Dict[str, float],
        shape_pts: Optional[List[Tuple[float, float]]] = None,
        shape_cum_m: Optional[List[float]] = None,
    ) -> LineString:
        """
        Prefer a segment of the GTFS shape between the two stops, falling back
        to a straight line if shape information is missing.

        Uses index slicing + interpolation on cumulative distances (no Shapely
        split), which is faster and more stable than buffer/split.
        """
        if not shape_line:
            return LineString([(lon1, lat1), (lon2, lat2)])
        if from_sid not in shape_dists or to_sid not in shape_dists:
            return LineString([(lon1, lat1), (lon2, lat2)])

        start_d = shape_dists[from_sid]
        end_d = shape_dists[to_sid]
        if start_d >= end_d:
            return LineString([(lon1, lat1), (lon2, lat2)])

        if shape_pts is not None and shape_cum_m is not None:
            try:
                return _slice_by_cumulative_distances(shape_pts, shape_cum_m, start_d, end_d)
            except Exception:
                pass
        # Fallback: interpolate endpoints and build segment (no split).
        try:
            return _slice_linestring_by_interpolate(shape_line, start_d, end_d)
        except Exception:
            return LineString([(lon1, lat1), (lon2, lat2)])


def _line_to_pts_and_cum_m(line: LineString) -> Tuple[List[Tuple[float, float]], List[float]]:
    """Precompute list of points and cumulative distances along the line (meters)."""
    pts = list(line.coords)
    if len(pts) < 2:
        return pts, [0.0]
    cum_m: List[float] = [0.0]
    for i in range(1, len(pts)):
        lon1, lat1 = pts[i - 1][0], pts[i - 1][1]
        lon2, lat2 = pts[i][0], pts[i][1]
        cum_m.append(cum_m[-1] + haversine_meters(lat1, lon1, lat2, lon2))
    return pts, cum_m


def _interpolate_segment(
    p0: Tuple[float, float], p1: Tuple[float, float], t: float
) -> Tuple[float, float]:
    """Linear interpolation between two points; t in [0, 1]."""
    return (p0[0] + t * (p1[0] - p0[0]), p0[1] + t * (p1[1] - p0[1]))


def _slice_by_cumulative_distances(
    pts: Sequence[Tuple[float, float]],
    cum_m: Sequence[float],
    start_d: float,
    end_d: float,
) -> LineString:
    """
    Slice the polyline between start_d and end_d (meters along line) using
    index range + two boundary interpolations. No Shapely split; fast and stable.
    """
    if not pts or len(cum_m) != len(pts) or start_d >= end_d:
        return LineString(pts[:2] if len(pts) >= 2 else pts)

    n = len(pts)
    total = cum_m[-1] if n else 0.0
    start_d = max(0.0, min(start_d, total))
    end_d = max(0.0, min(end_d, total))
    if start_d >= end_d:
        return LineString([pts[0], pts[-1]] if n >= 2 else pts)

    # Index range: i0 such that cum_m[i0] <= start_d < cum_m[i0+1]; end_d in [cum_m[i1-1], cum_m[i1]).
    i0 = bisect.bisect_right(cum_m, start_d) - 1
    i1 = bisect.bisect_left(cum_m, end_d)
    i0 = max(0, min(i0, n - 1))
    i1 = max(0, min(i1, n))
    if i1 <= i0:
        # Same segment: interpolate both boundaries on segment (i0, i0+1)
        if i0 >= n - 1:
            return LineString([pts[-1], pts[-1]])
        d0, d1 = cum_m[i0], cum_m[i0 + 1]
        span = d1 - d0 if d1 > d0 else 1.0
        t_start = (start_d - d0) / span
        t_end = (end_d - d0) / span
        return LineString([
            _interpolate_segment(pts[i0], pts[i0 + 1], t_start),
            _interpolate_segment(pts[i0], pts[i0 + 1], t_end),
        ])

    out: List[Tuple[float, float]] = []
    # Start boundary on segment (i0, i0+1)
    d0, d1 = cum_m[i0], cum_m[i0 + 1]
    span = d1 - d0 if d1 > d0 else 1.0
    t_start = (start_d - d0) / span
    out.append(_interpolate_segment(pts[i0], pts[i0 + 1], t_start))
    # Middle points from i0+1 through i1-1
    for j in range(i0 + 1, i1):
        out.append(pts[j])
    # End boundary: end_d lies on segment (i1-1, i1); when i1 == n use last point
    if i1 > 0 and i1 < n:
        d0, d1 = cum_m[i1 - 1], cum_m[i1]
        span = d1 - d0 if d1 > d0 else 1.0
        t_end = (end_d - d0) / span
        out.append(_interpolate_segment(pts[i1 - 1], pts[i1], t_end))
    elif i1 == n and n > 0:
        out.append(pts[n - 1])
    return LineString(out)


def _slice_linestring_by_interpolate(
    line: LineString, start_d: float, end_d: float
) -> LineString:
    """Fallback: build pts/cum_m from line and slice (no Shapely split)."""
    pts, cum_m = _line_to_pts_and_cum_m(line)
    return _slice_by_cumulative_distances(pts, cum_m, start_d, end_d)


def _edge_travel_time_seconds_postgis(
    stop_times: List[Dict],
    from_sid: str,
    to_sid: str,
) -> Optional[float]:
    """Scheduled travel time in seconds between two consecutive stops from PostGIS stop_times."""
    if not stop_times:
        return None
    by_stop: Dict[str, Dict] = {st["stop_id"]: st for st in stop_times}
    st_from = by_stop.get(from_sid)
    st_to = by_stop.get(to_sid)
    if not st_from or not st_to:
        return None

    def _pick_time(row: Dict, pref_field: str, alt_field: str) -> Optional[int]:
        t = (row.get(pref_field) or "").strip() if isinstance(row.get(pref_field), str) else ""
        if t:
            return parse_gtfs_time_to_seconds(t)
        t_alt = (row.get(alt_field) or "").strip() if isinstance(row.get(alt_field), str) else ""
        if t_alt:
            return parse_gtfs_time_to_seconds(t_alt)
        return None

    t_from = _pick_time(st_from, "departure_time", "arrival_time")
    t_to = _pick_time(st_to, "arrival_time", "departure_time")
    if t_from is None or t_to is None:
        return None
    dt = t_to - t_from
    if dt <= 0:
        return None
    return float(dt)


def build_graph_for_pattern_from_postgis(
    pattern_meta: "PatternMeta",
    date_ymd: str,
) -> GraphBuildResult:
    """
    Build a direction-aware graph from PostGIS data (patterns, pattern_stops, shapes_lines, stop_times).
    Returns the same GraphBuildResult structure as GraphBuilder.build_graph_for_pattern.
    """
    from . import db_access

    pid = pattern_meta.pattern_id
    stops = db_access.get_pattern_stops(pid)
    stop_ids = [s.stop_id for s in stops]
    n_stops = len(stop_ids)
    if n_stops < 2:
        g = nx.DiGraph()
        pattern = RoutePattern(
            pattern_id=pid,
            route_id=pattern_meta.route_id,
            direction_id=str(pattern_meta.direction_id) if pattern_meta.direction_id is not None else None,
            stop_ids=stop_ids,
            frequency=pattern_meta.frequency,
            representative_trip_id=pattern_meta.repr_trip_id,
            representative_shape_id=pattern_meta.repr_shape_id,
        )
        return GraphBuildResult(graph=g, edge_geometries={}, pattern=pattern, used_shape=False)

    shape_line: Optional[LineString] = None
    shape_pts: Optional[List[Tuple[float, float]]] = None
    shape_cum_m: Optional[List[float]] = None
    if pattern_meta.repr_shape_id:
        shape_line = db_access.get_shape_line(pattern_meta.repr_shape_id)
        if shape_line is not None and len(list(shape_line.coords)) >= 2:
            shape_pts, shape_cum_m = _line_to_pts_and_cum_m(shape_line)

    stop_times = db_access.get_stop_times_for_trip(pattern_meta.repr_trip_id)
    shape_dists: Dict[str, float] = {}
    for st in stop_times:
        sid = st.get("stop_id")
        dist = st.get("shape_dist_traveled")
        if sid and dist is not None:
            try:
                shape_dists[sid] = float(dist)
            except (TypeError, ValueError):
                pass

    g = nx.DiGraph()
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry] = {}

    # Nodes
    for seq, stop in enumerate(stops):
        node_id = pattern_stop_node_id(pid, stop.stop_id, seq)
        g.add_node(
            node_id,
            pattern_id=pid,
            route_id=pattern_meta.route_id,
            direction_id=pattern_meta.direction_id,
            stop_id=stop.stop_id,
            stop_sequence=seq,
            stop_name=stop.name,
            lat=stop.lat,
            lon=stop.lon,
            out_heading_deg=None,
            frequency=pattern_meta.frequency,
        )

    # Edges
    for i in range(n_stops - 1):
        a = stop_ids[i]
        b = stop_ids[i + 1]
        stop_a = next((s for s in stops if s.stop_id == a), None)
        stop_b = next((s for s in stops if s.stop_id == b), None)
        if not stop_a or not stop_b:
            continue
        lat1, lon1 = stop_a.lat, stop_a.lon
        lat2, lon2 = stop_b.lat, stop_b.lon
        out_heading = bearing_deg(lat1, lon1, lat2, lon2)
        node_a = pattern_stop_node_id(pid, a, i)
        node_b = pattern_stop_node_id(pid, b, i + 1)
        if g.has_node(node_a):
            g.nodes[node_a]["out_heading_deg"] = out_heading

        if a in shape_dists and b in shape_dists:
            start_d, end_d = shape_dists[a], shape_dists[b]
            if end_d > start_d:
                distance_m = end_d - start_d
            else:
                distance_m = haversine_meters(lat1, lon1, lat2, lon2)
        else:
            distance_m = haversine_meters(lat1, lon1, lat2, lon2)

        travel_time_s = _edge_travel_time_seconds_postgis(stop_times, a, b)
        if travel_time_s is None:
            travel_time_s = distance_m / 8.33 if distance_m > 0 else 1.0

        geom = _build_edge_geom_postgis(
            a, b, lat1, lon1, lat2, lon2,
            shape_line, shape_dists, shape_pts, shape_cum_m,
        )
        edge_geoms[(node_a, node_b)] = EdgeGeometry(from_stop_id=a, to_stop_id=b, linestring=geom)
        g.add_edge(
            node_a,
            node_b,
            weight=travel_time_s,
            travel_time_s=travel_time_s,
            distance_m=distance_m,
        )

    # Last stop out_heading
    if n_stops >= 2:
        last_sid = stop_ids[n_stops - 1]
        prev_sid = stop_ids[n_stops - 2]
        stop_prev = next((s for s in stops if s.stop_id == prev_sid), None)
        stop_last = next((s for s in stops if s.stop_id == last_sid), None)
        if stop_prev and stop_last:
            incoming = bearing_deg(
                stop_prev.lat, stop_prev.lon,
                stop_last.lat, stop_last.lon,
            )
            last_node = pattern_stop_node_id(pid, last_sid, n_stops - 1)
            if g.has_node(last_node):
                g.nodes[last_node]["out_heading_deg"] = incoming

    pattern = RoutePattern(
        pattern_id=pid,
        route_id=pattern_meta.route_id,
        direction_id=str(pattern_meta.direction_id) if pattern_meta.direction_id is not None else None,
        stop_ids=stop_ids,
        frequency=pattern_meta.frequency,
        representative_trip_id=pattern_meta.repr_trip_id,
        representative_shape_id=pattern_meta.repr_shape_id,
    )
    return GraphBuildResult(
        graph=g,
        edge_geometries=edge_geoms,
        pattern=pattern,
        used_shape=shape_line is not None,
    )


def _build_edge_geom_postgis(
    from_sid: str,
    to_sid: str,
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    shape_line: Optional[LineString],
    shape_dists: Dict[str, float],
    shape_pts: Optional[List[Tuple[float, float]]] = None,
    shape_cum_m: Optional[List[float]] = None,
) -> LineString:
    """Same logic as GraphBuilder._build_edge_geom for PostGIS path."""
    if not shape_line:
        return LineString([(lon1, lat1), (lon2, lat2)])
    if from_sid not in shape_dists or to_sid not in shape_dists:
        return LineString([(lon1, lat1), (lon2, lat2)])
    start_d = shape_dists[from_sid]
    end_d = shape_dists[to_sid]
    if start_d >= end_d:
        return LineString([(lon1, lat1), (lon2, lat2)])
    if shape_pts is not None and shape_cum_m is not None:
        try:
            return _slice_by_cumulative_distances(shape_pts, shape_cum_m, start_d, end_d)
        except Exception:
            pass
    try:
        return _slice_linestring_by_interpolate(shape_line, start_d, end_d)
    except Exception:
        return LineString([(lon1, lat1), (lon2, lat2)])

