"""
Virtual junction nodes on detour ride edges: split corridor LineStrings at crossings
or geocoded snap points so A* can route through intersections that are not GTFS stops.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import networkx as nx
from shapely.geometry import LineString, Point, shape
from shapely.ops import nearest_points
from shapely.strtree import STRtree

from .graph_builder import EdgeGeometry, bearing_deg, haversine_meters


def _polyline_cumdist_m(coords: Sequence[Tuple[float, float]]) -> List[float]:
    if not coords:
        return []
    cum: List[float] = [0.0]
    for i in range(1, len(coords)):
        lon1, lat1 = coords[i - 1][0], coords[i - 1][1]
        lon2, lat2 = coords[i][0], coords[i][1]
        cum.append(cum[-1] + haversine_meters(lat1, lon1, lat2, lon2))
    return cum


def _project_lonlat_onto_polyline(
    coords: Sequence[Tuple[float, float]],
    cum_m: Sequence[float],
    lon: float,
    lat: float,
) -> Tuple[float, float, float, float, float]:
    """
    Project (lon,lat) onto the polyline. Returns:
    (dist_along_m, proj_lon, proj_lat, frac_on_segment, segment_index).
    """
    if len(coords) < 2:
        return 0.0, float(coords[0][0]), float(coords[0][1]), 0.0, 0
    best_d = float("inf")
    best_along = 0.0
    best_plon, best_plat = coords[0][0], coords[0][1]
    best_frac = 0.0
    best_seg = 0
    for i in range(len(coords) - 1):
        lon1, lat1 = coords[i][0], coords[i][1]
        lon2, lat2 = coords[i + 1][0], coords[i + 1][1]
        # Project onto segment in Euclidean lon/lat (short segments).
        dx, dy = lon2 - lon1, lat2 - lat1
        seg_len2 = dx * dx + dy * dy
        if seg_len2 < 1e-24:
            t = 0.0
        else:
            t = ((lon - lon1) * dx + (lat - lat1) * dy) / seg_len2
            t = max(0.0, min(1.0, t))
        plon = lon1 + t * (lon2 - lon1)
        plat = lat1 + t * (lat2 - lat1)
        d = haversine_meters(lat, lon, plat, plon)
        if d < best_d:
            best_d = d
            seg_m = cum_m[i + 1] - cum_m[i]
            best_along = cum_m[i] + t * seg_m
            best_plon, best_plat = plon, plat
            best_frac = t
            best_seg = i
    return best_along, best_plon, best_plat, best_frac, float(best_seg)


def stable_junction_id(u: str, v: str, lon: float, lat: float) -> str:
    h = hashlib.sha256(f"{u}|{v}|{lon:.7f}|{lat:.7f}".encode()).hexdigest()[:16]
    return f"jx:{h}"


def _junction_out_heading(
    coords: Sequence[Tuple[float, float]],
    seg_idx: int,
    frac: float,
) -> float:
    """Bearing along corridor past the junction (toward end of segment / next vertex)."""
    if len(coords) < 2:
        return 0.0
    i = int(min(max(seg_idx, 0), len(coords) - 2))
    lon1, lat1 = coords[i][0], coords[i][1]
    lon2, lat2 = coords[i + 1][0], coords[i + 1][1]
    if frac > 0.5 and i + 2 < len(coords):
        lon2, lat2 = coords[i + 1][0], coords[i + 1][1]
        lon3, lat3 = coords[i + 2][0], coords[i + 2][1]
        return bearing_deg(lat2, lon2, lat3, lon3)
    return bearing_deg(lat1, lon1, lat2, lon2)


def _node_template_for_junction(g: nx.DiGraph, u: str, v: str) -> Dict[str, Any]:
    nu = dict(g.nodes[u])
    nv = dict(g.nodes[v])
    freq_u = float(nu.get("frequency") or 0.0)
    freq_v = float(nv.get("frequency") or 0.0)
    freq = max(freq_u, freq_v)
    return {
        "pattern_id": nu.get("pattern_id"),
        "route_id": nu.get("route_id"),
        "direction_id": nu.get("direction_id"),
        "stop_id": None,
        "stop_sequence": -1,
        "stop_name": None,
        "frequency": freq,
    }


def replace_ride_edge_with_junction_chain(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    u: str,
    v: str,
    split_lonlats: List[Tuple[float, float]],
    *,
    junction_reason: str,
    min_end_m: float = 5.0,
) -> bool:
    """
    Replace ride edge (u,v) with u -> j1 -> ... -> v; split travel_time_s and distance_m
    proportionally by haversine segment length.
    """
    if not g.has_edge(u, v):
        return False
    edata = dict(g[u][v])
    if edata.get("is_transfer"):
        return False
    eg = edge_geoms.get((u, v))
    if eg is None or eg.linestring is None:
        return False

    coords = [tuple(map(float, c)) for c in eg.linestring.coords]
    if len(coords) < 2:
        return False

    cum_m = _polyline_cumdist_m(coords)
    total_m = cum_m[-1]
    if total_m <= 0:
        return False

    tt = float(edata.get("travel_time_s", edata.get("weight", 0.0)))
    dist_edge = float(edata.get("distance_m", 0.0) or 0.0)
    if dist_edge <= 0:
        dist_edge = total_m

    projected: List[Tuple[float, float, float, float, float]] = []
    for lon, lat in split_lonlats:
        along, plon, plat, frac, seg_i = _project_lonlat_onto_polyline(coords, cum_m, lon, lat)
        if along < min_end_m or along > total_m - min_end_m:
            continue
        projected.append((along, plon, plat, frac, seg_i))

    projected.sort(key=lambda x: x[0])
    merged: List[Tuple[float, float, float, float, float]] = []
    for row in projected:
        if not merged:
            merged.append(row)
            continue
        if haversine_meters(row[2], row[1], merged[-1][2], merged[-1][1]) < 8.0:
            a0, x0, y0, f0, s0 = merged[-1]
            a1, x1, y1, f1, s1 = row
            merged[-1] = ((a0 + a1) / 2, (x0 + x1) / 2, (y0 + y1) / 2, (f0 + f1) / 2, s0)
        else:
            merged.append(row)

    if not merged:
        return False

    g.remove_edge(u, v)
    del edge_geoms[(u, v)]

    orig_from = eg.from_stop_id
    orig_to = eg.to_stop_id
    tmpl = _node_template_for_junction(g, u, v)

    node_sequence: List[str] = [u]

    for _along, plon, plat, frac, seg_i in merged:
        jid = stable_junction_id(u, v, plon, plat)
        while g.has_node(jid):
            jid = stable_junction_id(jid, v, plon, plat)
        oh = _junction_out_heading(coords, int(seg_i), frac)
        g.add_node(
            jid,
            lat=float(plat),
            lon=float(plon),
            out_heading_deg=oh,
            is_junction=True,
            junction_reason=junction_reason,
            pattern_id=tmpl["pattern_id"],
            route_id=tmpl["route_id"],
            direction_id=tmpl["direction_id"],
            stop_id=None,
            stop_sequence=-1,
            stop_name=None,
            frequency=tmpl["frequency"],
        )
        node_sequence.append(jid)

    node_sequence.append(v)

    def line_between(a: str, b: str) -> LineString:
        return LineString(
            [
                (float(g.nodes[a]["lon"]), float(g.nodes[a]["lat"])),
                (float(g.nodes[b]["lon"]), float(g.nodes[b]["lat"])),
            ]
        )

    seg_lens: List[float] = []
    for i in range(len(node_sequence) - 1):
        na, nb = node_sequence[i], node_sequence[i + 1]
        la = float(g.nodes[na]["lat"]), float(g.nodes[na]["lon"])
        lb = float(g.nodes[nb]["lat"]), float(g.nodes[nb]["lon"])
        seg_lens.append(haversine_meters(la[0], la[1], lb[0], lb[1]))
    tot_seg = sum(seg_lens) or 1.0

    for i in range(len(node_sequence) - 1):
        a, b = node_sequence[i], node_sequence[i + 1]
        w_rel = seg_lens[i] / tot_seg
        sub_tt = tt * w_rel
        sub_dist = dist_edge * w_rel
        sub_ls = line_between(a, b)
        g.add_edge(
            a,
            b,
            weight=sub_tt,
            travel_time_s=sub_tt,
            distance_m=sub_dist,
            is_transfer=False,
        )
        edge_geoms[(a, b)] = EdgeGeometry(
            from_stop_id=orig_from,
            to_stop_id=orig_to,
            linestring=sub_ls,
        )

    return True


@dataclass
class JunctionBuildConfig:
    crossing_enabled: bool = True
    near_miss_m: float = 35.0
    dedupe_cluster_m: float = 18.0
    geocode_snap_max_m: float = 85.0


def _ride_edge_keys(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for u, v, d in g.edges(data=True):
        if d.get("is_transfer"):
            continue
        if (u, v) not in edge_geoms:
            continue
        out.append((u, v))
    return out


def _pattern_id(g: nx.DiGraph, n: str) -> Optional[str]:
    return g.nodes[n].get("pattern_id")


def find_corridor_crossing_points(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    aoi_geom: Any,
    *,
    near_miss_m: float,
) -> List[Tuple[float, float, str, str]]:
    """
    For each pair of ride edges with different pattern_id, find intersection or near-miss
    point inside AOI. Returns list of (lon, lat, u, v) to split that edge.
    """
    ride = _ride_edge_keys(g, edge_geoms)
    items: List[Tuple[str, str, LineString, str]] = []
    for u, v in ride:
        eg = edge_geoms.get((u, v))
        if not eg or not eg.linestring or eg.linestring.is_empty:
            continue
        ls = eg.linestring
        pid = _pattern_id(g, u)
        if pid is None:
            continue
        try:
            if not ls.intersects(aoi_geom):
                # quick reject
                if ls.distance(aoi_geom) > 0.02:
                    continue
        except Exception:
            pass
        items.append((u, v, ls, pid))

    if len(items) < 2:
        return []

    boxes = [it[2].envelope for it in items]
    tree = STRtree(boxes)
    near_deg = max(near_miss_m / 85000.0, 2e-5)  # ~85 km/deg latitude
    results: List[Tuple[float, float, str, str]] = []

    for i in range(len(items)):
        u1, v1, ls1, p1 = items[i]
        candidates = tree.query(ls1.buffer(near_deg))
        for j in np.ravel(candidates):
            j = int(j)
            if j <= i:
                continue
            u2, v2, ls2, p2 = items[j]
            if p1 == p2:
                continue
            inter = ls1.intersection(ls2)
            pt: Optional[Point] = None
            if not inter.is_empty:
                if inter.geom_type == "Point":
                    pt = inter
                elif inter.geom_type == "MultiPoint":
                    geoms = getattr(inter, "geoms", None)
                    if geoms is not None:
                        pt = sorted(geoms, key=lambda p: (p.x, p.y))[0]
                elif inter.geom_type == "GeometryCollection":
                    for g0 in getattr(inter, "geoms", []):
                        if g0.geom_type == "Point":
                            pt = g0
                            break
            if pt is None:
                p_a, p_b = nearest_points(ls1, ls2)
                d_m = haversine_meters(p_a.y, p_a.x, p_b.y, p_b.x)
                if d_m <= near_miss_m:
                    mid_lon = (p_a.x + p_b.x) / 2
                    mid_lat = (p_a.y + p_b.y) / 2
                    pt = Point(mid_lon, mid_lat)
            if pt is None:
                continue
            try:
                if not aoi_geom.contains(pt) and not aoi_geom.intersects(pt.buffer(1e-4)):
                    # keep if close to aoi
                    if aoi_geom.distance(pt) > 0.01:
                        continue
            except Exception:
                pass
            results.append((float(pt.x), float(pt.y), u1, v1))
            results.append((float(pt.x), float(pt.y), u2, v2))

    return results


def _cluster_merge_splits(
    splits: List[Tuple[float, float, str, str]],
    dedupe_m: float,
) -> List[Tuple[float, float, str, str]]:
    """Merge split points that are close for the same (u,v)."""
    by_edge: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for lon, lat, u, v in splits:
        by_edge.setdefault((u, v), []).append((lon, lat))

    merged_out: List[Tuple[float, float, str, str]] = []
    for (u, v), pts in by_edge.items():
        kept: List[Tuple[float, float]] = []
        for lon, lat in pts:
            placed = False
            for i, (lx, ly) in enumerate(kept):
                if haversine_meters(lat, lon, ly, lx) <= dedupe_m:
                    kept[i] = ((lx + lon) / 2, (ly + lat) / 2)
                    placed = True
                    break
            if not placed:
                kept.append((lon, lat))
        for lon, lat in kept:
            merged_out.append((lon, lat, u, v))
    return merged_out


def apply_corridor_junctions(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    aoi_geojson: Dict[str, Any],
    cfg: JunctionBuildConfig,
) -> int:
    """Insert junction splits from pairwise corridor geometry. Returns count of edges replaced."""
    if not cfg.crossing_enabled:
        return 0
    try:
        aoi_geom = shape(aoi_geojson)
    except Exception:
        return 0
    raw = find_corridor_crossing_points(
        g, edge_geoms, aoi_geom, near_miss_m=cfg.near_miss_m
    )
    splits = _cluster_merge_splits(raw, cfg.dedupe_cluster_m)
    # Group by edge; process longer chains by sorting splits along edge — but edges are independent
    by_edge: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for lon, lat, u, v in splits:
        by_edge.setdefault((u, v), []).append((lon, lat))

    n_replaced = 0
    for (u, v), lonlats in by_edge.items():
        if replace_ride_edge_with_junction_chain(
            g,
            edge_geoms,
            u,
            v,
            lonlats,
            junction_reason="corridor_crossing",
        ):
            n_replaced += 1
    return n_replaced


def snap_geocode_points_to_edges(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    waypoints_lonlat: Sequence[Tuple[float, float]],
    max_snap_m: float,
) -> List[Tuple[float, float, str, str]]:
    """
    For each waypoint, find nearest ride edge and project onto it; if within max_snap_m,
    return (proj_lon, proj_lat, u, v).
    """
    ride = _ride_edge_keys(g, edge_geoms)
    out: List[Tuple[float, float, str, str]] = []
    for lon, lat in waypoints_lonlat:
        best: Optional[Tuple[float, float, str, str, float]] = None
        for eu, ev in ride:
            eg = edge_geoms.get((eu, ev))
            if not eg or not eg.linestring:
                continue
            coords = [tuple(map(float, c)) for c in eg.linestring.coords]
            if len(coords) < 2:
                continue
            cum = _polyline_cumdist_m(coords)
            _, plon, plat, _, _ = _project_lonlat_onto_polyline(coords, cum, lon, lat)
            d = haversine_meters(lat, lon, plat, plon)
            if best is None or d < best[4]:
                best = (plon, plat, eu, ev, d)
        if best is not None and best[4] <= max_snap_m:
            out.append((best[0], best[1], best[2], best[3]))
    return out


def apply_geocode_snap_junctions(
    g: nx.DiGraph,
    edge_geoms: Dict[Tuple[str, str], EdgeGeometry],
    waypoints_lonlat: Sequence[Tuple[float, float]],
    max_snap_m: float,
) -> int:
    pts = snap_geocode_points_to_edges(g, edge_geoms, waypoints_lonlat, max_snap_m)
    splits = [(plon, plat, eu, ev) for plon, plat, eu, ev in pts]
    splits = _cluster_merge_splits(splits, 12.0)
    by_edge: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for lon, lat, u, v in splits:
        by_edge.setdefault((u, v), []).append((lon, lat))
    n = 0
    for (u, v), lonlats in by_edge.items():
        if replace_ride_edge_with_junction_chain(
            g, edge_geoms, u, v, lonlats, junction_reason="geocode_snap"
        ):
            n += 1
    return n
