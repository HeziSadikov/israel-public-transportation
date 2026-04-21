"""
Precompute legal divergence / rejoin anchor candidates along a Valhalla-matched path.

Uses intersecting_edges at each edge's end_node (from /trace_attributes) plus optional
PostGIS osm_turn_restrictions and osm_road_segments for turn legality.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


def _heading_diff_deg(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def _road_class_rank(rc: Optional[str]) -> float:
    if not rc:
        return 1.0
    order = (
        "motorway",
        "trunk",
        "primary",
        "secondary",
        "tertiary",
        "unclassified",
        "residential",
        "service_other",
    )
    try:
        return float(order.index(str(rc))) if rc in order else 4.0
    except ValueError:
        return 4.0


_BAD_USE = frozenset(
    {
        "footway",
        "steps",
        "sidewalk",
        "track",
        "alley",
        "parking_aisle",
        "driveway",
        "cycleway",
        "mountain_bike",
        "other",
    }
)


def _intersecting_allows_bus(inter: Dict[str, Any]) -> bool:
    u = inter.get("use")
    if isinstance(u, str) and u in _BAD_USE:
        return False
    drv = inter.get("driveability")
    if drv is not None and drv not in ("forward", "both"):
        return False
    return True


def _is_corridor_continuation(
    inter: Dict[str, Any],
    next_edge: Optional[Dict[str, Any]],
    *,
    heading_tol_deg: float = 14.0,
) -> bool:
    """True if this intersecting edge is the continuation along the matched trace (not a side exit)."""
    if inter.get("to_edge_name_consistency") is True:
        return True
    nbh = None
    if next_edge is not None:
        nbh = next_edge.get("begin_heading")
        if nbh is None:
            inn = next_edge.get("edge")
            if isinstance(inn, dict):
                nbh = inn.get("begin_heading")
    ibh = inter.get("begin_heading")
    if ibh is not None and nbh is not None:
        try:
            if _heading_diff_deg(float(ibh), float(nbh)) <= heading_tol_deg:
                return True
        except (TypeError, ValueError):
            pass
    return False


def _coord_at_shape_index(shape_lonlat: Sequence[Tuple[float, float]], idx: int) -> Optional[Tuple[float, float]]:
    if not shape_lonlat:
        return None
    if idx < 0:
        idx = 0
    if idx >= len(shape_lonlat):
        idx = len(shape_lonlat) - 1
    return (float(shape_lonlat[idx][0]), float(shape_lonlat[idx][1]))


def _cum_dist_at_edge_ends(edges: List[Dict[str, Any]]) -> List[float]:
    """Cumulative distance (m) at the end of each edge along the matched path."""
    cum: List[float] = []
    acc = 0.0
    for e in edges:
        km = float(e.get("length") or 0.0)
        acc += km * 1000.0
        cum.append(acc)
    return cum


def _lookup_osm_way_for_intersecting(
    conn: Any,
    *,
    via_node_id: int,
    begin_heading: float,
    tol_deg: float = 22.0,
) -> Optional[int]:
    """Best-effort: resolve outgoing way_id at node matching Valhalla begin_heading."""
    if not via_node_id:
        return None
    from backend.infra import db_access as db

    close = False
    if conn is None:
        conn = db._get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT osm_way_id, heading_start_deg
                FROM osm_road_segments
                WHERE from_node_id = %s
                """,
                (int(via_node_id),),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            best_wid: Optional[int] = None
            best_d = 1e9
            for r in rows:
                h = r["heading_start_deg"] if hasattr(r, "keys") else r[1]
                wid = r["osm_way_id"] if hasattr(r, "keys") else r[0]
                if h is None or wid is None:
                    continue
                try:
                    d = _heading_diff_deg(float(h), float(begin_heading))
                except (TypeError, ValueError):
                    continue
                if d < best_d:
                    best_d = d
                    best_wid = int(wid)
            if best_wid is not None and best_d <= tol_deg:
                return best_wid
            return None
    except Exception:
        return None
    finally:
        if close:
            conn.close()


def _turn_restricted(
    conn: Any,
    *,
    from_way_id: int,
    via_node_id: int,
    to_way_id: int,
) -> bool:
    if not from_way_id or not via_node_id or not to_way_id:
        return False
    from backend.infra import db_access as db

    close = False
    if conn is None:
        conn = db._get_conn()
        close = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM osm_turn_restrictions
                WHERE from_way_id = %s AND via_node_id = %s AND to_way_id = %s
                LIMIT 1
                """,
                (int(from_way_id), int(via_node_id), int(to_way_id)),
            )
            return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        if close:
            conn.close()


def score_legal_exit_intersection(
    inter: Dict[str, Any],
    *,
    next_edge: Optional[Dict[str, Any]],
    incoming_way_id: int,
    end_osm_node_id: Optional[int],
    conn: Any,
    node_traffic_signal: Optional[bool] = None,
) -> Optional[float]:
    """
    Return a higher-is-better score for a non-continuation intersecting edge, or None to reject.
    """
    if _is_corridor_continuation(inter, next_edge):
        return None
    if not _intersecting_allows_bus(inter):
        return None
    rc = inter.get("road_class")
    base = 10.0 - min(8.0, _road_class_rank(str(rc) if rc else None))
    if node_traffic_signal:
        base += 0.5
    ih = inter.get("begin_heading")
    to_wid: Optional[int] = None
    if end_osm_node_id and ih is not None:
        try:
            to_wid = _lookup_osm_way_for_intersecting(conn, via_node_id=end_osm_node_id, begin_heading=float(ih))
        except (TypeError, ValueError):
            to_wid = None
    if (
        to_wid is not None
        and incoming_way_id
        and end_osm_node_id
        and _turn_restricted(conn, from_way_id=incoming_way_id, via_node_id=end_osm_node_id, to_way_id=to_wid)
    ):
        return None
    return base


def build_exit_candidate_records(
    edges: List[Dict[str, Any]],
    shape_lonlat: Optional[Sequence[Tuple[float, float]]],
    *,
    conn: Any = None,
    role: str = "exit",
) -> List[Dict[str, Any]]:
    """
    One record per legal divergence opportunity (non-continuation intersecting edge at an end_node).
    role: 'exit' (forward trace) or used internally for reverse as 'rejoin' after distance remap.
    """
    if not edges:
        return []
    cum_end = _cum_dist_at_edge_ends(edges)
    out: List[Dict[str, Any]] = []
    for i, e in enumerate(edges):
        next_e = edges[i + 1] if i + 1 < len(edges) else None
        way_in = int(e.get("way_id") or 0)
        end_n = e.get("end_osm_node_id")
        if end_n is None:
            en = e.get("end_node")
            if isinstance(en, dict):
                end_n = en.get("node_id")
        try:
            end_osm_node_id = int(end_n) if end_n is not None else None
        except (TypeError, ValueError):
            end_osm_node_id = None
        enode = e.get("end_node")
        if not isinstance(enode, dict):
            enode = {}
        inters = enode.get("intersecting_edges")
        if not isinstance(inters, list) or not inters:
            continue
        ei_b = e.get("begin_shape_index")
        ei_e = e.get("end_shape_index")
        try:
            esi = int(ei_e) if ei_e is not None else None
        except (TypeError, ValueError):
            esi = None
        if shape_lonlat and esi is not None:
            ll = _coord_at_shape_index(shape_lonlat, esi)
        else:
            ll = None
        if ll is None and shape_lonlat and cum_end:
            approx_idx = min(len(shape_lonlat) - 1, max(0, int((cum_end[i] / max(cum_end[-1], 1.0)) * (len(shape_lonlat) - 1))))
            ll = (float(shape_lonlat[approx_idx][0]), float(shape_lonlat[approx_idx][1]))
        dist_m = cum_end[i] if cum_end else 0.0

        node_sig = enode.get("traffic_signal")
        for inter in inters:
            if not isinstance(inter, dict):
                continue
            sc = score_legal_exit_intersection(
                inter,
                next_edge=next_e,
                incoming_way_id=way_in,
                end_osm_node_id=end_osm_node_id,
                conn=conn,
                node_traffic_signal=bool(node_sig) if node_sig is not None else None,
            )
            if sc is None:
                continue
            if ll is None:
                continue
            rec = {
                "role": role,
                "shape_dist_m": float(dist_m),
                "lon": float(ll[0]) if ll else None,
                "lat": float(ll[1]) if ll else None,
                "osm_node_id": end_osm_node_id,
                "incoming_way_id": way_in or None,
                "score": float(sc),
                "trace_meta": {
                    "edge_index": i,
                    "intersecting_begin_heading": inter.get("begin_heading"),
                    "intersecting_use": inter.get("use"),
                    "intersecting_road_class": inter.get("road_class"),
                },
            }
            out.append(rec)
    return out


def build_rejoin_candidates_from_reverse_trace(
    edges_rev: List[Dict[str, Any]],
    shape_lonlat_rev: Optional[Sequence[Tuple[float, float]]],
    total_path_m: float,
    *,
    conn: Any = None,
) -> List[Dict[str, Any]]:
    """
    Rejoin candidates: run exit logic on reversed path; map shape_dist_m to forward frame.
    """
    raw = build_exit_candidate_records(edges_rev, shape_lonlat_rev, conn=conn, role="rejoin_raw")
    out: List[Dict[str, Any]] = []
    for r in raw:
        r2 = dict(r)
        d_rev = float(r2["shape_dist_m"])
        r2["role"] = "rejoin"
        r2["shape_dist_m"] = max(0.0, float(total_path_m) - d_rev)
        out.append(r2)
    return out


def merge_and_rank_records(
    exit_rows: List[Dict[str, Any]],
    rejoin_rows: List[Dict[str, Any]],
    *,
    per_role_limit: int = 24,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Sort by score descending, dedupe nearby same node, take top per_role_limit each; set rank_in_role."""

    def _dedupe_rank(rows: List[Dict[str, Any]], *, role: str) -> List[Dict[str, Any]]:
        rows = sorted(rows, key=lambda x: float(x.get("score") or 0.0), reverse=True)
        seen: set[Tuple[Optional[int], int]] = set()
        out: List[Dict[str, Any]] = []
        for r in rows:
            node = r.get("osm_node_id")
            dm = int(round(float(r.get("shape_dist_m") or 0.0) / 25.0))
            key = (node, dm)
            if key in seen:
                continue
            seen.add(key)
            rr = dict(r)
            rr["role"] = role
            rr["rank_in_role"] = len(out)
            out.append(rr)
            if len(out) >= per_role_limit:
                break
        return out

    return _dedupe_rank(exit_rows, role="exit"), _dedupe_rank(rejoin_rows, role="rejoin")
