"""Hard validation for detour candidates (carriageway / direction plausibility)."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import DecodedCandidate, RoadSegmentRef


def _bearing_delta_deg(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def _probe_bearing(coords: List[Tuple[float, float]], from_start: bool, probe_m: float = 20.0) -> Optional[float]:
    if len(coords) < 2:
        return None
    seq = coords if from_start else list(reversed(coords))
    lon0, lat0 = seq[0]
    acc = 0.0
    for i in range(1, len(seq)):
        lon1, lat1 = seq[i]
        from shapely.geometry import LineString

        seg_m = float(LineString([(lon0, lat0), (lon1, lat1)]).length * 111_320.0)
        acc += seg_m
        if acc >= probe_m or i == len(seq) - 1:
            p = math.pi / 180.0
            y = math.sin((lon1 - lon0) * p) * math.cos(lat1 * p)
            x = math.cos(lat0 * p) * math.sin(lat1 * p) - math.sin(lat0 * p) * math.cos(lat1 * p) * math.cos((lon1 - lon0) * p)
            b = math.degrees(math.atan2(y, x))
            return (b + 360.0) % 360.0
        lon0, lat0 = lon1, lat1
    return None


def _segment_identity(seg: RoadSegmentRef) -> Tuple[str, int]:
    """Prefer DB segment_id when not synthetic; else way_id."""
    if not seg.synthetic and int(seg.segment_id) > 0 and int(seg.segment_id) < 900_000_000:
        return ("segment", int(seg.segment_id))
    return ("way", int(seg.osm_way_id or 0))


def validate_detour_carriageway(
    *,
    route_coords_lonlat: List[Tuple[float, float]],
    decoded: Optional[DecodedCandidate],
    expected_exit_bearing_deg: Optional[float],
    expected_rejoin_bearing_deg: Optional[float],
    max_entry_delta_deg: float = 95.0,
    max_rejoin_delta_deg: float = 95.0,
    expected_exit_segment_ids: Optional[Set[int]] = None,
    expected_rejoin_segment_ids: Optional[Set[int]] = None,
    blocked_segment_ids: Optional[Set[int]] = None,
    hard_reject_wrong_entry_exit_segment: bool = True,
) -> Tuple[bool, List[str]]:
    """
    Reject if the detour polyline starts/ends clearly opposite to the planned service direction
    on the exit/rejoin bearings (wrong carriageway / reverse).
    When segment ids are available on decoded edges and expected sets are non-empty, prefer segment identity.
    """
    reasons: List[str] = []
    if not route_coords_lonlat or len(route_coords_lonlat) < 2:
        return False, ["EMPTY_ROUTE_GEOMETRY"]

    start_b = _probe_bearing(route_coords_lonlat, from_start=True)
    end_b = _probe_bearing(route_coords_lonlat, from_start=False)

    if expected_exit_bearing_deg is not None and start_b is not None:
        if _bearing_delta_deg(start_b, expected_exit_bearing_deg) > max_entry_delta_deg:
            reasons.append("WRONG_ENTRY_CARRIAGEWAY")

    if expected_rejoin_bearing_deg is not None and end_b is not None:
        if _bearing_delta_deg(end_b, expected_rejoin_bearing_deg) > max_rejoin_delta_deg:
            reasons.append("WRONG_REJOIN_CARRIAGEWAY")

    segs = list(decoded.road_segments) if decoded else []
    if segs:
        first = segs[0]
        last = segs[-1]
        fk, fid = _segment_identity(first)
        lk, lid = _segment_identity(last)

        if blocked_segment_ids:
            for s in segs:
                if s.synthetic:
                    continue
                sk, sid = _segment_identity(s)
                if sk == "segment" and sid in blocked_segment_ids:
                    reasons.append("REENTERS_BLOCKED_SEGMENTS")
                    break

        if expected_exit_segment_ids and fk == "segment" and fid > 0:
            if fid not in expected_exit_segment_ids and hard_reject_wrong_entry_exit_segment:
                reasons.append("WRONG_ENTRY_SEGMENT")

        if expected_rejoin_segment_ids and lk == "segment" and lid > 0:
            if lid not in expected_rejoin_segment_ids and hard_reject_wrong_entry_exit_segment:
                reasons.append("WRONG_REJOIN_SEGMENT")

    _ = decoded
    return (len(reasons) == 0), reasons


def validation_codes_to_api(reasons: List[str]) -> List[Dict[str, Any]]:
    return [{"code": r, "severity": "hard"} for r in reasons]
