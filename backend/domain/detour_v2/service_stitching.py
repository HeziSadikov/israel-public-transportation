"""GTFS stop service classification relative to detour anchors and optional detour geometry."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString, Point

from .models import StitchingResult
from .policy import DetourPolicyConfig


def stops_with_dist(
    line: LineString,
    total_m: float,
    stop_rows: List[Dict[str, Any]],
    stop_lonlat: Dict[str, Tuple[float, float]],
) -> List[Tuple[str, int, float]]:
    """(stop_id, sequence, dist_m along shape)."""
    out: List[Tuple[str, int, float]] = []
    for row in stop_rows:
        sid = str(row.get("stop_id") or "")
        seq = int(row.get("stop_sequence") or 0)
        sdt = row.get("shape_dist_traveled")
        if sdt is not None:
            try:
                out.append((sid, seq, float(sdt)))
                continue
            except Exception:
                pass
        ll = stop_lonlat.get(sid)
        if not ll:
            continue
        lon, lat = ll
        try:
            d = float(line.project(Point(lon, lat)) / max(line.length, 1e-15) * total_m)
            out.append((sid, seq, d))
        except Exception:
            continue
    out.sort(key=lambda x: x[1])
    return out


def compute_stitching(
    *,
    line: LineString,
    total_m: float,
    stop_rows: List[Dict[str, Any]],
    stop_lonlat: Dict[str, Tuple[float, float]],
    exit_dist_m: float,
    rejoin_dist_m: float,
    policy: DetourPolicyConfig,
    detour_line: Optional[LineString] = None,
) -> StitchingResult:
    ordered = stops_with_dist(line, total_m, stop_rows, stop_lonlat)
    if not ordered:
        return StitchingResult(
            exit_stop_index=-1,
            rejoin_stop_index=-1,
            served_stop_ids=[],
            skipped_stop_ids=[],
            stitch_ok=True,
            skipped_reasons={},
            stitch_notes=["no_stops"],
            served_before_exit_ids=[],
            served_after_rejoin_ids=[],
        )

    exit_idx = -1
    rejoin_idx = -1
    for i, (_sid, _seq, d) in enumerate(ordered):
        if d <= exit_dist_m:
            exit_idx = i
    for i, (_sid, _seq, d) in enumerate(ordered):
        if d >= rejoin_dist_m:
            rejoin_idx = i
            break
    if exit_idx < 0:
        exit_idx = 0
    if rejoin_idx < 0:
        rejoin_idx = len(ordered) - 1
    if rejoin_idx <= exit_idx:
        rejoin_idx = min(len(ordered) - 1, exit_idx + 1)

    served_before = [ordered[i][0] for i in range(0, exit_idx + 1)]
    served_after = [ordered[i][0] for i in range(rejoin_idx, len(ordered))]
    skipped = [ordered[i][0] for i in range(exit_idx + 1, rejoin_idx)]

    notes: List[str] = []
    stitch_ok = rejoin_idx > exit_idx
    if not stitch_ok:
        notes.append("rejoin_not_downstream")

    skipped_reasons: Dict[str, str] = {s: "between_anchors" for s in skipped}

    # Rescue: detour path passes near skipped stop (no zig-zag heuristic — proximity only).
    radius_m = float(getattr(policy.service, "stop_service_radius_m", 70.0))
    if detour_line is not None and not detour_line.is_empty and skipped and stitch_ok:
        deg_per_m = 1.0 / 111_320.0
        rescued: List[str] = []
        still_skipped: List[str] = []
        for sid in skipped:
            ll = stop_lonlat.get(str(sid))
            if not ll:
                still_skipped.append(sid)
                continue
            try:
                pt = Point(ll[0], ll[1])
                dist_deg = float(detour_line.distance(pt))
                if dist_deg <= radius_m * deg_per_m:
                    rescued.append(sid)
                    notes.append(f"served_via_detour_proximity:{sid}")
                    skipped_reasons[sid] = "served_via_detour_proximity"
                else:
                    still_skipped.append(sid)
            except Exception:
                still_skipped.append(sid)
        skipped = still_skipped
        for s in rescued:
            if s in skipped_reasons and skipped_reasons[s] == "between_anchors":
                del skipped_reasons[s]
        served_merged = list(dict.fromkeys(served_before + rescued + served_after))
        return StitchingResult(
            exit_stop_index=exit_idx,
            rejoin_stop_index=rejoin_idx,
            served_stop_ids=served_merged,
            skipped_stop_ids=skipped,
            stitch_ok=stitch_ok,
            skipped_reasons=skipped_reasons,
            stitch_notes=notes,
            served_before_exit_ids=served_before,
            served_after_rejoin_ids=served_after,
        )

    return StitchingResult(
        exit_stop_index=exit_idx,
        rejoin_stop_index=rejoin_idx,
        served_stop_ids=list(dict.fromkeys(served_before + served_after)),
        skipped_stop_ids=skipped,
        stitch_ok=stitch_ok,
        skipped_reasons=skipped_reasons,
        stitch_notes=notes,
        served_before_exit_ids=served_before,
        served_after_rejoin_ids=served_after,
    )
