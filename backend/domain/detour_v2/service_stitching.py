"""GTFS stop service classification relative to detour anchors."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

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
    # Optional: mark skipped as served if detour passes nearby — omitted without detour geometry here
    stitch_ok = rejoin_idx > exit_idx
    if not stitch_ok:
        notes.append("rejoin_not_downstream")

    return StitchingResult(
        exit_stop_index=exit_idx,
        rejoin_stop_index=rejoin_idx,
        served_stop_ids=list(dict.fromkeys(served_before + served_after)),
        skipped_stop_ids=skipped,
        skipped_reasons={s: "between_anchors" for s in skipped},
        stitch_ok=stitch_ok,
        stitch_notes=notes,
    )
