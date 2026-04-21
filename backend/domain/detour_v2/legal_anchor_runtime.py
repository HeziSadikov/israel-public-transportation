"""Runtime selection of precomputed legal anchor pairs for detour v2."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .models import AnchorPair
from .policy import DetourPolicyConfig


def load_index_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    exits: List[Dict[str, Any]] = []
    rejoins: List[Dict[str, Any]] = []
    for r in rows:
        role = (r.get("role") or "").strip().lower()
        if role == "exit":
            exits.append(r)
        elif role == "rejoin":
            rejoins.append(r)
    return exits, rejoins


def select_legal_anchor_pair(
    exit_rows: List[Dict[str, Any]],
    rejoin_rows: List[Dict[str, Any]],
    *,
    blocked_start_m: float,
    blocked_end_m: float,
    total_shape_m: float,
    policy: DetourPolicyConfig,
) -> Optional[AnchorPair]:
    """
    Pick best-scoring (exit, rejoin) with distances in the same windows as enumerate_anchor_candidates.
    """
    ap = policy.anchor
    gap = float(ap.min_anchor_gap_m)
    bs, be = float(blocked_start_m), float(blocked_end_m)
    exit_lo = max(0.0, bs - float(ap.search_before_window_m))
    exit_hi = max(0.0, bs - gap)
    rejoin_lo = min(float(total_shape_m), be + gap)
    rejoin_hi = min(float(total_shape_m), be + float(ap.search_after_window_m))

    best: Optional[Tuple[float, AnchorPair]] = None
    for ex in exit_rows:
        dx = float(ex.get("shape_dist_m") or 0.0)
        if not (exit_lo <= dx <= exit_hi):
            continue
        sx = float(ex.get("score") or 0.0)
        for rj in rejoin_rows:
            dr = float(rj.get("shape_dist_m") or 0.0)
            if not (rejoin_lo <= dr <= rejoin_hi):
                continue
            if dx >= dr:
                continue
            sr = float(rj.get("score") or 0.0)
            combo = sx + sr
            lon_e = ex.get("lon")
            lat_e = ex.get("lat")
            lon_r = rj.get("lon")
            lat_r = rj.get("lat")
            if lon_e is None or lat_e is None or lon_r is None or lat_r is None:
                continue
            pair = AnchorPair(
                exit_lon=float(lon_e),
                exit_lat=float(lat_e),
                rejoin_lon=float(lon_r),
                rejoin_lat=float(lat_r),
                exit_stop_id=None,
                rejoin_stop_id=None,
                exit_shape_dist_m=dx,
                rejoin_shape_dist_m=dr,
                anchor_quality_note="legal_index",
                anchor_geometry_source="matched_physical",
                anchor_source="legal_index",
            )
            if best is None or combo > best[0]:
                best = (combo, pair)
    return best[1] if best else None
