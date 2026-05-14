"""Score v3 A* detour candidates vs pattern backbone and length."""

from __future__ import annotations

from typing import Any, Dict, Sequence


def score_v3_path(
    *,
    detour_segment_ids: Sequence[int],
    pattern_segment_ids: Sequence[int],
    distance_m: float,
) -> tuple[float, Dict[str, float]]:
    """Return (total_score, breakdown) — higher is better (soft ranking)."""
    pat_set = set(int(x) for x in pattern_segment_ids)
    overlap = sum(1 for s in detour_segment_ids if int(s) in pat_set)
    overlap_r = overlap / max(1, len(detour_segment_ids))
    # Prefer shorter detours; reward reuse of corridor segments.
    score = -float(distance_m) * 0.01 + overlap_r * 250.0 + overlap * 3.0
    return score, {
        "overlap_count": float(overlap),
        "overlap_ratio": float(overlap_r),
        "distance_m": float(distance_m),
        "raw_score": float(score),
    }


def score_candidate(
    *,
    detour_segment_ids: Sequence[int],
    pattern_segment_ids: Sequence[int],
    distance_m: float,
) -> tuple[float, Dict[str, Any]]:
    """Public alias for :func:`score_v3_path` (M5 API)."""
    return score_v3_path(
        detour_segment_ids=detour_segment_ids,
        pattern_segment_ids=pattern_segment_ids,
        distance_m=distance_m,
    )


__all__ = ["score_v3_path", "score_candidate"]
