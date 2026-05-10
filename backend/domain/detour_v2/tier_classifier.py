"""
Deterministic tier + confidence from decoded path, feasibility warnings, and stitching.

Tiers (highest to lowest trust):
- EMERGENCY_FALLBACK: only set by orchestrator when using last-resort geometry.
- AUTO_OK: major-road dominated, no skipped stops, stitching ok, low warning burden.
- REVIEW_RECOMMENDED: acceptable primary network but skipped stops or mild uncertainty.
- LOW_CONFIDENCE: significant local/unknown/synthetic share or many warnings.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from .models import DecodedCandidate, DetourTier, FeasibilityResult, RoadSegmentRef, StitchingResult
from .bus_feasibility_evaluator import MAJOR, LOCAL


def _major_length_fraction(segments: List[RoadSegmentRef]) -> float:
    total = sum(s.length_m for s in segments) or 1.0
    major_m = sum(s.length_m for s in segments if (s.highway or "").lower() in MAJOR)
    return major_m / total


def _local_unknown_fraction(segments: List[RoadSegmentRef]) -> Tuple[float, float]:
    """Returns (local_fraction, synthetic_fraction)."""
    total = sum(s.length_m for s in segments) or 1.0
    local_m = sum(s.length_m for s in segments if (s.highway or "").lower() in LOCAL)
    syn_m = sum(s.length_m for s in segments if s.synthetic)
    return local_m / total, syn_m / total


def classify_tier(
    *,
    feasibility: FeasibilityResult,
    decoded: Optional[DecodedCandidate],
    stitch: Optional[StitchingResult] = None,
    is_emergency_fallback: bool = False,
    gtfs_evidence_way_fraction: float = 0.0,
    stitch_ok: Optional[bool] = None,
    skipped_stop_count: Optional[int] = None,
) -> Tuple[DetourTier, float, List[str], List[str], bool]:
    """
    Returns (tier, confidence_score 0..1, merged_warnings, hard_constraints_passed, review_required).

    gtfs_evidence_way_fraction: share of decoded distance on ways with gtfs_bus_way_evidence.
    """
    ok_downstream = stitch.stitch_ok if stitch is not None else bool(stitch_ok)
    skipped_n_eff = len(stitch.skipped_stop_ids) if stitch is not None else int(skipped_stop_count or 0)

    hard_passed: List[str] = []
    if feasibility.accepted and "path_intersects_blockage" not in feasibility.hard_reject_reasons:
        hard_passed.append("avoids_blockage")
    if feasibility.accepted and "wrong_direction_rejoin" not in feasibility.hard_reject_reasons:
        hard_passed.append("maintains_route_direction")
    if ok_downstream:
        hard_passed.append("rejoins_downstream")
    if decoded and decoded.road_segments:
        hard_passed.append("valid_geometry")

    warnings = list(feasibility.warnings)
    notes_as_warn = [f"note:{n}" for n in (feasibility.notes or []) if n and not n.startswith("backtrack_heading")]
    warnings.extend(notes_as_warn)

    if is_emergency_fallback:
        w = list(warnings)
        w.append("emergency_fallback_geometry")
        return "EMERGENCY_FALLBACK", max(0.05, feasibility.confidence_score * 0.5), w, hard_passed, True

    if not feasibility.accepted:
        # Should not be tier-classified for display as winner; still return LOW_CONFIDENCE shell
        return (
            "LOW_CONFIDENCE",
            0.0,
            list(warnings) + list(feasibility.hard_reject_reasons),
            hard_passed,
            True,
        )

    segs = list(decoded.road_segments) if decoded else []
    major_f = _major_length_fraction(segs) if segs else 0.0
    lf, sf = _local_unknown_fraction(segs) if segs else (0.0, 0.0)
    skipped_n = skipped_n_eff
    warn_count = len(warnings)

    conf = float(feasibility.confidence_score)
    conf = max(0.0, min(1.0, conf - 0.08 * warn_count - 0.12 * skipped_n - 0.15 * sf - 0.1 * lf))

    tier: DetourTier = "REVIEW_RECOMMENDED"
    review = True

    # AUTO_OK thresholds (deterministic, documented)
    if (
        major_f >= 0.80
        and skipped_n == 0
        and stitch is not None
        and stitch.stitch_ok
        and lf <= 0.12
        and sf <= 0.05
        and warn_count == 0
        and gtfs_evidence_way_fraction >= 0.35
    ):
        tier = "AUTO_OK"
        review = False
        conf = min(1.0, conf + 0.1)
    elif major_f >= 0.55 and skipped_n <= 2 and lf <= 0.25 and sf <= 0.12 and warn_count <= 3:
        tier = "REVIEW_RECOMMENDED"
        review = skipped_n > 0 or warn_count > 0 or gtfs_evidence_way_fraction < 0.25
    else:
        tier = "LOW_CONFIDENCE"
        review = True

    return tier, conf, warnings, hard_passed, review
