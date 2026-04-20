"""Rank feasible candidates; lowest score wins."""

from __future__ import annotations

from typing import List, Optional

from .models import DecodedCandidate, FeasibilityResult, RankedCandidate
from .policy import DetourPolicyConfig


def rank_candidates(
    items: List[tuple[str, float, float, FeasibilityResult, dict, Optional[DecodedCandidate]]],
    policy: DetourPolicyConfig,
) -> List[RankedCandidate]:
    """
    items: (strategy, travel_time_s, distance_m, feasibility, score_breakdown_extra, decoded)
    Lowest score wins. Rejected candidates get total_score=inf but still carry a full breakdown.
    """
    ranked: List[RankedCandidate] = []
    for strategy, t_s, d_m, feas, extra, decoded in items:
        base_breakdown = {
            "travel_time_s": t_s,
            "segment_penalty_s": feas.segment_penalty_s,
            "turn_penalty_s": feas.turn_penalty_s,
            "uncertainty_penalty_s": feas.uncertainty_penalty_s,
            "service_penalty_s": feas.service_penalty_s,
            "evidence_bonus_s": feas.evidence_bonus_s,
            "sharp_turn_count": float(feas.sharp_turn_count),
        }
        base_breakdown.update(extra)

        if not feas.accepted:
            base_breakdown["reject"] = 1.0
            ranked.append(
                RankedCandidate(
                    strategy=strategy,
                    total_score=float("inf"),
                    travel_time_s=t_s,
                    distance_m=d_m,
                    decoded=decoded,
                    feasibility=feas,
                    rejection_reasons=list(feas.hard_reject_reasons),
                    score_breakdown=base_breakdown,
                )
            )
            continue
        total = (
            t_s
            + feas.segment_penalty_s
            + feas.turn_penalty_s
            + feas.uncertainty_penalty_s
            + feas.service_penalty_s
            + feas.evidence_bonus_s
        )
        base_breakdown["total_score"] = total
        ranked.append(
            RankedCandidate(
                strategy=strategy,
                total_score=total,
                travel_time_s=t_s,
                distance_m=d_m,
                decoded=decoded,
                feasibility=feas,
                rejection_reasons=[],
                score_breakdown=base_breakdown,
            )
        )
    ranked.sort(key=lambda r: (r.total_score, r.distance_m, r.strategy))
    return ranked
