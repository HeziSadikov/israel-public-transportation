"""PostGIS access for pattern_edge / pattern_edge_match (physical layer)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from backend.domain.detour_physical.matched_trip_geometry import MatchedTripPhysical, concatenate_matched_summaries
from backend.domain.detour_v2.policy import PhysicalPathPolicy, get_default_policy
from backend.infra import db_access as db


def get_match_summary_for_pattern_stop_pair(
    pattern_id: str,
    from_stop_sequence: int,
    to_stop_sequence: int,
) -> Optional[Dict[str, Any]]:
    """Delegate to db_access; returns None if tables empty or no row."""
    return db.get_pattern_edge_match_summary_for_stop_pair(
        pattern_id, from_stop_sequence, to_stop_sequence
    )


def get_concatenated_matched_geom_for_trip(
    trip_id: str,
    *,
    policy: Optional[PhysicalPathPolicy] = None,
) -> MatchedTripPhysical:
    """
    Load ordered pattern_edge_match_summary rows and concatenate matched_geom in travel order.
    Policy thresholds are applied by callers (impact vs anchors may differ).
    """
    pol = policy or get_default_policy().physical_path
    ordered, n_pairs = db.fetch_pattern_edge_summaries_for_trip_ordered(trip_id)
    if n_pairs <= 0:
        return MatchedTripPhysical(
            line=None,
            coverage_ratio=0.0,
            ambiguous_stop_pairs=0,
            weak_stop_pairs=0,
            fallback_reason="no_stop_pairs",
            per_pair=[],
        )

    line, cov, amb, weak, reason, per_meta = concatenate_matched_summaries(
        ordered,
        pair_count_expected=n_pairs,
        min_summary_confidence=pol.min_summary_confidence,
        max_mean_offset_m=pol.max_mean_offset_m,
    )

    return MatchedTripPhysical(
        line=line,
        coverage_ratio=cov,
        ambiguous_stop_pairs=amb,
        weak_stop_pairs=weak,
        fallback_reason=reason,
        per_pair=per_meta,
    )
