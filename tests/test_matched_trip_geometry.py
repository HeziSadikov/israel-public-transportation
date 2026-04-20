"""Unit tests for MatchedTripPhysical concatenation (no DB)."""

from __future__ import annotations

from shapely.geometry import LineString

from backend.domain.detour_physical.matched_trip_geometry import (
    MatchedTripPhysical,
    concatenate_matched_summaries,
)
from backend.domain.detour_v2.policy import DetourPolicyConfig, PhysicalPathPolicy


def test_concatenate_two_segments_dedupes_join():
    a = LineString([(34.0, 31.0), (34.01, 31.0)])
    b = LineString([(34.01, 31.0), (34.02, 31.0)])
    rows = [
        {"matched_geom": a, "is_ambiguous": False, "confidence": 0.9, "mean_offset_m": 10.0},
        {"matched_geom": b, "is_ambiguous": False, "confidence": 0.9, "mean_offset_m": 10.0},
    ]
    line, cov, amb, weak, reason, meta = concatenate_matched_summaries(
        rows, pair_count_expected=2, min_summary_confidence=0.35, max_mean_offset_m=35.0
    )
    assert reason is None
    assert cov == 1.0
    assert amb == 0
    assert line is not None
    assert len(list(line.coords)) == 3


def test_matched_trip_passes_policy():
    pol = DetourPolicyConfig(physical_path=PhysicalPathPolicy())
    m = MatchedTripPhysical(
        line=LineString([(0, 0), (1, 0)]),
        coverage_ratio=0.9,
        ambiguous_stop_pairs=0,
        weak_stop_pairs=0,
    )
    assert m.passes_path_thresholds(pol.physical_path)
    assert m.passes_anchor_thresholds(pol.physical_path)
