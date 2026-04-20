"""Concatenate per-stop-pair matched geometries into one trip LineString (explicit travel order)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from shapely.geometry import LineString

if TYPE_CHECKING:
    from backend.domain.detour_v2.policy import PhysicalPathPolicy


@dataclass
class MatchedTripPhysical:
    """Result of loading persisted pattern_edge_match_summary chain for a trip."""

    line: Optional[LineString]
    coverage_ratio: float = 0.0
    ambiguous_stop_pairs: int = 0
    weak_stop_pairs: int = 0
    fallback_reason: Optional[str] = None
    per_pair: List[Dict[str, Any]] = field(default_factory=list)

    def passes_path_thresholds(self, pol: "PhysicalPathPolicy") -> bool:
        """Gate for using matched geometry for impact / trip-level logic."""
        return self._passes(pol, cov_req=pol.min_trip_coverage_ratio)

    def passes_anchor_thresholds(self, pol: "PhysicalPathPolicy") -> bool:
        """Stricter coverage gate before physical-first anchor selection."""
        return self._passes(pol, cov_req=pol.anchor_min_coverage_ratio)

    def _passes(self, pol: "PhysicalPathPolicy", *, cov_req: float) -> bool:
        if self.line is None or self.line.is_empty:
            return False
        if self.coverage_ratio < cov_req:
            return False
        if self.ambiguous_stop_pairs > pol.max_ambiguous_stop_pairs:
            return False
        if self.weak_stop_pairs > pol.max_weak_stop_pairs:
            return False
        return True


def concatenate_matched_summaries(
    ordered_summaries: List[Dict[str, Any]],
    *,
    pair_count_expected: int,
    min_summary_confidence: float = 0.35,
    max_mean_offset_m: float = 35.0,
) -> Tuple[Optional[LineString], float, int, int, Optional[str], List[Dict[str, Any]]]:
    """
    Build one LineString by appending each pair's matched_geom in order.
    Dedupe consecutive duplicate coordinates at joins. No PostGIS ST_LineMerge blind merge.
    Returns: (line, coverage_ratio, ambiguous_count, weak_count, fallback_reason, per_pair_meta)
    """
    per_meta: List[Dict[str, Any]] = []
    if pair_count_expected <= 0:
        return None, 0.0, 0, 0, "no_pairs", per_meta
    if not ordered_summaries:
        return None, 0.0, 0, 0, "no_summaries", per_meta

    merged: List[Tuple[float, float]] = []
    ambiguous = 0
    weak = 0
    present = 0
    cum_m = 0.0

    for row in ordered_summaries:
        g = row.get("matched_geom")
        amb = bool(row.get("is_ambiguous"))
        conf = row.get("confidence")
        mean_off = row.get("mean_offset_m")
        pair_weak = False
        if amb:
            ambiguous += 1
        if conf is not None and float(conf) < float(min_summary_confidence):
            pair_weak = True
        if mean_off is not None and float(mean_off) > float(max_mean_offset_m):
            pair_weak = True
        if pair_weak:
            weak += 1

        leg_len_m = 0.0
        if g is None:
            per_meta.append(
                {
                    **{k: row.get(k) for k in ("from_stop_sequence", "to_stop_sequence", "pattern_edge_id")},
                    "cum_dist_start_m": cum_m,
                    "cum_dist_end_m": cum_m,
                    "present": False,
                    "weak_pair": pair_weak,
                }
            )
            continue
        try:
            if hasattr(g, "coords"):
                geom = g
            else:
                from shapely import wkb

                if isinstance(g, memoryview):
                    geom = wkb.loads(bytes(g))
                elif isinstance(g, bytes):
                    geom = wkb.loads(g)
                else:
                    per_meta.append({"present": False, "weak_pair": pair_weak})
                    continue
            if geom.is_empty or geom.geom_type != "LineString":
                per_meta.append({"present": False, "weak_pair": pair_weak})
                continue
            pts = list(geom.coords)
            if len(pts) < 2:
                per_meta.append({"present": False, "weak_pair": pair_weak})
                continue
            present += 1
            from shapely.geometry import LineString as _LS

            leg_len_m = float(_LS(pts).length * 111_320.0)
            start_cum = cum_m
            for i, p in enumerate(pts):
                lon, lat = float(p[0]), float(p[1])
                if merged and merged[-1][0] == lon and merged[-1][1] == lat:
                    continue
                if merged and i == 0:
                    if abs(merged[-1][0] - lon) < 1e-9 and abs(merged[-1][1] - lat) < 1e-9:
                        continue
                merged.append((lon, lat))
            cum_m += leg_len_m
            per_meta.append(
                {
                    **{k: row.get(k) for k in ("from_stop_sequence", "to_stop_sequence", "pattern_edge_id")},
                    "entry_segment_id": row.get("entry_segment_id"),
                    "exit_segment_id": row.get("exit_segment_id"),
                    "cum_dist_start_m": start_cum,
                    "cum_dist_end_m": cum_m,
                    "present": True,
                    "weak_pair": pair_weak,
                }
            )
        except Exception:
            per_meta.append({"present": False, "weak_pair": pair_weak})
            continue

    cov = present / float(pair_count_expected) if pair_count_expected > 0 else 0.0
    if len(merged) < 2:
        return None, cov, ambiguous, weak, "insufficient_points", per_meta
    return LineString(merged), cov, ambiguous, weak, None, per_meta
