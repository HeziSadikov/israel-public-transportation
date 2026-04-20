"""Dataclasses for GTFS edge ↔ OSM chain matching."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class EdgeMatchScore:
    """Aggregate quality of a matched directed edge chain."""

    continuity: float = 0.0
    coverage_ratio: float = 0.0
    mean_offset_m: float = 0.0
    mean_heading_error_deg: float = 0.0
    side_switch_count: int = 0
    total: float = 0.0


@dataclass
class EdgeMatchResult:
    """Result of map-matching a GTFS shape slice to the road graph."""

    success: bool
    edge_records: List[Dict[str, Any]] = field(default_factory=list)
    score: Optional[EdgeMatchScore] = None
    is_ambiguous: bool = False
    notes: List[str] = field(default_factory=list)
