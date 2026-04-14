from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class GTFSFeed:
    """
    Lightweight in-memory GTFS container used by domain services and tests.

    The project currently hydrates this structure from PostGIS or test fixtures
    instead of parsing raw GTFS files directly in this module.
    """

    version_id: str
    source_path: Optional[Path] = None
    routes: List[Dict[str, Any]] = field(default_factory=list)
    trips: List[Dict[str, Any]] = field(default_factory=list)
    stop_times: List[Dict[str, Any]] = field(default_factory=list)
    stops: List[Dict[str, Any]] = field(default_factory=list)
    calendar_dates: List[Dict[str, Any]] = field(default_factory=list)
    calendar: List[Dict[str, Any]] = field(default_factory=list)
    shapes: List[Dict[str, Any]] = field(default_factory=list)
    agencies: List[Dict[str, Any]] = field(default_factory=list)

