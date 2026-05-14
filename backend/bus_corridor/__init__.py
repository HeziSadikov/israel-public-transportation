"""
Detour v3 bus-corridor evidence layer (M4 / M5).

* ``match_patterns_to_osm`` — map-match each representative GTFS pattern to
  the directed ``osm_road_segments`` graph (Valhalla ``trace_attributes``
  bootstrap, then resolve to our own segment_id sequence).
* ``pattern_osm_path``     — read/write helpers for ``pattern_osm_segments``.
* ``build_bus_evidence``   — aggregate ``pattern_osm_segments`` into
  ``gtfs_bus_segment_evidence`` / ``gtfs_bus_turn_evidence``.

Implemented in M4 / M5.
"""

from .build_bus_evidence import build_bus_evidence
from .match_patterns_to_osm import (
    SOURCE_VALHALLA_TRACE,
    PatternOsmMatchResult,
    match_patterns_to_osm,
    match_single_pattern_to_osm,
)
from .pattern_osm_path import (
    clear_pattern_osm_path,
    load_pattern_osm_path,
    pattern_osm_path_row_count,
)

__all__ = [
    "SOURCE_VALHALLA_TRACE",
    "PatternOsmMatchResult",
    "build_bus_evidence",
    "clear_pattern_osm_path",
    "load_pattern_osm_path",
    "match_patterns_to_osm",
    "match_single_pattern_to_osm",
    "pattern_osm_path_row_count",
]
