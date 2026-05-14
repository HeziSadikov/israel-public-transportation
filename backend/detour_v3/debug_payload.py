"""Build debug payloads for detour v3 responses."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


def build_debug_payload(
    *,
    pattern_segment_ids: Sequence[int],
    banned_segment_count: int,
    banned_turn_pair_count: int,
    attempts: Optional[List[Dict[str, Any]]] = None,
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "engine": "detour_v3",
        "pattern_osm_segment_ids": [int(x) for x in pattern_segment_ids],
        "banned_segments": int(banned_segment_count),
        "banned_turn_pairs": int(banned_turn_pair_count),
        "attempts": list(attempts or []),
        "notes": list(notes or []),
    }


__all__ = ["build_debug_payload"]
