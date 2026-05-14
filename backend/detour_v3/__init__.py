"""
Detour v3 runtime engine (M5 / M6).

Replaces runtime Valhalla `/route` with own A* over ``osm_road_segments`` +
``osm_segment_turns`` + bus-corridor evidence. The legacy ``detour_v2``
package is kept intact as the fallback engine.
"""


from .compute import POLICY_VERSION_V3, compute_detour_for_trip

__all__ = ["POLICY_VERSION_V3", "compute_detour_for_trip"]
