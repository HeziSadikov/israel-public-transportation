"""
Pure tag → allow/deny rules for the detour v3 OSM importer.

These functions are unit-testable and have no DB / network side effects.
They encode the "bus-permissive-but-safe" policy from the plan:

  blocked if access=no/private, motor_vehicle=no, vehicle=no, bus=no, psv=no
  allowed if bus=yes/designated/permissive, psv=yes/designated/permissive,
          or a normal road class with non-restrictive access tags.

Only ``highway=*`` tags in :data:`HIGHWAY_ALLOWLIST` are written by default.
Other ways are skipped unless explicit ``bus``/``psv``/``access`` overrides
allow them and the highway class is not in :data:`HIGHWAY_HARD_BLOCKLIST`.
"""

from __future__ import annotations

from typing import Mapping, Optional


# v1 bus-usable highway classes (incl. link variants and bus-only ways).
HIGHWAY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "motorway",
        "motorway_link",
        "trunk",
        "trunk_link",
        "primary",
        "primary_link",
        "secondary",
        "secondary_link",
        "tertiary",
        "tertiary_link",
        "unclassified",
        "residential",
        "service",
        "living_street",
        "busway",
        "bus_guideway",
        "road",
    }
)

# Never useful for buses even with explicit access tags.
HIGHWAY_HARD_BLOCKLIST: frozenset[str] = frozenset(
    {
        "footway",
        "path",
        "cycleway",
        "steps",
        "pedestrian",
        "construction",
        "corridor",
        "elevator",
        "platform",
        "proposed",
        "raceway",
        "bridleway",
    }
)

# Access tag values that explicitly forbid vehicular use.
_ACCESS_DENY_VALUES: frozenset[str] = frozenset(
    {
        "no",
        "private",
        "forestry",
        "agricultural",
        "delivery",
        "customers",
        "permit",
        "military",
        "emergency",
    }
)

# Access tag values that explicitly allow buses / PSV.
_BUS_ALLOW_VALUES: frozenset[str] = frozenset(
    {"yes", "designated", "permissive", "official"}
)

# Access tag values that explicitly forbid buses / PSV.
_BUS_DENY_VALUES: frozenset[str] = frozenset({"no", "private", "permit"})


def _norm(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    return s or None


def is_highway_candidate(highway: Optional[str], tags: Mapping[str, str]) -> bool:
    """
    Return True if a way with this ``highway`` tag should be considered by
    the importer. Hard-blocklisted classes are rejected even with bus/psv
    overrides. Other unknown highway values are kept only when bus / psv /
    access explicitly opens them.
    """
    hw = _norm(highway)
    if not hw:
        return False
    if hw in HIGHWAY_HARD_BLOCKLIST:
        return False
    if hw in HIGHWAY_ALLOWLIST:
        return True

    # Unknown class → keep only if explicitly bus/psv/access-yes.
    bus = _norm(tags.get("bus"))
    psv = _norm(tags.get("psv"))
    access = _norm(tags.get("access"))
    if bus in _BUS_ALLOW_VALUES:
        return True
    if psv in _BUS_ALLOW_VALUES:
        return True
    if access in _BUS_ALLOW_VALUES:
        return True
    return False


def is_bus_usable(tags: Mapping[str, str]) -> bool:
    """
    Return True if a way's tags suggest a bus may use it.

    Order:
      1. bus=yes/designated/permissive  → True (override).
      2. psv=yes/designated/permissive  → True (override).
      3. access in {no, private, ...}   → False.
      4. motor_vehicle=no               → False.
      5. vehicle=no                     → False.
      6. bus=no / psv=no                → False.
      7. otherwise                      → True if highway is in allowlist.
    """
    bus = _norm(tags.get("bus"))
    psv = _norm(tags.get("psv"))
    access = _norm(tags.get("access"))
    motor_vehicle = _norm(tags.get("motor_vehicle"))
    vehicle = _norm(tags.get("vehicle"))

    if bus in _BUS_ALLOW_VALUES:
        return True
    if psv in _BUS_ALLOW_VALUES:
        return True

    if access in _ACCESS_DENY_VALUES:
        return False
    if motor_vehicle in _BUS_DENY_VALUES:
        return False
    if vehicle in _BUS_DENY_VALUES:
        return False
    if bus in _BUS_DENY_VALUES:
        return False
    if psv in _BUS_DENY_VALUES:
        return False

    highway = _norm(tags.get("highway"))
    return bool(highway and highway in HIGHWAY_ALLOWLIST)


def oneway_direction(tags: Mapping[str, str]) -> str:
    """
    Return ``'forward'``, ``'backward'``, or ``'both'`` from the way's
    ``oneway`` tag (and ``junction=roundabout`` shorthand).

    OSM convention: ``oneway=yes`` / ``1`` / ``true`` → forward only.
    ``oneway=-1`` / ``reverse`` → backward only. ``junction=roundabout``
    implies forward-only unless explicitly overridden.
    """
    oneway = _norm(tags.get("oneway"))
    junction = _norm(tags.get("junction"))

    if oneway in {"yes", "true", "1"}:
        return "forward"
    if oneway in {"-1", "reverse"}:
        return "backward"
    if oneway in {"no", "false", "0"}:
        return "both"
    if junction in {"roundabout", "circular"} and oneway is None:
        return "forward"
    return "both"


__all__ = [
    "HIGHWAY_ALLOWLIST",
    "HIGHWAY_HARD_BLOCKLIST",
    "is_highway_candidate",
    "is_bus_usable",
    "oneway_direction",
]
