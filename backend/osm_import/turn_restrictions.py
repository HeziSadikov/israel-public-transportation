"""
Pure parsers for OSM ``type=restriction`` (and ``type=restriction:bus``)
relations.

These helpers turn pyosmium relation objects (or any object with
``tags`` and ``members``) into ``RestrictionRow`` dataclasses ready to be
inserted into ``osm_turn_restrictions``. No DB or network I/O.

v1 scope (per the plan):

* via-node restrictions: ``no_left_turn``, ``no_right_turn``, ``no_straight_on``,
  ``no_u_turn``, ``only_left_turn``, ``only_right_turn``, ``only_straight_on``.
* via-way restrictions are parsed (``via_way_id`` populated) but the segment
  resolution into ``osm_segment_turns`` is M2.
* bus-specific forms: ``type=restriction:bus`` flips ``applies_to_bus=True``;
  ``except=bus``/``psv`` flags are recorded so M2 can ignore restrictions that
  don't apply to bus traffic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional


_SUPPORTED_RESTRICTION_TYPES: frozenset[str] = frozenset(
    {
        "no_left_turn",
        "no_right_turn",
        "no_straight_on",
        "no_u_turn",
        "only_left_turn",
        "only_right_turn",
        "only_straight_on",
        # Less common but seen in OSM:
        "no_entry",
        "no_exit",
    }
)


@dataclass
class RestrictionRow:
    """Row ready to insert into ``osm_turn_restrictions``."""

    from_way_id: int
    to_way_id: int
    via_node_id: Optional[int]
    via_way_id: Optional[int]
    restriction_type: str
    applies_to_bus: bool
    except_bus: bool
    except_psv: bool
    tags_json: dict[str, str] = field(default_factory=dict)


def _norm(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    return s or None


def _split_except(value: Optional[str]) -> set[str]:
    if not value:
        return set()
    parts = [p.strip().lower() for p in str(value).replace(";", ",").split(",")]
    return {p for p in parts if p}


def parse_restriction_relation(
    rel_type: Optional[str],
    tags: Mapping[str, str],
    members: Iterable[Any],
) -> Optional[RestrictionRow]:
    """
    Parse a relation's ``tags`` and ``members`` into a :class:`RestrictionRow`,
    or ``None`` when the relation is not a turn restriction we care about.

    ``rel_type`` is the relation's ``type`` tag (already extracted to avoid
    pyosmium API coupling here). ``members`` may be any iterable whose items
    expose ``type`` (``'n'``/``'w'``/``'r'`` or ``'node'``/``'way'``/...) ,
    ``ref``, and ``role``. Both pyosmium ``RelationMember`` and a plain
    ``(type, ref, role)`` tuple are accepted via :func:`_member_view`.

    The accepted ``restriction_type`` comes from either the ``restriction``
    tag or, for bus-specific forms (``type=restriction:bus``), the
    ``restriction:bus`` tag.
    """
    rt_raw = _norm(rel_type)
    if rt_raw not in {"restriction", "restriction:bus"}:
        return None

    applies_to_bus = rt_raw == "restriction:bus"

    restriction_value: Optional[str]
    if applies_to_bus:
        restriction_value = _norm(tags.get("restriction:bus")) or _norm(tags.get("restriction"))
    else:
        restriction_value = _norm(tags.get("restriction"))

    if restriction_value is None:
        return None
    if restriction_value not in _SUPPORTED_RESTRICTION_TYPES:
        return None

    except_values = _split_except(tags.get("except"))
    except_bus = "bus" in except_values
    except_psv = "psv" in except_values or "public_transport" in except_values

    from_way_id: Optional[int] = None
    to_way_id: Optional[int] = None
    via_node_id: Optional[int] = None
    via_way_id: Optional[int] = None

    for m in members:
        mtype, mref, mrole = _member_view(m)
        role = _norm(mrole)
        if role == "from" and mtype == "way":
            from_way_id = int(mref)
        elif role == "to" and mtype == "way":
            to_way_id = int(mref)
        elif role == "via":
            if mtype == "node" and via_node_id is None:
                via_node_id = int(mref)
            elif mtype == "way" and via_way_id is None:
                via_way_id = int(mref)

    if from_way_id is None or to_way_id is None:
        return None
    if via_node_id is None and via_way_id is None:
        return None

    tag_dict = {str(k): str(v) for k, v in tags.items()}

    return RestrictionRow(
        from_way_id=from_way_id,
        to_way_id=to_way_id,
        via_node_id=via_node_id,
        via_way_id=via_way_id,
        restriction_type=restriction_value,
        applies_to_bus=applies_to_bus,
        except_bus=except_bus,
        except_psv=except_psv,
        tags_json=tag_dict,
    )


def _member_view(m: Any) -> tuple[str, int, str]:
    """
    Normalize a relation member into ``(type, ref, role)`` where ``type`` is
    ``'node'`` / ``'way'`` / ``'relation'``.

    Accepts:
      * pyosmium ``osmium.osm.RelationMember`` (``type`` is ``'n'``/``'w'``/``'r'``).
      * plain ``(type, ref, role)`` 3-tuple/list.
      * any object exposing ``.type``, ``.ref``, ``.role`` attributes.
    """
    if isinstance(m, (tuple, list)) and len(m) == 3:
        mtype, mref, mrole = m
    else:
        mtype = getattr(m, "type", None)
        mref = getattr(m, "ref", None)
        mrole = getattr(m, "role", None)
    mtype_str = _norm(mtype) or ""
    if mtype_str == "n":
        mtype_str = "node"
    elif mtype_str == "w":
        mtype_str = "way"
    elif mtype_str == "r":
        mtype_str = "relation"
    try:
        ref_int = int(mref) if mref is not None else 0
    except (TypeError, ValueError):
        ref_int = 0
    return mtype_str, ref_int, mrole or ""


__all__ = [
    "RestrictionRow",
    "parse_restriction_relation",
]
