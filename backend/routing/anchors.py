"""Detour v3 — exit/rejoin anchors on a pattern segment path around blocked segments."""

from __future__ import annotations

from dataclasses import dataclass
from typing import AbstractSet, List, Sequence, Tuple


@dataclass(frozen=True, slots=True)
class SegmentAnchorPair:
    """One bypass window: route from exit_node toward rejoin_node after a blocked chain."""

    exit_segment_id: int
    rejoin_segment_id: int
    exit_node_id: int
    rejoin_node_id: int
    blocked_first_idx: int
    blocked_last_idx: int


def iter_blocked_ranges(
    pattern_segment_ids: Sequence[int],
    banned_segments: AbstractSet[int],
) -> List[Tuple[int, int]]:
    """Inclusive index ranges ``(first, last)`` where ``pattern_segment_ids[k] ∈ banned_segments``."""
    n = len(pattern_segment_ids)
    out: List[Tuple[int, int]] = []
    i = 0
    while i < n:
        if pattern_segment_ids[i] not in banned_segments:
            i += 1
            continue
        j = i
        while j < n and pattern_segment_ids[j] in banned_segments:
            j += 1
        out.append((i, j - 1))
        i = j
    return out


def anchor_pairs_for_pattern_blocks(
    pattern_segment_ids: Sequence[int],
    banned_segments: AbstractSet[int],
) -> List[SegmentAnchorPair]:
    """
    For each maximal contiguous blocked subsequence inside ``pattern_segment_ids``,
    return exit/rejoin *nodes* bordering that subsequence.

    Exit is ``to_node`` of the last segment before the block; rejoin is ``from_node``
    of the first segment after the block. Omitted ranges that touch pattern ends
    (cannot exit before blockage or cannot rejoin after) are skipped.
    """
    segs = list(pattern_segment_ids)
    if len(segs) < 2 or not banned_segments:
        return []
    pairs: List[SegmentAnchorPair] = []
    for bi, bj in iter_blocked_ranges(segs, banned_segments):
        exit_idx = bi - 1
        rejoin_idx = bj + 1
        if exit_idx < 0 or rejoin_idx >= len(segs):
            continue
        # Segments bordering the blockage (stay on-bus-corridor backbone).
        exit_seg_id = int(segs[exit_idx])
        rejoin_seg_id = int(segs[rejoin_idx])
        pairs.append(
            SegmentAnchorPair(
                exit_segment_id=exit_seg_id,
                rejoin_segment_id=rejoin_seg_id,
                exit_node_id=-1,
                rejoin_node_id=-1,
                blocked_first_idx=bi,
                blocked_last_idx=bj,
            )
        )
    return pairs


def resolve_anchor_nodes(
    anchors: Sequence[SegmentAnchorPair],
    segment_to_nodes: dict[int, Tuple[int, int]],
) -> List[SegmentAnchorPair]:
    """Fill ``exit_node_id`` / ``rejoin_node_id`` from ``segment_id → (from_node, to_node)``."""
    out: List[SegmentAnchorPair] = []
    for a in anchors:
        ex = segment_to_nodes.get(a.exit_segment_id)
        rj = segment_to_nodes.get(a.rejoin_segment_id)
        if not ex or not rj:
            continue
        exit_to = int(ex[1])
        rejoin_fr = int(rj[0])
        out.append(
            SegmentAnchorPair(
                exit_segment_id=a.exit_segment_id,
                rejoin_segment_id=a.rejoin_segment_id,
                exit_node_id=exit_to,
                rejoin_node_id=rejoin_fr,
                blocked_first_idx=a.blocked_first_idx,
                blocked_last_idx=a.blocked_last_idx,
            )
        )
    return out


__all__ = [
    "SegmentAnchorPair",
    "anchor_pairs_for_pattern_blocks",
    "iter_blocked_ranges",
    "resolve_anchor_nodes",
]
