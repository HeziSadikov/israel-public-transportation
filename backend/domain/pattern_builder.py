from __future__ import annotations

import hashlib
from collections import defaultdict, Counter
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set, Any

from backend.infra.db_access import get_stop_times_for_trip
from .service_calendar import ServiceCalendar


@dataclass
class RoutePattern:
    pattern_id: str
    route_id: str
    direction_id: Optional[str]
    stop_ids: List[str]
    frequency: int
    representative_trip_id: str
    representative_shape_id: Optional[str]


class PatternBuilder:
    def __init__(self, feed: Any):
        self.feed = feed
        self.calendar = ServiceCalendar(feed)

    def build_patterns_for_route(
        self,
        route_id: str,
        direction_id: Optional[str],
        yyyymmdd: str,
        max_trips: Optional[int] = None,
        use_all_trips: bool = False,
        stop_times_preloaded: Optional[Dict[str, List[Dict]]] = None,
    ) -> Dict[str, RoutePattern]:
        if use_all_trips:
            active_services = None  # no filter: use every trip for this route/direction
        else:
            active_services = self.calendar.active_service_ids_for_date(yyyymmdd)

        trips = [
            t
            for t in self.feed.trips
            if t.get("route_id") == route_id
            and (direction_id is None or t.get("direction_id") == str(direction_id))
            and (active_services is None or t.get("service_id") in active_services)
        ]

        if max_trips is not None:
            trips = trips[:max_trips]

        if stop_times_preloaded is not None:
            stop_times_by_trip = {t["trip_id"]: stop_times_preloaded.get(t["trip_id"], []) for t in trips}
        else:
            stop_times_by_trip = {}
            for trip in trips:
                trip_id = trip["trip_id"]
                stop_times_by_trip[trip_id] = get_stop_times_for_trip(trip_id)

        patterns: Dict[str, List[Tuple[str, str, Optional[str]]]] = defaultdict(list)

        for trip in trips:
            trip_id = trip["trip_id"]
            sts = sorted(
                stop_times_by_trip.get(trip_id, []),
                key=lambda r: int(r["stop_sequence"]),
            )
            stop_ids = [r["stop_id"] for r in sts]
            if len(stop_ids) < 2:
                continue

            pattern_key_str = f"{route_id}|{trip.get('direction_id','')}|" + ",".join(
                stop_ids
            )
            pattern_id = hashlib.sha256(pattern_key_str.encode("utf-8")).hexdigest()[:16]
            patterns[pattern_id].append(
                (trip_id, trip.get("direction_id"), trip.get("shape_id") or None)
            )

        result: Dict[str, RoutePattern] = {}
        for pid, trip_infos in patterns.items():
            freq = len(trip_infos)
            trip_ids = [ti[0] for ti in trip_infos]
            direction_ids = [ti[1] for ti in trip_infos]
            shape_ids = [ti[2] for ti in trip_infos if ti[2] is not None]

            rep_trip_id = trip_ids[0]
            rep_dir = Counter(direction_ids).most_common(1)[0][0]
            rep_shape = shape_ids[0] if shape_ids else None

            # Reconstruct stop_ids from representative trip
            sts = sorted(
                stop_times_by_trip.get(rep_trip_id, []),
                key=lambda r: int(r["stop_sequence"]),
            )
            stop_ids = [r["stop_id"] for r in sts]

            result[pid] = RoutePattern(
                pattern_id=pid,
                route_id=route_id,
                direction_id=rep_dir,
                stop_ids=stop_ids,
                frequency=freq,
                representative_trip_id=rep_trip_id,
                representative_shape_id=rep_shape,
            )

        return result

    def pick_most_frequent_pattern(
        self, patterns: Dict[str, RoutePattern]
    ) -> Optional[RoutePattern]:
        if not patterns:
            return None
        return max(patterns.values(), key=lambda p: p.frequency)


def resolve_most_frequent_route_pattern(
    feed: Any,
    route_id: str,
    direction_id: Optional[str],
    yyyymmdd: str,
) -> Optional[RoutePattern]:
    """Same pattern choice as legacy detours/by-area: most frequent stop-sequence pattern on service date."""
    pb = PatternBuilder(feed)
    patterns = pb.build_patterns_for_route(
        route_id=route_id,
        direction_id=direction_id,
        yyyymmdd=yyyymmdd,
        max_trips=None,
    )
    return pb.pick_most_frequent_pattern(patterns) if patterns else None


def resolve_representative_trip_id(
    feed: Any,
    route_id: str,
    direction_id: Optional[str],
    yyyymmdd: str,
) -> Optional[str]:
    """GTFS trip id for the representative trip of the main pattern (detour v2 trip-scoped context)."""
    chosen = resolve_most_frequent_route_pattern(feed, route_id, direction_id, yyyymmdd)
    return chosen.representative_trip_id if chosen else None

