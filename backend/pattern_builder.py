from __future__ import annotations

import hashlib
from collections import defaultdict, Counter
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set

from .gtfs_loader import GTFSFeed
from .sqlite_db import get_stop_times_for_trip
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
    def __init__(self, feed: GTFSFeed):
        self.feed = feed
        self.calendar = ServiceCalendar(feed)

    def build_patterns_for_route(
        self,
        route_id: str,
        direction_id: Optional[str],
        yyyymmdd: str,
        max_trips: Optional[int] = None,
    ) -> Dict[str, RoutePattern]:
        active_services: Set[str] = self.calendar.active_service_ids_for_date(yyyymmdd)

        trips = [
            t
            for t in self.feed.trips
            if t.get("route_id") == route_id
            and (direction_id is None or t.get("direction_id") == str(direction_id))
            and t.get("service_id") in active_services
        ]

        if max_trips is not None:
            trips = trips[:max_trips]

        # Build stop_times index only for the relevant trips of this route/date,
        # using SQLite to fetch stop_times per trip.
        stop_times_by_trip: Dict[str, List[Dict]] = defaultdict(list)
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

