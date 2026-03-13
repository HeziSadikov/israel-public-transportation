from __future__ import annotations

from typing import Any

import psycopg2
from psycopg2.extras import DictCursor

from .db_access import DB_URL, get_active_feed_id


def load_active_feed() -> Any:
    """
    Load core GTFS tables for the active feed from PostGIS and construct a
    simple feed object compatible with PatternBuilder and ServiceCalendar
    (routes, agencies, trips, stops, calendar, calendar_dates, shapes).
    """
    conn = psycopg2.connect(DB_URL, cursor_factory=DictCursor)
    try:
        feed_id = get_active_feed_id(conn)
        with conn.cursor() as cur:
            # Agencies
            cur.execute(
                """
                SELECT agency_id, name, url, timezone, lang, phone
                FROM agencies
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            agencies = [
                {
                    "agency_id": r["agency_id"],
                    "agency_name": r["name"],
                    "agency_url": r["url"],
                    "agency_timezone": r["timezone"],
                    "agency_lang": r["lang"],
                    "agency_phone": r["phone"],
                }
                for r in cur.fetchall()
            ]

            # Routes
            cur.execute(
                """
                SELECT route_id, agency_id, short_name, long_name, route_type, route_color, route_text_color
                FROM routes
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            routes = [
                {
                    "route_id": r["route_id"],
                    "agency_id": r["agency_id"],
                    "route_short_name": r["short_name"],
                    "route_long_name": r["long_name"],
                    "route_type": r["route_type"],
                    "route_color": r["route_color"],
                    "route_text_color": r["route_text_color"],
                }
                for r in cur.fetchall()
            ]

            # Stops
            cur.execute(
                """
                SELECT stop_id, name, lat, lon, zone_id, parent_station
                FROM stops
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            stops = [
                {
                    "stop_id": r["stop_id"],
                    "stop_name": r["name"],
                    "stop_lat": r["lat"],
                    "stop_lon": r["lon"],
                    "zone_id": r["zone_id"],
                    "parent_station": r["parent_station"],
                }
                for r in cur.fetchall()
            ]

            # Trips
            cur.execute(
                """
                SELECT trip_id, route_id, service_id, direction_id, shape_id, headsign, block_id
                FROM trips
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            trips = []
            for r in cur.fetchall():
                dir_val = r["direction_id"]
                trips.append(
                    {
                        "trip_id": r["trip_id"],
                        "route_id": r["route_id"],
                        "service_id": r["service_id"],
                        "direction_id": None if dir_val is None else str(dir_val),
                        "shape_id": r["shape_id"],
                        "trip_headsign": r["headsign"],
                        "block_id": r["block_id"],
                    }
                )

            # Calendar
            cur.execute(
                """
                SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday,
                       sunday, start_date, end_date
                FROM calendar
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            calendar = [
                {
                    "service_id": r["service_id"],
                    "monday": str(r["monday"] or 0),
                    "tuesday": str(r["tuesday"] or 0),
                    "wednesday": str(r["wednesday"] or 0),
                    "thursday": str(r["thursday"] or 0),
                    "friday": str(r["friday"] or 0),
                    "saturday": str(r["saturday"] or 0),
                    "sunday": str(r["sunday"] or 0),
                    "start_date": f"{int(r['start_date']):08d}" if r["start_date"] is not None else "00000000",
                    "end_date": f"{int(r['end_date']):08d}" if r["end_date"] is not None else "00000000",
                }
                for r in cur.fetchall()
            ]

            # Calendar dates
            cur.execute(
                """
                SELECT service_id, date, exception_type
                FROM calendar_dates
                WHERE feed_id = %s
                """,
                (feed_id,),
            )
            calendar_dates = [
                {
                    "service_id": r["service_id"],
                    "date": f"{int(r['date']):08d}",
                    "exception_type": str(r["exception_type"]),
                }
                for r in cur.fetchall()
            ]

            # Shapes
            cur.execute(
                """
                SELECT shape_id, seq, lat, lon, dist_traveled
                FROM shapes
                WHERE feed_id = %s
                ORDER BY shape_id, seq
                """,
                (feed_id,),
            )
            shapes = [
                {
                    "shape_id": r["shape_id"],
                    "shape_pt_sequence": r["seq"],
                    "shape_pt_lat": r["lat"],
                    "shape_pt_lon": r["lon"],
                    "shape_dist_traveled": r["dist_traveled"],
                }
                for r in cur.fetchall()
            ]

        # Build a simple feed-like object.
        feed = type("FeedStub", (), {})()
        feed.version_id = f"postgis-{feed_id}"
        feed.routes = routes
        feed.agencies = agencies
        feed.trips = trips
        feed.stop_times = []
        feed.stops = stops
        feed.calendar_dates = calendar_dates
        feed.calendar = calendar
        feed.shapes = shapes
        return feed
    finally:
        conn.close()

