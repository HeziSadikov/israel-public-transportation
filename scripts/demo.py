from __future__ import annotations

import os
from typing import Any, Dict

import httpx

from backend.infra.logging_utils import ensure_cli_action_logging, log


API_BASE = os.getenv("API_BASE", "http://localhost:8000")


def main() -> None:
    ensure_cli_action_logging()
    log("demo", "phase=main start")
    print(f"Using API base: {API_BASE}")

    with httpx.Client(timeout=60.0) as client:
        # Try to update feed (will fall back to last good if offline)
        print("Calling /feed/update ...")
        log("demo", "phase=feed_update start")
        try:
          r = client.post(f"{API_BASE}/feed/update")
          r.raise_for_status()
          print("Feed update response:", r.json())
          log("demo", "phase=feed_update done")
        except Exception as e:
          print("Feed update failed or offline, continuing with existing feed:", e)
          log("demo", f"phase=feed_update error={e!s}")

        # Pick a sample route by searching
        print("Searching for a sample route via /routes/search ...")
        log("demo", "phase=routes_search start")
        r = client.post(
            f"{API_BASE}/routes/search",
            json={"q": "", "limit": 1},
        )
        r.raise_for_status()
        log("demo", "phase=routes_search done")
        routes = r.json()
        if not routes:
            print("No routes found in feed; aborting demo.")
            return
        route = routes[0]
        route_id = route["route_id"]
        print("Using route_id:", route_id)

        # Build graph for this route
        print("Building graph via /graph/build ...")
        log("demo", "phase=graph_build start")
        r = client.post(
            f"{API_BASE}/graph/build",
            json={"route_id": route_id, "pretty_osm": False},
        )
        r.raise_for_status()
        log("demo", "phase=graph_build done")
        build = r.json()
        print("Graph build response:", build)
        pattern_id = build["pattern_id"]

        # Get ordered stops
        print("Fetching stops via /graph/stops ...")
        log("demo", "phase=graph_stops start")
        r = client.get(
            f"{API_BASE}/graph/stops",
            params={"route_id": route_id, "pattern_id": pattern_id},
        )
        r.raise_for_status()
        log("demo", "phase=graph_stops done")
        stops_body = r.json()
        stops = stops_body["stops"]
        if len(stops) < 3:
            print("Not enough stops for demo; aborting.")
            return

        start = stops[0]
        end = stops[-1]
        middle = stops[len(stops) // 2]
        print(f"Start stop: {start['stop_id']}  End stop: {end['stop_id']}")

        # Construct a simple blockage line between two consecutive stops near the middle
        mid_idx = max(1, min(len(stops) - 2, len(stops) // 2))
        s1 = stops[mid_idx]
        s2 = stops[mid_idx + 1]
        blockage_geojson: Dict[str, Any] = {
            "type": "LineString",
            "coordinates": [
                [s1["lon"], s1["lat"]],
                [s2["lon"], s2["lat"]],
            ],
        }

        print("Calling /detour with a sample blockage ...")
        log("demo", "phase=detour start")
        r = client.post(
            f"{API_BASE}/detour",
            json={
                "route_id": route_id,
                "pattern_id": pattern_id,
                "start_stop_id": start["stop_id"],
                "end_stop_id": end["stop_id"],
                "blockage_geojson": blockage_geojson,
            },
        )
        if r.status_code != 200:
            print("Detour failed with status", r.status_code, "body:", r.text)
            log("demo", f"phase=detour error status={r.status_code}")
            return
        detour = r.json()
        print("Detour response summary:")
        print("  blocked_edges_count:", detour["blocked_edges_count"])
        print("  stop_path length:", len(detour["stop_path"]))
        log("demo", "phase=detour done")
    log("demo", "phase=main done")


if __name__ == "__main__":
    main()

