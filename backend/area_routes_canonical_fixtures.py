"""
Fixed coordinates for automated area-route checks (no map UI).

Used by scripts/_bench_area_pg.py and optional PostGIS integration tests.
GeoJSON follows RFC 7946: Polygon rings are [lon, lat]; WKT here is lon lat (PostGIS 4326).
"""
from __future__ import annotations

import os
from typing import Any, Dict

# ~Tel Aviv–area box; dense shapes in national Israel GTFS (adjust if your feed differs).
CANONICAL_AREA_POLYGON_WKT = (
    "POLYGON ((34.78 32.06, 34.82 32.06, 34.82 32.09, 34.78 32.09, 34.78 32.06))"
)

CANONICAL_AREA_POLYGON_GEOJSON: Dict[str, Any] = {
    "type": "Polygon",
    "coordinates": [
        [
            [34.78, 32.06],
            [34.82, 32.06],
            [34.82, 32.09],
            [34.78, 32.09],
            [34.78, 32.06],
        ]
    ],
}

# Default calendar day for window tests; override with AREA_TEST_DATE_YMD if feed calendar differs.
CANONICAL_AREA_DATE_YMD = os.getenv("AREA_TEST_DATE_YMD", "20260405")

# Same window as the React area search defaults (04:00–23:59) on a single day.
CANONICAL_AREA_START_SEC = 4 * 3600
CANONICAL_AREA_END_SEC = 23 * 3600 + 59 * 60


def _sec_to_hhmm(total_sec: int) -> str:
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    return f"{h:02d}:{m:02d}"


# Strings accepted by POST /area/routes (AreaRoutesQuery).
CANONICAL_AREA_START_TIME_HHMM = _sec_to_hhmm(CANONICAL_AREA_START_SEC)
CANONICAL_AREA_END_TIME_HHMM = _sec_to_hhmm(CANONICAL_AREA_END_SEC)
