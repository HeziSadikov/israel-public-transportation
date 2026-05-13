"""
End-to-end detour v2 tests against a real local Valhalla + real GTFS database.

Gated behind the RUN_VALHALLA_E2E=1 environment variable.  Run with:
    RUN_VALHALLA_E2E=1 pytest tests/test_detour_v2_e2e.py -v

Before running:
  1. Start the backend with a live database (PGHOST / PGDATABASE etc. set).
  2. Start Valhalla (docker compose up valhalla).
  3. Set VALHALLA_URL to point at the local instance.
  4. Replace the placeholder trip_ids below with real GTFS trip IDs from your feed.

Scenarios:
  E2E_01  Jabotinsky Street closure (Petah Tikva / Bnei Brak area).
  E2E_02  Route 4 coastal section.
  E2E_03  Polygon overlapping a route that runs through a one-way system.
  E2E_04  Polygon far from route → no_impact.
  E2E_05  Dense urban polygon where no safe path exists → no_safe_detour or emergency_fallback.
"""

from __future__ import annotations

import os
import pytest

from backend.domain.detour_v2.compute import compute_detour_for_trip
from backend.domain.detour_v2.policy import DetourPolicyConfig

SKIP_REASON = "Set RUN_VALHALLA_E2E=1 to run end-to-end tests against a live Valhalla + DB."

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_VALHALLA_E2E", "").strip() != "1",
    reason=SKIP_REASON,
)


def _policy() -> DetourPolicyConfig:
    pol = DetourPolicyConfig()
    pol.search.per_trip_deadline_ms = 60_000
    pol.search.valhalla_concurrency = 2
    return pol


# ---------------------------------------------------------------------------
# E2E scenarios — fill in real trip_ids before running.
# ---------------------------------------------------------------------------

E2E_SCENARIOS = [
    {
        "id": "E2E_01_jabotinsky",
        "description": "Jabotinsky Street blockage (Petah Tikva area).",
        "trip_id": "REPLACE_WITH_REAL_TRIP_ID",
        "service_date": "20260511",
        "blockage_geojson": {
            "type": "Polygon",
            "coordinates": [[[34.876, 32.082], [34.888, 32.082], [34.888, 32.086], [34.876, 32.086], [34.876, 32.082]]],
        },
        "expect": {
            "not_status": ["error"],
            "anchor_road_class_rank_max": 1,
        },
    },
    {
        "id": "E2E_02_route4",
        "description": "Route 4 coastal section (between Tel Aviv and Netanya).",
        "trip_id": "REPLACE_WITH_REAL_TRIP_ID",
        "service_date": "20260511",
        "blockage_geojson": {
            "type": "Polygon",
            "coordinates": [[[34.848, 32.145], [34.855, 32.145], [34.855, 32.150], [34.848, 32.150], [34.848, 32.145]]],
        },
        "expect": {
            "not_status": ["error"],
        },
    },
    {
        "id": "E2E_03_one_way_system",
        "description": "Urban polygon in a one-way street system.",
        "trip_id": "REPLACE_WITH_REAL_TRIP_ID",
        "service_date": "20260511",
        "blockage_geojson": {
            "type": "Polygon",
            "coordinates": [[[34.780, 32.075], [34.785, 32.075], [34.785, 32.080], [34.780, 32.080], [34.780, 32.075]]],
        },
        "expect": {
            "not_status": ["error"],
        },
    },
    {
        "id": "E2E_04_far_polygon",
        "description": "Polygon far from any route shape → no_impact.",
        "trip_id": "REPLACE_WITH_REAL_TRIP_ID",
        "service_date": "20260511",
        "blockage_geojson": {
            "type": "Polygon",
            "coordinates": [[[35.200, 33.100], [35.205, 33.100], [35.205, 33.105], [35.200, 33.105], [35.200, 33.100]]],
        },
        "expect": {
            "status": "no_impact",
        },
    },
]


@pytest.mark.parametrize("scenario", E2E_SCENARIOS, ids=lambda s: s["id"])
def test_e2e_detour_v2(scenario):
    trip_id = scenario["trip_id"]
    if "REPLACE_WITH_REAL_TRIP_ID" in trip_id:
        pytest.skip(f"{scenario['id']}: real trip_id not yet configured")

    out = compute_detour_for_trip(
        trip_id=trip_id,
        blockage_geojson=scenario["blockage_geojson"],
        service_date=str(scenario["service_date"]),
        policy=_policy(),
    )

    exp = scenario.get("expect") or {}

    if "status" in exp:
        assert out.status == exp["status"], (
            f"{scenario['id']}: expected status={exp['status']} got {out.status} err={out.error}"
        )

    not_statuses = exp.get("not_status") or []
    for bad in not_statuses:
        assert out.status != bad, f"{scenario['id']}: status must not be {bad}, got {out.status} err={out.error}"

    max_rank = exp.get("anchor_road_class_rank_max")
    if max_rank is not None and out.anchors:
        actual_max = max(out.anchors.exit_road_class_rank, out.anchors.rejoin_road_class_rank)
        assert actual_max <= int(max_rank), (
            f"{scenario['id']}: anchor road_class_rank {actual_max} > allowed {max_rank} "
            f"(exit={out.anchors.exit_road_class}, rejoin={out.anchors.rejoin_road_class})"
        )

    # Anchors must never be inside the blockage polygon.
    if out.anchors and out.status not in ("no_impact", "error", "no_safe_detour"):
        from shapely.geometry import Point, shape as shp
        poly = shp(scenario["blockage_geojson"])
        for label, lon, lat in [
            ("exit", out.anchors.exit_lon, out.anchors.exit_lat),
            ("rejoin", out.anchors.rejoin_lon, out.anchors.rejoin_lat),
        ]:
            assert not poly.contains(Point(lon, lat)), (
                f"{scenario['id']}: {label} anchor ({lon},{lat}) is inside the blockage polygon"
            )
