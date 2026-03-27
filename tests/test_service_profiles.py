from __future__ import annotations

from backend import service_calendar as sc


def test_default_profile_for_date_weekparts():
    assert sc.default_profile_for_date("20260323") == "weekday"  # Monday
    assert sc.default_profile_for_date("20260327") == "friday"   # Friday
    assert sc.default_profile_for_date("20260328") == "saturday"  # Saturday
    assert sc.default_profile_for_date("20260329") == "sunday"    # Sunday


def test_resolve_service_profile_special_from_feed():
    feed = {
        "calendar_dates": [
            {"date": "20260327", "service_id": "X", "exception_type": "1"},
        ]
    }

    class _Feed:
        calendar_dates = feed["calendar_dates"]

    assert sc.resolve_service_profile("20260327", feed=_Feed()) == "special:20260327"
    assert sc.resolve_service_profile("20260328", feed=_Feed()) == "saturday"
