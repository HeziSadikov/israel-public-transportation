"""GET /feed/status includes GTFS calendar span from PostGIS."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch):
    from backend.mcp_server.transport import http as app_mod

    monkeypatch.setattr(
        app_mod,
        "get_feed_status",
        lambda: {
            "active": None,
            "history_len": 0,
            "last_update_attempt": None,
            "last_update_ok": None,
        },
    )
    return TestClient(app_mod.app)


def test_feed_status_includes_calendar_span(client, monkeypatch):
    from backend.mcp_server.transport import http as app_mod

    monkeypatch.setattr(
        app_mod.db_access_module,
        "get_active_feed_calendar_span",
        lambda _conn=None: (20260301, 20260331),
    )
    r = client.get("/api/v1/feed/status")
    assert r.status_code == 200
    data = r.json()
    assert data["calendar_min_ymd"] == 20260301
    assert data["calendar_max_ymd"] == 20260331


def test_feed_status_calendar_none_when_db_raises(client, monkeypatch):
    from backend.mcp_server.transport import http as app_mod

    def _boom(_conn=None):
        raise RuntimeError("no db")

    monkeypatch.setattr(
        app_mod.db_access_module,
        "get_active_feed_calendar_span",
        _boom,
    )
    r = client.get("/api/v1/feed/status")
    assert r.status_code == 200
    data = r.json()
    assert data["calendar_min_ymd"] is None
    assert data["calendar_max_ymd"] is None
