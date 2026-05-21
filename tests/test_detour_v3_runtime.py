"""Tests for detour v3 startup feed / import-run resolution."""

from __future__ import annotations

from backend.infra.detour_v3_runtime import resolve_detour_v3_import_run_id


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *args, **kwargs):
        pass

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return _FakeCursor(self._row)


def test_resolve_import_run_id_from_db(monkeypatch):
    monkeypatch.setenv("DETOUR_V3_IMPORT_RUN_ID", "")
    monkeypatch.delenv("DETOUR_V3_IMPORT_RUN_ID", raising=False)
    from backend.infra import config as cfg

    monkeypatch.setattr(cfg, "DETOUR_V3_IMPORT_RUN_ID", None)
    conn = _FakeConn({"id": 4})
    assert resolve_detour_v3_import_run_id(conn) == 4


def test_resolve_import_run_id_env_override(monkeypatch):
    from backend.infra import config as cfg

    monkeypatch.setattr(cfg, "DETOUR_V3_IMPORT_RUN_ID", 7)
    conn = _FakeConn({"id": 4})
    assert resolve_detour_v3_import_run_id(conn) == 7
