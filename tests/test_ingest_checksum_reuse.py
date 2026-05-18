"""Tests for ingest checksum reuse and transactional feed reactivation."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from backend.infra import pipeline_skip as ps


def test_find_feed_by_zip_checksum_prefers_active():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.return_value = (42, True)

    match = ps.find_feed_by_zip_checksum(conn, "abc123")
    assert match is not None
    assert match.feed_id == 42
    assert match.active is True
    sql = cur.execute.call_args[0][0]
    assert "ORDER BY active DESC" in sql


def test_may_skip_failed_prior_stage_blocks_skip():
    assert not ps.may_skip(
        "fp", "fp", force=False, last_outcome=ps.OUTCOME_FAILED
    )
    assert not ps.may_skip(
        "fp", "fp", force=False, last_outcome=ps.OUTCOME_RUNNING
    )


def test_force_disables_may_skip():
    assert not ps.may_skip(
        "fp", "fp", force=True, last_outcome=ps.OUTCOME_SUCCEEDED
    )


@patch("backend.scripts.ingest_gtfs_postgis._try_reuse_feed_by_checksum")
@patch("backend.scripts.ingest_gtfs_postgis._sha256_file", return_value="deadbeef")
@patch("backend.scripts.ingest_gtfs_postgis._connect")
def test_ingest_gtfs_force_skips_reuse_check(mock_connect, _mock_sha, mock_reuse):
    from backend.scripts.ingest_gtfs_postgis import ingest_gtfs
    from pathlib import Path

    conn = MagicMock()
    conn.autocommit = False
    mock_connect.return_value = conn

    with patch("backend.scripts.ingest_gtfs_postgis.zipfile.ZipFile"):
        with patch.object(conn, "__enter__", return_value=conn):
            with patch.object(conn, "__exit__", return_value=False):
                try:
                    ingest_gtfs(
                        Path("x.zip"),
                        "postgresql://localhost/test",
                        force=True,
                        reuse_existing=True,
                    )
                except Exception:
                    pass

    mock_reuse.assert_not_called()


@patch("backend.infra.pipeline_skip.reactivate_feed")
@patch("backend.infra.pipeline_skip.find_feed_by_zip_checksum")
@patch("backend.infra.pipeline_skip.ensure_pipeline_schema")
def test_try_reuse_reactivates_inactive(mock_ensure, mock_find, mock_reactivate):
    from backend.scripts.ingest_gtfs_postgis import _try_reuse_feed_by_checksum

    mock_find.return_value = ps.FeedChecksumMatch(feed_id=7, active=False)
    conn = MagicMock()

    assert _try_reuse_feed_by_checksum(conn, "chk", reuse_existing=True) is True
    mock_reactivate.assert_called_once_with(conn, 7, "chk")
    mock_ensure.assert_called_once()


@patch("backend.infra.pipeline_skip.mark_feed_succeeded")
@patch("backend.infra.pipeline_skip.find_feed_by_zip_checksum")
@patch("backend.infra.pipeline_skip.ensure_pipeline_schema")
def test_try_reuse_active_skips_load(mock_ensure, mock_find, mock_mark):
    from backend.scripts.ingest_gtfs_postgis import _try_reuse_feed_by_checksum

    mock_find.return_value = ps.FeedChecksumMatch(feed_id=3, active=True)
    conn = MagicMock()

    assert _try_reuse_feed_by_checksum(conn, "chk", reuse_existing=True) is True
    mock_mark.assert_called_once()
    conn.commit.assert_called_once()


def test_reactivate_feed_sets_single_active(monkeypatch):
    conn = MagicMock()
    conn.autocommit = False
    calls: list[str] = []

    def fake_acquire(_conn, _unit):
        return None

    monkeypatch.setattr(ps, "acquire_unit_lock", fake_acquire)
    monkeypatch.setattr(ps, "mark_feed_succeeded", lambda *a, **k: calls.append("mark"))

    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.side_effect = [("abc",), (1,)]

    ps.reactivate_feed(conn, 99, "abc")
    conn.commit.assert_called()
    assert "mark" in calls
    update_calls = [c for c in cur.execute.call_args_list if "UPDATE feed_versions" in str(c)]
    assert len(update_calls) >= 2
