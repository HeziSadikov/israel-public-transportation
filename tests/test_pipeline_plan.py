"""Tests for read-only pipeline planner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from backend.infra import pipeline_skip as ps
from backend.infra.pipeline_plan import (
    ARTIFACT_ORDER,
    PlanRequest,
    PlanStep,
    compute_plan,
    format_plan_table,
)


def test_format_plan_table_stable_columns():
    steps = [
        PlanStep("ingest", "skip", "reused existing feed_id=1 by checksum", "abc", "abc", "succeeded"),
        PlanStep("patterns", "run", "fingerprint changed", "fp1", "fp0", "failed"),
    ]
    text = format_plan_table(steps)
    lines = text.splitlines()
    assert lines[0].startswith("artifact")
    assert "ingest" in lines[2]
    assert "patterns" in lines[3]


def test_artifact_order_fixed():
    assert ARTIFACT_ORDER[0] == "ingest"
    assert "gtfs_bus_way_evidence" in ARTIFACT_ORDER
    assert "bus_evidence" in ARTIFACT_ORDER
    assert ARTIFACT_ORDER.index("gtfs_bus_way_evidence") < ARTIFACT_ORDER.index("bus_evidence")


@patch("backend.infra.pipeline_plan._active_feed_checksum", return_value=(1, "zipck"))
@patch("backend.infra.pipeline_plan.ps.find_feed_by_zip_checksum")
def test_plan_ingest_skip_reuse_message(mock_find, _mock_active):
    mock_find.return_value = ps.FeedChecksumMatch(feed_id=5, active=True)
    conn = MagicMock()
    with patch("backend.infra.pipeline_plan.ps.get_feed_stage") as mock_stage:
        mock_stage.return_value = {
            "outcome": ps.OUTCOME_SUCCEEDED,
            "input_fingerprint": "deadbeef",
        }
        req = PlanRequest(with_ingest=True, gtfs_zip_path=Path("gtfs.zip"))
        with patch.object(Path, "is_file", return_value=True):
            with patch(
                "backend.infra.pipeline_plan.ps.sha256_file", return_value="deadbeef"
            ):
                steps = compute_plan(conn, req)
    ingest = next(s for s in steps if s.artifact == "ingest")
    assert ingest.action == "skip"
    assert "reused existing feed_id=5 by checksum" in ingest.reason


@patch("backend.infra.pipeline_plan._active_feed_checksum", return_value=(1, "zipck"))
@patch("backend.infra.pipeline_plan._plan_graphs")
def test_plan_force_ingest_marks_downstream_run(mock_graphs, _mock_active):
    mock_graphs.return_value = PlanStep("graphs", "run", "ingest --force will create new feed_id")
    conn = MagicMock()
    req = PlanRequest(with_ingest=True, ingest_force=True, skip_patterns=False)
    with patch("backend.infra.pipeline_plan._plan_ingest") as mock_ingest:
        mock_ingest.return_value = PlanStep("ingest", "run", "ingest --force (new feed_versions row)")
        with patch("backend.infra.pipeline_plan._plan_feed_stage") as mock_feed:
            mock_feed.return_value = PlanStep("patterns", "run", "ingest --force will create new feed_id")
            steps = compute_plan(conn, req)
    pat = next(s for s in steps if s.artifact == "patterns")
    assert pat.action == "run"
    assert "new feed_id" in pat.reason


def test_may_skip_failed_blocks():
    assert not ps.may_skip("a", "a", force=False, last_outcome=ps.OUTCOME_FAILED)


@patch("backend.infra.pipeline_skip.feed_pipeline_stages_available", return_value=False)
@patch("backend.infra.pipeline_skip.get_feed_stage", return_value=None)
@patch("backend.infra.pipeline_plan._active_feed_checksum", return_value=(1, "zipck"))
@patch("backend.infra.pipeline_plan.ps.find_feed_by_zip_checksum")
def test_plan_ingest_without_stage_registry(mock_find, _active, _stage, _avail):
    mock_find.return_value = ps.FeedChecksumMatch(feed_id=2, active=True)
    conn = MagicMock()
    req = PlanRequest(with_ingest=True, gtfs_zip_path=Path("gtfs.zip"))
    with patch.object(Path, "is_file", return_value=True):
        with patch("backend.infra.pipeline_plan.ps.sha256_file", return_value="zipck"):
            steps = compute_plan(conn, req)
    ingest = next(s for s in steps if s.artifact == "ingest")
    assert ingest.action == "skip"
    assert "feed_pipeline_stages not migrated" in ingest.reason


def test_get_feed_stage_missing_table_returns_none():
    import psycopg2
    from psycopg2 import errors as pg_errors

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.execute.side_effect = pg_errors.UndefinedTable("missing")
    assert ps.get_feed_stage(conn, 1, "ingest") is None
    conn.rollback.assert_called()


@patch("backend.infra.pipeline_plan.ps.osm_import_registry_available", return_value=False)
@patch("backend.infra.pipeline_plan.ps.feed_pipeline_stages_available", return_value=True)
@patch("backend.infra.pipeline_plan._active_feed_checksum", return_value=(1, "zipck"))
@patch("backend.infra.pipeline_plan.ps.find_feed_by_zip_checksum")
def test_compute_plan_ingest_only_without_osm_registry(
    mock_find, _active, _feed_stages, _osm_avail
):
    mock_find.return_value = None
    conn = MagicMock()
    req = PlanRequest(with_ingest=True, gtfs_zip_path=Path("gtfs.zip"))
    with patch.object(Path, "is_file", return_value=True):
        with patch("backend.infra.pipeline_plan.ps.sha256_file", return_value="zipck"):
            with patch(
                "backend.infra.pipeline_plan.ps.get_latest_osm_dataset_fingerprint"
            ) as mock_osm_fp:
                steps = compute_plan(conn, req)
                mock_osm_fp.assert_not_called()
    by_name = {s.artifact: s for s in steps}
    assert by_name["ingest"].action == "run"
    assert by_name["osm_import"].action == "skip"
    assert by_name["osm_import"].reason == "not requested"
    assert by_name["segment_turns"].action == "skip"
    assert by_name["bus_evidence"].action == "skip"


def test_distinct_evidence_artifacts_in_order():
    assert "gtfs_bus_way_evidence" in ARTIFACT_ORDER
    assert "bus_evidence" in ARTIFACT_ORDER
    assert ARTIFACT_ORDER.index("gtfs_bus_way_evidence") != ARTIFACT_ORDER.index("bus_evidence")
