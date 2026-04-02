"""Tests for pipeline/gold/run_logger.py."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipeline.exceptions import PostgresError
from pipeline.gold.run_logger import RunLogger, RunRecord, RunStatus
from pipeline.settings import PostgresSettings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(monkeypatch: pytest.MonkeyPatch) -> PostgresSettings:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    return PostgresSettings(
        host="localhost",
        port=5432,
        db="test_db",
        user="test_user",
        password="test",  # type: ignore[arg-type]
    )


@pytest.fixture()
def run_logger(settings: PostgresSettings) -> RunLogger:
    return RunLogger(settings=settings)


@pytest.fixture()
def success_record() -> RunRecord:
    return RunRecord(
        run_id="scheduled__2024-01-01",
        dag_id="gold_dag",
        task_id="load_fact_trips",
        status=RunStatus.SUCCESS,
        started_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 0, 2, 22, tzinfo=timezone.utc),
        partition_date="2024-01-01",
        rows_written=9_823_441,
        duration_seconds=142.3,
        delta_path="s3://bucket/gold/fact_trips",
    )


@pytest.fixture()
def failed_record() -> RunRecord:
    return RunRecord(
        run_id="scheduled__2024-01-01",
        dag_id="gold_dag",
        task_id="load_fact_trips",
        status=RunStatus.FAILED,
        started_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        error_message="JDBC connection refused",
    )


def _make_mock_conn() -> MagicMock:
    mock_cur = MagicMock()
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur
    mock_conn.commit = MagicMock()
    return mock_conn


# ---------------------------------------------------------------------------
# RunRecord
# ---------------------------------------------------------------------------


class TestRunRecord:
    def test_frozen(self, success_record: RunRecord) -> None:
        with pytest.raises(Exception):
            success_record.rows_written = 0  # type: ignore[misc]

    def test_as_dict_contains_status_value(self, success_record: RunRecord) -> None:
        d = success_record.as_dict()
        assert d["status"] == "success"

    def test_as_dict_timestamps_are_iso_strings(
        self, success_record: RunRecord
    ) -> None:
        d = success_record.as_dict()
        assert "T" in d["started_at"]
        assert "T" in d["finished_at"]

    def test_default_metadata_is_empty_dict(self) -> None:
        r = RunRecord(
            run_id="r1",
            dag_id="d1",
            task_id="t1",
            status=RunStatus.RUNNING,
            started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert r.metadata == {}

    def test_status_enum_values(self) -> None:
        assert RunStatus.SUCCESS.value == "success"
        assert RunStatus.FAILED.value == "failed"
        assert RunStatus.RUNNING.value == "running"

    def test_failed_record_has_error_message(
        self, failed_record: RunRecord
    ) -> None:
        assert failed_record.error_message == "JDBC connection refused"
        assert failed_record.status == RunStatus.FAILED


# ---------------------------------------------------------------------------
# RunLogger.log
# ---------------------------------------------------------------------------


class TestRunLoggerLog:
    def test_successful_log_commits(
        self, run_logger: RunLogger, success_record: RunRecord
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.gold.run_logger.psycopg.connect", return_value=mock_conn):
            run_logger.log(success_record)
        mock_conn.commit.assert_called_once()

    def test_log_executes_create_table_and_insert(
        self, run_logger: RunLogger, success_record: RunRecord
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.gold.run_logger.psycopg.connect", return_value=mock_conn):
            run_logger.log(success_record)
        # Two execute calls: CREATE TABLE IF NOT EXISTS + INSERT
        assert mock_conn.cursor().__enter__().execute.call_count == 2

    def test_failed_record_logged_without_error(
        self, run_logger: RunLogger, failed_record: RunRecord
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.gold.run_logger.psycopg.connect", return_value=mock_conn):
            run_logger.log(failed_record)
        mock_conn.commit.assert_called_once()

    def test_postgres_error_wrapped(
        self, run_logger: RunLogger, success_record: RunRecord
    ) -> None:
        import psycopg as pg

        with patch(
            "pipeline.gold.run_logger.psycopg.connect",
            side_effect=pg.OperationalError("connection refused"),
        ):
            with pytest.raises(PostgresError) as exc_info:
                run_logger.log(success_record)
        assert exc_info.value.table == "pipeline_runs"

    def test_log_passes_correct_status_value(
        self, run_logger: RunLogger, success_record: RunRecord
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.gold.run_logger.psycopg.connect", return_value=mock_conn):
            run_logger.log(success_record)
        call_args = mock_conn.cursor().__enter__().execute.call_args_list
        insert_params = call_args[1][0][1]
        assert insert_params["status"] == "success"
