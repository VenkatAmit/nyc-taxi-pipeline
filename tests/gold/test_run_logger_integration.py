"""
RunLogger integration tests using shared conftest fixtures.
"""

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
def run_logger(postgres_settings: PostgresSettings) -> RunLogger:
    return RunLogger(settings=postgres_settings)


@pytest.fixture()
def success_record(
    sample_run_id: str,
    sample_started_at: datetime,
) -> RunRecord:
    return RunRecord(
        run_id=sample_run_id,
        dag_id="gold_dag",
        task_id="load_fact_trips",
        status=RunStatus.SUCCESS,
        started_at=sample_started_at,
        finished_at=datetime(2024, 1, 15, 6, 2, 22, tzinfo=timezone.utc),
        partition_date="2024-01-15",
        rows_written=9_823_441,
        duration_seconds=142.3,
        delta_path="s3://bucket/gold/fact_trips",
        metadata={"jdbc_partitions": 8},
    )


@pytest.fixture()
def failed_record(
    sample_run_id: str,
    sample_started_at: datetime,
) -> RunRecord:
    return RunRecord(
        run_id=sample_run_id,
        dag_id="gold_dag",
        task_id="load_fact_trips",
        status=RunStatus.FAILED,
        started_at=sample_started_at,
        error_message="JDBC connection refused after 3 retries",
    )


# ---------------------------------------------------------------------------
# RunRecord
# ---------------------------------------------------------------------------


class TestRunRecordWithSharedFixtures:
    def test_success_record_fields(self, success_record: RunRecord) -> None:
        assert success_record.status == RunStatus.SUCCESS
        assert success_record.rows_written == 9_823_441
        assert success_record.dag_id == "gold_dag"

    def test_failed_record_has_error(self, failed_record: RunRecord) -> None:
        assert failed_record.status == RunStatus.FAILED
        assert failed_record.error_message is not None
        assert failed_record.rows_written is None

    def test_as_dict_is_json_serialisable(self, success_record: RunRecord) -> None:
        import json

        d = success_record.as_dict()
        # Should not raise
        serialised = json.dumps(d)
        assert "success" in serialised

    def test_metadata_preserved_in_dict(self, success_record: RunRecord) -> None:
        d = success_record.as_dict()
        assert d["metadata"]["jdbc_partitions"] == 8

    def test_frozen_prevents_mutation(self, success_record: RunRecord) -> None:
        with pytest.raises(Exception):
            success_record.rows_written = 0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# RunLogger.log
# ---------------------------------------------------------------------------


class TestRunLoggerWithSharedFixtures:
    def test_log_success_record(
        self,
        run_logger: RunLogger,
        success_record: RunRecord,
        mock_psycopg_conn: MagicMock,
    ) -> None:
        with patch(
            "pipeline.gold.run_logger.psycopg.connect",
            return_value=mock_psycopg_conn,
        ):
            run_logger.log(success_record)

        mock_psycopg_conn.commit.assert_called_once()

    def test_log_failed_record(
        self,
        run_logger: RunLogger,
        failed_record: RunRecord,
        mock_psycopg_conn: MagicMock,
    ) -> None:
        with patch(
            "pipeline.gold.run_logger.psycopg.connect",
            return_value=mock_psycopg_conn,
        ):
            run_logger.log(failed_record)

        mock_psycopg_conn.commit.assert_called_once()

    def test_log_wraps_psycopg_error(
        self,
        run_logger: RunLogger,
        success_record: RunRecord,
    ) -> None:
        import psycopg as pg

        with patch(
            "pipeline.gold.run_logger.psycopg.connect",
            side_effect=pg.OperationalError("db down"),
        ):
            with pytest.raises(PostgresError) as exc_info:
                run_logger.log(success_record)

        assert exc_info.value.table == "pipeline_runs"

    def test_log_passes_status_value_not_enum(
        self,
        run_logger: RunLogger,
        success_record: RunRecord,
        mock_psycopg_conn: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        with patch(
            "pipeline.gold.run_logger.psycopg.connect",
            return_value=mock_psycopg_conn,
        ):
            run_logger.log(success_record)

        insert_call = mock_cursor.execute.call_args_list[-1]
        params = insert_call[0][1]
        assert params["status"] == "success"
        assert isinstance(params["status"], str)
