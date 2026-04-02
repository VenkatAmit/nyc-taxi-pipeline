"""
Run logging for the gold layer.

RunLogger writes a record to the pipeline_runs table in Postgres
after each GoldLoader execution — success or failure. This gives
operators a queryable audit trail without needing to dig through
Airflow logs.

RunRecord is a frozen dataclass: safe to pass between functions,
log as JSON, and use as a return value from GoldLoader.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import psycopg

from pipeline.exceptions import PostgresError
from pipeline.settings import PostgresSettings, get_settings

logger = logging.getLogger(__name__)

_CREATE_RUN_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          TEXT        NOT NULL,
    dag_id          TEXT        NOT NULL,
    task_id         TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    partition_date  DATE,
    rows_written    BIGINT,
    duration_seconds NUMERIC(10, 3),
    delta_path      TEXT,
    error_message   TEXT,
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at     TIMESTAMP WITH TIME ZONE,
    metadata        JSONB,
    PRIMARY KEY (run_id, dag_id, task_id)
);
"""

_INSERT_RUN_SQL = """
INSERT INTO pipeline_runs (
    run_id, dag_id, task_id, status, partition_date,
    rows_written, duration_seconds, delta_path, error_message,
    started_at, finished_at, metadata
)
VALUES (
    %(run_id)s, %(dag_id)s, %(task_id)s, %(status)s, %(partition_date)s,
    %(rows_written)s, %(duration_seconds)s, %(delta_path)s, %(error_message)s,
    %(started_at)s, %(finished_at)s, %(metadata)s
)
ON CONFLICT (run_id, dag_id, task_id) DO UPDATE SET
    status           = EXCLUDED.status,
    rows_written     = EXCLUDED.rows_written,
    duration_seconds = EXCLUDED.duration_seconds,
    error_message    = EXCLUDED.error_message,
    finished_at      = EXCLUDED.finished_at,
    metadata         = EXCLUDED.metadata;
"""


class RunStatus(str, Enum):
    """Pipeline run status values."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class RunRecord:
    """Immutable record of a single pipeline run.

    Attributes
    ----------
    run_id:
        Airflow run ID (e.g. scheduled__2024-01-01T00:00:00+00:00).
    dag_id:
        Airflow DAG ID.
    task_id:
        Airflow task ID within the DAG.
    status:
        Run status — one of RunStatus values.
    started_at:
        UTC timestamp when the run started.
    partition_date:
        Logical date of the data partition processed, if applicable.
    rows_written:
        Number of rows written to Delta Lake.
    duration_seconds:
        Wall-clock duration of the run in seconds.
    delta_path:
        S3 or local path of the Delta table written.
    error_message:
        Exception message if status is FAILED, else None.
    finished_at:
        UTC timestamp when the run finished.
    metadata:
        Arbitrary key-value pairs for extra context (Spark config,
        JDBC partition count, etc.).
    """

    run_id: str
    dag_id: str
    task_id: str
    status: RunStatus
    started_at: datetime
    partition_date: str | None = None
    rows_written: int | None = None
    duration_seconds: float | None = None
    delta_path: str | None = None
    error_message: str | None = None
    finished_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict for logging."""
        d = asdict(self)
        d["status"] = self.status.value
        d["started_at"] = self.started_at.isoformat()
        if self.finished_at:
            d["finished_at"] = self.finished_at.isoformat()
        return d


class RunLogger:
    """Writes RunRecord instances to the pipeline_runs Postgres table.

    Parameters
    ----------
    settings:
        PostgresSettings instance. Defaults to get_settings().postgres.

    Examples
    --------
    ::

        logger_svc = RunLogger()
        record = RunRecord(
            run_id="scheduled__2024-01-01",
            dag_id="gold_dag",
            task_id="load_fact_trips",
            status=RunStatus.SUCCESS,
            started_at=datetime.now(tz=timezone.utc),
            rows_written=9_823_441,
            duration_seconds=142.3,
            delta_path="s3://bucket/gold/fact_trips",
        )
        logger_svc.log(record)
    """

    def __init__(self, settings: PostgresSettings | None = None) -> None:
        self._settings = settings or get_settings().postgres

    def log(self, record: RunRecord) -> None:
        """Write a RunRecord to pipeline_runs.

        Uses INSERT ... ON CONFLICT DO UPDATE so re-logging the same
        (run_id, dag_id, task_id) is always safe — the latest values win.

        Raises
        ------
        PostgresError
            If the write fails.
        """
        try:
            with psycopg.connect(self._settings.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "CREATE TABLE IF NOT EXISTS pipeline_runs"
                        " AS SELECT * FROM pipeline_runs LIMIT 0"
                        if False
                        else _CREATE_RUN_LOG_TABLE_SQL
                    )
                    cur.execute(
                        _INSERT_RUN_SQL,
                        {
                            "run_id": record.run_id,
                            "dag_id": record.dag_id,
                            "task_id": record.task_id,
                            "status": record.status.value,
                            "partition_date": record.partition_date,
                            "rows_written": record.rows_written,
                            "duration_seconds": record.duration_seconds,
                            "delta_path": record.delta_path,
                            "error_message": record.error_message,
                            "started_at": record.started_at,
                            "finished_at": record.finished_at,
                            "metadata": json.dumps(record.metadata),
                        },
                    )
                conn.commit()
        except psycopg.Error as exc:
            raise PostgresError(
                "INSERT", "pipeline_runs", cause=exc
            ) from exc

        logger.info(
            "Run logged [%s] %s/%s — %s rows in %.1fs",
            record.status.value,
            record.dag_id,
            record.task_id,
            record.rows_written,
            record.duration_seconds or 0,
        )
