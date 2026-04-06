"""
Bronze DAG — raw ingestion and validation.

Orchestrates the bronze layer:
    1. ingest_trips  — BronzeIngestor.ingest_trips()
    2. ingest_zones  — BronzeIngestor.ingest_zones()
    3. validate_trips — GXValidator.validate_trips()
    4. validate_zones — GXValidator.validate_zones()

Design rules
------------
- Tasks contain NO business logic — they only call Service classes
- All branching, retries, and alerting are handled by Airflow
- The logical_date (formerly execution_date) drives partition_date
  so backfills work correctly without code changes

Schedule
--------
Monthly at 06:00 UTC on the 1st of each month.
Airflow cron: "0 6 1 * *"
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from pipeline.bronze.ingestor import BronzeIngestor
from pipeline.bronze.validator import GXValidator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email_on_retry": False,
}

# Source file root — override via Airflow Variable "bronze_source_root"
# e.g. s3://my-bucket/raw or /data/raw for local development
_DEFAULT_SOURCE_ROOT = "/data/raw"


def _source_root() -> str:
    """Read source root from Airflow Variable with fallback."""
    try:
        return str(Variable.get("bronze_source_root", default_var=_DEFAULT_SOURCE_ROOT))
    except Exception:
        return _DEFAULT_SOURCE_ROOT


def _partition_date(logical_date: datetime) -> date:
    """Derive the data partition date from the DAG logical_date.

    The logical_date is the start of the schedule interval, so for a
    daily DAG running at 06:00 UTC on 2024-01-02, logical_date is
    2024-01-01 — which is the correct partition date for that day's data.
    """
    return logical_date.date()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="bronze_dag",
    description="Ingest raw NYC taxi data into Postgres bronze tables",
    schedule="0 6 1 * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["bronze", "ingestion"],
)
def bronze_dag() -> None:

    @task
    def ingest_trips(logical_date: datetime | None = None) -> dict[str, object]:
        """Ingest raw trip data for the current partition date."""
        assert logical_date is not None, "logical_date must be provided by Airflow"
        partition = _partition_date(logical_date)
        source_root = _source_root()

        source_path = Path(source_root) / f"yellow_tripdata_{partition:%Y-%m}.parquet"

        logger.info("Ingesting trips: %s → partition %s", source_path, partition)

        ingestor = BronzeIngestor()
        result = ingestor.ingest_trips(
            source_path=source_path,
            partition_date=partition,
        )

        logger.info(
            "Ingested %d rows in %.2fs", result.rows_copied, result.duration_seconds
        )
        return {
            "table": result.table,
            "partition_date": str(result.partition_date),
            "rows_copied": result.rows_copied,
            "duration_seconds": result.duration_seconds,
        }

    @task
    def ingest_zones() -> dict[str, object]:
        """Ingest taxi zone lookup (full refresh, no partition)."""
        source_root = _source_root()
        source_path = Path(source_root) / "taxi_zone_lookup.csv"

        logger.info("Ingesting zones: %s", source_path)

        ingestor = BronzeIngestor()
        result = ingestor.ingest_zones(source_path=source_path)

        logger.info(
            "Ingested %d zones in %.2fs", result.rows_copied, result.duration_seconds
        )
        return {
            "table": result.table,
            "rows_copied": result.rows_copied,
            "duration_seconds": result.duration_seconds,
        }

    @task
    def validate_trips(
        logical_date: datetime | None = None,
        ingest_result: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Run GX expectations against raw_trips for this partition."""
        assert logical_date is not None
        partition = _partition_date(logical_date)

        logger.info("Validating trips for partition %s", partition)

        validator = GXValidator()
        report = validator.validate_trips(partition_date=partition)

        logger.info(
            "Validation %s: %d/%d expectations passed",
            "PASSED" if report.success else "FAILED",
            sum(1 for r in report.results if r.success),
            len(report.results),
        )

        # Raises ExpectationFailedError if any expectation failed —
        # Airflow marks the task failed and triggers retry/alert
        report.raise_if_failed()

        return {
            "success": report.success,
            "expectations_passed": sum(1 for r in report.results if r.success),
            "expectations_total": len(report.results),
            "total_failures": report.total_failure_count,
        }

    @task
    def validate_zones(
        ingest_result: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Run GX expectations against raw_zones."""
        logger.info("Validating zones")

        validator = GXValidator()
        report = validator.validate_zones()
        report.raise_if_failed()

        return {
            "success": report.success,
            "expectations_passed": sum(1 for r in report.results if r.success),
            "expectations_total": len(report.results),
        }

    # Silver dbt run — BashOperator delegates to dbt CLI
    run_silver = BashOperator(
        task_id="run_silver_dbt",
        bash_command=(
            "cd /opt/airflow/dbt "
            "&& dbt run --select staging.* silver.* --profiles-dir ."
        ),
        retries=1,
    )

    test_silver = BashOperator(
        task_id="test_silver_dbt",
        bash_command=(
            "cd /opt/airflow/dbt "
            "&& dbt test --select silver.* --profiles-dir ."
        ),
        retries=0,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    #
    #   ingest_trips ──┐
    #                  ├── validate_trips ──┐
    #   ingest_zones ──┘                   ├── run_silver ── test_silver
    #                  └── validate_zones ─┘
    #

    trips_result = ingest_trips()
    zones_result = ingest_zones()

    trips_validation = validate_trips(ingest_result=trips_result)
    zones_validation = validate_zones(ingest_result=zones_result)

    run_silver.set_upstream([trips_validation, zones_validation])  # type: ignore[arg-type]
    test_silver.set_upstream(run_silver)  # type: ignore[arg-type]


bronze_dag()
