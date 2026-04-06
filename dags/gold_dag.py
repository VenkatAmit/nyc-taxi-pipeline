"""
Gold DAG — load business-ready Delta Lake tables.

Orchestrates the gold layer:
    1. load_fact_trips — GoldLoader.load_fact_trips()
    2. load_dim_zones  — GoldLoader.load_dim_zones()
    3. log_run         — RunLogger.log() for audit trail

Triggered after bronze_dag completes successfully via an
ExternalTaskSensor — gold should never run on stale bronze data.

Design rules
------------
- Tasks contain NO business logic
- GoldLoader.stop() is always called in finally blocks so the
  SparkSession is released even on failure
- RunRecord is written for both success and failure so operators
  can query pipeline_runs for the full audit trail
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from pipeline.gold.loader import GoldLoader
from pipeline.gold.run_logger import RunLogger, RunRecord, RunStatus
from pipeline.settings import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email_on_retry": False,
}


def _partition_date(logical_date: datetime) -> date:
    return logical_date.date()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


@dag(
    dag_id="gold_dag",
    description="Load business-ready Delta Lake gold tables from Postgres silver",
    schedule="0 8 1 * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["gold", "delta-lake", "spark"],
)
def gold_dag() -> None:

    # Wait for bronze_dag to succeed for the same logical date
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_dag",
        external_task_id=None,  # wait for the whole DAG
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task
    def load_fact_trips(
        logical_date: datetime | None = None,
        run_id: str | None = None,
    ) -> dict[str, object]:
        """Load silver_trips for this partition into fact_trips Delta table."""
        assert logical_date is not None
        assert run_id is not None

        partition = _partition_date(logical_date)
        started_at = datetime.now(tz=UTC)

        logger.info("Loading fact_trips for partition %s", partition)

        loader = GoldLoader()
        run_logger = RunLogger()
        rows_written = 0
        # status = RunStatus.RUNNING

        # Log start
        run_logger.log(
            RunRecord(
                run_id=run_id,
                dag_id="gold_dag",
                task_id="load_fact_trips",
                status=RunStatus.RUNNING,
                started_at=started_at,
                partition_date=str(partition),
            )
        )

        try:
            rows_written = loader.load_fact_trips(partition_date=partition)
            finished_at = datetime.now(tz=UTC)
            duration = (finished_at - started_at).total_seconds()

            run_logger.log(
                RunRecord(
                    run_id=run_id,
                    dag_id="gold_dag",
                    task_id="load_fact_trips",
                    status=RunStatus.SUCCESS,
                    started_at=started_at,
                    finished_at=finished_at,
                    partition_date=str(partition),
                    rows_written=rows_written,
                    duration_seconds=duration,
                    delta_path=f"{get_settings().delta.gold_base_path}/fact_trips",
                    metadata={
                        "jdbc_partitions": get_settings().spark.jdbc_num_partitions,
                    },
                )
            )

            logger.info(
                "Loaded %d rows into fact_trips in %.1fs", rows_written, duration
            )
            return {
                "rows_written": rows_written,
                "partition_date": str(partition),
                "duration_seconds": duration,
            }

        except Exception as exc:
            finished_at = datetime.now(tz=UTC)
            run_logger.log(
                RunRecord(
                    run_id=run_id,
                    dag_id="gold_dag",
                    task_id="load_fact_trips",
                    status=RunStatus.FAILED,
                    started_at=started_at,
                    finished_at=finished_at,
                    partition_date=str(partition),
                    rows_written=rows_written,
                    error_message=str(exc),
                )
            )
            raise

        finally:
            # Always release the SparkSession — Airflow reuses worker processes
            loader.stop()

    @task
    def load_dim_zones(run_id: str | None = None) -> dict[str, object]:
        """Load silver_zones into dim_zones Delta table (full refresh)."""
        assert run_id is not None

        started_at = datetime.now(tz=UTC)
        logger.info("Loading dim_zones (full refresh)")

        loader = GoldLoader()
        run_logger = RunLogger()
        rows_written = 0

        run_logger.log(
            RunRecord(
                run_id=run_id,
                dag_id="gold_dag",
                task_id="load_dim_zones",
                status=RunStatus.RUNNING,
                started_at=started_at,
            )
        )

        try:
            rows_written = loader.load_dim_zones()
            finished_at = datetime.now(tz=UTC)
            duration = (finished_at - started_at).total_seconds()

            run_logger.log(
                RunRecord(
                    run_id=run_id,
                    dag_id="gold_dag",
                    task_id="load_dim_zones",
                    status=RunStatus.SUCCESS,
                    started_at=started_at,
                    finished_at=finished_at,
                    rows_written=rows_written,
                    duration_seconds=duration,
                    delta_path=f"{get_settings().delta.gold_base_path}/dim_zones",
                )
            )

            logger.info("Loaded %d zones in %.1fs", rows_written, duration)
            return {
                "rows_written": rows_written,
                "duration_seconds": duration,
            }

        except Exception as exc:
            finished_at = datetime.now(tz=UTC)
            run_logger.log(
                RunRecord(
                    run_id=run_id,
                    dag_id="gold_dag",
                    task_id="load_dim_zones",
                    status=RunStatus.FAILED,
                    started_at=started_at,
                    finished_at=finished_at,
                    rows_written=rows_written,
                    error_message=str(exc),
                )
            )
            raise

        finally:
            loader.stop()

    # ---------------------------------------------------------------------------
    # Task dependencies
    #
    #   wait_for_bronze ──┬── load_fact_trips ──┐
    #                     └── load_dim_zones  ──┘ (both run in parallel)
    #

    fact_trips_result = load_fact_trips()
    dim_zones_result = load_dim_zones()

    wait_for_bronze >> [fact_trips_result, dim_zones_result]  # type: ignore[operator]


gold_dag()
