"""
airflow/tasks/load.py
---------------------
Observability layer: pulls XCom metrics from all upstream tasks
and writes one summary row to pipeline_run_log per DAG run.

This is your monitoring table. Every run is recorded with:
- Row counts per layer
- Per-task timing
- Data quality results
- Pipeline run ID for tracing

Why XCom and not re-querying the DB?
XCom values are pushed by each task at completion time.
Re-querying would require knowing what was written in this run
vs previous runs — XCom gives us exactly the current run metrics
without any filtering logic.
"""

import os
import logging
import psycopg2
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def get_pipeline_db_conn():
    return psycopg2.connect(
        host=os.environ["PIPELINE_DB_HOST"],
        port=os.environ["PIPELINE_DB_PORT"],
        dbname=os.environ["PIPELINE_DB_NAME"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )


def load(**context):
    """
    Aggregates XCom metrics from upstream tasks and writes
    one row to pipeline_run_log.

    Always runs last. Uses trigger_rule=ALL_DONE in the DAG
    so it records even partial failures — useful for debugging.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    # ── Pull XCom metrics from upstream tasks ─────────────────
    # Returns None if a task was skipped or failed before pushing
    rows_ingested = ti.xcom_pull(task_ids="ingest", key="rows_ingested")
    ingest_duration = ti.xcom_pull(task_ids="ingest", key="ingest_duration_sec")
    trip_month = ti.xcom_pull(task_ids="ingest", key="trip_month")

    # Spark and dbt metrics — None until those branches are merged
    rows_cleaned = ti.xcom_pull(task_ids="spark_transform", key="rows_cleaned")
    spark_duration = ti.xcom_pull(task_ids="spark_transform", key="spark_duration_sec")
    rows_transformed = ti.xcom_pull(task_ids="spark_transform", key="rows_transformed")
    dbt_duration = ti.xcom_pull(task_ids="dbt_run", key="dbt_duration_sec")
    quality_passed = ti.xcom_pull(task_ids="gx_validate", key="quality_passed")
    quality_notes = ti.xcom_pull(task_ids="gx_validate", key="quality_notes")

    run_completed_at = datetime.now(timezone.utc)
    # Total duration from DAG start
    run_started_at = context["dag_run"].start_date
    total_duration = None
    if run_started_at:
        total_duration = round((run_completed_at - run_started_at).total_seconds(), 2)

    log.info(
        f"Writing pipeline_run_log | run_id={run_id} | "
        f"month={trip_month} | rows_ingested={rows_ingested}"
    )

    insert_sql = """
        INSERT INTO pipeline_run_log (
            pipeline_run_id,
            dag_id,
            trip_month,
            run_started_at,
            run_completed_at,
            rows_ingested,
            rows_cleaned,
            rows_transformed,
            quality_passed,
            quality_notes,
            ingest_duration_sec,
            spark_duration_sec,
            dbt_duration_sec,
            total_duration_sec
        )
        VALUES (
            %(run_id)s,
            %(dag_id)s,
            %(trip_month)s,
            %(run_started_at)s,
            %(run_completed_at)s,
            %(rows_ingested)s,
            %(rows_cleaned)s,
            %(rows_transformed)s,
            %(quality_passed)s,
            %(quality_notes)s,
            %(ingest_duration)s,
            %(spark_duration)s,
            %(dbt_duration)s,
            %(total_duration)s
        )
        ON CONFLICT (pipeline_run_id)
        DO UPDATE SET
            run_completed_at  = EXCLUDED.run_completed_at,
            rows_ingested     = EXCLUDED.rows_ingested,
            rows_cleaned      = EXCLUDED.rows_cleaned,
            rows_transformed  = EXCLUDED.rows_transformed,
            quality_passed    = EXCLUDED.quality_passed,
            quality_notes     = EXCLUDED.quality_notes,
            ingest_duration_sec  = EXCLUDED.ingest_duration_sec,
            spark_duration_sec   = EXCLUDED.spark_duration_sec,
            dbt_duration_sec     = EXCLUDED.dbt_duration_sec,
            total_duration_sec   = EXCLUDED.total_duration_sec
    """

    conn = get_pipeline_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                {
                    "run_id": run_id,
                    "dag_id": dag_id,
                    "trip_month": trip_month,
                    "run_started_at": run_started_at,
                    "run_completed_at": run_completed_at,
                    "rows_ingested": rows_ingested,
                    "rows_cleaned": rows_cleaned,
                    "rows_transformed": rows_transformed,
                    "quality_passed": quality_passed,
                    "quality_notes": quality_notes,
                    "ingest_duration": ingest_duration,
                    "spark_duration": spark_duration,
                    "dbt_duration": dbt_duration,
                    "total_duration": total_duration,
                },
            )
        conn.commit()
        log.info("pipeline_run_log written successfully")
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to write run log: {e}")
        raise
    finally:
        conn.close()

    ti.xcom_push(key="load_completed_at", value=run_completed_at.isoformat())
    return run_id
