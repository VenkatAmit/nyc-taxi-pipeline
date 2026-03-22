"""
airflow/tasks/delta_optimize.py
---------------------------------
Compacts small Delta files written during ingest and removes
old versions beyond the 7-day retention window.

Why this is needed:
- Each Spark write creates one Parquet file per partition worker
- Without OPTIMIZE, bronze accumulates 8-50 small files per month
- OPTIMIZE merges them into ~128 MB target files (Delta default)
- VACUUM removes _delta_log entries + Parquet files older than
  retentionHours, keeping storage clean

This runs after spark_transform because by then the bronze write
is complete and silver has already consumed the data for this run.
Running it before spark_transform would cause a concurrent write conflict.
"""

import os
import logging

log = logging.getLogger(__name__)

DELTA_BRONZE_PATH = os.environ.get(
    "DELTA_BRONZE_PATH", "/opt/airflow/data/delta/bronze/yellow_tripdata"
)


def get_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("nyc_taxi_delta_optimize")
        .config("spark.driver.memory", "2g")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def delta_optimize(**context):
    """
    OPTIMIZE + VACUUM the bronze Delta table.
    OPTIMIZE: bin-packs small files into ~128 MB files per partition.
    VACUUM:   deletes Parquet files no longer referenced by the
              Delta log, beyond the 168-hour (7-day) retention window.
    """
    ti = context["ti"]
    trip_month = ti.xcom_pull(task_ids="ingest", key="trip_month")

    log.info(f"Starting delta_optimize for bronze | trip_month={trip_month}")

    spark = get_spark_session()

    try:
        from delta.tables import DeltaTable

        dt = DeltaTable.forPath(spark, DELTA_BRONZE_PATH)

        # OPTIMIZE on the specific partition written this run
        log.info(f"Running OPTIMIZE on trip_month={trip_month}")
        dt.optimize().where(f"trip_month = '{trip_month}'").executeCompaction()
        log.info("OPTIMIZE complete")

        # VACUUM — removes files older than 168 hours (7 days)
        # retentionHours=168 preserves 7 days of time travel
        log.info("Running VACUUM (retentionHours=168)")
        dt.vacuum(retentionHours=168)
        log.info("VACUUM complete")

    finally:
        spark.stop()
        log.info("Spark session stopped")

    log.info("delta_optimize complete")
    return True
