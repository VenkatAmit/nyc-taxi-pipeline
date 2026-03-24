"""
airflow/tasks/ingest.py
-----------------------
Bronze layer: downloads one month of NYC TLC yellow taxi parquet
and writes to Delta Lake at DELTA_BRONZE_PATH/yellow_tripdata.

Why Delta instead of Postgres for bronze?
- Schema evolution: TLC adds columns yearly; Delta handles this automatically
- Time travel: every write is versioned — you can rewind to any run
- ACID writes: partial failures leave the table in a consistent state
- Parquet-native: no serialization overhead vs JDBC row-by-row insert

XCom output:
    rows_ingested (int)
    ingest_duration_sec (float)
    trip_month (str) -- YYYY-MM
"""

import os
import time
import logging
import requests
from pathlib import Path

log = logging.getLogger(__name__)

DELTA_BRONZE_PATH = os.environ.get(
    "DELTA_BRONZE_PATH", "/opt/airflow/data/delta/bronze/yellow_tripdata"
)


def download_parquet(trip_month: str, data_dir: str) -> Path:
    """
    Download TLC yellow taxi parquet for the given month.
    Skips download if file already exists (idempotent).
    """
    base_url = os.environ.get(
        "TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
    )
    filename = f"yellow_tripdata_{trip_month}.parquet"
    url = f"{base_url}/{filename}"
    dest = Path(data_dir) / filename

    if dest.exists():
        log.info(f"File already exists, skipping download: {dest}")
        return dest

    log.info(f"Downloading {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_bytes = 0
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            total_bytes += len(chunk)

    size_mb = total_bytes / 1024 / 1024
    log.info(f"Downloaded {filename} — {size_mb:.1f} MB")
    return dest


def get_spark_session():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("nyc_taxi_bronze_ingest")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_bronze_delta(parquet_path: Path, trip_month: str, run_id: str) -> int:
    """
    Read parquet file and write to Delta bronze table.
    Uses replaceWhere for idempotency — overwrites only the
    partition for this trip_month, leaving other months intact.
    """
    spark = get_spark_session()

    try:
        log.info(f"Reading parquet: {parquet_path}")
        df = spark.read.parquet(str(parquet_path))
        log.info(f"Parquet loaded: {len(df.columns)} columns")

        # Add pipeline metadata columns
        from pyspark.sql import functions as F

        df = (
            df.withColumn("trip_month", F.lit(trip_month))
            .withColumn("pipeline_run_id", F.lit(run_id))
            .withColumn("ingested_at", F.current_timestamp())
        )

        row_count = df.count()

        # Write to Delta — replaceWhere overwrites only this month's partition
        # This is the Delta equivalent of DELETE + INSERT and is ACID-safe
        log.info(f"Writing {row_count:,} rows to Delta bronze: {DELTA_BRONZE_PATH}")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"trip_month = '{trip_month}'")
            .partitionBy("trip_month")
            .save(DELTA_BRONZE_PATH)
        )

        log.info(f"Delta bronze write complete: {row_count:,} rows")
        return row_count

    finally:
        spark.stop()
        log.info("Spark session stopped")


def ingest(**context):
    """
    Main ingest task callable for Airflow PythonOperator.
    """
    start = time.time()
    run_id = context["run_id"]
    data_dir = os.environ.get("DATA_DIR", "/opt/airflow/data")

    data_interval_start = context.get("data_interval_start") or context.get(
        "logical_date"
    )
    trip_month = data_interval_start.strftime("%Y-%m")

    log.info(f"Starting ingest | run_id={run_id} | trip_month={trip_month}")

    parquet_path = download_parquet(trip_month, data_dir)
    rows_ingested = write_bronze_delta(parquet_path, trip_month, run_id)

    duration = round(time.time() - start, 2)
    log.info(
        f"Ingest complete | "
        f"month={trip_month} | rows={rows_ingested:,} | duration={duration}s"
    )

    ti = context["ti"]
    ti.xcom_push(key="rows_ingested", value=rows_ingested)
    ti.xcom_push(key="ingest_duration_sec", value=duration)
    ti.xcom_push(key="trip_month", value=trip_month)

    return rows_ingested
