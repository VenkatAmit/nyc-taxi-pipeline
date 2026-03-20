"""
airflow/tasks/ingest.py
-----------------------
Bronze layer: downloads one month of NYC TLC yellow taxi parquet
and bulk loads into raw_trips.

Why parquet and not CSV?
TLC publishes data in parquet format since 2022. Parquet is columnar,
compressed, and reads 10x faster than CSV for analytical queries.
This is what production pipelines consume.

XCom output:
    rows_ingested (int)
    ingest_duration_sec (float)
    trip_month (str) -- YYYY-MM
"""

import os
import time
import logging
import requests
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────
BATCH_SIZE = 10_000  # rows per insert batch


def get_pipeline_db_conn():
    return psycopg2.connect(
        host=os.environ["PIPELINE_DB_HOST"],
        port=os.environ["PIPELINE_DB_PORT"],
        dbname=os.environ["PIPELINE_DB_NAME"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )


def download_parquet(trip_month: str, data_dir: str) -> Path:
    """
    Download TLC yellow taxi parquet for the given month.
    Skips download if file already exists (idempotent).

    Args:
        trip_month: YYYY-MM format e.g. "2024-01"
        data_dir: local directory to save the file

    Returns:
        Path to the downloaded parquet file
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


def load_parquet_to_bronze(
    parquet_path: Path,
    trip_month: str,
    run_id: str,
    conn,
) -> int:
    """
    Read parquet file and bulk insert into raw_trips.
    Processes in chunks to avoid OOM on large files.

    Returns:
        Total rows inserted
    """
    log.info(f"Reading parquet: {parquet_path}")

    # TLC column name mapping — parquet uses camelCase
    column_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
    }

    df = pd.read_parquet(parquet_path)
    log.info(f"Parquet loaded: {len(df):,} rows x {df.shape[1]} columns")

    # Rename columns to snake_case
    df = df.rename(columns=column_map)

    # Keep only columns we have in the schema
    available = [c for c in column_map.values() if c in df.columns]
    df = df[available]

    # Add pipeline metadata columns
    df["trip_month"] = trip_month
    df["pipeline_run_id"] = run_id
    df["ingested_at"] = datetime.now(timezone.utc)

    db_columns = available + ["trip_month", "pipeline_run_id", "ingested_at"]

    insert_sql = f"""
        INSERT INTO raw_trips ({", ".join(db_columns)})
        VALUES %s
    """

    total_rows = 0
    # Process in chunks to avoid memory spikes during insert
    chunk_size = BATCH_SIZE
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]  # noqa: E203

        rows = []
        for row in chunk.itertuples(index=False):
            values = []
            for col in db_columns:
                val = getattr(row, col, None)
                # Convert pandas NA/NaT to None for psycopg2
                if pd.isna(val) if not isinstance(val, (str, datetime)) else False:
                    values.append(None)
                else:
                    values.append(val)
            rows.append(tuple(values))

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=BATCH_SIZE)
            conn.commit()
            total_rows += len(rows)
            log.info(
                f"Inserted batch {start // chunk_size + 1} "
                f"— cumulative {total_rows:,} rows"
            )
        except Exception as e:
            conn.rollback()
            log.error(f"Batch insert failed at row {start}: {e}")
            raise

    return total_rows


def ingest(**context):
    """
    Main ingest task callable for Airflow PythonOperator.

    Derives trip_month from the DAG execution date so the pipeline
    is backfill-ready — each scheduled run processes exactly one month.
    """
    start = time.time()
    run_id = context["run_id"]
    data_dir = os.environ.get("DATA_DIR", "/opt/airflow/data")

    # Derive trip month from data_interval_start
    # For @monthly DAG this is the first day of the month being processed
    # Falls back to logical_date if data_interval_start is not available
    data_interval_start = context.get("data_interval_start") or context.get(
        "logical_date"
    )
    trip_month = data_interval_start.strftime("%Y-%m")

    log.info(f"Starting ingest | run_id={run_id} | trip_month={trip_month}")

    # Download parquet
    parquet_path = download_parquet(trip_month, data_dir)

    # Load to bronze
    conn = get_pipeline_db_conn()
    try:
        rows_ingested = load_parquet_to_bronze(parquet_path, trip_month, run_id, conn)
    finally:
        conn.close()

    duration = round(time.time() - start, 2)
    log.info(
        f"Ingest complete | "
        f"month={trip_month} | rows={rows_ingested:,} | duration={duration}s"
    )

    # Push metrics to XCom for the load task to record
    ti = context["ti"]
    ti.xcom_push(key="rows_ingested", value=rows_ingested)
    ti.xcom_push(key="ingest_duration_sec", value=duration)
    ti.xcom_push(key="trip_month", value=trip_month)

    return rows_ingested
