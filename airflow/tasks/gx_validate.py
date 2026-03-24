"""
airflow/tasks/gx_validate.py
-----------------------------
Data quality layer: validates all three medallion layers.

Bronze: sampled from Delta Lake via PySpark (not Postgres).
Silver: sampled from Postgres cleaned_trips.
Gold:   row counts from agg_hourly_metrics + agg_zone_summary
        (updated from old trip_metrics / zone_summary names).

XCom output:
    quality_passed (bool)
    quality_notes (str)
"""

from typing import List, Tuple, Dict

import os
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import sqlalchemy
import great_expectations as ge

log = logging.getLogger(__name__)

REPORT_DIR = "/opt/airflow/ge_reports"
SAMPLE_SIZE = 50_000
DELTA_BRONZE_PATH = os.environ.get(
    "DELTA_BRONZE_PATH", "/opt/airflow/data/delta/bronze/yellow_tripdata"
)


def get_engine():
    host = os.environ["PIPELINE_DB_HOST"]
    port = os.environ["PIPELINE_DB_PORT"]
    db = os.environ["PIPELINE_DB_NAME"]
    user = os.environ["PIPELINE_DB_USER"]
    password = os.environ["PIPELINE_DB_PASSWORD"]
    return sqlalchemy.create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )


def get_row_count(engine, table: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM {table}"))
        return result.scalar()


def load_sample(
    engine, table: str, limit: int = SAMPLE_SIZE
) -> ge.dataset.PandasDataset:
    df = pd.read_sql(f"SELECT * FROM {table} LIMIT {limit}", engine)
    return ge.from_pandas(df)


def get_delta_spark_session():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("nyc_taxi_gx_bronze")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def validate_bronze(
    trip_month: str, rows_ingested: int = None
) -> Tuple[bool, List[str]]:
    """
    Validate bronze using XCom row count — avoids full Delta scan.
    """
    log.info("Validating bronze layer (Delta via XCom count)...")
    failures = []

    if rows_ingested is not None:
        count = rows_ingested
        log.info(f"Delta bronze row count for {trip_month} (from XCom): {count:,}")
        if not (1_000_000 <= count <= 5_000_000):
            failures.append(f"bronze row count out of range: {count:,}")
    else:
        log.warning("rows_ingested not available from XCom — skipping count check")

    passed = len(failures) == 0
    log.info(f"Bronze validation: {'PASS' if passed else 'FAIL'}")
    return passed, failures


def validate_silver(
    engine, trip_month: str = None, rows_cleaned: int = None
) -> Tuple[bool, List[str]]:
    log.info("Validating silver layer (cleaned_trips)...")
    failures = []

    if trip_month:
        with engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(
                    "SELECT COUNT(*) FROM cleaned_trips WHERE trip_month = :m"
                ),
                {"m": trip_month},
            )
            count = result.scalar()
        log.info(f"cleaned_trips row count for {trip_month}: {count:,}")
        if count < 100_000:
            failures.append(
                f"silver row count suspiciously low for {trip_month}: {count:,}"
            )
        if rows_cleaned and abs(count - rows_cleaned) > 100:
            failures.append(
                f"silver count mismatch: gx={count:,} vs spark_transform={rows_cleaned:,}"
            )
    else:
        count = get_row_count(engine, "cleaned_trips")
        log.info(f"cleaned_trips total row count: {count:,}")
        if count < 100_000:
            failures.append(f"silver row count suspiciously low: {count:,}")

    ds = load_sample(engine, "cleaned_trips")

    r = ds.expect_column_values_to_be_between(
        "fare_amount", min_value=-1500, max_value=5000
    )
    if not r["success"]:
        failures.append("silver fare_amount out of range")

    r = ds.expect_column_values_to_be_between(
        "trip_duration_min", min_value=0, max_value=300
    )
    if not r["success"]:
        failures.append("silver trip_duration_min out of range")

    r = ds.expect_column_values_to_not_be_null("pickup_datetime")
    if not r["success"]:
        failures.append("silver null pickup_datetime")

    passed = len(failures) == 0
    log.info(f"Silver validation: {'PASS' if passed else 'FAIL'}")
    return passed, failures


def validate_gold(engine, trip_month: str = None) -> Tuple[bool, List[str]]:
    """
    Validate gold layer — references updated to match the new
    star schema model names (agg_hourly_metrics, agg_zone_summary).
    """
    log.info("Validating gold layer (agg_hourly_metrics + agg_zone_summary)...")
    failures = []

    # Check table exists before querying
    with engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name='agg_hourly_metrics'"
            )
        )
        if result.scalar() == 0:
            failures.append("agg_hourly_metrics table does not exist yet")
            log.warning("Gold tables not yet created — skipping gold validation")
            return False, failures

    if trip_month:
        with engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(
                    "SELECT COUNT(*) FROM public.agg_hourly_metrics WHERE trip_month_num = :m"
                ),
                {"m": int(trip_month.split("-")[1])},
            )
            hm_count = result.scalar()
        log.info(f"agg_hourly_metrics row count for month {trip_month}: {hm_count}")
    else:
        hm_count = get_row_count(engine, "public.agg_hourly_metrics")
        log.info(f"agg_hourly_metrics total row count: {hm_count}")
    if hm_count < 100:
        failures.append(f"agg_hourly_metrics row count too low: {hm_count}")

    ds = load_sample(engine, "public.agg_hourly_metrics", limit=min(hm_count, 50_000))
    r = ds.expect_column_values_to_not_be_null("time_of_day_bucket")
    if not r["success"]:
        failures.append("agg_hourly_metrics null time_of_day_bucket")

    r = ds.expect_column_values_to_be_between(
        "avg_fare_amount", min_value=0, max_value=500
    )
    if not r["success"]:
        failures.append("agg_hourly_metrics avg_fare_amount out of range")

    zs_count = get_row_count(engine, "public.agg_zone_summary")
    log.info(f"agg_zone_summary row count: {zs_count}")
    if zs_count < 100:
        failures.append(f"agg_zone_summary row count: {zs_count}")

    ds = load_sample(engine, "public.agg_zone_summary", limit=252)
    r = ds.expect_column_values_to_not_be_null("zone_key")
    if not r["success"]:
        failures.append("agg_zone_summary null zone_key")

    passed = len(failures) == 0
    log.info(f"Gold validation: {'PASS' if passed else 'FAIL'}")
    return passed, failures


def build_html_report(results: Dict, report_path: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    overall = all(r["passed"] for r in results.values())
    status_color = "#2d6a4f" if overall else "#c1121f"
    status_label = "PASSED" if overall else "FAILED"

    rows = ""
    for layer, data in results.items():
        color = "#2d6a4f" if data["passed"] else "#c1121f"
        label = "PASS" if data["passed"] else "FAIL"
        notes = (
            "<br>".join(data["failures"]) if data["failures"] else "All checks passed"
        )
        rows += f"""
        <tr>
          <td>{layer}</td>
          <td style="color:{color};font-weight:600">{label}</td>
          <td>{notes}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>NYC Taxi — Data Quality Report</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; max-width: 860px;
            margin: 40px auto; padding: 0 20px; color: #1a1a2e; }}
    h1   {{ font-size: 22px; margin-bottom: 4px; }}
    .meta {{ color: #6c757d; font-size: 13px; margin-bottom: 24px; }}
    .badge {{ display: inline-block; padding: 6px 16px; border-radius: 20px;
              font-weight: 600; font-size: 15px; color: #fff;
              background: {status_color}; margin-bottom: 24px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th    {{ text-align: left; padding: 10px 12px; background: #f1f3f5;
             border-bottom: 2px solid #dee2e6; }}
    td    {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6;
             vertical-align: top; }}
    tr:last-child td {{ border-bottom: none; }}
  </style>
</head>
<body>
  <h1>NYC Taxi Pipeline — Data Quality Report</h1>
  <div class="meta">Generated: {timestamp} | Sample size: {SAMPLE_SIZE:,} rows</div>
  <div class="badge">{status_label}</div>
  <table>
    <thead><tr><th>Layer</th><th>Status</th><th>Notes</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>"""

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        f.write(html)
    log.info(f"HTML report written to {report_path}")


def gx_validate(**context):
    start = time.time()
    ti = context["ti"]
    run_id = context["run_id"]

    trip_month = ti.xcom_pull(task_ids="ingest", key="trip_month")
    if not trip_month:
        trip_month = context.get("data_interval_start").strftime("%Y-%m")

    engine = get_engine()

    rows_ingested = ti.xcom_pull(task_ids="ingest", key="rows_ingested")
    bronze_passed, bronze_failures = validate_bronze(trip_month, rows_ingested)
    rows_cleaned = ti.xcom_pull(task_ids="spark_transform", key="rows_cleaned")
    silver_passed, silver_failures = validate_silver(engine, trip_month, rows_cleaned)
    gold_passed, gold_failures = validate_gold(engine, trip_month)

    results = {
        "bronze (Delta)": {"passed": bronze_passed, "failures": bronze_failures},
        "silver (cleaned_trips)": {
            "passed": silver_passed,
            "failures": silver_failures,
        },
        "gold (agg_hourly_metrics + agg_zone_summary)": {
            "passed": gold_passed,
            "failures": gold_failures,
        },
    }

    overall_passed = bronze_passed and silver_passed and gold_passed

    safe_run_id = run_id.replace(":", "-").replace("+", "")
    report_path = f"{REPORT_DIR}/quality_report_{safe_run_id}.html"
    build_html_report(results, report_path)

    all_failures = bronze_failures + silver_failures + gold_failures
    quality_notes = "; ".join(all_failures) if all_failures else "all checks passed"

    duration = round(time.time() - start, 2)
    log.info(
        f"gx_validate complete | passed={overall_passed} | "
        f"duration={duration}s | notes={quality_notes}"
    )

    ti.xcom_push(key="quality_passed", value=overall_passed)
    ti.xcom_push(key="quality_notes", value=quality_notes)

    FAIL_ON_QUALITY_ISSUES = (
        os.environ.get("FAIL_ON_QUALITY_ISSUES", "false").lower() == "true"
    )
    if not overall_passed and FAIL_ON_QUALITY_ISSUES:
        raise ValueError(f"Data quality checks failed: {quality_notes}")

    return overall_passed
