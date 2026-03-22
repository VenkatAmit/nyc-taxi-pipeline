"""
airflow/tasks/gx_validate.py
-----------------------------
Data quality layer: validates all three medallion layers using
Great Expectations and generates an HTML report.

Validation suites:
  bronze — raw_trips:         row count, no nulls on key columns
  silver — cleaned_trips:     row count, fare > 0, duration > 0
  gold   — trip_metrics:      exact row count, no null pickup_hour
  gold   — zone_summary:      exact row count, no null pu_location_id

HTML report written to /opt/airflow/ge_reports/
(mounted to ./great_expectations/reports/ on the host)

XCom output:
    quality_passed (bool)
    quality_notes (str)   — summary of any failures
"""

import os
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import sqlalchemy
import great_expectations as ge

log = logging.getLogger(__name__)

REPORT_DIR = "/opt/airflow/ge_reports"


def get_engine():
    host = os.environ["PIPELINE_DB_HOST"]
    port = os.environ["PIPELINE_DB_PORT"]
    db = os.environ["PIPELINE_DB_NAME"]
    user = os.environ["PIPELINE_DB_USER"]
    password = os.environ["PIPELINE_DB_PASSWORD"]
    return sqlalchemy.create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )


def load_table(engine, query: str) -> ge.dataset.PandasDataset:
    """Load a query result into a GE PandasDataset."""
    df = pd.read_sql(query, engine)
    return ge.from_pandas(df)


def validate_bronze(engine) -> tuple[bool, list[str]]:
    log.info("Validating bronze layer (raw_trips)...")
    ds = load_table(engine, "SELECT * FROM raw_trips LIMIT 5000000")
    failures = []

    r = ds.expect_table_row_count_to_be_between(
        min_value=3_000_000, max_value=4_000_000
    )
    if not r["success"]:
        failures.append(f"bronze row count: {r['result']['observed_value']:,}")

    for col in ["pickup_datetime", "dropoff_datetime", "fare_amount", "trip_distance"]:
        r = ds.expect_column_values_to_not_be_null(col)
        if not r["success"]:
            failures.append(f"bronze null check failed: {col}")

    r = ds.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=500)
    if not r["success"]:
        failures.append("bronze fare_amount out of range")

    passed = len(failures) == 0
    log.info(
        f"Bronze validation: {'PASS' if passed else 'FAIL'} — {failures or 'all checks passed'}"
    )
    return passed, failures


def validate_silver(engine) -> tuple[bool, list[str]]:
    log.info("Validating silver layer (cleaned_trips)...")
    ds = load_table(engine, "SELECT * FROM cleaned_trips LIMIT 5000000")
    failures = []

    r = ds.expect_table_row_count_to_be_between(
        min_value=3_000_000, max_value=3_200_000
    )
    if not r["success"]:
        failures.append(f"silver row count: {r['result']['observed_value']:,}")

    r = ds.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=500)
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
    log.info(
        f"Silver validation: {'PASS' if passed else 'FAIL'} — {failures or 'all checks passed'}"
    )
    return passed, failures


def validate_gold(engine) -> tuple[bool, list[str]]:
    log.info("Validating gold layer (trip_metrics + zone_summary)...")
    failures = []

    # trip_metrics
    ds = load_table(engine, "SELECT * FROM public_public.trip_metrics")
    r = ds.expect_table_row_count_to_equal(750)
    if not r["success"]:
        failures.append(f"trip_metrics row count: {r['result']['observed_value']}")

    r = ds.expect_column_values_to_not_be_null("pickup_hour")
    if not r["success"]:
        failures.append("trip_metrics null pickup_hour")

    r = ds.expect_column_values_to_be_between(
        "avg_fare_usd", min_value=0, max_value=500
    )
    if not r["success"]:
        failures.append("trip_metrics avg_fare_usd out of range")

    # zone_summary
    ds = load_table(engine, "SELECT * FROM public_public.zone_summary")
    r = ds.expect_table_row_count_to_equal(252)
    if not r["success"]:
        failures.append(f"zone_summary row count: {r['result']['observed_value']}")

    r = ds.expect_column_values_to_not_be_null("pu_location_id")
    if not r["success"]:
        failures.append("zone_summary null pu_location_id")

    passed = len(failures) == 0
    log.info(
        f"Gold validation: {'PASS' if passed else 'FAIL'} — {failures or 'all checks passed'}"
    )
    return passed, failures


def build_html_report(results: dict, report_path: str):
    """Generate a standalone HTML report from validation results."""
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
  <div class="meta">Generated: {timestamp}</div>
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
    """
    Main GE validation task callable for Airflow PythonOperator.
    Validates all three layers, writes HTML report, pushes XCom.
    """
    start = time.time()
    ti = context["ti"]
    run_id = context["run_id"]

    engine = get_engine()

    bronze_passed, bronze_failures = validate_bronze(engine)
    silver_passed, silver_failures = validate_silver(engine)
    gold_passed, gold_failures = validate_gold(engine)

    results = {
        "bronze (raw_trips)": {"passed": bronze_passed, "failures": bronze_failures},
        "silver (cleaned_trips)": {
            "passed": silver_passed,
            "failures": silver_failures,
        },
        "gold (trip_metrics + zone_summary)": {
            "passed": gold_passed,
            "failures": gold_failures,
        },
    }

    overall_passed = bronze_passed and silver_passed and gold_passed

    # Write HTML report — filename includes run_id for history
    safe_run_id = run_id.replace(":", "-").replace("+", "")
    report_path = f"{REPORT_DIR}/quality_report_{safe_run_id}.html"
    build_html_report(results, report_path)

    # Build quality_notes string for pipeline_run_log
    all_failures = bronze_failures + silver_failures + gold_failures
    quality_notes = "; ".join(all_failures) if all_failures else "all checks passed"

    duration = round(time.time() - start, 2)
    log.info(
        f"gx_validate complete | passed={overall_passed} | "
        f"duration={duration}s | notes={quality_notes}"
    )

    ti.xcom_push(key="quality_passed", value=overall_passed)
    ti.xcom_push(key="quality_notes", value=quality_notes)

    # Do not fail the task on quality issues — log and report instead
    # Set this to True if you want the pipeline to hard-fail on bad data
    FAIL_ON_QUALITY_ISSUES = False
    if not overall_passed and FAIL_ON_QUALITY_ISSUES:
        raise ValueError(f"Data quality checks failed: {quality_notes}")

    return overall_passed
