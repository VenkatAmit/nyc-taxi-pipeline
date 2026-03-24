# Product Requirements Document
## NYC Taxi Medallion Data Pipeline

**Version:** 1.0
**Date:** March 24, 2026
**Author:** Venkat Amit Kommineni
**Status:** In Review

---

## 1. Overview

### 1.1 Purpose
Build a production-grade batch data pipeline that ingests, cleans, and aggregates
23 months of NYC TLC yellow taxi trip data (Jan 2024 - Nov 2025) into a queryable
analytical store, enabling downstream analysis of trip patterns, fare distributions,
and zone-level demand.

### 1.2 Problem Statement
NYC TLC publishes monthly yellow taxi trip data as raw Parquet files on a public
S3 endpoint. This raw data contains:
- Schema drift across years (new columns, type changes)
- Data quality issues (negative fares, impossible durations, invalid payment codes)
- No historical versioning or rollback capability
- No aggregated views suitable for direct BI consumption

There is no automated pipeline to ingest, clean, and serve this data in a structured,
queryable format. Analysts must manually download and process files, leading to
inconsistent results and no single source of truth.

### 1.3 Goals
- Automate ingestion of 23 months of TLC data with zero manual intervention
- Apply consistent cleaning rules to produce a reliable silver layer
- Build a star schema gold layer suitable for BI and analytics
- Provide per-run data quality reports with pass/fail status per layer
- Enable full backfill replay from scratch via idempotent pipeline design

### 1.4 Non-Goals
- Real-time or streaming ingestion (batch only, monthly cadence)
- Green taxi, FHV, or HVFHV data (yellow taxi only in v1)
- Serving a REST API or dashboard (pipeline only)
- Production cloud deployment (local Docker only in v1)

---

## 2. Users and Stakeholders

| Role | Need |
|------|------|
| Data Engineer | Reliable, observable pipeline with clear failure modes and logs |
| Data Analyst | Clean, queryable gold tables with consistent schema and no nulls in key columns |
| Engineering Manager | Pipeline health visibility via pipeline_run_log and GE reports |

---

## 3. Functional Requirements

### 3.1 Ingestion (Bronze Layer)

| ID | Requirement |
|----|-------------|
| F-01 | Download one month of TLC yellow taxi Parquet (~50 MB) per pipeline run |
| F-02 | Skip download if file already exists (idempotent) |
| F-03 | Cast TimestampNTZ columns to Timestamp before writing to Delta |
| F-04 | Write to Delta Lake partitioned by trip_month using replaceWhere (ACID, idempotent) |
| F-05 | Push rows_ingested, trip_month, ingest_duration_sec to Airflow XCom |

### 3.2 Transformation (Silver Layer)

| ID | Requirement |
|----|-------------|
| F-06 | Read bronze Delta for the current trip_month only |
| F-07 | Apply 5 cleaning rules: date scope, fare range, distance range, duration range, valid payment type |
| F-08 | Compute derived columns: trip_duration_min, speed_mph, is_airport_trip, tip_percentage with outlier caps |
| F-09 | Write cleaned rows to Postgres cleaned_trips via JDBC, deleting existing rows for the month first |
| F-10 | Push rows_cleaned, rows_dropped, spark_duration_sec to XCom |

### 3.3 Delta Maintenance

| ID | Requirement |
|----|-------------|
| F-11 | Run OPTIMIZE on the current month's bronze partition after each transform |
| F-12 | Run VACUUM with 168-hour (7-day) retention to remove stale Parquet files |

### 3.4 Gold Layer (dbt)

| ID | Requirement |
|----|-------------|
| F-13 | Seed taxi_zones lookup table (265 NYC zones) before each run |
| F-14 | Build 3 dimension tables: dim_date, dim_zone, dim_vendor |
| F-15 | Build fact_trips with MD5 surrogate key and FK references to all dims |
| F-16 | Build 2 aggregate tables: agg_hourly_metrics, agg_zone_summary |
| F-17 | Run 21 dbt schema tests and fail the task if any test fails |

### 3.5 Data Quality (Great Expectations)

| ID | Requirement |
|----|-------------|
| F-18 | Validate bronze row count against XCom value from ingest |
| F-19 | Validate silver per-month row count against XCom value from spark_transform |
| F-20 | Check silver null values for pickup_datetime, fare_amount, trip_distance |
| F-21 | Validate gold table existence and row counts for agg_hourly_metrics and agg_zone_summary |
| F-22 | Write one HTML report per run to great_expectations/reports/ |
| F-23 | Pipeline continues regardless of quality failures by default (FAIL_ON_QUALITY_ISSUES=false) |

### 3.6 Observability

| ID | Requirement |
|----|-------------|
| F-24 | Write one row to pipeline_run_log per DAG run with timing, row counts, and quality results |
| F-25 | Load task always runs last with trigger_rule=ALL_DONE to capture partial failure metrics |

---

## 4. Non-Functional Requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NF-01 | Idempotency | Every task must be safely re-runnable with no duplicate data on retry |
| NF-02 | Observability | Every task pushes structured metrics to XCom |
| NF-03 | Memory | Pipeline must run on a single machine with 8 GB RAM available to Docker |
| NF-04 | Throughput | Each monthly run must complete within 2 hours (execution_timeout) |
| NF-05 | Backfill | Full 23-month backfill must complete sequentially without manual intervention |
| NF-06 | Code quality | All Python files pass black + flake8; all SQL files pass sqlfluff via pre-commit |
| NF-07 | Schema safety | All NUMERIC columns must be sized to handle real-world outliers without overflow |

---

## 5. Data Model

### Bronze
- **Storage:** Delta Lake at `/opt/airflow/data/delta/bronze/yellow_tripdata`
- **Partition:** `trip_month` (YYYY-MM)
- **Format:** Parquet (Delta versioned with time travel)

### Silver
- **Table:** `cleaned_trips` (Postgres)
- **Key columns:** `pickup_datetime`, `trip_month`, `fare_amount`, `trip_distance`
- **Derived:** `trip_duration_min`, `speed_mph`, `is_airport_trip`, `tip_percentage`

### Gold (star schema)

| Table | Purpose |
|-------|---------|
| `dim_date` | Hour-level date dimension with time_of_day_bucket, is_weekend |
| `dim_zone` | 265 NYC taxi zones with borough and service_zone |
| `dim_vendor` | 3 vendors (Creative Mobile, VeriFone, unknown) |
| `fact_trips` | Core fact table with FK references to all dims |
| `agg_hourly_metrics` | Hourly aggregates by date_key and borough |
| `agg_zone_summary` | Monthly aggregates by pickup zone |

### Meta
- **Table:** `pipeline_run_log` — one row per DAG run with full timing and quality results

---

## 6. Cleaning Rules

| Rule | Field | Condition | Rationale |
|------|-------|-----------|----------|
| Date scope | tpep_pickup_datetime | Within trip_month calendar month | TLC files contain cross-month records |
| Valid fare | fare_amount | > 0 and <= 500 | Voided trips and extreme outliers |
| Valid distance | trip_distance | > 0.01 and <= 200 | Meter artifacts and GPS errors |
| Valid duration | dropoff minus pickup | > 0 and <= 300 min | Unclosed meters and clock errors |
| Valid payment | payment_type | In {1, 2, 3, 4, 5, 6} | TLC data dictionary codes only |

Expected drop rate: 8-22% per month depending on data quality.

---

## 7. Pipeline DAG

```
ingest -> spark_transform -> delta_optimize -> dbt_run -> gx_validate -> load
```

| Task | Retry | Timeout | trigger_rule |
|------|-------|---------|-------------|
| ingest | 1 | 2 hours | all_success |
| spark_transform | 1 | 2 hours | all_success |
| delta_optimize | 1 | 2 hours | all_success |
| dbt_run | 1 | 2 hours | all_success |
| gx_validate | 1 | 2 hours | all_success |
| load | 1 | 2 hours | all_done |

---

## 8. Tech Stack

| Component | Technology | Version |
|-----------|-----------|--------|
| Orchestration | Apache Airflow | 2.8.1 |
| Processing | PySpark | 3.4.1 |
| Bronze storage | Delta Lake | 2.4.0 |
| Silver/Gold storage | PostgreSQL | 15 |
| SQL transformation | dbt-postgres | 1.7.18 |
| Data quality | Great Expectations | 0.17.23 |
| Containerization | Docker + Compose | latest |

---

## 9. Out of Scope for v1

- Cloud deployment (AWS/GCP/Azure)
- Streaming ingestion
- Green taxi, FHV, HVFHV data
- BI dashboard (Superset/Metabase)
- Automated alerting on quality failures
- dbt incremental models (currently all materialized=table)
- Unit test suite for Python tasks

---

## 10. Success Criteria

| Metric | Target |
|--------|--------|
| Backfill completion | All 23 months complete end-to-end with no manual intervention |
| dbt test pass rate | 21/21 tests passing on every run |
| Silver row count | > 1M rows per month after cleaning |
| Pipeline run time | < 2 hours per month |
| Data quality | Bronze and silver PASS on GE report for all completed months |
| Observability | pipeline_run_log has one row per completed run with non-null timing |
