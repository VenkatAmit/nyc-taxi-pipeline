# Pipeline

End-to-end walkthrough of how data moves from the TLC public dataset to the Gold Delta Lake tables. This document covers every task, transformation, and handoff in execution order.

---

## Overview

The pipeline runs on a monthly cadence. Each calendar month of TLC data is processed as a single batch, identified by a `partition_date` (the first day of the month, e.g. `2024-01-01`).

Two Airflow DAGs execute sequentially:

```
bronze_dag  (06:00 UTC, 1st of month)
│
├── ingest_trips       Download parquet → Postgres raw_trips
├── validate_bronze    Great Expectations suite
├── run_silver_dbt     dbt run staging.* silver.*
└── test_silver_dbt    dbt test silver.*
        │
        ▼ (ExternalTaskSensor, +2h offset)
gold_dag  (08:00 UTC, 1st of month)
│
└── load_gold          PySpark JDBC read → Delta Lake MERGE
```

Both DAGs have `max_active_runs=1` to prevent concurrent Spark and Postgres resource contention across months during backfill.

---

## Stage 1 — ingest_trips

**DAG:** `bronze_dag`
**Class:** `BronzeIngestor` (`pipeline/bronze/ingestor.py`)
**Operator:** `PythonOperator`

### What happens

1. Constructs the TLC parquet URL for the target month:
   `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
2. Streams the parquet file in chunks (~50 MB per month) — no full file held in memory.
3. Deletes any existing rows in `raw_trips` where `partition_date = target_date` (idempotent re-run).
4. Writes rows to Postgres using psycopg3 `COPY` protocol — the fastest available Postgres bulk-insert path.
5. Pushes the ingested row count to XCom for downstream validation.

### Output

| Table | Schema | Rows (typical) |
|---|---|---|
| `raw_trips` | `public` | 2.5M – 3.5M per month |

### Key design decisions

- psycopg3 `COPY` is used instead of `INSERT` — for 3M rows this is approximately 10× faster.
- Streaming download avoids memory spikes from holding a full parquet file in the worker.
- `partition_date` delete-before-insert ensures re-runs are safe with no duplicate rows.
- TLC parquet files contain `TimestampNTZ` columns. These are cast to `TIMESTAMP WITH TIME ZONE` during ingestion.

---

## Stage 2 — validate_bronze

**DAG:** `bronze_dag`
**Class:** `GXValidator` (`pipeline/bronze/validator.py`)
**Operator:** `PythonOperator`

### What happens

1. Opens a Great Expectations SQL datasource against the `pipeline_db` Postgres instance.
2. Retrieves the XCom row count from `ingest_trips`.
3. Runs four expectation classes against the `partition_date` slice of `raw_trips`:

| Expectation | What it checks |
|---|---|
| `TripsRowCountExpectation` | Row count within expected bounds for the month |
| `TripsNullExpectation` | Null rate on `pickup_datetime`, `dropoff_datetime`, `pu_location_id`, `do_location_id` below threshold |
| `TripsPositiveAmountExpectation` | `fare_amount`, `tip_amount`, `total_amount` between –$500 and $100,000 (`mostly=0.99`) |
| `TripsDateRangeExpectation` | `pickup_datetime` falls within the partition month |

4. Writes an HTML report to `/opt/airflow/ge_reports/` for each run.
5. Raises `ValidationError` if any expectation's `success` flag is `False`.

### GX version

Great Expectations is pinned to `0.18.19`. Version 1.x removed the `context.sources` API used by this pipeline. The pin is enforced in `Dockerfile.airflow` and documented in [ADR 004](adr/004-great-expectations-0.18.md).

### Output

- Validation result written to Airflow XCom
- HTML report written to `great_expectations/reports/`

---

## Stage 3 — run_silver_dbt

**DAG:** `bronze_dag`
**Operator:** `BashOperator`

### What happens

```bash
cd /opt/airflow/dbt && dbt run --select staging.* silver.* --profiles-dir .
```

dbt executes four models in dependency order:

```
stg_trips (view)
stg_zones (view)
    │
    ▼
silver_zones (table, full refresh)
silver_trips (table, incremental)
```

### stg_trips

A view over `raw_trips`. Applies the following filters — rows that fail any filter are silently excluded from silver:

| Filter | Condition |
|---|---|
| Date range | `pickup_datetime` between `2009-01-01` and `2030-01-01` |
| Distance | `trip_distance > 0` |
| Fare | `fare_amount > 0` |
| Duration | `dropoff_datetime > pickup_datetime` |
| Payment type | `payment_type IN (1, 2, 3, 4, 5, 6)` |

### silver_zones

Full-refresh table built from the `taxi_zones` dbt seed (265 NYC TLC zones). Adds:
- `borough` (cleaned, standardised)
- `service_zone_code` (normalised to `yellow`, `boro`, `ewr`, `unknown`)
- `is_manhattan` boolean

### silver_trips

Incremental table. On each run:

1. dbt computes the incoming `partition_date` from the staging view.
2. Deletes existing rows matching that `partition_date` (`delete+insert` strategy).
3. Inserts the new batch with all enriched columns.

**Enriched columns added in silver:**

| Column | Type | Derivation |
|---|---|---|
| `trip_id` | `text` | MD5 of 6 fields + ROW_NUMBER() |
| `trip_duration_minutes` | `numeric` | `EXTRACT(EPOCH FROM dropoff - pickup) / 60` |
| `fare_per_mile` | `numeric` | `fare_amount / trip_distance` (null if distance = 0) |
| `tip_pct` | `numeric` | `tip_amount / total_amount * 100` |
| `has_tip` | `boolean` | `tip_amount > 0` |
| `is_airport_trip` | `boolean` | pickup or dropoff zone in JFK / LGA / EWR zone IDs |
| `transformed_at` | `timestamptz` | `NOW()` at dbt run time |

---

## Stage 4 — test_silver_dbt

**DAG:** `bronze_dag`
**Operator:** `BashOperator`

### What happens

```bash
cd /opt/airflow/dbt && dbt test --select silver.* --profiles-dir .
```

Runs 21 schema tests defined in `dbt/models/silver/schema.yml`:

| Test type | Columns tested |
|---|---|
| `not_null` | `trip_id`, `pickup_datetime`, `dropoff_datetime`, `pu_location_id`, `do_location_id`, `fare_amount`, `total_amount`, `partition_date`, `transformed_at` |
| `unique` | `trip_id`, `location_id` (silver_zones) |
| `accepted_values` — borough | Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, Unknown, N/A |
| `accepted_values` — service_zone_code | yellow, boro, ewr, unknown |
| `relationships` | `pu_location_id` → `silver_zones.location_id`, `do_location_id` → `silver_zones.location_id` |

A test failure raises `AirflowException` and blocks `gold_dag` from running via the `ExternalTaskSensor`.

---

## Stage 5 — load_gold

**DAG:** `gold_dag`
**Class:** `GoldLoader` (`pipeline/gold/loader.py`)
**Operator:** `PythonOperator`

### What happens

The `gold_dag` waits for `bronze_dag` to reach `success` state via `ExternalTaskSensor` (with `execution_delta=timedelta(hours=2)`, `mode="reschedule"`). Once unblocked:

1. **SparkSession initialised** — `get_spark()` builds a singleton session with Delta Lake extensions, Delta catalog, and the PostgreSQL JDBC jar (`org.postgresql:postgresql:42.7.3`) loaded via Maven.

2. **JDBC read from silver** — Spark reads `public_silver.silver_trips` partitioned by `partition_date` ranges across `SPARK_JDBC_NUM_PARTITIONS` parallel JDBC connections. This avoids a single serial scan of the full table.

3. **dbt gold models run** — Six dbt models execute against the silver data now visible to Spark:

   | Model | Type | Grain |
   |---|---|---|
   | `dim_date` | Table | One row per hour with date attributes |
   | `dim_zone` | Table | One row per TLC zone (265 rows) |
   | `dim_vendor` | Table | One row per vendor |
   | `fact_trips` | Table | One row per trip |
   | `agg_hourly_metrics` | Table | One row per hour per zone |
   | `agg_zone_summary` | Table | One row per zone per month |

4. **Delta MERGE** — Each model is written to Delta Lake using `MERGE` on the model's primary key. Existing rows for the partition are updated; new rows are inserted.

5. **OPTIMIZE + VACUUM** — After each merge, the Delta table is optimised (bin-pack small files) and vacuumed (7-day retention). This keeps the Delta log compact.

6. **Run log written** — `RunLogger` writes one observability row to `pipeline_run_log` with timing, row counts, and quality flags.

### SparkSession configuration

| Config key | Value | Purpose |
|---|---|---|
| `spark.jars.packages` | `io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.7.3` | Delta + JDBC drivers |
| `spark.sql.extensions` | `io.delta.sql.DeltaSparkSessionExtension` | Delta SQL support |
| `spark.sql.catalog.spark_catalog` | `org.apache.spark.sql.delta.catalog.DeltaCatalog` | Delta catalog |
| `spark.driver.memory` | `SPARK_DRIVER_MEMORY` env (default 2g) | Driver heap |
| `spark.executor.memory` | `SPARK_EXECUTOR_MEMORY` env (default 2g) | Executor heap |

### Delta table layout

```
$DELTA_GOLD_BASE_PATH/
├── fact_trips/          (partitioned by trip_month)
├── dim_date/
├── dim_zone/
├── dim_vendor/
├── agg_hourly_metrics/  (partitioned by trip_month)
└── agg_zone_summary/    (partitioned by trip_month)
```

---

## End-to-end data lineage

```
TLC S3 parquet
    │  psycopg3 COPY
    ▼
raw_trips (Postgres, public schema)
    │  dbt view
    ▼
stg_trips (Postgres, view — filtered, cast)
    │  dbt incremental
    ▼
silver_trips (Postgres, public_silver schema)
    │  PySpark JDBC
    ▼
[Spark DataFrame]
    │  dbt + Delta MERGE
    ▼
fact_trips / dim_* / agg_* (Delta Lake)
```

Every arrow in this diagram has an Airflow task boundary. No step reads from a layer above it. No step skips a layer.

---

## Timing (single month, Apple Silicon, 8 GB Docker)

| Stage | Typical duration |
|---|---|
| ingest_trips | 3 – 5 min |
| validate_bronze | 1 – 2 min |
| run_silver_dbt | 2 – 4 min |
| test_silver_dbt | 1 – 2 min |
| load_gold (Spark startup + MERGE) | 8 – 12 min |
| **Total per month** | **15 – 25 min** |

For the 23-month backfill, sequential runs completed in approximately 8 hours with `max_active_runs=1`.

---

## Failure modes and recovery

| Failure point | Symptom | Recovery |
|---|---|---|
| TLC URL unavailable | `ingest_trips` fails on HTTP error | Re-trigger the DAG run for that month — ingest is idempotent |
| GX validation fails | `validate_bronze` raises `ValidationError` | Inspect HTML report in `great_expectations/reports/`, fix source data or expectation, re-run |
| dbt model fails | `run_silver_dbt` raises non-zero exit | Check dbt logs in Airflow task log, fix model, re-run from `run_silver_dbt` |
| Spark OOM | `load_gold` killed by JVM | Increase `SPARK_DRIVER_MEMORY` / `SPARK_EXECUTOR_MEMORY` in `.env`, restart stack |
| JDBC connection refused | `load_gold` fails on read | Verify `pipeline_db` is healthy (`docker compose ps`), re-run `load_gold` |

See [runbook.md](runbook.md) for detailed recovery procedures.
