# NYC Taxi Pipeline

Production-grade batch data pipeline processing 100M+ NYC taxi trips.

**Stack:** Apache Airflow · PySpark · dbt · Great Expectations · Delta Lake · PostgreSQL · Docker

## Architecture
```
NYC Taxi Parquet (TLC public data)
        |
        v
Airflow DAG (@daily, one month per run)
  ingest        — download parquet files to local storage
  spark_transform — PySpark: clean, partition, enrich (100M+ rows)
  dbt run       — SQL models: silver cleaning, gold aggregations
  dbt test      — schema tests, not_null, accepted_values, range checks
  ge_validate   — Great Expectations HTML validation report
  load          — pipeline_run_log, metrics, Delta Lake commit
        |
        v
PostgreSQL + Delta Lake (medallion schema)
  raw_trips        bronze
  cleaned_trips    silver  (dbt)
  trip_metrics     gold    (dbt)
  zone_summary     gold    (dbt)
  pipeline_run_log observability
```

## Setup

Run `docker compose up -d` — the DAG auto-backfills Jan 2024 → Nov 2025 (23 runs, sequential, no manual triggers needed).

## Branches

| Branch | Status | What it adds |
|--------|--------|-------------|
| feature/core-pipeline | in progress | Airflow + Postgres foundation |
| feature/spark-transform | planned | PySpark transform layer |
| feature/dbt-models | planned | SQL models + data tests |
| feature/great-expectations | planned | HTML data quality reports |
| feature/delta-lake | merged | Delta bronze, star schema gold, catchup backfill |
