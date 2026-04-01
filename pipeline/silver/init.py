"""
Silver layer — dbt transformations on Postgres.

Responsibilities:
- Clean and conform raw bronze data
- Incremental dbt models over raw_trips and raw_zones
- Runs via BashOperator in Airflow: `dbt run --select silver.*`

No Python runtime classes live here — dbt owns this layer entirely.
See dbt/models/silver/ for model definitions.
"""
