# cab pipeline CLI

Thin operator interface over the Airflow REST API.
No direct database access — all commands talk to Airflow over HTTP.

## Setup

```bash
# Install CLI dependencies
uv sync --extra cli

# Configure credentials via environment variables (or .env file)
export AIRFLOW_API_URL=http://localhost:8080
export AIRFLOW_USERNAME=admin
export AIRFLOW_PASSWORD=admin
```

## Commands

### Check connectivity
```bash
cab health
```

### Trigger runs
```bash
# Trigger bronze ingestion for today
cab pipeline run bronze

# Trigger bronze for a specific date
cab pipeline run bronze --date 2024-01-01

# Trigger bronze and wait for it to finish
cab pipeline run bronze --date 2024-01-01 --wait

# Trigger gold (checks bronze succeeded first)
cab pipeline run gold --date 2024-01-01

# Trigger bronze then gold in sequence (waits between)
cab pipeline run all --date 2024-01-01
```

### Check status
```bash
# Show latest runs for all pipeline DAGs
cab pipeline status

# Show last 5 runs for a specific DAG
cab pipeline status --dag bronze_dag --limit 5
```

### Backfill a date range
```bash
# Backfill all DAGs for Jan–Mar 2024
cab pipeline backfill --start 2024-01-01 --end 2024-03-31

# Dry run first to see what would be triggered
cab pipeline backfill --start 2024-01-01 --end 2024-03-31 --dry-run

# Backfill with 2 parallel runs
cab pipeline backfill --start 2024-01-01 --end 2024-03-31 --concurrency 2

# Backfill a single DAG only
cab pipeline backfill --start 2024-01-01 --end 2024-03-31 --dag bronze_dag
```

### Fetch logs
```bash
# List task instances for a run (to find the task ID)
cab pipeline logs --dag bronze_dag --run-id <run_id>

# Fetch logs for a specific task
cab pipeline logs --dag bronze_dag --run-id <run_id> --task ingest_trips

# Stream logs live
cab pipeline logs --dag bronze_dag --run-id <run_id> --task ingest_trips --stream

# Fetch logs for a retry attempt
cab pipeline logs --dag bronze_dag --run-id <run_id> --task ingest_trips --try 2
```

## Environment variables

| Variable | Description | Required |
|---|---|---|
| `AIRFLOW_API_URL` | Airflow webserver base URL | Yes |
| `AIRFLOW_USERNAME` | Basic auth username | Yes |
| `AIRFLOW_PASSWORD` | Basic auth password | Yes |

All variables can be set in a `.env` file at the repo root.
