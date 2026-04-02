"""
cab pipeline CLI — entry point.

Usage
-----
    cab pipeline run bronze
    cab pipeline run gold
    cab pipeline run all
    cab pipeline status
    cab pipeline backfill --start 2024-01-01 --end 2024-03-01
    cab pipeline logs --dag bronze_dag --run-id <run_id>

All commands talk to the Airflow REST API — no direct database access.
Credentials are read from environment variables (see AirflowSettings).
"""

from __future__ import annotations

import typer

from cli.commands.run import run_app
from cli.commands.status import status_app
from cli.commands.backfill import backfill_app
from cli.commands.logs import logs_app

app = typer.Typer(
    name="cab",
    help="cab-spark-data-pipeline operator CLI",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

pipeline = typer.Typer(
    name="pipeline",
    help="Trigger, monitor, and manage pipeline DAG runs",
    no_args_is_help=True,
)

app.add_typer(pipeline, name="pipeline")
pipeline.add_typer(run_app, name="run")
pipeline.add_typer(status_app, name="status")
pipeline.add_typer(backfill_app, name="backfill")
pipeline.add_typer(logs_app, name="logs")


@app.command("health")
def health() -> None:
    """Check Airflow API connectivity."""
    from cli.airflow_client import AirflowClient

    with AirflowClient() as client:
        result = client.health()
        meta = result.get("metadatabase", {}).get("status", "unknown")
        scheduler = result.get("scheduler", {}).get("status", "unknown")
        typer.echo(f"Metadatabase : {meta}")
        typer.echo(f"Scheduler    : {scheduler}")


if __name__ == "__main__":
    app()
