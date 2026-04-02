"""
cab pipeline status — show latest run state for all pipeline DAGs.

Usage
-----
    cab pipeline status
    cab pipeline status --dag bronze_dag
    cab pipeline status --limit 5
"""

from __future__ import annotations

from typing import Annotated

import typer

from cli.airflow_client import AirflowClient
from pipeline.exceptions import OrchestratorError

status_app = typer.Typer(help="Show pipeline DAG run status", no_args_is_help=False)

_ALL_DAGS = ["bronze_dag", "gold_dag"]

_STATE_COLORS = {
    "success": typer.colors.GREEN,
    "failed": typer.colors.RED,
    "upstream_failed": typer.colors.RED,
    "running": typer.colors.YELLOW,
    "queued": typer.colors.CYAN,
}


def _state_colored(state: str) -> str:
    color = _STATE_COLORS.get(state, typer.colors.WHITE)
    return str(typer.style(f"{state:<16}", fg=color))


@status_app.callback(invoke_without_command=True)
def status(
    ctx: typer.Context,
    dag: Annotated[
        str | None,
        typer.Option("--dag", "-d", help="Filter to a specific DAG ID"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Number of recent runs to show per DAG"),
    ] = 3,
) -> None:
    """Show latest run status for all pipeline DAGs."""
    if ctx.invoked_subcommand is not None:
        return

    dag_ids = [dag] if dag else _ALL_DAGS

    try:
        with AirflowClient() as client:
            for dag_id in dag_ids:
                runs = client.list_dag_runs(dag_id, limit=limit)
                typer.secho(f"\n{dag_id}", bold=True)
                typer.echo("─" * 72)

                if not runs:
                    typer.echo("  No runs found.")
                    continue

                typer.echo(f"  {'RUN ID':<45} {'STATE':<16} {'STARTED':<20}")
                typer.echo("  " + "─" * 70)

                for run in runs:
                    run_id = run.get("dag_run_id", "")[:44]
                    state = run.get("state", "unknown")
                    started = (run.get("start_date") or "")[:19].replace("T", " ")

                    typer.echo(f"  {run_id:<45} {_state_colored(state)} {started:<20}")

    except OrchestratorError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None
