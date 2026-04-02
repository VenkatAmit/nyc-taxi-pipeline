"""
cab pipeline run — trigger DAG runs.

Commands
--------
    cab pipeline run bronze [--date 2024-01-01]
    cab pipeline run gold   [--date 2024-01-01]
    cab pipeline run all    [--date 2024-01-01] [--wait]
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Annotated

import typer
from pipeline.exceptions import OrchestratorError

from cli.airflow_client import AirflowClient

run_app = typer.Typer(help="Trigger pipeline DAG runs", no_args_is_help=True)

_BRONZE_DAG = "bronze_dag"
_GOLD_DAG = "gold_dag"
_TERMINAL_STATES = {"success", "failed", "upstream_failed"}


def _format_date(date_str: str | None) -> str | None:
    """Normalise a YYYY-MM-DD date string to ISO-8601 with UTC timezone."""
    if date_str is None:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        return dt.isoformat()
    except ValueError:
        typer.echo(f"Invalid date format: {date_str!r} — expected YYYY-MM-DD", err=True)
        raise typer.Exit(code=1)


def _poll_until_done(
    client: AirflowClient,
    dag_id: str,
    run_id: str,
    poll_interval: int = 10,
) -> str:
    """Poll a DAG run until it reaches a terminal state. Returns final state."""
    typer.echo(f"  Waiting for {dag_id} [{run_id}]...")
    while True:
        run = client.get_dag_run(dag_id, run_id)
        state = run.get("state", "unknown")
        if state in _TERMINAL_STATES:
            return state
        typer.echo(f"  [{dag_id}] state: {state} — checking again in {poll_interval}s")
        time.sleep(poll_interval)


@run_app.command("bronze")
def run_bronze(
    date: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Logical date override (YYYY-MM-DD)"),
    ] = None,
    wait: Annotated[
        bool,
        typer.Option("--wait", "-w", help="Wait for the run to complete"),
    ] = False,
) -> None:
    """Trigger the bronze ingestion DAG."""
    logical_date = _format_date(date)
    try:
        with AirflowClient() as client:
            run = client.trigger_dag(_BRONZE_DAG, logical_date=logical_date)
            run_id = run["dag_run_id"]
            typer.echo(f"Triggered {_BRONZE_DAG}: {run_id}")

            if wait:
                state = _poll_until_done(client, _BRONZE_DAG, run_id)
                color = typer.colors.GREEN if state == "success" else typer.colors.RED
                typer.secho(f"  Final state: {state}", fg=color)
                if state != "success":
                    raise typer.Exit(code=1)
    except OrchestratorError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)


@run_app.command("gold")
def run_gold(
    date: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Logical date override (YYYY-MM-DD)"),
    ] = None,
    wait: Annotated[
        bool,
        typer.Option("--wait", "-w", help="Wait for the run to complete"),
    ] = False,
    skip_bronze_check: Annotated[
        bool,
        typer.Option(
            "--skip-bronze-check", help="Skip checking bronze succeeded first"
        ),
    ] = False,
) -> None:
    """Trigger the gold loader DAG.

    By default checks that bronze succeeded for the same date first.
    Use --skip-bronze-check to override (e.g. for manual backfills).
    """
    logical_date = _format_date(date)
    try:
        with AirflowClient() as client:
            if not skip_bronze_check:
                runs = client.list_dag_runs(_BRONZE_DAG, limit=1, state="success")
                if not runs:
                    typer.echo(
                        "Warning: no successful bronze run found. "
                        "Use --skip-bronze-check to proceed anyway.",
                        err=True,
                    )
                    raise typer.Exit(code=1)

            run = client.trigger_dag(_GOLD_DAG, logical_date=logical_date)
            run_id = run["dag_run_id"]
            typer.echo(f"Triggered {_GOLD_DAG}: {run_id}")

            if wait:
                state = _poll_until_done(client, _GOLD_DAG, run_id)
                color = typer.colors.GREEN if state == "success" else typer.colors.RED
                typer.secho(f"  Final state: {state}", fg=color)
                if state != "success":
                    raise typer.Exit(code=1)
    except OrchestratorError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)


@run_app.command("all")
def run_all(
    date: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Logical date override (YYYY-MM-DD)"),
    ] = None,
) -> None:
    """Trigger bronze then gold in sequence.

    Waits for bronze to succeed before triggering gold.
    Exits non-zero if either DAG fails.
    """
    logical_date = _format_date(date)
    try:
        with AirflowClient() as client:
            # Bronze first
            bronze_run = client.trigger_dag(_BRONZE_DAG, logical_date=logical_date)
            bronze_run_id = bronze_run["dag_run_id"]
            typer.echo(f"Triggered {_BRONZE_DAG}: {bronze_run_id}")

            bronze_state = _poll_until_done(client, _BRONZE_DAG, bronze_run_id)
            if bronze_state != "success":
                typer.secho(
                    f"Bronze failed ({bronze_state}) — aborting.", fg=typer.colors.RED
                )
                raise typer.Exit(code=1)
            typer.secho("Bronze succeeded.", fg=typer.colors.GREEN)

            # Gold after bronze succeeds
            gold_run = client.trigger_dag(_GOLD_DAG, logical_date=logical_date)
            gold_run_id = gold_run["dag_run_id"]
            typer.echo(f"Triggered {_GOLD_DAG}: {gold_run_id}")

            gold_state = _poll_until_done(client, _GOLD_DAG, gold_run_id)
            color = typer.colors.GREEN if gold_state == "success" else typer.colors.RED
            typer.secho(f"Gold final state: {gold_state}", fg=color)

            if gold_state != "success":
                raise typer.Exit(code=1)

    except OrchestratorError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)
