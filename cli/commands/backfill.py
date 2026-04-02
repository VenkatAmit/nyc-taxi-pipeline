"""
cab pipeline backfill — trigger runs for a date range.

Usage
-----
    cab pipeline backfill --start 2024-01-01 --end 2024-03-01
    cab pipeline backfill --start 2024-01-01 --end 2024-03-01 --dag bronze_dag
    cab pipeline backfill --start 2024-01-01 --end 2024-03-01 --concurrency 2
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

import typer

from cli.airflow_client import AirflowClient
from cli.exceptions import OrchestratorError

backfill_app = typer.Typer(
    help="Trigger backfill runs for a date range", no_args_is_help=False
)

_ALL_DAGS = ["bronze_dag", "gold_dag"]
_TERMINAL_STATES = {"success", "failed", "upstream_failed"}


def _date_range(start: date, end: date) -> list[date]:
    """Return list of dates from start to end inclusive."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _to_iso(d: date) -> str:
    return datetime(d.year, d.month, d.day, tzinfo=UTC).isoformat()


def _poll_run(
    client: AirflowClient,
    dag_id: str,
    run_id: str,
    poll_interval: int = 15,
) -> str:
    while True:
        run = client.get_dag_run(dag_id, run_id)
        state = run.get("state", "unknown")
        if state in _TERMINAL_STATES:
            return str(state)  # ← add str() cast to satisfy type checker
        time.sleep(poll_interval)


@backfill_app.callback(invoke_without_command=True)
def backfill(
    ctx: typer.Context,
    start: Annotated[
        str,
        typer.Option("--start", "-s", help="Start date inclusive (YYYY-MM-DD)"),
    ],
    end: Annotated[
        str,
        typer.Option("--end", "-e", help="End date inclusive (YYYY-MM-DD)"),
    ],
    dag: Annotated[
        str | None,
        typer.Option(
            "--dag", "-d", help="DAG to backfill (default: bronze_dag then gold_dag)"
        ),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", "-c", help="Max parallel DAG runs"),
    ] = 1,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Print dates that would be triggered without triggering"
        ),
    ] = False,
) -> None:
    """Trigger DAG runs for every date in a range.

    Respects --concurrency to avoid flooding the Airflow scheduler.
    Each date is triggered and polled before the next batch starts.
    """
    if ctx.invoked_subcommand is not None:
        return

    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        typer.echo("Invalid date format — expected YYYY-MM-DD", err=True)
        raise typer.Exit(code=1) from None

    if start_date > end_date:
        typer.echo("--start must be before --end", err=True)
        raise typer.Exit(code=1)

    dates = _date_range(start_date, end_date)
    dag_ids = [dag] if dag else _ALL_DAGS

    typer.echo(
        f"Backfill: {len(dates)} dates x {len(dag_ids)} DAG(s) "
        f"[concurrency={concurrency}]"
    )

    if dry_run:
        typer.secho("Dry run — no runs triggered:", fg=typer.colors.YELLOW)
        for d in dates:
            for dag_id in dag_ids:
                typer.echo(f"  {dag_id}  {d}")
        return

    results: dict[str, dict[str, str]] = {}
    failed_count = 0

    try:
        with AirflowClient() as client:
            # Process dates in batches of concurrency size
            for i in range(0, len(dates), concurrency):
                batch = dates[i : i + concurrency]
                batch_runs: list[tuple[str, str, str]] = []

                for d in batch:
                    for dag_id in dag_ids:
                        iso_date = _to_iso(d)
                        try:
                            run = client.trigger_dag(dag_id, logical_date=iso_date)
                            run_id = run["dag_run_id"]
                            batch_runs.append((dag_id, run_id, str(d)))
                            typer.echo(f"  Triggered {dag_id} [{d}]: {run_id}")
                        except OrchestratorError as exc:
                            typer.secho(
                                f"  Failed to trigger {dag_id} [{d}]: {exc}",
                                fg=typer.colors.RED,
                                err=True,
                            )
                            failed_count += 1

                # Poll all runs in this batch to completion
                for dag_id, run_id, date_str in batch_runs:
                    state = _poll_run(client, dag_id, run_id)
                    results.setdefault(date_str, {})[dag_id] = state
                    color = (
                        typer.colors.GREEN if state == "success" else typer.colors.RED
                    )
                    typer.secho(
                        f"  {dag_id} [{date_str}] → {state}",
                        fg=color,
                    )
                    if state != "success":
                        failed_count += 1

    except OrchestratorError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    # Summary
    total = len(dates) * len(dag_ids)
    succeeded = total - failed_count
    typer.echo(f"\nBackfill complete: {succeeded}/{total} runs succeeded.")

    if failed_count:
        raise typer.Exit(code=1)
