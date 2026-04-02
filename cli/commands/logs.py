"""
cab pipeline logs — fetch or stream task logs.

Usage
-----
    cab pipeline logs --dag bronze_dag --run-id <run_id>
    cab pipeline logs --dag bronze_dag --run-id <run_id> --task ingest_trips
    cab pipeline logs --dag bronze_dag --run-id <run_id> --task ingest_trips --try 2
"""

from __future__ import annotations

from typing import Annotated

import typer
from cli.airflow_client import AirflowClient
from cli.exceptions import OrchestratorError

logs_app = typer.Typer(help="Fetch task logs from Airflow", no_args_is_help=False)


@logs_app.callback(invoke_without_command=True)
def logs(
    ctx: typer.Context,
    dag: Annotated[
        str,
        typer.Option("--dag", "-d", help="DAG ID"),
    ],
    run_id: Annotated[
        str,
        typer.Option("--run-id", "-r", help="DAG run ID"),
    ],
    task: Annotated[
        str | None,
        typer.Option("--task", "-t", help="Task ID (omit to list all task instances)"),
    ] = None,
    try_number: Annotated[
        int,
        typer.Option("--try", help="Attempt number (default: 1)"),
    ] = 1,
    stream: Annotated[
        bool,
        typer.Option("--stream", "-s", help="Stream log lines as they arrive"),
    ] = False,
) -> None:
    """Fetch or stream logs for a DAG run's task instances.

    If --task is omitted, lists all task instances and their states
    for the given run so you can identify which task to inspect.
    """
    if ctx.invoked_subcommand is not None:
        return

    try:
        with AirflowClient() as client:
            if task is None:
                # No task specified — list task instances for the run
                instances = client.list_task_instances(dag, run_id)
                if not instances:
                    typer.echo(f"No task instances found for {dag}/{run_id}")
                    return

                typer.secho(f"\nTask instances for {dag} [{run_id}]", bold=True)
                typer.echo("─" * 60)
                typer.echo(f"  {'TASK ID':<35} {'STATE':<16} {'TRY'}")
                typer.echo("  " + "─" * 58)

                for ti in instances:
                    task_id = ti.get("task_id", "")[:34]
                    state = ti.get("state") or "none"
                    try_num = ti.get("try_number", 1)
                    color = {
                        "success": typer.colors.GREEN,
                        "failed": typer.colors.RED,
                        "running": typer.colors.YELLOW,
                    }.get(state, typer.colors.WHITE)
                    typer.echo(
                        f"  {task_id:<35} "
                        f"{typer.style(f'{state:<16}', fg=color)} "
                        f"{try_num}"
                    )

                typer.echo(
                    "\nRe-run with --task <task_id> to fetch logs for a specific task."
                )
                return

            # Task specified — fetch or stream logs
            if stream:
                typer.secho(
                    f"Streaming logs: {dag}/{run_id}/{task} (try {try_number})",
                    bold=True,
                )
                typer.echo("─" * 60)
                for line in client.stream_task_log(dag, run_id, task, try_number):
                    typer.echo(line)
            else:
                log_content = client.get_task_log(dag, run_id, task, try_number)
                typer.secho(
                    f"Logs: {dag}/{run_id}/{task} (try {try_number})",
                    bold=True,
                )
                typer.echo("─" * 60)
                typer.echo(log_content)

    except OrchestratorError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None
