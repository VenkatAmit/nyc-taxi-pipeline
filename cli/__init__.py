"""
CLI command modules.

Each module exposes a Typer sub-app registered in cli/main.py:

    run.py      — cab pipeline run bronze/gold/all
    status.py   — cab pipeline status
    backfill.py — cab pipeline backfill --start --end
    logs.py     — cab pipeline logs --dag --run-id
"""
