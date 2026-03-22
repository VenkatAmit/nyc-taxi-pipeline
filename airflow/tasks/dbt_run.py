"""
airflow/tasks/dbt_run.py
------------------------
Gold layer: runs dbt models and tests against cleaned_trips.

Runs inside the Airflow container using the docker profile,
which points to pipeline_db:5432 on the internal Docker network.

XCom output:
    dbt_duration_sec (float)
    dbt_models_passed (int)
    dbt_tests_passed (int)
"""

import os
import time
import logging
import subprocess

log = logging.getLogger(__name__)

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "docker"


def _run_dbt_command(command: list[str]) -> tuple[int, str, str]:
    """Run a dbt command, return (returncode, stdout, stderr)."""
    env = os.environ.copy()
    result = subprocess.run(
        command,
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def dbt_run(**context):
    """
    Runs dbt run + dbt test sequentially.
    Fails the task if either step has non-zero exit code.
    Pushes duration and pass counts to XCom for pipeline_run_log.
    """
    start = time.time()
    ti = context["ti"]

    # ── dbt run ───────────────────────────────────────────────
    log.info("Running dbt run...")
    rc, stdout, stderr = _run_dbt_command(
        [
            "dbt",
            "run",
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--target",
            DBT_TARGET,
        ]
    )
    log.info(f"dbt run stdout:\n{stdout}")
    if stderr:
        log.warning(f"dbt run stderr:\n{stderr}")
    if rc != 0:
        raise RuntimeError(f"dbt run failed with exit code {rc}:\n{stderr}")

    # Parse pass count from output — "Completed successfully" line
    models_passed = stdout.count("OK created")
    log.info(f"dbt run complete — {models_passed} models passed")

    # ── dbt test ──────────────────────────────────────────────
    log.info("Running dbt test...")
    rc, stdout, stderr = _run_dbt_command(
        [
            "dbt",
            "test",
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--target",
            DBT_TARGET,
        ]
    )
    log.info(f"dbt test stdout:\n{stdout}")
    if stderr:
        log.warning(f"dbt test stderr:\n{stderr}")
    if rc != 0:
        raise RuntimeError(f"dbt test failed with exit code {rc}:\n{stderr}")

    tests_passed = stdout.count("PASS")
    log.info(f"dbt test complete — {tests_passed} tests passed")

    duration = round(time.time() - start, 2)
    log.info(f"dbt_run task complete | duration={duration}s")

    ti.xcom_push(key="dbt_duration_sec", value=duration)
    ti.xcom_push(key="dbt_models_passed", value=models_passed)
    ti.xcom_push(key="dbt_tests_passed", value=tests_passed)

    return tests_passed
