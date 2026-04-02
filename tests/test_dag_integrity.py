"""
DAG integrity tests.

Validates that DAG files import cleanly, contain the expected tasks,
have no cycles, and follow the thin-task contract (no business logic
in DAG files).

These tests run in CI without a real Airflow database — they use
DagBag which parses DAG files without executing them.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DAGS_DIR = Path(__file__).parent.parent / "dags"

# Modules that are permitted to be imported inside DAG files.
# Service classes from pipeline/ are ALLOWED — DAGs must import them
# to delegate task logic. Raw business logic written inline is NOT allowed.
_ALLOWED_PIPELINE_IMPORTS = {
    "pipeline.bronze.ingestor",
    "pipeline.bronze.validator",
    "pipeline.gold.loader",
    "pipeline.gold.run_logger",
    "pipeline.settings",
}

# Airflow imports always allowed in DAGs
_ALLOWED_AIRFLOW_IMPORTS = {
    "airflow",
    "airflow.decorators",
    "airflow.models",
    "airflow.operators.bash",
    "airflow.sensors.external_task",
}


def _get_dagbag() -> Any:
    """Return an Airflow DagBag for the dags/ directory."""
    try:
        from airflow.models import DagBag
        return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    except ImportError:
        pytest.skip("airflow not installed in test environment")


# ---------------------------------------------------------------------------
# DAG import tests
# ---------------------------------------------------------------------------


class TestDagImports:
    def test_dagbag_has_no_import_errors(self) -> None:
        """All DAG files must import without errors."""
        dagbag = _get_dagbag()
        assert dagbag.import_errors == {}, (
            f"DAG import errors: {dagbag.import_errors}"
        )

    def test_bronze_dag_present(self) -> None:
        dagbag = _get_dagbag()
        assert "bronze_dag" in dagbag.dags, "bronze_dag not found in DagBag"

    def test_gold_dag_present(self) -> None:
        dagbag = _get_dagbag()
        assert "gold_dag" in dagbag.dags, "gold_dag not found in DagBag"


# ---------------------------------------------------------------------------
# bronze_dag structure
# ---------------------------------------------------------------------------


class TestBronzeDagStructure:
    @pytest.fixture()
    def bronze_dag(self) -> Any:
        dagbag = _get_dagbag()
        return dagbag.dags["bronze_dag"]

    def test_expected_tasks_present(self, bronze_dag: Any) -> None:
        task_ids = {t.task_id for t in bronze_dag.tasks}
        expected = {
            "ingest_trips",
            "ingest_zones",
            "validate_trips",
            "validate_zones",
            "run_silver_dbt",
            "test_silver_dbt",
        }
        assert expected.issubset(task_ids), (
            f"Missing tasks: {expected - task_ids}"
        )

    def test_no_cycles(self, bronze_dag: Any) -> None:
        """DagBag construction would fail on cycles, but be explicit."""
        assert bronze_dag.topological_sort() is not None

    def test_max_active_runs_is_one(self, bronze_dag: Any) -> None:
        assert bronze_dag.max_active_runs == 1

    def test_catchup_disabled(self, bronze_dag: Any) -> None:
        assert bronze_dag.catchup is False

    def test_schedule_is_daily(self, bronze_dag: Any) -> None:
        assert bronze_dag.schedule_interval == "0 6 * * *"

    def test_validate_tasks_downstream_of_ingest(self, bronze_dag: Any) -> None:
        ingest_trips = bronze_dag.get_task("ingest_trips")
        downstream_ids = {t.task_id for t in ingest_trips.downstream_list}
        assert "validate_trips" in downstream_ids

    def test_silver_downstream_of_validation(self, bronze_dag: Any) -> None:
        run_silver = bronze_dag.get_task("run_silver_dbt")
        upstream_ids = {t.task_id for t in run_silver.upstream_list}
        assert "validate_trips" in upstream_ids
        assert "validate_zones" in upstream_ids

    def test_test_silver_downstream_of_run_silver(self, bronze_dag: Any) -> None:
        test_silver = bronze_dag.get_task("test_silver_dbt")
        upstream_ids = {t.task_id for t in test_silver.upstream_list}
        assert "run_silver_dbt" in upstream_ids

    def test_retries_configured(self, bronze_dag: Any) -> None:
        ingest_trips = bronze_dag.get_task("ingest_trips")
        assert ingest_trips.retries >= 1


# ---------------------------------------------------------------------------
# gold_dag structure
# ---------------------------------------------------------------------------


class TestGoldDagStructure:
    @pytest.fixture()
    def gold_dag(self) -> Any:
        dagbag = _get_dagbag()
        return dagbag.dags["gold_dag"]

    def test_expected_tasks_present(self, gold_dag: Any) -> None:
        task_ids = {t.task_id for t in gold_dag.tasks}
        expected = {
            "wait_for_bronze",
            "load_fact_trips",
            "load_dim_zones",
        }
        assert expected.issubset(task_ids), (
            f"Missing tasks: {expected - task_ids}"
        )

    def test_no_cycles(self, gold_dag: Any) -> None:
        assert gold_dag.topological_sort() is not None

    def test_max_active_runs_is_one(self, gold_dag: Any) -> None:
        assert gold_dag.max_active_runs == 1

    def test_catchup_disabled(self, gold_dag: Any) -> None:
        assert gold_dag.catchup is False

    def test_schedule_is_daily(self, gold_dag: Any) -> None:
        assert gold_dag.schedule_interval == "0 8 * * *"

    def test_wait_for_bronze_is_sensor(self, gold_dag: Any) -> None:
        from airflow.sensors.external_task import ExternalTaskSensor

        wait = gold_dag.get_task("wait_for_bronze")
        assert isinstance(wait, ExternalTaskSensor)

    def test_load_tasks_downstream_of_sensor(self, gold_dag: Any) -> None:
        wait = gold_dag.get_task("wait_for_bronze")
        downstream_ids = {t.task_id for t in wait.downstream_list}
        assert "load_fact_trips" in downstream_ids
        assert "load_dim_zones" in downstream_ids

    def test_gold_runs_after_bronze_schedule(self, gold_dag: Any) -> None:
        """Gold DAG scheduled 2h after bronze (08:00 vs 06:00)."""
        assert gold_dag.schedule_interval == "0 8 * * *"


# ---------------------------------------------------------------------------
# Thin task contract — static analysis
# ---------------------------------------------------------------------------


class TestThinTaskContract:
    """
    Static AST check: DAG files must not define functions longer than
    a threshold line count inside @task decorators. Long task bodies
    indicate business logic that should live in Service classes.
    """

    MAX_TASK_BODY_LINES = 60

    @pytest.mark.parametrize("dag_file", ["bronze_dag.py", "gold_dag.py"])
    def test_task_bodies_are_thin(self, dag_file: str) -> None:
        source = (DAGS_DIR / dag_file).read_text()
        tree = ast.parse(source)

        violations: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            # Check if decorated with @task
            decorators = [
                ast.unparse(d) for d in node.decorator_list
                if isinstance(d, ast.Name) and d.id == "task"
                or isinstance(d, ast.Attribute) and d.attr == "task"
            ]
            if not decorators:
                continue

            # Count non-blank, non-comment lines in the function body
            func_source = ast.get_source_segment(source, node) or ""
            body_lines = [
                l for l in func_source.splitlines()
                if l.strip() and not l.strip().startswith("#")
            ]
            if len(body_lines) > self.MAX_TASK_BODY_LINES:
                violations.append(
                    f"{dag_file}::{node.name} has {len(body_lines)} lines "
                    f"(max {self.MAX_TASK_BODY_LINES})"
                )

        assert not violations, (
            "Task bodies too long — move logic to Service classes:\n"
            + "\n".join(violations)
        )

    @pytest.mark.parametrize("dag_file", ["bronze_dag.py", "gold_dag.py"])
    def test_no_direct_psycopg_imports(self, dag_file: str) -> None:
        """DAG files must never import psycopg directly."""
        source = (DAGS_DIR / dag_file).read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("psycopg"), (
                        f"{dag_file} imports psycopg directly — "
                        "use BronzeIngestor instead"
                    )
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("psycopg"), (
                    f"{dag_file} imports from psycopg directly"
                )

    @pytest.mark.parametrize("dag_file", ["bronze_dag.py", "gold_dag.py"])
    def test_no_direct_pyspark_imports(self, dag_file: str) -> None:
        """DAG files must never import pyspark directly."""
        source = (DAGS_DIR / dag_file).read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("pyspark"), (
                        f"{dag_file} imports pyspark directly — "
                        "use GoldLoader instead"
                    )
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("pyspark"), (
                    f"{dag_file} imports from pyspark directly"
                )
