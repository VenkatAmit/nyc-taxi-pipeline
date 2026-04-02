"""
Exception hierarchy for the cab-spark-data-pipeline.

All pipeline exceptions inherit from PipelineError so callers can
catch broadly or narrowly depending on context.

Hierarchy
---------
PipelineError
├── ConfigurationError        — bad or missing settings
├── IngestError               — bronze ingestion failures
│   ├── SourceNotFoundError   — source file / API unavailable
│   └── SchemaValidationError — incoming data doesn't match expected schema
├── ValidationError           — Great Expectations failures
│   └── ExpectationFailedError— one or more GX expectations not met
├── StorageError              — read/write failures
│   ├── PostgresError         — psycopg3 / Postgres-specific failures
│   └── DeltaError            — Delta Lake read/write failures
├── TransformError            — dbt or Spark transformation failures
│   ├── DbtError              — dbt run / test failures
│   └── SparkError            — PySpark job failures
└── OrchestratorError         — Airflow / CLI interaction failures
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class PipelineError(Exception):
    """Base class for all pipeline exceptions.

    All exceptions raised within pipeline/ should inherit from this
    so that Airflow tasks can catch PipelineError and handle retries
    or alerting uniformly.
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause

    def __str__(self) -> str:
        base = super().__str__()
        if self.cause:
            return f"{base} — caused by: {type(self.cause).__name__}: {self.cause}"
        return base


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class ConfigurationError(PipelineError):
    """Raised when required settings are missing or invalid.

    Example: POSTGRES_PASSWORD not set in environment.
    """


# ---------------------------------------------------------------------------
# Ingestion (bronze layer)
# ---------------------------------------------------------------------------


class IngestError(PipelineError):
    """Base class for bronze ingestion failures."""


class SourceNotFoundError(IngestError):
    """Raised when the source file or API endpoint is unreachable.

    Example: NYC taxi S3 bucket path does not exist for the given date.
    """

    def __init__(self, source: str, *, cause: BaseException | None = None) -> None:
        super().__init__(f"Source not found: {source!r}", cause=cause)
        self.source = source


class SchemaValidationError(IngestError):
    """Raised when incoming data does not match the expected schema.

    Example: a required column is missing from the source Parquet file.
    """

    def __init__(
        self,
        table: str,
        missing_columns: list[str],
        *,
        cause: BaseException | None = None,
    ) -> None:
        cols = ", ".join(missing_columns)
        super().__init__(
            f"Schema mismatch for {table!r}: missing columns [{cols}]",
            cause=cause,
        )
        self.table = table
        self.missing_columns = missing_columns


# ---------------------------------------------------------------------------
# Validation (Great Expectations)
# ---------------------------------------------------------------------------


class ValidationError(PipelineError):
    """Base class for Great Expectations validation failures."""


class ExpectationFailedError(ValidationError):
    """Raised when one or more GX expectations are not met.

    Example: expect_column_values_to_not_be_null failed on trip_distance.
    """

    def __init__(
        self,
        expectation_name: str,
        failure_count: int,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(
            f"Expectation {expectation_name!r} failed on {failure_count} rows",
            cause=cause,
        )
        self.expectation_name = expectation_name
        self.failure_count = failure_count


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


class StorageError(PipelineError):
    """Base class for read/write failures."""


class PostgresError(StorageError):
    """Raised on psycopg3 or Postgres-level failures.

    Example: COPY command fails due to a constraint violation.
    """

    def __init__(
        self,
        operation: str,
        table: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(
            f"Postgres {operation} failed on table {table!r}",
            cause=cause,
        )
        self.operation = operation
        self.table = table


class DeltaError(StorageError):
    """Raised on Delta Lake read/write failures.

    Example: MERGE INTO fails due to a schema mismatch on the gold table.
    """

    def __init__(
        self,
        operation: str,
        path: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(
            f"Delta {operation} failed at path {path!r}",
            cause=cause,
        )
        self.operation = operation
        self.path = path


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


class TransformError(PipelineError):
    """Base class for transformation failures."""


class DbtError(TransformError):
    """Raised when a dbt run or test command exits non-zero.

    Example: dbt run --select silver.* fails on a model.
    """

    def __init__(
        self,
        command: str,
        exit_code: int,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(
            f"dbt command failed (exit {exit_code}): {command!r}",
            cause=cause,
        )
        self.command = command
        self.exit_code = exit_code


class SparkError(TransformError):
    """Raised when a PySpark job fails.

    Example: JDBC read times out, or Delta MERGE raises an AnalysisException.
    """

    def __init__(
        self,
        job: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(f"Spark job failed: {job!r}", cause=cause)
        self.job = job


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


class OrchestratorError(PipelineError):
    """Raised on Airflow REST API or CLI interaction failures.

    Example: DAG trigger returns a non-2xx response.
    """

    def __init__(
        self,
        operation: str,
        status_code: int | None = None,
        *,
        cause: BaseException | None = None,
    ) -> None:
        detail = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(
            f"Orchestrator operation failed: {operation!r}{detail}",
            cause=cause,
        )
        self.operation = operation
        self.status_code = status_code
