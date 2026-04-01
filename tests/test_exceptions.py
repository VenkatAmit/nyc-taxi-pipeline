"""Tests for pipeline/exceptions.py."""

from __future__ import annotations

import pytest

from pipeline.exceptions import (
    ConfigurationError,
    DbtError,
    DeltaError,
    ExpectationFailedError,
    IngestError,
    OrchestratorError,
    PipelineError,
    PostgresError,
    SchemaValidationError,
    SourceNotFoundError,
    SparkError,
    StorageError,
    TransformError,
    ValidationError,
)


class TestHierarchy:
    """All exceptions must inherit from PipelineError."""

    def test_configuration_error(self) -> None:
        assert issubclass(ConfigurationError, PipelineError)

    def test_ingest_error(self) -> None:
        assert issubclass(IngestError, PipelineError)

    def test_source_not_found_is_ingest(self) -> None:
        assert issubclass(SourceNotFoundError, IngestError)

    def test_schema_validation_is_ingest(self) -> None:
        assert issubclass(SchemaValidationError, IngestError)

    def test_validation_error(self) -> None:
        assert issubclass(ValidationError, PipelineError)

    def test_expectation_failed_is_validation(self) -> None:
        assert issubclass(ExpectationFailedError, ValidationError)

    def test_storage_error(self) -> None:
        assert issubclass(StorageError, PipelineError)

    def test_postgres_error_is_storage(self) -> None:
        assert issubclass(PostgresError, StorageError)

    def test_delta_error_is_storage(self) -> None:
        assert issubclass(DeltaError, StorageError)

    def test_transform_error(self) -> None:
        assert issubclass(TransformError, PipelineError)

    def test_dbt_error_is_transform(self) -> None:
        assert issubclass(DbtError, TransformError)

    def test_spark_error_is_transform(self) -> None:
        assert issubclass(SparkError, TransformError)

    def test_orchestrator_error(self) -> None:
        assert issubclass(OrchestratorError, PipelineError)


class TestPipelineError:
    def test_message(self) -> None:
        e = PipelineError("something went wrong")
        assert str(e) == "something went wrong"

    def test_cause_in_str(self) -> None:
        cause = ValueError("root cause")
        e = PipelineError("outer", cause=cause)
        assert "root cause" in str(e)
        assert "ValueError" in str(e)

    def test_cause_stored(self) -> None:
        cause = RuntimeError("boom")
        e = PipelineError("msg", cause=cause)
        assert e.cause is cause

    def test_catchable_as_exception(self) -> None:
        with pytest.raises(Exception):
            raise PipelineError("test")


class TestSourceNotFoundError:
    def test_message_contains_source(self) -> None:
        e = SourceNotFoundError("s3://bucket/missing.parquet")
        assert "s3://bucket/missing.parquet" in str(e)

    def test_source_attribute(self) -> None:
        e = SourceNotFoundError("s3://bucket/missing.parquet")
        assert e.source == "s3://bucket/missing.parquet"


class TestSchemaValidationError:
    def test_message_contains_table_and_columns(self) -> None:
        e = SchemaValidationError("raw_trips", ["trip_distance", "fare_amount"])
        assert "raw_trips" in str(e)
        assert "trip_distance" in str(e)
        assert "fare_amount" in str(e)

    def test_attributes(self) -> None:
        e = SchemaValidationError("raw_trips", ["col_a"])
        assert e.table == "raw_trips"
        assert e.missing_columns == ["col_a"]


class TestExpectationFailedError:
    def test_message(self) -> None:
        e = ExpectationFailedError("expect_column_values_to_not_be_null", 42)
        assert "expect_column_values_to_not_be_null" in str(e)
        assert "42" in str(e)

    def test_attributes(self) -> None:
        e = ExpectationFailedError("my_expectation", 7)
        assert e.expectation_name == "my_expectation"
        assert e.failure_count == 7


class TestPostgresError:
    def test_message(self) -> None:
        e = PostgresError("COPY", "raw_trips")
        assert "COPY" in str(e)
        assert "raw_trips" in str(e)

    def test_attributes(self) -> None:
        e = PostgresError("INSERT", "raw_zones")
        assert e.operation == "INSERT"
        assert e.table == "raw_zones"


class TestDeltaError:
    def test_message(self) -> None:
        e = DeltaError("MERGE", "s3://bucket/gold/fact_trips")
        assert "MERGE" in str(e)
        assert "s3://bucket/gold/fact_trips" in str(e)

    def test_attributes(self) -> None:
        e = DeltaError("WRITE", "s3://bucket/gold/dim_zones")
        assert e.operation == "WRITE"
        assert e.path == "s3://bucket/gold/dim_zones"


class TestDbtError:
    def test_message_contains_exit_code(self) -> None:
        e = DbtError("dbt run --select silver.*", 1)
        assert "1" in str(e)
        assert "dbt run" in str(e)

    def test_attributes(self) -> None:
        e = DbtError("dbt test", 2)
        assert e.command == "dbt test"
        assert e.exit_code == 2


class TestSparkError:
    def test_message(self) -> None:
        e = SparkError("gold_load_job")
        assert "gold_load_job" in str(e)

    def test_attribute(self) -> None:
        e = SparkError("my_job")
        assert e.job == "my_job"


class TestOrchestratorError:
    def test_message_with_status_code(self) -> None:
        e = OrchestratorError("trigger_dag", 404)
        assert "trigger_dag" in str(e)
        assert "404" in str(e)

    def test_message_without_status_code(self) -> None:
        e = OrchestratorError("trigger_dag")
        assert "trigger_dag" in str(e)

    def test_attributes(self) -> None:
        e = OrchestratorError("list_dags", 500)
        assert e.operation == "list_dags"
        assert e.status_code == 500
