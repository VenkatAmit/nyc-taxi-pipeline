"""
Shared pytest fixtures for the cab-spark-data-pipeline test suite.

Fixtures here are available to all test modules without explicit import.
Module-specific fixtures live in the relevant test file.

Fixture inventory
-----------------
Settings:
    postgres_settings   — PostgresSettings with test values
    delta_settings      — DeltaSettings pointed at a temp dir
    spark_settings      — SparkSettings with minimal resource allocation
    airflow_settings    — AirflowSettings for CLI tests

Postgres mocks:
    mock_psycopg_conn   — mock psycopg3 connection with cursor + copy support
    mock_psycopg_connect — patches psycopg.connect to return mock_psycopg_conn

File fixtures:
    trips_csv_path      — minimal valid trips CSV in a temp dir
    zones_csv_path      — minimal valid zones CSV in a temp dir

Spark mocks:
    mock_spark_session  — mock SparkSession with reader chain
    mock_dataframe      — mock DataFrame with count() and write chain

Delta mocks:
    mock_delta_table    — mock DeltaTable with merge chain
"""

from __future__ import annotations

import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import MagicMock, patch

import pytest

from pipeline.settings import (
    AirflowSettings,
    DeltaSettings,
    PostgresSettings,
    SparkSettings,
)


# ---------------------------------------------------------------------------
# Settings fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def postgres_settings(monkeypatch: pytest.MonkeyPatch) -> PostgresSettings:
    """PostgresSettings wired to a local test database."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "test_cab_pipeline")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    return PostgresSettings()


@pytest.fixture()
def delta_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DeltaSettings:
    """DeltaSettings pointed at a pytest tmp_path so tests don't touch S3."""
    gold_path = str(tmp_path / "gold")
    checkpoint_path = str(tmp_path / "checkpoints")
    monkeypatch.setenv("DELTA_GOLD_BASE_PATH", gold_path)
    monkeypatch.setenv("DELTA_CHECKPOINT_PATH", checkpoint_path)
    return DeltaSettings()


@pytest.fixture()
def spark_settings() -> SparkSettings:
    """SparkSettings with minimal resource allocation for tests."""
    return SparkSettings(
        app_name="test-pipeline",
        master="local[1]",
        executor_memory="512m",
        driver_memory="512m",
        jdbc_num_partitions=2,
        jdbc_fetch_size=1000,
    )


@pytest.fixture()
def airflow_settings(monkeypatch: pytest.MonkeyPatch) -> AirflowSettings:
    """AirflowSettings for CLI tests."""
    monkeypatch.setenv("AIRFLOW_API_URL", "http://localhost:8080")
    monkeypatch.setenv("AIRFLOW_USERNAME", "admin")
    monkeypatch.setenv("AIRFLOW_PASSWORD", "admin")
    return AirflowSettings()


# ---------------------------------------------------------------------------
# Postgres mock fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_copy() -> MagicMock:
    """Mock psycopg3 copy context manager."""
    copy = MagicMock()
    copy.__enter__ = MagicMock(return_value=copy)
    copy.__exit__ = MagicMock(return_value=False)
    copy.write_row = MagicMock()
    return copy


@pytest.fixture()
def mock_cursor(mock_copy: MagicMock) -> MagicMock:
    """Mock psycopg3 cursor context manager."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.copy = MagicMock(return_value=mock_copy)
    cur.execute = MagicMock()
    cur.fetchall = MagicMock(return_value=[])
    cur.fetchone = MagicMock(return_value=None)
    return cur


@pytest.fixture()
def mock_psycopg_conn(mock_cursor: MagicMock) -> MagicMock:
    """Mock psycopg3 connection."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor = MagicMock(return_value=mock_cursor)
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    return conn


@pytest.fixture()
def mock_psycopg_connect(
    mock_psycopg_conn: MagicMock,
) -> Iterator[MagicMock]:
    """Patch psycopg.connect across all pipeline modules."""
    with patch("psycopg.connect", return_value=mock_psycopg_conn) as mock:
        yield mock


# ---------------------------------------------------------------------------
# File fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def trips_csv_path(tmp_path: Path) -> Path:
    """Minimal valid trips CSV file."""
    path = tmp_path / "yellow_tripdata_2024-01.csv"
    rows = [
        {
            "VendorID": 1,
            "tpep_pickup_datetime": "2024-01-15 08:00:00",
            "tpep_dropoff_datetime": "2024-01-15 08:20:00",
            "passenger_count": 2,
            "trip_distance": 3.5,
            "PULocationID": 100,
            "DOLocationID": 200,
            "fare_amount": 14.50,
            "tip_amount": 2.00,
            "total_amount": 16.50,
        },
        {
            "VendorID": 2,
            "tpep_pickup_datetime": "2024-01-15 09:00:00",
            "tpep_dropoff_datetime": "2024-01-15 09:35:00",
            "passenger_count": 1,
            "trip_distance": 8.2,
            "PULocationID": 150,
            "DOLocationID": 250,
            "fare_amount": 28.00,
            "tip_amount": 5.00,
            "total_amount": 33.00,
        },
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture()
def zones_csv_path(tmp_path: Path) -> Path:
    """Minimal valid taxi zone lookup CSV."""
    path = tmp_path / "taxi_zone_lookup.csv"
    rows = [
        {"LocationID": 1, "Borough": "Manhattan", "Zone": "Midtown", "service_zone": "Yellow Zone"},
        {"LocationID": 2, "Borough": "Brooklyn", "Zone": "Park Slope", "service_zone": "Boro Zone"},
        {"LocationID": 3, "Borough": "Queens", "Zone": "JFK Airport", "service_zone": "Airports"},
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.fixture()
def bad_schema_csv_path(tmp_path: Path) -> Path:
    """CSV with wrong columns — triggers SchemaValidationError."""
    path = tmp_path / "bad_schema.csv"
    path.write_text("col_a,col_b,col_c\n1,2,3\n4,5,6\n")
    return path


# ---------------------------------------------------------------------------
# Spark mock fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_dataframe() -> MagicMock:
    """Mock PySpark DataFrame with count and write chain."""
    df = MagicMock()
    df.count = MagicMock(return_value=1000)
    df.alias = MagicMock(return_value=df)

    write = MagicMock()
    write.format = MagicMock(return_value=write)
    write.mode = MagicMock(return_value=write)
    write.option = MagicMock(return_value=write)
    write.save = MagicMock()
    df.write = write
    return df


@pytest.fixture()
def mock_spark_session(mock_dataframe: MagicMock) -> MagicMock:
    """Mock SparkSession with JDBC reader chain."""
    reader = MagicMock()
    reader.format = MagicMock(return_value=reader)
    reader.option = MagicMock(return_value=reader)
    reader.load = MagicMock(return_value=mock_dataframe)

    spark = MagicMock()
    spark.read = reader
    spark.sparkContext = MagicMock()
    spark.sparkContext.setLogLevel = MagicMock()
    return spark


# ---------------------------------------------------------------------------
# Delta mock fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_delta_table() -> MagicMock:
    """Mock DeltaTable with merge chain."""
    merge = MagicMock()
    merge.alias = MagicMock(return_value=merge)
    merge.merge = MagicMock(return_value=merge)
    merge.whenMatchedUpdateAll = MagicMock(return_value=merge)
    merge.whenNotMatchedInsertAll = MagicMock(return_value=merge)
    merge.execute = MagicMock()

    table = MagicMock()
    table.alias = MagicMock(return_value=merge)
    return table


@pytest.fixture()
def mock_delta_module(
    mock_delta_table: MagicMock,
    mock_spark_session: MagicMock,
) -> MagicMock:
    """Mock delta.tables module."""
    module = MagicMock()
    module.DeltaTable.isDeltaTable = MagicMock(return_value=True)
    module.DeltaTable.forPath = MagicMock(return_value=mock_delta_table)
    return module


# ---------------------------------------------------------------------------
# Run record fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_run_id() -> str:
    return "scheduled__2024-01-15T06:00:00+00:00"


@pytest.fixture()
def sample_partition_date() -> date:
    return date(2024, 1, 15)


@pytest.fixture()
def sample_started_at() -> datetime:
    return datetime(2024, 1, 15, 6, 0, 0, tzinfo=timezone.utc)
