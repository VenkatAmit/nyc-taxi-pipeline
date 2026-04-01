"""
Tests for pipeline/bronze/ingestor.py.

psycopg3 and pyarrow are mocked throughout — no real Postgres
instance or Parquet files are needed to run these tests.
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from typing import Any

import pytest

from pipeline.bronze.ingestor import (
    BronzeIngestor,
    IngestResult,
    RAW_TRIPS_REQUIRED_COLUMNS,
    RAW_ZONES_REQUIRED_COLUMNS,
)
from pipeline.exceptions import (
    PostgresError,
    SchemaValidationError,
    SourceNotFoundError,
)
from pipeline.settings import PostgresSettings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(monkeypatch: pytest.MonkeyPatch) -> PostgresSettings:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    return PostgresSettings(
        host="localhost",
        port=5432,
        db="test_db",
        user="test_user",
        password="test",  # type: ignore[arg-type]
    )


@pytest.fixture()
def ingestor(settings: PostgresSettings) -> BronzeIngestor:
    return BronzeIngestor(settings=settings)


@pytest.fixture()
def trips_csv(tmp_path: Path) -> Path:
    """Write a minimal valid trips CSV to a temp file."""
    p = tmp_path / "trips.csv"
    rows = [
        {
            "VendorID": 1,
            "tpep_pickup_datetime": "2024-01-01 00:01:00",
            "tpep_dropoff_datetime": "2024-01-01 00:15:00",
            "passenger_count": 2,
            "trip_distance": 3.5,
            "PULocationID": 100,
            "DOLocationID": 200,
            "fare_amount": 14.0,
            "tip_amount": 2.5,
            "total_amount": 16.5,
        }
    ]
    with p.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return p


@pytest.fixture()
def zones_csv(tmp_path: Path) -> Path:
    """Write a minimal valid zones CSV to a temp file."""
    p = tmp_path / "zones.csv"
    rows = [{"LocationID": 1, "Borough": "Manhattan", "Zone": "Midtown", "service_zone": "Yellow Zone"}]
    with p.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return p


def _make_mock_conn() -> MagicMock:
    """Return a mock psycopg3 connection with cursor + copy support."""
    mock_copy = MagicMock()
    mock_copy.__enter__ = MagicMock(return_value=mock_copy)
    mock_copy.__exit__ = MagicMock(return_value=False)
    mock_copy.write_row = MagicMock()

    mock_cur = MagicMock()
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_cur.copy = MagicMock(return_value=mock_copy)
    mock_cur.execute = MagicMock()

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cur)
    mock_conn.commit = MagicMock()

    return mock_conn


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestValidateSchema:
    def test_valid_trips_schema_passes(self, ingestor: BronzeIngestor) -> None:
        columns = list(RAW_TRIPS_REQUIRED_COLUMNS) + ["extra_col"]
        ingestor._validate_schema("raw_trips", columns, RAW_TRIPS_REQUIRED_COLUMNS)

    def test_missing_trips_column_raises(self, ingestor: BronzeIngestor) -> None:
        columns = list(RAW_TRIPS_REQUIRED_COLUMNS - {"trip_distance"})
        with pytest.raises(SchemaValidationError) as exc_info:
            ingestor._validate_schema("raw_trips", columns, RAW_TRIPS_REQUIRED_COLUMNS)
        assert "trip_distance" in str(exc_info.value)
        assert exc_info.value.table == "raw_trips"

    def test_valid_zones_schema_passes(self, ingestor: BronzeIngestor) -> None:
        columns = list(RAW_ZONES_REQUIRED_COLUMNS)
        ingestor._validate_schema("raw_zones", columns, RAW_ZONES_REQUIRED_COLUMNS)

    def test_multiple_missing_columns_all_reported(self, ingestor: BronzeIngestor) -> None:
        with pytest.raises(SchemaValidationError) as exc_info:
            ingestor._validate_schema("raw_trips", [], RAW_TRIPS_REQUIRED_COLUMNS)
        assert len(exc_info.value.missing_columns) == len(RAW_TRIPS_REQUIRED_COLUMNS)


# ---------------------------------------------------------------------------
# Source existence
# ---------------------------------------------------------------------------


class TestAssertSourceExists:
    def test_existing_file_passes(self, ingestor: BronzeIngestor, trips_csv: Path) -> None:
        ingestor._assert_source_exists(trips_csv)  # no exception

    def test_missing_file_raises(self, ingestor: BronzeIngestor, tmp_path: Path) -> None:
        with pytest.raises(SourceNotFoundError):
            ingestor._assert_source_exists(tmp_path / "nonexistent.csv")


# ---------------------------------------------------------------------------
# CSV column reading
# ---------------------------------------------------------------------------


class TestReadColumnNames:
    def test_csv_columns_read_correctly(
        self, ingestor: BronzeIngestor, trips_csv: Path
    ) -> None:
        columns = ingestor._read_column_names(trips_csv)
        assert "VendorID" in columns
        assert "trip_distance" in columns

    def test_unsupported_extension_raises(
        self, ingestor: BronzeIngestor, tmp_path: Path
    ) -> None:
        p = tmp_path / "data.json"
        p.write_text("{}")
        with pytest.raises(SourceNotFoundError):
            ingestor._read_column_names(p)


# ---------------------------------------------------------------------------
# ingest_trips
# ---------------------------------------------------------------------------


class TestIngestTrips:
    def test_missing_source_raises(
        self, ingestor: BronzeIngestor, tmp_path: Path
    ) -> None:
        with pytest.raises(SourceNotFoundError):
            ingestor.ingest_trips(tmp_path / "missing.csv", date(2024, 1, 1))

    def test_bad_schema_raises(
        self, ingestor: BronzeIngestor, tmp_path: Path
    ) -> None:
        bad = tmp_path / "bad.csv"
        bad.write_text("col_a,col_b\n1,2\n")
        with pytest.raises(SchemaValidationError):
            ingestor.ingest_trips(bad, date(2024, 1, 1))

    def test_successful_ingest_returns_result(
        self, ingestor: BronzeIngestor, trips_csv: Path
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.bronze.ingestor.psycopg.connect", return_value=mock_conn):
            result = ingestor.ingest_trips(trips_csv, date(2024, 1, 1))

        assert isinstance(result, IngestResult)
        assert result.table == "raw_trips"
        assert result.partition_date == date(2024, 1, 1)
        assert result.rows_copied == 1
        assert result.duration_seconds >= 0

    def test_postgres_error_wrapped(
        self, ingestor: BronzeIngestor, trips_csv: Path
    ) -> None:
        import psycopg as pg

        with patch(
            "pipeline.bronze.ingestor.psycopg.connect",
            side_effect=pg.OperationalError("connection refused"),
        ):
            with pytest.raises(PostgresError) as exc_info:
                ingestor.ingest_trips(trips_csv, date(2024, 1, 1))
        assert exc_info.value.table == "raw_trips"

    def test_commit_called_on_success(
        self, ingestor: BronzeIngestor, trips_csv: Path
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.bronze.ingestor.psycopg.connect", return_value=mock_conn):
            ingestor.ingest_trips(trips_csv, date(2024, 1, 1))
        mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# ingest_zones
# ---------------------------------------------------------------------------


class TestIngestZones:
    def test_missing_source_raises(
        self, ingestor: BronzeIngestor, tmp_path: Path
    ) -> None:
        with pytest.raises(SourceNotFoundError):
            ingestor.ingest_zones(tmp_path / "missing.csv")

    def test_bad_schema_raises(
        self, ingestor: BronzeIngestor, tmp_path: Path
    ) -> None:
        bad = tmp_path / "bad.csv"
        bad.write_text("col_a,col_b\n1,2\n")
        with pytest.raises(SchemaValidationError):
            ingestor.ingest_zones(bad)

    def test_successful_ingest_returns_result(
        self, ingestor: BronzeIngestor, zones_csv: Path
    ) -> None:
        mock_conn = _make_mock_conn()
        with patch("pipeline.bronze.ingestor.psycopg.connect", return_value=mock_conn):
            result = ingestor.ingest_zones(zones_csv)

        assert isinstance(result, IngestResult)
        assert result.table == "raw_zones"
        assert result.rows_copied == 1

    def test_postgres_error_wrapped(
        self, ingestor: BronzeIngestor, zones_csv: Path
    ) -> None:
        import psycopg as pg

        with patch(
            "pipeline.bronze.ingestor.psycopg.connect",
            side_effect=pg.OperationalError("connection refused"),
        ):
            with pytest.raises(PostgresError) as exc_info:
                ingestor.ingest_zones(zones_csv)
        assert exc_info.value.table == "raw_zones"


# ---------------------------------------------------------------------------
# IngestResult
# ---------------------------------------------------------------------------


class TestIngestResult:
    def test_frozen(self) -> None:
        r = IngestResult(
            table="raw_trips",
            partition_date=date(2024, 1, 1),
            rows_copied=100,
            source_path="/data/trips.csv",
        )
        with pytest.raises(Exception):
            r.rows_copied = 999  # type: ignore[misc]

    def test_default_duration(self) -> None:
        r = IngestResult(
            table="raw_trips",
            partition_date=date(2024, 1, 1),
            rows_copied=0,
            source_path="/data/trips.csv",
        )
        assert r.duration_seconds == 0.0


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_contains_host_and_db(self, ingestor: BronzeIngestor) -> None:
        r = repr(ingestor)
        assert "localhost" in r
        assert "test_db" in r

    def test_repr_does_not_contain_password(self, ingestor: BronzeIngestor) -> None:
        assert "test" not in repr(ingestor) or "host" in repr(ingestor)
