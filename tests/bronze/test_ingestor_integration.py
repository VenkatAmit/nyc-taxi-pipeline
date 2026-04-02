"""
BronzeIngestor tests using shared conftest fixtures.

These tests use the conftest postgres mock fixtures directly,
verifying the ingestor works correctly with the standardised
mock connection rather than ad-hoc mocks.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.bronze.ingestor import BronzeIngestor, IngestResult
from pipeline.exceptions import PostgresError, SchemaValidationError, SourceNotFoundError
from pipeline.settings import PostgresSettings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def ingestor(postgres_settings: PostgresSettings) -> BronzeIngestor:
    return BronzeIngestor(settings=postgres_settings)


# ---------------------------------------------------------------------------
# ingest_trips — using shared file fixtures
# ---------------------------------------------------------------------------


class TestIngestTripsWithSharedFixtures:
    def test_ingest_trips_success(
        self,
        ingestor: BronzeIngestor,
        trips_csv_path: Path,
        mock_psycopg_conn: MagicMock,
    ) -> None:
        with patch("pipeline.bronze.ingestor.psycopg.connect", return_value=mock_psycopg_conn):
            result = ingestor.ingest_trips(trips_csv_path, date(2024, 1, 1))

        assert isinstance(result, IngestResult)
        assert result.table == "raw_trips"
        assert result.rows_copied == 2
        assert result.partition_date == date(2024, 1, 1)
        mock_psycopg_conn.commit.assert_called_once()

    def test_ingest_trips_missing_file(
        self,
        ingestor: BronzeIngestor,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(SourceNotFoundError):
            ingestor.ingest_trips(tmp_path / "missing.csv", date(2024, 1, 1))

    def test_ingest_trips_bad_schema(
        self,
        ingestor: BronzeIngestor,
        bad_schema_csv_path: Path,
    ) -> None:
        with pytest.raises(SchemaValidationError):
            ingestor.ingest_trips(bad_schema_csv_path, date(2024, 1, 1))

    def test_ingest_trips_postgres_error_wrapped(
        self,
        ingestor: BronzeIngestor,
        trips_csv_path: Path,
    ) -> None:
        import psycopg as pg

        with patch(
            "pipeline.bronze.ingestor.psycopg.connect",
            side_effect=pg.OperationalError("connection refused"),
        ):
            with pytest.raises(PostgresError) as exc_info:
                ingestor.ingest_trips(trips_csv_path, date(2024, 1, 1))
        assert exc_info.value.table == "raw_trips"
        assert "connection refused" in str(exc_info.value)

    def test_ingest_trips_result_is_frozen(
        self,
        ingestor: BronzeIngestor,
        trips_csv_path: Path,
        mock_psycopg_conn: MagicMock,
    ) -> None:
        with patch("pipeline.bronze.ingestor.psycopg.connect", return_value=mock_psycopg_conn):
            result = ingestor.ingest_trips(trips_csv_path, date(2024, 1, 1))
        with pytest.raises(Exception):
            result.rows_copied = 999  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ingest_zones — using shared file fixtures
# ---------------------------------------------------------------------------


class TestIngestZonesWithSharedFixtures:
    def test_ingest_zones_success(
        self,
        ingestor: BronzeIngestor,
        zones_csv_path: Path,
        mock_psycopg_conn: MagicMock,
    ) -> None:
        with patch("pipeline.bronze.ingestor.psycopg.connect", return_value=mock_psycopg_conn):
            result = ingestor.ingest_zones(zones_csv_path)

        assert result.table == "raw_zones"
        assert result.rows_copied == 3
        mock_psycopg_conn.commit.assert_called_once()

    def test_ingest_zones_missing_file(
        self,
        ingestor: BronzeIngestor,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(SourceNotFoundError):
            ingestor.ingest_zones(tmp_path / "missing.csv")

    def test_ingest_zones_bad_schema(
        self,
        ingestor: BronzeIngestor,
        bad_schema_csv_path: Path,
    ) -> None:
        with pytest.raises(SchemaValidationError):
            ingestor.ingest_zones(bad_schema_csv_path)


# ---------------------------------------------------------------------------
# DSN and JDBC properties
# ---------------------------------------------------------------------------


class TestPostgresSettingsProperties:
    def test_dsn_contains_all_parts(
        self, postgres_settings: PostgresSettings
    ) -> None:
        dsn = postgres_settings.dsn
        assert "localhost" in dsn
        assert "5432" in dsn
        assert "test_cab_pipeline" in dsn
        assert "test_user" in dsn
        assert "test_password" in dsn

    def test_jdbc_url_format(self, postgres_settings: PostgresSettings) -> None:
        assert postgres_settings.jdbc_url.startswith("jdbc:postgresql://")
        assert "localhost:5432" in postgres_settings.jdbc_url

    def test_jdbc_properties_has_driver(
        self, postgres_settings: PostgresSettings
    ) -> None:
        assert postgres_settings.jdbc_properties["driver"] == "org.postgresql.Driver"
