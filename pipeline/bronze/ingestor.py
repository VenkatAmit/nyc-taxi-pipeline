"""
Bronze ingestion layer — raw NYC taxi data into Postgres.

BronzeIngestor reads source Parquet files, validates the
incoming schema, then bulk-loads rows into Postgres using the native
COPY protocol via psycopg3.

Why COPY over INSERT
--------------------
psycopg3's COPY is the fastest Postgres bulk-load mechanism — it
bypasses the SQL parser and goes directly to the storage layer.
For the NYC taxi dataset (~10M rows/month) this is typically 5-10x
faster than executemany(INSERT ...).

Design
------
- No SparkSession — this class runs on the slim bronze Docker image
- Dependency injection for the connection string — testable without
  a real Postgres instance
- Idempotent via a staging table + UPSERT pattern: raw data is
  COPY'd into a temp table, then merged into the target so re-running
  the same date partition is safe
- Schema validation happens before any writes — bad data is rejected
  before it touches the database
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Final, Iterator, Sequence

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from pipeline.exceptions import (
    PostgresError,
    SchemaValidationError,
    SourceNotFoundError,
)
from pipeline.settings import PostgresSettings, get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

#: Columns required in every raw_trips source file.
RAW_TRIPS_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "tip_amount",
        "total_amount",
    }
)

#: Columns required in every raw_zones source file.
RAW_ZONES_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    }
)

# ---------------------------------------------------------------------------
# DDL — tables are created if they don't exist
# ---------------------------------------------------------------------------

_CREATE_RAW_TRIPS_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS raw_trips (
    vendor_id           SMALLINT,
    pickup_datetime     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dropoff_datetime    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    passenger_count     SMALLINT,
    trip_distance       NUMERIC(8, 2),
    pu_location_id      SMALLINT NOT NULL,
    do_location_id      SMALLINT NOT NULL,
    fare_amount         NUMERIC(10, 2),
    tip_amount          NUMERIC(10, 2),
    total_amount        NUMERIC(10, 2),
    partition_date      DATE NOT NULL,
    ingested_at         TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
) PARTITION BY RANGE (partition_date);
"""

_CREATE_RAW_ZONES_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS raw_zones (
    location_id     SMALLINT PRIMARY KEY,
    borough         TEXT NOT NULL,
    zone            TEXT NOT NULL,
    service_zone    TEXT NOT NULL,
    ingested_at     TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
"""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IngestResult:
    """Summary returned after a successful ingest run."""

    table: str
    partition_date: date
    rows_copied: int
    source_path: str
    duration_seconds: float = field(default=0.0)


# ---------------------------------------------------------------------------
# BronzeIngestor
# ---------------------------------------------------------------------------


class BronzeIngestor:
    """Ingests raw NYC taxi source files into Postgres bronze tables.

    Parameters
    ----------
    settings:
        PostgresSettings instance. Defaults to get_settings().postgres.
        Pass an explicit instance in tests or Airflow tasks that build
        their own settings from task context.

    Examples
    --------
    Typical Airflow task usage::

        ingestor = BronzeIngestor()
        result = ingestor.ingest_trips(
            source_path=Path("/data/yellow_tripdata_2024-01.parquet"),
            partition_date=date(2024, 1, 1),
        )
        logger.info("Ingested %d rows", result.rows_copied)
    """

    def __init__(self, settings: PostgresSettings | None = None) -> None:
        self._settings = settings or get_settings().postgres

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest_trips(
        self,
        source_path: Path,
        partition_date: date,
    ) -> IngestResult:
        """Ingest a raw trips source file for a given partition date.

        Parameters
        ----------
        source_path:
            Path to the source Parquet or CSV file.
        partition_date:
            Logical date for this partition (used for idempotent UPSERT
            and Postgres table partitioning).

        Returns
        -------
        IngestResult
            Summary of the completed ingest.

        Raises
        ------
        SourceNotFoundError
            If source_path does not exist.
        SchemaValidationError
            If required columns are missing from the source file.
        PostgresError
            If the COPY or UPSERT operation fails.
        """
        import time

        start = time.monotonic()

        self._assert_source_exists(source_path)
        columns = self._read_column_names(source_path)
        self._validate_schema("raw_trips", columns, RAW_TRIPS_REQUIRED_COLUMNS)

        rows_copied = 0
        try:
            with self._connect() as conn:
                self._ensure_tables(conn)
                rows_copied = self._copy_trips(conn, source_path, partition_date)
                conn.commit()
        except psycopg.Error as exc:
            raise PostgresError("COPY", "raw_trips", cause=exc) from exc

        duration = time.monotonic() - start
        logger.info(
            "Ingested %d rows into raw_trips [partition=%s, source=%s, %.2fs]",
            rows_copied,
            partition_date,
            source_path.name,
            duration,
        )
        return IngestResult(
            table="raw_trips",
            partition_date=partition_date,
            rows_copied=rows_copied,
            source_path=str(source_path),
            duration_seconds=duration,
        )

    def ingest_zones(self, source_path: Path) -> IngestResult:
        """Ingest the taxi zone lookup file (full refresh).

        Zone data is small and static — we truncate and reload rather
        than partition. Re-running is always safe.

        Parameters
        ----------
        source_path:
            Path to the taxi_zone_lookup.csv file.

        Returns
        -------
        IngestResult

        Raises
        ------
        SourceNotFoundError, SchemaValidationError, PostgresError
        """
        import time

        start = time.monotonic()

        self._assert_source_exists(source_path)
        columns = self._read_column_names(source_path)
        self._validate_schema("raw_zones", columns, RAW_ZONES_REQUIRED_COLUMNS)

        rows_copied = 0
        try:
            with self._connect() as conn:
                self._ensure_tables(conn)
                rows_copied = self._copy_zones(conn, source_path)
                conn.commit()
        except psycopg.Error as exc:
            raise PostgresError("COPY", "raw_zones", cause=exc) from exc

        duration = time.monotonic() - start
        logger.info(
            "Ingested %d rows into raw_zones [source=%s, %.2fs]",
            rows_copied,
            source_path.name,
            duration,
        )
        return IngestResult(
            table="raw_zones",
            partition_date=date.today(),
            rows_copied=rows_copied,
            source_path=str(source_path),
            duration_seconds=duration,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> Connection[dict]:  # type: ignore[type-arg]
        """Open a psycopg3 connection with dict row factory."""
        return psycopg.connect(self._settings.dsn, row_factory=dict_row)

    def _assert_source_exists(self, path: Path) -> None:
        if not path.exists():
            raise SourceNotFoundError(str(path))

    def _read_column_names(self, path: Path) -> list[str]:
        """Return column names from the source file without loading all data."""
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            return self._parquet_columns(path)
        if suffix == ".csv":
            return self._csv_columns(path)
        raise SourceNotFoundError(
            f"Unsupported file format {suffix!r} — expected .parquet or .csv"
        )

    @staticmethod
    def _parquet_columns(path: Path) -> list[str]:
        try:
            import pyarrow.parquet as pq  # type: ignore[import]

            schema = pq.read_schema(path)
            return list(schema.names)
        except ImportError as exc:
            raise SourceNotFoundError(
                "pyarrow is required to read Parquet files. "
                "Add it to the bronze Docker image.",
                cause=exc,
            ) from exc

    @staticmethod
    def _csv_columns(path: Path) -> list[str]:
        with path.open() as f:
            header = f.readline().strip()
        return [col.strip() for col in header.split(",")]

    @staticmethod
    def _validate_schema(
        table: str,
        actual_columns: list[str],
        required_columns: frozenset[str],
    ) -> None:
        missing = sorted(required_columns - set(actual_columns))
        if missing:
            raise SchemaValidationError(table, missing)

    def _ensure_tables(self, conn: Connection[dict]) -> None:  # type: ignore[type-arg]
        """Create bronze tables if they don't already exist."""
        with conn.cursor() as cur:
            cur.execute(_CREATE_RAW_TRIPS_SQL)
            cur.execute(_CREATE_RAW_ZONES_SQL)
            self._ensure_partition(conn)

    def _ensure_partition(self, conn: Connection[dict]) -> None:  # type: ignore[type-arg]
        """Create a monthly partition for raw_trips if it doesn't exist.

        Partition naming convention: raw_trips_YYYY_MM
        """
        # Partition management is handled at the DDL level in production.
        # For local dev / CI, a default partition catches all dates.
        sql = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class
                WHERE relname = 'raw_trips_default'
            ) THEN
                EXECUTE 'CREATE TABLE raw_trips_default
                         PARTITION OF raw_trips DEFAULT';
            END IF;
        END
        $$;
        """
        with conn.cursor() as cur:
            cur.execute(sql)

    def _copy_trips(
        self,
        conn: Connection[dict],  # type: ignore[type-arg]
        source_path: Path,
        partition_date: date,
    ) -> int:
        """COPY trips data into a staging table then UPSERT into raw_trips."""
        staging = "raw_trips_staging"
        self._create_staging(conn, staging, "raw_trips")

        rows = list(self._iter_trips_rows(source_path, partition_date))
        with conn.cursor() as cur:
            with cur.copy(
                f"""
                COPY {staging} (
                    vendor_id, pickup_datetime, dropoff_datetime,
                    passenger_count, trip_distance,
                    pu_location_id, do_location_id,
                    fare_amount, tip_amount, total_amount,
                    partition_date
                ) FROM STDIN
                """
            ) as copy:
                for row in rows:
                    copy.write_row(row)

            # Idempotent merge: delete existing rows for this partition then insert
            cur.execute(
                "DELETE FROM raw_trips WHERE partition_date = %s",
                (partition_date,),
            )
            cur.execute(
                f"""
                INSERT INTO raw_trips
                SELECT * FROM {staging}
                """
            )
            cur.execute(f"DROP TABLE {staging}")

        return len(rows)

    def _copy_zones(
        self,
        conn: Connection[dict],  # type: ignore[type-arg]
        source_path: Path,
    ) -> int:
        """Truncate raw_zones and COPY fresh data."""
        rows = list(self._iter_zones_rows(source_path))
        with conn.cursor() as cur:
            cur.execute("TRUNCATE raw_zones")
            with cur.copy(
                """
                COPY raw_zones (location_id, borough, zone, service_zone)
                FROM STDIN
                """
            ) as copy:
                for row in rows:
                    copy.write_row(row)
        return len(rows)

    @staticmethod
    def _create_staging(
        conn: Connection[dict],  # type: ignore[type-arg]
        staging: str,
        like: str,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {staging}")
            cur.execute(
                f"CREATE TEMP TABLE {staging} (LIKE {like} INCLUDING ALL)"
            )

    @staticmethod
    def _iter_trips_rows(
        source_path: Path,
        partition_date: date,
    ) -> Iterator[tuple[object, ...]]:
        """Yield row tuples from the source file for COPY."""
        suffix = source_path.suffix.lower()
        if suffix == ".parquet":
            import pyarrow.parquet as pq  # type: ignore[import]

            table = pq.read_table(source_path)
            df = table.to_pydict()
            count = len(df["VendorID"])
            for i in range(count):
                yield (
                    df["VendorID"][i],
                    df["tpep_pickup_datetime"][i],
                    df["tpep_dropoff_datetime"][i],
                    df["passenger_count"][i],
                    df["trip_distance"][i],
                    df["PULocationID"][i],
                    df["DOLocationID"][i],
                    df["fare_amount"][i],
                    df["tip_amount"][i],
                    df["total_amount"][i],
                    partition_date,
                )
        elif suffix == ".csv":
            import csv

            with source_path.open(newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield (
                        int(row["VendorID"]),
                        row["tpep_pickup_datetime"],
                        row["tpep_dropoff_datetime"],
                        int(row["passenger_count"]) if row["passenger_count"] else None,
                        float(row["trip_distance"]),
                        int(row["PULocationID"]),
                        int(row["DOLocationID"]),
                        float(row["fare_amount"]),
                        float(row["tip_amount"]),
                        float(row["total_amount"]),
                        partition_date,
                    )

    @staticmethod
    def _iter_zones_rows(source_path: Path) -> Iterator[tuple[object, ...]]:
        import csv

        with source_path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield (
                    int(row["LocationID"]),
                    row["Borough"],
                    row["Zone"],
                    row["service_zone"],
                )

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"BronzeIngestor("
            f"host={self._settings.host!r}, "
            f"db={self._settings.db!r})"
        )
