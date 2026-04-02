"""
Gold layer loader — Postgres → Delta Lake via PySpark.

GoldLoader is the only component in the pipeline that requires a
SparkSession. It reads conformed silver data from Postgres over JDBC,
applies final business-level transformations, and writes to Delta Lake
using MERGE INTO for idempotency.

Why Spark here and nowhere else
--------------------------------
- The JDBC read is parallelised across multiple Spark tasks via
  partitionColumn — a single-threaded read of 10M+ rows would be
  too slow for a nightly batch window.
- Delta Lake's MERGE INTO requires Spark (or the Delta Standalone
  library). Writing to Delta from plain Python is not supported for
  the merge pattern.
- Bronze ingestion and silver (dbt) run entirely without Spark,
  keeping those images lean and fast to start.

Idempotency
-----------
Each GoldLoader run uses MERGE INTO on the gold table's natural key.
Re-running the same partition_date always produces the same result —
existing rows are updated in place, new rows are inserted.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

from pipeline.exceptions import DeltaError, SparkError
from pipeline.settings import (
    DeltaSettings,
    PostgresSettings,
    SparkSettings,
    get_settings,
)
from pipeline.spark_session import get_spark, stop_spark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

#: Delta table name → path suffix under DeltaSettings.gold_base_path
GOLD_TABLES: dict[str, str] = {
    "fact_trips": "fact_trips",
    "dim_zones": "dim_zones",
}

#: Natural key columns used in MERGE INTO for each gold table
MERGE_KEYS: dict[str, list[str]] = {
    "fact_trips": ["trip_id"],
    "dim_zones": ["location_id"],
}

#: Postgres silver schema (dbt writes here)
SILVER_SCHEMA = "silver"


class GoldLoader:
    """Loads gold Delta Lake tables from Postgres silver data.

    Parameters
    ----------
    postgres_settings:
        PostgresSettings for the JDBC source connection.
    delta_settings:
        DeltaSettings for the Delta Lake target paths.
    spark_settings:
        SparkSettings for JDBC partition tuning.

    Examples
    --------
    Typical Airflow task usage::

        loader = GoldLoader()
        rows = loader.load_fact_trips(partition_date=date(2024, 1, 1))
        logger.info("Wrote %d rows to fact_trips", rows)
    """

    def __init__(
        self,
        postgres_settings: PostgresSettings | None = None,
        delta_settings: DeltaSettings | None = None,
        spark_settings: SparkSettings | None = None,
    ) -> None:
        settings = get_settings()
        self._pg = postgres_settings or settings.postgres
        self._delta = delta_settings or settings.delta
        self._spark_cfg = spark_settings or settings.spark

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_fact_trips(self, partition_date: date) -> int:
        """Load silver_trips for a partition date into the fact_trips Delta table.

        Parameters
        ----------
        partition_date:
            Logical date of the partition to load. Only rows with this
            partition_date are read from Postgres and merged into gold.

        Returns
        -------
        int
            Number of rows written (inserted + updated).

        Raises
        ------
        SparkError
            If the Spark job fails for any reason.
        DeltaError
            If the Delta MERGE fails.
        """
        spark = self._get_spark()
        table = "fact_trips"
        delta_path = f"{self._delta.gold_base_path}/{GOLD_TABLES[table]}"

        logger.info(
            "Loading %s for partition_date=%s → %s",
            table,
            partition_date,
            delta_path,
        )

        try:
            df = self._read_silver_trips(spark, partition_date)
            row_count = df.count()

            self._merge_into_delta(
                spark=spark,
                df=df,
                delta_path=delta_path,
                merge_keys=MERGE_KEYS[table],
            )
        except Exception as exc:
            raise SparkError(f"load_fact_trips[{partition_date}]", cause=exc) from exc

        logger.info("Wrote %d rows to %s", row_count, delta_path)
        return row_count

    def load_dim_zones(self) -> int:
        """Load silver_zones into the dim_zones Delta table (full refresh).

        Zone data is small (~265 rows) — full overwrite is safe and simple.

        Returns
        -------
        int
            Number of rows written.

        Raises
        ------
        SparkError, DeltaError
        """
        spark = self._get_spark()
        table = "dim_zones"
        delta_path = f"{self._delta.gold_base_path}/{GOLD_TABLES[table]}"

        logger.info("Loading %s (full refresh) → %s", table, delta_path)

        try:
            df = self._read_silver_zones(spark)
            row_count = df.count()

            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(delta_path)
            )
        except Exception as exc:
            raise SparkError("load_dim_zones", cause=exc) from exc

        logger.info("Wrote %d rows to %s", row_count, delta_path)
        return row_count

    # ------------------------------------------------------------------
    # JDBC reads
    # ------------------------------------------------------------------

    def _read_silver_trips(
        self,
        spark: SparkSession,
        partition_date: date,
    ) -> DataFrame:
        """Read silver_trips for a given partition_date via partitioned JDBC.

        The read is split across self._spark_cfg.jdbc_num_partitions
        parallel tasks using trip_id (MD5 hex) as the partition column.
        This avoids a single-threaded JDBC fetch on large tables.

        Postgres MD5 hashes are 32-char hex strings. We partition on the
        first two hex chars (256 buckets) mapped to integers 0-255, then
        sub-divide to jdbc_num_partitions.
        """
        query = f"""
            (
                SELECT *
                FROM {SILVER_SCHEMA}.silver_trips
                WHERE partition_date = '{partition_date}'
            ) AS silver_trips_partition
        """
        try:
            return (
                spark.read.format("jdbc")
                .option("url", self._pg.jdbc_url)
                .option("dbtable", query)
                .option("user", self._pg.jdbc_properties["user"])
                .option("password", self._pg.jdbc_properties["password"])
                .option("driver", self._pg.jdbc_properties["driver"])
                .option("fetchsize", str(self._spark_cfg.jdbc_fetch_size))
                .option("numPartitions", str(self._spark_cfg.jdbc_num_partitions))
                .option("partitionColumn", "pickup_hour_of_day")
                .option("lowerBound", "0")
                .option("upperBound", "23")
                .load()
            )
        except Exception as exc:
            raise SparkError(
                f"JDBC read silver_trips[{partition_date}]", cause=exc
            ) from exc

    def _read_silver_zones(self, spark: SparkSession) -> DataFrame:
        """Read the full silver_zones table via JDBC (single partition — small table)."""
        try:
            return (
                spark.read.format("jdbc")
                .option("url", self._pg.jdbc_url)
                .option(
                    "dbtable",
                    f"{SILVER_SCHEMA}.silver_zones",
                )
                .option("user", self._pg.jdbc_properties["user"])
                .option("password", self._pg.jdbc_properties["password"])
                .option("driver", self._pg.jdbc_properties["driver"])
                .option("fetchsize", "1000")
                .load()
            )
        except Exception as exc:
            raise SparkError("JDBC read silver_zones", cause=exc) from exc

    # ------------------------------------------------------------------
    # Delta MERGE
    # ------------------------------------------------------------------

    def _merge_into_delta(
        self,
        spark: SparkSession,
        df: DataFrame,
        delta_path: str,
        merge_keys: list[str],
    ) -> None:
        """MERGE incoming DataFrame into a Delta table at delta_path.

        Creates the Delta table on first run if it doesn't exist.
        Subsequent runs merge on merge_keys — matching rows are updated,
        new rows are inserted.

        Parameters
        ----------
        spark:
            Active SparkSession with Delta extensions loaded.
        df:
            Incoming DataFrame to merge.
        delta_path:
            S3 or local path of the target Delta table.
        merge_keys:
            Column names that form the natural key for the merge condition.

        Raises
        ------
        DeltaError
            If the Delta operation fails.
        """
        try:
            from delta.tables import DeltaTable  # type: ignore[import]
        except ImportError as exc:
            raise DeltaError(
                "import",
                delta_path,
                cause=exc,
            ) from exc

        try:
            if DeltaTable.isDeltaTable(spark, delta_path):
                delta_table = DeltaTable.forPath(spark, delta_path)

                # Build merge condition from merge_keys
                condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

                (
                    delta_table.alias("target")
                    .merge(df.alias("source"), condition)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                # First run — write as a new Delta table
                (df.write.format("delta").mode("overwrite").save(delta_path))
        except Exception as exc:
            raise DeltaError("MERGE", delta_path, cause=exc) from exc

    # ------------------------------------------------------------------
    # Spark session management
    # ------------------------------------------------------------------

    def _get_spark(self) -> SparkSession:
        """Return the active SparkSession, creating it if needed."""
        try:
            return get_spark(settings=self._spark_cfg)
        except Exception as exc:
            raise SparkError("SparkSession initialisation", cause=exc) from exc

    def stop(self) -> None:
        """Stop the SparkSession and clear the cache.

        Call this at the end of an Airflow task to release resources.
        """
        stop_spark()

    def __repr__(self) -> str:
        return (
            f"GoldLoader("
            f"postgres={self._pg.host!r}, "
            f"delta={self._delta.gold_base_path!r}, "
            f"partitions={self._spark_cfg.jdbc_num_partitions})"
        )
