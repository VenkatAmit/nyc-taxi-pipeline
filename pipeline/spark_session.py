"""
SparkSession factory for the cab-spark-data-pipeline gold worker.

IMPORTANT — import boundary
---------------------------
This module is imported ONLY by pipeline/gold/loader.py.
Bronze ingestion and silver (dbt) run without a SparkSession.
Never import this from dags/, cli/, or pipeline/bronze/.

Usage
-----
    from pipeline.spark_session import get_spark

    spark = get_spark()                    # uses Settings defaults
    spark = get_spark(settings=my_settings)  # explicit settings

The session is a singleton per process — calling get_spark() twice
returns the same SparkSession. Call get_spark.cache_clear() in tests
that need a fresh session.

Prerequisites (gold Docker image only)
---------------------------------------
The Postgres JDBC jar must be present in $SPARK_HOME/jars/:
    postgresql-42.x.x.jar

It is COPY'd into the image in docker/Dockerfile.gold — it is not
pip-installable and must be handled at the Docker layer.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from pipeline.exceptions import ConfigurationError, SparkError
from pipeline.settings import SparkSettings, get_settings

if TYPE_CHECKING:
    # pyspark is only available inside the gold Docker image.
    # TYPE_CHECKING guard prevents ImportError on the host / bronze worker.
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Delta Lake package coordinates
# Pinned to match delta-spark==2.4.* which targets PySpark 3.4.x
# ---------------------------------------------------------------------------
_DELTA_MAVEN_COORDS = "io.delta:delta-core_2.12:2.4.0"
_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "spark.sql.catalog.spark_catalog"
_DELTA_CATALOG_IMPL = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def _build_spark_session(settings: SparkSettings) -> SparkSession:
    """Construct and configure a SparkSession.

    Separated from the cached wrapper so tests can call this directly
    with custom settings without touching the lru_cache.

    Raises
    ------
    ConfigurationError
        If PySpark is not installed in the current environment.
    SparkError
        If SparkSession construction fails for any other reason.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ConfigurationError(
            "PySpark is not installed. "
            "The gold worker requires the [spark] extra: "
            "`uv sync --extra spark` inside docker/Dockerfile.gold.",
            cause=exc,
        ) from exc

    try:
        spark = (
            SparkSession.builder.appName(settings.app_name)
            .master(settings.master)
            # ----------------------------------------------------------------
            # Delta Lake extensions
            # ----------------------------------------------------------------
            .config(
                "spark.sql.extensions",
                _DELTA_EXTENSION,
            )
            .config(
                _DELTA_CATALOG,
                _DELTA_CATALOG_IMPL,
            )
            # ----------------------------------------------------------------
            # Delta Lake package (fetched from Maven at session start)
            # ----------------------------------------------------------------
            .config(
                "spark.jars.packages",
                _DELTA_MAVEN_COORDS,
            )
            # ----------------------------------------------------------------
            # Memory
            # ----------------------------------------------------------------
            .config(
                "spark.executor.memory",
                settings.executor_memory,
            )
            .config(
                "spark.driver.memory",
                settings.driver_memory,
            )
            # ----------------------------------------------------------------
            # JDBC — Postgres driver jar is pre-loaded via spark.jars.packages
            # above; these settings tune connection pool behaviour.
            # ----------------------------------------------------------------
            .config(
                "spark.sql.execution.arrow.pyspark.enabled",
                "true",
            )
            # ----------------------------------------------------------------
            # Delta Lake optimistic concurrency + write performance
            # ----------------------------------------------------------------
            .config(
                "spark.databricks.delta.retentionDurationCheck.enabled",
                "false",
            )
            .config(
                "spark.sql.shuffle.partitions",
                str(settings.jdbc_num_partitions * 2),
            )
            .getOrCreate()
        )
    except Exception as exc:
        raise SparkError("SparkSession construction failed", cause=exc) from exc

    # Suppress verbose INFO logging — Spark is extremely chatty by default.
    spark.sparkContext.setLogLevel("WARN")

    return spark


@lru_cache(maxsize=1)
def get_spark(settings: SparkSettings | None = None) -> SparkSession:
    """Return the singleton SparkSession for this process.

    Parameters
    ----------
    settings:
        SparkSettings instance. Defaults to get_settings().spark.
        Pass an explicit instance in tests to avoid touching the
        global settings cache.

    Raises
    ------
    ConfigurationError
        If PySpark is not installed.
    SparkError
        If session construction fails.
    """
    resolved = settings if settings is not None else get_settings().spark
    return _build_spark_session(resolved)


def stop_spark() -> None:
    """Stop the active SparkSession and clear the cache.

    Call this at the end of a Airflow task or test teardown to release
    resources. A new session will be created on the next get_spark() call.
    """
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
    except ImportError:
        pass  # PySpark not installed — nothing to stop
    finally:
        get_spark.cache_clear()
