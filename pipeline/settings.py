"""
Centralised settings for the cab-spark-data-pipeline.

All configuration is read from environment variables (or a .env file
during local development). No secrets are hardcoded.

Usage
-----
    from pipeline.settings import get_settings

    settings = get_settings()
    dsn = settings.postgres.dsn
    s3_path = settings.delta.gold_base_path

Environment variables
---------------------
See each Settings class for the full list of supported variables.
A .env file at the repo root is loaded automatically in development.
In Docker / CI, variables are injected directly into the environment.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------------------------------------------------------------------------
# Postgres (bronze raw storage + silver dbt target)
# ---------------------------------------------------------------------------


class PostgresSettings(BaseSettings):
    """Connection settings for the Postgres instance hosting bronze + silver.

    Environment variables
    ---------------------
    POSTGRES_HOST      : str   — hostname or IP  (default: localhost)
    POSTGRES_PORT      : int   — port            (default: 5432)
    POSTGRES_DB        : str   — database name   (default: cab_pipeline)
    POSTGRES_USER      : str   — username        (default: postgres)
    POSTGRES_PASSWORD  : SecretStr — password    (required)
    """

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = "localhost"
    port: int = 5432
    db: str = "cab_pipeline"
    user: str = "postgres"
    password: SecretStr

    @property
    def dsn(self) -> str:
        """psycopg3-compatible connection string."""
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.db} "
            f"user={self.user} "
            f"password={self.password.get_secret_value()}"
        )

    @property
    def jdbc_url(self) -> str:
        """JDBC URL for PySpark reads in the gold layer."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        """Properties dict passed to spark.read.jdbc()."""
        return {
            "user": self.user,
            "password": self.password.get_secret_value(),
            "driver": "org.postgresql.Driver",
        }


# ---------------------------------------------------------------------------
# Delta Lake (gold storage)
# ---------------------------------------------------------------------------


class DeltaSettings(BaseSettings):
    """Storage settings for the Delta Lake gold layer.

    Environment variables
    ---------------------
    DELTA_GOLD_BASE_PATH : str — base S3 (or local) path for gold tables
                                 e.g. s3://my-bucket/gold
    DELTA_CHECKPOINT_PATH: str — path for Delta log checkpoints
    AWS_REGION           : str — AWS region (default: us-east-1)
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    gold_base_path: str
    checkpoint_path: str
    aws_region: str = "us-east-1"

    model_config = SettingsConfigDict(
        env_prefix="DELTA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("gold_base_path", "checkpoint_path", mode="before")
    @classmethod
    def strip_trailing_slash(cls, v: Any) -> str:
        """Normalise paths so callers can safely append /table_name."""
        if not isinstance(v, str):
            raise TypeError("path must be a string")
        return v.rstrip("/")


# ---------------------------------------------------------------------------
# Spark (gold worker only)
# ---------------------------------------------------------------------------


class SparkSettings(BaseSettings):
    """Tuning knobs for the PySpark session in the gold worker.

    Environment variables
    ---------------------
    SPARK_APP_NAME          : str — application name shown in Spark UI
    SPARK_MASTER            : str — master URL (default: local[*])
    SPARK_EXECUTOR_MEMORY   : str — executor memory (default: 4g)
    SPARK_DRIVER_MEMORY     : str — driver memory   (default: 2g)
    SPARK_JDBC_NUM_PARTITIONS: int — parallelism for JDBC reads (default: 8)
    SPARK_JDBC_FETCH_SIZE   : int — rows per JDBC fetch  (default: 50000)
    """

    model_config = SettingsConfigDict(
        env_prefix="SPARK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "cab-spark-data-pipeline"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    jdbc_num_partitions: int = 8
    jdbc_fetch_size: int = 50_000

    @field_validator("jdbc_num_partitions", mode="before")
    @classmethod
    def positive_partitions(cls, v: Any) -> int:
        value = int(v)
        if value < 1:
            raise ValueError("jdbc_num_partitions must be >= 1")
        return value


# ---------------------------------------------------------------------------
# Airflow API (CLI client)
# ---------------------------------------------------------------------------


class AirflowSettings(BaseSettings):
    """Connection settings for the Airflow REST API.

    Used exclusively by cli/airflow_client.py.
    The pipeline/ Service classes never reference this.

    Environment variables
    ---------------------
    AIRFLOW_API_URL  : str       — base URL, e.g. http://localhost:8080
    AIRFLOW_USERNAME : str       — basic-auth username
    AIRFLOW_PASSWORD : SecretStr — basic-auth password
    """

    model_config = SettingsConfigDict(
        env_prefix="AIRFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_url: str
    username: str
    password: SecretStr

    @field_validator("api_url", mode="before")
    @classmethod
    def strip_trailing_slash(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise TypeError("api_url must be a string")
        return v.rstrip("/")


# ---------------------------------------------------------------------------
# Aggregate settings + cached accessor
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Top-level settings container.

    Composes all sub-settings so callers can import one object:

        settings = get_settings()
        settings.postgres.dsn
        settings.delta.gold_base_path
        settings.spark.jdbc_num_partitions
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres: PostgresSettings = PostgresSettings()  # type: ignore[call-arg]
    delta: DeltaSettings = DeltaSettings()  # type: ignore[call-arg]
    spark: SparkSettings = SparkSettings()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance.

    Cached after first call so environment variables are read once.
    In tests, call get_settings.cache_clear() before patching env vars.
    """
    return Settings()
