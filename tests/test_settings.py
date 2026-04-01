"""Tests for pipeline/settings.py."""

from __future__ import annotations

import pytest

from pipeline.settings import (
    AirflowSettings,
    DeltaSettings,
    PostgresSettings,
    SparkSettings,
    get_settings,
)


# ---------------------------------------------------------------------------
# PostgresSettings
# ---------------------------------------------------------------------------


class TestPostgresSettings:
    def test_dsn_format(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        s = PostgresSettings()
        assert "host=localhost" in s.dsn
        assert "dbname=cab_pipeline" in s.dsn
        assert "secret" in s.dsn

    def test_jdbc_url_format(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        s = PostgresSettings()
        assert s.jdbc_url == "jdbc:postgresql://localhost:5432/cab_pipeline"

    def test_jdbc_properties_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        s = PostgresSettings()
        assert set(s.jdbc_properties.keys()) == {"user", "password", "driver"}
        assert s.jdbc_properties["driver"] == "org.postgresql.Driver"

    def test_custom_host_and_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_HOST", "db.internal")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        s = PostgresSettings()
        assert "host=db.internal" in s.dsn
        assert "port=5433" in s.dsn
        assert s.jdbc_url == "jdbc:postgresql://db.internal:5433/cab_pipeline"

    def test_password_is_secret_str(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "hunter2")
        s = PostgresSettings()
        assert "hunter2" not in repr(s)
        assert s.password.get_secret_value() == "hunter2"

    def test_missing_password_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
        with pytest.raises(Exception):
            PostgresSettings()


# ---------------------------------------------------------------------------
# DeltaSettings
# ---------------------------------------------------------------------------


class TestDeltaSettings:
    def test_trailing_slash_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold/")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints/")
        s = DeltaSettings()
        assert not s.gold_base_path.endswith("/")
        assert not s.checkpoint_path.endswith("/")

    def test_no_trailing_slash_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
        s = DeltaSettings()
        assert s.gold_base_path == "s3://bucket/gold"

    def test_default_aws_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
        s = DeltaSettings()
        assert s.aws_region == "us-east-1"


# ---------------------------------------------------------------------------
# SparkSettings
# ---------------------------------------------------------------------------


class TestSparkSettings:
    def test_defaults(self) -> None:
        s = SparkSettings()
        assert s.master == "local[*]"
        assert s.executor_memory == "4g"
        assert s.jdbc_num_partitions == 8
        assert s.jdbc_fetch_size == 50_000

    def test_invalid_partitions_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SPARK_JDBC_NUM_PARTITIONS", "0")
        with pytest.raises(Exception):
            SparkSettings()

    def test_custom_partitions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SPARK_JDBC_NUM_PARTITIONS", "16")
        s = SparkSettings()
        assert s.jdbc_num_partitions == 16


# ---------------------------------------------------------------------------
# AirflowSettings
# ---------------------------------------------------------------------------


class TestAirflowSettings:
    def test_trailing_slash_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIRFLOW_API_URL", "http://localhost:8080/")
        monkeypatch.setenv("AIRFLOW_USERNAME", "admin")
        monkeypatch.setenv("AIRFLOW_PASSWORD", "admin")
        s = AirflowSettings()
        assert s.api_url == "http://localhost:8080"

    def test_password_is_secret_str(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIRFLOW_API_URL", "http://localhost:8080")
        monkeypatch.setenv("AIRFLOW_USERNAME", "admin")
        monkeypatch.setenv("AIRFLOW_PASSWORD", "supersecret")
        s = AirflowSettings()
        assert "supersecret" not in repr(s)
        assert s.password.get_secret_value() == "supersecret"


# ---------------------------------------------------------------------------
# get_settings cache
# ---------------------------------------------------------------------------


class TestGetSettings:
    def test_returns_same_instance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
        get_settings.cache_clear()
        a = get_settings()
        b = get_settings()
        assert a is b

    def test_cache_clear_reloads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
        get_settings.cache_clear()
        a = get_settings()
        get_settings.cache_clear()
        b = get_settings()
        assert a is not b
