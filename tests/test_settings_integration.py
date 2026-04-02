"""
Cross-cutting settings integration tests.

Verifies that settings compose correctly and that the lru_cache
behaves as expected across test isolation boundaries.
"""

from __future__ import annotations

import pytest

from pipeline.settings import (
    AirflowSettings,
    DeltaSettings,
    PostgresSettings,
    SparkSettings,
    get_settings,
)


class TestSettingsComposition:
    def test_postgres_dsn_uses_all_fields(
        self, postgres_settings: PostgresSettings
    ) -> None:
        dsn = postgres_settings.dsn
        assert postgres_settings.host in dsn
        assert str(postgres_settings.port) in dsn
        assert postgres_settings.db in dsn
        assert postgres_settings.user in dsn

    def test_delta_paths_have_no_trailing_slash(
        self, delta_settings: DeltaSettings
    ) -> None:
        assert not delta_settings.gold_base_path.endswith("/")
        assert not delta_settings.checkpoint_path.endswith("/")

    def test_spark_settings_positive_partitions(
        self, spark_settings: SparkSettings
    ) -> None:
        assert spark_settings.jdbc_num_partitions >= 1
        assert spark_settings.jdbc_fetch_size >= 1

    def test_airflow_settings_no_trailing_slash(
        self, airflow_settings: AirflowSettings
    ) -> None:
        assert not airflow_settings.api_url.endswith("/")

    def test_password_not_in_repr(
        self, postgres_settings: PostgresSettings
    ) -> None:
        assert "test_password" not in repr(postgres_settings)

    def test_airflow_password_not_in_repr(
        self, airflow_settings: AirflowSettings
    ) -> None:
        assert "admin" not in repr(airflow_settings)


class TestGetSettingsCache:
    def test_cache_returns_same_instance(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "test")
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
        get_settings.cache_clear()

        a = get_settings()
        b = get_settings()
        assert a is b

    def test_cache_clear_produces_new_instance(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "test")
        monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
        monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
        get_settings.cache_clear()

        a = get_settings()
        get_settings.cache_clear()
        b = get_settings()
        assert a is not b

    def teardown_method(self) -> None:
        get_settings.cache_clear()
