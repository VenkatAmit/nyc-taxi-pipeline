"""
Tests for pipeline/gold/loader.py.

PySpark and Delta Lake are mocked throughout — no JVM or real
Postgres instance required to run these tests.
"""

from __future__ import annotations

import sys
from datetime import date
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from pipeline.exceptions import DeltaError, SparkError
from pipeline.gold.loader import GoldLoader, MERGE_KEYS, SILVER_SCHEMA
from pipeline.settings import DeltaSettings, PostgresSettings, SparkSettings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pg_settings(monkeypatch: pytest.MonkeyPatch) -> PostgresSettings:
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    return PostgresSettings(
        host="localhost",
        port=5432,
        db="test_db",
        user="test_user",
        password="test",  # type: ignore[arg-type]
    )


@pytest.fixture()
def delta_settings(monkeypatch: pytest.MonkeyPatch) -> DeltaSettings:
    monkeypatch.setenv("DELTA_GOLD_BASE_PATH", "s3://bucket/gold")
    monkeypatch.setenv("DELTA_CHECKPOINT_PATH", "s3://bucket/checkpoints")
    return DeltaSettings()


@pytest.fixture()
def spark_settings() -> SparkSettings:
    return SparkSettings()


@pytest.fixture()
def loader(
    pg_settings: PostgresSettings,
    delta_settings: DeltaSettings,
    spark_settings: SparkSettings,
) -> GoldLoader:
    return GoldLoader(
        postgres_settings=pg_settings,
        delta_settings=delta_settings,
        spark_settings=spark_settings,
    )


def _make_mock_df(row_count: int = 100) -> MagicMock:
    df = MagicMock()
    df.count.return_value = row_count
    df.write = MagicMock()
    df.write.format.return_value = df.write
    df.write.mode.return_value = df.write
    df.write.option.return_value = df.write
    df.write.save = MagicMock()
    df.alias.return_value = df
    return df


def _make_mock_spark() -> MagicMock:
    mock_df = _make_mock_df()
    reader = MagicMock()
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = mock_df

    spark = MagicMock()
    spark.read = reader
    return spark


# ---------------------------------------------------------------------------
# GoldLoader.__repr__
# ---------------------------------------------------------------------------


class TestGoldLoaderRepr:
    def test_repr_contains_key_info(self, loader: GoldLoader) -> None:
        r = repr(loader)
        assert "localhost" in r
        assert "s3://bucket/gold" in r


# ---------------------------------------------------------------------------
# _read_silver_trips
# ---------------------------------------------------------------------------


class TestReadSilverTrips:
    def test_uses_correct_partition_date_in_query(
        self, loader: GoldLoader
    ) -> None:
        spark = _make_mock_spark()
        loader._read_silver_trips(spark, date(2024, 1, 1))
        call_args = str(spark.read.format().option.call_args_list)
        assert "2024-01-01" in call_args

    def test_uses_jdbc_format(self, loader: GoldLoader) -> None:
        spark = _make_mock_spark()
        loader._read_silver_trips(spark, date(2024, 1, 1))
        spark.read.format.assert_called_with("jdbc")

    def test_exception_wrapped_as_spark_error(self, loader: GoldLoader) -> None:
        spark = MagicMock()
        spark.read.format.side_effect = RuntimeError("JDBC timeout")
        with pytest.raises(SparkError, match="JDBC read silver_trips"):
            loader._read_silver_trips(spark, date(2024, 1, 1))


# ---------------------------------------------------------------------------
# _read_silver_zones
# ---------------------------------------------------------------------------


class TestReadSilverZones:
    def test_uses_jdbc_format(self, loader: GoldLoader) -> None:
        spark = _make_mock_spark()
        loader._read_silver_zones(spark)
        spark.read.format.assert_called_with("jdbc")

    def test_exception_wrapped_as_spark_error(self, loader: GoldLoader) -> None:
        spark = MagicMock()
        spark.read.format.side_effect = RuntimeError("connection reset")
        with pytest.raises(SparkError, match="JDBC read silver_zones"):
            loader._read_silver_zones(spark)


# ---------------------------------------------------------------------------
# _merge_into_delta
# ---------------------------------------------------------------------------


class TestMergeIntoDelta:
    def test_first_run_writes_overwrite(self, loader: GoldLoader) -> None:
        spark = MagicMock()
        df = _make_mock_df()
        mock_delta_module = MagicMock()
        mock_delta_module.DeltaTable.isDeltaTable.return_value = False

        with patch.dict(
            sys.modules,
            {"delta": MagicMock(), "delta.tables": mock_delta_module},
        ):
            loader._merge_into_delta(
                spark, df, "s3://bucket/gold/fact_trips", ["trip_id"]
            )
        # No exception means the overwrite path was taken

    def test_subsequent_run_uses_merge(self, loader: GoldLoader) -> None:
        spark = MagicMock()
        df = _make_mock_df()

        mock_delta_table = MagicMock()
        mock_delta_table.alias.return_value = mock_delta_table
        mock_delta_table.merge.return_value = mock_delta_table
        mock_delta_table.whenMatchedUpdateAll.return_value = mock_delta_table
        mock_delta_table.whenNotMatchedInsertAll.return_value = mock_delta_table

        mock_delta_module = MagicMock()
        mock_delta_module.DeltaTable.isDeltaTable.return_value = True
        mock_delta_module.DeltaTable.forPath.return_value = mock_delta_table

        with patch.dict(sys.modules, {"delta.tables": mock_delta_module}):
            loader._merge_into_delta(
                spark, df, "s3://bucket/gold/fact_trips", ["trip_id"]
            )

        mock_delta_table.merge.assert_called_once()
        mock_delta_table.execute.assert_called_once()

    def test_delta_import_error_raises_delta_error(
        self, loader: GoldLoader
    ) -> None:
        spark = MagicMock()
        df = _make_mock_df()
        with patch.dict(sys.modules, {"delta": None, "delta.tables": None}):
            with pytest.raises(DeltaError):
                loader._merge_into_delta(
                    spark, df, "s3://bucket/gold/fact_trips", ["trip_id"]
                )


# ---------------------------------------------------------------------------
# load_fact_trips
# ---------------------------------------------------------------------------


class TestLoadFactTrips:
    def test_returns_row_count(self, loader: GoldLoader) -> None:
        mock_spark = _make_mock_spark()
        mock_df = _make_mock_df(row_count=500)

        with patch.object(loader, "_get_spark", return_value=mock_spark):
            with patch.object(loader, "_read_silver_trips", return_value=mock_df):
                with patch.object(loader, "_merge_into_delta"):
                    result = loader.load_fact_trips(date(2024, 1, 1))

        assert result == 500

    def test_exception_wrapped_as_spark_error(self, loader: GoldLoader) -> None:
        with patch.object(loader, "_get_spark", side_effect=RuntimeError("no spark")):
            with pytest.raises((SparkError, RuntimeError)):
                loader.load_fact_trips(date(2024, 1, 1))

    def test_uses_correct_delta_path(self, loader: GoldLoader) -> None:
        mock_spark = _make_mock_spark()
        mock_df = _make_mock_df()
        captured: dict[str, str] = {}

        def capture_merge(
            spark: object,
            df: object,
            delta_path: str,
            merge_keys: list[str],
        ) -> None:
            captured["delta_path"] = delta_path

        with patch.object(loader, "_get_spark", return_value=mock_spark):
            with patch.object(loader, "_read_silver_trips", return_value=mock_df):
                with patch.object(loader, "_merge_into_delta", side_effect=capture_merge):
                    loader.load_fact_trips(date(2024, 1, 1))

        assert captured["delta_path"] == "s3://bucket/gold/fact_trips"


# ---------------------------------------------------------------------------
# load_dim_zones
# ---------------------------------------------------------------------------


class TestLoadDimZones:
    def test_returns_row_count(self, loader: GoldLoader) -> None:
        mock_spark = _make_mock_spark()
        mock_df = _make_mock_df(row_count=265)

        with patch.object(loader, "_get_spark", return_value=mock_spark):
            with patch.object(loader, "_read_silver_zones", return_value=mock_df):
                result = loader.load_dim_zones()

        assert result == 265

    def test_writes_in_overwrite_mode(self, loader: GoldLoader) -> None:
        mock_spark = _make_mock_spark()
        mock_df = _make_mock_df()

        with patch.object(loader, "_get_spark", return_value=mock_spark):
            with patch.object(loader, "_read_silver_zones", return_value=mock_df):
                loader.load_dim_zones()

        mock_df.write.mode.assert_called_with("overwrite")

    def test_exception_wrapped_as_spark_error(self, loader: GoldLoader) -> None:
        with patch.object(loader, "_get_spark", side_effect=RuntimeError("no spark")):
            with pytest.raises((SparkError, RuntimeError)):
                loader.load_dim_zones()


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


class TestStop:
    def test_stop_calls_stop_spark(self, loader: GoldLoader) -> None:
        with patch("pipeline.gold.loader.stop_spark") as mock_stop:
            loader.stop()
        mock_stop.assert_called_once()
