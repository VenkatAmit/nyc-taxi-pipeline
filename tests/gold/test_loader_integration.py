"""
GoldLoader integration tests using shared conftest fixtures.

Uses the standardised mock_spark_session, mock_dataframe, and
mock_delta_module fixtures from conftest.py.
"""

from __future__ import annotations

import sys
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from pipeline.exceptions import DeltaError, SparkError
from pipeline.gold.loader import GoldLoader
from pipeline.settings import DeltaSettings, PostgresSettings, SparkSettings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def loader(
    postgres_settings: PostgresSettings,
    delta_settings: DeltaSettings,
    spark_settings: SparkSettings,
) -> GoldLoader:
    return GoldLoader(
        postgres_settings=postgres_settings,
        delta_settings=delta_settings,
        spark_settings=spark_settings,
    )


# ---------------------------------------------------------------------------
# load_fact_trips
# ---------------------------------------------------------------------------


class TestLoadFactTripsIntegration:
    def test_load_fact_trips_returns_row_count(
        self,
        loader: GoldLoader,
        mock_spark_session: MagicMock,
        mock_dataframe: MagicMock,
        mock_delta_module: MagicMock,
        sample_partition_date: date,
    ) -> None:
        with patch.object(loader, "_get_spark", return_value=mock_spark_session):
            with patch.object(loader, "_read_silver_trips", return_value=mock_dataframe):
                with patch.dict(sys.modules, {"delta.tables": mock_delta_module}):
                    with patch.object(loader, "_merge_into_delta"):
                        result = loader.load_fact_trips(sample_partition_date)

        assert result == 1000

    def test_load_fact_trips_calls_merge(
        self,
        loader: GoldLoader,
        mock_spark_session: MagicMock,
        mock_dataframe: MagicMock,
        sample_partition_date: date,
    ) -> None:
        merge_called_with: dict[str, object] = {}

        def capture(**kwargs: object) -> None:
            merge_called_with.update(kwargs)

        with patch.object(loader, "_get_spark", return_value=mock_spark_session):
            with patch.object(loader, "_read_silver_trips", return_value=mock_dataframe):
                with patch.object(loader, "_merge_into_delta") as mock_merge:
                    loader.load_fact_trips(sample_partition_date)

        mock_merge.assert_called_once()

    def test_load_fact_trips_uses_correct_delta_path(
        self,
        loader: GoldLoader,
        mock_spark_session: MagicMock,
        mock_dataframe: MagicMock,
        delta_settings: DeltaSettings,
        sample_partition_date: date,
    ) -> None:
        captured_path: list[str] = []

        def capture_path(
            spark: object,
            df: object,
            delta_path: str,
            merge_keys: list[str],
        ) -> None:
            captured_path.append(delta_path)

        with patch.object(loader, "_get_spark", return_value=mock_spark_session):
            with patch.object(loader, "_read_silver_trips", return_value=mock_dataframe):
                with patch.object(loader, "_merge_into_delta", side_effect=capture_path):
                    loader.load_fact_trips(sample_partition_date)

        assert captured_path[0] == f"{delta_settings.gold_base_path}/fact_trips"

    def test_load_fact_trips_spark_error_on_failure(
        self,
        loader: GoldLoader,
        sample_partition_date: date,
    ) -> None:
        with patch.object(
            loader, "_get_spark", side_effect=RuntimeError("no spark")
        ):
            with pytest.raises(SparkError):
                loader.load_fact_trips(sample_partition_date)


# ---------------------------------------------------------------------------
# load_dim_zones
# ---------------------------------------------------------------------------


class TestLoadDimZonesIntegration:
    def test_load_dim_zones_returns_row_count(
        self,
        loader: GoldLoader,
        mock_spark_session: MagicMock,
        mock_dataframe: MagicMock,
    ) -> None:
        with patch.object(loader, "_get_spark", return_value=mock_spark_session):
            with patch.object(loader, "_read_silver_zones", return_value=mock_dataframe):
                result = loader.load_dim_zones()

        assert result == 1000

    def test_load_dim_zones_writes_overwrite_mode(
        self,
        loader: GoldLoader,
        mock_spark_session: MagicMock,
        mock_dataframe: MagicMock,
    ) -> None:
        with patch.object(loader, "_get_spark", return_value=mock_spark_session):
            with patch.object(loader, "_read_silver_zones", return_value=mock_dataframe):
                loader.load_dim_zones()

        mock_dataframe.write.mode.assert_called_with("overwrite")

    def test_load_dim_zones_spark_error_on_failure(
        self, loader: GoldLoader
    ) -> None:
        with patch.object(
            loader, "_get_spark", side_effect=RuntimeError("no spark")
        ):
            with pytest.raises(SparkError):
                loader.load_dim_zones()


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


class TestGoldLoaderStop:
    def test_stop_calls_stop_spark(self, loader: GoldLoader) -> None:
        with patch("pipeline.gold.loader.stop_spark") as mock_stop:
            loader.stop()
        mock_stop.assert_called_once()

    def test_repr_contains_settings(
        self,
        loader: GoldLoader,
        postgres_settings: PostgresSettings,
        delta_settings: DeltaSettings,
    ) -> None:
        r = repr(loader)
        assert postgres_settings.host in r
        assert delta_settings.gold_base_path in r
