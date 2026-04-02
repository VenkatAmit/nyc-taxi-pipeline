"""
Bronze validation layer — Great Expectations against Postgres.

GXValidator runs data quality checks directly on the raw_trips and
raw_zones tables in Postgres using GX's SqlAlchemyDatasource. This
replaces the previous Spark batch datasource — no SparkSession is
needed at validation time.

Design
------
- Strategy pattern via Expectation ABC: each business rule is a
  self-contained class with a name and a run() method. New rules
  are added by subclassing Expectation — GXValidator never changes.
- Dependency injection for PostgresSettings — testable without a
  real Postgres instance.
- ValidationReport is a frozen dataclass: serialisable, hashable,
  safe to log and pass between Airflow tasks.
- All GX internals are imported lazily inside methods so the module
  can be imported on any environment even if great-expectations is
  not installed (it will raise ConfigurationError at runtime, not
  ImportError at module load).

Expectation catalogue
---------------------
Built-in expectations shipped with this module:

    TripsNotNullExpectation      — critical columns are not null
    TripsPositiveAmountExpectation — fare/tip/total > 0
    TripsDateRangeExpectation    — pickup within expected date range
    ZonesLocationIdExpectation   — location IDs are positive integers
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from pipeline.exceptions import (
    ConfigurationError,
    ExpectationFailedError,
    ValidationError,
)
from pipeline.settings import PostgresSettings, get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Expectation ABC — Strategy pattern
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExpectationResult:
    """Outcome of a single expectation run."""

    expectation_name: str
    success: bool
    observed_value: Any = None
    failure_count: int = 0
    details: str = ""


class Expectation(ABC):
    """Abstract base class for a single data quality rule.

    Subclass this to add new expectations. Each subclass is responsible
    for a single, well-named rule. GXValidator accepts a list of
    Expectation instances and runs them in order.

    Example
    -------
    ::

        class MyExpectation(Expectation):
            @property
            def name(self) -> str:
                return "my_custom_check"

            def run(
                self,
                validator: Any,
                table: str,
                partition_date: date | None,
            ) -> ExpectationResult:
                result = validator.expect_column_values_to_not_be_null(
                    column="my_column"
                )
                return ExpectationResult(
                    expectation_name=self.name,
                    success=result["success"],
                    failure_count=result["result"].get("unexpected_count", 0),
                )
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique human-readable name for this expectation."""

    @abstractmethod
    def run(
        self,
        validator: Any,
        table: str,
        partition_date: date | None,
    ) -> ExpectationResult:
        """Execute the expectation against the GX validator.

        Parameters
        ----------
        validator:
            A Great Expectations Validator object already pointed at
            the target table / batch.
        table:
            Name of the Postgres table being validated.
        partition_date:
            The logical date of the partition under validation,
            or None for dimension tables (e.g. raw_zones).

        Returns
        -------
        ExpectationResult
        """


# ---------------------------------------------------------------------------
# Built-in expectations
# ---------------------------------------------------------------------------


class TripsNotNullExpectation(Expectation):
    """Critical trips columns must not be null."""

    CRITICAL_COLUMNS: tuple[str, ...] = (
        "pickup_datetime",
        "dropoff_datetime",
        "pu_location_id",
        "do_location_id",
        "total_amount",
    )

    @property
    def name(self) -> str:
        return "trips_critical_columns_not_null"

    def run(
        self,
        validator: Any,
        table: str,
        partition_date: date | None,
    ) -> ExpectationResult:
        total_failures = 0
        for col in self.CRITICAL_COLUMNS:
            result = validator.expect_column_values_to_not_be_null(column=col)
            total_failures += result["result"].get("unexpected_count", 0)

        return ExpectationResult(
            expectation_name=self.name,
            success=total_failures == 0,
            failure_count=total_failures,
            details=f"checked columns: {', '.join(self.CRITICAL_COLUMNS)}",
        )


class TripsPositiveAmountExpectation(Expectation):
    """fare_amount, tip_amount, total_amount must be >= 0."""

    AMOUNT_COLUMNS: tuple[str, ...] = ("fare_amount", "tip_amount", "total_amount")

    @property
    def name(self) -> str:
        return "trips_amounts_non_negative"

    def run(
        self,
        validator: Any,
        table: str,
        partition_date: date | None,
    ) -> ExpectationResult:
        total_failures = 0
        for col in self.AMOUNT_COLUMNS:
            result = validator.expect_column_values_to_be_between(
                column=col,
                min_value=0,
                mostly=0.99,  # allow 1% outliers (refunds, corrections)
            )
            total_failures += result["result"].get("unexpected_count", 0)

        return ExpectationResult(
            expectation_name=self.name,
            success=total_failures == 0,
            failure_count=total_failures,
            details=f"checked columns: {', '.join(self.AMOUNT_COLUMNS)}",
        )


class TripsDateRangeExpectation(Expectation):
    """pickup_datetime must fall within the expected partition month."""

    @property
    def name(self) -> str:
        return "trips_pickup_within_partition_month"

    def run(
        self,
        validator: Any,
        table: str,
        partition_date: date | None,
    ) -> ExpectationResult:
        if partition_date is None:
            return ExpectationResult(
                expectation_name=self.name,
                success=True,
                details="skipped — no partition_date provided",
            )

        # Build month boundaries
        import calendar
        from datetime import datetime

        year, month = partition_date.year, partition_date.month
        last_day = calendar.monthrange(year, month)[1]
        min_dt = datetime(year, month, 1)
        max_dt = datetime(year, month, last_day, 23, 59, 59)

        result = validator.expect_column_values_to_be_between(
            column="pickup_datetime",
            min_value=str(min_dt),
            max_value=str(max_dt),
            mostly=0.98,  # 2% tolerance for cross-midnight edge cases
        )
        failure_count = result["result"].get("unexpected_count", 0)

        return ExpectationResult(
            expectation_name=self.name,
            success=result["success"],
            failure_count=failure_count,
            details=f"expected range: {min_dt} → {max_dt}",
        )


class ZonesLocationIdExpectation(Expectation):
    """location_id must be a positive integer."""

    @property
    def name(self) -> str:
        return "zones_location_id_positive"

    def run(
        self,
        validator: Any,
        table: str,
        partition_date: date | None,
    ) -> ExpectationResult:
        result = validator.expect_column_values_to_be_between(
            column="location_id",
            min_value=1,
        )
        failure_count = result["result"].get("unexpected_count", 0)

        return ExpectationResult(
            expectation_name=self.name,
            success=result["success"],
            failure_count=failure_count,
        )


# ---------------------------------------------------------------------------
# Default expectation suites
# ---------------------------------------------------------------------------

DEFAULT_TRIPS_EXPECTATIONS: tuple[Expectation, ...] = (
    TripsNotNullExpectation(),
    TripsPositiveAmountExpectation(),
    TripsDateRangeExpectation(),
)

DEFAULT_ZONES_EXPECTATIONS: tuple[Expectation, ...] = (ZonesLocationIdExpectation(),)


# ---------------------------------------------------------------------------
# ValidationReport
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationReport:
    """Aggregated result of a full validation run."""

    table: str
    partition_date: date | None
    results: tuple[ExpectationResult, ...]
    success: bool = field(init=False)

    def __post_init__(self) -> None:
        # frozen dataclass — use object.__setattr__ to set derived field
        object.__setattr__(self, "success", all(r.success for r in self.results))

    @property
    def failed_expectations(self) -> list[ExpectationResult]:
        return [r for r in self.results if not r.success]

    @property
    def total_failure_count(self) -> int:
        return sum(r.failure_count for r in self.results)

    def raise_if_failed(self) -> None:
        """Raise ExpectationFailedError if any expectation failed.

        Convenience method for Airflow tasks that want to fail the task
        on validation failure rather than handle the report manually.
        """
        if not self.success:
            failed = self.failed_expectations[0]
            raise ExpectationFailedError(
                failed.expectation_name,
                failed.failure_count,
            )


# ---------------------------------------------------------------------------
# GXValidator
# ---------------------------------------------------------------------------


class GXValidator:
    """Runs Great Expectations checks against Postgres bronze tables.

    Parameters
    ----------
    settings:
        PostgresSettings instance. Defaults to get_settings().postgres.
    expectations:
        Sequence of Expectation instances to run. Pass an explicit list
        to override the defaults for a specific table.

    Examples
    --------
    Typical Airflow task usage::

        validator = GXValidator()
        report = validator.validate_trips(partition_date=date(2024, 1, 1))
        report.raise_if_failed()
    """

    def __init__(
        self,
        settings: PostgresSettings | None = None,
        expectations: Sequence[Expectation] | None = None,
    ) -> None:
        self._settings = settings or get_settings().postgres
        self._expectations = expectations  # None → use table defaults

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_trips(
        self,
        partition_date: date | None = None,
        expectations: Sequence[Expectation] | None = None,
    ) -> ValidationReport:
        """Validate the raw_trips table for a given partition date.

        Parameters
        ----------
        partition_date:
            Restrict validation to this partition. If None, the full
            table is validated (useful for backfill checks).
        expectations:
            Override the instance-level expectations for this call.

        Returns
        -------
        ValidationReport

        Raises
        ------
        ConfigurationError
            If great-expectations is not installed.
        ValidationError
            If GX raises an unexpected internal error.
        """
        suite = expectations or self._expectations or DEFAULT_TRIPS_EXPECTATIONS
        return self._run(
            table="raw_trips",
            partition_date=partition_date,
            expectations=suite,
        )

    def validate_zones(
        self,
        expectations: Sequence[Expectation] | None = None,
    ) -> ValidationReport:
        """Validate the raw_zones table.

        Returns
        -------
        ValidationReport
        """
        suite = expectations or self._expectations or DEFAULT_ZONES_EXPECTATIONS
        return self._run(
            table="raw_zones",
            partition_date=None,
            expectations=suite,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run(
        self,
        table: str,
        partition_date: date | None,
        expectations: Sequence[Expectation],
    ) -> ValidationReport:
        """Build a GX validator and execute all expectations."""
        gx_validator = self._build_gx_validator(table, partition_date)
        results: list[ExpectationResult] = []

        for expectation in expectations:
            try:
                result = expectation.run(gx_validator, table, partition_date)
                results.append(result)
                status = "PASS" if result.success else "FAIL"
                logger.info(
                    "[%s] %s — failures: %d",
                    status,
                    result.expectation_name,
                    result.failure_count,
                )
            except Exception as exc:
                raise ValidationError(
                    f"Expectation {expectation.name!r} raised unexpectedly",
                    cause=exc,
                ) from exc

        report = ValidationReport(
            table=table,
            partition_date=partition_date,
            results=tuple(results),
        )
        logger.info(
            "Validation %s for %s [partition=%s]: %d/%d passed",
            "PASSED" if report.success else "FAILED",
            table,
            partition_date,
            sum(1 for r in results if r.success),
            len(results),
        )
        return report

    def _build_gx_validator(
        self,
        table: str,
        partition_date: date | None,
    ) -> Any:
        """Construct a GX Validator pointed at the target Postgres table.

        Uses SqlAlchemyDatasource — no Spark required.
        """
        try:
            import great_expectations as gx
        except ImportError as exc:
            raise ConfigurationError(
                "great-expectations is not installed.",
                cause=exc,
            ) from exc

        try:
            context = gx.get_context()

            connection_string = (
                f"postgresql+psycopg2://"
                f"{self._settings.user}:"
                f"{self._settings.password.get_secret_value()}"
                f"@{self._settings.host}:{self._settings.port}"
                f"/{self._settings.db}"
            )

            datasource = context.sources.add_or_update_sql(  # type: ignore[attr-defined]
                name="postgres_bronze",
                connection_string=connection_string,
            )

            # Build a batch query — filter by partition_date if provided
            if partition_date is not None:
                query = (
                    f"SELECT * FROM {table} WHERE partition_date = '{partition_date}'"
                )
            else:
                query = f"SELECT * FROM {table}"

            asset = datasource.add_query_asset(
                name=f"{table}_asset",
                query=query,
            )
            batch_request = asset.build_batch_request()
            suite = context.add_or_update_expectation_suite(  # type: ignore[attr-defined]
                expectation_suite_name=f"{table}_suite"
            )
            return context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite,
            )
        except Exception as exc:
            raise ValidationError(
                f"Failed to build GX validator for {table!r}",
                cause=exc,
            ) from exc

    def __repr__(self) -> str:
        return (
            f"GXValidator("
            f"host={self._settings.host!r}, "
            f"db={self._settings.db!r}, "
            f"expectations={len(self._expectations or [])})"
        )
