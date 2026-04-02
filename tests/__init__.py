"""
Test suite for cab-spark-data-pipeline.

Structure mirrors the pipeline/ package:

    tests/bronze/  — BronzeIngestor, GXValidator tests
    tests/gold/    — GoldLoader, RunLogger tests
    tests/cli/     — AirflowClient tests

All tests run on the host (Python 3.14) without a JVM, real Postgres,
or real Airflow instance. External dependencies are mocked via pytest-mock.

Fixtures shared across test modules live in conftest.py.
"""
