"""
Tests for pipeline/gold/.

Covers:
    test_loader.py    — GoldLoader JDBC read, Delta MERGE, stop()
    test_run_logger.py — RunLogger, RunRecord, RunStatus

PySpark, Delta Lake, and psycopg3 are mocked — no JVM or real
Postgres required.
"""
