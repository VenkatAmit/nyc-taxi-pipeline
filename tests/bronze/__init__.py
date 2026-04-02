"""
Tests for pipeline/bronze/.

Covers:
    test_ingestor.py  — BronzeIngestor schema validation, COPY logic,
                        IngestResult, psycopg3 error wrapping
    test_validator.py — GXValidator Expectation ABC, ExpectationResult,
                        ValidationReport, built-in expectations

psycopg3 and Great Expectations are mocked — no real Postgres required.
"""
