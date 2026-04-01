"""
Gold layer — business-ready Delta Lake tables.

Responsibilities:
- Read conformed data from Postgres via JDBC (partitioned read)
- Apply final aggregations and joins
- Write to Delta Lake with MERGE INTO for idempotency

Classes (added in PR 08):
- GoldLoader   — pipeline/gold/loader.py
- RunLogger    — pipeline/gold/run_logger.py
- RunRecord    — pipeline/gold/run_logger.py  (frozen dataclass)
"""
