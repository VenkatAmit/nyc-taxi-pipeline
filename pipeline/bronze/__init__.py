"""
Bronze layer — raw ingestion into Postgres.

Responsibilities:
- Ingest raw NYC taxi source files
- Validate schema on arrival
- Bulk-insert into Postgres via psycopg3 COPY

Classes (added in PR 05 and PR 06):
- BronzeIngestor  — pipeline/bronze/ingestor.py
- GXValidator     — pipeline/bronze/validator.py
"""
