# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `pyproject.toml` with uv, ruff, black, isort, mypy --strict, and pytest config
- `uv.lock` for fully reproducible installs
- `.python-version` pinning Python 3.14 for host tooling
- MIT `LICENSE`
- This `CHANGELOG.md`
- `CONTRIBUTING.md` with development setup guide
- `.pre-commit-config.yaml` with ruff, black, mypy, and trufflehog hooks

### Architecture
- Bronze layer moved from Delta Lake to Postgres (append-only, partitioned)
- Gold layer remains on Delta Lake (ACID, time travel, analytical queries)
- Silver layer (dbt) runs natively against Postgres via dbt-postgres adapter
- `pipeline/` shared package introduced; business logic extracted from DAG files
- Separate Docker images: `Dockerfile.bronze` (no JVM) and `Dockerfile.gold` (PySpark + JDBC)
- CLI added under `cli/` as a thin Airflow REST API client (Typer + httpx)
