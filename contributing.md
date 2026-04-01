# Contributing

## Prerequisites

- [uv](https://docs.astral.sh/uv/) ≥ 0.11
- Python 3.14 (host tooling — managed by uv via `.python-version`)
- Python 3.11 (Spark workers — inside Docker only, never on host)
- Docker + Docker Compose

---

## Environment split

This repo runs two Python versions intentionally:

| Context | Python | Why |
|---|---|---|
| Host (linting, mypy, CLI) | 3.14 | Latest language features; uv manages it |
| Docker bronze worker | 3.11 | `psycopg3` + `great-expectations` |
| Docker gold worker | 3.11 | PySpark 3.4 only supports 3.8–3.11 |

Never install `pyspark` on your host. The `[spark]` extra is for Docker only.

---

## Setup

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all dev dependencies (no Spark — host only)
uv sync

# Install pre-commit hooks
uv run pre-commit install
```

---

## Running checks locally

```bash
# Lint
uv run ruff check .

# Format
uv run black --check .

# Type check
uv run mypy pipeline/ cli/

# Tests (Python 3.11 — matches Docker)
uv run --python 3.11 pytest
```

---

## PR conventions

- One concern per PR — keep diffs reviewable
- All PRs must pass CI before merge (lint + test + DAG integrity + Docker build)
- Follow the phase order in the roadmap — later phases depend on earlier ones
- No business logic in `dags/` — DAG tasks delegate to `pipeline/` Service classes
- `cli/` must not import from `pipeline/` — it speaks to Airflow over HTTP only

---

## Layer import rules

```
Orchestration (dags/)   →   Service (pipeline/bronze|silver|gold/)
                        →   Infrastructure (pipeline/settings.py, spark_session.py)

Service                 →   Infrastructure only
                        ✗   never imports from Orchestration

CLI (cli/)              →   Infrastructure (pipeline/settings.py for AirflowSettings)
                        ✗   never imports from Service or Orchestration
```

These rules are enforced by ruff's `flake8-tidy-imports` config in `pyproject.toml`.

---

## Commit message format

```
<type>(<scope>): <short summary>

Types: feat | fix | refactor | test | chore | docs
Scope: bronze | silver | gold | cli | infra | ci

Examples:
  feat(bronze): replace Delta write with psycopg3 COPY
  refactor(gold): extract GoldLoader from load.py
  chore(ci): add trufflehog to pre-commit hooks
```
