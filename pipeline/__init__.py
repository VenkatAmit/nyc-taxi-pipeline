"""
cab-spark-data-pipeline — shared pipeline package.

Layer import rules (enforced by ruff flake8-tidy-imports):

    Orchestration (dags/)
        → Service     (pipeline/bronze/, pipeline/silver/, pipeline/gold/)
        → Infra       (pipeline/settings.py, pipeline/exceptions.py,
                       pipeline/spark_session.py)

    Service
        → Infra only
        ✗ never imports from Orchestration

    CLI (cli/)
        → Infra only  (pipeline/settings.py for AirflowSettings)
        ✗ never imports from Service or Orchestration
"""
