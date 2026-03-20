"""
airflow/dags/taxi_pipeline.py
------------------------------
NYC Taxi batch pipeline — medallion architecture
bronze (raw_trips) -> silver (dbt) -> gold (dbt)

Schedule: @monthly — processes one month of TLC data per run
Backfill-ready: each run is parameterised by data_interval_start
"""

from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/tasks")

from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from ingest import ingest  # noqa: E402
from load import load  # noqa: E402

default_args = {
    "owner": "venkat-amit",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description=(
        "NYC Taxi batch pipeline: "
        "ingest TLC parquet -> PySpark transform -> dbt models -> GE validation"
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "medallion", "pyspark", "dbt"],
) as dag:
    t_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        provide_context=True,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True,
        trigger_rule="all_done",
    )

    # Remaining tasks added in subsequent feature branches:
    # t_spark_transform  -> feature/spark-transform
    # t_dbt_run          -> feature/dbt-models
    # t_dbt_test         -> feature/dbt-models
    # t_ge_validate      -> feature/great-expectations

    t_ingest >> t_load
