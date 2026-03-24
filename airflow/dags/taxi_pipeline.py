"""
airflow/dags/taxi_pipeline.py
------------------------------
NYC Taxi batch pipeline — medallion architecture
bronze (Delta) -> silver (cleaned_trips) -> gold (dbt) -> validation (GE)

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
from spark_transform import spark_transform  # noqa: E402
from delta_optimize import delta_optimize  # noqa: E402
from dbt_run import dbt_run  # noqa: E402
from gx_validate import gx_validate  # noqa: E402

default_args = {
    "owner": "venkat-amit",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description=(
        "NYC Taxi batch pipeline: "
        "ingest TLC parquet -> Delta bronze -> PySpark silver "
        "-> delta_optimize -> dbt gold -> GE validation"
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 12, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["nyc-taxi", "medallion", "pyspark", "dbt", "great-expectations", "delta"],
) as dag:
    t_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    t_spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=spark_transform,
    )

    t_delta_optimize = PythonOperator(
        task_id="delta_optimize",
        python_callable=delta_optimize,
    )

    t_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_run,
    )

    t_gx_validate = PythonOperator(
        task_id="gx_validate",
        python_callable=gx_validate,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        trigger_rule="all_done",
    )

    (
        t_ingest
        >> t_spark_transform
        >> t_delta_optimize
        >> t_dbt_run
        >> t_gx_validate
        >> t_load
    )
