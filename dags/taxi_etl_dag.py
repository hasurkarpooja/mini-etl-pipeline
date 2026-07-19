"""
Airflow DAG: nyc_taxi_etl

Orchestrates the extract -> transform -> load pipeline for NYC Yellow Taxi
trip data. Runs monthly, retries on failure, and logs each stage.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from extract import download_month  # noqa: E402
from transform import run as run_transform  # noqa: E402
from load import load_hourly_zone_fares  # noqa: E402


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="nyc_taxi_etl",
    default_args=default_args,
    description="End-to-end batch ETL for NYC Yellow Taxi trip data",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "pyspark", "portfolio"],
) as dag:

    def _extract(**context):
        month = context["ds"][:7]  # YYYY-MM
        download_month(month)

    def _transform(**context):
        month = context["ds"][:7]
        run_transform(
            input_path=f"data/raw/{month}.parquet",
            output_path=f"data/curated/{month}",
        )

    def _load(**context):
        month = context["ds"][:7]
        load_hourly_zone_fares(f"data/curated/{month}")

    extract_task = PythonOperator(task_id="extract", python_callable=_extract)
    transform_task = PythonOperator(task_id="transform", python_callable=_transform)
    load_task = PythonOperator(task_id="load", python_callable=_load)

    extract_task >> transform_task >> load_task
