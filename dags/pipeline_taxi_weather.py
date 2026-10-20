"""DAG bulanan: ingest -> clean -> quality -> aggregate.

Satu run = satu bulan data (data interval), bukan waktu eksekusi.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

SRC = "/opt/airflow/src"
# Bulan yang diproses diturunkan dari data interval run ini, bukan hardcode
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

default_args = {
    "owner": "dayat",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_taxi_weather",
    description="Medallion: NYC Taxi + cuaca Open-Meteo, bronze -> silver -> gold",
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),  # hanya 3 bulan
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["medallion", "portfolio"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"python {SRC}/ingest.py --months {MONTH}",
    )

    clean = BashOperator(
        task_id="clean_silver",
        bash_command=f"python {SRC}/clean.py --months {MONTH}",
    )

    quality = BashOperator(
        task_id="quality_check",
        bash_command=f"python {SRC}/quality.py --months {MONTH}",
        retries=0,  # pelanggaran kualitas deterministik, retry tidak membantu
    )

    aggregate = BashOperator(
        task_id="aggregate_gold",
        bash_command=f"python {SRC}/aggregate.py",
    )

    ingest >> clean >> quality >> aggregate