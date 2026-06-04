"""
Пример DAG: сбор отчётов Cian Builder Reports за диапазон дат.

Пайплайн на каждую дату:
  collect → [upload_to_s3 / load_to_bq / load_to_ch] → cleanup

Добавление загрузки в хранилище:
  S3: добавьте LocalFilesystemToS3Operator между collect и cleanup.
  BigQuery: LocalFilesystemToGCSOperator → GCSToBigQueryOperator (WRITE_TRUNCATE, partition table$YYYYMMDD).
  ClickHouse: кастомный оператор DELETE + INSERT по дате.

Несколько клиентов с одного IP и всё равно 429?
  Создайте Airflow Pool "cian_api" с нужным числом слотов и передайте pool="cian_api" в partial().
"""

from __future__ import annotations

import os
from datetime import date, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow_provider_cian.operators.builder_reports import CianBuilderReportsOperator


def get_date_range(date_from: str, date_to: str) -> list[str]:
    """Возвращает список дат от date_to до date_from включительно, по убыванию."""
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    days = (end - start).days + 1
    return [(end - timedelta(days=i)).isoformat() for i in range(days)]


yesterday = date.today() - timedelta(days=1)

default_date_to = yesterday.isoformat()
default_date_from = (yesterday - timedelta(days=29)).isoformat()


@dag(
    dag_id="cian_builder_reports",
    schedule=None,
    start_date=None,
    catchup=False,
    max_active_tasks=3,
    params={
        "date_from": Param(default_date_from, type="string", description="Начальная дата (включительно), YYYY-MM-DD"),
        "date_to": Param(default_date_to, type="string", description="Конечная дата (включительно), YYYY-MM-DD"),
    },
    tags=["cian", "builder-reports"],
)
def cian_builder_reports_dag():
    @task
    def get_dates(**context) -> list[str]:
        params = context["params"]
        return get_date_range(params["date_from"], params["date_to"])

    dates = get_dates()

    collect = CianBuilderReportsOperator.partial(
        task_id="collect",
        cian_conn_id="cian_default",
        base_dir="/tmp/cian",
        output_format="json",
    ).expand(date=dates)

    def _cleanup(ti, **context):
        paths = ti.xcom_pull(task_ids="collect")
        if isinstance(paths, str):
            paths = [paths]
        for path in (paths or []):
            if path and os.path.exists(path):
                os.remove(path)

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        trigger_rule="all_done",
    )

    collect >> cleanup


cian_builder_reports_dag()
