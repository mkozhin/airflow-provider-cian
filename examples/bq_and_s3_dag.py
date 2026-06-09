"""
DAG: сбор Cian Builder Reports → BigQuery + S3-совместимое хранилище.

Пайплайн (параллельно по датам):
  ensure_gcs_bucket → collect → upload_gcs → load_bq   ↘
                                                          cleanup
                               upload_s3               ↗

collect пишет JSONL за каждую дату локально.
upload_gcs кладёт файл в GCS (промежуточное хранилище для BQ).
load_bq загружает из GCS в BigQuery (партиция table$YYYYMMDD, WRITE_TRUNCATE).
  Таблица создаётся автоматически (CREATE_IF_NEEDED) с партиционированием по `date` (DAY).
upload_s3 кладёт файл в S3-совместимое хранилище в Hive-партиции:
  {S3_PREFIX}/_year={YYYY}/_month={MM}/_day={DD}/_date={YYYYMMDD}/{date}.json
cleanup удаляет директорию запуска со всеми файлами после завершения всех загрузок.
"""

from __future__ import annotations

import os
import re
from datetime import date, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from google.api_core.exceptions import Conflict

from airflow_provider_cian.operators.builder_reports import CianBuilderReportsOperator

# ── Конфигурация — каждый клиент получает свой DAG-файл с собственными значениями

CIAN_CONN_ID = "cian_default"
BASE_DIR     = "/tmp/cian"

GCP_CONN_ID  = "google_cloud_default"
GCS_BUCKET   = "my-gcs-bucket"
GCS_PREFIX   = "cian/staging"
BQ_PROJECT   = "my-gcp-project"
BQ_DATASET   = "cian"
BQ_TABLE     = "builder_reports"

S3_CONN_ID   = "aws_default"
S3_BUCKET    = "project-osnova"
S3_PREFIX    = "raw/placements/price/cian/new"

# ── BQ schema (18 полей) ──────────────────────────────────────────────────────
# datetime — TIMESTAMP: BQ принимает ISO 8601 с offset, хранит в UTC.
# В запросах: DATETIME(datetime, 'Europe/Moscow')

BQ_SCHEMA = [
    {"name": "id",                    "type": "STRING",    "mode": "NULLABLE"},
    {"name": "newbuilding_id",        "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "newbuilding_name",      "type": "STRING",    "mode": "NULLABLE"},
    {"name": "date",                  "type": "DATE",      "mode": "NULLABLE"},
    {"name": "datetime",              "type": "STRING",    "mode": "NULLABLE"},
    {"name": "action_type",           "type": "STRING",    "mode": "NULLABLE"},
    {"name": "searcher_phone",        "type": "STRING",    "mode": "NULLABLE"},
    {"name": "searcher_ct_phone",     "type": "STRING",    "mode": "NULLABLE"},
    {"name": "builder_user_ct_phone", "type": "STRING",    "mode": "NULLABLE"},
    {"name": "builder_user_phone",    "type": "STRING",    "mode": "NULLABLE"},
    {"name": "builder_sip_uri",       "type": "STRING",    "mode": "NULLABLE"},
    {"name": "call_duration",         "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "tariff_price",          "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "auction_bet",           "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "cashback_spent",        "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "billing_price",         "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "has_claim",             "type": "BOOLEAN",   "mode": "NULLABLE"},
    {"name": "is_targeted",           "type": "BOOLEAN",   "mode": "NULLABLE"},
]

# ── default_args — применяются ко всем тасками DAG ───────────────────────────

DEFAULT_ARGS = {
    "owner":             "analytics",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── helpers ───────────────────────────────────────────────────────────────────

def safe_id(run_id: str | None) -> str:
    return re.sub(r"[^\w-]", "_", run_id or "")


def date_range(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end   = date.fromisoformat(date_to)
    days  = (end - start).days + 1
    if days <= 0:
        raise AirflowException(
            f"date_from ({date_from}) must be <= date_to ({date_to})"
        )
    return [(end - timedelta(days=i)).isoformat() for i in range(days)]


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="cian_to_bq_and_s3",
    doc_md=__doc__,
    schedule=None,
    start_date=None,
    catchup=False,
    max_active_tasks=5,
    default_args=DEFAULT_ARGS,
    params={
        "date_from": Param(
            (date.today() - timedelta(days=30)).isoformat(),
            type="string",
            description="Начальная дата (включительно), YYYY-MM-DD",
        ),
        "date_to": Param(
            (date.today() - timedelta(days=1)).isoformat(),
            type="string",
            description="Конечная дата (включительно), YYYY-MM-DD",
        ),
    },
    tags=["cian", "bigquery", "s3"],
)
def cian_to_bq_and_s3():

    # ── Task definitions ──────────────────────────────────────────────────────

    @task
    def get_dates(**context) -> list[str]:
        p = context["params"]
        return date_range(p["date_from"], p["date_to"])

    @task
    def make_gcs_params(paths: list[str], dates: list[str], run_id: str | None = None) -> list[dict]:
        sid = safe_id(run_id)
        return [
            {"src": path, "dst": f"{GCS_PREFIX}/{sid}/{d}.json"}
            for path, d in zip(paths, dates)
        ]

    @task
    def make_bq_params(dates: list[str], run_id: str | None = None) -> list[dict]:
        sid = safe_id(run_id)
        return [
            {
                "source_objects":                    [f"{GCS_PREFIX}/{sid}/{d}.json"],
                "destination_project_dataset_table": f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}${d.replace('-', '')}",
            }
            for d in dates
        ]

    @task
    def make_s3_params(paths: list[str], dates: list[str]) -> list[dict]:
        params = []
        for path, d in zip(paths, dates):
            year, month, day = d.split("-")
            date_compact = d.replace("-", "")
            params.append({
                "filename": path,
                "dest_key": f"{S3_PREFIX}/_year={year}/_month={month}/_day={day}/_date={date_compact}/{d}.json",
            })
        return params

    @task
    def ensure_gcs_bucket() -> None:
        client = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        try:
            bucket = client.create_bucket(GCS_BUCKET)
        except Conflict:
            bucket = client.get_bucket(GCS_BUCKET)
        bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 1}}]
        bucket.patch()

    @task(trigger_rule="all_done")
    def cleanup(paths: list[str]) -> None:
        if not paths:
            return
        run_dir = os.path.dirname(paths[0])
        if not os.path.isdir(run_dir):
            return
        for fname in os.listdir(run_dir):
            os.remove(os.path.join(run_dir, fname))
        os.rmdir(run_dir)

    # ── Operator definitions (partial — без вызова) ───────────────────────────

    collect    = CianBuilderReportsOperator.partial(
        task_id="collect",
        cian_conn_id=CIAN_CONN_ID,
        base_dir=BASE_DIR,
        output_format="json",
    )

    upload_gcs = LocalFilesystemToGCSOperator.partial(
        task_id="upload_gcs",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
    )

    load_bq    = GCSToBigQueryOperator.partial(
        task_id="load_bq",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        schema_fields=BQ_SCHEMA,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY", "field": "date"},
    )

    upload_s3  = LocalFilesystemToS3Operator.partial(
        task_id="upload_s3",
        aws_conn_id=S3_CONN_ID,
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    # ── Execution & dependencies ──────────────────────────────────────────────

    dates      = get_dates()
    paths      = collect.expand(date=dates).output   # XComArg, не MappedOperator
    gcs_params = make_gcs_params(paths, dates)
    bq_params  = make_bq_params(dates)
    s3_params  = make_s3_params(paths, dates)

    bucket_ready = ensure_gcs_bucket()
    gcs_done     = upload_gcs.expand_kwargs(gcs_params)
    bq_done      = load_bq.expand_kwargs(bq_params)
    s3_done      = upload_s3.expand_kwargs(s3_params)

    bucket_ready >> gcs_done >> bq_done      # сначала бакет, потом загрузка, потом BQ
    [bq_done, s3_done] >> cleanup(paths)     # папку удаляем после завершения всех загрузок


cian_to_bq_and_s3()
