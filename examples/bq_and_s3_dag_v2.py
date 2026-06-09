"""
DAG v2 (оптимизированный): сбор Cian Builder Reports → BigQuery + S3.

Отличия от v1 (bq_and_s3_dag.py):
- collect → upload_gcs → load_bq → upload_s3 объединены в один таск process_date.
  Количество Celery task executions: 4×N → N (где N = количество дат).
  Меньше Python-процессов → меньше нагрузка на CPU.
- pool="cian_pool" ограничивает суммарную параллельность между всеми DAG-ами.
  Создай пул в Airflow UI: Admin → Pools → cian_pool, slots = 2–3.

Пайплайн:
  ensure_gcs_bucket ──┐
                       ├─→ process_date (×N дат, pool=cian_pool) → cleanup
  get_dates ──────────┘
"""

from __future__ import annotations

import os
import re
from datetime import date, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from airflow_provider_cian.operators.builder_reports import CianBuilderReportsOperator

# ── Конфигурация ──────────────────────────────────────────────────────────────

CIAN_CONN_ID = "cian_default"
BASE_DIR     = "/tmp/cian"

GCP_CONN_ID  = "google_cloud_default"
GCS_BUCKET   = "my-gcs-bucket"
GCS_PREFIX   = "cian/staging"
BQ_PROJECT   = "my-gcp-project"
BQ_DATASET   = "cian"
BQ_TABLE     = "builder_reports"

S3_CONN_ID   = "aws_default"
S3_BUCKET    = "my-s3-bucket"
S3_PREFIX    = "raw/placements/price/cian/new"

POOL         = "cian_pool"

# ── BQ schema (18 полей) ──────────────────────────────────────────────────────

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

# ── default_args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":             "analytics",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure":  False,
    "email_on_retry":    False,
}

# ── helpers ───────────────────────────────────────────────────────────────────

def safe_id(run_id: str | None) -> str:
    return re.sub(r"[^\w-]", "_", run_id or "")


def date_range(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end   = date.fromisoformat(date_to)
    days  = (end - start).days + 1
    return [(end - timedelta(days=i)).isoformat() for i in range(days)]


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="cian_to_bq_and_s3_v2",
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
    tags=["cian", "bigquery", "s3", "v2"],
)
def cian_to_bq_and_s3_v2():

    @task
    def get_dates(**context) -> list[str]:
        p = context["params"]
        return date_range(p["date_from"], p["date_to"])

    @task
    def ensure_gcs_bucket() -> None:
        client = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        try:
            bucket = client.create_bucket(GCS_BUCKET)
        except Conflict:
            bucket = client.get_bucket(GCS_BUCKET)
        bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 1}}]
        bucket.patch()

    @task(pool=POOL)
    def process_date(date: str, run_id: str | None = None) -> str:
        sid          = safe_id(run_id)
        date_compact = date.replace("-", "")
        gcs_path     = f"{GCS_PREFIX}/{sid}/{date}.json"

        # 1. Сбор данных из API → локальный файл
        local_path = CianBuilderReportsOperator(
            task_id="_collect",
            cian_conn_id=CIAN_CONN_ID,
            date=date,
            base_dir=BASE_DIR,
            output_format="json",
        ).execute({"run_id": run_id or ""})

        # 2. Локальный файл → GCS (промежуточное хранилище для BQ)
        GCSHook(gcp_conn_id=GCP_CONN_ID).upload(
            bucket_name=GCS_BUCKET,
            object_name=gcs_path,
            filename=local_path,
        )

        # 3. GCS → BigQuery (партиция table$YYYYMMDD, WRITE_TRUNCATE)
        schema = [
            bigquery.SchemaField(f["name"], f["type"], mode=f["mode"])
            for f in BQ_SCHEMA
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        BigQueryHook(gcp_conn_id=GCP_CONN_ID).get_client().load_table_from_uri(
            f"gs://{GCS_BUCKET}/{gcs_path}",
            f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}${date_compact}",
            job_config=job_config,
        ).result()

        # 4. Локальный файл → S3 (Hive-партиция)
        year, month, day = date.split("-")
        s3_key = (
            f"{S3_PREFIX}/_year={year}/_month={month}/_day={day}"
            f"/_date={date_compact}/{date}.json"
        )
        S3Hook(aws_conn_id=S3_CONN_ID).load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        return local_path

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

    # ── Execution & dependencies ──────────────────────────────────────────────

    dates        = get_dates()
    bucket_ready = ensure_gcs_bucket()
    processing   = process_date.expand(date=dates)

    bucket_ready >> processing
    cleanup(processing)


cian_to_bq_and_s3_v2()
