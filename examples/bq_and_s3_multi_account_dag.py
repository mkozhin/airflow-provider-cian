"""
DAG: сбор Cian Builder Reports для нескольких кабинетов → BigQuery + S3.

## Структура

```
get_dates ──┐
            ├─→ [cabinet_{id}] TaskGroup (по одному на каждый аккаунт)
ensure_gcs_bucket ─┘
```

Внутри каждого `TaskGroup` (`cabinet_{cabinet_id}`):

```
collect (×N дат) → upload_gcs → load_bq
                 ↘ upload_s3
                                → cleanup
```

## Каждый кабинет

- хранит файлы в отдельной папке: `{BASE_DIR}/{cabinet_id}/{run_id}/{date}.json`
- кладёт промежуточные файлы в GCS: `{GCS_PREFIX}/{cabinet_id}/{run_id}/{date}.json`
- загружает в отдельную BQ таблицу: `{BQ_TABLE}_{cabinet_id}${YYYYMMDD}`
- кладёт в S3: `{S3_PREFIX}/{cabinet_id}/_year=.../_month=.../_day=.../_date=.../{date}.json`
- чистит только свою папку после завершения загрузок

## Пустой день

collect за день без данных возвращает None и не пишет XCom, поэтому mapped-таски
заливки (upload_gcs/upload_s3/load_bq) внутри кабинета разворачиваются только за
дни с данными. Если у кабинета пуст весь период, его агрегаторы получают None,
возвращают [], и upload_*/load_bq разворачиваются в ноль инстансов — Airflow
помечает их skipped, а dag_run остаётся success. Кабинеты независимы: пустой
кабинет не подавляет заливки соседних кабинетов с данными.

## Формат extra в Airflow-коннекторе

```json
{"accounts": [{"id": "123", "token": "abc..."}, {"id": "456", "token": "def..."}]}
```

При пустом или отсутствующем коннекторе (`list_accounts` возвращает `[]`):
DAG импортируется без TaskGroup-ов и без ошибок.

## Параллелизм

`pool=POOL` в `default_args` ограничивает суммарную параллельность по всем DAG-ам.
`max_active_tasks` ограничивает параллельность внутри одного DAG-рана.
"""

import os
import shutil
from datetime import date, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow_provider_cian.accounts import Account, list_accounts, sanitize_id
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

POOL             = "cian_pool"
MAX_ACTIVE_TASKS = 5

# ── BQ schema (20 полей, включая snapshot_ts) ────────────────────────────────

BQ_SCHEMA = [
    {"name": "id",                    "type": "STRING",    "mode": "NULLABLE"},
    {"name": "account_id",            "type": "STRING",    "mode": "NULLABLE"},
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
    {"name": "snapshot_ts",           "type": "STRING",    "mode": "NULLABLE"},
]

# ── default_args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":             "example",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "pool":              POOL,
}

# ── helpers ───────────────────────────────────────────────────────────────────

def date_range(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end   = date.fromisoformat(date_to)
    days  = (end - start).days + 1
    if days <= 0:
        raise ValueError(f"date_from ({date_from}) must be <= date_to ({date_to})")
    return [(end - timedelta(days=i)).isoformat() for i in range(days)]


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="cian_to_bq_and_s3_multi_account",
    doc_md=__doc__,
    schedule=None,
    start_date=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
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
    tags=["cian", "bigquery", "s3", "multi-account"],
)
def cian_to_bq_and_s3_multi_account():

    @task
    def get_dates(**context) -> list[str]:
        p = context["params"]
        return date_range(p["date_from"], p["date_to"])

    @task
    def ensure_gcs_bucket() -> None:
        client = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        bucket = client.bucket(GCS_BUCKET)
        if not bucket.exists():
            bucket = client.create_bucket(GCS_BUCKET)
        bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 1}}]
        bucket.patch()

    @task
    def make_gcs_params(items: list[dict] | None, cabinet_id: str, **context) -> list[dict]:
        items = list(items or [])   # None, когда XCom не записал ни один день (весь период пуст)
        if not items:
            return []               # ранний выход ДО чтения context["run_id"]: прямой вызов
        sid = sanitize_id(context["run_id"])   # из теста без контекста иначе даст KeyError
        return [
            {"src": it["path"], "dst": f"{GCS_PREFIX}/{cabinet_id}/{sid}/{it['date']}.json"}
            for it in items
        ]

    @task
    def make_bq_params(items: list[dict] | None, cabinet_id: str, **context) -> list[dict]:
        items = list(items or [])   # строим из items, а не из dates — иначе load_bq
        if not items:               # пошёл бы за GCS-объектом, которого нет за пустой день
            return []               # ранний выход ДО чтения context["run_id"]
        sid = sanitize_id(context["run_id"])
        return [
            {
                "source_objects":                    [f"{GCS_PREFIX}/{cabinet_id}/{sid}/{it['date']}.json"],
                "destination_project_dataset_table": (
                    f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_{cabinet_id}${it['date'].replace('-', '')}"
                ),
            }
            for it in items
        ]

    @task
    def make_s3_params(items: list[dict] | None, cabinet_id: str) -> list[dict]:
        items = list(items or [])
        params = []
        for it in items:
            d = it["date"]
            year, month, day = d.split("-")
            date_compact = d.replace("-", "")
            params.append({
                "filename": it["path"],
                "dest_key": (
                    f"{S3_PREFIX}/{cabinet_id}"
                    f"/_year={year}/_month={month}/_day={day}"
                    f"/_date={date_compact}/{d}.json"
                ),
            })
        return params

    @task(trigger_rule="all_done")
    def cleanup(items: list[dict] | None, cabinet_id: str, **context) -> None:
        if not items:
            return
        sid = sanitize_id(context["run_id"])
        run_dir = os.path.join(BASE_DIR, cabinet_id, sid)
        if not os.path.isdir(run_dir):
            return
        shutil.rmtree(run_dir)

    def make_cabinet_group(account: Account, dates, bucket_ready) -> None:
        cab_id = account.id
        with TaskGroup(group_id=f"cabinet_{cab_id}"):
            collect = CianBuilderReportsOperator.partial(
                task_id="collect",
                cian_conn_id=CIAN_CONN_ID,
                base_dir=BASE_DIR,
                output_format="json",
                account_id=cab_id,
                add_snapshot_ts=True,
            )
            upload_gcs = LocalFilesystemToGCSOperator.partial(
                task_id="upload_gcs",
                gcp_conn_id=GCP_CONN_ID,
                bucket=GCS_BUCKET,
            )
            load_bq = GCSToBigQueryOperator.partial(
                task_id="load_bq",
                gcp_conn_id=GCP_CONN_ID,
                bucket=GCS_BUCKET,
                schema_fields=BQ_SCHEMA,
                source_format="NEWLINE_DELIMITED_JSON",
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning={"type": "DAY", "field": "date"},
            )
            upload_s3 = LocalFilesystemToS3Operator.partial(
                task_id="upload_s3",
                aws_conn_id=S3_CONN_ID,
                dest_bucket=S3_BUCKET,
                replace=True,
            )

            collected  = collect.expand(date=dates).output   # XComArg — список {"date","path"} только за дни с данными
            gcs_params = make_gcs_params(collected, cabinet_id=cab_id)
            bq_params  = make_bq_params(collected, cabinet_id=cab_id)
            s3_params  = make_s3_params(collected, cabinet_id=cab_id)

            gcs_done = upload_gcs.expand_kwargs(gcs_params)
            bq_done  = load_bq.expand_kwargs(bq_params)
            s3_done  = upload_s3.expand_kwargs(s3_params)

            bucket_ready >> gcs_done >> bq_done
            [bq_done, s3_done] >> cleanup(collected, cabinet_id=cab_id)

    accounts     = list_accounts(CIAN_CONN_ID)
    dates        = get_dates()
    bucket_ready = ensure_gcs_bucket()

    for account in accounts:
        make_cabinet_group(account, dates, bucket_ready)


cian_to_bq_and_s3_multi_account()
