# Changelog

## Unreleased

- **BREAKING**: `CianBuilderReportsOperator.execute()` return type changed from `str` to `dict | None`. A day with data now returns a self-describing dict `{"date": ..., "path": ...}`; an empty day (`reports: []`) returns `None` and writes **no file** and **no XCom**. DAGs (and XCom consumers) that read the `collect` result as a path string must unwrap `item["path"]`, and aggregators must start with `items = list(items or [])` because a fully empty period makes Airflow hand them `None`, not `[]`
- Fixed: an empty day no longer produces an empty S3/GCS object (0-byte JSON / header-only CSV); nothing is uploaded for a day without data
- Fixed: a BigQuery partition for an already-loaded date is no longer truncated to empty by a `WRITE_TRUNCATE` load of an empty file when Cian returns no data on a re-run (silent data loss during backfill)
- Changed: `CianHook.get_builder_reports` strictly validates the API response shape — a 200 response without a list at `result.reports` now raises `AirflowException` instead of being silently collapsed into `[]`, so a broken API response is no longer masked as an empty day

## [0.3.0] - 2026-06-26

- **BREAKING**: `Account` (dataclass) and `list_accounts` (formerly `get_accounts`) moved to `airflow_provider_cian.accounts`; removed from `airflow_provider_cian.hooks` and `airflow_provider_cian.hooks.cian`. Update imports: `from airflow_provider_cian.accounts import Account, list_accounts` (these are standalone symbols, not part of `CianHook`; see [0.2.0] entry)
- Feat: `add_snapshot_ts` parameter in `CianBuilderReportsOperator` (default `False`); when enabled, adds a `snapshot_ts` field (`dag_run.start_date` as `YYYY-MM-DDTHH:MM:SS`, naive UTC) to each JSON record — allows tracking changes to `billing_price`/`is_targeted` over time across repeated collections of the same dates; fully backward-compatible; ignored for `output_format='csv'`

## [0.2.0] - 2026-06-09

- Feat: multi-account support — `Account` dataclass and `get_accounts(conn_id)` in `CianHook`; reads cabinet tokens from connection Extra JSON (`{"accounts": [{"id": "...", "token": "..."}]}`)
- Feat: `account_id` parameter in `CianBuilderReportsOperator`; files are isolated per cabinet: `{base_dir}/{cabinet_id}/{run_id}/{date}.ext`
- Feat: example DAG `bq_and_s3_multi_account_dag.py` — dynamic `TaskGroup` per cabinet with GCS → BigQuery and S3 uploads

## [0.1.3] - 2026-06-05

- Fix: `get_newbuilding_name` returns `"Неизвестно"` when Cian API responds with HTTP 400 (newbuilding deleted from directory); task no longer fails on such records
- Fix: `date` field in output always equals the operator's `date` parameter instead of the Moscow calendar date derived from the record timestamp; eliminates BigQuery partition rejection for records collected near midnight UTC

## [0.1.2] - 2026-06-04

- Feat: split `date` field into `date` (Moscow date, `YYYY-MM-DD`) and `datetime` (ISO with `+03:00` offset) — schema grows from 17 to 18 fields
- Fix: API datetime without timezone now gets explicit Moscow offset (`+03:00`) instead of being treated as naive

## [0.1.1] - 2026-06-04

- Fix: rename package to `airflow-provider-cian` (was `apache-airflow-provider-cian`)
- Fix: correct `get-newbuilding` response field `displayName` → `name`
- Fix: add missing `searcher_ct_phone` field to output schema (17 fields total)
- Docs: add Russian README (`README_ru.md`)

## [0.1.0] - 2026-06-04

- Initial release
- `CianHook`: авторизация, rate limiting (≤10 req/s), retry при 429/5xx
- `CianBuilderReportsOperator`: сбор звонков и чатов за день, вывод в JSON/CSV
