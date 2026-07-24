# Changelog

## [0.5.1] - 2026-07-24

- Fixed: the task no longer fails with `ValueError: Invalid isoformat string` on records whose timestamp carries an unusual number of sub-second digits (the production case had 5: `2026-07-23T18:14:20.86363+03:00`). The cause is `datetime.fromisoformat()` in CPython before 3.11, which accepts a fractional part of **only** 3 or 6 digits — any other precision raises; the Airflow worker runs Python 3.10, and the provider still supports `>=3.10`
- Changed: sub-second digits in the `datetime` field are dropped by truncation (not rounding) before parsing, so the value is always `YYYY-MM-DDTHH:MM:SS+03:00` — exactly the format already documented in the READMEs. Records with 3- or 6-digit fractions, which used to parse successfully and reach the output with sub-second precision, are now truncated as well. The schema, the field order and the CSV layout are unchanged; in BigQuery `datetime` is a `STRING`, so no reload of historical partitions is required. Second precision is enforced on the parsed value itself (`microsecond=0`), not only on the shape of the incoming string, so it holds on every supported interpreter (3.11+ accepts ISO forms 3.10 does not). See `docs/adr/0005-datetime-drop-subsecond.md`
- Changed: a record whose `date` field is present but not a string (e.g. an epoch number) now raises `AirflowException` naming the record `id`, like the missing-`date` case, instead of a bare `AttributeError`
- Changed: a `date` string the stdlib cannot parse now raises `AirflowException: Record id=<id> has an unparsable 'date': '<value>'` (chained from the original `ValueError`) instead of the raw `ValueError`. The quoted value is the one the API sent, before normalisation. Previously the `ValueError` quoted the string after the `Z` → `+00:00` substitution — close enough to the input that the incident's `.86363` was still readable in the traceback — but 0.5.1 also strips the sub-second digits before parsing, so an unwrapped `ValueError` would from now on hide the very precision that root-caused the incident

## [0.5.0] - 2026-07-22

- **BREAKING**: every output record (JSON and CSV) now carries a new `account_id` field, inserted as the second field right after `id`. The base schema grows from 18 to 19 fields (`snapshot_ts` becomes the 20th field in JSON when `add_snapshot_ts=True`). The value is the sanitized Account ID of the cabinet — the same value used in the file path — so records from different cabinets can be told apart in the data itself, not only from the file path
- **BREAKING**: `login` is now **required** in single-account mode. `CianBuilderReportsOperator.execute()` raises `AirflowException` (before creating the hook or hitting the API) when neither `account_id` nor `conn.login` yields a non-empty Account ID; the previous behaviour of writing files to a path without a cabinet segment is removed
- **BREAKING**: the output file path always contains the `<account_id>` segment (`{base_dir}/{account_id}/{run_id}/{date}.{ext}`); it can no longer be `{base_dir}/{run_id}/{date}.{ext}`
- **BREAKING**: `resolve_token` now raises `AirflowException` on an ambiguous id collision (two or more `extra.accounts` entries collapsing to the same sanitized id) instead of silently returning the first entry's token. This is a deliberate asymmetry with `list_accounts()`, which deduplicates such collisions with a WARNING and keeps the first; at execution time an ambiguous token lookup fails loudly. Unique/not-found/missing-token cases are unchanged
- **BREAKING**: `resolve_cabinet_id` and `resolve_token` now sanitize the `account_id` (`[^\w-]` → `_`) uniformly — `resolve_cabinet_id` returns the sanitized value and `resolve_token` looks up the token by the canonical (sanitized) key. Callers that passed a raw `account_id` with special characters directly will see the sanitized form in the path, data and token lookup (sanitization is idempotent for values coming from `list_accounts()`, so the normal DAG path is unaffected)
- **BREAKING**: `CianBuilderReportsOperator.execute()` now validates `date` up front and raises `AirflowException` unless it is a zero-padded, calendar-valid ISO date (`YYYY-MM-DD`). Malformed values that were previously accepted (e.g. `2024-1-5`, a timestamp, full-width digits, or any non-`YYYY-MM-DD` string) now fail fast — before the hook is built or the API is called — instead of producing a malformed output path / BigQuery partition; this also closes path traversal via the templated `date` (the only path component that was not sanitized)
- Changed: the example DAGs' `BQ_SCHEMA` gains the `account_id STRING NULLABLE` column (v1/v2: 19 fields; multi-account with `snapshot_ts`: 20 fields) — downstream BigQuery tables must add this column before deploying 0.5.0, as the load jobs use `ignore_unknown_values=False`

## [0.4.0] - 2026-07-10

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
