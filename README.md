# airflow-provider-cian

---

*Powered by [Claude Code](https://claude.ai/code)*

---

Airflow provider for Cian.ru Builder API — collect calls and chats statistics.

## Installation

```bash
pip install airflow-provider-cian
```

Requirements: Python 3.10+, Apache Airflow 2.9.1–2.x.

## Connection Setup

### Single account

Create an HTTP connection in Airflow (Admin → Connections):

| Field | Value |
|---|---|
| Connection Id | `cian_default` (or any name) |
| Connection Type | `HTTP` |
| Host | `https://public-api.cian.ru` |
| Password | Bearer token from your Cian Builder cabinet |

The provider reads `conn.host` as base URL and `conn.password` as Bearer token.

### Multiple accounts

To collect data from several Cian cabinets using a single connection, put tokens in the **Extra** field as JSON:

```json
{
  "accounts": [
    {"id": "111", "token": "Bearer <token-for-cabinet-111>"},
    {"id": "222", "token": "Bearer <token-for-cabinet-222>"}
  ]
}
```

The `id` can be any string that uniquely identifies the cabinet (e.g. the numeric cabinet ID from Cian). Non-alphanumeric characters are sanitised to `_` for use in file paths and BigQuery table names.

### Token resolution

The token source is controlled **solely by `account_id`** on the operator — not by whether `Password` or `Extra` is filled in:

| `account_id` on operator | Token taken from |
|---|---|
| not set (`None`) | `conn.password` — error if empty |
| `"111"` | `extra.accounts` entry where `id == "111"` — error if not found or token empty |

`Password` and `Extra` are independent fields and do not interfere with each other. If both are filled in, the operator uses one and ignores the other depending on `account_id`.

## Operator Parameters

`CianBuilderReportsOperator`:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cian_conn_id` | str | `cian_default` | Airflow connection ID |
| `date` | str | required | Collection date, `YYYY-MM-DD`. Supports `{{ ds }}` template |
| `base_dir` | str | `/tmp/cian` | Base directory for output files |
| `output_format` | str | `json` | `json` (JSONL) or `csv` |
| `account_id` | str \| None | `None` | Cabinet ID for multi-account mode (matches `id` in Extra JSON) |
| `add_snapshot_ts` | bool | `False` | Add `snapshot_ts` field (naive-UTC run start time, ISO 8601) to each JSON record. Ignored for `output_format='csv'`. |

The operator returns the output file path via `return_value` XCom.

Output file path depends on how the cabinet ID is resolved:

| `account_id` | `conn.login` | Path |
|---|---|---|
| not set | not set | `{base_dir}/{run_id}/{date}.{ext}` |
| not set | `"123"` | `{base_dir}/123/{run_id}/{date}.{ext}` |
| `"123"` | any | `{base_dir}/123/{run_id}/{date}.{ext}` |

In single-account mode, setting `Login` on the connection acts as a cabinet ID for path isolation.

### Output Schema

Base schema (18 fields) — present in all records regardless of format:

`id`, `newbuilding_id`, `newbuilding_name`, `date`, `datetime`, `action_type`, `searcher_phone`,
`searcher_ct_phone`, `builder_user_ct_phone`, `builder_user_phone`, `builder_sip_uri`,
`call_duration`, `tariff_price`, `auction_bet`, `cashback_spent`, `billing_price`,
`has_claim`, `is_targeted`

- `date` — collection date (`YYYY-MM-DD`), always equals the operator's `date` parameter; safe for BigQuery date partitioning
- `datetime` — original API datetime with explicit Moscow offset (`YYYY-MM-DDTHH:MM:SS+03:00`)
- `is_targeted` is computed: `billing_price > 0`.

When `add_snapshot_ts=True` and `output_format='json'`, each record also contains a 19th field:

- `snapshot_ts` — `dag_run.start_date` formatted as `YYYY-MM-DDTHH:MM:SS` (naive UTC, no timezone offset). All records within a single DAG run share the same value.

### Snapshot Versioning

The `billing_price` and `is_targeted` fields can change retroactively after initial collection (Cian may adjust billing post-factum). To track these changes over time, enable `add_snapshot_ts=True`:

```python
collect = CianBuilderReportsOperator.partial(
    task_id="collect",
    cian_conn_id="cian_default",
    output_format="json",
    add_snapshot_ts=True,
).expand(date=dates)
```

Each JSON record will include `snapshot_ts` — the real wall-clock start time of the DAG run (`dag_run.start_date`, naive UTC). All records within the same run share one timestamp.

To query the latest snapshot per call in ClickHouse:

```sql
SELECT *
FROM cian_calls
WHERE snapshot_ts = (
    SELECT max(snapshot_ts) FROM cian_calls
)
```

Or to build a history of `billing_price` changes:

```sql
SELECT id, billing_price, snapshot_ts
FROM cian_calls
ORDER BY id, snapshot_ts
```

> **BigQuery note:** the example `BQ_SCHEMA` in `examples/` is fixed at 18 fields. When `add_snapshot_ts=True`, either add a `snapshot_ts STRING` column to the schema or use `ignore_unknown_values=True` on the load job — otherwise BigQuery will reject records with the extra field.

> **JSON only:** `add_snapshot_ts=True` has no effect when `output_format='csv'`. The CSV schema remains 18 fields.

## Multi-Account Support

`get_accounts()` reads the connection Extra JSON and returns a list of `Account` objects. Use it at DAG parse time to build one `TaskGroup` per cabinet:

```python
from airflow_provider_cian.hooks.cian import Account, get_accounts

accounts = get_accounts("cian_default")  # returns [] if no accounts configured
for account in accounts:
    with TaskGroup(group_id=f"cabinet_{account.id}"):
        CianBuilderReportsOperator.partial(
            task_id="collect",
            cian_conn_id="cian_default",
            account_id=account.id,   # routes to the correct token
            ...
        ).expand(date=dates)
```

See `examples/bq_and_s3_multi_account_dag.py` for a full working example with GCS, BigQuery and S3 uploads.

## Example DAG

```python
from datetime import date, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow_provider_cian.operators.builder_reports import CianBuilderReportsOperator
import os

@dag(schedule=None, catchup=False, max_active_tasks=3)
def cian_reports():
    @task
    def get_dates():
        yesterday = date.today() - timedelta(days=1)
        return [(yesterday - timedelta(days=i)).isoformat() for i in range(7)]

    dates = get_dates()

    collect = CianBuilderReportsOperator.partial(
        task_id="collect",
        cian_conn_id="cian_default",
        base_dir="/tmp/cian",
        output_format="json",
    ).expand(date=dates)

    # Add upload here, e.g. LocalFilesystemToS3Operator.partial(...).expand(filename=collect)

    def cleanup(ti, **ctx):
        for path in (ti.xcom_pull(task_ids="collect") or []):
            if path and os.path.exists(path):
                os.remove(path)

    collect >> PythonOperator(task_id="cleanup", python_callable=cleanup, trigger_rule="all_done")

cian_reports()
```

## Rate Limiting

The API limit is **≤10 req/s per token** (per Cian account). The hook adds a 100ms sleep before each request. `max_active_tasks=3` on the DAG level provides additional safety margin.

If multiple clients share the same IP and you still get 429 errors, create an Airflow Pool:

```bash
airflow pools set cian_api 5 "Cian API rate limit pool"
```

Then pass `pool="cian_api"` to `CianBuilderReportsOperator.partial(...)`.

## Error Handling

`CianNotFoundError` (subclass of `AirflowException`) is raised when the API returns a "not found" response for a resource. `get_newbuilding_name` catches it internally and returns `"Неизвестно"` — DAG authors don't need to handle it for that method. For custom hook usage:

```python
from airflow_provider_cian.hooks import CianNotFoundError
```

## Retry Behaviour

On HTTP 429 or 5xx: exponential backoff — 1s, 2s, 4s (3 attempts total), then `AirflowException`.

## License

MIT
