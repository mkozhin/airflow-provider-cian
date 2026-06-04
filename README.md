# apache-airflow-provider-cian

Airflow provider for Cian.ru Builder API — collect calls and chats statistics.

## Installation

```bash
pip install apache-airflow-provider-cian
```

Requirements: Python 3.10+, Apache Airflow 2.9.1–2.x.

## Connection Setup

Create an HTTP connection in Airflow (Admin → Connections):

| Field | Value |
|---|---|
| Connection Id | `cian_default` (or any name) |
| Connection Type | `HTTP` |
| Host | `https://public-api.cian.ru` |
| Password | Bearer token from your Cian Builder cabinet |

The provider reads `conn.host` as base URL and `conn.password` as Bearer token.

## Operator Parameters

`CianBuilderReportsOperator`:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cian_conn_id` | str | `cian_default` | Airflow connection ID |
| `date` | str | required | Collection date, `YYYY-MM-DD`. Supports `{{ ds }}` template |
| `base_dir` | str | `/tmp/cian` | Base directory for output files |
| `output_format` | str | `json` | `json` (JSONL) or `csv` |

The operator returns the output file path via `return_value` XCom.

Output file path: `{base_dir}/{safe_run_id}/{date}.{ext}`

### Output Schema (16 fields)

`id`, `newbuilding_id`, `newbuilding_name`, `date`, `action_type`, `searcher_phone`,
`builder_user_ct_phone`, `builder_user_phone`, `builder_sip_uri`, `call_duration`,
`tariff_price`, `auction_bet`, `cashback_spent`, `billing_price`, `has_claim`, `is_targeted`

`is_targeted` is computed: `billing_price > 0`.

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

## Retry Behaviour

On HTTP 429 or 5xx: exponential backoff — 1s, 2s, 4s (3 attempts total), then `AirflowException`.

## License

MIT
