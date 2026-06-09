# airflow-provider-cian

---

*Powered by [Claude Code](https://claude.ai/code)*

---

Airflow-провайдер для Cian.ru Builder API — сбор статистики звонков и чатов.

## Установка

```bash
pip install airflow-provider-cian
```

Требования: Python 3.10+, Apache Airflow 2.9.1–2.x.

## Настройка подключения

### Один кабинет

Создайте HTTP-подключение в Airflow (Admin → Connections):

| Поле | Значение |
|---|---|
| Connection Id | `cian_default` (или любое имя) |
| Connection Type | `HTTP` |
| Host | `https://public-api.cian.ru` |
| Password | Bearer-токен из кабинета Cian застройщика |

Провайдер читает `conn.host` как базовый URL и `conn.password` как Bearer-токен.

### Несколько кабинетов

Чтобы собирать данные из нескольких кабинетов через одно подключение, укажите токены в поле **Extra** в формате JSON:

```json
{
  "accounts": [
    {"id": "111", "token": "Bearer <токен-кабинета-111>"},
    {"id": "222", "token": "Bearer <токен-кабинета-222>"}
  ]
}
```

`id` — произвольная строка, однозначно идентифицирующая кабинет (например, числовой ID кабинета из Cian). Не алфавитно-цифровые символы автоматически заменяются на `_` при использовании в путях к файлам и именах таблиц BigQuery.

## Параметры оператора

`CianBuilderReportsOperator`:

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `cian_conn_id` | str | `cian_default` | ID подключения Airflow |
| `date` | str | обязательный | Дата сбора, `YYYY-MM-DD`. Поддерживает шаблон `{{ ds }}` |
| `base_dir` | str | `/tmp/cian` | Базовая директория для файлов |
| `output_format` | str | `json` | `json` (JSONL) или `csv` |
| `account_id` | str \| None | `None` | ID кабинета для мульти-аккаунт режима (совпадает с `id` в Extra JSON) |

Оператор возвращает путь к файлу через XCom (`return_value`).

Путь к файлу зависит от того, как определяется ID кабинета:

| `account_id` | `conn.login` | Путь |
|---|---|---|
| не задан | не задан | `{base_dir}/{run_id}/{date}.{ext}` |
| не задан | `"123"` | `{base_dir}/123/{run_id}/{date}.{ext}` |
| `"123"` | любой | `{base_dir}/123/{run_id}/{date}.{ext}` |

В однокабинетном режиме поле `Login` подключения работает как ID кабинета для изоляции путей.

### Схема данных (18 полей)

`id`, `newbuilding_id`, `newbuilding_name`, `date`, `datetime`, `action_type`, `searcher_phone`,
`searcher_ct_phone`, `builder_user_ct_phone`, `builder_user_phone`, `builder_sip_uri`,
`call_duration`, `tariff_price`, `auction_bet`, `cashback_spent`, `billing_price`,
`has_claim`, `is_targeted`

- `date` — дата сбора (`YYYY-MM-DD`), всегда равна параметру `date` оператора; безопасна для партиционирования BigQuery по дате
- `datetime` — исходное datetime из API с явным московским смещением (`YYYY-MM-DDTHH:MM:SS+03:00`)
- `is_targeted` вычисляется: `billing_price > 0`.

## Поддержка нескольких кабинетов

`get_accounts()` читает Extra-поле подключения и возвращает список объектов `Account`. Используйте его на этапе парсинга DAG, чтобы создать по одному `TaskGroup` на каждый кабинет:

```python
from airflow_provider_cian.hooks.cian import Account, get_accounts

accounts = get_accounts("cian_default")  # вернёт [], если кабинеты не настроены
for account in accounts:
    with TaskGroup(group_id=f"cabinet_{account.id}"):
        CianBuilderReportsOperator.partial(
            task_id="collect",
            cian_conn_id="cian_default",
            account_id=account.id,   # выбирает нужный токен
            ...
        ).expand(date=dates)
```

Полный рабочий пример с выгрузкой в GCS, BigQuery и S3 — в файле `examples/bq_and_s3_multi_account_dag.py`.

## Пример DAG

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

    # Добавьте загрузку здесь, например LocalFilesystemToS3Operator.partial(...).expand(filename=collect)

    def cleanup(ti, **ctx):
        for path in (ti.xcom_pull(task_ids="collect") or []):
            if path and os.path.exists(path):
                os.remove(path)

    collect >> PythonOperator(task_id="cleanup", python_callable=cleanup, trigger_rule="all_done")

cian_reports()
```

## Rate Limiting

Лимит API — **≤10 запросов/сек на токен** (на кабинет Cian). Хук добавляет паузу 100ms перед каждым запросом. `max_active_tasks=3` на уровне DAG даёт дополнительный запас.

Если несколько клиентов работают с одного IP и всё равно получают 429 — создайте Airflow Pool:

```bash
airflow pools set cian_api 5 "Cian API rate limit pool"
```

Затем передайте `pool="cian_api"` в `CianBuilderReportsOperator.partial(...)`.

## Поведение при ошибках

При HTTP 429 или 5xx: экспоненциальный backoff — 1s, 2s, 4s (3 попытки), затем `AirflowException`.

## Лицензия

MIT
