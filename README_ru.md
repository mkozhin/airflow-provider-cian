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

### Откуда берётся токен

Источник токена определяется **исключительно параметром `account_id`** в операторе — не тем, заполнено ли поле `Password` или `Extra`:

| `account_id` в операторе | Токен берётся из |
|---|---|
| не задан (`None`) | `conn.password` — ошибка если пусто |
| `"111"` | запись в `extra.accounts` с `id == "111"` — ошибка если не найдена или токен пуст |

`Password` и `Extra` — независимые поля и не влияют друг на друга. Если заполнены оба, оператор использует один источник и полностью игнорирует второй в зависимости от `account_id`.

## Параметры оператора

`CianBuilderReportsOperator`:

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `cian_conn_id` | str | `cian_default` | ID подключения Airflow |
| `date` | str | обязательный | Дата сбора, `YYYY-MM-DD`. Поддерживает шаблон `{{ ds }}` |
| `base_dir` | str | `/tmp/cian` | Базовая директория для файлов |
| `output_format` | str | `json` | `json` (JSONL) или `csv` |
| `account_id` | str \| None | `None` | ID кабинета для мульти-аккаунт режима (совпадает с `id` в Extra JSON) |
| `add_snapshot_ts` | bool | `False` | Добавить поле `snapshot_ts` (naive-UTC время старта прогона, ISO 8601) в каждую JSON-запись. Игнорируется при `output_format='csv'`. |

### Возвращаемое значение (контракт `execute`)

За день **с данными** оператор пишет файл и возвращает самоописывающий dict, который кладётся в XCom `return_value`:

```python
{"date": "2026-07-01", "path": "/tmp/cian/<run_id>/2026-07-01.json"}
```

За **пустой день** (Cian вернул `reports: []`) оператор **не создаёт файл** и возвращает `None`. Airflow не пишет XCom для `None`, поэтому пустой день просто выпадает из списка результатов mapped-таска `collect` — ни одного downstream mapped-инстанса за него не создаётся.

> **Ломающее изменение (unreleased):** раньше `execute()` возвращал путь как обычную строку `str`. Теперь возвращается `dict | None`. DAG-и (и потребители XCom), читавшие результат `collect` как строку, должны разворачивать `item["path"]`.

**Предупреждение автору DAG — `items or []`:** любой агрегатор, потребляющий собранный список, ОБЯЗАН начинаться с `items = list(items or [])`. Когда пуст **весь период**, ни один mapped-инстанс не пишет XCom, и Airflow отдаёт агрегатору `None`, а не `[]` — наивный `len(items)` упадёт с `TypeError`.

**Полностью пустой период.** В v1 и multi-account DAG-ах (`bq_and_s3_dag.py`, `bq_and_s3_multi_account_dag.py`), где есть отдельные mapped-таски заливки/загрузки, агрегаторы возвращают `[]`, эти mapped-таски разворачиваются в **ноль инстансов** и помечаются `skipped`, а `dag_run` всё равно успешен. В v2 DAG (`bq_and_s3_dag_v2.py`) заливки происходят внутри одного таска `process_date`, поэтому пустой день — это просто `process_date`, возвращающий `None` в состоянии `success`; никакого `skipped` тут не появляется, даже когда пуст весь период.

**Сломанный ответ API.** Ответ 200 без `result.reports` (или с не-списком там) теперь **валит** таск с `AirflowException`, а не выдаётся молча за пустой день.

Путь к файлу зависит от того, как определяется ID кабинета:

| `account_id` | `conn.login` | Путь |
|---|---|---|
| не задан | не задан | `{base_dir}/{run_id}/{date}.{ext}` |
| не задан | `"123"` | `{base_dir}/123/{run_id}/{date}.{ext}` |
| `"123"` | любой | `{base_dir}/123/{run_id}/{date}.{ext}` |

В однокабинетном режиме поле `Login` подключения работает как ID кабинета для изоляции путей.

### Схема данных

Базовая схема (18 полей) — присутствует во всех записях независимо от формата вывода:

`id`, `newbuilding_id`, `newbuilding_name`, `date`, `datetime`, `action_type`, `searcher_phone`,
`searcher_ct_phone`, `builder_user_ct_phone`, `builder_user_phone`, `builder_sip_uri`,
`call_duration`, `tariff_price`, `auction_bet`, `cashback_spent`, `billing_price`,
`has_claim`, `is_targeted`

- `date` — дата сбора (`YYYY-MM-DD`), всегда равна параметру `date` оператора; безопасна для партиционирования BigQuery по дате
- `datetime` — исходное datetime из API с явным московским смещением (`YYYY-MM-DDTHH:MM:SS+03:00`)
- `is_targeted` вычисляется: `billing_price > 0`.

При `add_snapshot_ts=True` и `output_format='json'` каждая запись также содержит 19-е поле:

- `snapshot_ts` — `dag_run.start_date` в формате `YYYY-MM-DDTHH:MM:SS` (naive UTC, без смещения). Все записи одного прогона имеют одно и то же значение.

### Версионирование снапшотов

Поля `billing_price` и `is_targeted` могут изменяться задним числом после первичной выгрузки (Cian может доначислить или снять бюджет позже). Чтобы отслеживать изменения во времени, включите `add_snapshot_ts=True`:

```python
collect = CianBuilderReportsOperator.partial(
    task_id="collect",
    cian_conn_id="cian_default",
    output_format="json",
    add_snapshot_ts=True,
).expand(date=dates)
```

В каждую JSON-запись будет добавлено поле `snapshot_ts` — реальное wall-clock время старта прогона DAG (`dag_run.start_date`, naive UTC). Все записи одного прогона имеют одну метку.

Запрос последнего снапшота в ClickHouse:

```sql
SELECT *
FROM cian_calls
WHERE snapshot_ts = (
    SELECT max(snapshot_ts) FROM cian_calls
)
```

Или история изменений `billing_price`:

```sql
SELECT id, billing_price, snapshot_ts
FROM cian_calls
ORDER BY id, snapshot_ts
```

> **BigQuery:** схема `BQ_SCHEMA` в `examples/` фиксирована на 18 полях. При `add_snapshot_ts=True` потребуется либо добавить колонку `snapshot_ts STRING` в схему, либо использовать `ignore_unknown_values=True` в задании загрузки — иначе BigQuery отклонит записи с дополнительным полем.

> **Только для JSON:** `add_snapshot_ts=True` не влияет на вывод в CSV. Схема CSV остаётся 18-польной.

## Поддержка нескольких кабинетов

`list_accounts()` читает Extra-поле подключения и возвращает список объектов `Account`. Используйте его на этапе парсинга DAG, чтобы создать по одному `TaskGroup` на каждый кабинет:

```python
from airflow_provider_cian.accounts import Account, list_accounts

accounts = list_accounts("cian_default")  # вернёт [], если кабинеты не настроены
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

### Вспомогательные функции резолюции

`airflow_provider_cian.accounts` также содержит две низкоуровневые функции, которые используются хуком и оператором. Как правило, авторам DAG они не нужны напрямую:

- `resolve_cabinet_id(conn_id, account_id)` — возвращает cabinet id для операции. В режиме нескольких кабинетов (`account_id` задан) возвращает `account_id` сразу, без чтения подключения. В одиночном режиме лениво читает `conn.login`.
- `resolve_token(conn, account_id)` — возвращает токен аутентификации. В режиме нескольких кабинетов ищет первое совпадение в `conn.extra.accounts`. В одиночном режиме возвращает `conn.password`. Пробрасывает `AirflowException`, если токен не удалось получить.

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
    )
    collected = collect.expand(date=dates).output  # список {"date", "path"} — только дни с данными

    # Заливка идёт через агрегатор, который превращает собранные элементы в expand-kwargs.
    # `items or []` обязателен: за полностью пустой период Airflow отдаёт агрегатору
    # None (XCom не был записан), а не [].
    @task
    def to_s3_params(items):
        items = list(items or [])
        return [
            {"filename": it["path"], "dest_key": f"cian/{it['date']}.json"}
            for it in items
        ]

    # LocalFilesystemToS3Operator.partial(...).expand_kwargs(to_s3_params(collected))

    def cleanup(ti, **ctx):
        items = ti.xcom_pull(task_ids="collect")
        if isinstance(items, dict):          # единственный mapped-инстанс возвращает голый dict
            items = [items]
        for item in (items or []):           # None, когда пуст весь период
            path = item["path"]
            if path and os.path.exists(path):
                os.remove(path)

    collected >> PythonOperator(task_id="cleanup", python_callable=cleanup, trigger_rule="all_done")

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
