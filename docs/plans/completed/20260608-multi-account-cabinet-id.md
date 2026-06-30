# Multi-account и cabinet_id в airflow-provider-cian

## Overview

Добавить поддержку нескольких кабинетов (accounts) в одном Airflow-коннекторе Cian.

**Проблема:** у одного клиента может быть несколько API-кабинетов (каждый со своим токеном). Сейчас провайдер поддерживает только один токен на коннектор (`conn.password`).

**Решение:**
- Хук получает `token_override` для подмены токена + модульную функцию `get_accounts()` для чтения списка кабинетов из `extra`
- Оператор получает параметр `account` (dict с `id` и `token`) — при его наличии использует токен из аккаунта и включает `id` в путь файла; без него поведение прежнее (`conn.password` + `conn.login` как cabinet_id)
- Новый пример DAG строит TaskGroup на каждый кабинет, читая список аккаунтов из `extra` коннектора при парсинге

**Acceptance Criteria:**
- Существующие DAG (`bq_and_s3_dag.py`, `bq_and_s3_dag_v2.py`) и все их тесты работают без изменений
- Оператор без параметра `account` строит путь без `cabinet_id` (как раньше)
- Оператор с `account` строит путь `{base_dir}/{cabinet_id}/{run_id}/{date}.ext`
- `conn.login` как fallback cabinet_id работает без явного `account`
- Multi-account DAG парсится без ошибок даже если коннектор не настроен

## Context

- `airflow_provider_cian/hooks/cian.py` — `CianHook`, токен из `conn.password` в `_make_request`
- `airflow_provider_cian/operators/builder_reports.py` — `CianBuilderReportsOperator`, `_build_path(run_id)` без cabinet_id
- `examples/bq_and_s3_dag.py` — базовый пример v1 (отдельные таски), используется как основа для нового примера
- `tests/hooks/test_cian.py`, `tests/operators/test_builder_reports.py` — существующие тесты

## Development Approach

- **testing approach**: Regular (код → тесты)
- завершать каждую задачу полностью до перехода к следующей
- все тесты должны проходить перед переходом к следующей задаче
- обратная совместимость обязательна: существующие DAG-и и тесты без изменений

## Technical Details

**Формат `extra` в коннекторе:**
```json
{"accounts": [{"id": "123", "token": "abc..."}, {"id": "456", "token": "def..."}]}
```

**`Account` dataclass** — живёт в `hooks/cian.py`, экспортируется из `hooks/__init__.py`:
```python
@dataclass
class Account:
    id: str

    def __post_init__(self):
        self.id = re.sub(r"[^\w-]", "_", self.id)
```
`Account.id` всегда санитизирован — единственная точка санитизации. Токена в `Account` нет: он хранится в `conn.extra_dejson` и никогда не покидает хук.

**Санитизация:** происходит только в `Account.__post_init__`. `_build_path()` и `execute()` доверяют переданному `cabinet_id` (уже чистый). Для fallback `conn.login` в `execute()` создаётся временный `Account(id=conn.login).id`.

**Уникальность `id` после санитизации:** если два аккаунта дают одинаковый `id` после санитизации (напр. `"a.b"` и `"a/b"` → оба `"a_b"`) — логировать `WARNING` с указанием оригинальных значений, использовать только первый из дублей.

**Поведение `cabinet_id` в пути:**
- задан `account.id` → `{base_dir}/{cabinet_id}/{run_id}/{date}.ext`
- задан `conn.login` (без account) → `{base_dir}/{safe_login}/{run_id}/{date}.ext`
- ни то ни другое → `{base_dir}/{run_id}/{date}.ext` (текущее поведение)

**Пути S3/GCS с cabinet_id:**
- S3: `{S3_PREFIX}/{cabinet_id}/_year=YYYY/_month=MM/_day=DD/_date=YYYYMMDD/{date}.json`
- GCS: `{GCS_PREFIX}/{cabinet_id}/{run_id}/{date}.json`

**BigQuery — отдельная таблица на кабинет:** `{BQ_TABLE}_{cabinet_id}${date_compact}` — таблица создаётся автоматически через `CREATE_IF_NEEDED`, никаких коллизий между кабинетами.

**`_build_path` сигнатура:** `_build_path(self, run_id: str, cabinet_id: str | None = None)` — `cabinet_id` идёт вторым со значением по умолчанию `None`, все существующие вызовы `op._build_path("run-1")` остаются валидными. Санитизации внутри нет — доверяет аргументу.

**`get_accounts` — модульная функция:** возвращает `list[Account]` (токены не включаются — они остаются в connection). Вызывается при парсинге DAG. Оборачивается в `try/except`: при отсутствии коннектора возвращает `[]` с WARNING — DAG импортируется пустым, не падает. Проверяет уникальность `account.id` после санитизации и логирует WARNING при коллизиях.

**`CianHook` — разрешение токена:** `CianHook(cian_conn_id, account_id=None)`. В `_make_request` при `account_id is not None`: читает `conn.extra_dejson.get("accounts", [])` напрямую, ищет по санитизированному `id`, берёт токен. Ошибки соединения прокидываются без `try/except`. Если аккаунт не найден — `AirflowException(f"Account id={account_id!r} not found in connection {cian_conn_id!r} extra.accounts")`. Нет `token_override` в публичном интерфейсе.

**`account` в `template_fields`:** НЕ добавлять — `account` статический объект, не требует темплейтинга. В `template_fields` только `date` и `cian_conn_id` (как сейчас).

**`account: Account | None` в операторе:** тип заменяет `dict | None`. Валидация ключа `"id"` не нужна — тип системы гарантирует наличие `account.id`. В `execute()` оператор передаёт `account_id=self.account.id` в `CianHook` — поиск токена целиком в хуке.

**Хелперы в multi-account DAG:** `make_gcs_params`, `make_bq_params`, `make_s3_params` — module-level `@task`-функции с явным параметром `cabinet_id: str`. Вызываются из фабричной функции с литеральным значением `cab_id`. Airflow корректно обрабатывает смешанные XComArg + литеральные аргументы начиная с 2.4.

**`cleanup` в multi-account DAG:** module-level `@task(trigger_rule="all_done")`, вызывается внутри каждого TaskGroup независимо. Каждый кабинет чистит только свою папку (`{base_dir}/{cabinet_id}/{run_id}`), не дожидаясь других кабинетов.

**`pool` в `default_args`:** `"pool": POOL` задаётся в `default_args` DAG — применяется ко всем тасками автоматически. Лёгкие таски тоже занимают слот, но освобождают его быстро — на практике это приемлемо. Упрощает код: не нужно указывать `pool=POOL` на каждом операторе.

## Implementation Steps

### Task 1: CianHook — Account dataclass, account_id и get_accounts

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `airflow_provider_cian/hooks/__init__.py`
- Modify: `tests/hooks/test_cian.py`

- [x] добавить `Account` dataclass в `hooks/cian.py` (поле `id: str`, `__post_init__` санитизирует через `re.sub(r"[^\w-]", "_", self.id)`)
- [x] добавить `account_id: str | None = None` в `CianHook.__init__`, сохранить как `self.account_id`
- [x] в `_make_request` при `self.account_id is not None`: читать `conn.extra_dejson.get("accounts", [])` напрямую, искать запись где `re.sub(r"[^\w-]", "_", a["id"]) == self.account_id`, брать `a["token"]`; если не найден — `AirflowException(f"Account id={self.account_id!r} not found in connection {self.cian_conn_id!r} extra.accounts")`; иначе — `conn.password` как раньше
- [x] добавить `get_accounts(conn_id: str) -> list[Account]`: читает `conn.extra_dejson.get("accounts", [])` через `BaseHook.get_connection`; создаёт `Account(id=a["id"])` (санитизация в `__post_init__`); проверяет уникальность `account.id` и логирует WARNING при коллизиях (берёт первый из дублей); оборачивает всё в `try/except Exception` → логирует WARNING и возвращает `[]`
- [x] экспортировать `Account` и `get_accounts` из `hooks/__init__.py`
- [x] написать тест: `CianHook(account_id="abc")` с matching аккаунтом в extra — `_make_request` использует токен аккаунта (не `conn.password`)
- [x] написать тест: `CianHook(account_id="missing")` — бросает `AirflowException` с `id` в сообщении
- [x] написать тест: без `account_id` — используется `conn.password` (регрессия)
- [x] написать тест: `get_accounts` возвращает `list[Account]` с санитизированными `id`
- [x] написать тест: `get_accounts` возвращает `[]` если ключ `accounts` отсутствует
- [x] написать тест: `get_accounts` возвращает `[]` если коннектор не найден (исключение не бросает)
- [x] написать тест: `Account(id="a.b/c")` → `account.id == "a_b_c"` (санитизация в __post_init__)
- [x] запустить тесты — все должны пройти

### Task 2: CianBuilderReportsOperator — параметр account и cabinet_id в пути

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [x] добавить `account: Account | None = None` в `__init__` (НЕ добавлять в `template_fields`); импортировать `Account` из `airflow_provider_cian.hooks.cian`
- [x] изменить сигнатуру `_build_path(self, run_id: str, cabinet_id: str | None = None)` — `cabinet_id` опциональный второй параметр; если задан: `{base_dir}/{cabinet_id}/{safe_run_id}/{date}.ext`, иначе как раньше; санитизации внутри нет — доверяет аргументу
- [x] в `execute()` вычислять `cabinet_id`: `self.account.id` если `account` задан, иначе `Account(id=conn.login).id` если `conn.login` задан, иначе `None`
- [x] в `execute()` создавать хук: `CianHook(self.cian_conn_id, account_id=self.account.id)` при `account` задан, иначе `CianHook(self.cian_conn_id)` — поиск токена целиком в хуке
- [x] написать тест: существующие `TestBuildPath` тесты (`op._build_path("run-1")`) проходят без изменений (backward compat)
- [x] написать тест: `_build_path("run-1", "abc")` включает `"abc"` в путь
- [x] написать тест: `_build_path` не применяет санитизацию — `_build_path("run-1", "a_b")` содержит `"a_b"` без изменений
- [x] написать тест: `execute()` с `account=Account(id="abc")` — путь содержит `"abc"`, хук создаётся с `account_id="abc"`
- [x] написать тест: `execute()` с `account=Account(id="a.b")` — путь содержит `"a_b"` (санитизация через Account)
- [x] написать тест: `execute()` без `account`, с `conn.login="msk"` — путь содержит `"msk"`
- [x] написать тест: `execute()` без `account`, без `conn.login` — путь как раньше (нет лишней папки)
- [x] написать тест: `execute()` с `account=Account(id="abc")` — хук создаётся без `token_override` (токен ищет сам хук)
- [x] запустить тесты — все должны пройти

### Task 3: Новый пример DAG для multi-account

**Files:**
- Create: `examples/bq_and_s3_multi_account_dag.py`
- Create: `tests/test_example_dag_multi_account.py`

- [x] создать фабричную функцию `make_cabinet_group(account: Account, dates, bucket_ready)` — создаёт `TaskGroup(group_id=f"cabinet_{account.id}")`
- [x] внутри группы: `collect.partial(..., account=account)`, `upload_gcs`, `load_bq`, `upload_s3`, `cleanup` — структура как в `bq_and_s3_dag.py`; `cleanup` внутри TaskGroup, оперирует только путями своего кабинета
- [x] S3-путь включает `cabinet_id`: `{S3_PREFIX}/{cabinet_id}/_year=.../_month=.../_day=.../_date=.../{date}.json`
- [x] GCS-путь включает `cabinet_id`: `{GCS_PREFIX}/{cabinet_id}/{run_id}/{date}.json`
- [x] BQ таблица на кабинет: `f"{BQ_TABLE}_{cab_id}${date_compact}"` вместо общей `{BQ_TABLE}${date_compact}`
- [x] хелперы `make_gcs_params`, `make_bq_params`, `make_s3_params` — module-level `@task` с параметром `cabinet_id: str`, вызываются из фабрики с `account.id`
- [x] `cleanup` — module-level `@task(trigger_rule="all_done")`, вызывается внутри каждой группы независимо
- [x] в теле DAG: `accounts = get_accounts(CIAN_CONN_ID)` при парсинге; цикл `for account in accounts: make_cabinet_group(...)`
- [x] если `accounts` пустой (или `get_accounts` вернул `[]` из-за ошибки) — DAG импортируется без TaskGroup-ов, без исключений
- [x] `POOL = "cian_pool"` вынести константой вверх файла; применить `pool=POOL` ко всем основным тасками: `collect`, `upload_gcs`, `load_bq`, `upload_s3` — глобальное ограничение нагрузки на Airflow-инстансе при одновременном запуске DAG-ов нескольких клиентов; служебные таски (`get_dates`, `ensure_gcs_bucket`, `make_*_params`, `cleanup`) без пула
- [x] `max_active_tasks` вынести константой вверх файла; ограничивает параллельность внутри конкретного DAG-ран чтобы не упираться в лимиты одного клиента/токена
- [x] добавить docstring с описанием структуры, ожидаемого формата extra и поведения при пустом коннекторе
- [x] написать smoke-тест: DAG импортируется без ошибок при замоканном `get_accounts` → `[]`
- [x] написать тест: при `get_accounts` возвращающем `[Account(id="aa"), Account(id="bb")]` — DAG содержит 2 TaskGroup-а с `group_id` `"cabinet_aa"` и `"cabinet_bb"`
- [x] запустить тесты — все должны пройти

### Task 4: Проверка

- [x] запустить полный тест-сьют: `pytest tests/`
- [x] убедиться что `examples/bq_and_s3_dag.py` и `bq_and_s3_dag_v2.py` не требуют изменений
- [x] переместить план в `docs/plans/completed/` (локально, без коммита)

## Post-Completion

**Ручная проверка в Airflow UI:**
- создать коннектор с `extra = {"accounts": [{"id": "test", "token": "..."}]}` — убедиться что DAG отображает TaskGroup `cabinet_test`
- создать коннектор с заполненным `login` — убедиться что single-account DAG строит пути с этим значением
- запустить DAG с одним кабинетом на тестовых данных

**Обновление README** — добавить раздел с описанием multi-account настройки и формата extra (при необходимости).
