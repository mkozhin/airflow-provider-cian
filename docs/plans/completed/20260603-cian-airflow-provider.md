# Airflow Provider: Cian Builder Reports

## Overview

Airflow-провайдер для сбора статистики звонков и чатов с платформы Циан (cian.ru) через публичный API застройщиков.

Провайдер предоставляет:
- `CianHook` — авторизация, rate limiting, запросы к API
- `CianBuilderReportsOperator` — сбор данных за один день, обогащение названиями ЖК, запись в файл

Провайдер только **собирает данные и пишет в локальный файл**. Загрузка в хранилища (S3, BigQuery, ClickHouse) выполняется стандартными операторами Airflow в DAG. Cleanup-файла — `PythonOperator` в конце пайплайна.

### Acceptance Criteria
- `pip install -e .` устанавливается без ошибок
- `airflow providers list` показывает провайдер `apache-airflow-provider-cian`
- `CianBuilderReportsOperator` создаёт корректный `.json`/`.csv` файл с 16 полями
- Файл за день с 7 записями и 3 уникальными ЖК требует ровно 1 + 3 = 4 запроса к API
- При retry старый файл удаляется и создаётся заново
- Два параллельных запуска одного DAG за один период не перетирают файлы друг друга
- DAG парсится без ошибок и генерирует правильный список дат (desc)

### Поля недоступные через API (явно исключены)
Следующие поля из интерфейса Циан **отсутствуют в публичном API** и не включаются в схему:
- **Статус звонка** (Успешный / Выключен застройщик) — нет поля в ответе API
- **Статус ответа (только ОЗ)** — нет поля в ответе API
- **Тип лида** (Звонок / Обратный звонок) — `actionType` = "call" для всех типов
- **Название тарифа** (pro / pro+ / premium / lite) — нет эндпоинта в публичном API

## Context (from discovery)

- Новый проект с нуля, файлов кода ещё нет
- Airflow 2.9.1, Python 3.10+
- Docker на небольшом сервере — приоритет: меньше RAM/CPU
- Несколько клиентов (кабинетов Циана) — каждый со своим DAG и своим `cian_conn_id`
- Builder API: `https://public-api.cian.ru/builders/swagger/latest` (15 эндпоинтов)

## Development Approach

- **testing approach**: Regular (сначала код, потом тесты)
- Каждый таск завершается полностью перед следующим
- Тесты обязательны для каждого таска
- API вызовы в тестах мокируются через `unittest.mock.patch`

## Testing Strategy

- **unit tests**: pytest + `unittest.mock.patch`
- Тест-файлы: `tests/hooks/test_cian.py`, `tests/operators/test_builder_reports.py`
- Покрытие: успешные пути + ошибки API + граничные случаи (пустой ответ, 429, retry)

## Solution Overview

### API (Builder-specific)

Документация: `https://public-api.cian.ru/builders/docs/latest`

| Эндпоинт | Метод | Описание |
|---|---|---|
| `/v1/get-builder-reports/?onDate=YYYY-MM-DD` | GET | Все звонки и чаты за день (без пагинации) |
| `/v1/get-newbuilding/?newbuildingId=X` | GET | Название ЖК по ID |

**Пагинация:** API не поддерживает `page`/`pageSize` — параметры игнорируются. Всегда возвращаются все записи за день одним запросом.

**Rate limit:** ≤10 req/s **per-token** (per-account). Каждый кабинет Циана имеет свой токен и независимый лимит. При HTTP 429 — экспоненциальный backoff с 3 попытками.

### Connection в Airflow

Используем встроенный `http` conn type (не регистрируем кастомный):
- `conn_type`: `http`
- `host`: `https://public-api.cian.ru`
- `password`: Bearer токен

### Итоговая схема данных (16 полей)

| Поле | Источник |
|---|---|
| `id` | API напрямую |
| `newbuilding_id` | API напрямую |
| `newbuilding_name` | get-newbuilding (кэш по уникальным ID) |
| `date` | API напрямую |
| `action_type` | API напрямую |
| `searcher_phone` | API напрямую |
| `builder_user_ct_phone` | API напрямую |
| `builder_user_phone` | API напрямую |
| `builder_sip_uri` | API напрямую |
| `call_duration` | API напрямую (секунды, int) |
| `tariff_price` | API напрямую |
| `auction_bet` | API напрямую |
| `cashback_spent` | API напрямую |
| `billing_price` | API напрямую |
| `has_claim` | API напрямую |
| `is_targeted` | Вычисляется: `billing_price > 0` |

### Алгоритм оператора

1. Если файл `{base_dir}/{safe_run_id}/{date}.{ext}` существует — удалить (идемпотентность при retry)
2. Создать директорию `os.makedirs(dir, exist_ok=True)`
3. Запросить все записи за дату (`get-builder-reports`) — один запрос
4. Собрать уникальные `newbuildingId`, запросить название каждого (`get-newbuilding`), кэшировать в dict
5. Обогатить каждую запись: `newbuilding_name`, `is_targeted`
6. Записать построчно в файл (JSON или CSV)
7. Вернуть путь к файлу через `return output_path`

### Пайплайн в DAG (типичный)

```
collect(date) → [upload / copy / load] → cleanup(os.remove via PythonOperator)
```

- `collect` возвращает путь через XCom (`return_value`)
- `cleanup` получает путь через `xcom_pull` от `collect`
- Каждый шаг — mapped task (`.expand(date=dates)`)

## Technical Details

### CianHook

```python
class CianHook(BaseHook):
    conn_name_attr = 'cian_conn_id'
    default_conn_name = 'cian_default'
    conn_type = 'http'

    def get_builder_reports(self, date: str) -> list[dict]: ...
    def get_newbuilding_name(self, newbuilding_id: int) -> str: ...  # AirflowException при ошибке
    def _make_request(self, path: str, params: dict) -> dict: ...
```

**Rate limiting и retry:**
- `time.sleep(0.1)` перед каждым запросом (≤10 req/s)
- При HTTP 429 или 5xx: backoff — sleep 1s, 2s, 4s (3 попытки), затем `AirflowException`
- Реализуется вручную без внешних библиотек
- `get_newbuilding_name`: при ошибке **кидает `AirflowException`** (пустое название недопустимо)

### CianBuilderReportsOperator

```python
class CianBuilderReportsOperator(BaseOperator):
    def __init__(
        self,
        *,
        cian_conn_id: str = 'cian_default',
        date: str,                    # YYYY-MM-DD
        base_dir: str = '/tmp/cian',  # базовая директория для файлов
        output_format: str = 'json',  # 'json' (JSONL, расширение .json) или 'csv'
        **kwargs,
    ): ...

    def execute(self, context) -> str:
        safe_run_id = re.sub(r'[^\w-]', '_', context['run_id'])
        ext = 'json' if self.output_format == 'json' else 'csv'
        output_path = os.path.join(self.base_dir, safe_run_id, f"{self.date}.{ext}")
        if os.path.exists(output_path):
            os.remove(output_path)   # идемпотентность при retry
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        ...
        return output_path           # BaseOperator пушит return_value в XCom
```

**Форматы:**
- `json`: одна запись на строку `json.dumps(row, ensure_ascii=False) + '\n'`, расширение `.json`
- `csv`: `csv.DictWriter` с `quoting=csv.QUOTE_ALL` (телефоны как строки), расширение `.csv`

### Пример DAG

```python
# dag.params:
#   date_from: дефолт = вчера - N дней (более ранняя дата)
#   date_to:   дефолт = вчера          (более поздняя дата)
# Список дат desc: от date_to к date_from (свежие первыми)
# max_active_tasks=3 (rate limit per-token, параллельность на уровне DAG)
```

### Provider Info (Airflow 2.9.1)

`get_provider_info()` живёт в `airflow_provider_cian/__init__.py` (как в google-sheets и rmq провайдерах):

```python
# airflow_provider_cian/__init__.py
from airflow_provider_cian._version import __version__

def get_provider_info() -> dict:
    return {
        "package-name": "apache-airflow-provider-cian",
        "name": "Cian",
        "description": "Airflow provider for Cian.ru Builder API",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Cian",
                "external-doc-url": "https://public-api.cian.ru/builders/docs/latest",
                "tags": ["service"],
            },
        ],
        "operators": [
            {
                "integration-name": "Cian",
                "python-modules": ["airflow_provider_cian.operators.builder_reports"],
            },
        ],
        "hooks": [
            {
                "integration-name": "Cian",
                "python-modules": ["airflow_provider_cian.hooks.cian"],
            },
        ],
    }
```

`_version.py` — генерируется автоматически setuptools-scm из git-тегов, в gitignore.

Entry point в `pyproject.toml`:
```toml
[project.entry-points."apache_airflow_provider"]
provider_info = "airflow_provider_cian:get_provider_info"
```

## Implementation Steps

### Task 1: Scaffolding пакета

**Files:**
- Create: `pyproject.toml`
- Create: `airflow_provider_cian/__init__.py`  ← содержит `get_provider_info()`
- Create: `airflow_provider_cian/hooks/__init__.py`
- Create: `airflow_provider_cian/operators/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/hooks/__init__.py`
- Create: `tests/operators/__init__.py`
- Create: `tests/test_provider_info.py`
- Create: `readme.md`
- Create: `CHANGELOG.md`
- Create: `LICENSE`

- [ ] создать `pyproject.toml`:
  - `[build-system]`: `setuptools>=68`, `wheel`, `setuptools-scm>=8`
  - `dynamic = ["version"]`
  - `description = "Apache Airflow provider for Cian.ru Builder API — collect calls and chats statistics"`
  - `readme = "readme.md"` (lowercase — это страница пакета на PyPI)
  - `authors = [{name = "Michael Kozhin", email = "michael@kozhin.cc"}]`
  - `keywords = ["airflow", "cian", "provider", "real-estate", "builder-api"]`
  - зависимости: `apache-airflow>=2.9.1,<3.0`, `requests>=2.28`
  - dev: `pytest`
  - classifiers: `Framework :: Apache Airflow`, `Framework :: Apache Airflow :: Provider`, `Development Status :: 4 - Beta`, `Topic :: Software Development :: Libraries :: Python Modules`, `Topic :: Internet :: WWW/HTTP`, `License :: OSI Approved :: MIT License`
  - `[project.urls]`: Homepage, Documentation, Repository, Changelog, Issues → `https://github.com/mkozhin/airflow-provider-cian`
  - entry point: `provider_info = "airflow_provider_cian:get_provider_info"`
  - `[tool.setuptools_scm]`: `version_file = "airflow_provider_cian/_version.py"`
- [ ] создать пакетную структуру с `__init__.py` (без `sensors/` — YAGNI)
- [ ] поместить `get_provider_info()` в `airflow_provider_cian/__init__.py` с полным dict: `package-name`, `name`, `description`, `versions` (из `_version.py`), `integrations`, `operators`, `hooks`
- [ ] добавить `airflow_provider_cian/_version.py` в `.gitignore` (генерируется setuptools-scm)
- [ ] создать `LICENSE` (MIT) и `CHANGELOG.md` с начальной записью `## [0.1.0]`
- [ ] создать `readme.md` с разделами: Installation, Requirements, Connection setup (conn_type=HTTP, host, password=Bearer token), Usage (пример DAG), License
- [ ] убедиться что `pip install -e .` отрабатывает без ошибок
- [ ] написать `tests/test_provider_info.py`: проверить что `get_provider_info()` возвращает dict с ключами `package-name`, `name`, `description`, `versions`, `integrations`, `operators`, `hooks`
- [ ] запустить тесты — должны пройти перед Task 2

### Task 2: CianHook

**Files:**
- Create: `airflow_provider_cian/hooks/cian.py`
- Create: `tests/hooks/test_cian.py`

- [ ] создать `CianHook(BaseHook)` с атрибутами: `conn_type = 'http'`, `hook_name = "Cian Builder API"` (имя в форме соединения Airflow UI)
- [ ] реализовать `_make_request(path, params)`: токен из `conn.password`, base URL из `conn.host`; `time.sleep(0.1)` перед запросом; при 429/5xx — retry с backoff 1s/2s/4s, затем `AirflowException`
- [ ] реализовать `get_builder_reports(date: str) -> list[dict]`: GET `/v1/get-builder-reports/?onDate={date}`, один запрос, возвращает `result.reports` (пустой список если нет данных)
- [ ] реализовать `get_newbuilding_name(newbuilding_id: int) -> str`: GET `/v1/get-newbuilding/?newbuildingId={id}`; при любой ошибке кидает `AirflowException` (пустое название недопустимо)
- [ ] реализовать `test_connection(self) -> tuple[bool, str]`: вызывает `get_builder_reports(date=today)`, возвращает `(True, "Connection successful")` или `(False, str(e))`; нужна для кнопки «Test Connection» в Admin → Connections
- [ ] написать тесты с `unittest.mock.patch('requests.get')`:
  - успешный `get_builder_reports` → список записей
  - пустой ответ (`reports: []`) → `[]`
  - HTTP 429 → retry → успех на 2-й попытке
  - HTTP 429 три раза → `AirflowException`
  - успешный `get_newbuilding_name` → строка с названием
  - `get_newbuilding_name` при HTTP ошибке → `AirflowException`
  - `test_connection()` при успешном запросе → `(True, "Connection successful")`
  - `test_connection()` при ошибке → `(False, <сообщение об ошибке>)`
- [ ] запустить тесты — должны пройти перед Task 3

### Task 3: CianBuilderReportsOperator

**Files:**
- Create: `airflow_provider_cian/operators/builder_reports.py`
- Create: `tests/operators/test_builder_reports.py`

- [ ] создать `CianBuilderReportsOperator(BaseOperator)` с атрибутами класса: `template_fields = ("date", "cian_conn_id")` (поддержка `date="{{ ds }}"` в DAG), `ui_color = "#e8f5e9"`; параметры: `cian_conn_id`, `date`, `base_dir`, `output_format`; в `__init__` валидировать: `if output_format not in ('json', 'csv'): raise ValueError(...)`
- [ ] реализовать `execute(context) -> str`: тонкий координатор — вызывает `_build_path`, `os.remove`/`os.makedirs`, hook, `_enrich`, `_write`, `return path`
- [ ] реализовать `_build_path(run_id: str) -> str`: санитизация `run_id` (`re.sub(r'[^\w-]', '_', run_id)`), сборка пути `{base_dir}/{safe_run_id}/{date}.{ext}`
- [ ] реализовать `_enrich(records: list[dict], hook: CianHook) -> list[dict]`: кэш уникальных `newbuildingId` → названия через `hook.get_newbuilding_name`, добавить `newbuilding_name` и `is_targeted = billing_price > 0`; сюда добавляются все будущие обогащения
- [ ] реализовать `_write(records: list[dict], path: str) -> None`: JSON — `json.dumps(row, ensure_ascii=False) + '\n'` построчно; CSV — `csv.DictWriter` с `quoting=csv.QUOTE_ALL`; сюда добавляются все будущие форматы
- [ ] написать тесты с моком `CianHook`:
  - обычный запуск JSON → файл содержит правильные данные, расширение `.json`
  - обычный запуск CSV → телефоны в кавычках, расширение `.csv`
  - пустой день → файл создаётся пустым (CSV: только заголовок)
  - `is_targeted=True` когда `billing_price > 0`, `False` когда `= 0`
  - кэш ЖК: 3 записи, 2 уникальных ID → ровно 2 вызова `get_newbuilding_name`
  - retry-идемпотентность: если файл уже есть — удаляется и создаётся заново
  - `base_dir` кастомный → файл в нужной директории
  - два разных `run_id` → файлы в разных поддиректориях
- [ ] запустить тесты — должны пройти перед Task 4

### Task 4: Пример DAG

**Files:**
- Create: `examples/builder_reports_dag.py`
- Create: `tests/test_example_dag.py`

- [ ] создать `examples/builder_reports_dag.py`:
  - `dag.params`: `date_from` (дефолт: вчера - 30 дней), `date_to` (дефолт: вчера)
  - `max_active_tasks=3` (ограничение параллельности, rate limit per-token)
  - функция `get_date_range(date_from, date_to) -> list[str]`: даты от `date_to` до `date_from` включительно, по убыванию
  - `CianBuilderReportsOperator.partial(...).expand(date=dates)`
  - `PythonOperator` для cleanup: `os.remove(ti.xcom_pull(task_ids='collect'))`
  - комментарии: как добавить upload в S3/BQ/ClickHouse между collect и cleanup
  - комментарий: если несколько клиентов с одного IP дают 429, создать Airflow Pool
- [ ] написать `tests/test_example_dag.py`:
  - `get_date_range('2024-01-01', '2024-01-05')` → `['2024-01-05', ..., '2024-01-01']`
  - `get_date_range('2024-01-01', '2024-01-01')` → `['2024-01-01']`
  - дефолтный диапазон → список из 30 элементов
- [ ] проверить парсинг DAG: `python examples/builder_reports_dag.py`
- [ ] запустить тесты — должны пройти перед Task 5

### Task 5: CI/CD пайплайн публикации

**Files:**
- Create: `.github/workflows/publish.yml`

- [ ] создать `.github/workflows/publish.yml`:
  - триггер: `push: tags: ["v*"]`
  - job `test`: matrix python `["3.10", "3.11", "3.12"]`, `fetch-depth: 0` (нужно для setuptools-scm), `pip install -e ".[dev]"`, `pytest tests/ -v`
  - job `publish`: `needs: test`, `environment: pypi`, `permissions: id-token: write` (OIDC), `fetch-depth: 0` (тоже нужен!), `python -m build`, `pypa/gh-action-pypi-publish@release/v1`
- [ ] проверить что `.gitignore` содержит `airflow_provider_cian/_version.py`
- [ ] настроить OIDC Trusted Publishing на PyPI: Project → Manage → Publishing → Add publisher → GitHub Actions, owner: `mkozhin`, repo: `airflow-provider-cian`, workflow: `publish.yml`, environment: `pypi`
- [ ] создать GitHub Environment `pypi` в repo Settings → Environments (до первого push тега)

### Task 6: Финальная проверка

- [ ] запустить полный набор тестов: `pytest tests/ -v`
- [ ] проверить установку: `pip install -e .`
- [ ] проверить парсинг DAG: `python examples/builder_reports_dag.py`
- [ ] проверить отображение провайдера: `airflow providers list` (если доступен Airflow)

### Task 7: Документация и завершение

**Files:**
- Create: `README.md`

- [ ] написать `README.md`:
  - установка
  - настройка Connection (`conn_type=http`, `host`, `password=Bearer токен`)
  - параметры оператора: `cian_conn_id`, `date`, `base_dir`, `output_format`
  - пример DAG с пайплайном collect → upload → cleanup
  - раздел про rate limiting: per-token лимит, `max_active_tasks`, опциональный Pool для per-IP случая
- [ ] перенести план: `mkdir -p docs/plans/completed && mv docs/plans/20260603-cian-airflow-provider.md docs/plans/completed/`

## Post-Completion

**Проверка в реальном Airflow:**
- Установить провайдер в Docker-окружении с Airflow 2.9.1
- Создать Connection с реальным токеном Циана
- Запустить DAG вручную за один день, проверить файл на выходе
- Проверить изоляцию: запустить два DAG-а за тот же период одновременно, файлы не должны пересекаться
- Проверить что файл корректно загружается в целевое хранилище стандартными операторами

**Загрузка в хранилища (отдельная задача):**
- S3: `LocalFilesystemToS3Operator` после `CianBuilderReportsOperator`
- BigQuery: `LocalFilesystemToGCSOperator` → `GCSToBigQueryOperator` (партиция `table$YYYYMMDD`, `WRITE_TRUNCATE`)
- ClickHouse: кастомный оператор DELETE + INSERT по дате
