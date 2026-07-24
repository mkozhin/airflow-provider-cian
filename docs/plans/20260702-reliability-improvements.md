# Повышение надёжности провайдера: кеширование, Session, защита ответов, явные контракты

## Overview

Набор точечных улучшений надёжности по итогам архитектурного ревью (2026-07-02):

- `CianHook` перестаёт перечитывать Airflow-коннектор и открывать новое TCP/TLS-соединение
  на каждый HTTP-запрос (сейчас — N+1 обращений к metadata-DB/secrets-backend и N+1
  TLS-handshake за один таск с N уникальными новостройками).
- Ответ API с кодом 200, но не-JSON телом (заглушка реверс-прокси при инциденте) даёт
  осмысленный `AirflowException` вместо сырого `json.JSONDecodeError`.
- Политика ретраев остаётся зашитой в коде (решение зафиксировано), но чистится:
  джиттер к backoff и удаление лишней переменной `last_exc`. Per-request throttle
  `time.sleep(0.1)` **сохраняется** — это задокументированный client-side rate limit
  под лимит Cian ≤10 req/s (README), а не мусор (решение по итогам codex-аудита 2026-07-02).
- Фолбэк-имя новостройки `"Неизвестно"` выносится из кода хука в параметр со старым
  значением по умолчанию.
- Комбинация `add_snapshot_ts=True` + `output_format="csv"` перестаёт молча игнорироваться —
  оператор падает с `ValueError` в `__init__` (fail-fast, как уже сделано для `output_format`).
- В README фиксируется ограничение: collect- и upload-таски должны видеть общий `base_dir`
  (общая FS или один воркер) — архитектурно ничего не меняем.
- `ruff` + `mypy` добавляются в dev-зависимости и прогоняются в существующем `publish.yml`
  перед тестами (отдельный CI на push сознательно не заводится).

## Context (from discovery)

- Файлы: `airflow_provider_cian/hooks/cian.py` (97 строк — весь HTTP-транспорт),
  `airflow_provider_cian/operators/builder_reports.py` (валидация в `__init__`, `execute`),
  `README.md` / `README_ru.md`, `pyproject.toml`, `.github/workflows/publish.yml`,
  `CHANGELOG.md`.
- Паттерны: сегрегированный интерфейс `accounts.py` (ADR-0002) — не трогаем;
  `resolve_token(conn, account_id)` принимает уже полученный `conn` — кеширование коннектора
  в хуке с этим сочетается напрямую.
- Тесты: `tests/hooks/test_cian.py` (mock `requests.get` + `get_connection`),
  `tests/operators/test_builder_reports.py`. Существующие тесты мокают `CianHook.get_connection`
  на инстансе — после кеширования моки должны продолжить работать; тесты ретраев, вероятно,
  мокают/считают `time.sleep` — их нужно обновить под джиттер и удалённый `sleep(0.1)`.
- Зависимости: `apache-airflow>=2.9.1,<3.0` (целенаправленно, не менять), `requests>=2.28`.

## Зафиксированные решения (вне объёма)

Не делаем (решения из обсуждения 2026-07-02):

- CI на push/PR — пропускаем; линт/тайпчек живут в `publish.yml`.
- `ObjectStoragePath` / запись напрямую в объектное хранилище — только README-предупреждение.
- Конфигурация ретраев через connection extra — политика остаётся в коде.
- Вынос списка кабинетов из connection extra (в Variables и т.п.) — остаётся в `extra`.
- Кастомный connection type / `connection-types` в provider info — `conn_type="http"` остаётся.
- Миграция на Airflow 3.x — ветка 2.x целенаправленна.

## Development Approach

- **testing approach**: Regular (сначала код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change
- maintain backward compatibility (единственное осознанное ужесточение контракта —
  `ValueError` для `csv` + `add_snapshot_ts`, фиксируется в CHANGELOG)

## Testing Strategy

- **unit tests**: обязательны в каждой задаче (см. Development Approach)
- **e2e tests**: в проекте нет UI/e2e — не применимо
- Тестовая команда проекта: `pytest tests/ -v`
- Ретраи/джиттер тестировать детерминированно: monkeypatch `time.sleep` и `random.uniform`

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

Все изменения хука концентрируются внутри `CianHook`, его публичный интерфейс
(`get_builder_reports`, `get_newbuilding_name`, `test_connection`) не меняется —
добавляются только кешированные `conn` / `get_conn()` (стандартный паттерн Airflow-хуков)
и необязательный параметр конструктора. Инвариант ADR-0002 (оператор не касается токена)
не затрагивается. Оператор получает одну новую проверку в `__init__` и один
проброс-параметр. Никаких новых модулей и швов не появляется.

## Technical Details

### Кеш коннектора (hooks/cian.py)

- `functools.cached_property` `conn` → `self.get_connection(self.cian_conn_id)`.
- `_make_request` использует `self.conn` вместо повторного `get_connection`.
- Хук создаётся в `execute()` на каждый запуск таска — время жизни кеша равно времени
  жизни таска, ротация коннектора подхватывается следующим запуском.

### requests.Session (hooks/cian.py)

- Обычный метод `def get_conn(self) -> requests.Session` с ленивым полем `self._session`
  (НЕ `cached_property` — иначе `self.get_conn()` перестанет быть вызовом метода и
  сломается стратегия тестов `patch.object(CianHook, "get_conn")`).
- `_make_request` вызывает `self.get_conn().get(...)` вместо `requests.get(...)`.
- Заголовок `Authorization` остаётся per-request (токен резолвится через `resolve_token`
  от кешированного `conn`) — в дефолты Session токен не кладём, чтобы не расширять
  поверхность утечки.

### Защита resp.json() (hooks/cian.py)

- `resp.json()` оборачивается в `try/except ValueError` → `AirflowException` с URL и
  `resp.text[:200]` (`requests.exceptions.JSONDecodeError` — подкласс `ValueError`,
  отдельная ловля не нужна).

### Чистка ретраев (hooks/cian.py)

- Backoff остаётся `[1, 2, 4]` (в коде), к каждой задержке добавляется джиттер
  `random.uniform(0, 1)` секунд.
- `time.sleep(0.1)` в начале каждой попытки **сохраняется** как client-side rate limit
  (≤10 req/s, задокументирован в README); добавить поясняющий комментарий в код,
  чтобы его снова не приняли за мусор.
- Переменная `last_exc` удаляется — исключение поднимается сразу на последней попытке.

### Параметр фолбэк-имени (hooks/cian.py, operators/builder_reports.py)

- `CianHook.__init__(..., newbuilding_fallback_name: str = "Неизвестно")`;
  `get_newbuilding_name` возвращает `self.newbuilding_fallback_name` при `CianNotFoundError`.
- `CianBuilderReportsOperator.__init__(..., newbuilding_fallback_name: str = "Неизвестно")`,
  пробрасывается в конструктор хука в `execute()`.
- **Контракт kwargs хука**: оператор пробрасывает параметр всегда (явно), инвариант
  «оператор передаёт хуку только conn и account» осознанно расширяется до трёх kwargs —
  тест `test_execute_with_account_hook_kwargs_only_conn_and_account` обновляется
  соответствующе (параметр не является секретом, ADR-0002 не затрагивается).

### Fail-fast для CSV + snapshot_ts (operators/builder_reports.py)

- В `__init__`: `if add_snapshot_ts and output_format != "json": raise ValueError(...)`
  с текстом, отсылающим к ADR-0001 (snapshot_ts — JSON-only).
- Условие `self.output_format == "json"` в `execute()` при этом становится избыточным,
  но остаётся как защита инварианта (или упрощается — на усмотрение при реализации,
  поведение идентично).

### Линт и тайпчек (pyproject.toml, publish.yml)

- `[project.optional-dependencies] dev = ["pytest", "ruff", "mypy"]`.
- Секции `[tool.ruff]` (line-length, целевая версия py310) и `[tool.mypy]`
  (`python_version = "3.10"`, `ignore_missing_imports = true` — у Airflow/Google-провайдеров
  нет полных стабов; строгость наращивать не требуется).
- **Область линта**: `ruff check .` — весь репозиторий (пакет, тесты, examples),
  но **только дефолтный rule set ruff (E4/E7/E9, F)** — никаких расширенных select,
  чтобы задача не разрослась в стилевую чистку. Предсуществующие замечания дефолтного
  набора чистятся в рамках Task 7 (известно как минимум: неиспользуемый импорт `Account`
  в `tests/hooks/test_cian.py` — F401, он используется только в докстрингах).
  mypy — только `airflow_provider_cian`.
- В job `test` файла `publish.yml` — шаг с `ruff check .` и `mypy airflow_provider_cian`
  перед `pytest`, с условием `if: matrix.python-version == '3.10'`, чтобы линт
  не выполнялся трижды в матрице.

### CHANGELOG

- В `CHANGELOG.md` сейчас НЕТ секции `Unreleased` — её нужно создать.
- Все changelog-правки централизуются в Task 8 (одна задача — одна секция):
  Changed (breaking: `ValueError` для `csv`+`add_snapshot_ts`),
  Added (`newbuilding_fallback_name`), Fixed/Internal (кеш коннектора, Session,
  защита JSON, джиттер), Docs (README про общий FS).

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): изменения кода, тестов и документации в этом репозитории
- **Post-Completion** (без чекбоксов): ручная проверка на живом Airflow, релиз

## Implementation Steps

### Task 1: Кеш коннектора в CianHook

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `tests/hooks/test_cian.py`

- [ ] добавить `cached_property` `conn` в `CianHook`, возвращающий `self.get_connection(self.cian_conn_id)`
- [ ] заменить в `_make_request` локальный `get_connection` на `self.conn`
- [ ] написать тест: два последовательных вызова API-методов хука → `get_connection` вызван ровно один раз
- [ ] проверить и при необходимости адаптировать существующие моки `get_connection` в тестах хука и оператора
- [ ] run tests (`pytest tests/ -v`) — must pass before task 2

### Task 2: requests.Session через get_conn()

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `tests/hooks/test_cian.py`

- [ ] добавить `get_conn()` с кешированной `requests.Session` на инстансе
- [ ] перевести `_make_request` с `requests.get` на `self.get_conn().get(...)` (заголовок Authorization остаётся per-request)
- [ ] мигрировать ВСЕ моки `patch("requests.get", ...)` в `test_cian.py` (~18 мест: TestGetBuilderReports, TestGetNewbuildingName, TestTestConnection, TestCianHookWithAccountId) — стратегия: патчить `CianHook.get_conn`, возвращая mock-сессию (локализует правку, не зависит от сигнатуры Session; ассерты `call_args[1]["params"]`/`["headers"]` остаются валидны)
- [ ] убедиться, что ни один тест не делает реальных сетевых вызовов после миграции (упавший патч `requests.get` молча пропускает вызовы к Session)
- [ ] написать тест: несколько запросов используют один и тот же объект Session
- [ ] написать тест: `requests.RequestException` из Session по-прежнему оборачивается в `AirflowException`
- [ ] run tests — must pass before task 3

### Task 3: Защита resp.json() от не-JSON ответа

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `tests/hooks/test_cian.py`

- [ ] обернуть `resp.json()` в try/except `ValueError` → `AirflowException` с URL и `resp.text[:200]`
- [ ] расширить helper `_mock_response` в `test_cian.py` для не-JSON случая (`resp.json.side_effect = ValueError`, задать `resp.text`)
- [ ] написать тест: статус 200 + HTML/пустое тело → `AirflowException` с URL в сообщении (не `JSONDecodeError`)
- [ ] написать тест: валидный JSON по-прежнему возвращается без изменений
- [ ] run tests — must pass before task 4

### Task 4: Чистка ретраев — джиттер и удаление last_exc (throttle сохраняется)

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `tests/hooks/test_cian.py`

- [ ] СОХРАНИТЬ `time.sleep(0.1)` перед каждым запросом, добавить комментарий: client-side rate limit ≤10 req/s (задокументирован в README)
- [ ] добавить джиттер `random.uniform(0, 1)` к каждой задержке backoff `[1, 2, 4]`
- [ ] убрать переменную `last_exc` — поднимать исключение сразу на последней попытке
- [ ] обновить существующие тесты ретраев (счётчики/аргументы `time.sleep`), если они чувствительны к джиттеру
- [ ] написать тест: backoff-задержки между попытками лежат в диапазонах [1;2], [2;3], [4;5] (monkeypatch `time.sleep` + `random.uniform`)
- [ ] написать тест: сообщение исключения после исчерпания попыток не изменилось (номер попытки, код, URL)
- [ ] run tests — must pass before task 5

### Task 5: Параметр newbuilding_fallback_name

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/hooks/test_cian.py`
- Modify: `tests/operators/test_builder_reports.py`
- Modify: `README.md`
- Modify: `README_ru.md`

- [ ] добавить параметр `newbuilding_fallback_name: str = "Неизвестно"` в `CianHook.__init__`, использовать в `get_newbuilding_name`
- [ ] добавить одноимённый параметр в `CianBuilderReportsOperator.__init__` и пробрасывать в хук в `execute()` (всегда, явно — см. Technical Details «Контракт kwargs хука»)
- [ ] обновить `test_execute_with_account_hook_created_with_account_id` (`assert_called_once_with` с новым kwarg) и `test_execute_with_account_hook_kwargs_only_conn_and_account` (ожидаемый набор kwargs: `{"cian_conn_id", "account_id", "newbuilding_fallback_name"}`)
- [ ] написать тест хука: кастомное значение возвращается при 400 (`CianNotFoundError`)
- [ ] написать тест хука: дефолт остался `"Неизвестно"` (существующий `test_not_found_400_returns_неизвестно` продолжает проходить)
- [ ] написать тест оператора: параметр доходит до хука / попадает в `newbuilding_name` записи
- [ ] задокументировать `newbuilding_fallback_name` в таблицах параметров README.md и README_ru.md (дефолт `"Неизвестно"`)
- [ ] run tests — must pass before task 6

### Task 6: ValueError для add_snapshot_ts + csv

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`
- Modify: `README.md`
- Modify: `README_ru.md`

- [ ] добавить в `__init__` проверку: `add_snapshot_ts=True` при `output_format != "json"` → `ValueError` с отсылкой к ADR-0001
- [ ] написать тест: комбинация `csv` + `add_snapshot_ts=True` → `ValueError` при конструировании
- [ ] написать тест: `json` + `add_snapshot_ts=True` по-прежнему конструируется без ошибок
- [ ] обновить/заменить тест `test_snapshot_ts_not_in_csv_even_when_flag_on` (комбинация теперь недостижима)
- [ ] исправить README.md:71 и README_ru.md:71: вместо «Ignored» / «не влияет» для CSV — «unsupported, raises ValueError» (таблица параметров + JSON-only заметка)
- [ ] run tests — must pass before task 7

### Task 7: ruff + mypy в dev-зависимостях и publish.yml

**Files:**
- Modify: `pyproject.toml`
- Modify: `.github/workflows/publish.yml`

- [ ] добавить `ruff` и `mypy` в `[project.optional-dependencies] dev`
- [ ] добавить секции `[tool.ruff]` (ТОЛЬКО дефолтный rule set — E4/E7/E9, F; без расширенных select) и `[tool.mypy]` (py310, `ignore_missing_imports = true`)
- [ ] добавить в job `test` файла `publish.yml` шаг линта/тайпчека перед `pytest` с `if: matrix.python-version == '3.10'` (один прогон вместо трёх)
- [ ] прогнать `ruff check .` и `mypy airflow_provider_cian` локально, устранить замечания дефолтного набора (стилевые правки вне дефолтных правил — вне объёма)
- [ ] почистить известные предсуществующие замечания: неиспользуемый импорт `Account` в `tests/hooks/test_cian.py` (F401)
- [ ] run tests — must pass before task 8

### Task 8: README — ограничение общего base_dir; CHANGELOG

**Files:**
- Modify: `README.md`
- Modify: `README_ru.md`
- Modify: `CHANGELOG.md`

- [ ] добавить в оба README раздел-предупреждение: оператор пишет в локальный `base_dir`, путь передаётся через XCom — collect- и upload-таски должны выполняться на одном воркере или видеть общую FS (Celery/K8s без общего тома — не поддерживается из коробки)
- [ ] сослаться на примеры (`examples/bq_and_s3_multi_account_dag.py` и др.) как на схему, где это требование действует
- [ ] создать секцию `## Unreleased` в `CHANGELOG.md` (её сейчас нет) и внести ВСЕ изменения плана одной правкой: Changed (breaking: `ValueError` для `csv`+`add_snapshot_ts`), Added (`newbuilding_fallback_name`), Fixed/Internal (кеш коннектора, Session, защита JSON, джиттер), Docs (общий FS)
- [ ] проверить консистентность README.md и README_ru.md между собой (включая правки из задач 5 и 6)
- [ ] run tests — must pass before task 9

### Task 9: Verify acceptance criteria

- [ ] verify all requirements from Overview are implemented
- [ ] verify edge cases are handled (не-JSON 200, исчерпание ретраев, csv+snapshot_ts, кастомный fallback)
- [ ] `grep resolve_token airflow_provider_cian/operators/` пуст — инвариант ADR-0002 не нарушен
- [ ] run full test suite: `pytest tests/ -v`
- [ ] run `ruff check .` и `mypy airflow_provider_cian` — чисто
- [ ] verify test coverage meets project standard (новые ветки покрыты)

### Task 10: [Final] Update documentation

- [ ] update README.md if needed (сверх задачи 8 — если по ходу реализации что-то изменилось)
- [ ] update CONTEXT.md, если появились новые термины/инварианты
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Ручные шаги вне этого репозитория — без чекбоксов*

**Manual verification:**
- прогнать реальный DAG (single- и multi-account) на живом Airflow: убедиться, что за таск
  выполняется одно чтение коннектора (по логам secrets-backend/metadata-DB) и что выгрузка
  идентична прежней
- проверить поведение при недоступном API: задержки с джиттером в логах, итоговое сообщение об ошибке

**External system updates:**
- при следующем релизе (тег `v*`) убедиться, что новый шаг lint/mypy в `publish.yml` зелёный
- потребители, использовавшие `csv` + `add_snapshot_ts=True` (молча игнорировалось),
  получат `ValueError` — упомянуть в release notes
