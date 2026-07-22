# Code-review fixes for account_id branch

## Overview

- Устраняет три находки code-review по ветке `account-id-field` (фича `account_id` в выгрузке). Все три — с выбранным подходом B (обсуждено 2026-07-21).
- Что решаем:
  1. **Path safety / контракт `date`**: `_build_path` санитизирует `run_id` и `cabinet_id`, но `self.date` (template field) подставляется в имя файла как есть — единственный несанитизированный компонент пути. Плюс `date` семантически обязана быть `YYYY-MM-DD` (уходит в имена BQ-партиций `$YYYYMMDD`, в S3-пути `_year=/_month=/_day=`). Решение: строгая валидация формата в начале `execute()`.
  2. **Silent wrong-token в `resolve_token`**: после введения санитизации ключа поиска две записи `extra.accounts`, схлопывающиеся в один id после санитизации (напр. сырые `"a.b"` и `"a_b"` → `"a_b"`), приводят к молчаливому выбору токена первой записи. `list_accounts()` такой случай дедуплицирует с WARNING, а `resolve_token` (runtime) — нет. Решение: детектить неоднозначность и падать явной ошибкой.
  3. **Дублирование `sanitize_id` в примере**: `examples/bq_and_s3_multi_account_dag.py` держит локальный `safe_id()` с тем же регэкспом, что и публичный `sanitize_id`. Диффом мы объявили `sanitize_id` «единым источником», но пример катает свой. Латентный риск: если `sanitize_id` изменят, пути примера молча разойдутся с путями оператора. Решение: пример использует `sanitize_id` из пакета.

## ⚠️ Важно: пункт 2 разворачивает задокументированный инвариант

Поведение `resolve_token` «no dedup / первое совпадение / молчит на runtime» — это **осознанное задокументированное решение** (завершённый план `docs/plans/completed/20260625-account-resolution-module.md`: «БЕЗ дедупа (первое совпадение)», «молчит… не регрессировать» относительно старого `_make_request`). Пункт 2 сознательно меняет этот контракт: при коллизии двух записей теперь падаем, а не берём первую. Пользователь подтвердил это решение 2026-07-21 (потенциальный источник скрытых ошибок в будущем перевешивает старую совместимость с first-match). Как следствие пункт 2 обязан:
- обновить существующий тест, кодифицирующий старое поведение (`test_no_warnings_logged_when_duplicates_in_accounts`);
- обновить docstring `resolve_token` (сейчас говорит «first account»);
- обновить публичную документацию `README.md:187` и `README_ru.md:186` (они прямо обещают «первую запись» — после правки станут неверны);
- задокументировать смену контракта в CONTEXT.md и CHANGELOG (0.5.0, пометка `**BREAKING**`), зафиксировав намеренную асимметрию: `list_accounts` дедуплицирует с WARNING (берёт первый) — `resolve_token` теперь падает на неоднозначности.

**Почему `**BREAKING**` оправдан (проверено против `main`, ответ на plan-review Minor #1):** `resolve_token` — публичная функция с релиза 0.3.0/0.4.0. Хотя санитизация *ключа поиска* — нововведение 0.5.0, `acc.id` **всегда** санитизирован (`Account.__post_init__`) уже в релизе, поэтому записи `"a.b"` и `"a/b"` и в 0.4.0 давали один хранимый `acc.id == "a_b"`; поиск `resolve_token(conn, "a_b")` в релизе молча возвращал первую (`tok1`). Значит коллизия-first-match — достижимое поведение релизной публичной функции, и переход на исключение его меняет → BREAKING. Оговорка: `resolve_token` читает `extra.accounts` заново через `_parse_accounts` (без дедупликации), поэтому дедуп в `list_accounts()` не сужает радиус — коллизирующий коннектор теперь громко падает в runtime для затронутого кабинета (задуманное fail-loud-поведение).

## Context (from discovery)

- **Файлы:**
  - `airflow_provider_cian/operators/builder_reports.py` — валидация `date` в `execute()` (пункт 1). `import re` был убран из файла в ходе фичи — вернуть.
  - `airflow_provider_cian/accounts.py` — детект коллизии в `resolve_token` (пункт 2).
  - `examples/bq_and_s3_multi_account_dag.py` — заменить `safe_id` на `sanitize_id` (пункт 3).
  - Тесты: `tests/operators/test_builder_reports.py`, `tests/test_accounts.py`, `tests/test_example_dag_multi_account.py`.
  - Документация (пункт 2, смена контракта): `CONTEXT.md`, `CHANGELOG.md`, `README.md`, `README_ru.md`.
- **Существующий код (проверено против репозитория):**
  - `_build_path(self, run_id, cabinet_id)` → `os.path.join(self.base_dir, cabinet_id, sanitize_id(run_id), f"{self.date}.{ext}")`. `self.date` не санитизируется.
  - `execute()` уже имеет ранний fail-fast `if not cabinet_id` (пункт 1 добавляется РАНЬШЕ него — самый дешёвый чек).
  - `resolve_token` (multi-account ветка): цикл `for acc, token in _parse_accounts(conn): if acc.id == canonical: ...`. Первое совпадение возвращается. Тексты ошибок содержат подстроки `"not found in connection"` и `"missing required 'token' field"` — они проверяются существующими тестами, менять их нельзя.
  - `_parse_accounts` документирован как «no dedup» — не трогаем его контракт; дедуп/детект делаем в `resolve_token`.
  - Пример: `def safe_id(run_id): return re.sub(r"[^\w-]", "_", run_id)`, вызывается в `make_gcs_params`, `make_bq_params`, `cleanup` (3 вызова). `cabinet_id` в примере берётся из `account.id` (уже санитизирован `Account`).
  - `sanitize_id(value: str) -> str` — публичная функция в `airflow_provider_cian/accounts.py`.

## Development Approach

- **testing approach**: Regular (код, затем тесты в рамках каждой задачи)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: каждая кодовая задача включает новые/обновлённые тесты** (Задачи 1–3; Задача 4 — verify, Задача 5 — closure)
- **CRITICAL: все тесты должны проходить перед началом следующей задачи**
- run tests after each change
- обратная совместимость: пункт 1 добавляет новую валидацию (задачи с невалидной `date` теперь падают явной ошибкой вместо записи файла с кривым путём) — осознанное ужесточение, не требует bump мажора отдельно (уже в рамках 0.5.0)

## Testing Strategy

- **unit tests**: pytest, `tests/operators/test_builder_reports.py`, `tests/test_accounts.py`, `tests/test_example_dag_multi_account.py`
- **e2e tests**: нет в проекте; structure-тесты примеров уже импортируют модуль примера и упадут при поломке импорта. Пункт 3 дополнительно закрывается поведенческим тестом (`run_id` со спецсимволом → санитизированный сегмент), а не только import-тестом
- прогнать весь suite `pytest` в конце

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Solution Overview

- Пункт 1: ранняя проверка в `execute()` — строгий ASCII-контракт `YYYY-MM-DD` + календарная валидность, которая закрывает path-traversal через `date`, режет Unicode-цифры/не-строку и ловит опечатки рано.
- Пункт 2: в `resolve_token` собрать все совпадения по каноническому ключу; при >1 — падать с явной ошибкой про неоднозначность; при 1 — прежнее поведение (проверка токена/возврат); при 0 — прежняя ошибка «not found». Тексты существующих ошибок сохраняются дословно.
- Пункт 3: пример импортирует `sanitize_id`, локальный `safe_id` удаляется, `import re` в примере убирается, если больше нигде не используется.

## Technical Details

- **Пункт 1 — валидация `date`:**
  - Вернуть `import re` в `builder_reports.py`.
  - В **самый верх** `execute()` — ДО вычисления `snapshot_ts` (при дефолтном `add_snapshot_ts=False` условие `snapshot_ts` короткозамыкается и не трогает `context["dag_run"]`, поэтому тест на невалидную `date` может обойтись минимальным контекстом без `dag_run`, как `test_execute_with_empty_account_id_raises`):
    ```python
    # ASCII-only zero-padded shape (\d is Unicode-aware — would pass full-width digits),
    # then real calendar validity via strptime. Non-str/None → отсекается isinstance → AirflowException.
    if not (isinstance(self.date, str)
            and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.date)):
        raise AirflowException(
            f"date must be an ISO date string (YYYY-MM-DD), got {self.date!r}"
        )
    try:
        _datetime.strptime(self.date, "%Y-%m-%d")
    except ValueError as e:
        raise AirflowException(
            f"date must be a valid calendar date (YYYY-MM-DD), got {self.date!r}"
        ) from e
    ```
  - **Почему не голый `\d`-regex (правка по codex-review):** `\d` в Python Unicode-aware — пропускает полноширинные цифры (`２０２６-０７-２１`); `[0-9]` берёт только ASCII. `isinstance(str)` убирает `TypeError` на `None`/не-строке. `strptime(self.date, "%Y-%m-%d")` отсекает календарно-невозможные (`2026-13-45`, `2024-02-30`). Полагаться на «это ловит Cian API» нельзя — в коде хука такой гарантии нет. (`_datetime` уже импортирован в `builder_reports.py`; `strptime` — единый выбор, без `fromisoformat`.)
  - Отвергает: `2024/01/15`, `2024-1-5`, `../../evil`, `""`, `2026-13-45`, `2024-02-30`, `２０２６-０７-２１`, `None`/не-строку. Принимает строго zero-padded реальный ISO (`2024-01-15`).
- **Пункт 2 — детект коллизии в `resolve_token`:**
  - Заменить цикл на сбор совпадений, сохранив тексты ошибок:
    ```python
    if account_id is not None:
        canonical = sanitize_id(account_id)
        matches = [tok for acc, tok in _parse_accounts(conn) if acc.id == canonical]
        if len(matches) > 1:
            raise AirflowException(
                f"Account id={account_id!r} is ambiguous in connection {conn.conn_id!r}: "
                f"{len(matches)} accounts collapse to the same sanitized id {canonical!r}"
            )
        if not matches:
            raise AirflowException(
                f"Account id={account_id!r} not found in connection {conn.conn_id!r} extra.accounts"
            )
        token = matches[0]
        if not token:
            raise AirflowException(
                f"Account id={account_id!r} found in connection "
                f"{conn.conn_id!r} but is missing required 'token' field"
            )
        return token
    ```
  - Подстроки `"not found in connection"` и `"missing required 'token' field"` сохранены дословно (проверяются тестами). Новое сообщение про неоднозначность их не содержит — ложных срабатываний match-тестов не будет.
  - Обновить docstring `resolve_token` (`accounts.py:111-115`): убрать «finds the **first** account», описать детект неоднозначности.
  - Сломается существующий тест `test_no_warnings_logged_when_duplicates_in_accounts` (`tests/test_accounts.py:332-339`): он заводит коллизию `{"id":"a.b"}` + `{"id":"a/b"}` (обе → `a_b`) и ждёт `resolve_token(conn, "a_b") == "tok1"`. Переписать его на ожидание новой `AirflowException` про неоднозначность, СОХРАНИВ исходный смысл `mock_log.warning.assert_not_called()` (на runtime по-прежнему без WARNING — падаем исключением, не логируем).
  - Документирование смены контракта: CONTEXT.md (статья про resolve_token / резолюцию аккаунтов) + запись в CHANGELOG 0.5.0 про то, что `resolve_token` теперь падает на неоднозначной коллизии id (асимметрия с `list_accounts`, который дедуплицирует с WARNING).
- **Пункт 3 — пример на `sanitize_id`:**
  - Импорт: `from airflow_provider_cian.accounts import Account, list_accounts, sanitize_id`.
  - Удалить `def safe_id(...)`, заменить 3 вызова `safe_id(...)` → `sanitize_id(...)`.
  - Проверить, используется ли `import re` в примере ещё где-то; если нет — удалить.
  - **Поведенческий тест (правка по codex-review, вариант B):** вместо мока внутренностей — прогнать существующий поведенческий тест с `run_id` со спецсимволом. В `tests/test_example_dag_multi_account.py` есть `TestCleanup`, который реально зовёт `cleanup(...)`; аналогично `make_gcs_params`/`make_bq_params` строят путь из `run_id`. Взять один из них с `run_id="run.1"` и проверить, что в пути/`run_dir` сегмент `run_1` (санитизация применена). Это доказывает использование канонизации, не завязываясь на имя функции. Заодно обновить устаревший комментарий про `safe_id(run_id)` в `tests/test_example_dag_multi_account.py:164`.

## What Goes Where

- **Implementation Steps** (`[ ]`): изменения кода/тестов/примера в этом репозитории.
- **Post-Completion** (без чекбоксов): пункт 3 формально вне scope исходной задачи про `account_id` (в примере диффом менялась только `BQ_SCHEMA`) — включён по решению пользователя 2026-07-21.

## Implementation Steps

### Task 1: Validate date format in execute()

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [x] вернуть `import re` в `builder_reports.py`
- [x] в **самый верх** `execute()` (до вычисления `snapshot_ts`) добавить строгую проверку из Technical Details: ASCII-regex `[0-9]{4}-[0-9]{2}-[0-9]{2}` + `isinstance(str)` + календарная валидация `strptime`/`fromisoformat`, все ошибки → `AirflowException`
- [x] написать тест: параметризованный `test_execute_with_invalid_date_raises` на кейсы `"../../evil"`, `"2024/01/15"`, `"2024-1-5"`, `"not-a-date"`, `""`, `"2026-13-45"`, `"2024-02-30"`, `"２０２６-０７-２１"` (Unicode-цифры), `None` — падает `AirflowException` (match про date), значение/тип отражены в тексте, hook НЕ создаётся (`MockHook.assert_not_called()`)
- [x] в этом же тесте (правка по codex/plan-review): патчить `airflow_provider_cian.accounts.BaseHook.get_connection` и проверять `assert_not_called()` — доказать, что валидация идёт ДО `resolve_cabinet_id()`/чтения connection. **ВАЖНО:** этот ассерт осмыслен только в single-account режиме (`account_id=None`), т.к. `resolve_cabinet_id` читает connection лишь при `account_id is None`; в multi-режиме она возвращается раньше и `get_connection` не зовётся вообще (ассерт был бы вакуумным). Тест-кейс для этого ассерта — с `account_id=None`
- [x] проверить, что валидный `date="2024-01-15"` в существующих execute-тестах по-прежнему проходит (без изменений)
- [x] run tests — must pass before task 2

### Task 2: Detect ambiguous account_id collision in resolve_token

**Files:**
- Modify: `airflow_provider_cian/accounts.py`
- Modify: `tests/test_accounts.py`
- Modify: `CONTEXT.md`
- Modify: `CHANGELOG.md`
- Modify: `README.md`
- Modify: `README_ru.md`

- [x] в multi-account ветке `resolve_token` собрать все совпадения по `canonical` в список; при `len > 1` — `AirflowException` про неоднозначность (текст из Technical Details, с исходным `account_id`, `conn_id`, каноническим id и числом совпадений); при 0 — прежняя ошибка «not found»; при 1 — прежнее поведение (проверка пустого токена / возврат)
- [x] сохранить дословно тексты ошибок `"not found in connection"` и `"missing required 'token' field"`
- [x] обновить docstring `resolve_token`: убрать «finds the **first** account», описать детект неоднозначности; заодно убрать/актуализировать устаревшую фразу «matching current `_make_request` behavior» (`accounts.py:118`)
- [x] **расширить существующий тест** `test_no_warnings_logged_when_duplicates_in_accounts` (`tests/test_accounts.py:332-339`) — он уже заводит коллизию `"a.b"`+`"a/b"` (обе → `a_b`): теперь ожидать `AirflowException` про неоднозначность, сохранив `mock_log.warning.assert_not_called()` (на runtime без WARNING — падаем исключением). Это и есть collision-тест — отдельный НЕ добавляем (правка по codex-review: не дублировать)
- [x] в этом тесте (правка по codex-review) закрепить диагностическую полноту сообщения: проверить наличие исходного `account_id`, `conn_id`, канонического id и признака множественности (число совпадений / «collapse»)
- [x] **обязательно** переименовать `test_multi_mode_returns_first_matching_token` (`tests/test_accounts.py:277`) → `test_multi_mode_matches_sanitized_account_id` — «first matching» больше не концепция (правка по codex-review: сделать обязательным)
- [x] НЕ добавлять новые тесты на unique/not-found/missing-token — они уже покрыты существующими тестами (`tests/test_accounts.py:272+`) с точными текстами ошибок; просто прогнать их и убедиться, что не сломались (правка по codex-review: не дублировать покрытие)
- [x] обновить `README.md:187` и `README_ru.md:186` (правка по codex-review): убрать обещание «первой записи» как поведения при коллизии, добавить «падает при неоднозначной коллизии id», сохранив тезис про канонический ключ поиска (сырой `account_id` находит санитизированную запись)
- [x] обновить CONTEXT.md: `resolve_token` теперь падает на неоднозначной коллизии id (асимметрия с `list_accounts`, который дедуплицирует с WARNING)
- [x] обновить CHANGELOG.md (секция 0.5.0), пометить как `- **BREAKING**:` — `resolve_token` теперь падает на неоднозначной коллизии id вместо молчаливого выбора первой записи
- [x] run tests — must pass before task 3

### Task 3: Use sanitize_id in the multi-account example

**Files:**
- Modify: `examples/bq_and_s3_multi_account_dag.py`
- Modify: `tests/test_example_dag_multi_account.py`

- [x] добавить `sanitize_id` в импорт из `airflow_provider_cian.accounts`
- [x] удалить локальную `def safe_id(...)`, заменить 3 вызова `safe_id(...)` на `sanitize_id(...)` (`make_gcs_params`, `make_bq_params`, `cleanup`)
- [x] удалить `import re` из примера, если он больше нигде не используется (проверить grep)
- [x] поведенческий тест (правка по codex-review, вариант B): прогнать существующий поведенческий хелпер (`TestCleanup` или `make_gcs_params`/`make_bq_params`) с `run_id="run.1"` и проверить, что в пути/`run_dir` сегмент `run_1` — доказывает, что санитизация реально применяется (не мокая внутренности)
- [x] обновить устаревший комментарий про `safe_id(run_id)` в `tests/test_example_dag_multi_account.py:164`
- [x] прогнать существующие тесты примера, чтобы импорт/структура не сломались
- [x] run tests — must pass before task 4

### Task 4: Verify acceptance criteria

- [x] verify: невалидная `date` (traversal, мусор, календарно-невозможная, Unicode-цифры, не-строка) падает `AirflowException` до создания hook И до чтения connection; валидная `YYYY-MM-DD` работает как раньше
- [x] verify: `resolve_token` при коллизии двух записей падает явной ошибкой с диагностическими полями, штатные уникальные/отсутствующие/без-token случаи не изменились; docstring обновлён (нет «first account»); смена контракта отражена в CONTEXT.md, README.md/README_ru.md и CHANGELOG 0.5.0 (BREAKING)
- [x] verify: пример импортирует и использует `sanitize_id`, `safe_id` удалён, лишний `import re` убран; поведенческий тест доказывает санитизацию `run_id`; пути примера логически совпадают с путём оператора (тот же `sanitize_id`)
- [x] run full test suite: `pytest`

### Task 5: [Final] Close plan

*Все правки документации (CONTEXT.md/README/CHANGELOG) выполняются в Task 2 — здесь только закрытие плана.*

- [x] move this plan to `docs/plans/completed/` (harness performs the actual move after all phases)

## Post-Completion

*Informational only — no checkboxes*

- Пункт 3 (пример на `sanitize_id`) формально вне scope исходной задачи про `account_id`; включён по явному решению пользователя.
- Правки входят в тот же незарелиженный 0.5.0 (ветка `account-id-field`) — отдельного релиза не требуют; коммитить решает пользователь (отдельными коммитами по задачам или одним).
