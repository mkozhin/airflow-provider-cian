# Деуглубление: модуль Account resolution (`accounts.py`)

## Overview

Рефакторинг резолюции **Account** в один модуль `airflow_provider_cian/accounts.py` со **свободными функциями** и **сегрегированным интерфейсом** (идентичность отдельно от токена). Сейчас концептуальный форк **«single vs multi-account mode»** разорван по двум файлам, а парсинг `extra.accounts` дублируется.

**Точная природа изменения (важно, не путать два уровня):**
- **Поведение оператора/хука и формат вывода — НЕ меняются** (внешнее наблюдаемое поведение пайплайна сохраняется; это проверяется регрессией существующих тестов).
- **Публичная поверхность импортов — МЕНЯЕТСЯ (breaking):** `Account` и `get_accounts` удаляются из `airflow_provider_cian.hooks.cian` и `airflow_provider_cian.hooks`, переезжают в `airflow_provider_cian.accounts` (`get_accounts` → `list_accounts`). Это breaking change для любого, кто их импортирует (README это документирует). **Санкционировано:** фича ещё не в проде, потребителей нет (ре-экспорт/shim сознательно НЕ делаем). В CHANGELOG отражается как breaking.

**Проблема (friction):**
- `operator.execute()` выводит `cabinet_id` (multi: `account_id`; single: `conn.login`).
- `hook._make_request()` выводит токен (multi: из `extra.accounts`; single: `conn.password`).
- Это **один и тот же** форк single/multi, разорванный по двум файлам.
- Парсинг `extra.accounts` + санитизация `Account` **дублируется** в `get_accounts()` и `_make_request()`.

**Выгода (locality + leverage):** вся логика резолюции Account живёт в одном доме; оператор и хук становятся тонкими вызывающими. Будущие изменения (новый источник id, новая схема auth) трогают один модуль. Deletion-тест на модуль: удалить → разбор `extra.accounts`, санитизация, форк single/multi и фолбэки расползаются обратно в 3 места (оператор, хук, `get_accounts`) → концентрирует.

**Не относится к фиче `snapshot_ts`** (`docs/plans/20260625-add-snapshot-ts.md`) — независимый рефакторинг, не смешивать.

## Context (from discovery)

- Файлы:
  - `airflow_provider_cian/hooks/cian.py` — содержит `Account`, `get_accounts`, `CianHook._make_request` (инлайн-резолюция токена, строки ~108-137)
  - `airflow_provider_cian/hooks/__init__.py` — **ре-экспортит** `Account, CianHook, CianNotFoundError, get_accounts` в `__all__` (легко пропустить — ломается при удалении имён из `cian.py`)
  - `airflow_provider_cian/operators/builder_reports.py` — `execute()` (инлайн-вывод `cabinet_id`, строки ~63-70); `:13` импортирует `Account` из `hooks.cian`; `:69` — `Account(id=conn.login).id`
  - `airflow_provider_cian/accounts.py` — **создаётся**
- Точки использования публичного API (clean break, ре-экспорт НЕ нужен — фича не в проде):
  - `airflow_provider_cian/hooks/__init__.py` — `Account`, `get_accounts` в импорте и `__all__`
  - `examples/bq_and_s3_multi_account_dag.py:56` — `from airflow_provider_cian.hooks.cian import Account, get_accounts`; `:257` — `get_accounts(CIAN_CONN_ID)`; `:34` — упоминание в комментарии
  - `tests/hooks/test_cian.py:10` — импорт `Account, CianHook, CianNotFoundError, get_accounts`; `:345-405` — 8 вызовов `get_accounts`; `:391` — patch `airflow_provider_cian.hooks.cian.log` (для WARNING дедупа)
  - `tests/test_example_dag_multi_account.py:10` — импорт `Account`; `:18` — patch-таргет `airflow_provider_cian.hooks.cian.get_accounts`; `:13-17` — комментарий с обоснованием patch-механики
  - `tests/operators/test_builder_reports.py:14` — импорт `Account` из `hooks.cian`
  - Доки: `README.md:97-102`, `README_ru.md:97-102` (импорт + вызов `get_accounts`), `CHANGELOG.md:5` (`Account` + `get_accounts(conn_id) in CianHook`)
- Команда тестов: `pytest` (`testpaths = ["tests"]`).
- Инвариант (`CONTEXT.md` → Flagged ambiguities): токен **никогда** не сериализуется в метадату Airflow; оператор знает только `account_id`, токен достаёт хук.

## Development Approach

- **Testing approach**: TDD-aligned, как в проекте; тесты в каждой задаче обязательны, `pytest` зелёный перед следующей.
- **Поведение оператора/хука сохраняется** (формат вывода, тексты ошибок, число `get_connection`), но **публичные импорты — breaking** (см. Overview). Сохранение поведения подтверждается регрессией: существующие `TestExecuteWithAccount`, тесты хука и перенесённые тесты `get_accounts`→`list_accounts` проходят.
- **Стратегия зелёного gate при чистом разрыве:** перенос имён (`Account`, `get_accounts`) атомарен — нельзя «переключить половину», иначе `pytest` краснеет на сборке (ломаются `hooks/__init__.py`, оператор, тест-импорты). Поэтому: **Task 1 строит `accounts.py` в полной изоляции** (старый `hooks/cian.py` не трогаем; `Account` временно дублируется в двух модулях — оба зелёные), а **Task 2 — единый атомарный switchover всех потребителей** за один проход. Это честнее транзитного shim и держит зелёный на каждом gate.

## Testing Strategy

- **Unit-тесты** для нового модуля `accounts.py`:
  - `list_accounts` — успешный разбор, дедуп санитизированных id, пропуск записей без `id`, возврат `[]` при отсутствии коннектора/ошибке (defensive); **WARNING-логи присутствуют** (skip-missing-id, duplicate) через patch `airflow_provider_cian.accounts.log`
  - `resolve_cabinet_id` — multi (`account_id`) **без чтения коннектора** (assert: `BaseHook.get_connection` НЕ вызван в multi-режиме); single c `conn.login`; single без `conn.login` → `None`; токен/директория не запрашиваются
  - `resolve_token` — multi (токен из `extra.accounts`), single (`conn.password`); ошибки: account не найден, у account нет `token`, нет `conn.password` — с **дословно теми же** сообщениями, что сейчас в `_make_request`; **WARNING-логи ОТСУТСТВУЮТ** (assert: при дублях/skip в execution-time хук молчит — политика отлична от `list_accounts`)
- **Регрессия (поведение оператора/хука сохранено):** существующие `TestExecuteWithAccount`, тесты хука и перенесённые тесты `get_accounts`→`list_accounts` проходят. Отдельно проверить: в multi-режиме оператора число вызовов `get_connection` не выросло (нет лишнего раннего fetch).
- **E2E**: нет UI/e2e — не применимо.

## Solution Overview

Новый модуль `accounts.py` — свободные функции, дом концепта «Account resolution». Интерфейс **сегрегирован**: `resolve_cabinet_id` (только идентичность, токена не касается) и `resolve_token` (только токен) — две разные функции, **НЕ** единый `resolve() -> (cabinet_id, token)`. Так оператор физически не получает токен (зовёт только `resolve_cabinet_id`), а хук — только токен. Эта сегрегация фиксируется в ADR-0002, чтобы её не «упростили» обратно.

## Technical Details

**`accounts.py` (свободные функции):**
- `@dataclass Account` — переезжает из `hooks/cian.py` (санитизация id в `__post_init__`).
- `_parse_accounts(conn) -> list[tuple[Account, str | None]]` (приватная) — **чистый** разбор `conn.extra_dejson["accounts"]`: санитизация id, сохранение токена. **БЕЗ логирования и БЕЗ дедупа** (политику накладывают вызывающие — см. «Сохранение поведения» ниже). Молча пропускает entry без `id` (как сейчас `_make_request`). Единственное место итерации директории.
- `list_accounts(conn_id: str) -> list[Account]` — бывший `get_accounts`. **Defensive** (ловит любые ошибки → `[]`): зовётся на parse-time в DAG, не должен ломать парсинг. Принимает `conn_id` (сам берёт коннектор). **Сохраняет текущее поведение `get_accounts`: дедуп санитизированных id + WARNING-логи** (skip-missing-id, duplicate). Токены наружу не отдаёт.
- `resolve_cabinet_id(conn_id: str, account_id: str | None) -> str | None` — токен-free, директория-free. **Принимает `conn_id` и читает коннектор ЛЕНИВО — только в single-режиме** (`account_id is None` → `Account(id=conn.login).id` если `conn.login`, иначе `None`). В multi-режиме возвращает `account_id` **без обращения к коннектору** (сохраняет текущее поведение: в multi оператор conn не читает). Ошибки в single-режиме пробрасывает.
- `resolve_token(conn, account_id: str | None) -> str` — только токен: при `account_id` — поиск первого совпадения через `_parse_accounts`, иначе `conn.password`. **БЕЗ warnings и БЕЗ дедупа** (как сейчас в `_make_request`). Пробрасывает `AirflowException` с **дословно теми же сообщениями** (account не найден / нет `token` / нет `conn.password`).

**Сигнатуры — `conn_id` vs `conn` (намеренная асимметрия, обусловлена сохранением поведения):**
- `list_accounts(conn_id)` и `resolve_cabinet_id(conn_id, …)` — берут `conn_id`, читают коннектор сами и ЛЕНИВО: `resolve_cabinet_id` в multi-режиме коннектор не трогает → **в multi-режиме НЕ появляется лишний ранний `get_connection`** (текущее поведение: оператор в multi conn не читает).
- `resolve_token(conn, …)` — берёт уже полученный `conn`, т.к. `_make_request` всё равно читает коннектор для `base_url`; повторный fetch не нужен.

**Сохранение поведения (критично — две разные политики над одним парсером):**
- `list_accounts`: дедуп + WARNING-логи + defensive-catch → `[]` (как `get_accounts` сейчас). Логгер — `airflow_provider_cian.accounts.log`.
- `resolve_token`: **молчит** (никаких warnings в execution-time хука), без дедупа (первое совпадение), дословные тексты исключений. Это поведение `_make_request` сегодня — не регрессировать.
- Тесты обязаны зафиксировать ОБЕ политики раздельно (см. Task 1).

**Вызывающие после рефакторинга:**
- `CianHook._make_request`: получает `conn = self.get_connection(...)`, затем `token = resolve_token(conn, self.account_id)`. Инлайн-блок резолюции токена удаляется. **`Account` хуку больше НЕ нужен** (санитизация ушла внутрь `resolve_token`) — импорт `Account` из хука убирается.
- `CianBuilderReportsOperator.execute`: `cabinet_id = resolve_cabinet_id(self.cian_conn_id, self.account_id)` — **оператор сам коннектор не читает** (в multi-режиме его и не нужно читать). Инлайн-ветвление cabinet_id удаляется. Хук по-прежнему создаётся с `account_id` (токен достаёт сам). Импорт `Account` из оператора убирается.

## What Goes Where

- **Implementation Steps** (`[ ]`): новый модуль `accounts.py` целиком (Task 1, изолированно), атомарный switchover всех потребителей (Task 2), ADR-0002, верификация, доки.
- **Post-Completion** (без чекбоксов): публикация версии на PyPI (breaking — bump версии); потребители ВЫХОДНЫХ данных не затронуты (формат вывода тот же), но потребители ИМПОРТОВ `Account`/`get_accounts` из `hooks` сломаются (в проде таких нет — санкционировано).

## Implementation Steps

> **Порядок задач обусловлен зелёным gate.** Перенос `Account`/`get_accounts` атомарен (см. Development Approach): нельзя удалить имена из `hooks/cian.py`, не переключив одновременно `hooks/__init__.py`, оператор и все тест-импорты, иначе `pytest` краснеет уже на сборке. Поэтому новый модуль строится изолированно (Task 1), а единственный «разрыв» делается одним атомарным проходом (Task 2).

### Task 1: Создать `accounts.py` целиком (изолированно, старый код не трогаем)

**Files:**
- Create: `airflow_provider_cian/accounts.py`
- Create: `tests/test_accounts.py`

- [x] создать `accounts.py`; определить `@dataclass Account` (санитизация id в `__post_init__`) — **копия**, `hooks/cian.py` пока не трогаем (временное сосуществование двух `Account` — оба модуля зелёные)
- [x] реализовать приватный `_parse_accounts(conn) -> list[tuple[Account, str | None]]` — **чистый** разбор `extra.accounts`: санитизация id, токен; **БЕЗ логирования и БЕЗ дедупа**; молча пропускает entry без `id` (политику накладывают вызывающие)
- [x] реализовать `list_accounts(conn_id)` — defensive (ошибки → `[]`); поверх `_parse_accounts` добавляет **дедуп санитизированных id + WARNING-логи** (skip-missing-id, duplicate) — сохранить текущее поведение `get_accounts`; логгер `airflow_provider_cian.accounts.log`; наружу только `list[Account]` (без токенов)
- [x] реализовать `resolve_cabinet_id(conn_id, account_id) -> str | None` — токен-free, директория-free; **в multi-режиме (`account_id`) коннектор НЕ читает**, возвращает `account_id`; в single-режиме (`account_id is None`) ЛЕНИВО читает коннектор → `Account(id=conn.login).id` если `conn.login`, иначе `None`
- [x] реализовать `resolve_token(conn, account_id) -> str` — поверх `_parse_accounts`: multi (первое совпадение по id), single (`conn.password`); **БЕЗ warnings, БЕЗ дедупа**; дословно те же сообщения `AirflowException` (account не найден / нет `token` / нет `conn.password`)
- [x] написать тесты `tests/test_accounts.py`: `Account` (санитизация `a.b`→`a_b`); `list_accounts` (разбор+дедуп, пропуск без `id`, `[]` при ошибке/отсутствии коннектора, **WARNING присутствуют** через patch `airflow_provider_cian.accounts.log`); `resolve_cabinet_id` (multi **без вызова `get_connection`** / single+login / single без login → None); `resolve_token` (multi-успех / single-успех / account не найден / нет `token` / нет `conn.password`, **WARNING ОТСУТСТВУЮТ** при дублях/skip)
- [x] запустить полный `pytest` — зелёный (новый модуль автономен, старый код не изменён)

### Task 2: Атомарный switchover всех потребителей на `accounts.py`

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `airflow_provider_cian/hooks/__init__.py`
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `examples/bq_and_s3_multi_account_dag.py`
- Modify: `tests/hooks/test_cian.py`
- Modify: `tests/operators/test_builder_reports.py`
- Modify: `tests/test_example_dag_multi_account.py`

- [x] `hooks/cian.py`: удалить локальные `Account` и `get_accounts`; импортировать **только `resolve_token`** из `accounts` (**`Account` хуку больше не нужен** — санитизация ушла в `resolve_token`); в `_make_request` заменить инлайн-резолюцию токена (строки ~112-137) на `token = resolve_token(conn, self.account_id)`
- [x] `hooks/__init__.py`: убрать `Account`/`get_accounts` из импорта и `__all__` (оставить `CianHook`, `CianNotFoundError`)
- [x] `operators/builder_reports.py`: импорт `resolve_cabinet_id` из `accounts` (убрать импорт `Account` из `hooks.cian`); в `execute()` заменить инлайн-ветвление cabinet_id (строки ~64-70) на `cabinet_id = resolve_cabinet_id(self.cian_conn_id, self.account_id)` — **оператор сам коннектор не читает** (в multi-режиме лишнего `get_connection` не появляется); хук создаётся с `account_id` как прежде
- [x] `examples/bq_and_s3_multi_account_dag.py`: импорт `Account, list_accounts` из `airflow_provider_cian.accounts`; `list_accounts(CIAN_CONN_ID)` (стр. 257); поправить комментарий (стр. 34)
- [x] `tests/hooks/test_cian.py`: импорт `Account` из `accounts`; **удалить** 8 тестов `get_accounts` (`:345-405`) — они уже перенесены как тесты `list_accounts` в Task 1; тесты `_make_request` оставить (проверяют поведение через `resolve_token`)
- [x] `tests/operators/test_builder_reports.py`: импорт `Account` из `accounts`; прогнать `TestExecuteWithAccount` — поведение путей сохранено (правим только импорты/моки)
- [x] `tests/test_example_dag_multi_account.py`: импорт `Account` из `accounts`; patch-таргет `:18` → `airflow_provider_cian.accounts.list_accounts`; обновить комментарий-обоснование `:13-17`
- [x] запустить полный `pytest` — все тесты зелёные (switchover завершён, поведение сохранено)

### Task 3: ADR-0002 — сегрегация интерфейса

**Files:**
- Create: `docs/adr/0002-account-resolution-segregated-interface.md`

- [x] записать ADR: почему `resolve_cabinet_id` и `resolve_token` — две раздельные функции, а не единый `resolve() -> (cabinet_id, token)`; обоснование — инвариант «токен не доходит до оператора/XCom» (`CONTEXT.md`); явно: будущему разработчику НЕ сливать их обратно
- [x] упомянуть связь с ADR-0001 (оба про границы провайдера)

### Task 4: Verify acceptance criteria

- [x] весь функционал из Overview реализован; внешнее поведение не изменилось
- [x] инвариант: оператор нигде не получает токен (зовёт только `resolve_cabinet_id`); grep по `resolve_token` — только в `hooks/cian.py`
- [x] запустить полный `pytest` — зелёный
- [x] grep: не осталось ссылок на `airflow_provider_cian.hooks.cian` для `get_accounts`/`Account` (включая `hooks/__init__.py`, examples, тесты, доки)

### Task 5: [Final] Документация и завершение

- [x] `README.md:97-102` и `README_ru.md:97-102` — заменить `get_accounts` → `list_accounts` и путь импорта на `from airflow_provider_cian.accounts import Account, list_accounts` (безусловно — оба файла определённо ссылаются на старый API)
- [x] `CHANGELOG.md` — добавить запись как **BREAKING**: `Account`/`list_accounts` (бывший `get_accounts`) переехали в `airflow_provider_cian.accounts`, удалены из `airflow_provider_cian.hooks(.cian)`; уточнить, что `get_accounts` больше не «в `CianHook`» (стр. 5 описывает это неверно после рефакторинга)
- [x] `CONTEXT.md` — исправить предсуществующий дрейф: строки 12 и 16 говорят про параметр оператора `account`, а реальный параметр — `account_id` (`builder_reports.py:51`); привести термин в соответствие коду. Новых терминов не требуется (Account/Account ID/single-multi mode уже есть)
- [x] переместить план в `docs/plans/completed/` (skipped - docs/plans/ is in .gitignore, plan will be moved after branch merge)

## Post-Completion

*Информационно, без чекбоксов*

- Публикация новой версии провайдера на PyPI после мёрджа (тег через setuptools-scm); версия — с учётом breaking-изменения публичных импортов.
- Поведение оператора/хука и формат вывода не меняются; согласование с потребителями ВЫХОДНЫХ данных не требуется. Потребители импортов `Account`/`get_accounts` из `hooks` — в проде отсутствуют (изменение санкционировано).
