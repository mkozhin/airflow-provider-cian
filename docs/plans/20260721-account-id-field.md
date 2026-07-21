# Add account_id field to JSON and CSV outputs

## Overview

- `CianBuilderReportsOperator` пишет в каждую строку выгрузки (JSON и CSV) новое поле `account_id` — Account ID кабинета из Airflow connection.
- Решает проблему: при загрузке файлов от нескольких кабинетов в одно хранилище (BigQuery/S3) принадлежность записи кабинету видна только из пути файла; после этого изменения она есть в самих данных.
- `account_id` — пользовательская метка из connection (`extra.accounts[].id` в multi-account режиме, иначе `Account(id=conn.login).id`), канонический термин глоссария **Account ID** (CONTEXT.md). Cian API реального идентификатора кабинета не даёт: по swagger (`https://public-api.cian.ru/builders/swagger/latest`) нет эндпоинта «кто я» и нет полей `builderId`/`accountId` в ответах — проверено 2026-07-21.
- Имя поля `account_id` выбрано осознанно (гриль-сессия 2026-07-21): совпадает с термином глоссария, кодом (`accounts.py`, параметр оператора `account_id`) и именованием BQ-таблиц `{BQ_TABLE}_{account_id}`; вариант `cabinet_id` отвергнут — он в списке _Avoid_ глоссария.
- **Breaking change → релиз 0.5.0**: поле добавляется всегда (без opt-in флага); `login` в connection становится обязательным для single-account режима; путь файла всегда содержит сегмент кабинета.

## Context (from discovery)

- Файлы: `airflow_provider_cian/operators/builder_reports.py` (основные изменения), `airflow_provider_cian/accounts.py` (две строки: санитизация в multi-account ветке `resolve_cabinet_id` и ключа поиска в `resolve_token`, плюс их docstrings), `tests/operators/test_builder_reports.py` (51 тест), `tests/test_accounts.py`, `examples/bq_and_s3_dag.py` + `bq_and_s3_dag_v2.py` + `bq_and_s3_multi_account_dag.py` (три `BQ_SCHEMA`), `CONTEXT.md`, `README.md`, `README_ru.md`, `CHANGELOG.md`.
- Паттерны: `_CSV_FIELDS` — канонический набор полей (сейчас 18); `snapshot_ts` — единственное opt-in JSON-only расширение (ADR-0001); `resolve_cabinet_id()` уже вызывается в `execute()` и возвращает `None` в single-account режиме без `login`.
- Существующие тесты, которые заведомо сломаются (по результатам plan-review, проверено против кода):
  - Task 2: `TestBuildPath` (вызовы `_build_path` без cabinet_id); `TestBuildPathWithCabinetId::test_existing_build_path_calls_without_cabinet_id_still_work` (backward-compat ветка удаляется); `TestExecuteWithAccount::test_execute_without_account_without_conn_login_path_has_no_extra_dir` (проверяет удаляемое поведение «без login — путь без сегмента»); `TestExecuteEmptyDay` — все 5 тестов (`_make_conn_mock(login=None)` + прямые вызовы `op._build_path("run-empty")`); `TestSnapshotTs` — фикстуры с `login=None` и прямой вызов `op._build_path("run-snap")`; `TestExecute` (фикстуры connection без `login`).
  - Task 3: `TestEnrich` (новая сигнатура `_enrich`); проверки `set(_CSV_FIELDS)`; хардкод-проверки количества колонок `== 18` в `TestWrite::test_csv_creates_file_with_header` и `TestSnapshotTs::test_snapshot_ts_not_in_csv_even_when_flag_on`; докстринг «exactly 19» в `test_snapshot_ts_json_records`.

## Development Approach

- **testing approach**: Regular (код, затем тесты в рамках каждой задачи)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every CODE-CHANGING task MUST include new/updated tests** (Tasks 1–4; Tasks 5–7 — documentation/closure, для них тесты не требуются, но полный suite гоняется в Task 6)
  - tests are not optional - they are a required part of the checklist
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change
- backward compatibility intentionally broken (documented breaking change, 0.5.0)

## Testing Strategy

- **unit tests**: pytest, `tests/operators/test_builder_reports.py` и `tests/test_accounts.py` — обновить сломанные, добавить новые на account_id
- **e2e tests**: нет в проекте (только unit + structure-тесты примерных DAG); прогнать весь suite `pytest`. Существующие structure-тесты примеров import/structure-only и не проверяют `BQ_SCHEMA` — regression-тесты схем добавляются в Task 4, иначе рассинхрон схем BigQuery прошёл бы незамеченным

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

- Валидация «Account ID обязателен» делается один раз в `execute()` сразу после `resolve_cabinet_id()` — до запроса к API, падаем быстро и без побочных эффектов.
- `_enrich()` получает значение явным параметром и вставляет его в `row` второй позицией (после `"id"`); порядок ключей dict сохраняется в JSON, порядок CSV задаёт `_CSV_FIELDS`.
- `_build_path()` упрощается: ветка `cabinet_id is None` становится мёртвой и удаляется, `cabinet_id` — обязательный параметр.

## Technical Details

- `_CSV_FIELDS`: вставить `"account_id"` после `"id"` → 19 полей; обновить комментарий «CSV uses exactly these 18 fields» → 19. `snapshot_ts` остаётся единственным опциональным JSON-only полем (в JSON с `add_snapshot_ts=True` становится 20-м полем).
- Валидация: `if not cabinet_id` (а не `is None`) — отвергает и `None` (single-account без `login`), и пустую строку `account_id=""`, которую `resolve_cabinet_id()` вернула бы как есть (`os.path.join(base_dir, "", ...)` молча съел бы сегмент кабинета). Текст ошибки:
  `AirflowException(f"Account ID is required: set 'login' in connection {self.cian_conn_id!r} or pass a non-empty account_id")`
- Единообразная санитизация (гриль-сессия 2026-07-21): multi-account ветка `resolve_cabinet_id()` тоже прогоняет значение через `Account(id=account_id).id`, а `resolve_token()` санитизирует ключ поиска — инвариант: оператор всюду (путь, данные, поиск токена) работает с одной канонической (санитизированной) формой Account ID, сырой ввод работает end-to-end. Для штатного пути через `list_accounts()` поведение не меняется (санитизация идемпотентна). Побочный эффект: сырые метки типа `" "` превращаются в `"_"` — garbage in, garbage out, дополнительной валидации не делаем.
- В `resolve_token()` канонический ключ вычислять один раз до цикла (`canonical = Account(id=account_id).id`), не создавать `Account` на каждой итерации.
- Порядок: сначала `resolve_cabinet_id()` (санитизация внутри), затем `if not cabinet_id` (пустая строка остаётся пустой после санитизации и отсекается).
- Путь: всегда `base_dir/<account_id>/<safe_run_id>/<date>.<ext>`.
- Внутренние имена `resolve_cabinet_id()` и локальная переменная `cabinet_id` сохраняются как есть (переименование — scope creep); это тот же канонический Account ID, что и публичное поле `account_id` — при реализации отметить коротким комментарием.
- Возвращаемый XCom-контракт `{"date": ..., "path": ...}` не меняется.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): изменения кода, тестов и документации в этом репозитории
- **Post-Completion** (без чекбоксов): обновления downstream-потребителей (схемы BigQuery-таблиц), выпуск релиза тегом

## Implementation Steps

### Task 1: Canonicalize Account ID in accounts.py

**Files:**
- Modify: `airflow_provider_cian/accounts.py`
- Modify: `tests/test_accounts.py`

- [x] в multi-account ветке `resolve_cabinet_id()` возвращать `Account(id=account_id).id` вместо сырого `account_id`; обновить docstring (сейчас обещает «returns account_id directly»)
- [x] в `resolve_token()` санитизировать ключ поиска: `canonical = Account(id=account_id).id` один раз до цикла, сравнивать `acc.id == canonical`; обновить docstring
- [x] написать тесты: `resolve_cabinet_id` санитизирует сырой `account_id` (`"a.b"` → `"a_b"`), идемпотентна для уже санитизированного, `""` остаётся `""`
- [x] написать тесты: `resolve_token` находит токен по сырому `account_id="a.b"` при `extra.accounts[].id="a.b"` (оба санитизируются в `"a_b"`) и по-прежнему падает «not found» для действительно отсутствующего аккаунта
- [x] run tests - must pass before task 2

### Task 2: Require account_id in execute() and simplify _build_path()

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [x] в `execute()` после `resolve_cabinet_id()` добавить проверку: `if not cabinet_id` → `AirflowException` с текстом из Technical Details (до создания hook и запроса к API; покрывает и `None`, и `account_id=""`)
- [x] упростить `_build_path(run_id, cabinet_id)`: сделать `cabinet_id` обязательным, удалить ветку `None`
- [x] обновить ВСЕ фикстуры с `login=None` и ВСЕ прямые вызовы `_build_path` без cabinet_id по всему файлу тестов: `TestBuildPath`, `TestExecute`, `TestExecuteEmptyDay` (сохранить проверки поведения пустого дня — ADR-0003), `TestSnapshotTs` (`_run_with_snapshot`, `test_snapshot_ts_absent_by_default`, `test_empty_records_with_snapshot_ts_returns_none_and_no_file`)
- [x] удалить тесты удаляемого поведения: `TestBuildPathWithCabinetId::test_existing_build_path_calls_without_cabinet_id_still_work`; `TestExecuteWithAccount::test_execute_without_account_without_conn_login_path_has_no_extra_dir` (либо переписать на ожидание `AirflowException`)
- [x] написать тест: `execute()` без `login` в single-account режиме падает с `AirflowException` (проверить текст ошибки), не делает HTTP-запросов и не создаёт hook (`CianHook.assert_not_called()`)
- [x] написать тест: `execute()` с `account_id=""` падает с той же ранней `AirflowException` до создания `CianHook`
- [x] проверить ожидаемые пути с сегментом кабинета во всех обновлённых фикстурах
- [x] run tests - must pass before task 3

### Task 3: Add account_id to enriched rows and _CSV_FIELDS

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [ ] вставить `"account_id"` в `_CSV_FIELDS` после `"id"`; обновить комментарий про 18 → 19 полей
- [ ] `_enrich(records, hook, account_id, snapshot_ts=None)`: добавить параметр, вставить `"account_id": account_id` в `row` сразу после `"id"`; в `execute()` передавать локальный `cabinet_id` (санитизированный результат `resolve_cabinet_id()`), НЕ `self.account_id` — в данные должно попасть ровно то же значение, что и в путь
- [ ] обновить существующие тесты `TestEnrich` под новую сигнатуру
- [ ] написать тесты: JSON-строки содержат `account_id` с ожидаемым значением и ключ идёт сразу после `id`; CSV-заголовок содержит `account_id` второй колонкой
- [ ] написать тесты: фактические значения в CSV-строках — прочитать файл через `csv.DictReader` и проверить `row["account_id"]` в single-account и multi-account режимах (несколько строк)
- [ ] написать тест: multi-account режим (`account_id` передан) — в строках именно `account_id`, а не `login`; при сыром `account_id="a.b"` в строках и в пути одинаковое санитизированное `"a_b"`
- [ ] написать тест: single-account с `login` — в строках санитизированный `Account(id=login).id`
- [ ] обновить хардкод-проверки количества колонок: `TestWrite::test_csv_creates_file_with_header` (`== 18` → `== 19`), `TestSnapshotTs::test_snapshot_ts_not_in_csv_even_when_flag_on` (`== 18` → `== 19`), докстринг `test_snapshot_ts_json_records` («exactly 19» → «exactly 20»)
- [ ] проверить тесты `TestSnapshotTs`: `snapshot_ts` по-прежнему JSON-only и идёт последним
- [ ] run tests - must pass before task 4

### Task 4: Update BQ_SCHEMA in example DAGs and add schema regression tests

**Files:**
- Modify: `examples/bq_and_s3_dag.py`
- Modify: `examples/bq_and_s3_dag_v2.py`
- Modify: `examples/bq_and_s3_multi_account_dag.py`
- Modify: `tests/test_example_dag_v1.py`
- Modify: `tests/test_example_dag_v2.py`
- Modify: `tests/test_example_dag_multi_account.py`

Причина: у `GCSToBigQueryOperator` и load job BigQuery `ignore_unknown_values=False` по умолчанию — новое JSON-поле `account_id`, отсутствующее в схеме, сломает загрузку у всех, кто использует примеры как есть.

- [ ] `bq_and_s3_dag.py` (`BQ_SCHEMA`, ~строка 61): добавить `{"name": "account_id", "type": "STRING", "mode": "NULLABLE"}` после `id` → 19 полей
- [ ] `bq_and_s3_dag_v2.py` (`BQ_SCHEMA`, ~строка 59): то же → 19 полей
- [ ] `bq_and_s3_multi_account_dag.py` (`BQ_SCHEMA`, ~строка 89, сейчас 19 полей со `snapshot_ts`): добавить `account_id` после `id` → 20 полей
- [ ] написать regression-тесты схем в `tests/test_example_dag_v1.py`, `test_example_dag_v2.py`, `test_example_dag_multi_account.py` (эти файлы уже импортируют модули примеров; выделенные `*_structure.py`-файлы не трогать): состав `BQ_SCHEMA` совпадает с `_CSV_FIELDS` оператора (плюс `snapshot_ts` для multi-account примера), и новое поле присутствует ПОЛНЫМ словарём `{"name": "account_id", "type": "STRING", "mode": "NULLABLE"}` сразу после `id`
- [ ] run tests - must pass before task 5

### Task 5: Update documentation, changelog and ADR

*Documentation/closure task — новые тесты не требуются; полный suite гоняется в Task 6.*

**Files:**
- Modify: `CONTEXT.md`
- Modify: `README.md`
- Modify: `README_ru.md`
- Modify: `CHANGELOG.md`
- Create: `docs/adr/0004-account-id-in-output.md`

- [ ] CONTEXT.md: дополнить статью глоссария **Account ID** — значение теперь пишется в каждую запись выгрузки полем `account_id` (не только в пути); санитизация единообразна во всех употреблениях (путь, данные, поиск токена)
- [ ] CONTEXT.md: обновить устаревшее определение single-account режима («cabinet ID берётся из `conn.login` (если задан) или отсутствует в пути» → login обязателен, путь всегда с сегментом кабинета)
- [ ] README.md и README_ru.md: обновить таблицу полей выгрузки (`account_id` после `id`), описание connection (`login` обязателен), пример пути, а также ВСЕ утверждения о количестве полей: «18 fields» → 19 (включая «CSV schema remains 18 fields»), `snapshot_ts` «19th field» → «20th field» (README_ru: «19-е поле» → «20-е поле»)
- [ ] README.md и README_ru.md: переписать заметку про BigQuery-схемы примеров раздельно — базовая схема (v1/v2: 19 полей) и multi-account со `snapshot_ts` (20 полей); механическая замена числа не годится
- [ ] README.md и README_ru.md: обновить раздел про resolution-функции — `resolve_cabinet_id` теперь возвращает санитизированное значение, `resolve_token` ищет по каноническому ключу
- [ ] README.md (~строка 47) и README_ru.md (эквивалент): исправить утверждение «The `id` can be any string … (e.g. the numeric cabinet ID from Cian)» — Cian API не выдаёт id кабинета, это пользовательская метка (можно порекомендовать ИНН/название юрлица из договора)
- [ ] README.md (~строки 91–99) и README_ru.md (эквивалент): обновить матрицу разрешения Account ID (`| account_id | conn.login | Path |`) — удалить строку `not set / not set → {base_dir}/{run_id}/...` (поведение удаляется, теперь это ошибка) и переформулировать фразу «setting `Login` … acts as a cabinet ID for path isolation» под обязательность `login`
- [ ] CHANGELOG.md: секция 0.5.0, записи в стиле проекта `- **BREAKING**:` — новое обязательное поле `account_id` в JSON и CSV; `login` обязателен в single-account режиме; путь файла всегда содержит `<account_id>`; `resolve_cabinet_id`/`resolve_token` теперь санитизируют `account_id` (у тех, кто передавал сырое значение с спецсимволами напрямую, изменится путь); обновлены `BQ_SCHEMA` в примерах (исторические записи не трогать)
- [ ] создать `docs/adr/0004-account-id-in-output.md`: решение всегда писать `account_id` в выгрузку (дорого обратить — поле уходит в downstream-данные, как в ADR-0001), обязательность `login`, единообразная санитизация значения (один инвариант для пути, данных и поиска токена), отсутствие реального id кабинета в Cian API (swagger, проверено 2026-07-21), выбор имени `account_id` по глоссарию (вариант `cabinet_id` отвергнут — в списке _Avoid_)

### Task 6: Verify acceptance criteria

- [ ] verify: поле `account_id` присутствует в каждой строке JSON и CSV сразу после `id`, со значением (не только в заголовке)
- [ ] verify: без `login` (single-account) и с `account_id=""` таск падает с понятной ошибкой до создания hook и запроса к API
- [ ] verify: multi-account и single-account режимы дают правильные (санитизированные) значения `account_id`, сырой ввод работает end-to-end включая поиск токена
- [ ] verify: все три `BQ_SCHEMA` в примерах согласованы с `_CSV_FIELDS`, новое поле — полным словарём
- [ ] verify: в README/README_ru не осталось описаний пути без сегмента кабинета (матрица разрешения, фраза про опциональный `Login`)
- [ ] run full test suite: `pytest`
- [ ] verify: все утверждения о количестве полей в коде, тестах, README и CONTEXT.md корректны — «18 fields», «19th field»/«19-е поле» для `snapshot_ts`, докстринги (исторические записи CHANGELOG.md не трогать); нет мёртвых веток `cabinet_id is None`

### Task 7: [Final] Move plan to completed

*Closure task — тестов не требует (полный suite прогнан в Task 6).*

- [ ] update CONTEXT.md, если во время реализации обнаружились новые паттерны сверх правок Task 5 (CLAUDE.md в репозитории нет — не создавать)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**External system updates:**
- обновить схемы downstream-таблиц BigQuery (новая колонка `account_id` STRING) до деплоя провайдера 0.5.0 — иначе загрузки упадут (`ignore_unknown_values=False`)
- исторические данные провайдер НЕ мигрирует (решение гриль-сессии 2026-07-21): это ломающее изменение, потребитель сам решает, что делать с существующими таблицами — миграция, удаление, пересборка или NULL в старых строках
- в single-account инсталляциях заполнить `login` в connection `cian_default` до обновления, иначе таски начнут падать
- выпустить релиз 0.5.0 тегом (setuptools-scm + publish workflow)
