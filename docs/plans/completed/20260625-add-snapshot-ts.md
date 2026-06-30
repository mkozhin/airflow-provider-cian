# Добавление параметра `add_snapshot_ts` в `CianBuilderReportsOperator`

## Overview

Перенести в провайдер cian фичу, реализованную в провайдере avito (`AvitoCallsOperator`): новый параметр оператора `add_snapshot_ts: bool = False`. Когда включён — в каждую JSON-запись добавляется поле `snapshot_ts` со значением реального времени старта прогона DAG (`dag_run.start_date`). _Отличие от avito:_ avito берёт `logical_date`; здесь источник изменён осознанно (см. Design decisions #1 и ADR).

**Зачем:** статус звонка в cian (целевой/нецелевой) выводится из бюджета `billing_price` и может меняться задним числом — бюджет доначисляется или снимается уже после первичной выгрузки даты (см. CONTEXT.md → «Статус звонка»). Поле `snapshot_ts` внутри JSON помечает запись временем запуска DAG, позволяя в аналитическом слое (ClickHouse/Spark/BigQuery) выбирать последний снапшот по дате либо строить историю изменения `billing_price`/`is_targeted` во времени. Сейчас при повторной выгрузке тех же дат записи перезаписываются и история теряется.

**Важно про downstream-пути:** `execute()` в cian возвращает в XCom только `str`-путь к файлу (без `snapshot_ts`). Поэтому даунстрим-таск НЕ получает `snapshot_ts` из результата оператора — для версионированных (неперезаписывающих) S3/GCS-ключей он должен сам взять `logical_date`/`ds` из своего Airflow-контекста или прочитать поле из JSON. Эта фича добавляет версионирование на уровне *данных*, а не на уровне *путей выгрузки*.

**Интеграция:** изменение полностью обратно совместимо. По умолчанию `add_snapshot_ts=False` — поведение оператора не меняется, схема вывода остаётся 18-польной.

## Context (from discovery)

- Провайдер cian, Python 3.12, Apache Airflow. Единственный оператор — `CianBuilderReportsOperator`.
- Файлы:
  - `airflow_provider_cian/operators/builder_reports.py` — оператор (метод `execute`, `_enrich`, `_write`, константа `_CSV_FIELDS`)
  - `tests/operators/test_builder_reports.py` — тесты оператора
  - `README.md`, `README_ru.md`, `CHANGELOG.md` — документация
- Референс — `airflow-provider-avito`, коммиты: `5b4e60e` (оператор), `3ab3ae2` (тесты), `1a4a4de` (документация + фиксы code review), `73a2aaf` (только release CHANGELOG). Эталонные тесты — `tests/test_operator.py` (тесты `test_snapshot_ts_*`).
- Команда тестов: `pytest` (`testpaths = ["tests"]`).

### Ключевые отличия cian от avito (влияют на реализацию)

- В avito `execute()` группирует записи по датам и возвращает `list[CallRecord]` (TypedDict с ключами `date`/`path`/`snapshot_ts`). В cian `execute()` пишет один файл и возвращает `str` (путь). **Решено: возвращаемое значение cian НЕ меняем** — `snapshot_ts` прокидывается только внутрь записей, не в возвращаемое значение.
- В cian запись формируется явным словарём в методе `_enrich()` — туда и добавляется поле.
- В cian есть метод `_write()` без обращения к `context`; вычисление `snapshot_ts` делается в `execute()` и передаётся в `_enrich()`.

## Design decisions (из брейншторма)

1. **Источник `snapshot_ts` — `context["dag_run"].start_date`, НЕ `logical_date`** (расхождение с avito по источнику, см. ADR). Причина: `snapshot_ts` определён как «дата получения данных» (реальное wall-clock время старта прогона). `logical_date` — это начало интервала данных, при cron-`schedule` и бэкафилле оно расходится с реальным временем выгрузки; `dag_run.start_date` всегда равен реальному старту прогона и постоянен в рамках прогона (все маппленные даты получают одну метку). Подробный разбор — в ADR `docs/adr/0001-snapshot-ts-source.md`.
2. **Формат `snapshot_ts`** — `start_date.strftime("%Y-%m-%dT%H:%M:%S")`: naive-UTC строка без таймзоны (`start_date` хранится в UTC). Зеркало avito. НЕ приводим к MSK с офсетом, несмотря на конвенцию поля `datetime`, **намеренно**: `datetime` — это время события, полученное от API Cian (бизнес-данные, MSK с офсетом), а `snapshot_ts` — метаданность пайплайна (когда мы выгрузили данные). Это разные категории времени, и разный формат осознанно их разделяет. См. ADR.
3. **Только JSON** — при `output_format="csv"` параметр игнорируется (схема `_CSV_FIELDS` фиксирована и не трогается). Полное зеркалирование поведения avito.
4. **Возвращаемое значение оператора не меняется** — остаётся `str` (путь к файлу).
5. **Надёжность источника** — в Airflow 2.9+ (ветка 2.x) `context["dag_run"]` всегда присутствует во время выполнения таска, а `dag_run.start_date` к этому моменту всегда заполнен. Поэтому отдельная защита/тест на `None`/отсутствие (как требовалось бы для nullable `logical_date`) не нужны. Это упрощение относительно первоначального плана с `logical_date`.

## Development Approach

- **Testing approach**: TDD-aligned, как принято в проекте (тесты для каждого изменения, обязательны).
- Каждая задача завершается написанием/обновлением тестов; все тесты должны проходить до перехода к следующей задаче.
- Изменения малые и сфокусированные, обратная совместимость сохраняется.

## Testing Strategy

- **Unit-тесты**: обязательны для каждой задачи. Покрыть:
  - `snapshot_ts` попадает в каждую JSON-запись при `add_snapshot_ts=True`
  - значение `snapshot_ts` равно ожидаемому, выведенному из `dag_run.start_date`
  - поле НЕ добавляется по умолчанию (`add_snapshot_ts=False`); набор ключей ровно 18 (флаг OFF) / 19 (флаг ON)
  - поле НЕ добавляется в CSV даже при включённом флаге, схема CSV остаётся 18-польной
  - пустой список записей проходит через `execute()` и возвращает валидный `str`-путь
- **E2E**: в проекте нет UI/e2e — не применимо.

## Solution Overview

В начале `execute()` вычисляется `snapshot_ts` (строка или `None`) из `context["dag_run"].start_date`. Значение передаётся в `_enrich()`, который при формировании каждой записи добавляет ключ `snapshot_ts` только когда значение задано и `output_format == "json"`. `_write()` и `_CSV_FIELDS` не меняются — добавление поля явно ограничено JSON-веткой, поэтому в CSV поле никогда не попадает (гарантируется тестом `len(rows[0]) == 18`).

## Technical Details

- **Новый параметр `__init__`:** `add_snapshot_ts: bool = False` → `self.add_snapshot_ts`.
- **Вычисление в `execute()` — в начале метода:**
  ```python
  def execute(self, context) -> str:
      snapshot_ts = (
          context["dag_run"].start_date.strftime("%Y-%m-%dT%H:%M:%S")
          if self.add_snapshot_ts else None
      )
      # ... далее без изменений: резолв хука, output_path, os.remove, os.makedirs ...
  ```
  **Почему `dag_run.start_date`, а не `logical_date`:** см. Design decisions #1 и ADR — это реальное wall-clock время старта прогона («дата получения данных»), постоянное на весь прогон, корректное при cron-`schedule` и бэкафилле.
  **Почему в начале метода:** дёшево и читаемо (run-level значение считается один раз сверху); побочный эффект — `snapshot_ts` вычисляется до `os.remove`/`os.makedirs`. Жёсткой необходимости в этом нет (в Airflow 2.x `dag_run.start_date` всегда доступен, в отличие от nullable `logical_date`), но порядок оставляем чистым.
  Передаётся: `enriched = self._enrich(records, hook, snapshot_ts)`.
- **Сигнатура `_enrich`:** `def _enrich(self, records, hook, snapshot_ts: str | None = None)`.
  После сборки словаря записи (перед `result.append`):
  ```python
  row = { ... 18 полей ... }
  if snapshot_ts and self.output_format == "json":
      row["snapshot_ts"] = snapshot_ts
  result.append(row)
  ```
- **Схема вывода:** при `add_snapshot_ts=True` и `output_format="json"` появляется 19-е поле `snapshot_ts` (ISO 8601 `YYYY-MM-DDTHH:MM:SS`, naive). По умолчанию — 18 полей без изменений.

## What Goes Where

- **Implementation Steps** (`[ ]`): код оператора, тесты, README/README_ru/CHANGELOG.
- **Post-Completion** (без чекбоксов): ручная проверка в реальном DAG, согласование схемы с потребителями (BigQuery/ClickHouse), публикация версии на PyPI.

## Implementation Steps

### Task 1: Параметр и проброс `snapshot_ts` в оператор

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [x] **(деуглубление, см. ниже «Schema locality»)** ввести рядом с `_CSV_FIELDS` именованную константу `_SNAPSHOT_FIELD = "snapshot_ts"` с комментарием: `_CSV_FIELDS` — канонический базовый набор Builder Report (CSV использует ровно его), `_SNAPSHOT_FIELD` — единственное опциональное расширение, только для JSON-вывода (см. ADR-0001). Чтобы `snapshot_ts` не был магической строкой-спецслучаем
- [x] добавить параметр `add_snapshot_ts: bool = False` в `__init__` и сохранить в `self.add_snapshot_ts`
- [x] в `execute()` (в начале метода) вычислить `snapshot_ts` из `context["dag_run"].start_date` (формат `%Y-%m-%dT%H:%M:%S`) при включённом флаге, иначе `None`
- [x] изменить сигнатуру `_enrich` на `(self, records, hook, snapshot_ts: str | None = None)` и передать значение из `execute()`
- [x] в `_enrich` добавлять ключ `_SNAPSHOT_FIELD` в запись только при `snapshot_ts and self.output_format == "json"`; `_CSV_FIELDS` не трогать
- [x] добавить в начало тестового файла импорт `from datetime import datetime` (сейчас его нет; `MagicMock` уже импортирован) и импорт `_SNAPSHOT_FIELD` рядом с `_CSV_FIELDS`
- [x] обновить helper `_make_operator()` — добавить параметр `add_snapshot_ts: bool = False` (или `**kwargs`), пробросить в конструктор оператора (сейчас принимает только `tmp_dir`/`output_format`)
- [x] обновить helper `_make_context()` — добавить мок `dag_run` с `start_date`: `dag_run = MagicMock(); dag_run.start_date = datetime(2024, 1, 15, 12, 0, 0)` → `{"run_id": ..., "dag_run": dag_run}` (время намеренно НЕ полночь, чтобы ловить путаницу date/datetime). Для тестов с `add_snapshot_ts=False` `dag_run` не читается — существующие тесты не ломаются
- [x] написать тест: при `add_snapshot_ts=True` каждая JSON-запись содержит `_SNAPSHOT_FIELD` со значением ровно `"2024-01-15T12:00:00"` (= `start_date.strftime`)
- [x] написать тест (key-set, флаг ON): набор ключей JSON-записи равен `set(_CSV_FIELDS) | {_SNAPSHOT_FIELD}` (ровно 19 полей)
- [x] написать тест (key-set, флаг OFF, дефолт): `_SNAPSHOT_FIELD` в JSON-записи нет, набор ключей равен `set(_CSV_FIELDS)` (18 полей); убедиться, что существующий `test_enriched_record_has_exactly_csv_fields` продолжает проходить
- [x] написать тест: при `output_format="csv"` и `add_snapshot_ts=True` колонки `snapshot_ts` нет, схема CSV остаётся 18-польной (`len(rows[0]) == 18`)
- [x] написать тест: пустой результат хука (`get_builder_reports` → `[]`) при `add_snapshot_ts=True` проходит через `execute()` и возвращает валидный `str`-путь без исключений (через `execute()`, не `_enrich([])` напрямую)
- [x] запустить `pytest` — все тесты должны проходить перед следующей задачей

> Примечание: защитные тесты на отсутствие/`None` метки убраны намеренно — в Airflow 2.x `context["dag_run"].start_date` всегда доступен во время выполнения таска (см. Design decision #5), поэтому такой режим отказа нереалистичен и тестировать его — over-engineering.
>
> **Schema locality (деуглубление #1, минимальная форма):** базовая схема Builder Report остаётся единым списком `_CSV_FIELDS` (18 имён), `snapshot_ts` получает дом как именованная константа `_SNAPSHOT_FIELD` — единственное опциональное (JSON-only) расширение. Сознательно НЕ выносим маппинг/сборку записи в отдельный модуль: у неё один вызывающий (цикл `_enrich`) → настоящего шва нет, вынос был бы shallow-абстракцией «ради тестируемости».

### Task 2: Документация

**Files:**
- Modify: `README.md`
- Modify: `README_ru.md`
- Modify: `CHANGELOG.md`

- [x] добавить строку `add_snapshot_ts` в таблицу параметров оператора в обоих README (тип `bool`, дефолт `False`, описание)
- [x] добавить секцию «Snapshot versioning» / «Версионирование снапшотов» с пояснением и примером запроса ClickHouse/Spark (по образцу avito README, строки 71–85), с уточнением «только для `output_format='json'`»
- [x] обновить секцию «Output Schema» — отметить, что при `add_snapshot_ts=True` и JSON добавляется 19-е поле `snapshot_ts` (ISO 8601, naive-UTC, из `dag_run.start_date` — реальное время старта прогона)
- [x] **добавить BigQuery-caveat** в обоих README: в примерах `BQ_SCHEMA` фиксированная (18 полей), поэтому при `add_snapshot_ts=True` потребители JSON в BigQuery должны либо добавить поле `snapshot_ts` в схему, либо использовать `ignore_unknown_values=True`, иначе загрузка упадёт на лишнем поле
- [x] добавить запись в `CHANGELOG.md` (новая версия) по образцу avito CHANGELOG: новый параметр, поведение, обратная совместимость
- [x] **examples НЕ трогаем по умолчанию**: показывать `add_snapshot_ts=True` в `examples/*.py` только вместе с обновлением `BQ_SCHEMA` (добавить колонку `snapshot_ts`) — иначе пример сломает загрузку в BigQuery. Если не обновляем схему примеров — оставить examples как есть

### Task 3: Verify acceptance criteria

- [x] проверить, что все требования из Overview реализованы
- [x] проверить обратную совместимость: тесты без `add_snapshot_ts` проходят без изменений поведения
- [x] запустить полный набор тестов: `pytest`
- [x] убедиться, что схема CSV не изменилась (18 полей)

### Task 4: [Final] Документация и завершение

- [x] финально вычитать README/README_ru/CHANGELOG на согласованность
- [x] переместить этот план в `docs/plans/completed/` (will be moved by orchestrator via move-plan.sh)

## Post-Completion

*Только информационно, без чекбоксов*

**Ручная проверка:**
- запустить реальный DAG с `add_snapshot_ts=True` и убедиться, что `snapshot_ts` присутствует в выгруженных JSON-файлах, одинаков для всех дат одного прогона и соответствует реальному времени старта прогона (`dag_run.start_date`).

**Внешние системы:**
- согласовать новое поле `snapshot_ts` со схемой потребителей (BigQuery/ClickHouse): добавить столбец/настроить выборку последнего снапшота.
- опубликовать новую версию провайдера на PyPI после мёрджа (тег версии — через setuptools-scm).
