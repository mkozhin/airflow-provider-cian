# Пустой день Cian не порождает файл: оператор отдаёт дальше только дни с данными

## Overview

За дату, по которой Cian не вернул ни одной записи, провайдер сейчас всё равно создаёт файл
и DAG заливает его в S3/GCS/BigQuery. Это неверно: если данных нет — писать нечего.

Что меняется:

- `CianBuilderReportsOperator` при пустом ответе **не создаёт файл** и возвращает `None`.
  За день с данными возвращает самоописывающий `dict`: `{"date": ..., "path": ...}`.
- Поскольку Airflow не пишет XCom для `None` (`models/taskinstance.py:783`), пустой день
  просто **выпадает из списка** результатов mapped-таска `collect`. Downstream mapped-таски
  (`upload_gcs` / `upload_s3` / `load_bq`) разворачиваются **только по дням с данными** —
  инстансов за пустые дни не появляется вовсе.
- `CianHook.get_builder_reports` перестаёт молча превращать неожиданный ответ API в «нет данных».

⚠️ **Полностью пустой период ведёт себя по-разному в двух формах DAG — не путать:**

- **v1 и multi-account** (`bq_and_s3_dag.py`, `bq_and_s3_multi_account_dag.py`): есть отдельные
  mapped-таски заливки. Агрегатор возвращает `[]`, mapped-таск разворачивается в ноль
  инстансов, Airflow помечает его `skipped`.
- **v2** (`bq_and_s3_dag_v2.py`): отдельных mapped-тасков заливки **нет** — GCS, BQ и S3
  вызываются внутри `process_date` (`:139-195`). Пустой день даёт `process_date` в состоянии
  `success` с возвратом `None`; пропускаются только побочные эффекты. Никакого `skipped` здесь
  не возникает, в том числе когда пуст весь период.

Ключевые выгоды:

- В S3 больше нет пустых объектов (0 байт для JSON, только заголовок для CSV).
- Партиция BigQuery за уже выгруженную дату больше не обнуляется пустым файлом через
  `WRITE_TRUNCATE` (сейчас это тихая потеря данных при повторном прогоне бэкфила).
- Сбой API с кодом 200 и неожиданным телом больше не маскируется под честный пустой день.

## Context (from discovery)

### Где рождается баг

- `airflow_provider_cian/operators/builder_reports.py:84-86` — `_write()` вызывается
  безусловно, независимо от того, пуст ли `records`.
- `airflow_provider_cian/operators/builder_reports.py:141-150` — `_write([])` создаёт файл
  на 0 байт (json) или файл с одним CSV-заголовком (csv).
- `airflow_provider_cian/hooks/cian.py:33` — `data.get("result", {}).get("reports", [])`
  схлопывает любой ответ 200 с неожиданной структурой в `[]`, неотличимо от пустого дня.

### Кто заливает пустой файл

- `examples/bq_and_s3_dag_v2.py:188` — `S3Hook.load_file(filename=local_path, ...)`.
- `examples/bq_and_s3_dag.py:213` и `examples/bq_and_s3_multi_account_dag.py:240` —
  `LocalFilesystemToS3Operator` через `expand_kwargs`.
- BQ с `WRITE_TRUNCATE`: `examples/bq_and_s3_dag_v2.py:169`,
  `examples/bq_and_s3_dag.py:208`, `examples/bq_and_s3_multi_account_dag.py:236`.

### Что ломается в тестах

Три разные категории — их важно не путать.

**(а) Тесты, которые упадут из-за строгой валидации хука:**

- `tests/hooks/test_cian.py:71` — `test_missing_result_key_returns_empty_list`

**(б) Тесты, которые упадут из-за смены типа возврата `execute()` (`str` → `dict | None`).**
Ломается системно, потому что три вспомогательные функции возвращают результат `execute()`,
а вызывающие тесты трактуют его как строку:

- `TestExecute._run_operator` (возвращает `path`, `:287`) → `test_json_run_creates_file:292`,
  `test_csv_run_creates_file:297`, `test_returns_output_path:300`,
  `test_different_run_ids_separate_dirs:330`, `test_json_enriched_content:334`
- `TestSnapshotTs._run_with_snapshot` (`:371`) → `test_snapshot_ts_json_records:377`,
  `test_snapshot_ts_absent_by_default:393`, `test_snapshot_ts_not_in_csv_even_when_flag_on:400`
- `TestExecuteWithAccount._run_with_account_id` (`:465`) →
  `test_execute_with_account_path_contains_cabinet_id:471`,
  `test_execute_with_account_sanitized_id_in_path:476`,
  `test_execute_without_account_with_conn_login_path_has_login:504`,
  `test_execute_without_account_without_conn_login_path_has_no_extra_dir:524`,
  `test_account_id_takes_priority_over_conn_login:565`

⚠️ Часть этих падений **тихие, а не громкие**: `assert "abc" in path` не бросит исключение,
а молча превратится из поиска подстроки в поиск ключа словаря (`"abc" in {"date", "path"}`
→ `False`). Проверять глазами, а не только по факту зелёного прогона.

**(в) Тесты, которые упадут из-за того, что исполняют оператор с пустым списком записей,
хотя проверяют совсем другое** — им нужно передать непустые записи:

- `test_idempotent_retry_deletes_old_file:305`, `test_custom_base_dir:343`

Отдельно: `test_empty_records_json_creates_empty_file:255` и
`test_empty_records_csv_creates_header_only:261` вызывают `op._write([], path)` напрямую,
а `_write` мы не трогаем — поэтому они **не упадут**. Мы их заменяем сознательно, чтобы
перенести покрытие на уровень `execute()`, где теперь и живёт решение о пустоте.
`test_empty_records_with_snapshot_ts_returns_str_path:408` — единственный из «пустых»,
который действительно сломается.

Вызовы `_make_hook_mock([])` в `TestEnrich` (`:114`-`:212`) править **не нужно**: они
исполняют `_enrich`, а не `execute`.

### Покрытие примеров DAG (важно: его почти нет)

- `tests/test_example_dag.py` импортирует ровно одну функцию — `get_date_range` из
  `examples/builder_reports_dag.py` (`:5`). Импорт-тестов для `bq_and_s3_dag.py` (v1) и
  `bq_and_s3_dag_v2.py` **не существует**.
- Единственный настоящий импорт/структурный тест — `tests/test_example_dag_multi_account.py`,
  и он опирается на заглушки провайдеров `_make_provider_stubs` (`:40-74`), которые можно
  переиспользовать.

Следствие: тесты примеров в задачах 3-6 — это **написание с нуля**, а не «обновление».

### Проверено экспериментально (Airflow 2.9.1 и 2.11.2, `dag.test()` на sqlite)

Все три факта ниже подтверждены прогоном, а не выведены из чтения кода:

1. **`None` из mapped-таска не пишется в XCom**, инстанс остаётся `success`, а день выпадает
   из списка. Downstream получает `LazyXComAccess` только с непустыми днями, упорядоченный
   по map-индексу. Триггер-правила downstream при этом не нарушаются.
2. **Если XCom не записал ни один инстанс** (весь период пустой), downstream получает
   **`None`, а не `[]`**. Наивный `len(items)` падает с
   `TypeError: object of type 'NoneType' has no len()`. Агрегатор обязан писать `items or []`.
3. **`expand_kwargs([])` разворачивается в ноль инстансов**, Airflow помечает mapped-таск
   `skipped`, `dag_run.state = success`.

Отвергнутая альтернатива (тоже проверена прогоном): бросать `AirflowSkipException` за пустой
день. В текущей форме `bq_and_s3_dag.py` / `bq_and_s3_multi_account_dag.py` один пропущенный
map-инстанс делает `skipped` весь `make_*_params` (правило `all_success`,
`ti_deps/deps/trigger_rule_dep.py:392-395`, счётчик по всем map-индексам в
`_UpstreamTIStates.calculate`), и за прогон не выгружается **ни одна** дата, включая даты
с данными. Выбранный подход (`None` + фильтрация) этой проблемы не имеет.

### Зависимости

- `apache-airflow>=2.9.1,<3.0` (целенаправленно, не менять). Прод пользователя — 2.9.1.
- Инвариант ADR-0002 (оператор не касается токена) не затрагивается.
- ADR-0001 (`snapshot_ts` — JSON-only) не затрагивается.

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
- backward compatibility **осознанно нарушается** в одном месте: тип возвращаемого значения
  `execute()` меняется со `str` на `dict | None`. Это фиксируется в CHANGELOG как breaking.

## Testing Strategy

- **unit tests**: обязательны в каждой задаче (см. Development Approach)
- **e2e tests**: в проекте нет UI/e2e — не применимо
- Тестовая команда проекта: `pytest tests/ -v`
- Примеры DAG покрыты почти никак: импорт/структурный тест есть только у multi-account
  (`tests/test_example_dag_multi_account.py`, с заглушками `_make_provider_stubs:40-74`).
  `tests/test_example_dag.py` тестирует одну чистую функцию `get_date_range`. Для v1 и v2
  импорт-тесты пишутся с нуля, переиспользуя те же заглушки.
- Приоритет покрытия: агрегаторы (`make_*_params`, `to_s3_params`) — их надо тестировать
  напрямую, включая вход `None`.
- ⚠️ **Агрегаторы и `cleanup` объявлены внутри DAG-фабрик и обёрнуты в `@task`** — вызвать
  их из теста как обычные функции нельзя. Доступ только через
  `dag.get_task("<task_id>").python_callable`; внутри `TaskGroup` task_id имеет префикс
  (`cabinet_<id>.make_gcs_params`). Либо сознательно вынести функции на уровень модуля.
  Это касается всех задач 3, 5, 6.
- ⚠️ `make_gcs_params` и `make_bq_params` в multi-account (`:168`, `:176`) читают
  `context["run_id"]` — прямой вызов без контекста даст `KeyError`, даже когда `items is None`.
  Либо ранний `return []` до чтения контекста, либо передавать `run_id` в тесте.
- **Семантику Airflow, на которой держится всё решение (`None` → нет XCom; смешанный период;
  `expand_kwargs([])` → `skipped`), обязательны интеграционные тесты через `dag.test()`
  на реальных примерах DAG** (задачи 8-10), а не только юнит-тесты оператора.
- ⚠️ Два препятствия, которые надо снять до написания сценариев (см. задачу 8):
  существующие заглушки возвращают `MagicMock` вместо настоящих операторов, поэтому
  mapped-тасков в DAG-е не будет вовсе; и `AIRFLOW_HOME` нельзя переставить из теста,
  потому что Airflow конфигурируется при импорте, а импортируется на стадии сбора тестов.
- Поведение Airflow, от которого зависит решение (`None` → нет XCom; пустой expand → skipped),
  **не мокается**: оно проверено отдельным прогоном и зафиксировано в разделе Context.
  В юнит-тестах проверяется только контракт оператора (что вернул `None` / `dict`).

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

Решение опирается на штатную семантику Airflow «нет XCom — нет элемента в списке», а не на
`AirflowSkipException`. Это даёт три свойства сразу:

1. Пустой день не создаёт файл и не создаёт mapped-инстансов downstream.
2. Смещение дат (`zip(paths, dates)`) становится невозможным по конструкции, потому что
   каждый элемент списка самоописывающий — он знает свою дату.
3. Полностью пустой период естественно приводит к нулевому expand и `skipped` без единой
   строчки специального кода.

Оператор остаётся единственным местом, которое знает, есть ли данные за дату. Хук остаётся
единственным местом, которое знает, корректен ли ответ API. Никаких новых модулей и швов.

Примеры DAG перестраиваются так: `collect.expand(date=dates)` возвращает список словарей;
между `collect` и каждым `upload`/`load` встаёт маленький `@task`-агрегатор, который
превращает этот список в список kwargs. Форма DAG почти не меняется — исчезает только
`dates` из аргументов агрегаторов и появляется `or []`.

## Technical Details

### Хук: строгая проверка формы ответа (hooks/cian.py)

```python
def get_builder_reports(self, on_date: str) -> list[dict]:
    data = self._make_request("/v1/get-builder-reports/", {"onDate": on_date})
    # data — это resp.json(): валидный JSON может быть списком, строкой или null,
    # поэтому .get() вызывать нельзя, пока не убедились, что это dict.
    result = data.get("result") if isinstance(data, dict) else None
    reports = result.get("reports") if isinstance(result, dict) else None
    if not isinstance(reports, list):
        raise AirflowException(
            f"Unexpected Cian response for onDate={on_date}: "
            f"expected a list at 'result.reports', got {str(data)[:200]}"
        )
    return reports
```

⚠️ Тестовый helper `_mock_response` (`tests/hooks/test_cian.py:26-31`) пишет
`resp.json.return_value = json_data or {}`, поэтому им **невозможно** вернуть `[]` или `None` —
оба схлопнутся в `{}`. Для тестов верхнеуровневого не-словаря helper нужно переделать на
sentinel-значение по умолчанию вместо `or {}`.

Пустой `reports: []` — легитимный ответ (день без звонков). Отсутствующий или не-списочный
`reports` — это сломанный ответ, и он обязан валить таск, а не притворяться пустым днём.

Замечание: `test_connection` (`hooks/cian.py:51`) вызывает `get_builder_reports` и ловит
любое исключение → при неожиданном теле вернёт `(False, msg)`. Это желаемое поведение.

### Оператор: не писать файл, вернуть dict | None (operators/builder_reports.py)

```python
def execute(self, context) -> dict | None:
    snapshot_ts = ...
    cabinet_id = resolve_cabinet_id(self.cian_conn_id, self.account_id)
    hook = CianHook(cian_conn_id=self.cian_conn_id, account_id=self.account_id)

    output_path = self._build_path(context["run_id"], cabinet_id)

    # Снимаем возможный файл прошлой попытки ДО решения о пустоте:
    # иначе при ретрае, когда данные исчезли, наверху остался бы устаревший файл.
    if os.path.exists(output_path):
        os.remove(output_path)

    records = hook.get_builder_reports(self.date)
    if not records:
        self.log.info("Cian returned no reports for %s — nothing to write", self.date)
        return None

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    enriched = self._enrich(records, hook, snapshot_ts)
    self._write(enriched, output_path)

    return {"date": self.date, "path": output_path}
```

Порядок важен: удаление устаревшего файла происходит **до** проверки на пустоту, а
`makedirs` — **после**, чтобы за пустой день не оставалось ни файла, ни пустой директории
от текущего запуска (директория запуска может уже существовать из-за соседних дат — это
нормально).

`_write()` больше никогда не вызывается с пустым списком. Его собственная защита не нужна,
но и не мешает; специально «чинить» `_write` не требуется.

### Агрегаторы в примерах DAG

Общая форма (обратите внимание на `or []` — это факт №2 из Context, без него полностью
пустой период валит DAG):

```python
@task
def to_s3_params(items, cabinet_id=None, **context):
    items = list(items or [])          # None, когда XCom не записал ни один день
    params = []
    for it in items:
        d = it["date"]
        year, month, day = d.split("-")
        params.append({
            "filename": it["path"],
            "dest_key": f"{S3_PREFIX}/_year={year}/_month={month}/_day={day}"
                        f"/_date={d.replace('-', '')}/{d}.json",
        })
    return params
```

`make_bq_params` в обоих примерах сейчас строится из `dates`, **независимо** от того, есть ли
данные (`examples/bq_and_s3_dag.py:231` → `make_bq_params(dates)`;
`examples/bq_and_s3_multi_account_dag.py:249`). Его тоже нужно перевести на `items`, иначе
`load_bq` полезет за несуществующим GCS-объектом за пустой день.

`cleanup` в обоих примерах принимает `paths: list[str]` и берёт `os.path.dirname(paths[0])`.
Теперь на вход приходят словари (или `None`) — нужно `items = list(items or [])` и
`os.path.dirname(items[0]["path"])`.

### Пример v2 (bq_and_s3_dag_v2.py)

Здесь `process_date` — один `@task`, который сам вызывает `execute()`. Правка точечная:

```python
result = CianBuilderReportsOperator(...).execute({"run_id": run_id or ""})
if result is None:
    return None                     # день пустой: ни GCS, ни BQ, ни S3
local_path = result["path"]
```

`cleanup` (`examples/bq_and_s3_dag_v2.py:197-206`) уже начинается с `if not paths: return`,
что корректно обрабатывает и `None`, и `[]`, но `paths[0]` теперь — `None`-элементы
отфильтрованы Airflow'ом, а непустые остаются строками (`process_date` возвращает `local_path`).
Проверить и при необходимости привести к тому же виду.

### Пример builder_reports_dag.py

`_cleanup` (`examples/builder_reports_dag.py:69-75`) делает `ti.xcom_pull(task_ids="collect")`
и итерируется по путям. Теперь элементы — словари. Правка на одну строку:
`path = item["path"] if isinstance(item, dict) else item`, либо просто `item["path"]`
с фильтрацией `or []`.

### Документация

- В README.md / README_ru.md зафиксировать новый контракт: за день без данных файл не
  создаётся, `execute()` возвращает `None`, XCom не пишется; за день с данными возвращается
  `{"date", "path"}`. Явно предупредить про `items or []` в агрегаторах.
- В CONTEXT.md добавить термин **Пустой день (empty day)** — день, за который Cian вернул
  `reports: []`; отличается от сломанного ответа API, который теперь падает.
- В CHANGELOG.md — секция `## Unreleased` с **Breaking**: тип возврата `execute()`.

## Границы объёма: ядро и харнесс

**Задачи 1-7 — ядро правки, самодостаточное.** После них баг закрыт: пустой день не порождает
файл, партиция BQ не обнуляется, сломанный ответ API не маскируется под пустой день. Всё это
покрыто юнит-тестами, включая прямые ассерты `make_*_params(None) == []` — самой тонкой точки.

**Задачи 8-10 — интеграционный харнесс, помечен маркером `integration` и ядро не блокирует.**
Он ценен тем, что проверяет проводку реальных примеров DAG, но целится в `examples/`, а не в
код библиотеки, и требует изоляции метадата-БД Airflow — вещь version-sensitive и склонная
к флаки. Перед началом задачи 8 сознательно решить, оправдан ли живой `dag.test()` или
достаточно структурных ассертов на собранном DAG-объекте (рёбра, trigger rules, разворачивание
в ноль инстансов) без метадата-БД. Если харнесс окажется нестабильным — задачи 1-7 уже дают
рабочую правку, а компенсирующий контроль описан в Post-Completion (ручная проверка на живом
Airflow).

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): изменения кода, тестов и документации в этом репозитории
- **Post-Completion** (без чекбоксов): ручная проверка на живом Airflow, релиз

## Implementation Steps

### Task 1: Строгая валидация ответа в CianHook.get_builder_reports

**Files:**
- Modify: `airflow_provider_cian/hooks/cian.py`
- Modify: `tests/hooks/test_cian.py`

- [x] в `get_builder_reports` заменить `data.get("result", {}).get("reports", [])` на явную проверку: **сначала `isinstance(data, dict)`** (иначе `.get` на списке/строке даст `AttributeError`, а не `AirflowException`), затем `result` — dict, затем `result["reports"]` — list; иначе `AirflowException` с `onDate` и `str(data)[:200]`
- [x] добавить в код комментарий, почему `reports: []` — легитимный ответ, а отсутствие ключа — нет
- [x] обновить аннотацию `_make_request` (`hooks/cian.py:58`) с `-> dict` на `-> object` (или явный JSON-тип): она перестала быть правдой в тот момент, когда мы начали обрабатывать список/строку/`null`
- [x] переделать helper `_mock_response` (`tests/hooks/test_cian.py:26-31`): заменить `json_data or {}` на sentinel и расширить аннотацию с `dict | None` до `object`, иначе `[]` и `None` невозможно передать в тест
- [x] переписать `tests/hooks/test_cian.py:71` `test_missing_result_key_returns_empty_list` → `test_missing_result_key_raises` (ожидает `AirflowException`, в сообщении есть `onDate`)
- [x] написать тест: `{"result": {"reports": []}}` по-прежнему возвращает `[]` (не падает) — покрыт существующим `test_empty_reports_returns_empty_list`
- [x] написать тест: `{"result": {"reports": {}}}` (не список) → `AirflowException`
- [x] написать тест: `{"errors": [...]}` с кодом 200 → `AirflowException`, а не тихий `[]`
- [x] написать тест: тело верхнего уровня — список `[]` и `null` → `AirflowException`, а не `AttributeError`
- [x] проверить, что `test_connection` при неожиданном теле возвращает `(False, msg)`
- [x] run tests (`pytest tests/ -v`) — must pass before task 2

### Task 2: Оператор не пишет файл за пустой день и возвращает dict | None

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [x] в `execute()` перенести `os.makedirs` после проверки на пустоту; удаление устаревшего файла оставить до неё
- [x] при `not records`: залогировать и `return None` (файл не создавать)
- [x] при непустых записях вернуть `{"date": self.date, "path": output_path}`
- [x] обновить аннотацию возврата `execute()` на `dict | None`, обновить docstring
- [x] развернуть dict в трёх вспомогательных функциях тестов — `TestExecute._run_operator:287`, `TestSnapshotTs._run_with_snapshot:371`, `TestExecuteWithAccount._run_with_account_id:465` — чтобы вызывающие тесты продолжали получать строку-путь
- [x] пройти глазами все ассерты классов `TestExecute`, `TestSnapshotTs`, `TestExecuteWithAccount` (~12 тестов, перечислены в Context, категория «б»): `"abc" in path` тихо меняет смысл на проверку ключа словаря и НЕ падает — проверено: во всех классах `path` разворачивается helper'ом/`["path"]` в строку до ассертов
- [x] починить тесты, которые исполняют оператор с `_make_hook_mock([])`, но проверяют другое: `test_idempotent_retry_deletes_old_file:305`, `test_custom_base_dir:343` — передать непустые записи
- [x] добавить хотя бы один тест, который проверяет dict-контракт напрямую (а не через развёрнутый в helper путь): непустой день возвращает dict ровно с ключами `{"date", "path"}`, `date == self.date`. Проверять **свойство пути** — `os.path.exists(result["path"])` и `result["path"].endswith(".json")` — а НЕ вхождение вида `"abc" in result`: именно вхождение и деградирует молча из подстроки в ключ словаря (`test_returns_dict_contract`)
- [x] **сохранить** `test_empty_records_json_creates_empty_file:255` и `test_empty_records_csv_creates_header_only:261` как есть: они тестируют `_write` напрямую, `_write` мы не меняем, и это дешёвое стабильное покрытие. Удалять их незачем
- [x] **добавить** рядом новые тесты уровня `execute()`: за пустой день файл **не создан**, возвращён `None` (для json и для csv) (`TestExecuteEmptyDay`)
- [x] обновить аннотацию возврата оператора до `dict[str, str] | None`
- [x] заменить `test_empty_records_with_snapshot_ts_returns_str_path:408` на тест «пустые записи + `add_snapshot_ts=True` → `None`, файла нет» (`test_empty_records_with_snapshot_ts_returns_none_and_no_file`)
- [x] написать тест: устаревший файл прошлой попытки удаляется, даже если в этот раз данных нет (`test_stale_file_removed_even_when_no_data`)
- [x] написать тест: за пустой день не создаётся и директория запуска (если её не создали соседние даты) (`test_empty_day_creates_no_run_directory` + `..._but_keeps_sibling`)
- [x] run tests — must pass before task 3

### Task 3: Пример bq_and_s3_dag_v2.py — пропуск заливки за пустой день

**Files:**
- Modify: `examples/bq_and_s3_dag_v2.py`
- Create: `tests/example_stubs.py` (или `tests/conftest.py`)
- Modify: `tests/test_example_dag_multi_account.py`
- Create: `tests/test_example_dag_v2.py`

- [x] в `process_date` принять результат `execute()`, при `None` вернуть `None` до всех заливок (GCS, BQ, S3)
- [x] при непустом результате взять `local_path = result["path"]`
- [x] обновить аннотацию `process_date` с `-> str` на `-> str | None` и её docstring
- [x] проверить `cleanup` (`:197`) на устойчивость к `None`/пустому списку — уже начинается с `if not paths: return`, покрывает и `None`, и `[]`; правка не нужна (покрыто тестами)
- [x] обновить docstring DAG: за пустой день заливок нет, `process_date` возвращает `None` и остаётся `success`; **отдельных mapped upload-тасков в v2 нет, поэтому `skipped` тут не появляется никогда** (в отличие от v1/multi-account)
- [x] **вынести `_make_provider_stubs` из `tests/test_example_dag_multi_account.py:40-74` в общий `tests/example_stubs.py`** и переключить существующий тест на него — заглушки теперь нужны трём тестовым модулям
- [x] **расширить заглушки для v2**: существующие покрывают только transfer-операторы, а v2 импортирует `S3Hook` (`:25`), `BigQueryHook` (`:26`), `GCSHook` (`:27`), `google.api_core.exceptions.Conflict` (`:28`) и `google.cloud.bigquery` (`:29`) — без них тест упадёт на импорте, а positive-path не сможет создать `SchemaField`/`LoadJobConfig`
- [x] создать `tests/test_example_dag_v2.py` с импорт-тестом DAG — сейчас v2 не покрыт вообще
- [x] доступ к вложенным `@task`-функциям только через `dag.get_task("process_date").python_callable` / `dag.get_task("cleanup").python_callable`
- [x] написать тест `cleanup(None)` и `cleanup([])` не падают
- [x] написать тест: `process_date` при `execute() → None` не вызывает ни GCSHook, ни BigQueryHook, ни S3Hook (моки; проверить, что заливки действительно не произошло)
- [x] написать тест: `process_date` при непустом результате берёт путь из `result["path"]` и вызывает все три заливки
- [x] run tests — must pass before task 4

### Task 4: Пример builder_reports_dag.py — cleanup под новый контракт XCom

**Files:**
- Modify: `examples/builder_reports_dag.py`
- Modify: `tests/test_example_dag.py`

- [x] в `_cleanup` (`:69`) обработать `paths=None` и элементы-словари, удалять `item["path"]`
- [x] обновить docstring DAG: за пустой день файла нет, XCom пуст
- [x] экспортировать `_cleanup` так, чтобы он был тестируем напрямую (или тестировать через `ti.xcom_pull`-мок) — вынесен на уровень модуля
- [x] написать тест: `_cleanup` при `xcom_pull → None` не падает
- [x] написать тест: `_cleanup` при списке словарей удаляет именно `item["path"]`
- [x] написать тест: `_cleanup` при несуществующем файле не падает
- [x] run tests — must pass before task 5

### Task 5: Пример bq_and_s3_dag.py — агрегаторы поверх collected items

**Files:**
- Modify: `examples/bq_and_s3_dag.py`
- Create: `tests/test_example_dag_v1.py`

- [x] перевести `make_gcs_params`, `make_s3_params`, `make_bq_params` на единственный вход `items` (убрать `dates` и `zip`)
- [x] в каждом агрегаторе первой строкой `items = list(items or [])` — иначе полностью пустой период валит DAG (факт №2 в Context)
- [x] `make_bq_params` строить из `items`, а не из `dates` (сейчас определён на `:144`, вызывается на `:225`) — иначе `load_bq` пойдёт за несуществующим GCS-объектом
- [x] `cleanup` (`:175`) перевести на `items`, брать `os.path.dirname(items[0]["path"])`
- [x] обновить блок «Execution & dependencies» (`:220-234`): `collected = collect.expand(date=dates).output`, агрегаторы от `collected`
- [x] обновить аннотации агрегаторов и `cleanup`: вход больше не `list[str]`, а `list[dict] | None`
- [x] обновить docstring DAG: mapped-таски заливки создаются только за дни с данными; полностью пустой период → `upload_*`/`load_bq` в `skipped`
- [x] создать `tests/test_example_dag_v1.py` с импорт-тестом DAG (v1 сейчас не покрыт; переиспользовать `_make_provider_stubs`) — это написание с нуля, а не обновление
- [x] доступ к агрегаторам и `cleanup` только через `dag.get_task("<task_id>").python_callable` — они объявлены внутри DAG-фабрики и обёрнуты в `@task`, напрямую не вызываются
- [x] **написать тесты на `None`-путь: `make_gcs_params(None) == []`, `make_s3_params(None) == []`, `make_bq_params(None) == []`** — это самая тонкая точка всей правки (эмпирический факт №2), и держаться она должна на прямом ассерте, а не на импорт-тесте
- [x] написать тест: агрегаторы на списке из двух словарей дают ключи ровно за эти две даты (даты не смещены)
- [x] написать тест: `cleanup(None)` и `cleanup([])` не падают
- [x] run tests — must pass before task 6

### Task 6: Пример bq_and_s3_multi_account_dag.py — то же на уровне кабинета

**Files:**
- Modify: `examples/bq_and_s3_multi_account_dag.py`
- Modify: `tests/test_example_dag_multi_account.py`

- [x] перевести `make_gcs_params` (`:167`), `make_bq_params` (`:175`), `make_s3_params` (`:188`) на вход `items` + `cabinet_id`, убрать `zip(paths, dates)`
- [x] в каждом агрегаторе `items = list(items or [])`
- [x] ⚠️ в `make_gcs_params` (`:168`) и `make_bq_params` (`:176`) сделать `return []` **до** чтения `context["run_id"]` (`:169`, `:177`) — иначе прямой вызов из теста даст `KeyError`, даже когда `items is None`
- [x] `make_bq_params` строить из `items`, а не из `dates`
- [x] `cleanup` (`:204-212`) — правка **не нужна**: он вычисляет `run_dir` из `BASE_DIR`/`cabinet_id`/`sid` и никогда не индексирует `paths`, а `if not paths: return` уже покрывает и `None`, и `[]`. Только убедиться в этом и не «чинить» лишнего
- [x] обновить аннотации агрегаторов: вход `list[dict] | None`, не `list[str]`
- [x] обновить `make_cabinet_group` (`:214-257`): `collected = collect.expand(date=dates).output`, агрегаторы от `collected`
- [x] независимость кабинетов (пустой кабинет не подавляет соседей) проверяется интеграционно в задаче 10, здесь достаточно юнит-уровня
- [x] доступ к агрегаторам через `dag.get_task("cabinet_<id>.<task_id>").python_callable` — внутри `TaskGroup` task_id имеет префикс группы
- [x] написать тесты на `None`-путь для всех трёх агрегаторов: `make_*_params(None, cabinet_id="x") == []`
- [x] написать тест: агрегаторы кладут `cabinet_id` в ключи GCS/S3 и в имя BQ-таблицы
- [x] обновить тесты структуры в `tests/test_example_dag_multi_account.py` (ассерты по task_id, если менялись) — task_id не менялись; добавлены новые тестовые классы для агрегаторов и cleanup
- [x] обновить docstring DAG
- [x] run tests — must pass before task 7

### Task 7: Документация — README, CONTEXT, CHANGELOG

**Files:**
- Modify: `README.md`
- Modify: `README_ru.md`
- Modify: `CONTEXT.md`
- Modify: `CHANGELOG.md`

- [x] ⚠️ **починить работающие code snippets** в `README.md:184-198` и `README_ru.md:184-198`: там `cleanup` делает `os.path.exists(path)` над результатом `xcom_pull("collect")`, куда теперь придёт словарь; и комментарий `.expand(filename=collect)` тоже станет неверным. Заменить на `item["path"]` и агрегатор перед `expand`
- [x] в обоих README описать новый контракт `execute()`: `None` за пустой день (файл не создаётся, XCom не пишется), `{"date", "path"}` за день с данными
- [x] в обоих README добавить предупреждение для авторов DAG: агрегатор обязан писать `items or []`, потому что при полностью пустом периоде Airflow отдаёт `None`, а не `[]`
- [x] в обоих README описать поведение полностью пустого периода: mapped-таски разворачиваются в ноль инстансов и помечаются `skipped`, `dag_run` успешен (и отдельно оговорено, что в v2 `skipped` не бывает)
- [x] в обоих README отметить, что неожиданный ответ API (200 без `result.reports`) теперь валит таск, а не выглядит как пустой день
- [x] в `CONTEXT.md` добавить термин **Пустой день** и явно отделить его от сломанного ответа API
- [x] создать секцию `## Unreleased` в `CHANGELOG.md` (сейчас её нет): **Breaking** — тип возврата `execute()` со `str` на `dict | None`; **Fixed** — пустой файл в S3/GCS и обнуление партиции BQ; **Changed** — строгая валидация ответа хука
- [x] проверить консистентность README.md и README_ru.md между собой — обе секции добавлены идентично (EN/RU параллельны)
- [x] run tests — must pass before task 8 (162 passed)

### Task 8: Реальные операторы-заглушки, чтобы mapped-таски регистрировались

> **Решение по объёму (2026-07-10).** Живой `dag.test()` с метадата-БД отклонён: изоляция
> `AIRFLOW_HOME` из теста ненадёжна (Airflow конфигурируется при импорте, а импортируется на
> стадии сбора тестов), а сам подход version-sensitive и склонен к флаки. Вместо него — **структурные
> ассерты на собранном DAG-объекте** без метадата-БД (задачи 9-10). Самая тонкая точка,
> `make_*_params(None) == []`, уже покрыта прямыми юнит-ассертами в задачах 5-6, а семантика
> `None → нет XCom → ноль expand → skipped` подтверждена экспериментально (Context) и вынесена
> в ручную проверку Post-Completion.

**Files:**
- Modify: `tests/example_stubs.py`

Проблема, ради которой существует эта задача: `_make_provider_stubs` делает
`GCSToBQ.partial = MagicMock(return_value=MagicMock())`, поэтому `.expand_kwargs(...)` возвращает
`MagicMock`, а не `MappedOperator` — и в собранном DAG-е нет ни `upload_gcs`, ни `upload_s3`,
ни `load_bq`. Структурные ассерты по рёбрам и trigger rules на таком DAG-е проверяли бы пустоту.

- [x] добавить в `tests/example_stubs.py` **лёгкие реальные подклассы `BaseOperator`** для трёх transfer-операторов (`LocalFilesystemToGCSOperator`, `GCSToBigQueryOperator`, `LocalFilesystemToS3Operator`) с теми же `template_fields`, у которых `execute()` тривиален; они должны поддерживать `.partial()`/`.expand_kwargs()` как настоящие операторы
- [x] дать `_make_provider_stubs` (или отдельному хелперу — на усмотрение) режим, где transfer-операторы подставляются этими реальными подклассами, а не `MagicMock`; существующие импорт-тесты, которым хватает `MagicMock`, не ломать — добавлен параметр `real_transfer_operators=False` в `_make_provider_stubs` и `import_dag_module`
- [x] написать тест: при подстановке реальных подклассов `.expand_kwargs(...)` регистрирует в DAG настоящий `MappedOperator` (защита от регрессии — иначе структурные ассерты задач 9-10 молча проверяют пустоту) — тест в новом `tests/test_example_stubs.py` (плановый список Files назвал только `example_stubs.py`, но pytest-собираемый тест обязан жить в `test_*.py`)
- [x] run tests — must pass before task 9 (169 passed)

> ⚠️ **Замечание по реализации.** `partial()` валидирует kwargs против ИМЁН параметров
> `__init__` (через `BaseOperatorMeta.__param_names`, выводимых из литеральной сигнатуры).
> Голый `**kwargs` не проходит: `partial(gcp_conn_id=..., bucket=...)` бросает `TypeError`.
> Поэтому каждый подкласс объявляет все свои provider-специфичные kwargs (и `partial`-,
> и `expand_kwargs`-ключи) явными именованными параметрами `__init__` со значением `None`,
> принимает и игнорирует их. Добавлен новый файл `tests/test_example_stubs.py`.

### Task 9: Структурные ассерты на собранном bq_and_s3_dag.py

**Files:**
- Create: `tests/test_example_dag_v1_structure.py`

Проверяем **реальный** пример DAG, собранный с реальными операторами-заглушками задачи 8 —
без метадата-БД и без `dag.test()`. Ловим забытое ребро зависимости или неверный trigger rule
в отгружаемом коде, чего импорт-тест не видит.

- [x] собрать `examples/bq_and_s3_dag.py` с реальными операторами-заглушками; получить DAG-объект
- [x] тест: `upload_gcs`/`upload_s3`/`load_bq` присутствуют в `dag.task_dict` как `MappedOperator`
- [x] тест: рёбра зависимостей соответствуют плану (`collect → агрегатор → upload/load`, `cleanup` в конце), trigger rule `cleanup` — `all_done`, у остальных — дефолтный
- [x] тест: агрегатор `collect`→`upload_*` берёт вход из `collect.output` (проводка через `collected`, а не из отдельного `dates`)
- [x] тест «смешанный период» на уровне агрегатора: список из N словарей → ровно N kwargs, даты не смещены (это то, во что развернётся mapped-таск)
- [x] тест «полностью пустой период»: агрегаторы на `None` → `[]`, то есть mapped-таск развернулся бы в ноль инстансов (дублирует юнит-ассерт задачи 5 на уровне собранного DAG — оставить как явную страховку проводки)
- [x] run tests — must pass before task 10 (187 passed)

### Task 10: Структурные ассерты изоляции кабинетов (multi-account)

**Files:**
- Create: `tests/test_example_dag_multi_account_structure.py`

Отдельная задача, потому что проверяет **не** ту же семантику, что задача 9, а независимость
кабинетов: пустой кабинет не должен влиять на соседний.

- [x] собрать `examples/bq_and_s3_multi_account_dag.py` с двумя кабинетами и реальными операторами-заглушками
- [x] тест: у каждого кабинета свой `TaskGroup` с префиксом `cabinet_<id>.`, `upload_s3`/`upload_gcs`/`load_bq` зарегистрированы внутри своей группы
- [x] тест: рёбра и trigger rules внутри одной группы не ссылаются на таски другой группы (группы структурно независимы — пустой кабинет не может подавить соседа)
- [x] тест: агрегаторы каждого кабинета кладут его `cabinet_id` в ключи и имя BQ-таблицы (не путают кабинеты)
- [x] run tests — must pass before task 11 (197 passed)

### Task 11: Verify acceptance criteria

- [x] verify all requirements from Overview are implemented — подтверждено: хук валидирует форму ответа (`hooks/cian.py`), оператор возвращает `dict | None` без файла за пустой день (`operators/builder_reports.py`), все четыре примера DAG переведены на самоописывающие items; 197 тестов зелёные
- [x] edge case: пустой день — файла нет и сам этот день не создаёт директорию запуска (существующая директория, созданная соседними непустыми датами, — это норма), XCom не записан — покрыто `TestExecuteEmptyDay::test_empty_day_json_returns_none_no_file`, `test_empty_day_csv_returns_none_no_file`, `test_empty_day_creates_no_run_directory`, `test_empty_day_creates_no_run_directory_but_keeps_sibling` (`tests/operators/test_builder_reports.py:601-642`); отсутствие XCom обеспечивается возвратом `None` (штатная семантика Airflow, зафиксирована в Context)
- [x] edge case: полностью пустой период в v1/multi-account — агрегаторы на `None` → `[]` (структурно mapped-таск развернулся бы в ноль инстансов → `skipped`); живой прогон вынесен в Post-Completion — покрыто `tests/test_example_dag_v1.py:78,83,88`, `tests/test_example_dag_v1_structure.py:208,213,218`, `tests/test_example_dag_multi_account.py:160,165,170`
- [x] edge case: полностью пустой период в v2 — `process_date` в `success` с `None`, `skipped` нет вовсе — покрыто `tests/test_example_dag_v2.py::test_empty_day_skips_all_uploads` (result is None, ни один из GCS/BQ/S3 хуков не вызван)
- [x] edge case: ретрай дня, за который данные исчезли — устаревший файл удалён — покрыто `TestExecuteEmptyDay::test_stale_file_removed_even_when_no_data` (`tests/operators/test_builder_reports.py:644`)
- [x] edge case: ответ 200 без `result.reports` (включая тело-список и `null`) → `AirflowException` — покрыто `tests/hooks/test_cian.py`: `test_missing_result_key_raises:77`, `test_reports_not_a_list_raises:85`, `test_errors_body_with_200_raises:93`, `test_top_level_list_body_raises:101`, `test_top_level_null_body_raises:109`
- [x] edge case: смешанный период — заливка идёт только за дни с данными, даты не смещены — покрыто `tests/test_example_dag_v1.py::test_make_gcs_params_maps_each_item:103` и `test_make_s3_params_dates_not_shifted:113`, `tests/test_example_dag_v1_structure.py::test_s3_params_dates_not_shifted:176`, `tests/test_example_dag_multi_account.py::test_make_s3_params_contains_cabinet_id_and_dates_not_shifted:195`
- [x] `grep -rn "zip(paths" examples/` пуст — смещение дат невозможно по конструкции — подтверждено: grep возвращает пусто (exit 1, нет совпадений)
- [x] `grep -rn "resolve_token" airflow_provider_cian/operators/` пуст — инвариант ADR-0002 не нарушен — подтверждено: grep возвращает пусто (exit 1, нет совпадений)
- [x] verify test coverage meets project standard (новые ветки покрыты) — pytest-cov/coverage в окружении не установлены (pip install запрещён), поэтому покрытие проверено сопоставлением веток с тестами: новая ветка валидации хука (`isinstance`+`raise`) — 5 тестов выше; ветка `if not records: return None` — `TestExecuteEmptyDay`; ветка удаления устаревшего файла — `test_stale_file_removed_even_when_no_data`; `items = list(items or [])` во всех агрегаторах — `*_params(None) == []` тесты. Все новые ветки имеют выделенный тест; 197/197 зелёные

### Task 12: [Final] ADR-0003 и завершение

**Files:**
- Create: `docs/adr/0003-empty-day-signalling.md`

- [x] написать ADR-0003 «Пустой день сигнализируется отсутствием XCom, а не AirflowSkipException»: контекст, оба варианта, результат эксперимента на 2.9.1 (skip гасит `make_*_params` через `all_success` и выгрузка не идёт ни за одну дату), выбранное решение и его следствия (`items or []`, самоописывающий dict) — создан `docs/adr/0003-empty-day-signalling.md` в формате ADR-0001/0002 (RU)
- [x] update README.md / CONTEXT.md, если по ходу реализации что-то разошлось с задачей 7 — no drift: контракт `execute()` (`None`/`{"date","path"}`), `items or []`, поведение пустого периода и v2, и «сломанный ответ API валит таск» уже описаны в README.md/README_ru.md и CONTEXT.md, всё соответствует коду
- [x] **единственный final test gate**: `pytest tests/ -v` целиком зелёный, включая интеграционные тесты задач 8-10 — 197 passed
- [x] deferred to harness (do not move mid-run) — плановый файл перемещается харнессом после завершения всех фаз, вручную не трогаем

## Post-Completion

*Ручные шаги вне этого репозитория — без чекбоксов*

**Manual verification:**

- интеграционные тесты задач 8-10 прогнать на минимально поддерживаемой версии Airflow 2.9.1
  (это забота CI-матрицы или локального окружения, а не одного вызова `pytest`; в самом
  тесте версию не пинить)
- прогнать реальный DAG на живом Airflow 2.9.1 за период, где заведомо есть день без звонков:
  убедиться, что за этот день нет объекта в S3, нет mapped-инстанса `upload_s3`, а соседние
  даты выгрузились
- прогнать DAG за заведомо пустой период целиком: `upload_*` и `load_bq` должны быть `skipped`,
  `dag_run` — `success`
- проверить, что партиция BigQuery за ранее выгруженную дату не обнуляется при повторном
  прогоне, если Cian вернул пусто

**External system updates:**

- потребители, читавшие XCom `collect` как строку с путём, сломаются: теперь это `dict | None`.
  Упомянуть в release notes отдельным пунктом Breaking
- владельцам прод-DAG-ов, построенных по схеме `zip(paths, dates)`, нельзя выкатывать новую
  версию провайдера, не обновив DAG: список `paths` станет короче списка `dates`
