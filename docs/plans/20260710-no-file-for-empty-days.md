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

- [ ] в `_cleanup` (`:69`) обработать `paths=None` и элементы-словари, удалять `item["path"]`
- [ ] обновить docstring DAG: за пустой день файла нет, XCom пуст
- [ ] экспортировать `_cleanup` так, чтобы он был тестируем напрямую (или тестировать через `ti.xcom_pull`-мок)
- [ ] написать тест: `_cleanup` при `xcom_pull → None` не падает
- [ ] написать тест: `_cleanup` при списке словарей удаляет именно `item["path"]`
- [ ] написать тест: `_cleanup` при несуществующем файле не падает
- [ ] run tests — must pass before task 5

### Task 5: Пример bq_and_s3_dag.py — агрегаторы поверх collected items

**Files:**
- Modify: `examples/bq_and_s3_dag.py`
- Create: `tests/test_example_dag_v1.py`

- [ ] перевести `make_gcs_params`, `make_s3_params`, `make_bq_params` на единственный вход `items` (убрать `dates` и `zip`)
- [ ] в каждом агрегаторе первой строкой `items = list(items or [])` — иначе полностью пустой период валит DAG (факт №2 в Context)
- [ ] `make_bq_params` строить из `items`, а не из `dates` (сейчас определён на `:144`, вызывается на `:225`) — иначе `load_bq` пойдёт за несуществующим GCS-объектом
- [ ] `cleanup` (`:175`) перевести на `items`, брать `os.path.dirname(items[0]["path"])`
- [ ] обновить блок «Execution & dependencies» (`:220-234`): `collected = collect.expand(date=dates).output`, агрегаторы от `collected`
- [ ] обновить аннотации агрегаторов и `cleanup`: вход больше не `list[str]`, а `list[dict] | None`
- [ ] обновить docstring DAG: mapped-таски заливки создаются только за дни с данными; полностью пустой период → `upload_*`/`load_bq` в `skipped`
- [ ] создать `tests/test_example_dag_v1.py` с импорт-тестом DAG (v1 сейчас не покрыт; переиспользовать `_make_provider_stubs`) — это написание с нуля, а не обновление
- [ ] доступ к агрегаторам и `cleanup` только через `dag.get_task("<task_id>").python_callable` — они объявлены внутри DAG-фабрики и обёрнуты в `@task`, напрямую не вызываются
- [ ] **написать тесты на `None`-путь: `make_gcs_params(None) == []`, `make_s3_params(None) == []`, `make_bq_params(None) == []`** — это самая тонкая точка всей правки (эмпирический факт №2), и держаться она должна на прямом ассерте, а не на импорт-тесте
- [ ] написать тест: агрегаторы на списке из двух словарей дают ключи ровно за эти две даты (даты не смещены)
- [ ] написать тест: `cleanup(None)` и `cleanup([])` не падают
- [ ] run tests — must pass before task 6

### Task 6: Пример bq_and_s3_multi_account_dag.py — то же на уровне кабинета

**Files:**
- Modify: `examples/bq_and_s3_multi_account_dag.py`
- Modify: `tests/test_example_dag_multi_account.py`

- [ ] перевести `make_gcs_params` (`:167`), `make_bq_params` (`:175`), `make_s3_params` (`:188`) на вход `items` + `cabinet_id`, убрать `zip(paths, dates)`
- [ ] в каждом агрегаторе `items = list(items or [])`
- [ ] ⚠️ в `make_gcs_params` (`:168`) и `make_bq_params` (`:176`) сделать `return []` **до** чтения `context["run_id"]` (`:169`, `:177`) — иначе прямой вызов из теста даст `KeyError`, даже когда `items is None`
- [ ] `make_bq_params` строить из `items`, а не из `dates`
- [ ] `cleanup` (`:204-212`) — правка **не нужна**: он вычисляет `run_dir` из `BASE_DIR`/`cabinet_id`/`sid` и никогда не индексирует `paths`, а `if not paths: return` уже покрывает и `None`, и `[]`. Только убедиться в этом и не «чинить» лишнего
- [ ] обновить аннотации агрегаторов: вход `list[dict] | None`, не `list[str]`
- [ ] обновить `make_cabinet_group` (`:214-257`): `collected = collect.expand(date=dates).output`, агрегаторы от `collected`
- [ ] независимость кабинетов (пустой кабинет не подавляет соседей) проверяется интеграционно в задаче 10, здесь достаточно юнит-уровня
- [ ] доступ к агрегаторам через `dag.get_task("cabinet_<id>.<task_id>").python_callable` — внутри `TaskGroup` task_id имеет префикс группы
- [ ] написать тесты на `None`-путь для всех трёх агрегаторов: `make_*_params(None, cabinet_id="x") == []`
- [ ] написать тест: агрегаторы кладут `cabinet_id` в ключи GCS/S3 и в имя BQ-таблицы
- [ ] обновить тесты структуры в `tests/test_example_dag_multi_account.py` (ассерты по task_id, если менялись)
- [ ] обновить docstring DAG
- [ ] run tests — must pass before task 7

### Task 7: Документация — README, CONTEXT, CHANGELOG

**Files:**
- Modify: `README.md`
- Modify: `README_ru.md`
- Modify: `CONTEXT.md`
- Modify: `CHANGELOG.md`

- [ ] ⚠️ **починить работающие code snippets** в `README.md:184-198` и `README_ru.md:184-198`: там `cleanup` делает `os.path.exists(path)` над результатом `xcom_pull("collect")`, куда теперь придёт словарь; и комментарий `.expand(filename=collect)` тоже станет неверным. Заменить на `item["path"]` и агрегатор перед `expand`
- [ ] в обоих README описать новый контракт `execute()`: `None` за пустой день (файл не создаётся, XCom не пишется), `{"date", "path"}` за день с данными
- [ ] в обоих README добавить предупреждение для авторов DAG: агрегатор обязан писать `items or []`, потому что при полностью пустом периоде Airflow отдаёт `None`, а не `[]`
- [ ] в обоих README описать поведение полностью пустого периода: mapped-таски разворачиваются в ноль инстансов и помечаются `skipped`, `dag_run` успешен
- [ ] в обоих README отметить, что неожиданный ответ API (200 без `result.reports`) теперь валит таск, а не выглядит как пустой день
- [ ] в `CONTEXT.md` добавить термин **Пустой день** и явно отделить его от сломанного ответа API
- [ ] создать секцию `## Unreleased` в `CHANGELOG.md` (сейчас её нет): **Breaking** — тип возврата `execute()` со `str` на `dict | None`; **Fixed** — пустой файл в S3/GCS и обнуление партиции BQ; **Changed** — строгая валидация ответа хука
- [ ] проверить консистентность README.md и README_ru.md между собой
- [ ] run tests — must pass before task 8

### Task 8: Харнесс для интеграционных тестов (изоляция БД + настоящие операторы)

**Files:**
- Create: `tests/integration/conftest.py`
- Modify: `tests/example_stubs.py`
- Modify: `pyproject.toml`

Юнит-тесты не поймают ни неверную DAG-зависимость, ни неверное trigger rule, ни расхождение
поведения между версиями Airflow. Но прежде чем писать сценарии, надо решить две проблемы,
каждая из которых делает наивный интеграционный тест бессмысленным.

⚠️ **Проблема 1 — заглушки не создают настоящих тасков.** `_make_provider_stubs` делает
`GCSToBQ.partial = MagicMock(return_value=MagicMock())`
(`tests/test_example_dag_multi_account.py:56`). Значит `.expand_kwargs(...)` вернёт `MagicMock`,
а не `MappedOperator`, и в DAG-е не будет ни `upload_gcs`, ни `upload_s3`, ни `load_bq`.
Ассерт «развернулось ровно N инстансов» на таком DAG-е проверял бы пустоту. Для импорт-теста
`MagicMock` годится, для `dag.test()` — нет.

⚠️ **Проблема 2 — `AIRFLOW_HOME` нельзя переставить из теста.** Airflow читает настройки и
создаёт SQLAlchemy-сессию **при импорте**, а его импортируют уже на стадии сбора тестов
(`tests/operators/test_builder_reports.py:10`). Подмена `AIRFLOW_HOME` через `tmp_path`
внутри теста не переконфигурирует уже загруженный Airflow, и `airflow db migrate` из CLI
мигрирует не ту БД, к которой потом обратится `dag.test()`.

- [ ] заменить `MagicMock`-заглушки transfer-операторов на **лёгкие реальные подклассы `BaseOperator`** с теми же `template_fields`, у которых `execute()` только пишет вызов в список; они должны поддерживать `.partial()`/`.expand_kwargs()` как обычные операторы
- [ ] решить проблему изоляции БД одним из двух способов и зафиксировать выбор в conftest: (а) прогонять сценарий в **подпроцессе**, где `AIRFLOW_HOME` выставлен до любого импорта Airflow; либо (б) session-scoped фикстура, задающая тестовую БД **до сбора тестов** (`pytest` `-p` плагин или `conftest.py` на уровне корня), и инициализация БД в том же процессе
- [ ] зарегистрировать маркер `integration` в `pyproject.toml` (`[tool.pytest.ini_options] markers`), чтобы его можно было исключать из быстрого прогона
- [ ] smoke-тест харнесса: тривиальный DAG с одним mapped-таском проходит `dag.test()` и его `TaskInstance` читаются из БД
- [ ] написать тест: подставные transfer-операторы действительно регистрируются в DAG как `MappedOperator` (защита от регрессии проблемы 1)
- [ ] run tests — must pass before task 9

### Task 9: Интеграционные сценарии на реальном bq_and_s3_dag.py

**Files:**
- Create: `tests/integration/test_empty_day_v1.py`

Тест обязан гонять **реальный** пример DAG, а не синтетический стенд: синтетика доказала бы
только, что паттерн Airflow работает, и пропустила бы забытый `items or []` в конкретном
агрегаторе или неверное ребро зависимости в отгружаемом коде.

- [ ] загрузить реальный `examples/bq_and_s3_dag.py` через харнесс задачи 8, замокав `CianBuilderReportsOperator.execute`: `None` за выбранные даты, `{"date", "path"}` за остальные
- [ ] тест «смешанный период»: `collect` — все `success`, `upload_gcs`/`upload_s3`/`load_bq` разворачиваются ровно в N инстансов по числу непустых дней, ни одного `skipped`
- [ ] тест «полностью пустой период»: агрегаторы получают `None` (не `[]`), возвращают `[]`, mapped-таски разворачиваются в ноль инстансов и помечаются `skipped`, `dag_run.state == "success"`
- [ ] тест «даты не смещены»: `dest_key` каждого mapped-инстанса соответствует своей дате
- [ ] run tests — must pass before task 10

### Task 10: Интеграционный тест изоляции кабинетов (multi-account)

**Files:**
- Create: `tests/integration/test_empty_day_multi_account.py`

Отдельная задача, потому что она проверяет **не** ту же семантику, что задача 9, а другое
свойство: независимость кабинетов. Без этого прогон multi-account дублировал бы v1.

- [ ] загрузить реальный `examples/bq_and_s3_multi_account_dag.py` с двумя кабинетами
- [ ] сценарий: у кабинета A нет данных ни за один день, у кабинета B данные есть
- [ ] проверить, что заливки кабинета B прошли, а `skipped` кабинета A их не подавил
- [ ] проверить префиксы task_id внутри `TaskGroup` (`cabinet_<id>.upload_s3`)
- [ ] run tests — must pass before task 11

### Task 11: Verify acceptance criteria

- [ ] verify all requirements from Overview are implemented
- [ ] edge case: пустой день — файла нет и сам этот день не создаёт директорию запуска (существующая директория, созданная соседними непустыми датами, — это норма), XCom не записан
- [ ] edge case: полностью пустой период в v1/multi-account — mapped-таски `skipped`, `dag_run.state = success`
- [ ] edge case: полностью пустой период в v2 — `process_date` в `success` с `None`, `skipped` нет вовсе
- [ ] edge case: ретрай дня, за который данные исчезли — устаревший файл удалён
- [ ] edge case: ответ 200 без `result.reports` (включая тело-список и `null`) → `AirflowException`
- [ ] edge case: смешанный период — заливка идёт только за дни с данными, даты не смещены
- [ ] `grep -rn "zip(paths" examples/` пуст — смещение дат невозможно по конструкции
- [ ] `grep -rn "resolve_token" airflow_provider_cian/operators/` пуст — инвариант ADR-0002 не нарушен
- [ ] verify test coverage meets project standard (новые ветки покрыты)

### Task 12: [Final] ADR-0003 и завершение

**Files:**
- Create: `docs/adr/0003-empty-day-signalling.md`

- [ ] написать ADR-0003 «Пустой день сигнализируется отсутствием XCom, а не AirflowSkipException»: контекст, оба варианта, результат эксперимента на 2.9.1 (skip гасит `make_*_params` через `all_success` и выгрузка не идёт ни за одну дату), выбранное решение и его следствия (`items or []`, самоописывающий dict)
- [ ] update README.md / CONTEXT.md, если по ходу реализации что-то разошлось с задачей 7
- [ ] **единственный final test gate**: `pytest tests/ -v` целиком зелёный, включая интеграционные тесты задач 8-10 (промежуточных полных прогонов в задачах 9 и 11 сознательно нет — в каждой задаче гоняется только её набор)
- [ ] move this plan to `docs/plans/completed/`

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
