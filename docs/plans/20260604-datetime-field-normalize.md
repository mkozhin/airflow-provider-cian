# Schema: split date/datetime, normalize Moscow timezone

## Overview

Доработка схемы выходных данных `CianBuilderReportsOperator`:

1. Поле `date` (из API, содержит datetime без timezone) → переименовать в `datetime`, добавить явный офсет `+03:00` (Москва)
2. Добавить новое поле `date` перед `datetime` — только дата `"YYYY-MM-DD"`, извлечённая в московском времени
3. Схема: 17 → 18 полей

**Проблема:** API Циан возвращает datetime без указания timezone (`"2026-06-03T10:43:22"`), хотя в документации указано что время московское. Само значение времени верное — его менять не нужно. Без явного offset внешние системы (BigQuery, ClickHouse) могут неправильно интерпретировать поле: считать его UTC и отображать неверно.

**Решение:** при обогащении просто добавить суффикс `+03:00` к значению через `replace(tzinfo=MSK)` — время не сдвигается, только помечается как московское. Отдельно извлекать дату (`date`) для партиционирования.

## Context (from discovery)

- `airflow_provider_cian/operators/builder_reports.py` — `_enrich()` и `_CSV_FIELDS` нужно изменить
- `tests/operators/test_builder_reports.py` — обновить тестовые данные и проверки
- `README.md`, `README_ru.md` — обновить схему (17 → 18 полей)
- `CHANGELOG.md` — добавить запись для следующей версии

## Development Approach

- **testing approach**: Regular (сначала код, потом тесты)
- Один таск — изменения атомарные и тесно связанные
- Тесты обязательны

## Testing Strategy

- **unit tests**: pytest, `tests/operators/test_builder_reports.py`
- Покрыть: datetime с timezone → правильный offset; datetime без timezone → добавляется +03:00; дата извлекается корректно по московскому времени (включая граничный случай около полуночи)

## Solution Overview

В `_enrich()` добавить нормализацию datetime:

```python
from datetime import datetime, timezone, timedelta

MSK = timezone(timedelta(hours=3))

raw_dt = record.get("date")  # "2026-06-03T10:43:22" или "2026-06-03T10:43:22+03:00"
if raw_dt:
    dt = datetime.fromisoformat(raw_dt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    dt_str = dt.isoformat()          # "2026-06-03T10:43:22+03:00"
    date_str = dt.date().isoformat() # "2026-06-03"
else:
    dt_str = None
    date_str = None
```

Новая схема (18 полей):
`id, newbuilding_id, newbuilding_name, **date**, **datetime**, action_type, searcher_phone, searcher_ct_phone, builder_user_ct_phone, builder_user_phone, builder_sip_uri, call_duration, tariff_price, auction_bet, cashback_spent, billing_price, has_claim, is_targeted`

## Implementation Steps

### Task 1: Обновить оператор и тесты

**Files:**
- Modify: `airflow_provider_cian/operators/builder_reports.py`
- Modify: `tests/operators/test_builder_reports.py`

- [x] добавить импорт `timezone`, `timedelta` из `datetime` в `builder_reports.py`
- [x] добавить константу `_MSK = timezone(timedelta(hours=3))` на уровне модуля
- [x] в `_CSV_FIELDS`: заменить `"date"` на `"date"` + `"datetime"` (два поля рядом)
- [x] в `_enrich()`: реализовать нормализацию — парсинг, добавление `+03:00` если нет tzinfo, вычисление `date`
- [x] в `_enrich()`: переименовать ключ `"date"` → `"datetime"`, добавить ключ `"date"` перед ним
- [x] обновить тестовые данные `_sample_records()` — поле `"date"` в API-ответе задать без timezone
- [x] добавить тест: datetime без timezone → получает `+03:00`
- [x] добавить тест: datetime с уже имеющимся `+03:00` → остаётся без изменений
- [x] добавить тест: граничный случай `"2026-06-03T00:30:00"` → `date = "2026-06-03"` (не `"2026-06-02"`)
- [x] обновить существующие тесты где фигурирует поле `date` в enriched-записях
- [x] запустить тесты — все должны пройти

### Task 2: Обновить документацию

**Files:**
- Modify: `README.md`
- Modify: `README_ru.md`
- Modify: `CHANGELOG.md`

- [ ] обновить раздел "Output Schema" в `README.md`: 17 → 18 полей, добавить `datetime`, описание
- [ ] то же самое в `README_ru.md`
- [ ] добавить запись в `CHANGELOG.md` для версии `0.1.2`
- [ ] переместить план: `docs/plans/completed/`

### Task 3: Финальная проверка

- [ ] запустить полный тест-сьют: `pytest tests/ -v`
- [ ] убедиться что `_CSV_FIELDS` и `_enrich()` возвращают одинаковые 18 полей
- [ ] убедиться что JSON и CSV форматы содержат оба поля в правильном порядке

## Post-Completion

**Внешние системы:**
- При загрузке в BigQuery: поле `date` использовать как партиционный столбец (`DATE` тип), `datetime` как `TIMESTAMP` или `STRING` с явным offset
- При загрузке в ClickHouse: `date` → `Date`, `datetime` → `DateTime('Europe/Moscow')` или `String`
