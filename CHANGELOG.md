# Changelog

## [0.1.1] - 2026-06-04

- Fix: rename package to `airflow-provider-cian` (was `apache-airflow-provider-cian`)
- Fix: correct `get-newbuilding` response field `displayName` → `name`
- Fix: add missing `searcher_ct_phone` field to output schema (17 fields total)
- Docs: add Russian README (`README_ru.md`)

## [0.1.0] - 2026-06-04

- Initial release
- `CianHook`: авторизация, rate limiting (≤10 req/s), retry при 429/5xx
- `CianBuilderReportsOperator`: сбор звонков и чатов за день, вывод в JSON/CSV
