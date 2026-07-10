from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_cian.accounts import Account
from airflow_provider_cian.operators.builder_reports import CianBuilderReportsOperator, _CSV_FIELDS, _SNAPSHOT_FIELD


def _make_operator(
    tmp_dir: str,
    output_format: str = "json",
    add_snapshot_ts: bool = False,
) -> CianBuilderReportsOperator:
    return CianBuilderReportsOperator(
        task_id="test_collect",
        cian_conn_id="cian_test",
        date="2024-01-15",
        base_dir=tmp_dir,
        output_format=output_format,
        add_snapshot_ts=add_snapshot_ts,
    )


def _make_context(run_id: str = "scheduled__2024-01-15T00:00:00+00:00") -> dict:
    dag_run = MagicMock()
    dag_run.start_date = datetime(2024, 1, 15, 12, 0, 0)
    return {"run_id": run_id, "dag_run": dag_run}


def _sample_records() -> list[dict]:
    return [
        {
            "id": 1,
            "newbuildingId": 10,
            "date": "2024-01-15T10:43:22",
            "actionType": "call",
            "searcherPhone": "+79001112233",
            "searcherCtPhone": "+74951112233",
            "builderUserCtPhone": "+79001112234",
            "builderUserPhone": "+79001112235",
            "builderSipUri": "sip:100@example.com",
            "callDuration": 120,
            "tariffPrice": 50,
            "auctionBet": 10,
            "cashbackSpent": 0,
            "billingPrice": 50,
            "hasClaim": False,
        },
        {
            "id": 2,
            "newbuildingId": 10,
            "date": "2024-01-15T22:00:00",
            "actionType": "call",
            "searcherPhone": "+79009998877",
            "searcherCtPhone": None,
            "builderUserCtPhone": None,
            "builderUserPhone": "+79001112235",
            "builderSipUri": None,
            "callDuration": 0,
            "tariffPrice": 50,
            "auctionBet": 10,
            "cashbackSpent": 0,
            "billingPrice": 0,
            "hasClaim": False,
        },
    ]


def _make_hook_mock(records: list[dict], name_map: dict[int, str] | None = None) -> MagicMock:
    hook = MagicMock()
    hook.get_builder_reports.return_value = records
    if name_map:
        hook.get_newbuilding_name.side_effect = lambda nid: name_map[nid]
    else:
        hook.get_newbuilding_name.return_value = "ЖК Тест"
    return hook


class TestBuildPath:
    def test_sanitizes_run_id(self, tmp_path):
        op = _make_operator(str(tmp_path))
        path = op._build_path("scheduled__2024-01-15T00:00:00+00:00")
        assert "+" not in path

    def test_json_extension(self, tmp_path):
        op = _make_operator(str(tmp_path), "json")
        path = op._build_path("run-1")
        assert path.endswith(".json")

    def test_csv_extension(self, tmp_path):
        op = _make_operator(str(tmp_path), "csv")
        path = op._build_path("run-1")
        assert path.endswith(".csv")

    def test_different_run_ids_give_different_dirs(self, tmp_path):
        op = _make_operator(str(tmp_path))
        path1 = op._build_path("run-1")
        path2 = op._build_path("run-2")
        assert os.path.dirname(path1) != os.path.dirname(path2)


class TestEnrich:
    def test_is_targeted_true_when_billing_positive(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 50, "date": "2024-01-15T10:00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert enriched[0]["is_targeted"] is True

    def test_is_targeted_false_when_billing_zero(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T10:00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert enriched[0]["is_targeted"] is False

    def test_cache_minimises_api_calls(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [
            {"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T10:00:00"},
            {"id": 2, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T11:00:00"},
            {"id": 3, "newbuildingId": 20, "billingPrice": 0, "date": "2024-01-15T12:00:00"},
        ]
        hook = _make_hook_mock([], {10: "ЖК А", 20: "ЖК Б"})
        op._enrich(records, hook)
        assert hook.get_newbuilding_name.call_count == 2

    def test_newbuilding_name_populated(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T10:00:00"}]
        hook = _make_hook_mock([], {10: "ЖК Речной"})
        enriched = op._enrich(records, hook)
        assert enriched[0]["newbuilding_name"] == "ЖК Речной"

    def test_datetime_without_timezone_gets_msk_offset(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2026-06-03T10:43:22"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert enriched[0]["datetime"] == "2026-06-03T10:43:22+03:00"
        assert enriched[0]["date"] == "2024-01-15"

    def test_datetime_with_existing_msk_offset_unchanged(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2026-06-03T10:43:22+03:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert enriched[0]["datetime"] == "2026-06-03T10:43:22+03:00"
        assert enriched[0]["date"] == "2024-01-15"

    def test_datetime_with_non_msk_offset_converted_to_msk(self, tmp_path):
        # UTC +00:00 → converted to MSK +03:00; date field is always the operator date
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2026-06-03T00:30:00+00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert enriched[0]["datetime"] == "2026-06-03T03:30:00+03:00"
        assert enriched[0]["date"] == "2024-01-15"

    def test_datetime_midnight_boundary_utc_vs_msk(self, tmp_path):
        # 2026-06-02T23:30:00+00:00 = 2026-06-03T02:30:00+03:00
        # date must be operator date "2024-01-15", not the MSK date from the timestamp
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2026-06-02T23:30:00+00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert enriched[0]["datetime"] == "2026-06-03T02:30:00+03:00"
        assert enriched[0]["date"] == "2024-01-15"

    def test_date_field_is_operator_date_not_msk_date(self, tmp_path):
        # Late UTC timestamp rolls over to next MSK day; date field must stay as operator date
        op = _make_operator(str(tmp_path))  # date="2024-01-15"
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T23:30:00+00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        # MSK datetime would be 2024-01-16T02:30:00+03:00, but date must remain "2024-01-15"
        assert enriched[0]["datetime"] == "2024-01-16T02:30:00+03:00"
        assert enriched[0]["date"] == "2024-01-15"

    def test_missing_date_field_raises_airflow_exception(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 7, "newbuildingId": 10, "billingPrice": 0}]  # no "date" key
        hook = _make_hook_mock([])
        with pytest.raises(AirflowException, match="id=7"):
            op._enrich(records, hook)

    def test_enriched_record_has_exactly_csv_fields(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T10:00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook)
        assert set(enriched[0].keys()) == set(_CSV_FIELDS)

    def test_enrich_adds_snapshot_ts_when_provided(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T10:00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook, snapshot_ts="2024-01-15T12:00:00")
        assert enriched[0][_SNAPSHOT_FIELD] == "2024-01-15T12:00:00"

    def test_enrich_skips_snapshot_ts_when_none(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0, "date": "2024-01-15T10:00:00"}]
        hook = _make_hook_mock([])
        enriched = op._enrich(records, hook, snapshot_ts=None)
        assert _SNAPSHOT_FIELD not in enriched[0]


class TestWrite:
    def test_json_creates_jsonl_file(self, tmp_path):
        op = _make_operator(str(tmp_path), "json")
        path = str(tmp_path / "out.json")
        records = [{"id": 1, "value": "тест"}]
        op._write(records, path)

        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["id"] == 1

    def test_csv_creates_file_with_header(self, tmp_path):
        op = _make_operator(str(tmp_path), "csv")
        path = str(tmp_path / "out.csv")
        records = [{f: None for f in _CSV_FIELDS}]
        records[0]["searcher_phone"] = "+79001112233"
        op._write(records, path)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        assert "searcher_phone" in rows[0]
        assert "datetime" in rows[0]
        assert len(rows[0]) == 18

    def test_csv_phones_quoted(self, tmp_path):
        op = _make_operator(str(tmp_path), "csv")
        path = str(tmp_path / "out.csv")
        records = [{f: None for f in _CSV_FIELDS}]
        records[0]["searcher_phone"] = "+79001112233"
        op._write(records, path)

        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert '"+79001112233"' in content

    def test_empty_records_json_creates_empty_file(self, tmp_path):
        op = _make_operator(str(tmp_path), "json")
        path = str(tmp_path / "out.json")
        op._write([], path)
        assert os.path.getsize(path) == 0

    def test_empty_records_csv_creates_header_only(self, tmp_path):
        op = _make_operator(str(tmp_path), "csv")
        path = str(tmp_path / "out.csv")
        op._write([], path)
        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert "id" in content
        lines = content.strip().split("\n")
        assert len(lines) == 1


def _make_conn_mock(login=None) -> MagicMock:
    conn = MagicMock(spec=Connection)
    conn.login = login
    return conn


class TestExecute:
    def _run_operator(self, tmp_path, output_format="json", run_id="run-1", records=None):
        if records is None:
            records = _sample_records()
        op = _make_operator(str(tmp_path), output_format)
        hook = _make_hook_mock(records, {10: "ЖК Тест"})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context(run_id))
        # execute() now returns {"date", "path"} | None; unwrap so string-based
        # asserts below keep working.
        path = result["path"] if result else result
        return path, hook

    def test_json_run_creates_file(self, tmp_path):
        path, _ = self._run_operator(tmp_path)
        assert os.path.exists(path)
        assert path.endswith(".json")

    def test_csv_run_creates_file(self, tmp_path):
        path, _ = self._run_operator(tmp_path, "csv")
        assert os.path.exists(path)
        assert path.endswith(".csv")

    def test_returns_output_path(self, tmp_path):
        path, _ = self._run_operator(tmp_path)
        assert isinstance(path, str)
        assert "2024-01-15" in path

    def test_returns_dict_contract(self, tmp_path):
        """A day with data returns exactly {"date", "path"} describing a real file.

        Asserts a path *property* (exists + extension), not membership: ``"x" in
        result`` would silently degrade to a dict-key check and pass vacuously.
        """
        op = _make_operator(str(tmp_path))
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context("run-1"))
        assert isinstance(result, dict)
        assert set(result.keys()) == {"date", "path"}
        assert result["date"] == op.date
        assert os.path.exists(result["path"])
        assert result["path"].endswith(".json")

    def test_idempotent_retry_deletes_old_file(self, tmp_path):
        op = _make_operator(str(tmp_path))
        ctx = _make_context("run-retry")
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            path1 = op.execute(ctx)["path"]

        with open(path1, "w") as f:
            f.write("old content")

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            path2 = op.execute(ctx)["path"]

        assert path1 == path2
        content = open(path2).read()
        assert "old content" not in content

    def test_different_run_ids_separate_dirs(self, tmp_path):
        path1, _ = self._run_operator(tmp_path, run_id="run-aaa")
        path2, _ = self._run_operator(tmp_path, run_id="run-bbb")
        assert os.path.dirname(path1) != os.path.dirname(path2)

    def test_json_enriched_content(self, tmp_path):
        path, _ = self._run_operator(tmp_path)
        with open(path, encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]
        assert len(records) == 2
        first = records[0]
        assert first["datetime"] == "2024-01-15T10:43:22+03:00"
        assert first["date"] == "2024-01-15"
        assert first["newbuilding_name"] == "ЖК Тест"
        assert first["is_targeted"] is True

    def test_custom_base_dir(self, tmp_path):
        custom_dir = str(tmp_path / "custom")
        op = CianBuilderReportsOperator(
            task_id="t",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=custom_dir,
            output_format="json",
        )
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            path = op.execute(_make_context("run-1"))["path"]
        assert path.startswith(custom_dir)


class TestSnapshotTs:
    """Tests for the add_snapshot_ts parameter."""

    def _run_with_snapshot(self, tmp_path, output_format="json", records=None):
        if records is None:
            records = _sample_records()
        op = _make_operator(str(tmp_path), output_format=output_format, add_snapshot_ts=True)
        hook = _make_hook_mock(records, {10: "ЖК Тест"})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context("run-snap"))
        # execute() now returns {"date", "path"} | None; unwrap the path.
        return result["path"] if result else result

    def test_snapshot_ts_json_records(self, tmp_path):
        """snapshot_ts is present in every record, has the correct value, and key set is exactly 19."""
        path = self._run_with_snapshot(tmp_path)
        with open(path, encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]
        assert len(records) == 2
        # _make_context sets start_date = datetime(2024, 1, 15, 12, 0, 0)
        expected_ts = "2024-01-15T12:00:00"
        for record in records:
            assert _SNAPSHOT_FIELD in record
            assert record[_SNAPSHOT_FIELD] == expected_ts
            assert set(record.keys()) == set(_CSV_FIELDS) | {_SNAPSHOT_FIELD}

    def test_snapshot_ts_absent_by_default(self, tmp_path):
        op = _make_operator(str(tmp_path))  # add_snapshot_ts=False by default
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            path = op.execute(_make_context("run-nosnap"))["path"]
        with open(path, encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]
        assert _SNAPSHOT_FIELD not in records[0]
        assert set(records[0].keys()) == set(_CSV_FIELDS)

    def test_snapshot_ts_not_in_csv_even_when_flag_on(self, tmp_path):
        path = self._run_with_snapshot(tmp_path, output_format="csv")
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = list(reader)
        assert fieldnames == _CSV_FIELDS
        assert len(rows[0]) == 18

    def test_empty_records_with_snapshot_ts_returns_none_and_no_file(self, tmp_path):
        op = _make_operator(str(tmp_path), add_snapshot_ts=True)
        hook = _make_hook_mock([], {})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context("run-snap"))
        assert result is None
        expected_path = op._build_path("run-snap")
        assert not os.path.exists(expected_path)


class TestValidation:
    def test_invalid_format_raises_value_error(self):
        with pytest.raises(ValueError, match="output_format"):
            CianBuilderReportsOperator(
                task_id="t",
                cian_conn_id="cian_test",
                date="2024-01-15",
                output_format="parquet",
            )


class TestBuildPathWithCabinetId:
    def test_existing_build_path_calls_without_cabinet_id_still_work(self, tmp_path):
        """Backward compat: op._build_path("run-1") works without cabinet_id."""
        op = _make_operator(str(tmp_path))
        path = op._build_path("run-1")
        assert path.endswith(".json")
        assert "run-1" in path

    def test_with_cabinet_id_includes_cabinet_in_path(self, tmp_path):
        op = _make_operator(str(tmp_path))
        path = op._build_path("run-1", "abc")
        assert "abc" in path
        assert path.endswith(".json")

    def test_build_path_no_sanitization_trusted_cabinet_id(self, tmp_path):
        """_build_path trusts cabinet_id — already sanitized value passes through unchanged."""
        op = _make_operator(str(tmp_path))
        path = op._build_path("run-1", "a_b")
        assert "a_b" in path
        # No extra underscores introduced
        parts = path.split(os.sep)
        assert "a_b" in parts


class TestExecuteWithAccount:
    def _run_with_account_id(self, tmp_path, account_id, records=None):
        if records is None:
            records = _sample_records()
        op = CianBuilderReportsOperator(
            task_id="test_collect",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=str(tmp_path),
            output_format="json",
            account_id=account_id,
        )
        hook = _make_hook_mock(records, {10: "ЖК Тест"})

        with patch("airflow_provider_cian.operators.builder_reports.CianHook") as MockHook:
            MockHook.return_value = hook
            result = op.execute(_make_context("run-1"))

        # execute() now returns {"date", "path"} | None; unwrap the path.
        path = result["path"] if result else result
        return path, MockHook

    def test_execute_with_account_path_contains_cabinet_id(self, tmp_path):
        path, _ = self._run_with_account_id(tmp_path, Account(id="abc").id)
        assert "abc" in path

    def test_execute_with_account_sanitized_id_in_path(self, tmp_path):
        """Account(id='a.b') sanitizes to 'a_b' via __post_init__, path uses sanitized id."""
        path, _ = self._run_with_account_id(tmp_path, Account(id="a.b").id)
        assert "a_b" in path

    def test_execute_with_account_hook_created_with_account_id(self, tmp_path):
        _, MockHook = self._run_with_account_id(tmp_path, "abc")
        MockHook.assert_called_once_with(cian_conn_id="cian_test", account_id="abc")

    def test_execute_with_account_hook_kwargs_only_conn_and_account(self, tmp_path):
        """Operator passes only cian_conn_id and account_id to CianHook — nothing else."""
        _, MockHook = self._run_with_account_id(tmp_path, "abc")
        call_kwargs = MockHook.call_args.kwargs
        assert set(call_kwargs.keys()) == {"cian_conn_id", "account_id"}

    def test_execute_without_account_with_conn_login_path_has_login(self, tmp_path):
        op = CianBuilderReportsOperator(
            task_id="test_collect",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=str(tmp_path),
            output_format="json",
        )
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})
        mock_conn = _make_conn_mock(login="msk")

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=mock_conn):
            path = op.execute(_make_context("run-1"))["path"]

        assert "msk" in path

    def test_execute_without_account_without_conn_login_path_has_no_extra_dir(self, tmp_path):
        """Without account and without conn.login, path is {base_dir}/{run_id}/{date}.ext."""
        op = CianBuilderReportsOperator(
            task_id="test_collect",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=str(tmp_path),
            output_format="json",
        )
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})
        mock_conn = _make_conn_mock(login=None)

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=mock_conn):
            path = op.execute(_make_context("run-1"))["path"]

        # Path should be base_dir/run_id/date.json — no extra cabinet dir
        rel = os.path.relpath(path, str(tmp_path))
        parts = rel.split(os.sep)
        # Exactly 2 parts: <run_id_sanitized>/<date>.json
        assert len(parts) == 2, f"Expected 2 path parts, got {parts}"

    def test_execute_multi_mode_does_not_read_connection(self, tmp_path):
        """In multi-mode (account_id set), resolve_cabinet_id must not call get_connection."""
        op = CianBuilderReportsOperator(
            task_id="test_collect",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=str(tmp_path),
            output_format="json",
            account_id="some_account",
        )
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection") as mock_get_conn:
            op.execute(_make_context("run-1"))

        mock_get_conn.assert_not_called()

    def test_account_id_takes_priority_over_conn_login(self, tmp_path):
        """When account_id is set, it is used as cabinet_id even if conn.login is present."""
        op = CianBuilderReportsOperator(
            task_id="test_collect",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=str(tmp_path),
            output_format="json",
            account_id="acct123",
        )
        hook = _make_hook_mock(_sample_records(), {10: "ЖК Тест"})
        mock_conn = _make_conn_mock(login="some_login")

        with patch("airflow_provider_cian.operators.builder_reports.CianHook") as MockHook:
            MockHook.return_value = hook
            path = op.execute(_make_context("run-1"))["path"]

        # Path uses account_id, not conn.login
        assert "acct123" in path
        assert "some_login" not in path
        # Hook is created with account_id
        MockHook.assert_called_once_with(cian_conn_id="cian_test", account_id="acct123")


class TestExecuteEmptyDay:
    """execute() on an empty day: no file, no run directory, returns None."""

    def _run_empty(self, tmp_path, output_format="json", run_id="run-empty"):
        op = _make_operator(str(tmp_path), output_format)
        hook = _make_hook_mock([], {})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context(run_id))
        return result, op

    def test_empty_day_json_returns_none_no_file(self, tmp_path):
        result, op = self._run_empty(tmp_path, "json")
        assert result is None
        assert not os.path.exists(op._build_path("run-empty"))

    def test_empty_day_csv_returns_none_no_file(self, tmp_path):
        result, op = self._run_empty(tmp_path, "csv")
        assert result is None
        assert not os.path.exists(op._build_path("run-empty"))

    def test_empty_day_creates_no_run_directory(self, tmp_path):
        result, op = self._run_empty(tmp_path)
        assert result is None
        run_dir = os.path.dirname(op._build_path("run-empty"))
        assert not os.path.exists(run_dir)

    def test_empty_day_creates_no_run_directory_but_keeps_sibling(self, tmp_path):
        """A run directory created by a neighbouring non-empty date is left intact."""
        op = _make_operator(str(tmp_path))
        run_id = "run-shared"
        run_dir = os.path.dirname(op._build_path(run_id))
        os.makedirs(run_dir, exist_ok=True)  # a sibling date created this earlier
        hook = _make_hook_mock([], {})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context(run_id))
        assert result is None
        assert os.path.exists(run_dir)  # not removed
        assert not os.path.exists(op._build_path(run_id))  # but no file for this date

    def test_stale_file_removed_even_when_no_data(self, tmp_path):
        """A file left by a previous attempt is deleted even if this run has no data."""
        op = _make_operator(str(tmp_path))
        run_id = "run-retry-empty"
        stale_path = op._build_path(run_id)
        os.makedirs(os.path.dirname(stale_path), exist_ok=True)
        with open(stale_path, "w") as f:
            f.write("stale content")
        hook = _make_hook_mock([], {})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook), \
             patch("airflow_provider_cian.accounts.BaseHook.get_connection",
                   return_value=_make_conn_mock(login=None)):
            result = op.execute(_make_context(run_id))
        assert result is None
        assert not os.path.exists(stale_path)
