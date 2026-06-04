from __future__ import annotations

import csv
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from airflow_provider_cian.operators.builder_reports import CianBuilderReportsOperator


def _make_operator(tmp_dir: str, output_format: str = "json") -> CianBuilderReportsOperator:
    return CianBuilderReportsOperator(
        task_id="test_collect",
        cian_conn_id="cian_test",
        date="2024-01-15",
        base_dir=tmp_dir,
        output_format=output_format,
    )


def _make_context(run_id: str = "scheduled__2024-01-15T00:00:00+00:00") -> dict:
    return {"run_id": run_id}


def _sample_records() -> list[dict]:
    return [
        {
            "id": 1,
            "newbuildingId": 10,
            "date": "2024-01-15",
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
            "date": "2024-01-15",
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
        assert "scheduled__2024-01-15T00_00_00_00_00_" in path or "+" not in path

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
        hook = _make_hook_mock([{"id": 1, "newbuildingId": 10, "billingPrice": 50}])
        enriched = op._enrich([{"id": 1, "newbuildingId": 10, "billingPrice": 50}], hook)
        assert enriched[0]["is_targeted"] is True

    def test_is_targeted_false_when_billing_zero(self, tmp_path):
        op = _make_operator(str(tmp_path))
        hook = _make_hook_mock([{"id": 1, "newbuildingId": 10, "billingPrice": 0}])
        enriched = op._enrich([{"id": 1, "newbuildingId": 10, "billingPrice": 0}], hook)
        assert enriched[0]["is_targeted"] is False

    def test_cache_minimises_api_calls(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [
            {"id": 1, "newbuildingId": 10, "billingPrice": 0},
            {"id": 2, "newbuildingId": 10, "billingPrice": 0},
            {"id": 3, "newbuildingId": 20, "billingPrice": 0},
        ]
        hook = _make_hook_mock(records, {10: "ЖК А", 20: "ЖК Б"})
        op._enrich(records, hook)
        assert hook.get_newbuilding_name.call_count == 2

    def test_newbuilding_name_populated(self, tmp_path):
        op = _make_operator(str(tmp_path))
        records = [{"id": 1, "newbuildingId": 10, "billingPrice": 0}]
        hook = _make_hook_mock(records, {10: "ЖК Речной"})
        enriched = op._enrich(records, hook)
        assert enriched[0]["newbuilding_name"] == "ЖК Речной"


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
        records = [{f: None for f in ["id", "newbuilding_id", "newbuilding_name", "date",
                                       "action_type", "searcher_phone", "searcher_ct_phone",
                                       "builder_user_ct_phone", "builder_user_phone",
                                       "builder_sip_uri", "call_duration", "tariff_price",
                                       "auction_bet", "cashback_spent", "billing_price",
                                       "has_claim", "is_targeted"]}]
        records[0]["searcher_phone"] = "+79001112233"
        op._write(records, path)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        assert "searcher_phone" in rows[0]

    def test_csv_phones_quoted(self, tmp_path):
        op = _make_operator(str(tmp_path), "csv")
        path = str(tmp_path / "out.csv")
        records = [{f: None for f in ["id", "newbuilding_id", "newbuilding_name", "date",
                                       "action_type", "searcher_phone", "searcher_ct_phone",
                                       "builder_user_ct_phone", "builder_user_phone",
                                       "builder_sip_uri", "call_duration", "tariff_price",
                                       "auction_bet", "cashback_spent", "billing_price",
                                       "has_claim", "is_targeted"]}]
        records[0]["searcher_phone"] = "+79001112233"
        op._write(records, path)

        content = open(path, encoding="utf-8").read()
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


class TestExecute:
    def _run_operator(self, tmp_path, output_format="json", run_id="run-1", records=None):
        if records is None:
            records = _sample_records()
        op = _make_operator(str(tmp_path), output_format)
        hook = _make_hook_mock(records, {10: "ЖК Тест"})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook):
            path = op.execute(_make_context(run_id))
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

    def test_idempotent_retry_deletes_old_file(self, tmp_path):
        op = _make_operator(str(tmp_path))
        ctx = _make_context("run-retry")
        hook = _make_hook_mock([], {})

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook):
            path1 = op.execute(ctx)

        with open(path1, "w") as f:
            f.write("old content")

        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook):
            path2 = op.execute(ctx)

        assert path1 == path2
        content = open(path2).read()
        assert "old content" not in content

    def test_different_run_ids_separate_dirs(self, tmp_path):
        path1, _ = self._run_operator(tmp_path, run_id="run-aaa")
        path2, _ = self._run_operator(tmp_path, run_id="run-bbb")
        assert os.path.dirname(path1) != os.path.dirname(path2)

    def test_custom_base_dir(self, tmp_path):
        custom_dir = str(tmp_path / "custom")
        op = CianBuilderReportsOperator(
            task_id="t",
            cian_conn_id="cian_test",
            date="2024-01-15",
            base_dir=custom_dir,
            output_format="json",
        )
        hook = _make_hook_mock([], {})
        with patch("airflow_provider_cian.operators.builder_reports.CianHook", return_value=hook):
            path = op.execute(_make_context("run-1"))
        assert path.startswith(custom_dir)


class TestValidation:
    def test_invalid_format_raises_value_error(self):
        with pytest.raises(ValueError, match="output_format"):
            CianBuilderReportsOperator(
                task_id="t",
                cian_conn_id="cian_test",
                date="2024-01-15",
                output_format="parquet",
            )
