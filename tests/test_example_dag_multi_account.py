"""Tests for the multi-account example DAG."""

from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

from airflow_provider_cian.accounts import Account
from tests.example_stubs import _make_provider_stubs, get_dag_obj, import_dag_module

_MOD_NAME = "examples.bq_and_s3_multi_account_dag"


def _import_dag_module(mock_accounts: list[Account]):
    """Import (or re-import) the multi-account DAG module with mocked providers
    and a mocked list_accounts function.

    The DAG module calls list_accounts at import time; ``import_dag_module``
    patches ``airflow_provider_cian.accounts.list_accounts`` for the duration of
    the import (see its ``list_accounts=`` hook).
    """
    return import_dag_module(_MOD_NAME, list_accounts=mock_accounts)


class TestMultiAccountDagImport:
    """Smoke tests: DAG imports without errors."""

    def test_dag_imports_with_empty_accounts(self):
        """DAG imports successfully when list_accounts returns []."""
        mod = _import_dag_module([])
        assert hasattr(mod, "cian_to_bq_and_s3_multi_account")

    def test_dag_imports_with_two_accounts(self):
        """DAG imports successfully when list_accounts returns two accounts."""
        accounts = [Account(id="aa"), Account(id="bb")]
        mod = _import_dag_module(accounts)
        assert hasattr(mod, "cian_to_bq_and_s3_multi_account")

    def test_dag_imports_does_not_raise_on_missing_connection(self):
        """DAG import must not raise when the connection is missing.

        list_accounts is defensive: it catches AirflowNotFoundException internally
        and returns []. The DAG therefore imports successfully without any try/except
        at the DAG level.
        """
        from airflow.exceptions import AirflowNotFoundException

        sys.modules.pop(_MOD_NAME, None)
        stubs = _make_provider_stubs()
        previously_absent = [k for k in stubs if k not in sys.modules]
        sys.modules.update(stubs)
        try:
            # Simulate a missing connection at the BaseHook level.
            # list_accounts catches AirflowNotFoundException and returns [].
            with patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=AirflowNotFoundException("no conn"),
            ):
                mod = importlib.import_module(_MOD_NAME)
        finally:
            for k in previously_absent:
                sys.modules.pop(k, None)
            sys.modules.pop(_MOD_NAME, None)

        assert hasattr(mod, "cian_to_bq_and_s3_multi_account")


def _get_dag_obj(mod):
    """Return the actual DAG object from the @dag-decorated callable."""
    return get_dag_obj(mod, "cian_to_bq_and_s3_multi_account")


class TestMultiAccountDagTaskGroups:
    """Verify that TaskGroups are created for each account."""

    def test_no_task_groups_when_accounts_empty(self):
        """When accounts is [], the DAG body has no cabinet_* TaskGroups."""
        mod = _import_dag_module([])
        dag_obj = _get_dag_obj(mod)
        task_group_ids = set(dag_obj.task_group_dict.keys())
        cabinet_groups = [tgid for tgid in task_group_ids if tgid.startswith("cabinet_")]
        assert cabinet_groups == []

    def test_two_task_groups_for_two_accounts(self):
        """When two accounts are present, two TaskGroups are created."""
        accounts = [Account(id="aa"), Account(id="bb")]
        mod = _import_dag_module(accounts)
        dag_obj = _get_dag_obj(mod)

        task_group_ids = set(dag_obj.task_group_dict.keys())
        assert "cabinet_aa" in task_group_ids
        assert "cabinet_bb" in task_group_ids

    def test_task_group_ids_match_sanitized_account_ids(self):
        """TaskGroup ids use sanitized account ids (Account.__post_init__ sanitizes)."""
        # "a.b" → sanitized "a_b", "c/d" → "c_d"
        accounts = [Account(id="a.b"), Account(id="c/d")]
        mod = _import_dag_module(accounts)
        dag_obj = _get_dag_obj(mod)

        task_group_ids = set(dag_obj.task_group_dict.keys())
        assert "cabinet_a_b" in task_group_ids
        assert "cabinet_c_d" in task_group_ids


def _get_cabinet_callable(mod, cabinet_id, task_id):
    """Return the python_callable of a nested @task inside a cabinet TaskGroup.

    Aggregators and cleanup are declared inside the DAG factory, wrapped in
    @task and nested in a TaskGroup, so their task_id carries the group prefix
    (``cabinet_<id>.<task_id>``). They are only reachable through the assembled
    DAG object.
    """
    dag_obj = _get_dag_obj(mod)
    return dag_obj.get_task(f"cabinet_{cabinet_id}.{task_id}").python_callable


class TestMultiAccountAggregatorsEmptyPeriod:
    """Aggregators over a None input (whole period empty for a cabinet).

    When a cabinet's mapped ``collect`` writes no XCom, downstream receives
    ``None`` (not ``[]``). Each aggregator must coerce with ``or []`` and — for
    make_gcs_params / make_bq_params — return before reading context["run_id"],
    otherwise a direct call without context raises KeyError even for None input.
    """

    def test_make_gcs_params_none_returns_empty(self):
        mod = _import_dag_module([Account(id="aa")])
        make_gcs_params = _get_cabinet_callable(mod, "aa", "make_gcs_params")
        assert make_gcs_params(None, cabinet_id="aa") == []

    def test_make_s3_params_none_returns_empty(self):
        mod = _import_dag_module([Account(id="aa")])
        make_s3_params = _get_cabinet_callable(mod, "aa", "make_s3_params")
        assert make_s3_params(None, cabinet_id="aa") == []

    def test_make_bq_params_none_returns_empty(self):
        mod = _import_dag_module([Account(id="aa")])
        make_bq_params = _get_cabinet_callable(mod, "aa", "make_bq_params")
        assert make_bq_params(None, cabinet_id="aa") == []


class TestMultiAccountAggregatorsWithData:
    """Aggregators put cabinet_id into GCS/S3 keys and the BQ table name.

    Each item is self-describing ({"date", "path"}), so date shifting via
    zip(paths, dates) is impossible by construction.
    """

    ITEMS = [
        {"date": "2024-01-01", "path": "/tmp/cian/aa/run1/2024-01-01.json"},
        {"date": "2024-01-02", "path": "/tmp/cian/aa/run1/2024-01-02.json"},
    ]

    def test_make_gcs_params_contains_cabinet_id(self):
        mod = _import_dag_module([Account(id="aa")])
        make_gcs_params = _get_cabinet_callable(mod, "aa", "make_gcs_params")
        params = make_gcs_params(self.ITEMS, cabinet_id="aa", run_id="run1")
        assert len(params) == 2
        assert params[0]["src"] == "/tmp/cian/aa/run1/2024-01-01.json"
        assert "/aa/" in params[0]["dst"]
        # safe_id(run_id) must survive interpolation into the GCS object path.
        assert "/run1/" in params[0]["dst"]
        assert params[0]["dst"].endswith("2024-01-01.json")
        assert params[1]["dst"].endswith("2024-01-02.json")

    def test_make_s3_params_contains_cabinet_id_and_dates_not_shifted(self):
        mod = _import_dag_module([Account(id="aa")])
        make_s3_params = _get_cabinet_callable(mod, "aa", "make_s3_params")
        params = make_s3_params(self.ITEMS, cabinet_id="aa")
        assert len(params) == 2
        assert params[0]["filename"] == "/tmp/cian/aa/run1/2024-01-01.json"
        assert "/aa/" in params[0]["dest_key"]
        assert "_date=20240101" in params[0]["dest_key"]
        assert params[0]["dest_key"].endswith("2024-01-01.json")
        assert "_date=20240102" in params[1]["dest_key"]
        assert params[1]["dest_key"].endswith("2024-01-02.json")

    def test_make_bq_params_contains_cabinet_id_in_table(self):
        mod = _import_dag_module([Account(id="aa")])
        make_bq_params = _get_cabinet_callable(mod, "aa", "make_bq_params")
        params = make_bq_params(self.ITEMS, cabinet_id="aa", run_id="run1")
        assert len(params) == 2
        table0 = params[0]["destination_project_dataset_table"]
        assert "builder_reports_aa$" in table0
        assert table0.endswith("$20240101")
        assert "/aa/" in params[0]["source_objects"][0]
        assert params[0]["source_objects"][0].endswith("2024-01-01.json")
        assert params[1]["destination_project_dataset_table"].endswith("$20240102")


class TestMultiAccountCleanup:
    """cleanup must tolerate an empty mapped result (None / [])."""

    def test_cleanup_none_does_not_raise(self):
        mod = _import_dag_module([Account(id="aa")])
        cleanup = _get_cabinet_callable(mod, "aa", "cleanup")
        assert cleanup(None, cabinet_id="aa", run_id="run1") is None

    def test_cleanup_empty_list_does_not_raise(self):
        mod = _import_dag_module([Account(id="aa")])
        cleanup = _get_cabinet_callable(mod, "aa", "cleanup")
        assert cleanup([], cabinet_id="aa", run_id="run1") is None

    def test_cleanup_removes_run_dir(self, tmp_path):
        """The positive path: shutil.rmtree removes BASE_DIR/cabinet_id/sid."""
        mod = _import_dag_module([Account(id="aa")])
        cleanup = _get_cabinet_callable(mod, "aa", "cleanup")

        run_dir = tmp_path / "aa" / "run1"
        run_dir.mkdir(parents=True)
        (run_dir / "2024-01-01.json").write_text("{}")

        items = [{"date": "2024-01-01", "path": str(run_dir / "2024-01-01.json")}]
        # cleanup derives run_dir from the module-level BASE_DIR, so patch it.
        with patch.object(mod, "BASE_DIR", str(tmp_path)):
            cleanup(items, cabinet_id="aa", run_id="run1")

        assert not run_dir.exists()


class TestMultiAccountDagConstants:
    """Verify module-level constants and configuration."""

    def test_pool_constant_defined(self):
        mod = _import_dag_module([])
        assert hasattr(mod, "POOL")
        assert mod.POOL == "cian_pool"

    def test_max_active_tasks_constant_defined(self):
        mod = _import_dag_module([])
        assert hasattr(mod, "MAX_ACTIVE_TASKS")
        assert isinstance(mod.MAX_ACTIVE_TASKS, int)
        assert mod.MAX_ACTIVE_TASKS > 0

    def test_pool_in_default_args(self):
        mod = _import_dag_module([])
        assert "pool" in mod.DEFAULT_ARGS
        assert mod.DEFAULT_ARGS["pool"] == mod.POOL
