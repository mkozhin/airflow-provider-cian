"""Tests for the multi-account example DAG."""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock, patch

from airflow_provider_cian.accounts import Account

_MOD_NAME = "examples.bq_and_s3_multi_account_dag"
# The DAG module calls list_accounts at import time.
# Patching the source (airflow_provider_cian.accounts.list_accounts) works for fresh imports
# because 'from X import f' binds f from X at import time, so patching X.f before importlib.import_module
# causes the module to bind the mock. Patching the DAG module's own namespace would only work
# after the module is already loaded.
_PATCH_TARGET = "airflow_provider_cian.accounts.list_accounts"

# Provider sub-packages that are not installed in the test environment.
# NOTE: do NOT include "airflow.providers" — it is a real namespace package.
_MISSING_PROVIDERS = [
    "airflow.providers.amazon",
    "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.transfers",
    "airflow.providers.amazon.aws.transfers.local_to_s3",
    "airflow.providers.google",
    "airflow.providers.google.cloud",
    "airflow.providers.google.cloud.hooks",
    "airflow.providers.google.cloud.hooks.gcs",
    "airflow.providers.google.cloud.transfers",
    "airflow.providers.google.cloud.transfers.gcs_to_bigquery",
    "airflow.providers.google.cloud.transfers.local_to_gcs",
    "google",
    "google.api_core",
    "google.api_core.exceptions",
]


def _make_provider_stubs() -> dict[str, types.ModuleType]:
    """Build a dict of stub modules to inject into sys.modules."""
    stubs: dict[str, types.ModuleType] = {}

    for name in _MISSING_PROVIDERS:
        if name in sys.modules:
            continue
        mod = types.ModuleType(name)
        stubs[name] = mod

    def _get(name: str) -> types.ModuleType:
        return stubs.get(name, sys.modules.get(name, types.ModuleType(name)))

    gcs_mod = _get("airflow.providers.google.cloud.hooks.gcs")
    gcs_mod.GCSHook = MagicMock(name="GCSHook")

    gcs_to_bq_mod = _get("airflow.providers.google.cloud.transfers.gcs_to_bigquery")
    GCSToBQ = MagicMock(name="GCSToBigQueryOperator")
    GCSToBQ.partial = MagicMock(return_value=MagicMock())
    gcs_to_bq_mod.GCSToBigQueryOperator = GCSToBQ

    local_to_gcs_mod = _get("airflow.providers.google.cloud.transfers.local_to_gcs")
    LocalToGCS = MagicMock(name="LocalFilesystemToGCSOperator")
    LocalToGCS.partial = MagicMock(return_value=MagicMock())
    local_to_gcs_mod.LocalFilesystemToGCSOperator = LocalToGCS

    s3_mod = _get("airflow.providers.amazon.aws.transfers.local_to_s3")
    LocalToS3 = MagicMock(name="LocalFilesystemToS3Operator")
    LocalToS3.partial = MagicMock(return_value=MagicMock())
    s3_mod.LocalFilesystemToS3Operator = LocalToS3

    google_exceptions = _get("google.api_core.exceptions")
    google_exceptions.Conflict = type("Conflict", (Exception,), {})

    return stubs


def _import_dag_module(mock_accounts: list[Account]):
    """Import (or re-import) the multi-account DAG module with mocked providers
    and a mocked list_accounts function.

    Removes the cached module so that module-level code re-runs each time.
    """
    sys.modules.pop(_MOD_NAME, None)

    stubs = _make_provider_stubs()
    previously_absent = [k for k in stubs if k not in sys.modules]
    sys.modules.update(stubs)

    try:
        with patch(_PATCH_TARGET, return_value=mock_accounts):
            mod = importlib.import_module(_MOD_NAME)
    finally:
        for k in previously_absent:
            sys.modules.pop(k, None)

    return mod


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
    """Return the actual DAG object from a @dag-decorated callable.

    The @dag decorator in Airflow 2 wraps the function but also registers
    the DAG when the module is loaded (via the trailing call in the file).
    The decorated function exposes the underlying DAG via .dag attribute or
    can be retrieved from the DagBag/global registry.
    """
    decorated = mod.cian_to_bq_and_s3_multi_account
    # In Airflow 2.x the @dag decorator returns a DAGFactory / WrappedDAG;
    # the resolved DAG is accessible as .dag after the function is called.
    if hasattr(decorated, "dag"):
        return decorated.dag
    # Fallback: call the function to get the DAG (idempotent in Airflow 2.x).
    return decorated()


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
