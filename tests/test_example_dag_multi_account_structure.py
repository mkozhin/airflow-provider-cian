"""Structural assertions on the assembled multi-account example DAG.

Unlike the import test (tests/test_example_dag_multi_account.py), this module
builds the DAG with the *real* lightweight transfer-operator subclasses (Task 8)
and TWO cabinets, so ``upload_gcs`` / ``upload_s3`` / ``load_bq`` register as
genuine ``MappedOperator`` instances inside each cabinet's TaskGroup.

The property under test is DIFFERENT from the v1 structure test (Task 9): here we
verify cabinet INDEPENDENCE. Each cabinet gets its own TaskGroup, its edges and
trigger rules never reference the neighbouring cabinet's tasks, and its
aggregators stamp ITS OWN cabinet_id into the GCS/S3 keys and BQ table name. An
empty cabinet therefore cannot structurally suppress a neighbour that has data.
No metadata DB, no dag.test().
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

from airflow.models.mappedoperator import MappedOperator

from airflow_provider_cian.accounts import Account
from tests.example_stubs import _make_provider_stubs

_MOD_NAME = "examples.bq_and_s3_multi_account_dag"
_PATCH_TARGET = "airflow_provider_cian.accounts.list_accounts"

# The two cabinets every test in this module builds the DAG with.
_CABINETS = ("aa", "bb")
_MAPPED_TASK_IDS = ("upload_gcs", "upload_s3", "load_bq")


def _import_dag_module(mock_accounts: list[Account]):
    """Import (or re-import) the multi-account DAG with real transfer operators.

    Combines two things the plain helpers do separately: the real
    ``BaseOperator`` transfer-operator subclasses (so ``expand_kwargs`` yields
    genuine ``MappedOperator`` instances) AND a mocked ``list_accounts`` (so the
    cabinet TaskGroups are actually built at import time).
    """
    sys.modules.pop(_MOD_NAME, None)

    stubs = _make_provider_stubs(real_transfer_operators=True)
    previously_absent = [k for k in stubs if k not in sys.modules]
    sys.modules.update(stubs)

    try:
        with patch(_PATCH_TARGET, return_value=mock_accounts):
            mod = importlib.import_module(_MOD_NAME)
    finally:
        for k in previously_absent:
            sys.modules.pop(k, None)

    return mod


def _get_dag_obj(mod):
    decorated = mod.cian_to_bq_and_s3_multi_account
    if hasattr(decorated, "dag"):
        return decorated.dag
    return decorated()


def _import_two_cabinets():
    return _import_dag_module([Account(id="aa"), Account(id="bb")])


def _get_cabinet_callable(mod, cabinet_id, task_id):
    dag_obj = _get_dag_obj(mod)
    return dag_obj.get_task(f"cabinet_{cabinet_id}.{task_id}").python_callable


class TestCabinetTaskGroups:
    """Each cabinet has its own TaskGroup; mapped tasks live inside it."""

    def test_one_task_group_per_cabinet(self):
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        group_ids = set(dag_obj.task_group_dict.keys())
        for cab in _CABINETS:
            assert f"cabinet_{cab}" in group_ids

    def test_mapped_tasks_registered_inside_each_group(self):
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        for cab in _CABINETS:
            for task_id in _MAPPED_TASK_IDS:
                full_id = f"cabinet_{cab}.{task_id}"
                assert full_id in dag_obj.task_dict, f"{full_id} missing from DAG"
                task = dag_obj.get_task(full_id)
                assert isinstance(
                    task, MappedOperator
                ), f"{full_id} is {type(task).__name__}, not a MappedOperator"

    def test_mapped_tasks_belong_to_their_own_group(self):
        """upload_s3/upload_gcs/load_bq report the cabinet's group as their owner."""
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        for cab in _CABINETS:
            for task_id in _MAPPED_TASK_IDS:
                task = dag_obj.get_task(f"cabinet_{cab}.{task_id}")
                assert task.task_group.group_id == f"cabinet_{cab}"


class TestCabinetIndependence:
    """Edges and trigger rules within one cabinet never touch the other cabinet.

    A shared DAG-level upstream (get_dates / ensure_gcs_bucket) is allowed — it
    carries no ``cabinet_`` prefix — but no edge may cross into the *other*
    cabinet's group. That structural independence is what lets an empty cabinet
    stay out of a neighbour's way.
    """

    def _group_task_ids(self, dag_obj, cab):
        return {
            tid
            for tid in dag_obj.task_dict
            if tid.startswith(f"cabinet_{cab}.")
        }

    def test_no_edge_crosses_between_cabinets(self):
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        for cab, other in (("aa", "bb"), ("bb", "aa")):
            other_prefix = f"cabinet_{other}."
            for tid in self._group_task_ids(dag_obj, cab):
                task = dag_obj.get_task(tid)
                related = task.upstream_task_ids | task.downstream_task_ids
                offending = {r for r in related if r.startswith(other_prefix)}
                assert offending == set(), (
                    f"{tid} references tasks of the other cabinet: {offending}"
                )

    def test_intra_group_wiring_stays_within_prefix(self):
        """collect→aggregator→upload and the load chain stay in the same cabinet."""
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        for cab in _CABINETS:
            p = f"cabinet_{cab}."
            assert p + "make_gcs_params" in dag_obj.get_task(p + "upload_gcs").upstream_task_ids
            assert p + "make_bq_params" in dag_obj.get_task(p + "load_bq").upstream_task_ids
            assert p + "make_s3_params" in dag_obj.get_task(p + "upload_s3").upstream_task_ids
            assert p + "collect" in dag_obj.get_task(p + "make_gcs_params").upstream_task_ids
            assert p + "upload_gcs" in dag_obj.get_task(p + "load_bq").upstream_task_ids

    def test_cleanup_trigger_rule_is_all_done_per_cabinet(self):
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        for cab in _CABINETS:
            assert dag_obj.get_task(f"cabinet_{cab}.cleanup").trigger_rule == "all_done"

    def test_cleanup_upstreams_stay_within_own_cabinet(self):
        mod = _import_two_cabinets()
        dag_obj = _get_dag_obj(mod)
        for cab in _CABINETS:
            p = f"cabinet_{cab}."
            cleanup_up = dag_obj.get_task(p + "cleanup").upstream_task_ids
            assert p + "load_bq" in cleanup_up
            assert p + "upload_s3" in cleanup_up
            # every upstream is inside this cabinet
            assert all(u.startswith(p) for u in cleanup_up), cleanup_up


class TestPerCabinetAggregatorStamping:
    """Each cabinet's aggregators embed ITS OWN cabinet_id — cabinets don't mix."""

    ITEMS = [
        {"date": "2024-01-01", "path": "/tmp/cian/x/run1/2024-01-01.json"},
        {"date": "2024-01-02", "path": "/tmp/cian/x/run1/2024-01-02.json"},
    ]

    def test_gcs_keys_carry_own_cabinet_id(self):
        mod = _import_two_cabinets()
        for cab in _CABINETS:
            make_gcs_params = _get_cabinet_callable(mod, cab, "make_gcs_params")
            params = make_gcs_params(self.ITEMS, cabinet_id=cab, run_id="run1")
            assert len(params) == 2
            other = "bb" if cab == "aa" else "aa"
            for p in params:
                assert f"/{cab}/" in p["dst"]
                assert f"/{other}/" not in p["dst"]

    def test_s3_keys_carry_own_cabinet_id(self):
        mod = _import_two_cabinets()
        for cab in _CABINETS:
            make_s3_params = _get_cabinet_callable(mod, cab, "make_s3_params")
            params = make_s3_params(self.ITEMS, cabinet_id=cab)
            assert len(params) == 2
            other = "bb" if cab == "aa" else "aa"
            for p in params:
                assert f"/{cab}/" in p["dest_key"]
                assert f"/{other}/" not in p["dest_key"]

    def test_bq_table_carries_own_cabinet_id(self):
        mod = _import_two_cabinets()
        for cab in _CABINETS:
            make_bq_params = _get_cabinet_callable(mod, cab, "make_bq_params")
            params = make_bq_params(self.ITEMS, cabinet_id=cab, run_id="run1")
            assert len(params) == 2
            other = "bb" if cab == "aa" else "aa"
            for p in params:
                table = p["destination_project_dataset_table"]
                assert f"builder_reports_{cab}$" in table
                assert f"builder_reports_{other}$" not in table
                assert f"/{cab}/" in p["source_objects"][0]
