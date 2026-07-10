"""Structural assertions on the assembled v1 example DAG (bq_and_s3_dag.py).

Unlike the import test (tests/test_example_dag_v1.py), this module builds the DAG
with the *real* lightweight transfer-operator subclasses (Task 8), so
``upload_gcs`` / ``upload_s3`` / ``load_bq`` register as genuine
``MappedOperator`` instances. That lets us inspect the shipped wiring directly —
dependency edges and trigger rules — and catch a forgotten edge or a wrong
trigger rule that a plain import test cannot see. No metadata DB, no dag.test().

Aggregator *behaviour* (make_*_params over N items / over None) is deliberately
NOT retested here — that is unit-level coverage and lives in its canonical home
tests/test_example_dag_v1.py. This file keeps only genuinely structural asserts.
"""

from __future__ import annotations

import pytest
from airflow.models.mappedoperator import MappedOperator

from tests.example_stubs import import_dag_module

_MOD_NAME = "examples.bq_and_s3_dag"
_MAPPED_TASK_IDS = ("upload_gcs", "upload_s3", "load_bq")


def _build_dag_obj():
    mod = import_dag_module(_MOD_NAME, real_transfer_operators=True)
    decorated = mod.cian_to_bq_and_s3
    if hasattr(decorated, "dag"):
        return decorated.dag
    return decorated()


@pytest.fixture(scope="module")
def dag_obj():
    """Build the DAG with real transfer operators once for the whole module.

    Re-assembling the DAG (pop sys.modules, re-run module code, inject stubs) is
    expensive; every test here only inspects the built graph, so a single cached
    build is safe and much faster than rebuilding per test.
    """
    return _build_dag_obj()


class TestMappedTasksArePresent:
    """upload_gcs / upload_s3 / load_bq exist as real MappedOperators."""

    def test_mapped_tasks_registered(self, dag_obj):
        for task_id in _MAPPED_TASK_IDS:
            assert task_id in dag_obj.task_dict, f"{task_id} missing from DAG"
            task = dag_obj.get_task(task_id)
            assert isinstance(
                task, MappedOperator
            ), f"{task_id} is {type(task).__name__}, not a MappedOperator"

    def test_all_expected_tasks_present(self, dag_obj):
        expected = {
            "get_dates",
            "collect",
            "make_gcs_params",
            "make_bq_params",
            "make_s3_params",
            "ensure_gcs_bucket",
            "upload_gcs",
            "load_bq",
            "upload_s3",
            "cleanup",
        }
        assert expected <= set(dag_obj.task_dict.keys())


class TestDependencyEdges:
    """The shipped dependency graph matches the plan."""

    def test_get_dates_feeds_collect(self, dag_obj):
        assert "get_dates" in dag_obj.get_task("collect").upstream_task_ids

    def test_collect_feeds_all_aggregators(self, dag_obj):
        collect_down = dag_obj.get_task("collect").downstream_task_ids
        for agg in ("make_gcs_params", "make_bq_params", "make_s3_params"):
            assert agg in collect_down, f"collect should feed {agg}"

    def test_aggregators_feed_their_mapped_tasks(self, dag_obj):
        assert "make_gcs_params" in dag_obj.get_task("upload_gcs").upstream_task_ids
        assert "make_bq_params" in dag_obj.get_task("load_bq").upstream_task_ids
        assert "make_s3_params" in dag_obj.get_task("upload_s3").upstream_task_ids

    def test_bucket_ready_before_upload_gcs(self, dag_obj):
        assert "ensure_gcs_bucket" in dag_obj.get_task("upload_gcs").upstream_task_ids

    def test_upload_gcs_before_load_bq(self, dag_obj):
        assert "upload_gcs" in dag_obj.get_task("load_bq").upstream_task_ids

    def test_cleanup_is_downstream_of_loads(self, dag_obj):
        cleanup_up = dag_obj.get_task("cleanup").upstream_task_ids
        assert "load_bq" in cleanup_up
        assert "upload_s3" in cleanup_up

    def test_cleanup_has_no_downstream(self, dag_obj):
        assert dag_obj.get_task("cleanup").downstream_task_ids == set()


class TestTriggerRules:
    """cleanup runs all_done; every other task keeps the default all_success."""

    def test_cleanup_trigger_rule_all_done(self, dag_obj):
        assert dag_obj.get_task("cleanup").trigger_rule == "all_done"

    def test_other_tasks_default_trigger_rule(self, dag_obj):
        for task_id in (
            "collect",
            "make_gcs_params",
            "make_bq_params",
            "make_s3_params",
            "upload_gcs",
            "load_bq",
            "upload_s3",
        ):
            assert (
                dag_obj.get_task(task_id).trigger_rule == "all_success"
            ), f"{task_id} should keep the default trigger rule"


class TestAggregatorWiredToCollectOutput:
    """The aggregator draws its input from collect.output (via ``collected``).

    The wiring goes through the collected {"date","path"} dicts, not a separate
    ``dates`` list — so a collect→aggregator edge is the structural evidence that
    the aggregator maps over collect's XCom output rather than the raw date range.
    """

    def test_aggregator_upstream_is_collect_not_get_dates(self, dag_obj):
        gcs_up = dag_obj.get_task("make_gcs_params").upstream_task_ids
        assert "collect" in gcs_up
        # It must NOT read the raw date range directly.
        assert "get_dates" not in gcs_up
