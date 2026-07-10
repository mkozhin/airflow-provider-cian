"""Structural assertions on the assembled v1 example DAG (bq_and_s3_dag.py).

Unlike the import test (tests/test_example_dag_v1.py), this module builds the DAG
with the *real* lightweight transfer-operator subclasses (Task 8), so
``upload_gcs`` / ``upload_s3`` / ``load_bq`` register as genuine
``MappedOperator`` instances. That lets us inspect the shipped wiring directly —
dependency edges and trigger rules — and catch a forgotten edge or a wrong
trigger rule that a plain import test cannot see. No metadata DB, no dag.test().
"""

from __future__ import annotations

from airflow.models.mappedoperator import MappedOperator

from tests.example_stubs import import_dag_module

_MOD_NAME = "examples.bq_and_s3_dag"
_MAPPED_TASK_IDS = ("upload_gcs", "upload_s3", "load_bq")


def _import_dag_module():
    return import_dag_module(_MOD_NAME, real_transfer_operators=True)


def _get_dag_obj(mod):
    decorated = mod.cian_to_bq_and_s3
    if hasattr(decorated, "dag"):
        return decorated.dag
    return decorated()


def _get_callable(mod, task_id):
    dag_obj = _get_dag_obj(mod)
    return dag_obj.get_task(task_id).python_callable


class TestMappedTasksArePresent:
    """upload_gcs / upload_s3 / load_bq exist as real MappedOperators."""

    def test_mapped_tasks_registered(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        for task_id in _MAPPED_TASK_IDS:
            assert task_id in dag_obj.task_dict, f"{task_id} missing from DAG"
            task = dag_obj.get_task(task_id)
            assert isinstance(
                task, MappedOperator
            ), f"{task_id} is {type(task).__name__}, not a MappedOperator"

    def test_all_expected_tasks_present(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
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

    def test_get_dates_feeds_collect(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        assert "get_dates" in dag_obj.get_task("collect").upstream_task_ids

    def test_collect_feeds_all_aggregators(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        collect_down = dag_obj.get_task("collect").downstream_task_ids
        for agg in ("make_gcs_params", "make_bq_params", "make_s3_params"):
            assert agg in collect_down, f"collect should feed {agg}"

    def test_aggregators_feed_their_mapped_tasks(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        assert "make_gcs_params" in dag_obj.get_task("upload_gcs").upstream_task_ids
        assert "make_bq_params" in dag_obj.get_task("load_bq").upstream_task_ids
        assert "make_s3_params" in dag_obj.get_task("upload_s3").upstream_task_ids

    def test_bucket_ready_before_upload_gcs(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        assert "ensure_gcs_bucket" in dag_obj.get_task("upload_gcs").upstream_task_ids

    def test_upload_gcs_before_load_bq(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        assert "upload_gcs" in dag_obj.get_task("load_bq").upstream_task_ids

    def test_cleanup_is_downstream_of_loads(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        cleanup_up = dag_obj.get_task("cleanup").upstream_task_ids
        assert "load_bq" in cleanup_up
        assert "upload_s3" in cleanup_up

    def test_cleanup_has_no_downstream(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        assert dag_obj.get_task("cleanup").downstream_task_ids == set()


class TestTriggerRules:
    """cleanup runs all_done; every other task keeps the default all_success."""

    def test_cleanup_trigger_rule_all_done(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        assert dag_obj.get_task("cleanup").trigger_rule == "all_done"

    def test_other_tasks_default_trigger_rule(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
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

    def test_aggregator_upstream_is_collect_not_get_dates(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        gcs_up = dag_obj.get_task("make_gcs_params").upstream_task_ids
        assert "collect" in gcs_up
        # It must NOT read the raw date range directly.
        assert "get_dates" not in gcs_up


class TestMixedPeriodAggregation:
    """A list of N dicts expands to exactly N kwargs, dates not shifted.

    This is what the mapped upload task would expand into at runtime.
    """

    ITEMS = [
        {"date": "2024-01-01", "path": "/tmp/cian/run1/2024-01-01.json"},
        {"date": "2024-01-02", "path": "/tmp/cian/run1/2024-01-02.json"},
        {"date": "2024-01-03", "path": "/tmp/cian/run1/2024-01-03.json"},
    ]

    def test_gcs_params_one_per_item_aligned(self):
        mod = _import_dag_module()
        make_gcs_params = _get_callable(mod, "make_gcs_params")
        params = make_gcs_params(self.ITEMS, run_id="run1")
        assert len(params) == len(self.ITEMS)
        for item, p in zip(self.ITEMS, params):
            assert p["src"] == item["path"]
            assert p["dst"].endswith(f"{item['date']}.json")

    def test_s3_params_dates_not_shifted(self):
        mod = _import_dag_module()
        make_s3_params = _get_callable(mod, "make_s3_params")
        params = make_s3_params(self.ITEMS)
        assert len(params) == len(self.ITEMS)
        for item, p in zip(self.ITEMS, params):
            assert p["filename"] == item["path"]
            assert f"_date={item['date'].replace('-', '')}" in p["dest_key"]
            assert p["dest_key"].endswith(f"{item['date']}.json")

    def test_bq_params_one_partition_per_item(self):
        mod = _import_dag_module()
        make_bq_params = _get_callable(mod, "make_bq_params")
        params = make_bq_params(self.ITEMS, run_id="run1")
        assert len(params) == len(self.ITEMS)
        for item, p in zip(self.ITEMS, params):
            compact = item["date"].replace("-", "")
            assert p["destination_project_dataset_table"].endswith(f"${compact}")
            assert p["source_objects"][0].endswith(f"{item['date']}.json")


class TestFullyEmptyPeriod:
    """Aggregators over None → [] (would expand the mapped task to zero instances).

    Explicit wiring insurance at the built-DAG level, duplicating Task 5's unit
    assert: an empty expand yields zero mapped instances → Airflow marks the
    upload/load tasks skipped, dag_run stays success.
    """

    def test_gcs_params_none_is_empty(self):
        mod = _import_dag_module()
        make_gcs_params = _get_callable(mod, "make_gcs_params")
        assert make_gcs_params(None) == []

    def test_s3_params_none_is_empty(self):
        mod = _import_dag_module()
        make_s3_params = _get_callable(mod, "make_s3_params")
        assert make_s3_params(None) == []

    def test_bq_params_none_is_empty(self):
        mod = _import_dag_module()
        make_bq_params = _get_callable(mod, "make_bq_params")
        assert make_bq_params(None) == []
