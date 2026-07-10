"""Tests for the v1 example DAG (bq_and_s3_dag.py).

v1 has separate mapped upload tasks (upload_gcs / upload_s3 / load_bq) fed by
small @task aggregators (make_gcs_params / make_s3_params / make_bq_params).
Each aggregator now takes a single ``items`` input — the list of
``{"date", "path"}`` dicts produced by ``collect.expand`` — instead of zipping
``paths`` with ``dates``. When the whole period is empty, XCom yields ``None``
(not ``[]``), so every aggregator must start with ``items = list(items or [])``.

The google/amazon provider packages are not installed, so the DAG is imported
with the shared provider stubs (see tests/example_stubs.py).
"""

from __future__ import annotations

from tests.example_stubs import import_dag_module

_MOD_NAME = "examples.bq_and_s3_dag"


def _import_dag_module():
    return import_dag_module(_MOD_NAME)


def _get_dag_obj(mod):
    """Return the actual DAG object from the @dag-decorated callable."""
    decorated = mod.cian_to_bq_and_s3
    if hasattr(decorated, "dag"):
        return decorated.dag
    return decorated()


def _get_callable(mod, task_id):
    """Return the python_callable of a nested @task by its task_id.

    The aggregators and cleanup are declared inside the DAG factory and wrapped
    in @task, so they are only reachable through the assembled DAG object.
    """
    dag_obj = _get_dag_obj(mod)
    return dag_obj.get_task(task_id).python_callable


class TestV1DagImport:
    """Smoke tests: the v1 DAG imports without errors."""

    def test_dag_imports(self):
        mod = _import_dag_module()
        assert hasattr(mod, "cian_to_bq_and_s3")

    def test_dag_has_expected_tasks(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        task_ids = set(dag_obj.task_dict.keys())
        # The transfer operators (upload_gcs / load_bq / upload_s3) are MagicMock
        # stubs in this import test, so their expand_kwargs() does not register a
        # real MappedOperator — only the @task-based nodes and ``collect`` appear.
        for expected in (
            "collect",
            "make_gcs_params",
            "make_bq_params",
            "make_s3_params",
            "cleanup",
        ):
            assert expected in task_ids


class TestV1AggregatorsEmptyPeriod:
    """The single most fragile point: aggregators over a None input.

    When the whole period is empty, the mapped ``collect`` writes no XCom and
    downstream receives ``None`` (not ``[]``). A bare len()/iteration on None
    would raise TypeError, so each aggregator must coerce with ``or []``.
    """

    def test_make_gcs_params_none_returns_empty(self):
        mod = _import_dag_module()
        make_gcs_params = _get_callable(mod, "make_gcs_params")
        assert make_gcs_params(None) == []

    def test_make_s3_params_none_returns_empty(self):
        mod = _import_dag_module()
        make_s3_params = _get_callable(mod, "make_s3_params")
        assert make_s3_params(None) == []

    def test_make_bq_params_none_returns_empty(self):
        mod = _import_dag_module()
        make_bq_params = _get_callable(mod, "make_bq_params")
        assert make_bq_params(None) == []


class TestV1AggregatorsWithData:
    """Aggregators over a two-item list build keys for exactly those dates.

    Each item is self-describing ({"date", "path"}), so date shifting via
    zip(paths, dates) is impossible by construction.
    """

    ITEMS = [
        {"date": "2024-01-01", "path": "/tmp/cian/run1/2024-01-01.json"},
        {"date": "2024-01-02", "path": "/tmp/cian/run1/2024-01-02.json"},
    ]

    def test_make_gcs_params_maps_each_item(self):
        mod = _import_dag_module()
        make_gcs_params = _get_callable(mod, "make_gcs_params")
        params = make_gcs_params(self.ITEMS, run_id="run1")
        assert len(params) == 2
        assert params[0]["src"] == "/tmp/cian/run1/2024-01-01.json"
        assert params[0]["dst"].endswith("2024-01-01.json")
        # safe_id(run_id) must be interpolated into the GCS object path — guards
        # against silently dropping the per-run segment.
        assert "/run1/" in params[0]["dst"]
        assert params[1]["src"] == "/tmp/cian/run1/2024-01-02.json"
        assert params[1]["dst"].endswith("2024-01-02.json")

    def test_make_s3_params_dates_not_shifted(self):
        mod = _import_dag_module()
        make_s3_params = _get_callable(mod, "make_s3_params")
        params = make_s3_params(self.ITEMS)
        assert len(params) == 2
        # Item 0 → 2024-01-01, item 1 → 2024-01-02: path and date stay aligned.
        assert params[0]["filename"] == "/tmp/cian/run1/2024-01-01.json"
        assert "_date=20240101" in params[0]["dest_key"]
        assert params[0]["dest_key"].endswith("2024-01-01.json")
        assert params[1]["filename"] == "/tmp/cian/run1/2024-01-02.json"
        assert "_date=20240102" in params[1]["dest_key"]
        assert params[1]["dest_key"].endswith("2024-01-02.json")

    def test_make_bq_params_built_from_items(self):
        mod = _import_dag_module()
        make_bq_params = _get_callable(mod, "make_bq_params")
        params = make_bq_params(self.ITEMS, run_id="run1")
        assert len(params) == 2
        assert params[0]["destination_project_dataset_table"].endswith("$20240101")
        assert params[0]["source_objects"][0].endswith("2024-01-01.json")
        # run_id segment is interpolated into the GCS source object URI.
        assert "/run1/" in params[0]["source_objects"][0]
        assert params[1]["destination_project_dataset_table"].endswith("$20240102")
        assert params[1]["source_objects"][0].endswith("2024-01-02.json")


class TestV1Cleanup:
    """cleanup must tolerate an empty mapped result (None / [])."""

    def test_cleanup_none_does_not_raise(self):
        mod = _import_dag_module()
        cleanup = _get_callable(mod, "cleanup")
        assert cleanup(None) is None

    def test_cleanup_empty_list_does_not_raise(self):
        mod = _import_dag_module()
        cleanup = _get_callable(mod, "cleanup")
        assert cleanup([]) is None

    def test_cleanup_removes_run_dir(self, tmp_path):
        mod = _import_dag_module()
        cleanup = _get_callable(mod, "cleanup")
        run_dir = tmp_path / "run1"
        run_dir.mkdir()
        f = run_dir / "2024-01-01.json"
        f.write_text("{}")
        items = [{"date": "2024-01-01", "path": str(f)}]
        cleanup(items)
        assert not run_dir.exists()
