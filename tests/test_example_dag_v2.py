"""Tests for the v2 example DAG (bq_and_s3_dag_v2.py).

v2 has NO separate mapped upload tasks: GCS/BigQuery/S3 uploads all happen inside
the single ``process_date`` @task. An empty day therefore yields ``process_date``
returning ``None`` with no uploads at all — and, unlike v1/multi-account, never a
``skipped`` state.

The google/amazon provider packages are not installed, so the DAG is imported
with the shared provider stubs (see tests/example_stubs.py).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tests.example_stubs import get_dag_obj, import_dag_module

_MOD_NAME = "examples.bq_and_s3_dag_v2"


def _import_dag_module():
    return import_dag_module(_MOD_NAME)


def _get_dag_obj(mod):
    """Return the actual DAG object from the @dag-decorated callable."""
    return get_dag_obj(mod, "cian_to_bq_and_s3_v2")


def _get_callable(mod, task_id):
    """Return the python_callable of a nested @task by its task_id.

    Nested @task functions (process_date, cleanup) are not module-level; they
    are only reachable through the assembled DAG object.
    """
    dag_obj = _get_dag_obj(mod)
    return dag_obj.get_task(task_id).python_callable


class TestV2DagImport:
    """Smoke tests: the v2 DAG imports without errors."""

    def test_dag_imports(self):
        mod = _import_dag_module()
        assert hasattr(mod, "cian_to_bq_and_s3_v2")

    def test_dag_has_process_date_and_cleanup_tasks(self):
        mod = _import_dag_module()
        dag_obj = _get_dag_obj(mod)
        task_ids = set(dag_obj.task_dict.keys())
        assert "process_date" in task_ids
        assert "cleanup" in task_ids


class TestV2Cleanup:
    """cleanup must tolerate an empty mapped result (None / [])."""

    def test_cleanup_none_does_not_raise(self):
        mod = _import_dag_module()
        cleanup = _get_callable(mod, "cleanup")
        # Must not raise (empty period → mapped output is None).
        assert cleanup(None) is None

    def test_cleanup_empty_list_does_not_raise(self):
        mod = _import_dag_module()
        cleanup = _get_callable(mod, "cleanup")
        assert cleanup([]) is None

    def test_cleanup_removes_run_dir(self, tmp_path):
        """The positive path: cleanup removes the run dir of the given paths."""
        mod = _import_dag_module()
        cleanup = _get_callable(mod, "cleanup")

        run_dir = tmp_path / "run1"
        run_dir.mkdir()
        f = run_dir / "2024-01-01.json"
        f.write_text("{}")

        # process_date returns local path strings; cleanup deletes their run dir.
        cleanup([str(f)])
        assert not run_dir.exists()


class TestV2ProcessDate:
    """process_date behaviour for empty vs non-empty days."""

    def test_empty_day_skips_all_uploads(self):
        """When execute() returns None, no GCS/BigQuery/S3 upload happens."""
        mod = _import_dag_module()
        process_date = _get_callable(mod, "process_date")

        op_cls = MagicMock(name="CianBuilderReportsOperator")
        op_cls.return_value.execute.return_value = None

        gcs_hook = MagicMock(name="GCSHook")
        bq_hook = MagicMock(name="BigQueryHook")
        s3_hook = MagicMock(name="S3Hook")

        with patch.object(mod, "CianBuilderReportsOperator", op_cls), \
             patch.object(mod, "GCSHook", gcs_hook), \
             patch.object(mod, "BigQueryHook", bq_hook), \
             patch.object(mod, "S3Hook", s3_hook):
            result = process_date(date="2024-01-01", run_id="run1")

        assert result is None
        # No uploads whatsoever for an empty day.
        gcs_hook.assert_not_called()
        bq_hook.assert_not_called()
        s3_hook.assert_not_called()

    def test_non_empty_day_uploads_and_uses_result_path(self):
        """When execute() returns a dict, all three uploads run using result['path']."""
        mod = _import_dag_module()
        process_date = _get_callable(mod, "process_date")

        local_path = "/tmp/cian/run1/2024-01-01.json"
        op_cls = MagicMock(name="CianBuilderReportsOperator")
        op_cls.return_value.execute.return_value = {
            "date": "2024-01-01",
            "path": local_path,
        }

        gcs_hook = MagicMock(name="GCSHook")
        bq_hook = MagicMock(name="BigQueryHook")
        s3_hook = MagicMock(name="S3Hook")

        with patch.object(mod, "CianBuilderReportsOperator", op_cls), \
             patch.object(mod, "GCSHook", gcs_hook), \
             patch.object(mod, "BigQueryHook", bq_hook), \
             patch.object(mod, "S3Hook", s3_hook):
            result = process_date(date="2024-01-01", run_id="run1")

        assert result == local_path

        # All three uploads happened.
        gcs_hook.assert_called_once()
        bq_hook.assert_called_once()
        s3_hook.assert_called_once()

        # GCS upload uses the path returned by execute()["path"].
        _, gcs_kwargs = gcs_hook.return_value.upload.call_args
        assert gcs_kwargs["filename"] == local_path

        # BQ load reads from the gs:// staging URI and targets the day partition.
        bq_call = bq_hook.return_value.get_client.return_value.load_table_from_uri.call_args
        gs_uri, table = bq_call.args[0], bq_call.args[1]
        assert gs_uri.startswith("gs://")
        assert gs_uri.endswith("2024-01-01.json")
        assert "/run1/" in gs_uri  # safe_id(run_id) interpolated into the staging path
        assert table.endswith("$20240101")  # ${date_compact} partition decorator
        assert "job_config" in bq_call.kwargs

        # S3 upload uses the path returned by execute()["path"].
        _, s3_kwargs = s3_hook.return_value.load_file.call_args
        assert s3_kwargs["filename"] == local_path
