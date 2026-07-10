"""Tests for the shared example-DAG provider stubs (tests/example_stubs.py).

The key regression guard: when ``real_transfer_operators=True`` the transfer
operators are lightweight real ``BaseOperator`` subclasses, so
``.expand_kwargs(...)`` registers genuine ``MappedOperator`` instances in the
built DAG. Without this, ``upload_gcs`` / ``upload_s3`` / ``load_bq`` would be
plain ``MagicMock`` return values, absent from ``dag.task_dict`` — and the
structural assertions in Tasks 9-10 would silently inspect an empty DAG.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from airflow.models.mappedoperator import MappedOperator

from tests.example_stubs import (
    MAPPED_TASK_IDS as _MAPPED_TASK_IDS,
    GCSToBigQueryOperator,
    LocalFilesystemToGCSOperator,
    LocalFilesystemToS3Operator,
    _make_provider_stubs,
    get_dag_obj,
    import_dag_module,
)

_V1_MOD = "examples.bq_and_s3_dag"


def _get_dag(mod):
    return get_dag_obj(mod, "cian_to_bq_and_s3")


class TestStubOperatorSubclasses:
    """The three subclasses are genuine operators accepting provider kwargs."""

    def test_gcs_operator_accepts_partial_and_expand_kwargs(self):
        op = LocalFilesystemToGCSOperator(
            task_id="t", gcp_conn_id="c", bucket="b", src="s", dst="d"
        )
        assert isinstance(op, LocalFilesystemToGCSOperator)
        assert op.bucket == "b"
        assert op.execute({}) is None

    def test_bq_operator_accepts_all_partial_kwargs(self):
        op = GCSToBigQueryOperator(
            task_id="t",
            gcp_conn_id="c",
            bucket="b",
            schema_fields=[{"name": "x"}],
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning={"type": "DAY", "field": "date"},
            source_objects=["o"],
            destination_project_dataset_table="p.d.t$20200101",
        )
        assert op.write_disposition == "WRITE_TRUNCATE"
        assert op.execute({}) is None

    def test_s3_operator_accepts_all_partial_kwargs(self):
        op = LocalFilesystemToS3Operator(
            task_id="t",
            aws_conn_id="c",
            dest_bucket="b",
            replace=True,
            filename="f",
            dest_key="k",
        )
        assert op.replace is True
        assert op.execute({}) is None


class TestProviderStubsFlag:
    """The flag toggles between MagicMock and real operators."""

    def test_default_transfer_operators_are_magicmock(self):
        stubs = _make_provider_stubs()
        mod = stubs["airflow.providers.google.cloud.transfers.local_to_gcs"]
        assert isinstance(mod.LocalFilesystemToGCSOperator, MagicMock)

    def test_real_transfer_operators_are_baseoperator_subclasses(self):
        stubs = _make_provider_stubs(real_transfer_operators=True)
        gcs_mod = stubs["airflow.providers.google.cloud.transfers.local_to_gcs"]
        bq_mod = stubs["airflow.providers.google.cloud.transfers.gcs_to_bigquery"]
        s3_mod = stubs["airflow.providers.amazon.aws.transfers.local_to_s3"]
        assert gcs_mod.LocalFilesystemToGCSOperator is LocalFilesystemToGCSOperator
        assert bq_mod.GCSToBigQueryOperator is GCSToBigQueryOperator
        assert s3_mod.LocalFilesystemToS3Operator is LocalFilesystemToS3Operator


class TestMappedOperatorRegistration:
    """Regression guard: real subclasses register genuine MappedOperators."""

    def test_expand_kwargs_registers_real_mapped_operators(self):
        mod = import_dag_module(_V1_MOD, real_transfer_operators=True)
        dag = _get_dag(mod)
        for task_id in _MAPPED_TASK_IDS:
            assert task_id in dag.task_dict, f"{task_id} missing from DAG"
            task = dag.get_task(task_id)
            assert isinstance(
                task, MappedOperator
            ), f"{task_id} is {type(task).__name__}, not a MappedOperator"

    def test_default_stubs_do_not_register_mapped_operators(self):
        """Without the flag, expand_kwargs yields MagicMocks absent from the DAG."""
        mod = import_dag_module(_V1_MOD)
        dag = _get_dag(mod)
        for task_id in _MAPPED_TASK_IDS:
            assert task_id not in dag.task_dict
