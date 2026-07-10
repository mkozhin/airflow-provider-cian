"""Shared provider stubs for example-DAG import tests.

The google/amazon provider packages are NOT installed in the test environment,
so example DAGs that import them cannot be imported directly. This module builds
stub modules to inject into ``sys.modules`` so those DAGs import cleanly.

Used by:
- ``tests/test_example_dag_multi_account.py``
- ``tests/test_example_dag_v2.py``
(and future example-DAG import tests — keep it importable and side-effect free
at import time).
"""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock, patch

from airflow.models import BaseOperator

# Task ids of the three mapped upload/load tasks in the v1 and multi-account DAGs.
# Exported here so the structural tests share one definition instead of copy-pasting it.
MAPPED_TASK_IDS = ("upload_gcs", "upload_s3", "load_bq")

# Provider sub-packages that are not installed in the test environment.
# NOTE: do NOT include "airflow.providers" — it is a real namespace package.
_MISSING_PROVIDERS = [
    "airflow.providers.amazon",
    "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.hooks",
    "airflow.providers.amazon.aws.hooks.s3",
    "airflow.providers.amazon.aws.transfers",
    "airflow.providers.amazon.aws.transfers.local_to_s3",
    "airflow.providers.google",
    "airflow.providers.google.cloud",
    "airflow.providers.google.cloud.hooks",
    "airflow.providers.google.cloud.hooks.bigquery",
    "airflow.providers.google.cloud.hooks.gcs",
    "airflow.providers.google.cloud.transfers",
    "airflow.providers.google.cloud.transfers.gcs_to_bigquery",
    "airflow.providers.google.cloud.transfers.local_to_gcs",
    "google",
    "google.api_core",
    "google.api_core.exceptions",
    "google.cloud",
    "google.cloud.bigquery",
]


# ── real (lightweight) transfer-operator subclasses ──────────────────────────
#
# The default stubs make the transfer operators ``MagicMock`` with
# ``.partial = MagicMock(return_value=MagicMock())``. That means
# ``.expand_kwargs(...)`` returns a plain ``MagicMock`` rather than a genuine
# ``MappedOperator``, so ``upload_gcs`` / ``upload_s3`` / ``load_bq`` never appear
# in the built DAG's ``task_dict``. Structural assertions (Tasks 9-10) would then
# silently inspect an empty DAG.
#
# To let those assertions inspect real mapped tasks, ``_make_provider_stubs`` can
# substitute these real ``BaseOperator`` subclasses instead. They support
# ``.partial()`` / ``.expand_kwargs()`` exactly like normal operators (that is
# just ``BaseOperator`` machinery) and have a trivial ``execute()``.

# Each subclass declares its provider-specific keyword arguments EXPLICITLY in
# ``__init__``. Airflow's ``partial()`` validation
# (``mappedoperator.validate_mapping_kwargs``) checks kwargs against the operator's
# ``__init__`` parameter names, which ``BaseOperatorMeta`` derives from the literal
# signature — a bare ``**kwargs`` would leave those names unknown and make
# ``.partial(gcp_conn_id=..., bucket=...)`` raise ``TypeError``. Both the
# ``.partial(...)`` kwargs and the ``.expand_kwargs(...)`` keys therefore appear
# below. All are accepted and ignored; ``execute()`` is trivial.


class LocalFilesystemToGCSOperator(BaseOperator):
    template_fields = ("src", "dst", "bucket")

    def __init__(
        self,
        *,
        gcp_conn_id=None,
        bucket=None,
        src=None,
        dst=None,
        **kwargs,
    ) -> None:
        self.gcp_conn_id = gcp_conn_id
        self.bucket = bucket
        self.src = src
        self.dst = dst
        super().__init__(**kwargs)

    def execute(self, context):  # noqa: D401 - trivial
        return None


class GCSToBigQueryOperator(BaseOperator):
    template_fields = ("bucket", "source_objects", "destination_project_dataset_table")

    def __init__(
        self,
        *,
        gcp_conn_id=None,
        bucket=None,
        schema_fields=None,
        source_format=None,
        write_disposition=None,
        create_disposition=None,
        time_partitioning=None,
        source_objects=None,
        destination_project_dataset_table=None,
        **kwargs,
    ) -> None:
        self.gcp_conn_id = gcp_conn_id
        self.bucket = bucket
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.time_partitioning = time_partitioning
        self.source_objects = source_objects
        self.destination_project_dataset_table = destination_project_dataset_table
        super().__init__(**kwargs)

    def execute(self, context):  # noqa: D401 - trivial
        return None


class LocalFilesystemToS3Operator(BaseOperator):
    template_fields = ("filename", "dest_key", "dest_bucket")

    def __init__(
        self,
        *,
        aws_conn_id=None,
        dest_bucket=None,
        replace=None,
        filename=None,
        dest_key=None,
        **kwargs,
    ) -> None:
        self.aws_conn_id = aws_conn_id
        self.dest_bucket = dest_bucket
        self.replace = replace
        self.filename = filename
        self.dest_key = dest_key
        super().__init__(**kwargs)

    def execute(self, context):  # noqa: D401 - trivial
        return None


def _make_provider_stubs(
    *, real_transfer_operators: bool = False
) -> dict[str, types.ModuleType]:
    """Build a dict of stub modules to inject into ``sys.modules``.

    Covers both the transfer operators used by the v1/multi-account DAGs and the
    hooks (``S3Hook``/``BigQueryHook``/``GCSHook``), ``google.api_core.exceptions.Conflict``
    and ``google.cloud.bigquery`` used by the v2 DAG.

    When ``real_transfer_operators`` is ``True`` the three transfer operators are
    the lightweight real ``BaseOperator`` subclasses above (so ``expand_kwargs``
    yields genuine ``MappedOperator`` instances in the built DAG). Otherwise they
    are ``MagicMock`` — enough for plain import tests and backward compatible.
    """
    stubs: dict[str, types.ModuleType] = {}

    for name in _MISSING_PROVIDERS:
        if name in sys.modules:
            continue
        mod = types.ModuleType(name)
        stubs[name] = mod

    def _get(name: str) -> types.ModuleType:
        # Every name passed here is in _MISSING_PROVIDERS, so it is either a stub
        # we just built or an already-present real module in sys.modules.
        return stubs[name] if name in stubs else sys.modules[name]

    # ── hooks ─────────────────────────────────────────────────────────────────
    gcs_mod = _get("airflow.providers.google.cloud.hooks.gcs")
    gcs_mod.GCSHook = MagicMock(name="GCSHook")

    bq_hook_mod = _get("airflow.providers.google.cloud.hooks.bigquery")
    bq_hook_mod.BigQueryHook = MagicMock(name="BigQueryHook")

    s3_hook_mod = _get("airflow.providers.amazon.aws.hooks.s3")
    s3_hook_mod.S3Hook = MagicMock(name="S3Hook")

    # ── transfer operators ────────────────────────────────────────────────────
    gcs_to_bq_mod = _get("airflow.providers.google.cloud.transfers.gcs_to_bigquery")
    local_to_gcs_mod = _get("airflow.providers.google.cloud.transfers.local_to_gcs")
    s3_mod = _get("airflow.providers.amazon.aws.transfers.local_to_s3")

    if real_transfer_operators:
        gcs_to_bq_mod.GCSToBigQueryOperator = GCSToBigQueryOperator
        local_to_gcs_mod.LocalFilesystemToGCSOperator = LocalFilesystemToGCSOperator
        s3_mod.LocalFilesystemToS3Operator = LocalFilesystemToS3Operator
    else:
        GCSToBQ = MagicMock(name="GCSToBigQueryOperator")
        GCSToBQ.partial = MagicMock(return_value=MagicMock())
        gcs_to_bq_mod.GCSToBigQueryOperator = GCSToBQ

        LocalToGCS = MagicMock(name="LocalFilesystemToGCSOperator")
        LocalToGCS.partial = MagicMock(return_value=MagicMock())
        local_to_gcs_mod.LocalFilesystemToGCSOperator = LocalToGCS

        LocalToS3 = MagicMock(name="LocalFilesystemToS3Operator")
        LocalToS3.partial = MagicMock(return_value=MagicMock())
        s3_mod.LocalFilesystemToS3Operator = LocalToS3

    # ── google.api_core / google.cloud.bigquery ──────────────────────────────
    google_exceptions = _get("google.api_core.exceptions")
    google_exceptions.Conflict = type("Conflict", (Exception,), {})

    # ``from google.cloud import bigquery`` resolves the ``bigquery`` attribute
    # on the google.cloud package. A MagicMock lets the DAG access
    # bigquery.SchemaField / LoadJobConfig / SourceFormat.* / WriteDisposition.*
    # / TimePartitioning* without the real library being installed.
    google_cloud_mod = _get("google.cloud")
    bigquery_mock = MagicMock(name="bigquery")
    google_cloud_mod.bigquery = bigquery_mock
    stubs["google.cloud.bigquery"] = bigquery_mock  # type: ignore[assignment]

    return stubs


def get_dag_obj(mod: types.ModuleType, attr: str):
    """Return the assembled DAG object from a ``@dag``-decorated callable.

    In Airflow 2.x the ``@dag`` decorator exposes the resolved DAG via ``.dag``;
    the fallback calls the factory (idempotent) to obtain it.
    """
    decorated = getattr(mod, attr)
    if hasattr(decorated, "dag"):
        return decorated.dag
    return decorated()


def import_dag_module(
    mod_name: str,
    *,
    real_transfer_operators: bool = False,
    list_accounts: list | None = None,
) -> types.ModuleType:
    """Import (or re-import) an example DAG module with provider stubs injected.

    Pops the cached module first so module-level code re-runs on every call,
    injects the provider stubs into ``sys.modules`` for the duration of the
    import, then removes any stub modules that were not already present
    (restoring ``sys.modules`` to its prior state).

    When ``real_transfer_operators`` is ``True`` the transfer operators are the
    lightweight real ``BaseOperator`` subclasses, so the built DAG contains
    genuine ``MappedOperator`` instances (needed for structural assertions).

    When ``list_accounts`` is not ``None`` the import runs under a patch of
    ``airflow_provider_cian.accounts.list_accounts`` returning that list, so the
    multi-account DAG builds its cabinet TaskGroups from the given accounts. Pass
    ``[]`` for the no-cabinet case; ``None`` (the default) leaves the real
    ``list_accounts`` in place.
    """
    sys.modules.pop(mod_name, None)

    stubs = _make_provider_stubs(real_transfer_operators=real_transfer_operators)
    previously_absent = [k for k in stubs if k not in sys.modules]
    sys.modules.update(stubs)

    try:
        if list_accounts is not None:
            with patch(
                "airflow_provider_cian.accounts.list_accounts",
                return_value=list_accounts,
            ):
                return importlib.import_module(mod_name)
        return importlib.import_module(mod_name)
    finally:
        for k in previously_absent:
            sys.modules.pop(k, None)
