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
from unittest.mock import MagicMock

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


def _make_provider_stubs() -> dict[str, types.ModuleType]:
    """Build a dict of stub modules to inject into ``sys.modules``.

    Covers both the transfer operators used by the v1/multi-account DAGs and the
    hooks (``S3Hook``/``BigQueryHook``/``GCSHook``), ``google.api_core.exceptions.Conflict``
    and ``google.cloud.bigquery`` used by the v2 DAG.
    """
    stubs: dict[str, types.ModuleType] = {}

    for name in _MISSING_PROVIDERS:
        if name in sys.modules:
            continue
        mod = types.ModuleType(name)
        stubs[name] = mod

    def _get(name: str) -> types.ModuleType:
        return stubs.get(name, sys.modules.get(name, types.ModuleType(name)))

    # ── hooks ─────────────────────────────────────────────────────────────────
    gcs_mod = _get("airflow.providers.google.cloud.hooks.gcs")
    gcs_mod.GCSHook = MagicMock(name="GCSHook")

    bq_hook_mod = _get("airflow.providers.google.cloud.hooks.bigquery")
    bq_hook_mod.BigQueryHook = MagicMock(name="BigQueryHook")

    s3_hook_mod = _get("airflow.providers.amazon.aws.hooks.s3")
    s3_hook_mod.S3Hook = MagicMock(name="S3Hook")

    # ── transfer operators ────────────────────────────────────────────────────
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


def import_dag_module(mod_name: str) -> types.ModuleType:
    """Import (or re-import) an example DAG module with provider stubs injected.

    Pops the cached module first so module-level code re-runs on every call,
    injects the provider stubs into ``sys.modules`` for the duration of the
    import, then removes any stub modules that were not already present
    (restoring ``sys.modules`` to its prior state).
    """
    sys.modules.pop(mod_name, None)

    stubs = _make_provider_stubs()
    previously_absent = [k for k in stubs if k not in sys.modules]
    sys.modules.update(stubs)

    try:
        return importlib.import_module(mod_name)
    finally:
        for k in previously_absent:
            sys.modules.pop(k, None)
