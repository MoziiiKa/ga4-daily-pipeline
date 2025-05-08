"""BigQuery loader helper for GA4 daily pipeline."""

import json
from google.cloud import bigquery, storage
from .common import _log

from .config import BUCKET_NAME, CONTRACT_BLOB, DATASET_ID, TABLE_ID

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
# BUCKET_NAME    = "platform_assignment_bucket"
# CONTRACT_BLOB  = "contracts/Mozaffar_Kazemi_GA4Schema.json"
# DATASET_ID     = "Mozaffar_Kazemi_GA4Raw"
# TABLE_ID       = "Mozaffar_Kazemi_DailyEvents"

bq_client = bigquery.Client()
storage_client = storage.Client()


# ---------------------------------------------------------------------
# Contract helpers
# ---------------------------------------------------------------------
def _load_contract_columns() -> list[dict]:
    """Download contract JSON from GCS and return list of {name, type}."""
    blob = storage_client.bucket(BUCKET_NAME).blob(CONTRACT_BLOB)
    if not blob.exists():
        _log(
            "⚡️ Contract JSON missing in GCS; will save it after first load", "WARNING"
        )
        return []
    return json.loads(blob.download_as_bytes())


def _save_contract_from_table():
    """
    After the first load, fetch the table schema via the BigQuery API
    and persist it to GCS for future runs.
    """
    table_ref = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"
    table = bq_client.get_table(table_ref)  # fetches the Table which has .schema
    schema_json = [
        {"name": field.name, "type": field.field_type} for field in table.schema
    ]
    blob = storage_client.bucket(BUCKET_NAME).blob(CONTRACT_BLOB)
    blob.upload_from_string(
        json.dumps(schema_json, indent=2), content_type="application/json"
    )
    _log(f"➖ Initial schema saved to {CONTRACT_BLOB}", "INFO")


# ---------------------------------------------------------------------
# Dataset & job config helpers
# ---------------------------------------------------------------------
def _ensure_dataset() -> None:
    """Create dataset if absent (idempotent)."""
    ds_ref = bigquery.DatasetReference(bq_client.project, DATASET_ID)
    try:
        bq_client.get_dataset(ds_ref)
    except Exception:
        _log(f"➖ Dataset {DATASET_ID} absent—creating.", "INFO")
        bq_client.create_dataset(bigquery.Dataset(ds_ref), exists_ok=True)


def _load_config(autodetect: bool) -> bigquery.LoadJobConfig:
    """Return a configured LoadJobConfig."""
    job_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=autodetect,
        skip_leading_rows=1,  # SKIP HEADER ROW
    )
    if not autodetect:
        # explicit schema for subsequent runs
        contract_cols = _load_contract_columns()
        if contract_cols:
            job_cfg.schema = [
                bigquery.SchemaField(c["name"], c["type"]) for c in contract_cols
            ]
    return job_cfg


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def load_to_bq(gcs_uri: str) -> None:
    """
    Load CSV at gcs_uri into managed table, appending by date partition.
    On the first run, autodetect schema and then persist it for next runs.
    """
    _ensure_dataset()
    table_ref = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"

    # determine if this is the very first load
    try:
        bq_client.get_table(table_ref)
        first_run = False
    except Exception:
        first_run = True

    # configure and launch the load job
    job_cfg = _load_config(autodetect=first_run)
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_cfg)
    load_job.result()  # block until complete

    # on first run, fetch the real schema via the Table API and save to GCS
    if first_run:
        _save_contract_from_table()

    _log(f"➖ BigQuery load job {load_job.job_id} finished", "INFO")
