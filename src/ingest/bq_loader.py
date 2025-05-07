"""BigQuery loader helper for GA4 daily pipeline."""

import json
from google.cloud import bigquery, storage
from .common import _log

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
BUCKET_NAME = "platform_assignment_bucket"
CONTRACT_BLOB = "contracts/Mozaffar_Kazemi_GA4Schema.json"

DATASET_ID = "Mozaffar_Kazemi_GA4Raw"
TABLE_ID = "Mozaffar_Kazemi_DailyEvents"

bq_client = bigquery.Client()
storage_client = storage.Client()


# ---------------------------------------------------------------------
# Contract helpers
# ---------------------------------------------------------------------
def _load_contract_columns() -> list[dict]:
    """Download contract JSON from GCS and return list of {name, type}"""
    blob = storage_client.bucket(BUCKET_NAME).blob(CONTRACT_BLOB)
    if not blob.exists():
        _log("Contract JSON missing in GCS; will create after first load", "WARNING")
        return []
    return json.loads(blob.download_as_bytes())


def _save_contract(schema: list[bigquery.SchemaField]) -> None:
    blob = storage_client.bucket(BUCKET_NAME).blob(CONTRACT_BLOB)
    schema_json = [{"name": f.name, "type": f.field_type} for f in schema]
    blob.upload_from_string(
        json.dumps(schema_json, indent=2), content_type="application/json"
    )
    _log("Initial schema saved to contracts/Mozaffar_Kazemi_GA4Schema.json", "INFO")


# ---------------------------------------------------------------------
# Dataset & job config helpers
# ---------------------------------------------------------------------
def _ensure_dataset() -> None:
    """Create dataset if absent (idempotent)."""
    ds_ref = bigquery.DatasetReference(bq_client.project, DATASET_ID)
    try:
        bq_client.get_dataset(ds_ref)
    except Exception:
        _log(f"Dataset {DATASET_ID} absent—creating.", "INFO")
        bq_client.create_dataset(bigquery.Dataset(ds_ref), exists_ok=True)


def _load_config(autodetect: bool) -> bigquery.LoadJobConfig:
    """Return a configured LoadJobConfig."""
    job_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=autodetect,
    )

    if not autodetect:
        contract_cols = _load_contract_columns()
        job_cfg.schema = [
            bigquery.SchemaField(c["name"], c["type"]) for c in contract_cols
        ]

    return job_cfg


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def load_to_bq(gcs_uri: str) -> None:
    """Load CSV at gcs_uri into managed table, appending by date partition."""
    _ensure_dataset()

    table_ref = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"
    table_exists = True
    try:
        bq_client.get_table(table_ref)
    except Exception:
        table_exists = False

    job_cfg = _load_config(autodetect=not table_exists)
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_cfg)
    load_job.result()  # block until done

    if not table_exists:
        _save_contract(load_job.schema)

    _log(f"BigQuery load job {load_job.job_id} finished", "INFO")
