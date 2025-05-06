from google.cloud import bigquery
from pathlib import Path
import json, logging

BUCKET_NAME = "platform_assignment_bucket"
RAW_PREFIX  = "ga4_raw"

DATASET_ID = "ga4_raw"
TABLE_ID   = "daily_events"
CONTRACT_JSON = Path(__file__).parents[2] / "docs" / "ga4_csv_schema.json"

bq_client = bigquery.Client()
logger = logging.getLogger("ingest")

def _ensure_dataset():
    """Create ga4_raw if it doesn't exist (idempotent)."""
    ds_ref = bigquery.DatasetReference(bq_client.project, DATASET_ID)
    try:
        bq_client.get_dataset(ds_ref)
    except Exception:
        logger.info("Dataset ga4_raw absent—creating.")
        bq_client.create_dataset(bigquery.Dataset(ds_ref), exists_ok=True)  # docs show exists_ok pattern :contentReference[oaicite:0]{index=0}

def _load_config(autodetect=True):
    job_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,        # fixes embedded‑JSON rows :contentReference[oaicite:1]{index=1}
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # append pattern :contentReference[oaicite:2]{index=2}
        autodetect=autodetect,
        hive_partitioning_options=bigquery.external_config.HivePartitioningOptions(
            source_uri_prefix=f"gs://{BUCKET_NAME}/{RAW_PREFIX}/",  # maps folder dates to partitions :contentReference[oaicite:3]{index=3}
            mode="AUTO"
        ) if not autodetect else None
    )
    if not autodetect:                           # second run – lock explicit schema
        with open(CONTRACT_JSON, "r") as fp:
            job_cfg.schema = [bigquery.SchemaField(col["name"], col["type"])
                              for col in json.load(fp)]
    return job_cfg

def load_to_bq(gcs_uri: str):
    _ensure_dataset()
    table_ref = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"
    table_exists = True
    try:
        bq_client.get_table(table_ref)
    except Exception:
        table_exists = False

    job_cfg = _load_config(autodetect=not table_exists)
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_cfg)
    load_job.result()  # blocks until done

    # Capture schema after first load for drift checks
    if not table_exists:
        schema_json = [{"name": f.name, "type": f.field_type} for f in load_job.schema]
        with open(CONTRACT_JSON, "w") as fp:
            json.dump(schema_json, fp, indent=2)
        logger.info("Initial schema captured to docs/ga4_csv_schema.json")