import os
import json, pathlib
import functions_framework
from datetime import datetime, timedelta, timezone
from google.cloud import storage, logging as gcp_logging
from google.cloud import logging as gcp_logging

from .bq_loader import load_to_bq

# Improve logging
log_client = gcp_logging.Client()
log_handler = log_client.get_default_handler()
logger = log_client.get_logger("ga4-ingest")   # structured logger

def _log(msg: str, severity: str = "INFO"):
    logger.log_struct(
        {"message": msg},
        severity=severity,
        labels={
            "component": "ingest",
            "day": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
        },
    )


BUCKET_NAME = "platform_assignment_bucket"
RAW_PREFIX  = "ga4_raw"
FILE_NAME   = "ga4_public_dataset.csv"

storage_client = storage.Client()
log_client     = gcp_logging.Client()
logger         = log_client.logger("ingest")

@functions_framework.http
def main(request):
    # List every object and size
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_list = list(bucket.list_blobs())          # returns Iterable[Blob]
    for b in blob_list:
        _log(f"{b.name} — {b.size} bytes", severity="INFO")
    
    # Compute & log the bucket’s “last‑updated” timestamp
    last_update = max(b.updated for b in blob_list)
    _log(f"Bucket last updated: {last_update.isoformat()}", severity="INFO")

    # Build today’s prefix and target path (pre‑copy logic)
    today = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    target_path = f"{RAW_PREFIX}/{today}/{FILE_NAME}"
    target_blob = bucket.blob(target_path)

    # Read the contract columns
    CONTRACT_PATH = pathlib.Path(__file__).parents[2] / "docs" / "ga4_csv_schema.json"
    with open(CONTRACT_PATH, "r", encoding="utf‑8") as fp:
        CONTRACT_COLUMNS = [c["name"] for c in json.load(fp)]

    # Idempotency check
    if target_blob.exists():
        _log(f"{target_path} already exists — aborting copy", severity="WARNING")
        return "Exists", 200

    # Locate today’s drop file and copy it
    source_blob = bucket.blob(FILE_NAME)           # root‑level drop zone
    if not source_blob.exists():
        _log("No new file in drop zone", severity="ERROR")
        raise RuntimeError("Source file missing")

    source_blob.copy_to_bucket(bucket, new_name=target_path)  # physical copy
    # source_blob.delete()                                      # clean drop zone
    _log(f"Copied {FILE_NAME} to {target_path}", severity="INFO")

    # Download only the header row and do validation
    header_line = target_blob.download_as_text(max_bytes=4096).splitlines()[0]
    file_columns = header_line.split(",")

    if file_columns != CONTRACT_COLUMNS:
        _log("❌ Schema drift detected", severity="ERROR")
        raise RuntimeError("Column mismatch in daily CSV")
    _log("✅ Header matches contract", severity="INFO")

    # Quick volume sanity using blob.size
    SIZE_CACHE_KEY = f"size/{today}"                 # simple in‑function cache
    current_size = target_blob.size                  # bytes

    # Retrieve yesterday’s size from the same prefix (if present)
    yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y/%m/%d")
    prev_blob = bucket.blob(f"{RAW_PREFIX}/{yesterday}/{FILE_NAME}")
    prev_size = prev_blob.size if prev_blob.exists() else current_size

    delta_pct = abs(current_size - prev_size) / prev_size * 100
    _log(f"Size delta vs. yesterday: {delta_pct:.1f} %", severity="INFO")

    if delta_pct > 20:
        _log("❌ File size variance > 20 % — abort", severity="ERROR")
        raise RuntimeError("Abnormal file size; load stopped")
    elif delta_pct > 5:
        _log("⚠️ File size variance > 5 % — continue with warning", severity="WARNING")

    gcs_uri = f"gs://{BUCKET_NAME}/{target_path}"
    load_to_bq(gcs_uri)
    _log(f"✅ Loaded to BigQuery {gcs_uri}", severity="INFO")


    _log("Ingest step finished", "INFO")
    return ("OK", 200)        # Cloud Functions HTTP response

    # # Return the path for downstream steps
    # return {"gcs_uri": f"gs://{BUCKET_NAME}/{target_path}"}, 200