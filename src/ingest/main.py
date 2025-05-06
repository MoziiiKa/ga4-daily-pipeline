import os
import functions_framework
from datetime import datetime, timezone
from google.cloud import storage, logging as gcp_logging

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
        logger.log_text(f"{b.name} — {b.size} bytes", severity="INFO")
    
    # Compute & log the bucket’s “last‑updated” timestamp
    last_update = max(b.updated for b in blob_list)
    logger.log_text(f"Bucket last updated: {last_update.isoformat()}", severity="INFO")

    # Build today’s prefix and target path (pre‑copy logic)
    today = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    target_path = f"{RAW_PREFIX}/{today}/{FILE_NAME}"
    target_blob = bucket.blob(target_path)

    # Idempotency check
    if target_blob.exists():
        logger.log_text(f"{target_path} already exists — aborting copy", severity="WARNING")
        return "Exists", 200

    # Locate today’s drop file and copy it
    source_blob = bucket.blob(FILE_NAME)           # root‑level drop zone
    if not source_blob.exists():
        logger.log_text("No new file in drop zone", severity="ERROR")
        raise RuntimeError("Source file missing")

    source_blob.copy_to_bucket(bucket, new_name=target_path)  # physical copy
    # source_blob.delete()                                      # clean drop zone
    logger.log_text(f"Copied {FILE_NAME} to {target_path}", severity="INFO")

    # Return the path for downstream steps
    return {"gcs_uri": f"gs://{BUCKET_NAME}/{target_path}"}, 200