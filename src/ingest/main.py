"""Cloud Function v2 entry-point: GA4 daily ingest → BigQuery.

Responsibilities
‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
1.  List bucket objects; log names, sizes, last-updated timestamp.
2.  Copy today's drop-zone file into a date-prefixed folder.
3.  Validate header against contract + quick volume heuristic.
4.  Load to BigQuery (helper `load_to_bq`).
5.  Emit structured JSON logs throughout.
"""

import json
import re
from datetime import datetime, timedelta, timezone

import functions_framework
from google.cloud import storage

from .bq_loader import load_to_bq
from .common import _log

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
BUCKET_NAME = "platform_assignment_bucket"
RAW_PREFIX = "ga4_raw"
FILE_NAME = "ga4_public_dataset.csv"

storage_client = storage.Client()

# ---------------------------------------------------------------------
# Contract columns pulled from GCS
# ---------------------------------------------------------------------
schema_blob = storage_client.bucket(BUCKET_NAME).blob(
    "contracts/Mozaffar_Kazemi_GA4Schema.json"
)
CONTRACT_COLUMNS = [c["name"] for c in json.loads(schema_blob.download_as_bytes())]

HEADER_REGEX = re.compile(r"^([A-Za-z0-9_]+,)+[A-Za-z0-9_]+$")


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------
def _build_target_path() -> str:
    today = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    return f"{RAW_PREFIX}/{today}/{FILE_NAME}"


def _header_matches_contract(header_line: str) -> bool:
    if not HEADER_REGEX.match(header_line):
        raise ValueError("Header not comma‑delimited or contains invalid chars")

    if header_line.split(",") != CONTRACT_COLUMNS:
        raise ValueError("Schema drift detected")
    return True


# ---------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------
@functions_framework.http
def main(request):
    bucket = storage_client.bucket(BUCKET_NAME)

    # 1 — list objects & freshness
    blob_list = list(bucket.list_blobs())
    for b in blob_list:
        _log(f"{b.name} — {b.size} bytes", "INFO")
    _log(f"Bucket last updated: {max(b.updated for b in blob_list)}", "INFO")

    # 2 — copy today’s file into date partition
    target_path = _build_target_path()
    target_blob = bucket.blob(target_path)

    if target_blob.exists():
        _log(f"{target_path} already exists — aborting copy", "WARNING")
        return "Exists", 200

    source_blob = bucket.blob(FILE_NAME)
    if not source_blob.exists():
        _log("Source file missing", "ERROR")
        raise RuntimeError("No new file in drop zone")

    bucket.copy_blob(source_blob, bucket, new_name=target_path)
    _log(f"Copied {FILE_NAME} ➜ {target_path}", "INFO")

    # 3 — header validation
    header_line = target_blob.download_as_text().splitlines()[0]
    _header_matches_contract(header_line)
    _log("✅ Header matches contract", "INFO")

    # 4 — volume heuristic
    current_size = target_blob.size
    yesterday_prefix = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime(
        "%Y/%m/%d"
    )
    prev_blob = bucket.blob(f"{RAW_PREFIX}/{yesterday_prefix}/{FILE_NAME}")
    prev_size = prev_blob.size if prev_blob.exists() else current_size

    delta_pct = abs(current_size - prev_size) / prev_size * 100
    _log(f"Size delta vs. yesterday: {delta_pct:.1f} %", "INFO")

    if delta_pct > 20:
        _log("Variance > 20 % — aborting", "ERROR")
        raise RuntimeError("Abnormal file size")
    elif delta_pct > 5:
        _log("Variance > 5 % — continuing with warning", "WARNING")

    # 5 — load to BigQuery
    load_to_bq(f"gs://{BUCKET_NAME}/{target_path}")
    _log("✅ Loaded to BigQuery", "INFO")

    return "OK", 200

    # # Return the path for downstream steps
    # return {"gcs_uri": f"gs://{BUCKET_NAME}/{target_path}"}, 200
