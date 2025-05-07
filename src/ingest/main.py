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
from .common import _log  # structured logger helper

# ---------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------
BUCKET_NAME = "platform_assignment_bucket"
RAW_PREFIX = "ga4_raw"
FILE_NAME = "ga4_public_dataset.csv"

storage_client = storage.Client()


# def _find_contract_file() -> Path:
#     """Walk up until we find docs/ga4_csv_schema.json, then return its path."""
#     cur = Path(__file__).resolve()
#     for parent in cur.parents:
#         candidate = parent / "docs" / "ga4_csv_schema.json"
#         if candidate.exists():
#             return candidate
#     raise FileNotFoundError("ga4_csv_schema.json not found in parent tree")


# ---- contract (22 columns) ------------------------------------------
# CONTRACT_PATH = _find_contract_file()
# with open(CONTRACT_PATH, "r", encoding="utf-8") as fp:
#     CONTRACT_COLUMNS = [c["name"] for c in json.load(fp)]

schema_blob = storage_client.bucket(BUCKET_NAME).blob(
    "contracts/Mozaffar_Kazemi_GA4Schema.json"
)
CONTRACT_COLUMNS = [c["name"] for c in json.loads(schema_blob.download_as_bytes())]

HEADER_REGEX = re.compile(r"^([A-Za-z0-9_]+,)+[A-Za-z0-9_]+$")


def _build_target_path() -> str:
    """Return partitioned key: ga4_raw/YYYY/MM/DD/ga4_public_dataset.csv"""
    today = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    return f"{RAW_PREFIX}/{today}/{FILE_NAME}"


def _header_matches_contract(header_line: str) -> bool:
    """Raise ValueError on drift; return True when header is valid."""
    if not HEADER_REGEX.match(header_line):
        raise ValueError("Header not comma-delimited or contains invalid chars")

    cols = header_line.split(",")
    if cols != CONTRACT_COLUMNS:
        raise ValueError("Schema drift detected")
    return True


# ---------------------------------------------------------------------
# Cloud Function entry‑point
# ---------------------------------------------------------------------
@functions_framework.http
def main(request):  # noqa: D401
    bucket = storage_client.bucket(BUCKET_NAME)

    # 1 — List objects & last‑updated
    blob_list = list(bucket.list_blobs())
    for b in blob_list:
        _log(f"{b.name} — {b.size} bytes", "INFO")

    last_update = max(b.updated for b in blob_list)
    _log(f"Bucket last updated: {last_update.isoformat()}", "INFO")

    # 2 — Copy today’s file to date prefix
    target_path = _build_target_path()
    target_blob = bucket.blob(target_path)

    if target_blob.exists():
        _log(f"{target_path} already exists — aborting copy", "WARNING")
        return "Exists", 200

    source_blob = bucket.blob(FILE_NAME)
    if not source_blob.exists():
        _log("No new file in drop zone", "ERROR")
        raise RuntimeError("Source file missing")

    bucket.copy_blob(source_blob, bucket, new_name=target_path)
    # source_blob.delete()
    _log(f"Copied {FILE_NAME} to {target_path}", "INFO")

    # 3 — Header validation
    header_line = target_blob.download_as_text(max_bytes=4096).splitlines()[0]
    try:
        _header_matches_contract(header_line)
        _log("✅ Header matches contract", "INFO")
    except ValueError as err:
        _log(str(err), "ERROR")
        raise

    # Quick volume heuristic (±5 % warn, ±20 % fail)
    current_size = target_blob.size
    yesterday_prefix = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime(
        "%Y/%m/%d"
    )
    prev_blob = bucket.blob(f"{RAW_PREFIX}/{yesterday_prefix}/{FILE_NAME}")
    prev_size = prev_blob.size if prev_blob.exists() else current_size

    delta_pct = abs(current_size - prev_size) / prev_size * 100
    _log(f"Size delta vs. yesterday: {delta_pct:.1f} %", "INFO")

    if delta_pct > 20:
        _log("❌ File size variance > 20 % — abort", "ERROR")
        raise RuntimeError("Abnormal file size; load stopped")
    elif delta_pct > 5:
        _log("⚠️ File size variance > 5 % — continue with warning", "WARNING")

    # 4 — Load to BigQuery
    gcs_uri = f"gs://{BUCKET_NAME}/{target_path}"
    load_to_bq(gcs_uri)
    _log(f"✅ Loaded to BigQuery {gcs_uri}", "INFO")

    # 5 — Success exit
    _log("Ingest step finished", "INFO")
    return "OK", 200

    # # Return the path for downstream steps
    # return {"gcs_uri": f"gs://{BUCKET_NAME}/{target_path}"}, 200
