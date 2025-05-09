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
from datetime import datetime, timedelta, timezone

import functions_framework

from .bq_loader import load_to_bq
from .common import _log

from .config import (
    BUCKET_NAME,
    RAW_PREFIX,
    FILE_NAME,
    CONTRACT_BLOB,
    storage_client,
    HEADER_REGEX,
)


# ---------------------------------------------------------------------
# CONTRACT_COLUMNS: define only if not already defined (for test overrides)
# ---------------------------------------------------------------------

if "CONTRACT_COLUMNS" not in globals():
    CONTRACT_COLUMNS = None

# Load schema JSON from GCS if CONTRACT_COLUMNS isn't provided (production)
if not isinstance(CONTRACT_COLUMNS, list):
    blob = storage_client.bucket(BUCKET_NAME).blob(CONTRACT_BLOB)
    data = blob.download_as_bytes()
    CONTRACT_COLUMNS = [c["name"] for c in json.loads(data)]


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------


def _build_target_path() -> str:
    """
    Build the GCS target path for today's file under RAW_PREFIX.
    Format: {RAW_PREFIX}/YYYY/MM/DD/{FILE_NAME}
    """
    today = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    return f"{RAW_PREFIX}/{today}/{FILE_NAME}"


def _header_matches_contract(header: str, *, columns=None):
    """
    Returns True if `header` (a CSV header line) exactly matches
    the contract columns. Raises ValueError on mismatch.
    """
    # Validate basic format
    if not HEADER_REGEX.match(header):
        raise ValueError(f"Invalid header format: {header}")

    # Load or use provided schema columns
    if columns is None:
        columns = CONTRACT_COLUMNS

    header_cols = header.split(",")
    if header_cols != columns:
        raise ValueError(f"Header columns {header_cols} do not match schema {columns}")
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
    _log(f"➖ Bucket last updated: {max(b.updated for b in blob_list)}", "INFO")

    # 2 — copy today’s file into date partition
    target_path = _build_target_path()
    target_blob = bucket.blob(target_path)

    if not target_blob.exists():
        source_blob = bucket.blob(FILE_NAME)
        if not source_blob.exists():
            _log("🚨 Source file missing", "ERROR")
            raise RuntimeError("No new file in drop zone")

        bucket.copy_blob(source_blob, bucket, new_name=target_path)
        _log(f"➖ Copied {FILE_NAME} ➜ {target_path}", "INFO")
    else:
        _log(f"➖ {target_path} already exists — skipping copy, continuing", "INFO")

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

    if current_size is None:
        target_blob.reload()
        current_size = target_blob.size or 0

    if prev_size is None:
        prev_size = current_size

    delta_pct = abs(current_size - prev_size) / prev_size * 100 if prev_size else 0

    _log(f"➖ Size delta vs. yesterday: {delta_pct:.1f} %", "INFO")

    if delta_pct > 20:
        _log("Variance > 20 % — aborting", "ERROR")
        raise RuntimeError("Abnormal file size")
    elif delta_pct > 5:
        _log("Variance > 5 % — continuing with warning", "WARNING")

    # 5 — load to BigQuery
    load_to_bq(f"gs://{BUCKET_NAME}/{target_path}")
    _log("✅ Loaded to BigQuery", "INFO")

    return "Loaded to BigQuery successfuly!", 200
