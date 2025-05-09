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
# Lazy-loaded contract schema columns
# ---------------------------------------------------------------------

CONTRACT_COLUMNS = None


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------


def _load_contract_columns() -> list[str]:
    """Fetch the JSON schema from GCS and return a list of column names."""
    blob = storage_client.bucket(BUCKET_NAME).blob(CONTRACT_BLOB)
    data = blob.download_as_bytes()
    return [c['name'] for c in json.loads(data)]

def _build_target_path() -> str:
    """
    Build the GCS target path for today's file under RAW_PREFIX.
    Format: {RAW_PREFIX}/YYYY/MM/DD/{FILE_NAME}
    """
    today = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")
    return f"{RAW_PREFIX}/{today}/{FILE_NAME}"


def _header_matches_contract(header: str, *, columns=None) -> bool:
    """
    Check the CSV header line against expected contract columns.
    Raises ValueError if format or schema mismatch.
    """
    # 1. Validate basic CSV header format
    if not HEADER_REGEX.match(header):
        raise ValueError(f"Invalid header format: {header}")

    # 2. Determine schema columns
    global CONTRACT_COLUMNS
    if columns is None:
        if CONTRACT_COLUMNS is None:
            CONTRACT_COLUMNS = _load_contract_columns()
        columns = CONTRACT_COLUMNS

    # 3. Compare header names
    header_cols = header.split(",")
    if header_cols != columns:
        raise ValueError(
            f"Header columns {header_cols} do not match schema {columns}"
        )
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
