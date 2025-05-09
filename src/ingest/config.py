import re
import os
from google.cloud import bigquery, storage

# Cloud Storage
BUCKET_NAME = os.getenv("BUCKET_NAME", "platform_assignment_bucket")
RAW_PREFIX = os.getenv("RAW_PREFIX", "ga4_raw")
FILE_NAME = os.getenv("FILE_NAME", "ga4_public_dataset.csv")
CONTRACT_BLOB = os.getenv("CONTRACT_BLOB", "contracts/Mozaffar_Kazemi_GA4Schema.json")

# BigQuery
DATASET_ID = os.getenv("DATASET_ID", "Mozaffar_Kazemi_GA4Raw")
TABLE_ID = os.getenv("TABLE_ID", "Mozaffar_Kazemi_DailyEvents")

# Header regex for CSV validation
HEADER_REGEX = re.compile(r"^([A-Za-z0-9_]+,)+[A-Za-z0-9_]+$")

# GC clients
bq_client = bigquery.Client()
storage_client = storage.Client()
