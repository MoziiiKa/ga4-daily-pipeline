import os

# Cloud Storage
BUCKET_NAME = os.getenv("BUCKET_NAME", "platform_assignment_bucket")
RAW_PREFIX = os.getenv("RAW_PREFIX", "ga4_raw")
FILE_NAME = os.getenv("FILE_NAME", "ga4_public_dataset.csv")
CONTRACT_BLOB = os.getenv("CONTRACT_BLOB", "contracts/Mozaffar_Kazemi_GA4Schema.json")

# BigQuery
DATASET_ID = os.getenv("DATASET_ID", "Mozaffar_Kazemi_GA4Raw")
TABLE_ID = os.getenv("TABLE_ID", "Mozaffar_Kazemi_DailyEvents")
