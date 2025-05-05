# Run‑book — GA4 Daily Ingestion Pipeline

>- *Document owner*: **Mozaffar Kazemi** – Intern Candidate
>- *Last updated*: 05-05-2025

---

## 1 Purpose

Quick, step‑by‑step instructions for SREs or data engineers to re‑run or troubleshoot the GA4 daily ingestion job when automation fails.

---

## 2 Copy a historical file (backfill)

1. **Identify the date prefix** you want, e.g. `2025/04/30`.
2. **Copy the root object generation** (if object‑versioning) *or* the archived file from outside source:

   ```bash
   gsutil cp \
     gs://platform_assignment_bucket/backups/2025-04-30_ga4_public_dataset.csv \
     gs://platform_assignment_bucket/ga4_raw/2025/04/30/ga4_public_dataset.csv
   ```
3. **Trigger the Cloud Function manually** to replay load:

   ```bash
   curl -X POST "https://REGION-PROJECT.cloudfunctions.net/mozaffar_kazemi_ingest"
   ```

---

## 3 Replay a BigQuery load job

1. Find the failing job ID in **BigQuery › Job History** or Cloud Logging.
2. Re‑execute with the saved GCS URI:

   ```bash
   bq --location=europe-west4 load \
     --source_format=CSV \
     --allow_quoted_newlines=true \
     --autodetect=false \
     crystalloids-candidates:ga4_raw.daily_events \
     gs://platform_assignment_bucket/ga4_raw/2025/04/30/ga4_public_dataset.csv
   ```
3. Confirm rows appended with:

   ```bash
   bq query "SELECT COUNT(*) FROM ga4_raw.daily_events WHERE _PARTITIONDATE='2025-04-30'"
   ```

---

## 4 Interpret structured‑log entries

| Severity    | Meaning                                       | Immediate Action                                              |
| ----------- | --------------------------------------------- | ------------------------------------------------------------- |
| **INFO**    | Successful step, e.g., `Ingest step finished` | No action.                                                    |
| **WARNING** | Non‑fatal anomaly (size delta > 5 %)          | Review size variance; monitor dashboards.                     |
| **ERROR**   | Schema drift or size delta > 20 %             | Run § 2 or § 3 to backfill; open incident ticket if repeated. |

Log filter example:

```text
resource.type="cloud_function"
logName="projects/crystalloids-candidates/logs/ga4-ingest"
labels.component="ingest" AND severity>=ERROR
```

Create an alerting policy on that filter to page the data‑ops channel.

---

## 5 References

* [Dataplex Auto‑DQ row‑level threshold guidance (95 %)](https://cloud.google.com/dataplex/docs/auto-data-quality-overview)
* [BigQuery CSV load flags (`allow_quoted_newlines`)](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv?utm_source=chatgpt.com)
* [Hive partition external tables](https://cloud.google.com/bigquery/docs/hive-partitioned-queries?utm_source=chatgpt.com)
