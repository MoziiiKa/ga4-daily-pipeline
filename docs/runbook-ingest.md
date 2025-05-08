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
   curl \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   https://europe-west4-crystalloids-candidates.cloudfunctions.net/mozaffar_kazemi_ingest
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





------------------------------


# Run‑book — GA4 Daily Ingestion Pipeline

> * *Document owner*: **Mozaffar Kazemi** – Intern Candidate
> * *Last updated*: 2025-05-08

---

## 1 Purpose

Quick, step‑by‑step instructions for SREs or data engineers to re‑run or troubleshoot the GA4 daily ingestion pipeline when automation fails.

---

## 2 Backfill a Historical File

1. **Identify** the target date prefix, e.g., `2025/04/30`.
2. **Copy** the archived CSV into the partitioned drop zone:

   ```bash
   gsutil cp \
     gs://platform_assignment_bucket/backups/2025-04-30_ga4_public_dataset.csv \
     gs://platform_assignment_bucket/ga4_raw/2025/04/30/ga4_public_dataset.csv
   ```
3. **Invoke** the ingestion function to replay the pipeline:

   ```bash
   curl -X POST $INGEST_URL \
     -H "Authorization: Bearer $(gcloud auth print-identity-token)"
   ```

   > `$INGEST_URL` is set in `docs/schedule.md` under Environment Variables.

---

## 3 Replay a BigQuery Load Job

1. **Locate** the failed load job ID in **BigQuery › Job History** or in Cloud Logging logs.
2. **Rerun** the load with the contract schema locked:

   ```bash
   bq --location=europe-west4 load \
     --source_format=CSV \
     --allow_quoted_newlines=true \
     --autodetect=false \
     crystalloids-candidates:Mozaffar_Kazemi_GA4Raw.Mozaffar_Kazemi_DailyEvents \
     gs://platform_assignment_bucket/ga4_raw/2025/04/30/ga4_public_dataset.csv
   ```
3. **Verify** appended rows for that partition:

   ```bash
   bq query --nouse_legacy_sql \
     'SELECT COUNT(*) AS cnt \
      FROM `crystalloids-candidates.Mozaffar_Kazemi_GA4Raw.Mozaffar_Kazemi_DailyEvents` \
      WHERE PARSE_DATE("%Y/%m/%d","2025/04/30") = event_date'
   ```

---

## 4 Manual Workflow Execution

To rerun the full DAG (ingest → load → notifications):

```bash
# Using gcloud CLI
gcloud workflows run Mozaffar_Kazemi_Workflow \
  --location=europe-west4
```

Or in the console: go to **Workflows → Mozaffar\_Kazemi\_Workflow → Execute**.

---

## 5 Interpret Structured Logs

| Severity    | Description                                         | Action                                                               |
| ----------- | --------------------------------------------------- | -------------------------------------------------------------------- |
| **INFO**    | Informational events (e.g., `Ingest step finished`) | No action needed.                                                    |
| **WARNING** | Non-fatal anomalies (size delta > 5 %)              | Review size variance; confirm ingestion correctness.                 |
| **ERROR**   | Fatal errors (schema drift or size delta > 20 %)    | Run Sections 2–3 to backfill or replay; open incident if persisting. |

**Example Log Filter**:

```text
resource.type="cloud_function"
labels.component="ingest"
severity>=ERROR
```

Use this filter to create alert policies in Cloud Monitoring.

---

## 6 Verify IAM & Permissions

* **Scheduler SA** (`crystalloids-candidates@appspot.gserviceaccount.com`):

  * `roles/workflows.invoker` on `Mozaffar_Kazemi_Workflow`
  * `roles/iam.serviceAccountTokenCreator` on itself
* **Your User**:

  * `roles/workflows.admin`
  * (Optional) `roles/workflows.invoker` for manual runs

---

## 7 Change Management

* **Update schedule**:

  ```bash
  gcloud scheduler jobs update http Mozaffar_Kazemi_Schedule --schedule="<new-cron>"
  ```


* **Workflow revisions**: edit `workflows/ingest_workflow.yaml` then:
  
   ```bash
   gcloud workflows deploy Mozaffar_Kazemi_Workflow \
   --location=europe-west4 \
   --source=workflows/ingest_workflow.yaml
   ````

* **Schema contract updates**: overwrite schema JSON in GCS:

  ```bash
  gsutil cp docs/ga4_csv_schema.json \
    gs://platform_assignment_bucket/contracts/Mozaffar_Kazemi_GA4Schema.json
  ```

---

## 8 References

* [Dataplex Auto‑DQ row‑level threshold guidance (95 %)](https://cloud.google.com/dataplex/docs/auto-data-quality-overview)
* [BigQuery CSV load flags (`allow_quoted_newlines`)](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv?utm_source=chatgpt.com)
* [Hive partition external tables](https://cloud.google.com/bigquery/docs/hive-partitioned-queries?utm_source=chatgpt.com)
