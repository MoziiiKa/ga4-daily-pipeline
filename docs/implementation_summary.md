# Summarizing implemented actions

Here’s the **complete, chronological run-book** of everything I’ve done—step by step, from an empty project to a fully automated, tested, documented GA4 pipeline running at 07:00 CET every day.

---

1. **Confirm sandbox & quotas**

   * Verified access to `crystalloids-candidates` and that quotas for Cloud Functions v2, Cloud Scheduler, Workflows, BigQuery, Cloud Build, and Cloud Storage in `europe-west4` were sufficient.

2. **Bootstrap IAM (least privilege)**

   * Added **roles/iam.serviceAccountUser** on `platform-terraform-service-acc@…` so you can deploy functions.
   * Granted **roles/workflows.admin** and **roles/workflows.invoker** for Cloud Workflows.
   * Granted **roles/cloudscheduler.admin** for Scheduler jobs.
   * Granted **roles/iam.serviceAccountTokenCreator** on that SA so Scheduler can mint OAuth tokens.
   * Granted **roles/logging.viewer** (“Logs Viewer”) and **roles/monitoring.alertPolicyEditor** so you can inspect logs and set alerts.
   * Granted **roles/cloudbuild.serviceAccount** to the Cloud Build SA for Function builds.

3. **Enable required APIs**

   ```bash
   gcloud services enable \
     storage.googleapis.com \
     cloudfunctions.googleapis.com \
     workflows.googleapis.com \
     cloudscheduler.googleapis.com \
     cloudbuild.googleapis.com \
     bigquery.googleapis.com \
     monitoring.googleapis.com
   ```

4. **Initialize GitHub repo**

   * Created **`ga4-daily-pipeline`** on GitHub, default branch `main`, PR reviews required.
   * Cloned into Cloud Shell: `git clone ... && cd ga4-daily-pipeline`.

5. **Document file contract**

   * **Created `docs/incoming-files.md`**, adding:

     * Regex for `ga4_raw/\d{4}/\d{2}/\d{2}/ga4_public_dataset\.csv`
     * Loader flags (`CSV`, `allow_quoted_newlines`, `WRITE_APPEND`, `autodetect`)
     * **Warn/fail** volume thresholds (± 5 % / ± 20 %) with Dataplex & Great Expectations citations.

6. **Write run-book**

   * **Created `docs/runbook_ingest.md`** with backfill steps, BQ-replay commands, and structured-log interpretation.

7. **Scaffold ingestion function**

   * Made `src/ingest/` directory with:

     * `main.py` (entry point)
     * `common.py` (structured `_log()` helper)
     * `bq_loader.py` (BQ load helper)
     * `requirements.txt` pinning `google-cloud-storage`, `google-cloud-logging`, `google-cloud-bigquery`, `python-dateutil`, `functions-framework`.

8. **Implement `main.py`**

   * **List** all objects & log name/size.
   * **Compute** “bucket last updated” via `max(blob.updated)`.
   * **Build** date-partitioned key `_build_target_path()`.
   * **Idempotency**: skip if target exists.
   * **Copy** `ga4_public_dataset.csv` into `ga4_raw/YYYY/MM/DD/`.
   * **Header validation** via `_header_matches_contract()`.
   * **Volume heuristic** comparing `blob.size` vs yesterday’s (± 5 % warn, ± 20 % abort).
   * **Call** `load_to_bq(gcs://…)`, then return HTTP 200 on success or raise on error.

9. **Implement `bq_loader.py`**

   * **\_load\_contract\_columns()** reads `contracts/Mozaffar_Kazemi_GA4Schema.json` from GCS.
   * **\_save\_contract\_from\_table()** snapshots table.schema into that same blob on first run.
   * **\_ensure\_dataset()** creates `Mozaffar_Kazemi_GA4Raw` if missing.
   * **\_load\_config()** builds `LoadJobConfig` with `skip_leading_rows=1`, CSV flags, and explicit schema from contract.
   * **load\_to\_bq()** runs the load, waits, and on first run saves the contract.

10. **Unit tests & local CI**

    * Created `tests/` with:

      * `test_prefix.py` for `_build_target_path()`
      * `test_schema.py` for `_header_matches_contract()` (monkey-patching CONTRACT\_COLUMNS)
      * `test_bqconfig.py` for `_load_config()` flags.
    * Wrote `scripts/local_verify.sh` running `ruff`, `black --check`, `pytest`.
    * Installed a Git **pre-push** hook calling that script.

11. **Deploy Cloud Function v2**

    ```bash
    gcloud functions deploy mozaffar_kazemi_ingest \
      --gen2 --runtime=python312 --region=europe-west4 \
      --source=src/ingest --entry-point=main \
      --service-account=platform-terraform-service-acc@crystalloids-candidates.iam.gserviceaccount.com \
      --min-instances=0 --trigger-http --allow-unauthenticated
    ```

    * Manually invoked with `curl`/`gcloud functions call` until `✅ Loaded to BigQuery` appeared.

12. **Verify raw load in BigQuery**

    * Ran `bq show --schema --format=prettyjson …Ga4Raw.DailyEvents`.
    * Queried row counts, schema, etc.

13. **Build analytics layer**

    * **Created dataset**:

      ```bash
      bq --location=europe-west4 mk --dataset Mozaffar_Kazemi_GA4Model
      ```
    * **Authored 3 views** in `sql/views/`:

      1. `v_descriptive.sql` using `PARSE_DATE` on INT64 `event_date`
      2. `v_page_metrics.sql` extracting `page_title` via `JSON_EXTRACT_ARRAY(..., '$.event_params')` and setting `page_view_count=1`
      3. `v_top_page_titles.sql` aggregating and ranking by sum(page\_view\_count)
    * **Deployed** them via:

      ```bash
      bq query --location=europe-west4 --use_legacy_sql=false < sql/views/v_*.sql
      ```
    * **Validated** with `bq query 'SELECT * FROM … LIMIT 10'` for each view.

14. **Orchestrate with Workflows**

    * **Created** `workflows/ingest_workflow.yaml` with a `try`/`retry` block, using `sys.get_env("INGEST_URL")`.
    * **Deployed**:

      ```bash
      gcloud workflows deploy Mozaffar_Kazemi_Workflow \
        --location=europe-west4 \
        --source=workflows/ingest_workflow.yaml \
        --set-env-vars INGEST_URL=https://europe-west4-crystalloids-candidates.cloudfunctions.net/mozaffar_kazemi_ingest
      ```

15. **Schedule daily run at 07:00 CET**

    ```bash
    gcloud scheduler jobs create http Mozaffar_Kazemi_Schedule \
      --location=europe-west1 \
      --schedule="0 7 * * *" \
      --time-zone="Europe/Amsterdam" \
      --http-method=POST \
      --uri="https://workflowexecutions.googleapis.com/v1/projects/crystalloids-candidates/locations/europe-west4/workflows/Mozaffar_Kazemi_Workflow/executions" \
      --oauth-service-account-email="crystalloids-candidates@appspot.gserviceaccount.com" \
      --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
    ```

    * Verified in the Scheduler UI that **Next run** = tomorrow 07:00 CET.
    * Forced a run to confirm **200** and a new Workflows execution.

16. **Final verification**

    * **Scheduler UI** shows `Mozaffar_Kazemi_Schedule` in **europe-west1**.
    * **Workflows UI** shows `Mozaffar_Kazemi_Workflow` **ACTIVE** with environment var.
    * **Workflows Executions** tab logs the `call_ingest` step, structured logs, and no errors.
    * **BQ UI** confirms fresh rows in `Mozaffar_Kazemi_GA4Raw.DailyEvents` and that the three modelling views return expected data.

---