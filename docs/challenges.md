# Top 5 Challenges & Solutions

Below are the principal roadblocks I encountered while building the GA4 daily-ingest pipeline—and how I overcame each one.

---

## 1. Insufficient IAM Permissions  
**Challenge:**  
Initial deployments of the Cloud Function and Cloud Scheduler job failed with `iam.serviceAccounts.actAs` and 401 errors. I lacked the `roles/iam.serviceAccountUser`, `roles/iam.serviceAccountTokenCreator`, and scheduler-to-workflows invoker roles.  

**Solution:**  
Engaged the GCP admin to grant precisely:
- `roles/iam.serviceAccountUser` on our runtime SA,
- `roles/iam.serviceAccountTokenCreator` for OIDC/OAuth calls,
- `roles/workflows.admin` & `roles/workflows.invoker`,
- `roles/cloudscheduler.admin`.  
Once added, all deploys and test invocations succeeded.

---

## 2. Contract JSON Not Packaged in Function  
**Challenge:**  
The schema contract lived under `docs/ga4_csv_schema.json`, but Cloud Functions only packaged `src/ingest/`. At runtime the function threw `FileNotFoundError`.  

**Solution:**  
Moved the contract JSON into GCS (`gs://…/contracts/Mozaffar_Kazemi_GA4Schema.json`) and updated both `main.py` and `bq_loader.py` to download it at cold-start. This decoupled code vs. data and removed packaging issues.

---

## 3. Parsing Nested GA4 JSON in BigQuery  
**Challenge:**  
GA4’s `event_params` field is a JSON‐encoded string containing an array under the `"event_params"` key. My first `UNNEST(JSON_EXTRACT_ARRAY(event_params, '$'))` returned empty arrays, then extracting `page_view` as an `int` produced all NULLs.  

**Solution:**  
Switched to:
```sql
UNNEST(JSON_EXTRACT_ARRAY(event_params, '$.event_params')) AS param
````

and used `JSON_EXTRACT_SCALAR(param, '$.value.string_value')` for `page_title`, assigning `1` per row for `page_view_count`. This correctly flattened the nested array.

---

## 4. Casting GA4 Dates from INT64 → DATE

**Challenge:**
I initially referenced `_PARTITIONDATE`, which failed because my table wasn’t partitioned. Switching to `event_date` and doing `CAST(event_date AS DATE)` errored with “Invalid cast from INT64 to DATE.”

**Solution:**
Used:

```sql
PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS event_date
```

to convert the GA4 integer format `YYYYMMDD` into a proper `DATE`. This enabled grouping and filtering by real dates.

---

## 5. Workflows YAML Syntax & Environment Variable Binding

**Challenge:**
My first `workflows/ingest_workflow.yaml` parse-failed on an unsupported `retry:` block under `call:`, then choked on `${INGEST_URL}` as an undefined symbol.

**Solution:**
Refactored to use a `try:` wrapper with `retry:` in **snake\_case**, and accessed the function URL using:

```yaml
url: ${sys.get_env("INGEST_URL")}
```

At deploy time I set the env var via:

```bash
--set-env-vars INGEST_URL=https://…/mozaffar_kazemi_ingest
```

This combination passed validation and now reliably invokes the function with retries.

---
