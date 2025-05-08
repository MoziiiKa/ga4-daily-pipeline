# Future Work & Potential Improvements

Below are three high-impact areas to extend and harden the GA4 daily-ingest pipeline:

---

## 1. Full CI/CD with Cloud Build  
- **Why:** Currently deployments (Functions, Workflows, Scheduler) are manual CLI commands.  
- **How:**  
  - Install the Cloud Build GitHub App and create a `cloudbuild.yaml` in your repo root.  
  - Define build steps to lint (ruff), format (black), run tests (pytest), then deploy your Function, Workflow, and Scheduler via `gcloud`.  
  - Wire triggers on pushes to `main` and PR merges for hands-off, auditable delivery.  

---

## 2. Terraform‐First Infrastructure  
- **Why:** Only the landing bucket is in Terraform today—other resources (BigQuery datasets/tables, Function, Workflow, Scheduler, IAM bindings, alert policies) remain manual.  
- **How:**  
  - Author `*.tf` modules for each service: `google_cloudfunctions2_function`, `google_workflows_workflow`, `google_cloud_scheduler_job`, `google_bigquery_dataset`/`table`/`view`, and all necessary IAM roles.  
  - Manage `contracts/` JSON and seed objects with `google_storage_bucket_object`.  
  - Use Terraform workspaces or modules to deploy identical pipelines across dev/stage/prod.

---

## 3. Data Quality & Observability  
- **Why:** Right now we enforce header and size-delta rules in code, but lack a centralized data-quality dashboard or automated alerts on schema drift beyond basic logs.  
- **How:**  
  - Integrate **Dataplex** or **Great Expectations** to define and run cell- and row-level expectations (e.g. non-null user_id, geo fields within allowed set), storing results in BigQuery or Cloud Monitoring.  
  - Publish data-quality metrics via **Cloud Monitoring** dashboards and set alert policies (e.g. percent-of-failed expectations > X).  
  - Optionally push metrics to **Looker** or **Looker Studio** for cross-team visibility.

---
