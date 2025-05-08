# Workflow & Schedule

> - *Owner*: **Mozaffar Kazemi** – Intern Candidate
> - *Last updated*: 2025-05-08

---

## 1 Overview

This document describes how the automated GA4 ingestion pipeline is orchestrated and scheduled. It covers the Cloud Workflows definition, scheduling via Cloud Scheduler, security configurations, monitoring, and troubleshooting steps.

---

## 2 Workflow Definition

**Name**: `Mozaffar_Kazemi_Workflow`
**Location**: europe-west4
**Type**: Cloud Workflows (serverless orchestration)

### 2.1 Steps

1. **call\_ingest**

   * **Action**: HTTP POST to the Cloud Function trigger URL retrieved from environment variable `INGEST_URL`.
   * **Authentication**: OIDC token minted by the workflow service account.
   * **Retries**: Wrapped in a `try`/`retry` block with:

     * `max_retries: 5`
     * `backoff`:

       * `initial_delay: 10s`
       * `max_delay: 60s`
       * `multiplier: 2`

2. **success\_log**

   * **Action**: Return the string `"load-ok"` to mark the workflow as successful.

### 2.2 Environment Variables

* `INGEST_URL`: The HTTP endpoint of the deployed Cloud Function (e.g. `https://europe-west4-crystalloids-candidates.cloudfunctions.net/mozaffar_kazemi_ingest`).

### 2.3 Permissions

* **Workflow Service Account**: Must have `roles/iam.serviceAccountTokenCreator` to mint OIDC tokens.
* **Scheduler Service Account** (`crystalloids-candidates@appspot.gserviceaccount.com`): Must have `roles/workflows.invoker` on the workflow.

---

## 3 Scheduling

**Job Name**: `Mozaffar_Kazemi_Schedule`
**Location**: europe-west1 (supported region)
**Time Zone**: Europe/Amsterdam
**Cron Expression**: `0 7 * * *` (07:00 CET/CEST daily)

### 3.1 Creation Command

```bash
# Ensure Cloud Scheduler API is enabled
gcloud services enable cloudscheduler.googleapis.com

# Create the scheduled job
gcloud scheduler jobs create http Mozaffar_Kazemi_Schedule \
  --location=europe-west1 \
  --schedule="0 7 * * *" \
  --time-zone="Europe/Amsterdam" \
  --http-method=POST \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/crystalloids-candidates/locations/europe-west4/workflows/Mozaffar_Kazemi_Workflow/executions" \
  --oidc-service-account-email="crystalloids-candidates@appspot.gserviceaccount.com" \
  --oidc-token-audience="https://workflowexecutions.googleapis.com/v1/projects/crystalloids-candidates/locations/europe-west4/workflows/Mozaffar_Kazemi_Workflow/executions"
```

### 3.2 Permissions for Scheduler

* Grant `roles/iam.serviceAccountTokenCreator` to the Scheduler SA on itself.
* Grant `roles/workflows.invoker` on the workflow to the Scheduler SA.

---

## 4 Monitoring & Alerting

### 4.1 Workflows Executions

* **Cloud Console**: Serverless → Workflows → `Mozaffar_Kazemi_Workflow` → Executions.
* **Metrics**: Execution count, error count, average latency.

### 4.2 Cloud Scheduler Logs

* Navigate to Logging → Logs Explorer.
* **Resource**: `cloud_scheduler_job`.
* Filter by `logName = "projects/crystalloids-candidates/logs/cloudscheduler.googleapis.com%2Fexecutions"`.

### 4.3 Alerts

* **Workflow failures**: Create an alert on the log-based metric

  ```text
  resource.type="workflow_execution"
  jsonPayload.status="FAILED"
  ```
* **Scheduler auth errors**: Alert on severity=ERROR for Scheduler jobs to catch 401s.

---

## 5 Testing & Troubleshooting

### 5.1 Manual Execution

```bash
gcloud workflows run Mozaffar_Kazemi_Workflow \
  --location=europe-west4
```

* Inspect execution graph and logs in the Console.

### 5.2 Rerun Specific Steps

* To retry ingestion manually, invoke the Cloud Function:

```bash
curl -X POST $INGEST_URL
```

### 5.3 IAM Debugging

* **List bindings**:

```bash
gcloud projects get-iam-policy crystalloids-candidates
```

* Ensure the Scheduler SA has `roles/workflows.invoker` and `iam.serviceAccountTokenCreator`.

---

## 6 Change Management

* **Updating schedule**: `gcloud scheduler jobs update cron Mozaffar_Kazemi_Schedule --schedule="<new-cron>"`
* **Workflow revisions**: Edit `workflows/ingest_workflow.yaml` and run `gcloud workflows deploy`.

---
