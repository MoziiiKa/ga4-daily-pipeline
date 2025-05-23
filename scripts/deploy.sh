#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID="crystalloids-candidates"
REGION="europe-west4"
FUNCTION_NAME="mozaffar_kazemi_ingest"
RUNTIME="python312"
SRC_DIR="src/ingest"
ENTRY_POINT="main"
SERVICE_ACCOUNT="crystalloids-candidates@appspot.gserviceaccount.com"
# ──────────────────────────────────────────────────────────────────────────────

echo "👉 Deploying Cloud Function ${FUNCTION_NAME} to ${REGION} in project ${PROJECT_ID}…"

gcloud config set project "${PROJECT_ID}"
gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --runtime="${RUNTIME}" \
  --region="${REGION}" \
  --source="${SRC_DIR}" \
  --entry-point="${ENTRY_POINT}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --min-instances=0 \
  --trigger-http

echo "✅ Deployment of ${FUNCTION_NAME} complete!"
