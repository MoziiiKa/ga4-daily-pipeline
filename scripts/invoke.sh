#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
PROJECT_ID="crystalloids-candidates"
REGION="europe-west4"
FUNCTION_NAME="mozaffar_kazemi_ingest"
# ──────────────────────────────────────────────────────────────────────────────

# Build the function URL
FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"

echo "👉 Invoking ${FUNCTION_NAME} at ${FUNCTION_URL}…"

curl -v \
  -H "Authorization: Bearer $(gcloud auth print-identity-token --project=${PROJECT_ID})" \
  "${FUNCTION_URL}"

echo "✅ Invocation request sent!"
