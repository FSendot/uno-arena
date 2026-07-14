#!/usr/bin/env bash
# Explicit: install the portable observability chart after local S3 buckets exist.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
require_cmd helm
assert_kind_context

CHART="${REPO_ROOT}/infrastructure/observability/helm/uno-arena-observability"
TIMEOUT="${KIND_OBSERVABILITY_TIMEOUT:-600s}"
if kubectl -n observability get job/minio-create-observability-buckets >/dev/null 2>&1; then
  kubectl -n observability wait --for=condition=complete job/minio-create-observability-buckets --timeout="${TIMEOUT}"
fi
helm upgrade --install uno-arena-observability "${CHART}" -n observability -f "${CHART}/values.kind.yaml" --wait --timeout "${TIMEOUT}"
"${SCRIPT_DIR}/wait-observability.sh"
echo "ok kind-deploy-observability"
