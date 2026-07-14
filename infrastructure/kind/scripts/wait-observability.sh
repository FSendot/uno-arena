#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"
assert_kind_context
TIMEOUT="${KIND_OBSERVABILITY_TIMEOUT:-600s}"

for resource in deployment/minio deployment/alloy-otlp deployment/loki deployment/tempo deployment/prometheus deployment/kube-state-metrics deployment/grafana daemonset/alloy-logs; do
  kubectl -n observability rollout status "${resource}" --timeout="${TIMEOUT}"
done
if kubectl -n observability get job/minio-create-observability-buckets >/dev/null 2>&1; then
  kubectl -n observability wait --for=condition=complete job/minio-create-observability-buckets --timeout="${TIMEOUT}"
fi
echo "ok kind-observability-ready"
