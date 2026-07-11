#!/usr/bin/env bash
# Offline helm lint/template checks for Analytics chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/analytics/helm/analytics"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template analytics-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'image: "uno-arena/analytics:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/analytics@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'CLICKHOUSE_URL'
echo "${kind_out}" | grep -q 'CLICKHOUSE_USER'
echo "${kind_out}" | grep -q 'analytics-runtime-user'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'

if "${HELM}" template analytics-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template analytics-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template analytics-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template analytics-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template analytics-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

pin_out="$("${HELM}" template analytics-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/analytics@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

echo "ok analytics-helm"
