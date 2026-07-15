#!/usr/bin/env bash
# Offline helm lint/template checks for Gateway chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/gateway/helm/gateway"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template gateway-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'cpu: 500m'
echo "${kind_out}" | grep -q 'timeoutSeconds: 3'
echo "${kind_out}" | grep -q 'failureThreshold: 6'
echo "${kind_out}" | grep -q 'serviceAccountName: gateway'
echo "${kind_out}" | grep -q 'automountServiceAccountToken: false'
echo "${kind_out}" | grep -q 'unoarena.io/metrics-exposed: "true"'
echo "${kind_out}" | grep -q 'unoarena.io/component: api'
echo "${kind_out}" | grep -q 'unoarena.io/metrics-scrape: service'
echo "${kind_out}" | grep -q 'containerPort: 9090'
echo "${kind_out}" | grep -q 'port: 9090'
echo "${kind_out}" | grep -q 'name: SERVICE_VERSION'
echo "${kind_out}" | grep -q 'name: POD_UID'
echo "${kind_out}" | grep -q 'name: TELEMETRY_MODE'
! echo "${kind_out}" | grep -q 'GATEWAY_AUDIT_LOG_PATH'
echo "${kind_out}" | grep -q 'image: "uno-arena/gateway:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/gateway@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'REDIS_URL'
echo "${kind_out}" | grep -q 'redis://redis.uno-arena.svc.cluster.local:6379/6'
echo "${kind_out}" | grep -q 'GATEWAY_PLAYER_FEED_REDIS_URL'
echo "${kind_out}" | grep -q 'redis://redis.uno-arena.svc.cluster.local:6379/2'
echo "${kind_out}" | grep -q 'GATEWAY_SPECTATOR_REDIS_URL'
echo "${kind_out}" | grep -q 'redis://redis.uno-arena.svc.cluster.local:6379/5'
echo "${kind_out}" | grep -q 'GATEWAY_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'GATEWAY_IDENTITY_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'GATEWAY_ROOM_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'GATEWAY_TOURNAMENT_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'GATEWAY_SPECTATOR_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'key: "identity-internal-credential"'
echo "${kind_out}" | grep -q 'key: "room-service-credential"'
echo "${kind_out}" | grep -q 'key: "tournament-internal-credential"'
echo "${kind_out}" | grep -q 'key: "spectator-view-internal-credential"'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'KAFKA_BROKERS'
echo "${kind_out}" | grep -q 'kafka.uno-arena.svc.cluster.local:9092'
echo "${kind_out}" | grep -q 'KAFKA_CONSUMER_GROUP'
echo "${kind_out}" | grep -q 'KAFKA_SESSION_INVALIDATED_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_SESSION_INVALIDATED_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'GATEWAY_SESSION_INVALIDATION_TTL'
echo "${kind_out}" | grep -q 'value: "7h"'
echo "${kind_out}" | grep -q 'GATEWAY_BACKEND_HTTP_TIMEOUT'
echo "${kind_out}" | grep -q 'value: "12s"'
# ADR-0029 kind rationale is asserted in values.kind.yaml (source 30m + DLQ 6h → 7h).
grep -q 'ADR-0029' "${CHART}/values.kind.yaml"
grep -q 'GATEWAY_SESSION_INVALIDATION_TTL: "7h"' "${CHART}/values.kind.yaml"
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'
! echo "${kind_out}" | grep -q 'GATEWAY_CAPABILITY_MODE'
! echo "${kind_out}" | grep -q 'GATEWAY_ALLOW_FAKES'

if "${HELM}" template gateway-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template gateway-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template gateway-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template gateway-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template gateway-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

pin_out="$("${HELM}" template gateway-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/gateway@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

echo "ok gateway-helm"
