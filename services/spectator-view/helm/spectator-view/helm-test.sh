#!/usr/bin/env bash
# Offline helm lint/template checks for Spectator View chart overlays.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/spectator-view/helm/spectator-view"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template spectator-view-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'image: "uno-arena/spectator-view:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/spectator-view@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'REDIS_URL'
echo "${kind_out}" | grep -q 'redis://redis.uno-arena.svc.cluster.local:6379/5'
echo "${kind_out}" | grep -q 'SPECTATOR_REDIS_STREAM_MAXLEN'
echo "${kind_out}" | grep -q 'SPECTATOR_VIEW_INTERNAL_CREDENTIAL'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'KAFKA_BROKERS'
echo "${kind_out}" | grep -q 'kafka.uno-arena.svc.cluster.local:9092'
echo "${kind_out}" | grep -q 'KAFKA_CONSUMER_GROUP'
echo "${kind_out}" | grep -q 'KAFKA_SPECTATOR_SAFE_TOPIC'
echo "${kind_out}" | grep -q 'KAFKA_SPECTATOR_SAFE_DLQ_TOPIC'
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'
! echo "${kind_out}" | grep -q 'SPECTATOR_CAPABILITY_MODE'
# Kind enables the live-proven projection rebuilder.
echo "${kind_out}" | grep -q 'spectator-projection-rebuilder'
echo "${kind_out}" | grep -q 'WORKER_ROLE'

# Template privilege checks on the kind render.
reb_out="${kind_out}"
echo "${reb_out}" | grep -q 'WORKER_ROLE'
echo "${reb_out}" | grep -q 'spectator-projection-rebuilder'
echo "${reb_out}" | grep -q 'ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL'
echo "${reb_out}" | grep -q 'KAFKA_PROJECTION_REBUILD_TOPIC'
echo "${reb_out}" | grep -q 'spectator.projection.rebuild_requested'
test "$(echo "${reb_out}" | grep -c 'name: WORKER_ROLE')" -eq 1
rebuilder_doc="$(echo "${reb_out}" | awk '/name: .*projection-rebuilder$/{flag=1} flag; /^---$/{if(flag){exit}}')"
! echo "${rebuilder_doc}" | grep -q 'containerPort' || { echo "rebuilder must not expose ports" >&2; exit 1; }
! echo "${rebuilder_doc}" | grep -q 'readinessProbe' || { echo "rebuilder must not use HTTP probes" >&2; exit 1; }
! echo "${rebuilder_doc}" | grep -q 'SPECTATOR_VIEW_INTERNAL_CREDENTIAL' || { echo "rebuilder must not get API credential" >&2; exit 1; }

staging_disabled="$("${HELM}" template spectator-staging-disabled "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag= 2>/dev/null || true)"
if echo "${staging_disabled}" | grep -q 'spectator-projection-rebuilder'; then
  echo "staging must keep projection rebuilder disabled" >&2
  exit 1
fi

if "${HELM}" template spectator-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template spectator-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template spectator-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template spectator-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template spectator-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

pin_out="$("${HELM}" template spectator-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/spectator-view@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

echo "ok spectator-view-helm"
