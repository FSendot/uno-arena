#!/usr/bin/env bash
# Offline helm lint/template checks for Room Gameplay chart overlays.
# grep is the authoritative status in the assertion pipelines below. Avoid
# pipefail here: grep -q intentionally closes early and large rendered charts
# can otherwise make the producer report a nondeterministic SIGPIPE (141).
set -eu
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
CHART="${ROOT}/services/room-gameplay/helm/room-gameplay"
HELM="${HELM:-helm}"

command -v "${HELM}" >/dev/null 2>&1 || { echo "helm required" >&2; exit 1; }

"${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" >/dev/null

kind_out="$("${HELM}" template room-kind "${CHART}" -f "${CHART}/values.kind.yaml")"
echo "${kind_out}" | grep -q 'cpu: 500m'
echo "${kind_out}" | grep -q 'timeoutSeconds: 3'
echo "${kind_out}" | grep -q 'failureThreshold: 6'
echo "${kind_out}" | grep -q 'name: ROOM_RUNTIME_CPU_LIMIT'
echo "${kind_out}" | grep -A1 'name: ROOM_RUNTIME_CPU_LIMIT' | grep -q 'value: "500m"'
echo "${kind_out}" | grep -A1 'name: ROOM_RUNTIME_PROBE_TIMEOUT_SECONDS' | grep -q 'value: "10"'
echo "${kind_out}" | grep -A1 'name: ROOM_RUNTIME_PROBE_FAILURE_THRESHOLD' | grep -q 'value: "6"'
echo "${kind_out}" | grep -q 'image: "uno-arena/room-gameplay:local"'
echo "${kind_out}" | grep -q 'imagePullPolicy: IfNotPresent'
! echo "${kind_out}" | grep -q 'image: "uno-arena/room-gameplay@"'
echo "${kind_out}" | grep -q 'name: "uno-arena-local-credentials"'
echo "${kind_out}" | grep -q 'DATABASE_URL'
echo "${kind_out}" | grep -q 'room-database-url'
echo "${kind_out}" | grep -q 'GAME_INTEGRITY_AUDIT_CREDENTIAL'
echo "${kind_out}" | grep -q 'key: "game-integrity-audit-credential"'
echo "${kind_out}" | grep -q 'DEPLOYMENT_ENV'
echo "${kind_out}" | grep -q 'WORKER_ROLE'
echo "${kind_out}" | grep -q 'room-timer'
echo "${kind_out}" | grep -q 'type: ClusterIP'
! echo "${kind_out}" | grep -E 'type:\s*(NodePort|LoadBalancer)'
echo "${kind_out}" | grep -q 'name: room-kind-runtime-controller'
echo "${kind_out}" | grep -q 'replicas: 2'
echo "${kind_out}" | grep -q 'name: room-runtime'
echo "${kind_out}" | grep -q 'automountServiceAccountToken: false'
echo "${kind_out}" | grep -q 'pool_mode = transaction'
echo "${kind_out}" | grep -q 'replicas: 1'
echo "${kind_out}" | grep -q 'max_client_conn = 100'
echo "${kind_out}" | grep -q 'default_pool_size = 4'
echo "${kind_out}" | grep -q 'auth_type = plain'
echo "${kind_out}" | grep -q 'ROOM_RUNTIME_SECRET_ENV_JSON'
echo "${kind_out}" | grep -q 'room-runtime-router-credential'
echo "${kind_out}" | grep -q 'name: room-kind-integrity-reconciler'

compactor_out="$("${HELM}" template room-kind "${CHART}" -f "${CHART}/values.kind.yaml" --show-only templates/player-stream-compactor-deployment.yaml)"
echo "${compactor_out}" | grep -q 'name: room-kind-player-stream-compactor'
echo "${compactor_out}" | grep -q 'replicas: 1'
echo "${compactor_out}" | grep -q 'type: Recreate'
echo "${compactor_out}" | grep -A1 'name: WORKER_ROLE' | grep -q 'room-player-stream-compactor'
echo "${compactor_out}" | grep -A1 'name: ROOM_PLAYER_STREAM_MAXLEN' | grep -q 'value: "1024"'
echo "${compactor_out}" | grep -A1 'name: ROOM_PLAYER_STREAM_COMPACTOR_PAGE_SIZE' | grep -q 'value: "100"'
echo "${compactor_out}" | grep -A1 'name: ROOM_PLAYER_STREAM_TRIM_LIMIT' | grep -q 'value: "4096"'
echo "${compactor_out}" | grep -q 'key: "room-pgbouncer-database-url"'
echo "${compactor_out}" | grep -q 'automountServiceAccountToken: false'
! echo "${compactor_out}" | grep -Eq 'name: (SERVICE_CREDENTIAL|ROOM_SERVICE_CREDENTIAL|GATEWAY_SERVICE_CREDENTIAL)$'
! echo "${compactor_out}" | grep -q 'GAME_INTEGRITY_INTERNAL_CREDENTIAL'
! echo "${compactor_out}" | grep -q 'ROOM_TIMER_SERVICE_CREDENTIAL'

reconciler_doc="$(echo "${kind_out}" | awk '/name: room-kind-integrity-reconciler$/,/^---$/')"
echo "${reconciler_doc}" | grep -q 'replicas: 2'
echo "${reconciler_doc}" | grep -q 'value: "room-integrity-reconciler"'
echo "${reconciler_doc}" | grep -q 'ROOM_INTEGRITY_RECONCILER_CLAIM_BATCH'
echo "${reconciler_doc}" | grep -q 'key: "room-pgbouncer-database-url"'
echo "${reconciler_doc}" | grep -q 'key: "game-integrity-internal-credential"'
echo "${reconciler_doc}" | grep -q 'key: "game-integrity-audit-credential"'
! echo "${reconciler_doc}" | grep -Eq 'name: (SERVICE_CREDENTIAL|ROOM_SERVICE_CREDENTIAL|GATEWAY_SERVICE_CREDENTIAL)$'
! echo "${reconciler_doc}" | grep -q 'IDENTITY_SERVICE_CREDENTIAL'
! echo "${reconciler_doc}" | grep -q 'ROOM_TIMER_SERVICE_CREDENTIAL'
! echo "${reconciler_doc}" | grep -q 'ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL'
! echo "${reconciler_doc}" | grep -q 'ROOM_ANALYTICS_BACKFILL'
! echo "${reconciler_doc}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'

controller_doc="$(echo "${kind_out}" | awk '/name: room-kind-runtime-controller$/,/^---$/')"
test "$(echo "${controller_doc}" | grep -c 'secretKeyRef:')" -eq 1
echo "${controller_doc}" | grep -q 'key: "room-database-url"'
! echo "${controller_doc}" | grep -q 'key: "room-runtime-router-credential"'
! echo "${controller_doc}" | grep -q 'key: "game-integrity-internal-credential"'
echo "${controller_doc}" | grep -q 'ROOM_RUNTIME_CONTROLLER_CLAIM_BATCH'
echo "${controller_doc}" | grep -q 'ROOM_RUNTIME_CONTROLLER_CONCURRENCY'
echo "${controller_doc}" | grep -q '\\"ROOM_RUNTIME_ROUTER_CREDENTIAL\\"'
! echo "${controller_doc}" | grep -q '\\"IDENTITY_SERVICE_CREDENTIAL\\"'
echo "${controller_doc}" | grep -q '\\"ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_CURRENT\\"'
echo "${controller_doc}" | grep -q '\\"ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_PREVIOUS\\"'
echo "${controller_doc}" | grep -A1 'name: ROOM_INTERNAL_PRINCIPAL_KEY_VERSION' | grep -q 'value: "keyring-v1"'
echo "${kind_out}" | grep -q 'unoarena.io/principal-key-version: "keyring-v1"'
echo "${kind_out}" | grep -q 'key: "internal-principal-hmac-key-current"'
echo "${kind_out}" | grep -q 'key: "internal-principal-hmac-key-previous"'

no_previous_out="$("${HELM}" template room-no-previous "${CHART}" -f "${CHART}/values.kind.yaml" \
  --set-string env.ROOM_INTERNAL_PRINCIPAL_PREVIOUS_KEY_ID= \
  --set-string secretEnv.ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_PREVIOUS= \
  --set-string runtimeSecretEnv.ROOM_INTERNAL_PRINCIPAL_HMAC_KEY_PREVIOUS=)"
! echo "${no_previous_out}" | grep -q 'key: "internal-principal-hmac-key-previous"'
echo "${controller_doc}" | grep -q '\\"GAME_INTEGRITY_INTERNAL_CREDENTIAL\\"'
echo "${controller_doc}" | grep -q '\\"GAME_INTEGRITY_AUDIT_CREDENTIAL\\"'
echo "${controller_doc}" | grep -q '\\"ROOM_PGBOUNCER_URL\\"'
! echo "${controller_doc}" | grep -q '\\"SERVICE_CREDENTIAL\\"'
! echo "${controller_doc}" | grep -q '\\"ROOM_TIMER_SERVICE_CREDENTIAL\\"'
! echo "${controller_doc}" | grep -q '\\"ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL\\"'
! echo "${controller_doc}" | grep -q '\\"ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET\\"'

echo "${kind_out}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${kind_out}" | grep -q 'key: "analytics-room-credential"'
echo "${kind_out}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'key: "room-analytics-backfill-cursor-secret"'
echo "${kind_out}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'
echo "${kind_out}" | grep -q 'key: "room-public-list-cursor-secret"'
# Timer worker must not receive Analytics backfill auth, public-list cursor, or cursor MAC secrets.
timer_doc="$(echo "${kind_out}" | awk '/name: room-kind-timer-worker$/,/^---$/')"
echo "${timer_doc}" | grep -q 'key: "room-timer-service-credential"'
echo "${timer_doc}" | grep -q 'key: "room-pgbouncer-database-url"'
! echo "${timer_doc}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
! echo "${timer_doc}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
! echo "${timer_doc}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'
! echo "${timer_doc}" | grep -Eq 'name: (SERVICE_CREDENTIAL|ROOM_SERVICE_CREDENTIAL|GATEWAY_SERVICE_CREDENTIAL)$'
! echo "${timer_doc}" | grep -q 'IDENTITY_SERVICE_CREDENTIAL'
! echo "${timer_doc}" | grep -q 'GAME_INTEGRITY_INTERNAL_CREDENTIAL'
! echo "${timer_doc}" | grep -q 'ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL'

if "${HELM}" template room-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" >/dev/null 2>&1; then
  echo "staging without digest must fail" >&2
  exit 1
fi
if "${HELM}" template room-prod "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" >/dev/null 2>&1; then
  echo "production without digest must fail" >&2
  exit 1
fi

if "${HELM}" template room-staging-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with tag and empty digest must fail" >&2
  exit 1
fi
if "${HELM}" template room-prod-tag "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.tag=latest >/dev/null 2>&1; then
  echo "production with tag and empty digest must fail" >&2
  exit 1
fi

if "${HELM}" template room-staging-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "staging with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi
if "${HELM}" template room-prod-kind-override "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set kind=true --set image.tag=latest >/dev/null 2>&1; then
  echo "production with --set kind=true and tag must still fail (DEPLOYMENT_ENV!=local)" >&2
  exit 1
fi

for bad in \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaG" \
  "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" \
  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
  "latest" \
  "sha256:" \
  "SHA256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
do
  if "${HELM}" template room-bad-digest "${CHART}" -f "${CHART}/values.yaml" \
    --set image.digest="${bad}" --set image.tag= >/dev/null 2>&1; then
    echo "malformed digest must fail: ${bad}" >&2
    exit 1
  fi
done

pin_out="$("${HELM}" template room-pin "${CHART}" -f "${CHART}/values.yaml" \
  --set image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set image.tag=)"
echo "${pin_out}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/room-gameplay@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'

pin_tag_staging="$("${HELM}" template room-pin-staging "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set image.tag=latest)"
echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/room-gameplay@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"'
! echo "${pin_tag_staging}" | grep -q 'image: "registry.gitlab.com/group/uno-arena/room-gameplay:latest"'

pin_staging_full="$("${HELM}" template room-pin-staging-full "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.staging.yaml" \
  --set image.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --set image.tag=)"
echo "${pin_staging_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${pin_staging_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${pin_staging_full}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'
echo "${pin_staging_full}" | grep -q 'GAME_INTEGRITY_AUDIT_CREDENTIAL'
echo "${pin_staging_full}" | grep -q 'auth_type = scram-sha-256'
echo "${pin_staging_full}" | grep -q 'client_tls_sslmode = require'
echo "${pin_staging_full}" | grep -q 'server_tls_sslmode = verify-full'
echo "${pin_staging_full}" | grep -q 'kind: PeerAuthentication'
echo "${pin_staging_full}" | grep -q 'mode: STRICT'

pin_prod_full="$("${HELM}" template room-pin-prod-full "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
  --set image.digest=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd \
  --set image.tag=)"
echo "${pin_prod_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL'
echo "${pin_prod_full}" | grep -q 'ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET'
echo "${pin_prod_full}" | grep -q 'ROOM_PUBLIC_LIST_CURSOR_SECRET'
echo "${pin_prod_full}" | grep -q 'GAME_INTEGRITY_AUDIT_CREDENTIAL'
echo "${pin_prod_full}" | grep -q 'replicas: 6'
echo "${pin_prod_full}" | grep -q 'max_client_conn = 50000'
echo "${pin_prod_full}" | grep -q 'default_pool_size = 32'
echo "${pin_prod_full}" | grep -q 'reserve_pool_size = 8'
echo "${pin_prod_full}" | grep -q 'name: room-pin-prod-full-player-stream-compactor'

if "${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" --set playerStreamCompactor.maxLen=0 >/dev/null 2>&1; then
  echo "player stream maxLen=0 must fail schema validation" >&2
  exit 1
fi
if "${HELM}" lint "${CHART}" -f "${CHART}/values.kind.yaml" --set playerStreamCompactor.pageSize=501 >/dev/null 2>&1; then
  echo "player stream pageSize above bound must fail schema validation" >&2
  exit 1
fi

for insecure_override in \
  'topology.pgbouncer.authType=plain' \
  'topology.pgbouncer.tls.enabled=false' \
  'topology.pgbouncer.tls.secretName=' \
  'topology.pgbouncer.tls.clientMode=disable' \
  'topology.pgbouncer.tls.serverMode=require' \
  'topology.pgbouncer.mesh.enforceStrictMTLS=false'
do
  if "${HELM}" template room-insecure "${CHART}" -f "${CHART}/values.yaml" -f "${CHART}/values.production.yaml" \
    --set image.digest=sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
    --set "${insecure_override}" >/dev/null 2>&1; then
    echo "production render accepted insecure override: ${insecure_override}" >&2
    exit 1
  fi
done

echo "ok room-gameplay-helm"
