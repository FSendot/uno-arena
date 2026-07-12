#!/usr/bin/env bash
# Explicit live proof: durable session invalidation written on Redis DB 6 is visible to a
# second Hub/LiveFeed that never received Pub/Sub (cross-replica Redis admission only).
# Does NOT prove Kafka delivery of identity.session.invalidated or live SSE stream closure.
# Port-forwards redis only. Never applies/resets/deploys or installs dependencies.
set -euo pipefail
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd go
require_cmd nc
assert_kind_context

REMOTE_PORT=6379
SERVICE_NAME="redis"
SAFE_DB=6

PF_PID=""
PF_LOG=""
LOCAL_PORT=""

refuse_caller_redis_url() {
  if [[ -n "${GATEWAY_REDIS_URL:-}" ]]; then
    die "refusing caller-supplied GATEWAY_REDIS_URL; harness generates DB ${SAFE_DB} URL"
  fi
}

cleanup() {
  if [[ -n "${PF_PID}" ]]; then
    if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
      kill "${PF_PID}" 2>/dev/null || true
    fi
    wait "${PF_PID}" 2>/dev/null || true
  fi
  if [[ -n "${PF_LOG}" && -f "${PF_LOG}" ]]; then
    rm -f "${PF_LOG}"
  fi
}

trap cleanup EXIT
refuse_caller_redis_url

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/gateway-si-pf.XXXXXX")"
LOCAL_PORT_SPEC="0:${REMOTE_PORT}"

echo "port-forward local ${LOCAL_PORT_SPEC} -> svc/${SERVICE_NAME}:${REMOTE_PORT}" >&2
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 "svc/${SERVICE_NAME}" "${LOCAL_PORT_SPEC}" >"${PF_LOG}" 2>&1 &
PF_PID=$!

ready=0
for _ in $(seq 1 40); do
  if ! kill -0 "${PF_PID}" 2>/dev/null; then
    die "redis port-forward exited early (see ${PF_LOG})"
  fi
  if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
    if parsed="$(grep -E "Forwarding from 127\\.0\\.0\\.1:[0-9]+ -> ${REMOTE_PORT}" "${PF_LOG}" 2>/dev/null | head -n1 || true)" \
      && [[ -n "${parsed}" ]]; then
      LOCAL_PORT="$(sed -nE "s/.*Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${REMOTE_PORT}.*/\\1/p" <<<"${parsed}")"
      if [[ -n "${LOCAL_PORT}" ]] && [[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] \
        && (( LOCAL_PORT >= 1 && LOCAL_PORT <= 65535 )); then
        if nc -z 127.0.0.1 "${LOCAL_PORT}" 2>/dev/null; then
          ready=1
          break
        fi
      fi
    fi
  fi
  sleep 0.25
done
[[ "${ready}" == "1" ]] || die "redis port-forward failed (see ${PF_LOG})"

export GATEWAY_REDIS_URL="redis://127.0.0.1:${LOCAL_PORT}/${SAFE_DB}"
echo "GATEWAY_REDIS_URL=${GATEWAY_REDIS_URL}"
echo "note: Redis DB6 durable SI cross-Hub admission only; not Kafka→SSE end-to-end"

cd "${REPO_ROOT}/services/gateway/src"
GOWORK=off GOPROXY=off GOSUMDB=off go test -tags=redis_integration -count=1 -timeout=60s ./bff/store/ \
  -run 'TestSessionInvalidationStore_ApplyDuplicateConflictAndNotify|TestSessionInvalidation_CrossReplicaAdmissionReject|TestSessionInvalidationStore_RestoreMissingSessionHash'

echo "ok kind-test-gateway-si-redis-admission-live"
