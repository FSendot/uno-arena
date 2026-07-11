#!/usr/bin/env bash
# Explicit live kind Redis AOF acceptance: same-pod container restart preserves
# isolated DB15 prefix keys/streams after kill of Redis PID 1.
#
# Redis remains disposable/not authoritative: pod replacement or kind reset may
# lose emptyDir data; rebuild from Kafka is required afterward. This target only
# proves AOF + emptyDir survive a Redis process/container restart.
#
# Isolation: Redis logical DB 15 + generated prefix only. Cleanup deletes that
# prefix only (no FLUSHDB). Does not touch Room timer DB 2 or Spectator DB 5.
# Never applies/resets unrelated resources.
set -euo pipefail
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
assert_kind_context

REDIS_DEPLOY="redis"
REDIS_TEST_DB=15
APPENDFSYNC_WAIT_S="${REDIS_AOF_FSYNC_WAIT_S:-2}"
PREFIX="unoarena-redis-aof-$(date +%s)-${RANDOM}"
KEY="${PREFIX}:key"
STREAM="${PREFIX}:stream"
VALUE="aof-survived-${PREFIX}"

POD=""
RESTARTS_BEFORE=""

cleanup() {
  local pod
  pod="$(kubectl -n "${KIND_NAMESPACE}" get pod -l app.kubernetes.io/name=redis -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${pod}" ]]; then
    # Prefix-only cleanup on DB15 (Room integration also uses DB15; do not wipe the DB).
    kubectl -n "${KIND_NAMESPACE}" exec "${pod}" -c redis -- \
      redis-cli -n "${REDIS_TEST_DB}" --scan --pattern "${PREFIX}:*" 2>/dev/null \
      | while IFS= read -r k; do
          [[ -n "${k}" ]] || continue
          kubectl -n "${KIND_NAMESPACE}" exec "${pod}" -c redis -- \
            redis-cli -n "${REDIS_TEST_DB}" DEL "${k}" >/dev/null 2>&1 || true
        done || true
  fi
}
trap cleanup EXIT

redis_pod() {
  kubectl -n "${KIND_NAMESPACE}" get pod -l app.kubernetes.io/name=redis \
    -o jsonpath='{.items[0].metadata.name}'
}

redis_ready() {
  local pod="$1"
  kubectl -n "${KIND_NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false"
}

redis_restarts() {
  local pod="$1"
  kubectl -n "${KIND_NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo "0"
}

echo "Redis AOF acceptance: DB ${REDIS_TEST_DB} prefix ${PREFIX}"
echo "note: Redis emptyDir is disposable/not authoritative; Kafka rebuild required after pod replace/kind reset"

POD="$(redis_pod)"
[[ -n "${POD}" ]] || die "redis pod not found in ${KIND_NAMESPACE}"

ready=0
for _ in $(seq 1 60); do
  if [[ "$(redis_ready "${POD}")" == "true" ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == "1" ]] || die "redis pod ${POD} never became ready"

# Confirm AOF is enabled in the running process (post-apply).
aof="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- redis-cli CONFIG GET appendonly | tail -n 1 || true)"
[[ "${aof}" == "yes" ]] || die "expected appendonly=yes, got '${aof}'"
fsync="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- redis-cli CONFIG GET appendfsync | tail -n 1 || true)"
[[ "${fsync}" == "everysec" ]] || die "expected appendfsync=everysec, got '${fsync}'"
adir="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- redis-cli CONFIG GET appenddirname | tail -n 1 || true)"
[[ "${adir}" == "appendonlydir" ]] || die "expected appenddirname=appendonlydir, got '${adir}'"
dir="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- redis-cli CONFIG GET dir | tail -n 1 || true)"
[[ "${dir}" == "/data" ]] || die "expected dir=/data, got '${dir}'"

kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- \
  redis-cli -n "${REDIS_TEST_DB}" SET "${KEY}" "${VALUE}" >/dev/null
got="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- \
  redis-cli -n "${REDIS_TEST_DB}" GET "${KEY}" | tr -d '\r')"
[[ "${got}" == "${VALUE}" ]] || die "pre-kill GET mismatch: ${got}"

stream_id="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- \
  redis-cli -n "${REDIS_TEST_DB}" XADD "${STREAM}" '*' field "${VALUE}" | tr -d '\r')"
[[ -n "${stream_id}" && "${stream_id}" != "nil" ]] || die "XADD failed: ${stream_id}"

echo "waiting ${APPENDFSYNC_WAIT_S}s past appendfsync everysec"
sleep "${APPENDFSYNC_WAIT_S}"

RESTARTS_BEFORE="$(redis_restarts "${POD}")"
echo "killing Redis PID 1 in ${POD} (restartCount=${RESTARTS_BEFORE})"
# Expected: redis exits; kubectl exec returns non-zero when the process dies.
kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- kill 1 || true

# Wait for container restart (same pod, emptyDir retained) and readiness (AOF load).
restarted=0
for _ in $(seq 1 90); do
  POD="$(redis_pod)"
  [[ -n "${POD}" ]] || { sleep 1; continue; }
  cur="$(redis_restarts "${POD}")"
  if [[ "${cur}" =~ ^[0-9]+$ ]] && (( cur > RESTARTS_BEFORE )) && [[ "$(redis_ready "${POD}")" == "true" ]]; then
    restarted=1
    break
  fi
  sleep 1
done
[[ "${restarted}" == "1" ]] || die "redis container did not restart+ready after kill PID 1 (before=${RESTARTS_BEFORE})"

got="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- \
  redis-cli -n "${REDIS_TEST_DB}" GET "${KEY}" | tr -d '\r')"
[[ "${got}" == "${VALUE}" ]] || die "key did not survive Redis PID 1 restart: want ${VALUE} got ${got}"

len="$(kubectl -n "${KIND_NAMESPACE}" exec "${POD}" -c redis -- \
  redis-cli -n "${REDIS_TEST_DB}" XLEN "${STREAM}" | tr -d '\r')"
[[ "${len}" == "1" ]] || die "stream did not survive Redis PID 1 restart: XLEN want 1 got ${len}"

echo "ok kind-test-redis-aof pod=${POD} db=${REDIS_TEST_DB} prefix=${PREFIX} (Redis still disposable/not authoritative)"
