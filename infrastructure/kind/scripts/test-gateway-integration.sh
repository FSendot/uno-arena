#!/usr/bin/env bash
# Explicit, networked Gateway Redis integration against kind Redis DBs 11/12/13.
# Port-forwards redis (local port 0), runs make test-gateway-integration.
# Never accepts caller-supplied Redis URLs. Never applies/resets/deploys.
set -euo pipefail
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd go
require_cmd nc
require_cmd make
assert_kind_context

REMOTE_PORT=6379
SERVICE_NAME="redis"
SAFE_RATE_DB=11
SAFE_PLAYER_DB=12
SAFE_SPECTATOR_DB=13

PF_PID=""
PF_LOG=""
LOCAL_PORT=""

refuse_caller_redis_url() {
  if [[ -n "${GATEWAY_REDIS_URL:-}" || -n "${GATEWAY_PLAYER_FEED_REDIS_URL:-}" || -n "${GATEWAY_SPECTATOR_REDIS_URL:-}" ]]; then
    die "refusing caller-supplied Gateway Redis URLs; harness generates DB ${SAFE_RATE_DB}/${SAFE_PLAYER_DB}/${SAFE_SPECTATOR_DB} URLs"
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

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/gateway-redis-pf.XXXXXX")"
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

export GATEWAY_REDIS_URL="redis://127.0.0.1:${LOCAL_PORT}/${SAFE_RATE_DB}"
export GATEWAY_PLAYER_FEED_REDIS_URL="redis://127.0.0.1:${LOCAL_PORT}/${SAFE_PLAYER_DB}"
export GATEWAY_SPECTATOR_REDIS_URL="redis://127.0.0.1:${LOCAL_PORT}/${SAFE_SPECTATOR_DB}"
echo "GATEWAY_REDIS_URL=${GATEWAY_REDIS_URL}"
echo "GATEWAY_PLAYER_FEED_REDIS_URL=${GATEWAY_PLAYER_FEED_REDIS_URL}"
echo "GATEWAY_SPECTATOR_REDIS_URL=${GATEWAY_SPECTATOR_REDIS_URL}"
echo "note: Kafka/Debezium remain PENDING; suite uses isolated DBs 11/12/13"

make -C "${REPO_ROOT}" test-gateway-integration
echo "ok kind-test-gateway-integration"
