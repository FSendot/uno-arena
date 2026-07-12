#!/usr/bin/env bash
# Explicit, networked Spectator Redis store integration against kind Redis DB 14.
# Port-forwards redis (local port 0), runs make test-spectator-integration against
# ONLY DB 14 with a per-run random key prefix cleaned by the Go suite.
# Never accepts caller-supplied Redis URLs that are not DB 14.
# Never applies/resets/deploys. Cleanup touches only this script's child port-forward.
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
SAFE_DB=14

PF_PID=""
PF_LOG=""
LOCAL_PORT=""

refuse_caller_redis_url() {
  if [[ -n "${SPECTATOR_REDIS_URL:-}" ]]; then
    die "refusing caller-supplied SPECTATOR_REDIS_URL; harness generates DB ${SAFE_DB} URL"
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

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/spectator-redis-pf.XXXXXX")"
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

export SPECTATOR_REDIS_URL="redis://127.0.0.1:${LOCAL_PORT}/${SAFE_DB}"
echo "SPECTATOR_REDIS_URL=${SPECTATOR_REDIS_URL}"
echo "note: suite verifies Redis/Lua quarantine behavior (generated-prefix cleanup); does not exercise live Kafka delivery"

make -C "${REPO_ROOT}" test-spectator-integration

echo "ok kind-test-spectator-integration"
