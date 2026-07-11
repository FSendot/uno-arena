#!/usr/bin/env bash
# Isolated kubectl port-forward to Tournament Orchestration ClusterIP using local port 0.
# Only cleans up this script's child job — never kills by port or reused PID.
# Prints TOURNAMENT_BASE_URL=http://127.0.0.1:<port> for callers.
set -euo pipefail
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd nc
assert_kind_context

REMOTE_PORT=8080
SERVICE_NAME="${TOURNAMENT_SERVICE_NAME:-tournament-orchestration}"
PF_PID=""
PF_LOG=""
LOCAL_PORT=""

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

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  trap cleanup EXIT
fi

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/tournament-pf.XXXXXX")"
LOCAL_PORT_SPEC="0:${REMOTE_PORT}"

echo "port-forward local ${LOCAL_PORT_SPEC} -> svc/${SERVICE_NAME}:${REMOTE_PORT}" >&2
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 "svc/${SERVICE_NAME}" "${LOCAL_PORT_SPEC}" >"${PF_LOG}" 2>&1 &
PF_PID=$!

ready=0
for _ in $(seq 1 40); do
  if ! kill -0 "${PF_PID}" 2>/dev/null; then
    die "tournament port-forward exited early (see ${PF_LOG})"
  fi
  if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
    if parsed="$(grep -E "Forwarding from 127\\.0\\.0\\.1:[0-9]+ -> ${REMOTE_PORT}" "${PF_LOG}" 2>/dev/null | head -n1 || true)" \
      && [[ -n "${parsed}" ]]; then
      LOCAL_PORT="$(sed -nE "s/.*Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${REMOTE_PORT}.*/\\1/p" <<<"${parsed}")"
      if [[ -n "${LOCAL_PORT}" ]] && [[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] \
        && (( LOCAL_PORT >= 1 && LOCAL_PORT <= 65535 )); then
        if nc -z 127.0.0.1 "${LOCAL_PORT}" 2>/dev/null; then
          if ! kill -0 "${PF_PID}" 2>/dev/null; then
            die "tournament port-forward died after port became reachable (see ${PF_LOG})"
          fi
          ready=1
          break
        fi
      fi
    fi
  fi
  sleep 0.25
done
[[ "${ready}" == "1" ]] || die "tournament port-forward to 127.0.0.1 failed (see ${PF_LOG})"
[[ -n "${LOCAL_PORT}" ]] || die "failed to parse Forwarding from line in ${PF_LOG}"

export TOURNAMENT_BASE_URL="http://127.0.0.1:${LOCAL_PORT}"
echo "TOURNAMENT_BASE_URL=${TOURNAMENT_BASE_URL}"

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "holding port-forward (Ctrl-C to stop)" >&2
  wait "${PF_PID}"
fi
