#!/usr/bin/env bash
# Explicit, networked Game Integrity adapter integration against kind Kurrent.
# Verifies context kind-uno-arena, establishes/cleans port-forward, runs live tests.
# Never applies, resets, or deploys charts.
# Never inspects or kills arbitrary listeners on 2113 (or any other port).
set -euo pipefail
# Job control so background kubectl is a tracked shell job (jobs -pr) and cleanup
# cannot SIGTERM a reused PID that is not this script's child job.
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd go
require_cmd nc
assert_kind_context

REMOTE_PORT=2113
PF_PID=""
PF_LOG=""
LOCAL_PORT=""

# Validate an explicit local port as decimal 1..65535 (argv-safe; never embed in code).
validate_local_port() {
  local port="$1"
  if [[ ! "${port}" =~ ^[0-9]+$ ]]; then
    die "GAME_INTEGRITY_KURRENT_LOCAL_PORT must be a decimal integer, got: ${port}"
  fi
  # Reject leading zeros / empty after strip (except plain 0 which is invalid anyway).
  if [[ "${port}" =~ ^0[0-9] ]]; then
    die "GAME_INTEGRITY_KURRENT_LOCAL_PORT must be decimal 1..65535 without leading zeros"
  fi
  # Reject before Bash arithmetic: fixed-width (( )) can wrap very long digit strings.
  if (( ${#port} > 5 )); then
    die "GAME_INTEGRITY_KURRENT_LOCAL_PORT must be at most 5 digits, got length ${#port}"
  fi
  if (( 10#${port} < 1 || 10#${port} > 65535 )); then
    die "GAME_INTEGRITY_KURRENT_LOCAL_PORT must be in 1..65535, got: ${port}"
  fi
}

cleanup() {
  # Only signal our own still-tracked shell job. Never kill by port or by a
  # reused PID that is no longer this script's background job.
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

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/gi-kurrent-pf.XXXXXX")"

if [[ -n "${GAME_INTEGRITY_KURRENT_LOCAL_PORT:-}" ]]; then
  validate_local_port "${GAME_INTEGRITY_KURRENT_LOCAL_PORT}"
  LOCAL_PORT_SPEC="${GAME_INTEGRITY_KURRENT_LOCAL_PORT}:${REMOTE_PORT}"
else
  # Let kubectl allocate atomically (avoids bind-then-close TOCTOU).
  LOCAL_PORT_SPEC="0:${REMOTE_PORT}"
fi

echo "kind context ok: $(kubectl config current-context)"
echo "port-forward local ${LOCAL_PORT_SPEC} -> svc/kurrentdb:${REMOTE_PORT}"

kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 svc/kurrentdb "${LOCAL_PORT_SPEC}" >"${PF_LOG}" 2>&1 &
PF_PID=$!

ready=0
for _ in $(seq 1 40); do
  if ! kill -0 "${PF_PID}" 2>/dev/null; then
    die "kurrentdb port-forward exited early (see ${PF_LOG})"
  fi
  # Parse confirmed forwarding line while child is still our job.
  if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
    if parsed="$(grep -E "Forwarding from 127\\.0\\.0\\.1:[0-9]+ -> ${REMOTE_PORT}" "${PF_LOG}" 2>/dev/null | head -n1 || true)" \
      && [[ -n "${parsed}" ]]; then
      LOCAL_PORT="$(sed -nE "s/.*Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${REMOTE_PORT}.*/\\1/p" <<<"${parsed}")"
      if [[ -n "${LOCAL_PORT}" ]] && [[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] \
        && (( LOCAL_PORT >= 1 && LOCAL_PORT <= 65535 )); then
        if [[ -n "${GAME_INTEGRITY_KURRENT_LOCAL_PORT:-}" ]] \
          && [[ "${LOCAL_PORT}" != "${GAME_INTEGRITY_KURRENT_LOCAL_PORT}" ]]; then
          die "port-forward bound ${LOCAL_PORT} but GAME_INTEGRITY_KURRENT_LOCAL_PORT=${GAME_INTEGRITY_KURRENT_LOCAL_PORT}"
        fi
        if nc -z 127.0.0.1 "${LOCAL_PORT}" 2>/dev/null; then
          if ! kill -0 "${PF_PID}" 2>/dev/null; then
            die "kurrentdb port-forward died after port became reachable (see ${PF_LOG})"
          fi
          ready=1
          break
        fi
      fi
    fi
  fi
  sleep 0.25
done
[[ "${ready}" == "1" ]] || die "kurrentdb port-forward to 127.0.0.1 failed (see ${PF_LOG})"
[[ -n "${LOCAL_PORT}" ]] || die "failed to parse Forwarding from line in ${PF_LOG}"

# Always point at this script's port-forward — never reuse a caller URL that may
# target a dead or unrelated listener (e.g. a prior manual integration run).
export KURRENTDB_INTEGRATION_URL="kurrentdb://127.0.0.1:${LOCAL_PORT}?tls=false"
export DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-test}"
export GAME_INTEGRITY_ENVELOPE_PROVIDER=dev
export GAME_INTEGRITY_ENVELOPE_KEY_VERSION="${GAME_INTEGRITY_ENVELOPE_KEY_VERSION:-1}"
export GAME_INTEGRITY_ENVELOPE_DEV_KEYS="${GAME_INTEGRITY_ENVELOPE_DEV_KEYS:-1:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"
# Per-run readiness isolation (allowed only for DEPLOYMENT_ENV=test|local).
export GAME_INTEGRITY_READINESS_STREAM_SUFFIX="${GAME_INTEGRITY_READINESS_STREAM_SUFFIX:-kind-$(date +%s)-$$}"

echo "running GI integration against ${KURRENTDB_INTEGRATION_URL} (readiness suffix=${GAME_INTEGRITY_READINESS_STREAM_SUFFIX})"
cd "${REPO_ROOT}"
make test-game-integrity-integration
echo "ok kind-test-game-integrity-adapter"
