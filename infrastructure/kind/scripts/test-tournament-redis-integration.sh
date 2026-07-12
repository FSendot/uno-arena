#!/usr/bin/env bash
# Explicit, networked Tournament bracket Redis integration against kind Redis DB 14.
# (Default Redis databases=16 → valid indices 0..15; DB 16 is out of range.)
# Uses a random key prefix and deletes only that prefix through the Go test cleanup.
set -euo pipefail
set -m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd kubectl
require_cmd go
require_cmd nc
assert_kind_context

PF_PID=""
PF_LOG="$(mktemp "${TMPDIR:-/tmp}/tournament-redis-pf.XXXXXX")"
cleanup() {
  if [[ -n "${PF_PID}" ]] && jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  rm -f "${PF_LOG}"
}
trap cleanup EXIT

kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 svc/redis 0:6379 >"${PF_LOG}" 2>&1 &
PF_PID=$!

LOCAL_PORT=""
for _ in $(seq 1 40); do
  kill -0 "${PF_PID}" 2>/dev/null || die "redis port-forward exited early (see ${PF_LOG})"
  line="$(grep -E 'Forwarding from 127\.0\.0\.1:[0-9]+ -> 6379' "${PF_LOG}" | head -n1 || true)"
  if [[ -n "${line}" ]]; then
    LOCAL_PORT="$(sed -nE 's/.*127\.0\.0\.1:([0-9]+) -> 6379.*/\1/p' <<<"${line}")"
    if [[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] && nc -z 127.0.0.1 "${LOCAL_PORT}" 2>/dev/null; then
      break
    fi
  fi
  sleep 0.25
done
[[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] || die "failed to establish Redis port-forward (see ${PF_LOG})"

export TOURNAMENT_REDIS_URL="redis://127.0.0.1:${LOCAL_PORT}/14"
cd "${REPO_ROOT}/services/tournament-orchestration/src"
GOWORK=off GOPROXY=off GOSUMDB=off go test -count=1 -tags=redis_integration -timeout 120s ./store/ -run 'RealRedis'
echo "ok kind-test-tournament-redis-integration"
