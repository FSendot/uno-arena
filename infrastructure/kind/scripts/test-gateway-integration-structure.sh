#!/usr/bin/env bash
# Offline structure checks for Gateway Redis integration harness.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

fail=0
check() {
  local file="$1"
  local needle="$2"
  if ! grep -qF "${needle}" "${file}"; then
    echo "FAIL: ${file} missing ${needle}" >&2
    fail=1
  fi
}

INTEG="${SCRIPT_DIR}/test-gateway-integration.sh"
[[ -f "${INTEG}" ]] || { echo "FAIL: missing ${INTEG}" >&2; fail=1; }

check "${INTEG}" "assert_kind_context"
check "${INTEG}" "SAFE_RATE_DB=11"
check "${INTEG}" "SAFE_PLAYER_DB=12"
check "${INTEG}" "SAFE_SPECTATOR_DB=13"
check "${INTEG}" "GATEWAY_REDIS_URL"
check "${INTEG}" "test-gateway-integration"
check "${INTEG}" "refusing caller-supplied"

# Must not target Room timer DB 2 / Spectator DB 5 / Gateway rate DB 6 as test DBs.
if grep -E 'SAFE_(RATE|PLAYER|SPECTATOR)_DB=2\b|SAFE_(RATE|PLAYER|SPECTATOR)_DB=5\b|SAFE_(RATE|PLAYER|SPECTATOR)_DB=6\b' "${INTEG}" >/dev/null 2>&1; then
  echo "FAIL: integration harness must not reuse production Redis DBs 2/5/6" >&2
  fail=1
fi

[[ "${fail}" -eq 0 ]] || exit 1
echo "ok gateway-integration-structure"
