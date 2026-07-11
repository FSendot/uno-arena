#!/usr/bin/env bash
# Offline regression: postgres bootstrap must retain standalone set -euo pipefail
# so a failing psql/bootstrap command cannot reach the success echo or exit 0.
# Fixtures prove the production pattern; no database / network dependency.
#
# Note: bash ignores set -e inside commands that are part of an &&/|| list
# (including a whole subshell). Fixtures therefore capture subshell status with
# set +e / $? rather than `) && true || flag=1`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PG_BOOT="${ROOT}/infrastructure/bootstrap/bin/bootstrap-postgres.sh"
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# --- Structural: production script must enable strict shell options ---
if ! grep -qE '^set -euo pipefail$' "$PG_BOOT"; then
  fail "bootstrap-postgres.sh must have a standalone 'set -euo pipefail' line"
fi
if grep -q 'slots\.set' "$PG_BOOT"; then
  fail "bootstrap-postgres.sh still appends set options onto the slots comment"
fi
if ! grep -q 'psql_admin --single-transaction' "$PG_BOOT"; then
  fail "bootstrap-postgres.sh must call psql_admin --single-transaction"
fi
if ! grep -q 'bootstrap postgres context=.*ok version=' "$PG_BOOT"; then
  fail "bootstrap-postgres.sh must print the success echo after psql_admin"
fi

# Success echo must appear after the gated psql_admin invocation.
psql_line="$(grep -n 'psql_admin --single-transaction' "$PG_BOOT" | head -1 | cut -d: -f1)"
ok_line="$(grep -n 'bootstrap postgres context=.*ok version=' "$PG_BOOT" | head -1 | cut -d: -f1)"
if [[ -z "$psql_line" || -z "$ok_line" || "$ok_line" -le "$psql_line" ]]; then
  fail "success echo must follow psql_admin --single-transaction"
fi

# --- Behavioral fixtures ---
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
success_marker="${tmp}/reached_success"

run_subshell() {
  # Runner isolates the fixture so outer set -e does not abort the test harness.
  set +e
  "$@"
  local rc=$?
  set -e
  return "$rc"
}

# Without set -e, a failing "psql" still reaches the success echo (historical bug class).
bad_rc=0
run_subshell bash -c '
  set +e
  psql_admin() { return 1; }
  psql_admin --single-transaction -f /dev/null
  echo "bootstrap postgres context=fixture ok version=v"
  touch "'"$success_marker"'"
  exit 0
' || bad_rc=$?

if [[ "$bad_rc" -ne 0 || ! -f "$success_marker" ]]; then
  fail "fixture setup: expected missing set -e to reach success after failing psql_admin"
fi
rm -f "$success_marker"

# Production pattern: standalone set -euo pipefail aborts before success.
good_rc=0
run_subshell bash -c '
  set -euo pipefail
  psql_admin() { return 1; }
  psql_admin --single-transaction -f /dev/null
  echo "bootstrap postgres context=fixture ok version=v"
  touch "'"$success_marker"'"
  exit 0
' || good_rc=$?

if [[ "$good_rc" -eq 0 ]]; then
  fail "set -euo pipefail did not abort on failing psql_admin (exit 0)"
fi
if [[ -f "$success_marker" ]]; then
  fail "success marker written after failing psql_admin — fail-closed broken"
fi

# Same pattern for a failing pipeline under pipefail (bootstrap may pipe checksums).
pipeline_rc=0
run_subshell bash -c '
  set -euo pipefail
  false | cat >/dev/null
  echo "bootstrap postgres context=fixture ok version=v"
  touch "'"$success_marker"'"
  exit 0
' || pipeline_rc=$?

if [[ "$pipeline_rc" -eq 0 ]]; then
  fail "pipefail did not abort on failing pipeline before success echo"
fi
if [[ -f "$success_marker" ]]; then
  fail "success marker written after failing pipeline"
fi

# Extracted production tail: failing psql_admin must not print ok / exit 0.
prod_out="${tmp}/prod_tail.out"
prod_rc=0
run_subshell bash -c '
  set -euo pipefail
  CONTEXT_NAME=fixture
  EXPECTED_VERSION=v1
  psql_admin() { echo "psql: simulated failure" >&2; return 1; }
  psql_admin --single-transaction -f /dev/null
  echo "bootstrap postgres context=${CONTEXT_NAME} ok version=${EXPECTED_VERSION}"
' >"$prod_out" 2>&1 || prod_rc=$?

if [[ "$prod_rc" -eq 0 ]]; then
  fail "production-shaped tail exited zero after failing psql_admin"
fi
if grep -q 'ok version=' "$prod_out"; then
  fail "production-shaped tail printed success after failing psql_admin"
fi

if [[ "$failures" -ne 0 ]]; then
  echo "${failures} postgres bootstrap fail-closed failure(s)" >&2
  exit 1
fi

echo "ok postgres_bootstrap_fail_closed_test"
exit 0
