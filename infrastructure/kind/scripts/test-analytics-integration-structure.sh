#!/usr/bin/env bash
# Offline structural assertions for test-analytics-integration.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER="${SCRIPT_DIR}/test-analytics-integration.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "${ADAPTER}" ]] || die "adapter script missing: ${ADAPTER}"

if ! grep -E 'assert_kind_context' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must call assert_kind_context"
fi

if ! grep -F 'unoarena_analytics_test_' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use unoarena_analytics_test_ prefix"
fi
if ! grep -E 'refuse_caller_db_name|ANALYTICS_TEST_DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must refuse caller-supplied database name env vars"
fi
if grep -E 'DB_NAME="\$\{ANALYTICS_|DB_NAME="\$\{.*DATABASE|DB_NAME="\$\{CLICKHOUSE_DB' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not assign DB_NAME from caller environment"
fi

if ! grep -E 'validate_generated_db_name' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must validate generated database identifiers"
fi
if ! grep -F 'unoarena_analytics_test_[a-f0-9]+' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must regex-validate generated DB names as unoarena_analytics_test_[a-f0-9]+"
fi

if ! grep -F 'clickhouse_admin' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use clickhouse_admin for CREATE/DROP DATABASE"
fi
if ! grep -E 'CREATE DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must CREATE DATABASE for the ephemeral test DB"
fi
if ! grep -E 'kubectl.*exec' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl exec for admin SQL"
fi

if ! grep -E '0:\$\{REMOTE_PORT\}|0:8123' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward with local port 0"
fi
if ! grep -E 'port-forward[[:space:]].*--address=127\.0\.0\.1' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward --address=127.0.0.1"
fi
if ! grep -E 'svc/\$\{SERVICE_NAME\}|svc/clickhouse' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must port-forward svc/clickhouse"
fi
if ! grep -E 'PF_PID=\$!' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must record port-forward child as PF_PID=\$!"
fi
if ! grep -E 'jobs[[:space:]]+-pr' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must check jobs -pr membership before kill"
fi
if ! grep -E 'kill[[:space:]]+"\$\{PF_PID\}"|kill[[:space:]]+"\$PF_PID"' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must kill only PF_PID"
fi
if grep -E 'lsof' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not use lsof"
fi

if ! grep -F 'analytics_runtime' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use analytics_runtime for the test DSN"
fi
if ! grep -F 'ANALYTICS_CLICKHOUSE_URL' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export ANALYTICS_CLICKHOUSE_URL"
fi
if ! grep -F 'ANALYTICS_CLICKHOUSE_DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export ANALYTICS_CLICKHOUSE_DATABASE"
fi
if ! grep -E 'make[[:space:]]+test-analytics-integration' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must run make test-analytics-integration"
fi

if ! grep -E 'trap[[:space:]]+cleanup[[:space:]]+EXIT' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must trap cleanup EXIT"
fi
if ! grep -E 'DROP DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must DROP DATABASE"
fi
if grep -E 'DROP DATABASE[^
]*analytics[^_]' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not DROP the production analytics database"
fi
if grep -E 'DROP DATABASE[^;]*\|\|[[:space:]]*true' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not ignore DROP DATABASE errors with || true"
fi
if ! grep -E 'CLEANUP_DROP_FAILED|failed to DROP DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must surface DROP DATABASE cleanup failures"
fi

if ! grep -E 'mktemp' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must capture port-forward output in a per-run mktemp log"
fi

runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/analytics-int-struct.XXXXXX")"
cleanup_runtime() { rm -rf "${runtime_dir}"; }
trap cleanup_runtime EXIT

cat >"${runtime_dir}/helpers.sh" <<'EOF'
set -euo pipefail
die() { echo "die: $*" >&2; exit 1; }
SAFE_DB_PREFIX="unoarena_analytics_test_"
EOF
sed -n '/^validate_generated_db_name()/,/^}/p' "${ADAPTER}" >>"${runtime_dir}/helpers.sh"
sed -n '/^refuse_caller_db_name()/,/^}/p' "${ADAPTER}" >>"${runtime_dir}/helpers.sh"

assert_name_rejected() {
  local name="$1"
  if bash -c "source '${runtime_dir}/helpers.sh'; validate_generated_db_name '${name}'" >/dev/null 2>&1; then
    die "runtime must reject database name: ${name}"
  fi
}
assert_name_accepted() {
  local name="$1"
  if ! bash -c "source '${runtime_dir}/helpers.sh'; validate_generated_db_name '${name}'" >/dev/null 2>&1; then
    die "runtime must accept database name: ${name}"
  fi
}

assert_name_rejected "analytics"
assert_name_rejected "default"
assert_name_rejected "system"
assert_name_rejected ""
assert_name_rejected "unoarena_analytics_test_"
assert_name_rejected "unoarena_analytics_test_ABC"
assert_name_rejected "unoarena_analytics_test_ab-cd"
assert_name_rejected "evil;drop"
assert_name_accepted "unoarena_analytics_test_abc123"
assert_name_accepted "unoarena_analytics_test_deadbeefcafebabe"

if bash -c "source '${runtime_dir}/helpers.sh'; ANALYTICS_TEST_DATABASE=nope refuse_caller_db_name" >/dev/null 2>&1; then
  die "refuse_caller_db_name must reject ANALYTICS_TEST_DATABASE"
fi

echo "ok kind-test-analytics-integration-structure"
