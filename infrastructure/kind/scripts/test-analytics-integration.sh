#!/usr/bin/env bash
# Explicit, networked Analytics ClickHouse store integration against kind.
# Creates an ephemeral DB named unoarena_analytics_test_<random hex>, port-forwards
# clickhouse (local port 0), runs make test-analytics-integration as analytics_runtime
# against ONLY that DB, then DROP DATABASE.
# Never accepts caller-supplied database names. Never applies/resets/deploys.
# Cleanup touches only this run's DB and this script's child port-forward.
# Never drops or mutates the production analytics database.
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

REMOTE_PORT=8123
SERVICE_NAME="clickhouse"
ADMIN_USER="clickhouse_admin"
RUNTIME_USER="analytics_runtime"
SAFE_DB_PREFIX="unoarena_analytics_test_"

PF_PID=""
PF_LOG=""
LOCAL_PORT=""
DB_NAME=""
DB_CREATED=0
CLEANUP_DROP_FAILED=0

refuse_caller_db_name() {
  if [[ -n "${ANALYTICS_TEST_DATABASE:-}" ]]; then
    die "refusing caller-supplied ANALYTICS_TEST_DATABASE; harness generates its own name"
  fi
  if [[ -n "${ANALYTICS_INTEGRATION_DATABASE:-}" ]]; then
    die "refusing caller-supplied ANALYTICS_INTEGRATION_DATABASE; harness generates its own name"
  fi
  if [[ -n "${ANALYTICS_CLICKHOUSE_DATABASE:-}" ]]; then
    die "refusing caller-supplied ANALYTICS_CLICKHOUSE_DATABASE; harness generates its own name"
  fi
  if [[ -n "${CLICKHOUSE_DB:-}" ]]; then
    die "refusing caller-supplied CLICKHOUSE_DB; harness generates its own name"
  fi
}

validate_generated_db_name() {
  local name="$1"
  if [[ ! "${name}" =~ ^unoarena_analytics_test_[a-f0-9]+$ ]]; then
    die "refusing unsafe generated database identifier: ${name}"
  fi
  case "${name}" in
    analytics|default|system|"")
      die "refusing forbidden database identifier: ${name}"
      ;;
  esac
}

generate_safe_db_name() {
  local suffix
  suffix="$(LC_ALL=C tr -dc 'a-f0-9' </dev/urandom | head -c 16)"
  [[ "${#suffix}" -eq 16 ]] || die "failed to generate random database suffix"
  printf '%s%s' "${SAFE_DB_PREFIX}" "${suffix}"
}

ch_admin_query() {
  local q="$1"
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/clickhouse -- \
    clickhouse-client --user "${ADMIN_USER}" --password "${ADMIN_PASS}" --query "${q}"
}

create_temp_database() {
  validate_generated_db_name "${DB_NAME}"
  ch_admin_query "CREATE DATABASE IF NOT EXISTS ${DB_NAME}"
  DB_CREATED=1
}

drop_temp_database() {
  validate_generated_db_name "${DB_NAME}"
  # Never touch analytics / default / system.
  case "${DB_NAME}" in
    analytics|default|system)
      die "refusing DROP of forbidden database ${DB_NAME}"
      ;;
  esac
  ch_admin_query "DROP DATABASE IF EXISTS ${DB_NAME}"
}

cleanup() {
  if [[ "${DB_CREATED}" -eq 1 && -n "${DB_NAME}" ]]; then
    if ! drop_temp_database; then
      echo "error: failed to DROP DATABASE ${DB_NAME}" >&2
      CLEANUP_DROP_FAILED=1
    else
      DB_CREATED=0
    fi
  fi

  if [[ -n "${PF_PID}" ]]; then
    if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
      kill "${PF_PID}" 2>/dev/null || true
    fi
    wait "${PF_PID}" 2>/dev/null || true
  fi
  if [[ -n "${PF_LOG}" && -f "${PF_LOG}" ]]; then
    rm -f "${PF_LOG}"
  fi

  if [[ "${CLEANUP_DROP_FAILED}" -eq 1 ]]; then
    exit 1
  fi
}
trap cleanup EXIT

refuse_caller_db_name

DB_NAME="$(generate_safe_db_name)"
validate_generated_db_name "${DB_NAME}"

ADMIN_PASS="$(kubectl -n "${KIND_NAMESPACE}" get secret uno-arena-local-credentials \
  -o jsonpath='{.data.clickhouse-admin-password}' | base64 -d)"
RUNTIME_PASS="$(kubectl -n "${KIND_NAMESPACE}" get secret uno-arena-local-credentials \
  -o jsonpath='{.data.analytics-runtime-password}' | base64 -d)"
[[ -n "${ADMIN_PASS}" ]] || die "missing clickhouse-admin-password"
[[ -n "${RUNTIME_PASS}" ]] || die "missing analytics-runtime-password"

echo "kind context ok: $(kubectl config current-context)"
echo "creating ephemeral database ${DB_NAME}"
create_temp_database

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/analytics-ch-pf.XXXXXX")"
LOCAL_PORT_SPEC="0:${REMOTE_PORT}"

echo "port-forward local ${LOCAL_PORT_SPEC} -> svc/${SERVICE_NAME}:${REMOTE_PORT}"
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 "svc/${SERVICE_NAME}" "${LOCAL_PORT_SPEC}" >"${PF_LOG}" 2>&1 &
PF_PID=$!

ready=0
for _ in $(seq 1 40); do
  if ! kill -0 "${PF_PID}" 2>/dev/null; then
    die "clickhouse port-forward exited early (see ${PF_LOG})"
  fi
  if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
    if parsed="$(grep -E "Forwarding from 127\\.0\\.0\\.1:[0-9]+ -> ${REMOTE_PORT}" "${PF_LOG}" 2>/dev/null | head -n1 || true)" \
      && [[ -n "${parsed}" ]]; then
      LOCAL_PORT="$(sed -nE "s/.*Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${REMOTE_PORT}.*/\\1/p" <<<"${parsed}")"
      if [[ -n "${LOCAL_PORT}" ]] && [[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] \
        && (( LOCAL_PORT >= 1 && LOCAL_PORT <= 65535 )); then
        if nc -z 127.0.0.1 "${LOCAL_PORT}" 2>/dev/null; then
          if ! kill -0 "${PF_PID}" 2>/dev/null; then
            die "clickhouse port-forward died after port became reachable (see ${PF_LOG})"
          fi
          ready=1
          break
        fi
      fi
    fi
  fi
  sleep 0.25
done
[[ "${ready}" == "1" ]] || die "clickhouse port-forward to 127.0.0.1 failed (see ${PF_LOG})"
[[ -n "${LOCAL_PORT}" ]] || die "failed to parse Forwarding from line in ${PF_LOG}"

# Always point at this script's temp DB + port-forward — never reuse a caller URL/DB.
export ANALYTICS_CLICKHOUSE_URL="http://127.0.0.1:${LOCAL_PORT}"
export ANALYTICS_CLICKHOUSE_USER="${RUNTIME_USER}"
export ANALYTICS_CLICKHOUSE_PASSWORD="${RUNTIME_PASS}"
export ANALYTICS_CLICKHOUSE_DATABASE="${DB_NAME}"
export ANALYTICS_CLICKHOUSE_ADMIN_USER="${ADMIN_USER}"
export ANALYTICS_CLICKHOUSE_ADMIN_PASSWORD="${ADMIN_PASS}"
# Clear any ambiguous aliases so Go tests cannot pick production analytics.
unset CLICKHOUSE_URL CLICKHOUSE_USER CLICKHOUSE_PASSWORD CLICKHOUSE_DB || true

echo "running Analytics store integration against database ${DB_NAME} on 127.0.0.1:${LOCAL_PORT}"
cd "${REPO_ROOT}"
make test-analytics-integration
echo "ok kind-test-analytics-integration"
