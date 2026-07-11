#!/usr/bin/env bash
# Explicit, networked Room Gameplay Postgres + Redis store integration against kind.
# Creates an ephemeral DB named unoarena_room_gameplay_test_<random>, port-forwards
# postgres-room-gameplay and redis (local port 0 each), runs make test-room-integration as
# room_runtime against ONLY that DB + Redis DB 15 with a per-run key prefix, then
# terminates backends + DROP DATABASE + FLUSHDB 15.
# Never accepts caller-supplied database names. Never applies/resets/deploys.
# Cleanup touches only this run's DB, Redis DB 15 / prefix, and this script's child PFs.
# Live reset of the authoritative room_gameplay DB / live timer keys is NOT performed here.
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

REMOTE_PORT=5432
SERVICE_NAME="postgres-room-gameplay"
ADMIN_USER="room_admin"
RUNTIME_USER="room_runtime"
SAFE_DB_PREFIX="unoarena_room_gameplay_test_"

REDIS_SERVICE_NAME="redis"
REDIS_REMOTE_PORT=6379
REDIS_TEST_DB=15

PF_PID=""
PF_LOG=""
LOCAL_PORT=""
REDIS_PF_PID=""
REDIS_PF_LOG=""
REDIS_LOCAL_PORT=""
DB_NAME=""
DB_CREATED=0
CLEANUP_DROP_FAILED=0
REDIS_PREFIX=""

refuse_caller_db_name() {
  if [[ -n "${ROOM_TEST_DATABASE:-}" ]]; then
    die "refusing caller-supplied ROOM_TEST_DATABASE; harness generates its own name"
  fi
  if [[ -n "${ROOM_INTEGRATION_DATABASE:-}" ]]; then
    die "refusing caller-supplied ROOM_INTEGRATION_DATABASE; harness generates its own name"
  fi
  if [[ -n "${ROOM_POSTGRES_DATABASE:-}" ]]; then
    die "refusing caller-supplied ROOM_POSTGRES_DATABASE; harness generates its own name"
  fi
}

validate_generated_db_name() {
  local name="$1"
  if [[ ! "${name}" =~ ^unoarena_room_gameplay_test_[a-z0-9]+$ ]]; then
    die "refusing unsafe generated database identifier: ${name}"
  fi
  case "${name}" in
    room_gameplay|postgres|template0|template1|"")
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

sql_quote_ident() {
  local ident="$1"
  validate_generated_db_name "${ident}"
  printf '"%s"' "${ident}"
}

psql_admin() {
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/postgres-room-gameplay -- \
    psql -U "${ADMIN_USER}" -d postgres -v ON_ERROR_STOP=1 "$@"
}

create_temp_database() {
  local quoted
  quoted="$(sql_quote_ident "${DB_NAME}")"
  psql_admin -c "CREATE DATABASE ${quoted} OWNER ${RUNTIME_USER};"
  DB_CREATED=1
}

drop_temp_database() {
  local quoted
  quoted="$(sql_quote_ident "${DB_NAME}")"
  psql_admin -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${DB_NAME}' AND pid <> pg_backend_pid();"
  psql_admin -c "DROP DATABASE IF EXISTS ${quoted};"
}

flush_isolated_redis() {
  if [[ -z "${REDIS_LOCAL_PORT}" ]]; then
    return 0
  fi
  # Only the isolated test logical DB — never FLUSHALL / never DB 0 live keys.
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -h 127.0.0.1 -p "${REDIS_LOCAL_PORT}" -n "${REDIS_TEST_DB}" FLUSHDB >/dev/null 2>&1 || true
  else
    printf 'SELECT %s\r\nFLUSHDB\r\n' "${REDIS_TEST_DB}" | nc -w 2 127.0.0.1 "${REDIS_LOCAL_PORT}" >/dev/null 2>&1 || true
  fi
}

wait_port_forward() {
  local pid="$1"
  local log="$2"
  local remote="$3"
  local label="$4"
  local ready=0
  local parsed local_port=""
  for _ in $(seq 1 40); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      die "${label} port-forward exited early (see ${log})"
    fi
    if jobs -pr 2>/dev/null | grep -qx "${pid}"; then
      if parsed="$(grep -E "Forwarding from 127\\.0\\.0\\.1:[0-9]+ -> ${remote}" "${log}" 2>/dev/null | head -n1 || true)" \
        && [[ -n "${parsed}" ]]; then
        local_port="$(sed -nE "s/.*Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${remote}.*/\\1/p" <<<"${parsed}")"
        if [[ -n "${local_port}" ]] && [[ "${local_port}" =~ ^[0-9]+$ ]] \
          && (( local_port >= 1 && local_port <= 65535 )); then
          if nc -z 127.0.0.1 "${local_port}" 2>/dev/null; then
            if ! kill -0 "${pid}" 2>/dev/null; then
              die "${label} port-forward died after port became reachable (see ${log})"
            fi
            ready=1
            printf '%s' "${local_port}"
            return 0
          fi
        fi
      fi
    fi
    sleep 0.25
  done
  [[ "${ready}" == "1" ]] || die "${label} port-forward to 127.0.0.1 failed (see ${log})"
}

cleanup() {
  flush_isolated_redis

  if [[ "${DB_CREATED}" -eq 1 && -n "${DB_NAME}" ]]; then
    if ! drop_temp_database; then
      echo "error: failed to DROP DATABASE ${DB_NAME}" >&2
      CLEANUP_DROP_FAILED=1
    else
      DB_CREATED=0
    fi
  fi

  if [[ -n "${REDIS_PF_PID}" ]]; then
    if jobs -pr 2>/dev/null | grep -qx "${REDIS_PF_PID}"; then
      kill "${REDIS_PF_PID}" 2>/dev/null || true
    fi
    wait "${REDIS_PF_PID}" 2>/dev/null || true
  fi
  if [[ -n "${REDIS_PF_LOG}" && -f "${REDIS_PF_LOG}" ]]; then
    rm -f "${REDIS_PF_LOG}"
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
REDIS_PREFIX="roomtest_${DB_NAME##*_}:"

echo "kind context ok: $(kubectl config current-context)"
echo "creating ephemeral database ${DB_NAME} (OWNER ${RUNTIME_USER})"
create_temp_database

RUNTIME_PASS="$(kubectl -n "${KIND_NAMESPACE}" get secret uno-arena-local-credentials \
  -o jsonpath='{.data.room-runtime-password}' | base64 -d)"
[[ -n "${RUNTIME_PASS}" ]] || die "missing room-runtime-password in uno-arena-local-credentials"

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/room-pg-pf.XXXXXX")"
LOCAL_PORT_SPEC="0:${REMOTE_PORT}"

echo "port-forward local ${LOCAL_PORT_SPEC} -> svc/${SERVICE_NAME}:${REMOTE_PORT}"
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 "svc/${SERVICE_NAME}" "${LOCAL_PORT_SPEC}" >"${PF_LOG}" 2>&1 &
PF_PID=$!
LOCAL_PORT="$(wait_port_forward "${PF_PID}" "${PF_LOG}" "${REMOTE_PORT}" "postgres-room-gameplay")"
[[ -n "${LOCAL_PORT}" ]] || die "failed to parse postgres Forwarding from line in ${PF_LOG}"

REDIS_PF_LOG="$(mktemp "${TMPDIR:-/tmp}/room-redis-pf.XXXXXX")"
REDIS_LOCAL_PORT_SPEC="0:${REDIS_REMOTE_PORT}"
echo "port-forward local ${REDIS_LOCAL_PORT_SPEC} -> svc/${REDIS_SERVICE_NAME}:${REDIS_REMOTE_PORT}"
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 "svc/${REDIS_SERVICE_NAME}" "${REDIS_LOCAL_PORT_SPEC}" >"${REDIS_PF_LOG}" 2>&1 &
REDIS_PF_PID=$!
REDIS_LOCAL_PORT="$(wait_port_forward "${REDIS_PF_PID}" "${REDIS_PF_LOG}" "${REDIS_REMOTE_PORT}" "redis")"
[[ -n "${REDIS_LOCAL_PORT}" ]] || die "failed to parse redis Forwarding from line in ${REDIS_PF_LOG}"

# Isolated logical DB + prefix — never the live Room timer DB/prefix.
export ROOM_POSTGRES_URL="postgres://${RUNTIME_USER}:${RUNTIME_PASS}@127.0.0.1:${LOCAL_PORT}/${DB_NAME}?sslmode=disable"
export ROOM_REDIS_URL="redis://127.0.0.1:${REDIS_LOCAL_PORT}/${REDIS_TEST_DB}"
export ROOM_REDIS_KEY_PREFIX="${REDIS_PREFIX}"
flush_isolated_redis

echo "running Room store integration against database ${DB_NAME} on 127.0.0.1:${LOCAL_PORT} and redis db ${REDIS_TEST_DB} prefix ${REDIS_PREFIX}"
cd "${REPO_ROOT}"
make test-room-integration
echo "ok kind-test-room-integration"
