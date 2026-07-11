#!/usr/bin/env bash
# Explicit, networked Tournament Postgres store integration against kind.
# Creates an ephemeral DB named unoarena_tournament_test_<random>, port-forwards
# postgres-tournament (local port 0), runs make test-tournament-integration as
# tournament_runtime against ONLY that DB, then terminates backends + DROP DATABASE.
# Never accepts caller-supplied database names. Never applies/resets/deploys.
# Cleanup touches only this run's DB and this script's child port-forward.
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
SERVICE_NAME="postgres-tournament"
ADMIN_USER="tournament_admin"
RUNTIME_USER="tournament_runtime"
SAFE_DB_PREFIX="unoarena_tournament_test_"

PF_PID=""
PF_LOG=""
LOCAL_PORT=""
DB_NAME=""
DB_CREATED=0
CLEANUP_DROP_FAILED=0

# Reject any attempt to inject a database name from the environment.
refuse_caller_db_name() {
  if [[ -n "${TOURNAMENT_TEST_DATABASE:-}" ]]; then
    die "refusing caller-supplied TOURNAMENT_TEST_DATABASE; harness generates its own name"
  fi
  if [[ -n "${TOURNAMENT_INTEGRATION_DATABASE:-}" ]]; then
    die "refusing caller-supplied TOURNAMENT_INTEGRATION_DATABASE; harness generates its own name"
  fi
  if [[ -n "${TOURNAMENT_POSTGRES_DATABASE:-}" ]]; then
    die "refusing caller-supplied TOURNAMENT_POSTGRES_DATABASE; harness generates its own name"
  fi
}

# Generated names only: prefix + lowercase alphanumeric suffix (SQL-identifier safe).
validate_generated_db_name() {
  local name="$1"
  if [[ ! "${name}" =~ ^unoarena_tournament_test_[a-z0-9]+$ ]]; then
    die "refusing unsafe generated database identifier: ${name}"
  fi
  case "${name}" in
    tournament|postgres|template0|template1|"")
      die "refusing forbidden database identifier: ${name}"
      ;;
  esac
}

generate_safe_db_name() {
  local suffix
  # 16 lowercase hex chars from /dev/urandom (no caller input).
  suffix="$(LC_ALL=C tr -dc 'a-f0-9' </dev/urandom | head -c 16)"
  [[ "${#suffix}" -eq 16 ]] || die "failed to generate random database suffix"
  printf '%s%s' "${SAFE_DB_PREFIX}" "${suffix}"
}

# Quote a validated identifier for embedding in SQL (double-quote + escape).
sql_quote_ident() {
  local ident="$1"
  validate_generated_db_name "${ident}"
  # Identifiers are already restricted to [a-z0-9_]; still quote for clarity.
  printf '"%s"' "${ident}"
}

psql_admin() {
  # stdin SQL or -c via args; always tournament_admin against postgres maintenance DB.
  kubectl -n "${KIND_NAMESPACE}" exec -i deploy/postgres-tournament -- \
    psql -U "${ADMIN_USER}" -d postgres -v ON_ERROR_STOP=1 "$@"
}

create_temp_database() {
  local quoted
  quoted="$(sql_quote_ident "${DB_NAME}")"
  # OWNER tournament_runtime so the DML role can apply test schema DDL in an empty DB.
  psql_admin -c "CREATE DATABASE ${quoted} OWNER ${RUNTIME_USER};"
  DB_CREATED=1
}

drop_temp_database() {
  local quoted
  quoted="$(sql_quote_ident "${DB_NAME}")"
  # Terminate only backends on THIS database, then DROP it.
  psql_admin -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${DB_NAME}' AND pid <> pg_backend_pid();"
  psql_admin -c "DROP DATABASE IF EXISTS ${quoted};"
}

cleanup() {
  # Drop ephemeral DB first (while port-forward may still help kubectl exec — exec is in-cluster).
  if [[ "${DB_CREATED}" -eq 1 && -n "${DB_NAME}" ]]; then
    if ! drop_temp_database; then
      echo "error: failed to DROP DATABASE ${DB_NAME}" >&2
      CLEANUP_DROP_FAILED=1
    else
      DB_CREATED=0
    fi
  fi

  # Only signal our own still-tracked shell job. Never kill by port or reused PID.
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
    # Surface cleanup failure even if tests passed.
    exit 1
  fi
}
trap cleanup EXIT

refuse_caller_db_name

DB_NAME="$(generate_safe_db_name)"
validate_generated_db_name "${DB_NAME}"

echo "kind context ok: $(kubectl config current-context)"
echo "creating ephemeral database ${DB_NAME} (OWNER ${RUNTIME_USER})"
create_temp_database

RUNTIME_PASS="$(kubectl -n "${KIND_NAMESPACE}" get secret uno-arena-local-credentials \
  -o jsonpath='{.data.tournament-runtime-password}' | base64 -d)"
[[ -n "${RUNTIME_PASS}" ]] || die "missing tournament-runtime-password in uno-arena-local-credentials"

PF_LOG="$(mktemp "${TMPDIR:-/tmp}/tournament-pg-pf.XXXXXX")"
LOCAL_PORT_SPEC="0:${REMOTE_PORT}"

echo "port-forward local ${LOCAL_PORT_SPEC} -> svc/${SERVICE_NAME}:${REMOTE_PORT}"
kubectl -n "${KIND_NAMESPACE}" port-forward --address=127.0.0.1 "svc/${SERVICE_NAME}" "${LOCAL_PORT_SPEC}" >"${PF_LOG}" 2>&1 &
PF_PID=$!

ready=0
for _ in $(seq 1 40); do
  if ! kill -0 "${PF_PID}" 2>/dev/null; then
    die "postgres-tournament port-forward exited early (see ${PF_LOG})"
  fi
  if jobs -pr 2>/dev/null | grep -qx "${PF_PID}"; then
    if parsed="$(grep -E "Forwarding from 127\\.0\\.0\\.1:[0-9]+ -> ${REMOTE_PORT}" "${PF_LOG}" 2>/dev/null | head -n1 || true)" \
      && [[ -n "${parsed}" ]]; then
      LOCAL_PORT="$(sed -nE "s/.*Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${REMOTE_PORT}.*/\\1/p" <<<"${parsed}")"
      if [[ -n "${LOCAL_PORT}" ]] && [[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] \
        && (( LOCAL_PORT >= 1 && LOCAL_PORT <= 65535 )); then
        if nc -z 127.0.0.1 "${LOCAL_PORT}" 2>/dev/null; then
          if ! kill -0 "${PF_PID}" 2>/dev/null; then
            die "postgres-tournament port-forward died after port became reachable (see ${PF_LOG})"
          fi
          ready=1
          break
        fi
      fi
    fi
  fi
  sleep 0.25
done
[[ "${ready}" == "1" ]] || die "postgres-tournament port-forward to 127.0.0.1 failed (see ${PF_LOG})"
[[ -n "${LOCAL_PORT}" ]] || die "failed to parse Forwarding from line in ${PF_LOG}"

# Always point at this script's temp DB + port-forward — never reuse a caller URL.
export TOURNAMENT_POSTGRES_URL="postgres://${RUNTIME_USER}:${RUNTIME_PASS}@127.0.0.1:${LOCAL_PORT}/${DB_NAME}?sslmode=disable"

echo "running Tournament store integration against database ${DB_NAME} on 127.0.0.1:${LOCAL_PORT}"
cd "${REPO_ROOT}"
make test-tournament-integration
echo "ok kind-test-tournament-integration"
