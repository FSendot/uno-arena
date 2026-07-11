#!/usr/bin/env bash
# Offline structural assertions for test-room-integration.sh.
# Ensures ephemeral DB naming, no caller-supplied DB names, quoted identifiers,
# room_admin CREATE DATABASE OWNER room_runtime, local port 0 PF,
# runtime DSN only, and trap cleanup of only this run's DB + PF child.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER="${SCRIPT_DIR}/test-room-integration.sh"

die() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "${ADAPTER}" ]] || die "adapter script missing: ${ADAPTER}"

# Must assert kind context.
if ! grep -E 'assert_kind_context' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must call assert_kind_context"
fi

# Must generate names with the required prefix; must not accept env DB names.
if ! grep -F 'unoarena_room_gameplay_test_' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use unoarena_room_gameplay_test_ prefix"
fi
if ! grep -E 'refuse_caller_db_name|ROOM_TEST_DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must refuse caller-supplied database name env vars"
fi
if grep -E 'DB_NAME="\$\{ROOM_|DB_NAME="\$\{.*DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not assign DB_NAME from caller environment"
fi

# Identifier validation + SQL quoting.
if ! grep -E 'validate_generated_db_name' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must validate generated database identifiers"
fi
if ! grep -F 'sql_quote_ident' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must quote generated SQL identifiers via sql_quote_ident"
fi
if ! grep -F 'unoarena_room_gameplay_test_[a-z0-9]+' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must regex-validate generated DB names as unoarena_room_gameplay_test_[a-z0-9]+"
fi

# CREATE DATABASE via kubectl exec as room_admin with OWNER room_runtime.
if ! grep -F 'room_admin' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use room_admin for CREATE/DROP DATABASE"
fi
if ! grep -E 'CREATE DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must CREATE DATABASE for the ephemeral test DB"
fi
if ! grep -E 'OWNER[[:space:]]+\$\{RUNTIME_USER\}|OWNER room_runtime' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must CREATE DATABASE ... OWNER room_runtime"
fi
if ! grep -E 'kubectl.*exec' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl exec for admin SQL"
fi

# Port-forward postgres on local port 0; cleanup only PF_PID.
if ! grep -E '0:\$\{REMOTE_PORT\}|0:5432' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward with local port 0"
fi
if ! grep -E 'port-forward[[:space:]].*--address=127\.0\.0\.1' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must kubectl port-forward --address=127.0.0.1"
fi
if ! grep -E 'svc/\$\{SERVICE_NAME\}|svc/postgres-room-gameplay' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must port-forward svc/postgres-room-gameplay"
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

# Second port-forward to Redis (local port 0); isolated DB 15 + key prefix.
if ! grep -E 'svc/\$\{REDIS_SERVICE_NAME\}|svc/redis' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must port-forward svc/redis"
fi
if ! grep -E 'REDIS_PF_PID=\$!' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must record redis port-forward child as REDIS_PF_PID=\$!"
fi
if ! grep -E '0:\$\{REDIS_REMOTE_PORT\}|0:6379' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must redis port-forward with local port 0"
fi
if ! grep -E 'ROOM_REDIS_URL' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export ROOM_REDIS_URL"
fi
if ! grep -E '/15"|/15$' "${ADAPTER}" >/dev/null 2>&1 && ! grep -F '/${REDIS_TEST_DB}' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must target Redis logical DB 15"
fi
if ! grep -E 'ROOM_REDIS_KEY_PREFIX' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export ROOM_REDIS_KEY_PREFIX"
fi
if ! grep -E 'FLUSHDB' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must FLUSHDB only the isolated redis test DB"
fi
if grep -E 'FLUSHALL' "${ADAPTER}" >/dev/null 2>&1; then
  # Comments mentioning the forbidden command are fine; executable use is not.
  if grep -E '^[^#]*FLUSHALL' "${ADAPTER}" >/dev/null 2>&1; then
    die "adapter must never FLUSHALL"
  fi
fi
if ! grep -E 'kill[[:space:]]+"\$\{REDIS_PF_PID\}"' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must kill REDIS_PF_PID"
fi
if grep -E 'ROOM_REDIS_URL="\$\{ROOM_REDIS_URL:-' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not honor a pre-set ROOM_REDIS_URL over its port-forward"
fi

# Runtime user DSN to temp DB only; drives make test-room-integration.
if ! grep -F 'room_runtime' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use room_runtime for the test DSN"
fi
if ! grep -F 'ROOM_POSTGRES_URL' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export ROOM_POSTGRES_URL"
fi
if ! grep -E '127\.0\.0\.1:\$\{LOCAL_PORT\}/\$\{DB_NAME\}' "${ADAPTER}" >/dev/null 2>&1; then
  die "ROOM_POSTGRES_URL must target allocated LOCAL_PORT and generated DB_NAME"
fi
if grep -E 'ROOM_POSTGRES_URL="\$\{ROOM_POSTGRES_URL:-' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not honor a pre-set ROOM_POSTGRES_URL over its ephemeral DB"
fi
if ! grep -E 'make[[:space:]]+test-room-integration' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must run make test-room-integration"
fi

# Trap must terminate backends + DROP DATABASE for this DB only.
if ! grep -E 'trap[[:space:]]+cleanup[[:space:]]+EXIT' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must trap cleanup EXIT"
fi
if ! grep -F 'pg_terminate_backend' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must pg_terminate_backend connections to the temp DB"
fi
if ! grep -E 'DROP DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "cleanup must DROP DATABASE"
fi
if ! grep -F "datname = '\${DB_NAME}'" "${ADAPTER}" >/dev/null 2>&1; then
  die "pg_terminate_backend must filter datname to this run's DB_NAME only"
fi

# DROP failures must not be silently ignored.
if grep -E 'DROP DATABASE[^;]*\|\|[[:space:]]*true' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not ignore DROP DATABASE errors with || true"
fi
if ! grep -E 'CLEANUP_DROP_FAILED|failed to DROP DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must surface DROP DATABASE cleanup failures"
fi

# Per-run mktemp log.
if ! grep -E 'mktemp' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must capture port-forward output in a per-run mktemp log"
fi

# Runtime validation of generate/validate helpers (no kubectl / no cluster).
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/room-int-struct.XXXXXX")"
cleanup_runtime() { rm -rf "${runtime_dir}"; }
trap cleanup_runtime EXIT

cat >"${runtime_dir}/helpers.sh" <<'EOF'
set -euo pipefail
die() { echo "die: $*" >&2; exit 1; }
SAFE_DB_PREFIX="unoarena_room_gameplay_test_"
EOF
sed -n '/^validate_generated_db_name()/,/^}/p' "${ADAPTER}" >>"${runtime_dir}/helpers.sh"
sed -n '/^sql_quote_ident()/,/^}/p' "${ADAPTER}" >>"${runtime_dir}/helpers.sh"
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

assert_name_rejected "room_gameplay"
assert_name_rejected "postgres"
assert_name_rejected "template0"
assert_name_rejected "template1"
assert_name_rejected ""
assert_name_rejected "unoarena_room_gameplay_test_"
assert_name_rejected "unoarena_room_gameplay_test_ABC"
assert_name_rejected "unoarena_room_gameplay_test_ab-cd"
assert_name_rejected "evil;drop"
assert_name_accepted "unoarena_room_gameplay_test_abc123"
assert_name_accepted "unoarena_room_gameplay_test_deadbeefcafebabe"

quoted="$(bash -c "source '${runtime_dir}/helpers.sh'; sql_quote_ident 'unoarena_room_gameplay_test_abc123'")"
[[ "${quoted}" == '"unoarena_room_gameplay_test_abc123"' ]] || die "sql_quote_ident produced ${quoted}"

# refuse_caller_db_name must die when env is set.
if bash -c "source '${runtime_dir}/helpers.sh'; ROOM_TEST_DATABASE=nope refuse_caller_db_name" >/dev/null 2>&1; then
  die "refuse_caller_db_name must reject ROOM_TEST_DATABASE"
fi

echo "ok kind-test-room-integration-structure"
