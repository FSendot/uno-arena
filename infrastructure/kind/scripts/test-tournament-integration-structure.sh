#!/usr/bin/env bash
# Offline structural assertions for test-tournament-integration.sh.
# Ensures ephemeral DB naming, no caller-supplied DB names, quoted identifiers,
# tournament_admin CREATE DATABASE OWNER tournament_runtime, local port 0 PF,
# runtime DSN only, and trap cleanup of only this run's DB + PF child.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER="${SCRIPT_DIR}/test-tournament-integration.sh"

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
if ! grep -F 'unoarena_tournament_test_' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use unoarena_tournament_test_ prefix"
fi
if ! grep -E 'refuse_caller_db_name|TOURNAMENT_TEST_DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must refuse caller-supplied database name env vars"
fi
if grep -E 'DB_NAME="\$\{TOURNAMENT_|DB_NAME="\$\{.*DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not assign DB_NAME from caller environment"
fi

# Identifier validation + SQL quoting.
if ! grep -E 'validate_generated_db_name' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must validate generated database identifiers"
fi
if ! grep -F 'sql_quote_ident' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must quote generated SQL identifiers via sql_quote_ident"
fi
if ! grep -F 'unoarena_tournament_test_[a-z0-9]+' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must regex-validate generated DB names as unoarena_tournament_test_[a-z0-9]+"
fi

# CREATE DATABASE via kubectl exec as tournament_admin with OWNER tournament_runtime.
if ! grep -F 'tournament_admin' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use tournament_admin for CREATE/DROP DATABASE"
fi
if ! grep -E 'CREATE DATABASE' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must CREATE DATABASE for the ephemeral test DB"
fi
if ! grep -E 'OWNER[[:space:]]+\$\{RUNTIME_USER\}|OWNER tournament_runtime' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must CREATE DATABASE ... OWNER tournament_runtime"
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
if ! grep -E 'svc/\$\{SERVICE_NAME\}|svc/postgres-tournament' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must port-forward svc/postgres-tournament"
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

# Runtime user DSN to temp DB only; drives make test-tournament-integration
# which runs store then main-package service durable integration sequentially.
if ! grep -F 'tournament_runtime' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must use tournament_runtime for the test DSN"
fi
if ! grep -F 'TOURNAMENT_POSTGRES_URL' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must export TOURNAMENT_POSTGRES_URL"
fi
if ! grep -E '127\.0\.0\.1:\$\{LOCAL_PORT\}/\$\{DB_NAME\}' "${ADAPTER}" >/dev/null 2>&1; then
  die "TOURNAMENT_POSTGRES_URL must target allocated LOCAL_PORT and generated DB_NAME"
fi
if grep -E 'TOURNAMENT_POSTGRES_URL="\$\{TOURNAMENT_POSTGRES_URL:-' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must not honor a pre-set TOURNAMENT_POSTGRES_URL over its ephemeral DB"
fi
if ! grep -E 'make[[:space:]]+test-tournament-integration' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter must run make test-tournament-integration"
fi
if ! grep -E 'store \+ main-package|store and main-package|store \+ main' "${ADAPTER}" >/dev/null 2>&1; then
  die "adapter comments/output must mention store and main-package service integration lanes"
fi

# Makefile must run store integration then main-package (.) sequentially.
MAKEFILE="$(cd "${SCRIPT_DIR}/../../.." && pwd)/Makefile"
[[ -f "${MAKEFILE}" ]] || die "Makefile missing: ${MAKEFILE}"
recipe="$(awk '
  /^test-tournament-integration:/ { in_t=1; next }
  in_t && /^[^#[:space:]]/ { exit }
  in_t { print }
' "${MAKEFILE}")"
if ! grep -E '\./store/\.\.\.' <<<"${recipe}" >/dev/null 2>&1; then
  die "Makefile test-tournament-integration must run ./store/... first"
fi
if ! grep -E '[[:space:]]\.$' <<<"${recipe}" >/dev/null 2>&1; then
  die "Makefile test-tournament-integration must run main-package service integration (.)"
fi
if [[ "$(grep -Fc -- '-parallel 2 -timeout 300s' <<<"${recipe}")" -ne 2 ]]; then
  die "both Tournament integration lanes must bound local parallelism and allow the reviewed five-minute budget"
fi
store_line="$(grep -n '\./store/\.\.\.' <<<"${recipe}" | head -n1 | cut -d: -f1)"
main_line="$(grep -nE '[[:space:]]\.$' <<<"${recipe}" | head -n1 | cut -d: -f1)"
[[ -n "${store_line}" && -n "${main_line}" ]] || die "Makefile must define both store and main integration lanes"
(( store_line < main_line )) || die "Makefile must run ./store/... before main-package ."

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
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/tournament-int-struct.XXXXXX")"
cleanup_runtime() { rm -rf "${runtime_dir}"; }
trap cleanup_runtime EXIT

cat >"${runtime_dir}/helpers.sh" <<'EOF'
set -euo pipefail
die() { echo "die: $*" >&2; exit 1; }
SAFE_DB_PREFIX="unoarena_tournament_test_"
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

assert_name_rejected "tournament"
assert_name_rejected "postgres"
assert_name_rejected "template0"
assert_name_rejected "template1"
assert_name_rejected ""
assert_name_rejected "unoarena_tournament_test_"
assert_name_rejected "unoarena_tournament_test_ABC"
assert_name_rejected "unoarena_tournament_test_ab-cd"
assert_name_rejected "evil;drop"
assert_name_accepted "unoarena_tournament_test_abc123"
assert_name_accepted "unoarena_tournament_test_deadbeefcafebabe"

quoted="$(bash -c "source '${runtime_dir}/helpers.sh'; sql_quote_ident 'unoarena_tournament_test_abc123'")"
[[ "${quoted}" == '"unoarena_tournament_test_abc123"' ]] || die "sql_quote_ident produced ${quoted}"

# refuse_caller_db_name must die when env is set.
if bash -c "source '${runtime_dir}/helpers.sh'; TOURNAMENT_TEST_DATABASE=nope refuse_caller_db_name" >/dev/null 2>&1; then
  die "refuse_caller_db_name must reject TOURNAMENT_TEST_DATABASE"
fi

echo "ok kind-test-tournament-integration-structure"
