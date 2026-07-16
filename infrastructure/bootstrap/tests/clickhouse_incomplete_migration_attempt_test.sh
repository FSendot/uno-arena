#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
mkdir -p "${tmp}/bin" "${tmp}/migrations"
printf '%s\n' 'SELECT 1;' >"${tmp}/migrations/001_init.sql"

cat >"${tmp}/bin/curl" <<'SH'
#!/usr/bin/env bash
query="${*: -1}"
case "${query}" in
  *"system.tables"*) printf '%s\n' 1 ;;
  *"CREATE TABLE IF NOT EXISTS"*) ;;
  *"schema_migration_attempts"*"argMax(status"*"='running'"*) printf '%s\n' 002_expand ;;
  *) printf '%s\n' 1 ;;
esac
SH
cat >"${tmp}/plan" <<'SH'
#!/usr/bin/env bash
touch "${PLAN_WAS_CALLED}"
exit 99
SH
cat >"${tmp}/splitter" <<'SH'
#!/usr/bin/env bash
exit 99
SH
chmod +x "${tmp}/bin/curl" "${tmp}/plan" "${tmp}/splitter"

if PATH="${tmp}/bin:${PATH}" PLAN_WAS_CALLED="${tmp}/plan-called" \
  CLICKHOUSE_HOST=clickhouse.test ADMIN_USER=admin ADMIN_PASSWORD=admin-password \
  BOOTSTRAP_USER=bootstrap BOOTSTRAP_PASSWORD=bootstrap-password CONTEXT_NAME=analytics \
  MIGRATION_DIR="${tmp}/migrations" MIGRATION_PLAN_TOOL="${tmp}/plan" \
  CLICKHOUSE_SQL_SPLITTER="${tmp}/splitter" \
  "${ROOT}/infrastructure/bootstrap/bin/migrate-clickhouse.sh" \
  >"${tmp}/stdout" 2>"${tmp}/stderr"; then
  echo "runner replayed an incomplete ClickHouse migration" >&2
  exit 1
fi
grep -Fq 'incomplete ClickHouse migration attempt requires operator inspection: 002_expand' "${tmp}/stderr"
[[ ! -e "${tmp}/plan-called" ]] || { echo "planner ran before incomplete attempt rejection" >&2; exit 1; }
echo "ok clickhouse-incomplete-migration-attempt"
