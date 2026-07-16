#!/usr/bin/env bash
# Apply an immutable, contiguous migration release to an existing context database.
set -euo pipefail

: "${PGHOST:?}" "${PGDATABASE:?}" "${ADMIN_USER:?}" "${ADMIN_PASSWORD:?}"
: "${BOOTSTRAP_USER:?}" "${ADVISORY_LOCK_KEY:?}" "${CONTEXT_NAME:?}"
: "${MIGRATION_DIR:=/bootstrap/migrations/${CONTEXT_NAME}}"

PLAN_TOOL="${MIGRATION_PLAN_TOOL:-/bootstrap/bin/migration-plan.rb}"
[[ -x "${PLAN_TOOL}" && -d "${MIGRATION_DIR}" ]] || { echo "migration release is incomplete" >&2; exit 1; }
first_migration="$(find "${MIGRATION_DIR}" -maxdepth 1 -type f -name '001_*.sql' -print -quit)"
[[ -n "${first_migration}" ]] || { echo "001 migration is required" >&2; exit 1; }

psql_admin() {
  PGPASSWORD="${ADMIN_PASSWORD}" psql -X -v ON_ERROR_STOP=1 -U "${ADMIN_USER}" -d "${PGDATABASE}" "$@"
}

# The reviewed empty-database bootstrap remains the first-version path because it
# creates least-privilege roles, CDC publications, and the exact initial catalog.
if [[ "$(psql_admin -Atqc "SELECT to_regclass('public.schema_migrations') IS NOT NULL")" != "t" ]]; then
  MIGRATION_FILE="${first_migration}" EXPECTED_VERSION="$(basename "${first_migration}" .sql)" \
    /bootstrap/bin/bootstrap-postgres.sh
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
ledger="${tmp_dir}/ledger.tsv"

has_checksum="$(psql_admin -Atqc "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='schema_migrations' AND column_name='checksum')")"
if [[ "${has_checksum}" != "t" ]]; then
  first_checksum="$(ruby -rdigest -e 'puts Digest::SHA256.file(ARGV.fetch(0)).hexdigest' "${first_migration}")"
  recorded_bootstrap_checksum="$(psql_admin -Atqc "SELECT checksum FROM schema_bootstrap_meta WHERE version='001_init'")"
  [[ "${recorded_bootstrap_checksum}" == "${first_checksum}" ]] || {
    echo "legacy 001 checksum does not match the immutable migration release" >&2
    exit 1
  }
  psql_admin --single-transaction \
    -v "lock_key=${ADVISORY_LOCK_KEY}" -v "first_checksum=${first_checksum}" <<'SQL'
SELECT pg_advisory_xact_lock(:lock_key);
ALTER TABLE schema_migrations ADD COLUMN checksum TEXT;
UPDATE schema_migrations SET checksum = :'first_checksum' WHERE version = '001_init';
ALTER TABLE schema_migrations ALTER COLUMN checksum SET NOT NULL;
SQL
fi

psql_admin -AtF $'\t' -c "SELECT version, checksum FROM schema_migrations ORDER BY version" >"${ledger}"
plan="${tmp_dir}/plan.json"
ruby "${PLAN_TOOL}" "${MIGRATION_DIR}" "${ledger}" >"${plan}"
ruby -rjson -e '
  JSON.parse(File.read(ARGV.fetch(0))).fetch("pending").each do |row|
    puts [row.fetch("version"), row.fetch("checksum"), row.fetch("path")].join("\t")
  end
' "${plan}" >"${tmp_dir}/pending.tsv"

if [[ -s "${tmp_dir}/pending.tsv" ]]; then
  script="${tmp_dir}/apply.sql"
  {
    printf '%s\n' 'SELECT pg_advisory_xact_lock(:lock_key);' 'SET ROLE :"bootstrap_user";'
    while IFS=$'\t' read -r version checksum path; do
      [[ "${path}" != *"'"* ]] || { echo "migration path contains a quote" >&2; exit 1; }
      printf '\\echo applying %s\n' "${version}"
      printf "\\i '%s'\n" "${path}"
      printf "INSERT INTO schema_migrations (version, checksum) VALUES ('%s', '%s');\n" \
        "${version}" "${checksum}"
    done <"${tmp_dir}/pending.tsv"
    printf '%s\n' 'RESET ROLE;'
  } >"${script}"
  psql_admin --single-transaction -v "lock_key=${ADVISORY_LOCK_KEY}" \
    -v "bootstrap_user=${BOOTSTRAP_USER}" -f "${script}"
fi

psql_admin -AtF $'\t' -c "SELECT version, checksum FROM schema_migrations ORDER BY version" >"${ledger}"
ruby "${PLAN_TOOL}" "${MIGRATION_DIR}" "${ledger}" >/dev/null
latest="$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).fetch("latest")' "${plan}")"
echo "migrations postgres context=${CONTEXT_NAME} latest=${latest} ok"
