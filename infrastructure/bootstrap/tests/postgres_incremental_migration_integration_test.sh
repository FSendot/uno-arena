#!/usr/bin/env bash
# Prove the runner upgrades a non-empty v1 database, records v2 itself, and then no-ops.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE="${POSTGRES_TEST_IMAGE:-postgres:18.4-alpine3.24}"
container="uno-arena-migration-test-$RANDOM-$$"
tmp_dir="$(mktemp -d)"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

docker image inspect "${IMAGE}" >/dev/null 2>&1 || {
  echo "required local test image is missing: ${IMAGE}" >&2
  exit 1
}
docker run --rm -d --name "${container}" \
  -e POSTGRES_PASSWORD=test-admin-password \
  -p 127.0.0.1::5432 "${IMAGE}" >/dev/null
port="$(docker inspect -f '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "${container}")"

for _ in $(seq 1 60); do
  if PGPASSWORD=test-admin-password psql -X -h 127.0.0.1 -p "${port}" \
    -U postgres -d postgres -Atqc 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
PGPASSWORD=test-admin-password psql -X -h 127.0.0.1 -p "${port}" \
  -U postgres -d postgres -Atqc 'SELECT 1' >/dev/null

migrations="${tmp_dir}/migrations"
mkdir -p "${migrations}"
printf '%s\n' 'CREATE TABLE example (id integer PRIMARY KEY);' >"${migrations}/001_init.sql"
printf '%s\n' 'ALTER TABLE example ADD COLUMN expanded text;' >"${migrations}/002_expand.sql"
first_checksum="$(ruby -rdigest -e 'puts Digest::SHA256.file(ARGV.fetch(0)).hexdigest' "${migrations}/001_init.sql")"

PGPASSWORD=test-admin-password psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "${port}" \
  -U postgres -d postgres -v "first_checksum=${first_checksum}" <<'SQL' >/dev/null
CREATE ROLE bootstrap_role NOLOGIN;
CREATE TABLE example (id integer PRIMARY KEY);
ALTER TABLE example OWNER TO bootstrap_role;
CREATE TABLE schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE schema_migrations OWNER TO bootstrap_role;
INSERT INTO schema_migrations (version) VALUES ('001_init');
CREATE TABLE schema_bootstrap_meta (
  version text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE schema_bootstrap_meta OWNER TO bootstrap_role;
INSERT INTO schema_bootstrap_meta (version, checksum) VALUES ('001_init', :'first_checksum');
SQL

run_migrations() {
  PGHOST=127.0.0.1 PGPORT="${port}" PGDATABASE=postgres \
    ADMIN_USER=postgres ADMIN_PASSWORD=test-admin-password \
    BOOTSTRAP_USER=bootstrap_role ADVISORY_LOCK_KEY=920026 CONTEXT_NAME=test \
    MIGRATION_DIR="${migrations}" \
    MIGRATION_PLAN_TOOL="${ROOT_DIR}/infrastructure/bootstrap/bin/migration-plan.rb" \
    "${ROOT_DIR}/infrastructure/bootstrap/bin/migrate-postgres.sh"
}

run_migrations >/dev/null
ledger="$(PGPASSWORD=test-admin-password psql -X -h 127.0.0.1 -p "${port}" \
  -U postgres -d postgres -AtF '|' -c 'SELECT version, checksum FROM schema_migrations ORDER BY version')"
expected_second_checksum="$(ruby -rdigest -e 'puts Digest::SHA256.file(ARGV.fetch(0)).hexdigest' "${migrations}/002_expand.sql")"
expected_ledger="001_init|${first_checksum}
002_expand|${expected_second_checksum}"
[[ "${ledger}" == "${expected_ledger}" ]] || {
  echo "unexpected migration ledger:" >&2
  printf '%s\n' "${ledger}" >&2
  exit 1
}
column_count="$(PGPASSWORD=test-admin-password psql -X -h 127.0.0.1 -p "${port}" \
  -U postgres -d postgres -Atqc \
  "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='example' AND column_name='expanded'")"
[[ "${column_count}" == "1" ]] || { echo "v2 schema change was not applied" >&2; exit 1; }

run_migrations >/dev/null
row_count="$(PGPASSWORD=test-admin-password psql -X -h 127.0.0.1 -p "${port}" \
  -U postgres -d postgres -Atqc 'SELECT count(*) FROM schema_migrations')"
[[ "${row_count}" == "2" ]] || { echo "exact rerun was not a no-op" >&2; exit 1; }

printf '%s\n' 'ALTER TABLE example ADD COLUMN expanded text; -- drift' >"${migrations}/002_expand.sql"
if run_migrations >"${tmp_dir}/drift.out" 2>"${tmp_dir}/drift.err"; then
  echo "checksum drift unexpectedly succeeded" >&2
  exit 1
fi
grep -q 'checksum drift for 002_expand' "${tmp_dir}/drift.err" || {
  echo "checksum drift did not fail with the expected reason" >&2
  cat "${tmp_dir}/drift.err" >&2
  exit 1
}

echo "postgres incremental migration integration test: ok"
