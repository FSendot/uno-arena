#!/usr/bin/env bash
# Apply a contiguous idempotent ClickHouse migration release without resetting data.
set -euo pipefail

: "${CLICKHOUSE_HOST:?}" "${ADMIN_USER:?}" "${ADMIN_PASSWORD:?}"
: "${BOOTSTRAP_USER:?}" "${BOOTSTRAP_PASSWORD:?}"
: "${CONTEXT_NAME:=analytics}" "${CLICKHOUSE_PORT:=8123}"
: "${MIGRATION_DIR:=/bootstrap/migrations/${CONTEXT_NAME}}"
PLAN_TOOL="${MIGRATION_PLAN_TOOL:-/bootstrap/bin/migration-plan.rb}"
SPLITTER="${CLICKHOUSE_SQL_SPLITTER:-/bootstrap/bin/split-clickhouse-sql.rb}"
[[ -x "${PLAN_TOOL}" && -x "${SPLITTER}" && -d "${MIGRATION_DIR}" ]] || {
  echo "ClickHouse migration release is incomplete" >&2; exit 1;
}
first_migration="$(find "${MIGRATION_DIR}" -maxdepth 1 -type f -name '001_*.sql' -print -quit)"
[[ -n "${first_migration}" ]] || { echo "001 migration is required" >&2; exit 1; }

ch_admin() {
  curl -fsS "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/?database=default" \
    --user "${ADMIN_USER}:${ADMIN_PASSWORD}" --data-binary "$1"
}
ch_bootstrap() {
  curl -fsS "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/?database=analytics" \
    --user "${BOOTSTRAP_USER}:${BOOTSTRAP_PASSWORD}" --data-binary "$1"
}

if [[ "$(ch_admin "SELECT count() FROM system.tables WHERE database='analytics' AND name='schema_migrations'" | tr -d '[:space:]')" == "0" ]]; then
  MIGRATION_FILE="${first_migration}" EXPECTED_VERSION="$(basename "${first_migration}" .sql)" \
    /bootstrap/bin/bootstrap-clickhouse.sh
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
first_checksum="$(ruby -rdigest -e 'puts Digest::SHA256.file(ARGV.fetch(0)).hexdigest' "${first_migration}")"
ch_bootstrap "CREATE TABLE IF NOT EXISTS analytics.schema_migration_checksums
(version String, checksum FixedString(64), applied_at DateTime64(3, 'UTC') DEFAULT now64(3))
ENGINE = ReplacingMergeTree(applied_at) ORDER BY version"
ch_bootstrap "CREATE TABLE IF NOT EXISTS analytics.schema_migration_attempts
(version String, status Enum8('running'=1, 'succeeded'=2), attempted_at DateTime64(3, 'UTC') DEFAULT now64(3))
ENGINE = ReplacingMergeTree(attempted_at) ORDER BY version"
incomplete_attempt="$(ch_bootstrap "SELECT version FROM analytics.schema_migration_attempts
GROUP BY version HAVING argMax(status, tuple(attempted_at, status))='running'
ORDER BY version LIMIT 1" | tr -d '[:space:]')"
[[ -z "${incomplete_attempt}" ]] || {
  echo "incomplete ClickHouse migration attempt requires operator inspection: ${incomplete_attempt}" >&2
  exit 1
}
if [[ "$(ch_bootstrap "SELECT count() FROM analytics.schema_migration_checksums FINAL WHERE version='001_init'" | tr -d '[:space:]')" == "0" ]]; then
  recorded="$(ch_bootstrap "SELECT checksum FROM analytics.schema_bootstrap_meta FINAL WHERE version='001_init'" | tr -d '[:space:]')"
  [[ "${recorded}" == "${first_checksum}" ]] || { echo "legacy ClickHouse 001 checksum drift" >&2; exit 1; }
  ch_bootstrap "INSERT INTO analytics.schema_migration_checksums (version, checksum) VALUES ('001_init', '${first_checksum}')"
fi

ledger="${tmp_dir}/ledger.tsv"
ch_bootstrap "SELECT m.version, c.checksum FROM analytics.schema_migrations FINAL m
INNER JOIN analytics.schema_migration_checksums FINAL c USING (version) ORDER BY m.version FORMAT TabSeparated" >"${ledger}"
plan="${tmp_dir}/plan.json"
ruby "${PLAN_TOOL}" "${MIGRATION_DIR}" "${ledger}" >"${plan}"
ruby -rjson -e '
  JSON.parse(File.read(ARGV.fetch(0))).fetch("pending").each do |row|
    puts [row.fetch("version"), row.fetch("checksum"), row.fetch("path")].join("\t")
  end
' "${plan}" >"${tmp_dir}/pending.tsv"

while IFS=$'\t' read -r version checksum path; do
  [[ -n "${version}" ]] || continue
  ch_bootstrap "INSERT INTO analytics.schema_migration_attempts (version, status) VALUES ('${version}', 'running')"
  statements="${tmp_dir}/${version}"
  ruby "${SPLITTER}" "${path}" "${statements}"
  for statement in "${statements}"/*.sql; do
    sql="$(cat "${statement}")"
    ch_bootstrap "${sql}"
  done
  ch_bootstrap "INSERT INTO analytics.schema_migrations (version) VALUES ('${version}')"
  ch_bootstrap "INSERT INTO analytics.schema_migration_checksums (version, checksum) VALUES ('${version}', '${checksum}')"
  ch_bootstrap "INSERT INTO analytics.schema_migration_attempts (version, status) VALUES ('${version}', 'succeeded')"
done <"${tmp_dir}/pending.tsv"

ch_bootstrap "SELECT m.version, c.checksum FROM analytics.schema_migrations FINAL m
INNER JOIN analytics.schema_migration_checksums FINAL c USING (version) ORDER BY m.version FORMAT TabSeparated" >"${ledger}"
ruby "${PLAN_TOOL}" "${MIGRATION_DIR}" "${ledger}" >/dev/null
latest="$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).fetch("latest")' "${plan}")"
echo "migrations clickhouse context=${CONTEXT_NAME} latest=${latest} ok"
