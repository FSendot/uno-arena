#!/usr/bin/env bash
# Analytics-owned ClickHouse bootstrap (ADR-0028).
# Pre-mutation gate: validate empty/exact/drift BEFORE creating users/grants or DDL.
# Exact/no-op = completion marker row + exact expected analytics table and view sets
# + absence of the default-DB in-progress sentinel. ClickHouse does not provide
# cross-DDL transactions; empty apply is best-effort.
#
# Empty apply order (nontransactional):
#   1. FIRST mutation: admin-owned sentinel in existing `default` database
#      (BEFORE CREATE DATABASE analytics or any user/grant)
#   2. CREATE DATABASE analytics / users / bootstrap grants / migration / meta table
#   3. verify domain table+view sets (default sentinel still present)
#   4. runtime grants
#   5. DROP default sentinel
#   6. FINAL mutation: completion marker INSERT
# Preflight rejects if the default sentinel already exists. Any failure after the
# first mutation leaves the sentinel and/or nonempty analytics without a completion
# marker; the next run fails closed and requires a disposable reset (make kind-reset).
# There is no in-place repair of partial ClickHouse auth/DDL state.
# Catalog reads fail closed: capture query output via assignment under set -e
# (never mapfile/process-substitution that discards producer exit status).
set -euo pipefail

: "${CLICKHOUSE_HOST:?}"
: "${CLICKHOUSE_PORT:=8123}"
: "${ADMIN_USER:?}"
: "${ADMIN_PASSWORD:?}"
: "${BOOTSTRAP_USER:?}"
: "${BOOTSTRAP_PASSWORD:?}"
: "${RUNTIME_USER:?}"
: "${RUNTIME_PASSWORD:?}"
: "${EXPECTED_VERSION:?}"
: "${MIGRATION_FILE:?}"
: "${CONTEXT_NAME:=analytics}"
: "${FINGERPRINT_ENV:=/bootstrap/fingerprints/${CONTEXT_NAME}.env}"

SENTINEL_TABLE="_bootstrap_in_progress"

if [[ ! -f "$MIGRATION_FILE" ]]; then
  echo "migration file missing: $MIGRATION_FILE" >&2
  exit 1
fi
if [[ ! -f "$FINGERPRINT_ENV" ]]; then
  echo "fingerprint env missing: $FINGERPRINT_ENV" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$FINGERPRINT_ENV"
: "${EXPECTED_CHECKSUM:?}"
: "${EXPECTED_TABLES:?}"
EXPECTED_VIEWS="${EXPECTED_VIEWS:-}"

FILE_CHECKSUM="$(sha256sum "$MIGRATION_FILE" | awk '{print $1}')"
if [[ "$FILE_CHECKSUM" != "$EXPECTED_CHECKSUM" ]]; then
  echo "bootstrap failed: migration checksum ${FILE_CHECKSUM} != fingerprint ${EXPECTED_CHECKSUM}" >&2
  exit 1
fi

ch_query_admin() {
  local q="$1"
  curl -fsS "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/?database=default" \
    --user "${ADMIN_USER}:${ADMIN_PASSWORD}" \
    --data-binary "$q"
}

ch_query_bootstrap() {
  local q="$1"
  curl -fsS "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/" \
    --user "${BOOTSTRAP_USER}:${BOOTSTRAP_PASSWORD}" \
    --data-binary "$q"
}

IFS=',' read -r -a EXPECTED_TABLE_ARR <<<"$EXPECTED_TABLES"

# Fail closed: catalog read errors must exit before any mutation (no || true).
list_analytics_tables() {
  ch_query_admin "SELECT name FROM system.tables WHERE database = 'analytics' AND engine NOT LIKE '%View' ORDER BY name" \
    | tr -d '\r' | sed '/^$/d'
}

list_analytics_views() {
  ch_query_admin "SELECT name FROM system.tables WHERE database = 'analytics' AND engine LIKE '%View' ORDER BY name" \
    | tr -d '\r' | sed '/^$/d'
}

default_sentinel_present() {
  local count
  # Fail closed on catalog read error (exit, do not treat as absent).
  count="$(ch_query_admin "SELECT count() FROM system.tables WHERE database = 'default' AND name = '${SENTINEL_TABLE}'" | tr -d '[:space:]')" || exit 1
  [[ "$count" != "0" ]]
}

# Capture producer output via assignment so set -e / pipefail abort on query failure.
# Do NOT use mapfile < <(list_…) — process substitution discards the producer status.
# Bash clears errexit inside $(…) unless inherit_errexit is set; always `|| exit`
# after catalog captures so a failed list_* aborts before mutation.
shopt -s inherit_errexit

lines_to_array() {
  local -n _out="$1"
  local raw="$2"
  _out=()
  if [[ -z "$raw" ]]; then
    return 0
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" ]] && continue
    _out+=("$line")
  done <<<"$raw"
}

array_contains() {
  local needle="$1"
  shift
  local x
  for x in "$@"; do
    [[ "$x" == "$needle" ]] && return 0
  done
  return 1
}

sorted_csv() {
  if [[ "$#" -eq 0 ]]; then
    printf ''
    return 0
  fi
  printf '%s\n' "$@" | sort | paste -sd, -
}

expected_views_sorted=""
if [[ -n "$EXPECTED_VIEWS" ]]; then
  expected_views_sorted="$(echo "$EXPECTED_VIEWS" | tr ',' '\n' | sort | paste -sd, -)"
fi
expected_sorted="$(printf '%s\n' "${EXPECTED_TABLE_ARR[@]}" | sort | paste -sd, -)"

# Preflight: default-DB sentinel means a prior nontransactional apply failed → fail closed.
# Exact/no-op also requires no sentinel (checked here before any mutation).
if default_sentinel_present; then
  echo "bootstrap failed: default bootstrap-in-progress sentinel present (partial nontransactional apply). disposable reset required (make kind-reset)." >&2
  exit 1
fi

db_exists="$(ch_query_admin "SELECT count() FROM system.databases WHERE name = 'analytics'" | tr -d '[:space:]')" || exit 1
# Assignment + || exit fails closed if list_* / curl pipeline fails (set -e alone is not enough).
live_tables_raw="$(list_analytics_tables)" || exit 1
live_views_raw="$(list_analytics_views)" || exit 1
LIVE_TABLES=()
LIVE_VIEWS=()
lines_to_array LIVE_TABLES "$live_tables_raw"
lines_to_array LIVE_VIEWS "$live_views_raw"
# Treat missing DB as empty arrays
if [[ "$db_exists" != "1" ]]; then
  LIVE_TABLES=()
  LIVE_VIEWS=()
fi

user_object_count=$((${#LIVE_TABLES[@]} + ${#LIVE_VIEWS[@]}))

echo "bootstrap clickhouse expected=${EXPECTED_VERSION} checksum=${EXPECTED_CHECKSUM:0:12}… objects=${user_object_count}"

decide="fail"
if [[ "$user_object_count" -eq 0 ]]; then
  decide="apply"
else
  version_count="0"
  meta_count="0"
  found_version=""
  found_meta_version=""
  found_checksum=""
  if printf '%s\n' "${LIVE_TABLES[@]}" | grep -qx 'schema_migrations'; then
    version_count="$(ch_query_admin "SELECT count() FROM analytics.schema_migrations FINAL" | tr -d '[:space:]')"
    if [[ "$version_count" == "1" ]]; then
      found_version="$(ch_query_admin "SELECT version FROM analytics.schema_migrations FINAL" | tr -d '[:space:]')"
    fi
  fi
  if printf '%s\n' "${LIVE_TABLES[@]}" | grep -qx 'schema_bootstrap_meta'; then
    meta_count="$(ch_query_admin "SELECT count() FROM analytics.schema_bootstrap_meta FINAL" | tr -d '[:space:]')"
    if [[ "$meta_count" == "1" ]]; then
      found_meta_version="$(ch_query_admin "SELECT version FROM analytics.schema_bootstrap_meta FINAL" | tr -d '[:space:]')"
      found_checksum="$(ch_query_admin "SELECT checksum FROM analytics.schema_bootstrap_meta FINAL" | tr -d '[:space:]')"
    fi
  fi

  live_sorted="$(sorted_csv "${LIVE_TABLES[@]+"${LIVE_TABLES[@]}"}")"
  live_views_sorted="$(sorted_csv "${LIVE_VIEWS[@]+"${LIVE_VIEWS[@]}"}")"

  # Exact/no-op requires completion marker + exact table and view catalogs (no default sentinel;
  # sentinel already rejected by preflight above).
  # Nonempty without exact marker → fail closed (disposable reset required).
  if [[ "$version_count" == "1" \
     && "$meta_count" == "1" \
     && "$found_version" == "$EXPECTED_VERSION" \
     && "$found_meta_version" == "$EXPECTED_VERSION" \
     && "$found_checksum" == "$EXPECTED_CHECKSUM" \
     && "$live_sorted" == "$expected_sorted" \
     && "$live_views_sorted" == "$expected_views_sorted" ]]; then
    decide="noop"
  else
    echo "bootstrap failed: unexpected clickhouse state (zero mutations). nonempty without exact marker requires disposable reset (make kind-reset). tables=${live_sorted} views=${live_views_sorted} migrations=${version_count} meta=${meta_count} ver=${found_version} meta_ver=${found_meta_version} cksum=${found_checksum}" >&2
    exit 1
  fi
fi

# Exact match → zero mutations (no users/grants/DDL).
if [[ "$decide" == "noop" ]]; then
  echo "bootstrap no-op: exact version/checksum/catalog ${EXPECTED_VERSION}"
  echo "bootstrap clickhouse ok version=${EXPECTED_VERSION} (noop)"
  exit 0
fi

# Empty apply path. Default-DB sentinel is the FIRST mutation so any later failure
# leaves sentinel and/or nonempty-without-completion-marker (fail closed on next run).
echo "empty clickhouse analytics store; creating default in-progress sentinel then analytics DB/users/migration"
ch_query_admin "CREATE TABLE IF NOT EXISTS default.${SENTINEL_TABLE}
(
    started_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY started_at"

ch_query_admin "CREATE DATABASE IF NOT EXISTS analytics"
ch_query_admin "CREATE USER IF NOT EXISTS ${BOOTSTRAP_USER} IDENTIFIED WITH plaintext_password BY '${BOOTSTRAP_PASSWORD}'"
ch_query_admin "CREATE USER IF NOT EXISTS ${RUNTIME_USER} IDENTIFIED WITH plaintext_password BY '${RUNTIME_PASSWORD}'"
ch_query_admin "GRANT CREATE DATABASE, CREATE TABLE, INSERT, SELECT, ALTER, DROP ON *.* TO ${BOOTSTRAP_USER}"

# ClickHouse HTTP rejects multi-statement bodies (Code 62). Do not use multiquery=1.
# Split the fingerprint-checked migration and POST one statement per request under
# bootstrap credentials; stop at first failure (set -e). Sentinel remains first
# mutation and completion marker remains last → partial apply fails closed.
_BOOT_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPLIT_CLICKHOUSE_SQL="${_BOOT_BIN}/split-clickhouse-sql.rb"
stmt_dir="$(mktemp -d "${TMPDIR:-/tmp}/ch-bootstrap-stmts.XXXXXX")"
ruby "${SPLIT_CLICKHOUSE_SQL}" "${MIGRATION_FILE}" "${stmt_dir}"
shopt -s nullglob
stmt_files=("${stmt_dir}"/*.sql)
if [[ ${#stmt_files[@]} -eq 0 ]]; then
  echo "bootstrap failed: sql splitter produced no statements" >&2
  rm -rf "${stmt_dir}"
  exit 1
fi
for stmt_file in "${stmt_files[@]}"; do
  # Do not nest $(cat …) inside curl/ch_query — cat failure is masked and the
  # request still runs with an empty body. Guarded assignment + nonempty check.
  stmt="$(cat "${stmt_file}")" || {
    echo "bootstrap failed: could not read split statement ${stmt_file}" >&2
    rm -rf "${stmt_dir}"
    exit 1
  }
  if [[ -z "${stmt}" ]]; then
    echo "bootstrap failed: empty split statement ${stmt_file}" >&2
    rm -rf "${stmt_dir}"
    exit 1
  fi
  ch_query_bootstrap "${stmt}"
done
rm -rf "${stmt_dir}"

# Meta table structure only — completion row is written last after verify + grants + sentinel drop.
ch_query_bootstrap "CREATE TABLE IF NOT EXISTS analytics.schema_bootstrap_meta
(
    version String,
    checksum String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version"

# Post-apply exact verification of domain tables + views BEFORE grants / sentinel drop / marker.
live_tables_raw="$(list_analytics_tables)" || exit 1
live_views_raw="$(list_analytics_views)" || exit 1
LIVE_TABLES=()
LIVE_VIEWS=()
lines_to_array LIVE_TABLES "$live_tables_raw"
lines_to_array LIVE_VIEWS "$live_views_raw"

if ! default_sentinel_present; then
  echo "bootstrap failed: post-apply missing default in-progress sentinel (unexpected catalog)" >&2
  echo "cleanup: disposable kind reset via make kind-reset (nontransactional partial apply)" >&2
  exit 1
fi

live_sorted="$(sorted_csv "${LIVE_TABLES[@]+"${LIVE_TABLES[@]}"}")"
live_views_sorted="$(sorted_csv "${LIVE_VIEWS[@]+"${LIVE_VIEWS[@]}"}")"
version_count="$(ch_query_admin "SELECT count() FROM analytics.schema_migrations FINAL" | tr -d '[:space:]')"
found_version=""
if [[ "$version_count" == "1" ]]; then
  found_version="$(ch_query_admin "SELECT version FROM analytics.schema_migrations FINAL" | tr -d '[:space:]')"
fi

if [[ "$version_count" != "1" || "$found_version" != "$EXPECTED_VERSION" ]]; then
  echo "bootstrap failed: post-apply version mismatch migrations=${version_count} ver=${found_version}" >&2
  echo "cleanup: disposable kind reset via make kind-reset (nonempty without exact marker)" >&2
  exit 1
fi
if [[ "$live_sorted" != "$expected_sorted" ]]; then
  echo "bootstrap failed: post-apply catalog mismatch live=${live_sorted} expected=${expected_sorted}" >&2
  echo "cleanup: disposable kind reset via make kind-reset (nonempty without exact marker)" >&2
  exit 1
fi
if [[ "$live_views_sorted" != "$expected_views_sorted" ]]; then
  echo "bootstrap failed: post-apply view catalog mismatch live=${live_views_sorted} expected=${expected_views_sorted}" >&2
  echo "cleanup: disposable kind reset via make kind-reset (nonempty without exact marker)" >&2
  exit 1
fi

# Runtime DML grants before sentinel drop / completion marker so a grant failure cannot leave a false exact.
ch_query_admin "GRANT SELECT, INSERT ON analytics.* TO ${RUNTIME_USER}"
ch_query_admin "REVOKE CREATE DATABASE, CREATE TABLE, DROP, ALTER ON *.* FROM ${RUNTIME_USER}"

# Remove default in-progress sentinel, then final mutation only: completion marker.
ch_query_admin "DROP TABLE IF EXISTS default.${SENTINEL_TABLE}"
ch_query_bootstrap "INSERT INTO analytics.schema_bootstrap_meta (version, checksum) VALUES ('${EXPECTED_VERSION}', '${EXPECTED_CHECKSUM}')"

echo "bootstrap clickhouse ok version=${EXPECTED_VERSION}"
