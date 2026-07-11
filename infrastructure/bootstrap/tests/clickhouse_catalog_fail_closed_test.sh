#!/usr/bin/env bash
# Offline regression: catalog list failure must abort before mutation under set -e.
# Proves mapfile+process-substitution loses producer status (bad), bare assignment
# without inherit_errexit/||exit can continue (unsafe), and the production pattern
# `raw="$(list_…)" || exit 1` aborts before mutation. No ClickHouse / network dependency.
#
# Also inspects the production bootstrap-clickhouse.sh itself for pipefail and
# guarded captures (fixtures alone are not sufficient).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CH_BOOT="${ROOT}/infrastructure/bootstrap/bin/bootstrap-clickhouse.sh"
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# --- Structural: production script must retain pipefail + guarded captures ---
if ! grep -qE '^set -euo pipefail$|^set -o pipefail$' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must retain set -euo pipefail (or set -o pipefail)"
fi
# Prefer the combined form used in production.
if ! grep -qE '^set -euo pipefail$' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must use set -euo pipefail"
fi
if grep -nE 'mapfile\s+-t\s+\w+\s+<\s+<\(list_analytics_' "$CH_BOOT" >/dev/null; then
  fail "bootstrap-clickhouse.sh still uses mapfile+process-substitution for catalog lists"
fi
if ! grep -qE 'live_tables_raw="\$\(list_analytics_tables\)" \|\| exit' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must capture list_analytics_tables via assignment || exit"
fi
if ! grep -qE 'live_views_raw="\$\(list_analytics_views\)" \|\| exit' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must capture list_analytics_views via assignment || exit"
fi
if ! grep -q 'inherit_errexit' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must enable inherit_errexit for catalog captures"
fi
# Catalog list helpers must pipe through tr/sed under pipefail (producer status preserved).
if ! grep -A2 'list_analytics_tables()' "$CH_BOOT" | grep -q '|'; then
  fail "list_analytics_tables must use a pipeline (pipefail-relevant)"
fi

# --- Behavioral fixtures ---
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mutation_marker="${tmp}/mutated"

# Bad pattern (historical): process substitution discards producer exit status.
bad_continues=0
(
  set +e
  set -euo pipefail
  list_fail() { echo "should-not-print"; return 1; }
  # shellcheck disable=SC2034
  mapfile -t LIVE < <(list_fail)
  touch "$mutation_marker"
  exit 0
) && bad_continues=1 || true

if [[ "$bad_continues" -ne 1 || ! -f "$mutation_marker" ]]; then
  fail "fixture setup: expected bad mapfile pattern to continue after list_fail"
fi
rm -f "$mutation_marker"

# Bare assignment under set -e (without inherit_errexit / || exit) may continue —
# documenting why production uses || exit.
bare_continued=0
(
  set -euo pipefail
  # deliberately no inherit_errexit
  list_fail() { return 1; }
  # shellcheck disable=SC2034
  live_raw="$(list_fail)"
  touch "$mutation_marker"
  exit 0
) && bare_continued=1 || true

if [[ "$bare_continued" -eq 1 && -f "$mutation_marker" ]]; then
  : # expected on bash without inherit_errexit for assignments
elif [[ "$bare_continued" -eq 0 && ! -f "$mutation_marker" ]]; then
  : # also acceptable if this bash aborts bare assignment
else
  fail "unexpected bare-assignment behavior continued=${bare_continued} marker=$([[ -f $mutation_marker ]] && echo yes || echo no)"
fi
rm -f "$mutation_marker"

# Production pattern: assignment || exit aborts before mutation.
good_aborted=0
(
  set -euo pipefail
  shopt -s inherit_errexit
  list_fail() { return 1; }
  # shellcheck disable=SC2034
  live_raw="$(list_fail)" || exit 1
  touch "$mutation_marker"
  exit 0
) && true || good_aborted=1

if [[ "$good_aborted" -ne 1 ]]; then
  fail "assignment || exit did not abort on failing list function"
fi
if [[ -f "$mutation_marker" ]]; then
  fail "mutation marker written after failing list — fail-closed broken"
fi

# Same pattern for a failing pipeline (pipefail), matching list_analytics_*.
pipeline_aborted=0
(
  set -euo pipefail
  shopt -s inherit_errexit
  list_fail_pipe() { false | sed '/^$/d'; }
  # shellcheck disable=SC2034
  live_raw="$(list_fail_pipe)" || exit 1
  touch "$mutation_marker"
  exit 0
) && true || pipeline_aborted=1

if [[ "$pipeline_aborted" -ne 1 ]]; then
  fail "pipeline assignment || exit did not abort under pipefail"
fi
if [[ -f "$mutation_marker" ]]; then
  fail "mutation marker written after failing pipeline list"
fi

# Marker ordering: default sentinel → CREATE DATABASE analytics → users → grants → DROP default sentinel → INSERT last.
marker_order_ok=1
sentinel_idx=$(grep -n 'CREATE TABLE IF NOT EXISTS default.${SENTINEL_TABLE}' "$CH_BOOT" | head -1 | cut -d: -f1)
create_db_idx=$(grep -n 'CREATE DATABASE IF NOT EXISTS analytics' "$CH_BOOT" | head -1 | cut -d: -f1)
create_user_idx=$(grep -n 'CREATE USER IF NOT EXISTS' "$CH_BOOT" | head -1 | cut -d: -f1)
grant_idx=$(grep -n 'GRANT SELECT, INSERT ON analytics' "$CH_BOOT" | head -1 | cut -d: -f1)
drop_sentinel_idx=$(grep -n 'DROP TABLE IF EXISTS default.${SENTINEL_TABLE}' "$CH_BOOT" | head -1 | cut -d: -f1)
insert_idx=$(grep -n 'INSERT INTO analytics.schema_bootstrap_meta' "$CH_BOOT" | head -1 | cut -d: -f1)
preflight_idx=$(grep -n 'default bootstrap-in-progress sentinel present' "$CH_BOOT" | head -1 | cut -d: -f1)

if [[ -z "$sentinel_idx" || -z "$create_db_idx" || -z "$create_user_idx" || -z "$grant_idx" || -z "$drop_sentinel_idx" || -z "$insert_idx" ]]; then
  fail "marker ordering: missing default-sentinel/CREATE DATABASE/user/grant/drop/insert anchors"
  marker_order_ok=0
fi
if [[ -z "$preflight_idx" ]]; then
  fail "preflight must reject existing default sentinel"
  marker_order_ok=0
fi
if [[ "$marker_order_ok" -eq 1 ]]; then
  if ! (( preflight_idx < sentinel_idx && sentinel_idx < create_db_idx && create_db_idx < create_user_idx && create_user_idx < grant_idx && grant_idx < drop_sentinel_idx && drop_sentinel_idx < insert_idx )); then
    fail "marker ordering: expected preflight < default sentinel < CREATE DATABASE < CREATE USER < grants < DROP default sentinel < INSERT marker (got ${preflight_idx},${sentinel_idx},${create_db_idx},${create_user_idx},${grant_idx},${drop_sentinel_idx},${insert_idx})"
  fi
fi
# Must not create analytics-DB sentinel as first mutation.
if grep -qE 'CREATE TABLE IF NOT EXISTS analytics\.\$\{SENTINEL_TABLE\}|CREATE TABLE IF NOT EXISTS analytics\._bootstrap_in_progress' "$CH_BOOT"; then
  fail "sentinel must live in default DB, not analytics"
fi

# Migration apply: split + one POST per statement (ClickHouse Code 62 / no multiquery=1).
if ! grep -q 'split-clickhouse-sql' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must invoke split-clickhouse-sql"
fi
if grep -qE -- '--data-binary\s+@"\$\{MIGRATION_FILE\}"' "$CH_BOOT"; then
  fail "bootstrap-clickhouse.sh must not POST full migration file via --data-binary @MIGRATION_FILE"
fi
if grep -vE '^\s*#' "$CH_BOOT" | grep -q 'multiquery=1'; then
  fail "bootstrap-clickhouse.sh must not use unsupported multiquery=1"
fi
# Statement apply: never nest $(cat …) in the curl/ch_query argument (masks read failure).
if grep -vE '^\s*#' "$CH_BOOT" | grep -qE 'ch_query_bootstrap\s+"\$\(cat\b'; then
  fail "bootstrap-clickhouse.sh must not nest \$(cat …) inside ch_query_bootstrap argument"
fi
if ! grep -vE '^\s*#' "$CH_BOOT" | grep -qE 'stmt="\$\(cat "\$\{stmt_file\}"\)" \|\|'; then
  fail "bootstrap-clickhouse.sh must read each statement via guarded assignment || exit"
fi
if ! grep -vE '^\s*#' "$CH_BOOT" | grep -qE '\[\[ -z "\$\{stmt\}" \]\]'; then
  fail "bootstrap-clickhouse.sh must reject empty statement after guarded read"
fi

# --- Behavioral: statement read failure must not run request / marker path ---
request_marker="${tmp}/request"
completion_marker="${tmp}/completion"
rm -f "$request_marker" "$completion_marker"

# Bad pattern: nested $(cat) in the function argument — cat failure is masked and
# the request path still executes (empty body), then marker path can run.
bad_stmt_continues=0
(
  set -euo pipefail
  ch_query_bootstrap() { touch "$request_marker"; }
  missing="${tmp}/no-such-stmt.sql"
  ch_query_bootstrap "$(cat "${missing}")"
  touch "$completion_marker"
  exit 0
) && bad_stmt_continues=1 || true

if [[ "$bad_stmt_continues" -ne 1 || ! -f "$request_marker" ]]; then
  fail "fixture setup: expected nested \$(cat) to still invoke request path after read fail"
fi
if [[ ! -f "$completion_marker" ]]; then
  fail "fixture setup: expected nested \$(cat) path to reach completion marker"
fi
rm -f "$request_marker" "$completion_marker"

# Production pattern: guarded assignment || exit, nonempty check, then request.
# Failing read must abort with neither request nor marker executed.
good_stmt_aborted=0
(
  set -euo pipefail
  shopt -s inherit_errexit
  ch_query_bootstrap() { touch "$request_marker"; }
  missing="${tmp}/no-such-stmt.sql"
  stmt="$(cat "${missing}")" || exit 1
  if [[ -z "${stmt}" ]]; then
    exit 1
  fi
  ch_query_bootstrap "${stmt}"
  touch "$completion_marker"
  exit 0
) && true || good_stmt_aborted=1

if [[ "$good_stmt_aborted" -ne 1 ]]; then
  fail "guarded statement read || exit did not abort on missing file"
fi
if [[ -f "$request_marker" ]]; then
  fail "request path executed after statement read failure"
fi
if [[ -f "$completion_marker" ]]; then
  fail "completion marker path executed after statement read failure"
fi

if [[ "$failures" -ne 0 ]]; then
  echo "${failures} clickhouse catalog fail-closed failure(s)" >&2
  exit 1
fi
echo "ok clickhouse_catalog_fail_closed_test"
