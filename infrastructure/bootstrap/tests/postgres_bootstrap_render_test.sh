#!/usr/bin/env bash
# Render-intercept regression for bootstrap-postgres.sh final psql input.
# Quoted vs unquoted heredocs must emit single-backslash psql metacommands
# and valid dollar-quoting. Branch syntax must succeed on empty (apply) and
# exact (noop) paths. Does not weaken fail-closed behavior of the generator.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PG_BOOT="${ROOT}/infrastructure/bootstrap/bin/bootstrap-postgres.sh"
SQL_DIR="${ROOT}/infrastructure/bootstrap/sql"
failures=0

fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd bash
require_cmd sha256sum
require_cmd awk
require_cmd psql
require_cmd initdb
require_cmd postgres
require_cmd pg_isready
require_cmd python3

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"; [[ -n "${PG_PID:-}" ]] && kill "$PG_PID" 2>/dev/null || true; [[ -n "${PG_PID:-}" ]] && wait "$PG_PID" 2>/dev/null || true' EXIT

# --- Fixture migration + fingerprint matching checksum ---
mig="${tmp}/001_init.sql"
cat >"$mig" <<'MIG'
-- render-test fixture migration (no objects required for intercept)
MIG
cksum="$(sha256sum "$mig" | awk '{print $1}')"
fp="${tmp}/fixture.env"
cat >"$fp" <<EOF
EXPECTED_CHECKSUM=${cksum}
EXPECTED_TABLES=schema_migrations,schema_bootstrap_meta
EXPECTED_VIEWS=
EXPECTED_MATERIALIZED_VIEWS=
EXPECTED_SEQUENCES=
EXPECTED_INDEXES=
EOF

mkdir -p "${tmp}/bootstrap/sql" "${tmp}/bin"
cp "${SQL_DIR}/postgres_ensure_roles.sql" "${tmp}/bootstrap/sql/"
cp "${SQL_DIR}/postgres_grant_runtime.sql" "${tmp}/bootstrap/sql/"
cp "${SQL_DIR}/postgres_cdc_apply.sql" "${tmp}/bootstrap/sql/"
cp "${SQL_DIR}/postgres_cdc_verify.sql" "${tmp}/bootstrap/sql/"

captured="${tmp}/captured.psql"
cat >"${tmp}/bin/psql" <<EOF
#!/usr/bin/env bash
set -euo pipefail
out="${captured}"
file=""
args=("\$@")
i=0
while [[ \$i -lt \${#args[@]} ]]; do
  if [[ "\${args[\$i]}" == "-f" ]]; then
    i=\$((i + 1))
    file="\${args[\$i]}"
  fi
  i=\$((i + 1))
done
if [[ -z "\$file" || ! -f "\$file" ]]; then
  echo "fake psql: missing -f input" >&2
  exit 1
fi
cp "\$file" "\$out"
exit 0
EOF
chmod +x "${tmp}/bin/psql"

render_final() {
  local with_realtime="${1:-}"
  rm -f "$captured"
  local -a env_args=(
    env
    "PATH=${tmp}/bin:/sbin:/usr/sbin:/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"
    "PGHOST=127.0.0.1"
    "PGPORT=5432"
    "PGDATABASE=render_test_db"
    "ADMIN_USER=admin"
    "ADMIN_PASSWORD=admin-pass"
    "BOOTSTRAP_USER=boot_user"
    "BOOTSTRAP_PASSWORD=boot-pass"
    "RUNTIME_USER=rt_user"
    "RUNTIME_PASSWORD=rt-pass"
    "ADVISORY_LOCK_KEY=42"
    "EXPECTED_VERSION=001_init"
    "MIGRATION_FILE=${mig}"
    "CONTEXT_NAME=fixture"
    "FINGERPRINT_ENV=${fp}"
    "CDC_USER=cdc_user"
    "CDC_PASSWORD=cdc-pass"
    "CDC_PUBLICATION=cdc_pub"
    "CDC_TABLE=outbox_events"
    "CDC_PEER_TABLE="
  )
  if [[ -n "$with_realtime" ]]; then
    env_args+=(
      "CDC_REALTIME_USER=cdc_rt_user"
      "CDC_REALTIME_PASSWORD=cdc-rt-pass"
      "CDC_REALTIME_PUBLICATION=cdc_rt_pub"
      "CDC_REALTIME_TABLE=realtime_outbox_events"
    )
  else
    env_args+=(
      "CDC_REALTIME_USER="
      "CDC_REALTIME_PASSWORD="
      "CDC_REALTIME_PUBLICATION="
      "CDC_REALTIME_TABLE="
    )
  fi
  "${env_args[@]}" bash "$PG_BOOT" >/dev/null
  if [[ ! -f "$captured" ]]; then
    fail "bootstrap did not hand a -f file to psql (realtime=${with_realtime:-no})"
    return 1
  fi
  cp "$captured" "${tmp}/rendered${with_realtime:+_rt}.psql"
}

assert_rendered_psql() {
  local file="$1"
  local label="$2"
  local rc=0
  python3 - "$file" "$label" <<'PY' || rc=$?
import re, sys
path, label = sys.argv[1], sys.argv[2]
text = open(path, encoding="utf-8").read()
lines = text.splitlines()
failures = []

def fail(msg):
    failures.append(msg)
    print(f"FAIL: {label}: {msg}", file=sys.stderr)

needed = {
    "if": re.compile(r"^\\if\b"),
    "elif": re.compile(r"^\\elif\b"),
    "endif": re.compile(r"^\\endif\b"),
    "echo": re.compile(r"^\\echo\b"),
    "i": re.compile(r"^\\i\b"),
    "set": re.compile(r"^\\set\b"),
}
for name, pat in needed.items():
    if not any(pat.search(line) for line in lines):
        fail(f"missing single-backslash \\{name}")

if not any(re.search(r"(?<!\\)\\gset\s*$", line) for line in lines):
    fail(r"missing single-backslash \gset")

if not any(re.search(r"^\\if\s+:do_apply\b", line) for line in lines):
    fail(r"missing \if :do_apply")
if not any(re.search(r"^\\elif\s+:do_exact\b", line) for line in lines):
    fail(r"missing \elif :do_exact")
if not any(re.search(r"^\\elif\s+:do_fail\b", line) for line in lines):
    fail(r"missing \elif :do_fail")

for i, line in enumerate(lines, 1):
    if re.match(r"^\\\\(if|elif|else|endif|echo|i|set)\b", line) or re.search(r"\\\\gset\s*$", line):
        fail(f"double-backslash psql metacommand at line {i}: {line!r}")
    elif re.search(r"[^\\]\\\\(if|elif|else|endif|echo|i|set|gset)\b", line):
        fail(f"double-backslash psql metacommand at line {i}: {line!r}")

if re.search(r"DO \\\$\\\$|END \\\$\\\$", text):
    fail(r"escaped dollar quotes (\$\$) reached psql; want $$")
if "DO $$" not in text:
    fail(r"missing DO $$ dollar-quote open")

sys.exit(1 if failures else 0)
PY
  if [[ "$rc" -ne 0 ]]; then
    failures=$((failures + 1))
    return 1
  fi
  return 0
}

# --- Render without and with realtime CDC ---
render_final ""
assert_rendered_psql "$captured" "base" || true
render_final "rt"
assert_rendered_psql "$captured" "realtime" || true

# Fixture: deliberately broken double-backslash file must be detected.
bad="${tmp}/bad_double.psql"
cat >"$bad" <<'BAD'
SELECT true AS do_apply, false AS do_exact, false AS do_fail \gset
\if :do_apply
\echo apply
\\elif :do_exact
\echo exact
\\endif
BAD
bad_failures_before=$failures
set +e
assert_rendered_psql "$bad" "fixture-double" >/dev/null 2>"${tmp}/fixture-double.err"
bad_rc=$?
set -e
if [[ "$bad_rc" -eq 0 ]]; then
  fail "fixture: double-backslash detector inert"
else
  # Expected failure — do not count it against the suite.
  failures=$bad_failures_before
  if ! grep -q 'double-backslash psql metacommand' "${tmp}/fixture-double.err"; then
    fail "fixture: double-backslash detector did not report double-backslash"
    cat "${tmp}/fixture-double.err" >&2 || true
  fi
fi

# --- Branch syntax success on empty (apply) and exact (noop) paths ---
pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

PGDATA="${tmp}/pgdata"
PGPORT="$(pick_port)"
ADMIN_USER="$(whoami)"
initdb -D "$PGDATA" --auth-local=trust --auth-host=trust --username="$ADMIN_USER" >/dev/null
cat >>"$PGDATA/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = ${PGPORT}
unix_socket_directories = '${PGDATA}'
EOF
postgres -D "$PGDATA" >/dev/null 2>&1 &
PG_PID=$!
export PGHOST=127.0.0.1
export PGPORT
export PGUSER="$ADMIN_USER"
unset PGPASSWORD || true
for _ in $(seq 1 60); do
  if pg_isready -h "$PGHOST" -p "$PGPORT" -q; then
    break
  fi
  sleep 0.1
done
pg_isready -h "$PGHOST" -p "$PGPORT" -q

build_branch_driver() {
  local src="$1"
  local mode="$2"
  local out="$3"
  python3 - "$src" "$mode" "$out" <<'PY'
import re, sys
src, mode, out = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(src, encoding="utf-8").read().splitlines(keepends=True)
started = False
kept = []
for line in text:
    if re.match(r"^\\if\b", line):
        started = True
    if not started:
        continue
    if re.match(r"^\\(if|elif|else|endif|echo)\b", line):
        kept.append(line)
    elif re.match(r"^\\(i|set)\b", line):
        kept.append("\\echo noop-meta\n")
    elif re.match(r"^\\", line):
        # Preserve unknown / double-backslash metacommands so bad renders fail.
        kept.append(line)
    if re.match(r"^\\endif\b", line) or re.match(r"^\\\\endif\b", line):
        break
body = "".join(kept)
if mode == "apply":
    gset = "SELECT true AS do_apply, false AS do_exact, false AS do_fail \\gset\n"
elif mode == "exact":
    gset = "SELECT false AS do_apply, true AS do_exact, false AS do_fail \\gset\n"
else:
    raise SystemExit(f"unknown mode {mode}")
open(out, "w", encoding="utf-8").write(gset + body + "\\echo branch-ok\n")
PY
}

run_branch() {
  local mode="$1"
  local src="${tmp}/rendered.psql"
  local driver="${tmp}/branch_${mode}.psql"
  build_branch_driver "$src" "$mode" "$driver"
  local out="${tmp}/branch_${mode}.out"
  local rc=0
  PATH="/usr/bin:/bin:/usr/local/bin:${PATH}" \
    psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$ADMIN_USER" -d postgres \
      -f "$driver" >"$out" 2>&1 || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    fail "branch syntax ${mode} path failed (rc=${rc})"
    cat "$out" >&2 || true
    return
  fi
  if ! grep -q 'branch-ok' "$out"; then
    fail "branch syntax ${mode} path did not reach success echo"
    cat "$out" >&2 || true
  fi
}

render_final ""
cp "$captured" "${tmp}/rendered.psql"
run_branch apply
run_branch exact

# Negative: double-backslash elif must fail under ON_ERROR_STOP.
neg="${tmp}/neg_double.psql"
cat >"$neg" <<'NEG'
SELECT true AS do_apply, false AS do_exact, false AS do_fail \gset
\if :do_apply
\echo apply-body
\\elif :do_exact
\echo exact-body
\\endif
\echo should-not-reach
NEG
neg_rc=0
PATH="/usr/bin:/bin:/usr/local/bin:${PATH}" \
  psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$ADMIN_USER" -d postgres \
    -f "$neg" >"${tmp}/neg.out" 2>&1 || neg_rc=$?
if [[ "$neg_rc" -eq 0 ]]; then
  fail "negative fixture: double-backslash \\elif did not fail"
fi
if grep -q 'should-not-reach' "${tmp}/neg.out"; then
  fail "negative fixture: continued past invalid double-backslash metacommand"
fi

if [[ "$failures" -ne 0 ]]; then
  echo "${failures} postgres bootstrap render failure(s)" >&2
  exit 1
fi

echo "ok postgres_bootstrap_render_test"
exit 0
