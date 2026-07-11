#!/usr/bin/env bash
# Integration: apply/verify real CDC SQL against a disposable local Postgres.
# Starts an isolated initdb cluster on an ephemeral port. Never uses or resets
# authoritative context database names (identity, room_*, tournament, ranking, …).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APPLY="${ROOT}/infrastructure/bootstrap/sql/postgres_cdc_apply.sql"
VERIFY="${ROOT}/infrastructure/bootstrap/sql/postgres_cdc_verify.sql"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd initdb
require_cmd postgres
require_cmd psql
require_cmd pg_isready
require_cmd python3

AUTHORITATIVE_DB_NAMES=(
  identity
  identity_db
  room_gameplay
  room-gameplay
  room_gameplay_db
  tournament
  tournament_orchestration
  tournament_db
  ranking
  ranking_db
  analytics
  postgres
  template0
  template1
)

is_authoritative_db() {
  local name="$1"
  local banned
  for banned in "${AUTHORITATIVE_DB_NAMES[@]}"; do
    if [[ "$name" == "$banned" ]]; then
      return 0
    fi
  done
  return 1
}

pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

PGDATA="$(mktemp -d "${TMPDIR:-/tmp}/uno-cdc-it.XXXXXX")"
PGPORT="$(pick_port)"
# Disposable names only — never authoritative context DBs.
DB_NAME="cdc_it_${RANDOM}_$$"
BOOTSTRAP_USER="cdc_it_boot_${RANDOM}"
RUNTIME_USER="cdc_it_rt_${RANDOM}"
CDC_USER="cdc_it_cdc_${RANDOM}"
CDC_PASSWORD="cdc-it-pass-${RANDOM}"
CDC_PUBLICATION="cdc_it_pub_${RANDOM}"
CDC_TABLE="integration_outbox_events"
CDC_PEER_TABLE="realtime_outbox_events"
ADMIN_USER="$(whoami)"

if is_authoritative_db "$DB_NAME"; then
  echo "refusing authoritative database name: $DB_NAME" >&2
  exit 1
fi

cleanup() {
  if [[ -n "${PG_PID:-}" ]] && kill -0 "$PG_PID" 2>/dev/null; then
    kill "$PG_PID" 2>/dev/null || true
    wait "$PG_PID" 2>/dev/null || true
  fi
  rm -rf "$PGDATA"
}
trap cleanup EXIT

echo "initdb disposable cluster in $PGDATA port=$PGPORT db=$DB_NAME"
initdb -D "$PGDATA" --auth-local=trust --auth-host=trust --username="$ADMIN_USER" >/dev/null
cat >>"$PGDATA/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = ${PGPORT}
unix_socket_directories = '${PGDATA}'
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
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

psql -v ON_ERROR_STOP=1 -d postgres -v db_name="$DB_NAME" <<'SQL'
SELECT format('CREATE DATABASE %I', :'db_name')\gexec
SQL

psql_db() {
  psql -v ON_ERROR_STOP=1 -d "$DB_NAME" "$@"
}

echo "creating fixture roles + dual outbox tables"
psql_db \
  -v bootstrap_user="$BOOTSTRAP_USER" \
  -v runtime_user="$RUNTIME_USER" \
  -v cdc_table="$CDC_TABLE" \
  -v cdc_peer_table="$CDC_PEER_TABLE" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'bootstrap_user', 'boot-pass')\gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'runtime_user', 'rt-pass')\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'bootstrap_user')\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user')\gexec
GRANT USAGE, CREATE ON SCHEMA public TO :"bootstrap_user";
GRANT USAGE ON SCHEMA public TO :"runtime_user";

SELECT set_config('uno_arena.bootstrap_user', :'bootstrap_user', false);
SELECT set_config('uno_arena.cdc_table', :'cdc_table', false);
SELECT set_config('uno_arena.cdc_peer_table', :'cdc_peer_table', false);

DO $fix$
DECLARE
  boot name := current_setting('uno_arena.bootstrap_user');
  outbox name := current_setting('uno_arena.cdc_table');
  peer name := current_setting('uno_arena.cdc_peer_table');
BEGIN
  EXECUTE format('SET ROLE %I', boot);
  EXECUTE format(
    'CREATE TABLE public.%I (id bigserial PRIMARY KEY, payload text NOT NULL)',
    outbox
  );
  EXECUTE format(
    'CREATE TABLE public.%I (id bigserial PRIMARY KEY, payload text NOT NULL)',
    peer
  );
  EXECUTE 'RESET ROLE';
END
$fix$;
SQL

run_cdc_sql() {
  local file="$1"
  psql_db \
    -v bootstrap_user="$BOOTSTRAP_USER" \
    -v runtime_user="$RUNTIME_USER" \
    -v cdc_user="$CDC_USER" \
    -v cdc_password="$CDC_PASSWORD" \
    -v cdc_publication="$CDC_PUBLICATION" \
    -v cdc_table="$CDC_TABLE" \
    -v cdc_peer_table="$CDC_PEER_TABLE" \
    -f "$file"
}

echo "apply CDC SQL"
run_cdc_sql "$APPLY"

echo "verify CDC SQL (post-apply)"
run_cdc_sql "$VERIFY"

echo "assert LOGIN REPLICATION + exact one-table publication + SELECT-only + peer denial"
psql_db \
  -v bootstrap_user="$BOOTSTRAP_USER" \
  -v cdc_user="$CDC_USER" \
  -v cdc_publication="$CDC_PUBLICATION" \
  -v cdc_table="$CDC_TABLE" \
  -v cdc_peer_table="$CDC_PEER_TABLE" <<'SQL'
SELECT set_config('uno_arena.bootstrap_user', :'bootstrap_user', false);
SELECT set_config('uno_arena.cdc_user', :'cdc_user', false);
SELECT set_config('uno_arena.cdc_publication', :'cdc_publication', false);
SELECT set_config('uno_arena.cdc_table', :'cdc_table', false);
SELECT set_config('uno_arena.cdc_peer_table', :'cdc_peer_table', false);

DO $assert$
DECLARE
  boot name := current_setting('uno_arena.bootstrap_user');
  cdc_role name := current_setting('uno_arena.cdc_user');
  pub name := current_setting('uno_arena.cdc_publication');
  want name := current_setting('uno_arena.cdc_table');
  peer name := current_setting('uno_arena.cdc_peer_table');
  can_login boolean;
  can_repl boolean;
  pub_owner text;
  tables text[];
BEGIN
  SELECT r.rolcanlogin, r.rolreplication INTO can_login, can_repl
    FROM pg_roles r WHERE r.rolname = cdc_role;
  IF NOT can_login OR NOT can_repl THEN
    RAISE EXCEPTION 'integration: CDC role flags login=% repl=%', can_login, can_repl;
  END IF;

  SELECT pg_catalog.pg_get_userbyid(p.pubowner) INTO pub_owner
    FROM pg_publication p WHERE p.pubname = pub;
  IF pub_owner IS DISTINCT FROM boot THEN
    RAISE EXCEPTION 'integration: pub owner=% want bootstrap %', pub_owner, boot;
  END IF;

  SELECT coalesce(array_agg(pt.tablename ORDER BY pt.tablename), ARRAY[]::text[])
    INTO tables FROM pg_publication_tables pt WHERE pt.pubname = pub;
  IF tables IS DISTINCT FROM ARRAY[want::text] THEN
    RAISE EXCEPTION 'integration: publication tables=%', tables;
  END IF;

  IF has_database_privilege(cdc_role, current_database(), 'CREATE') THEN
    RAISE EXCEPTION 'integration: CDC must not have database CREATE';
  END IF;
  IF NOT has_table_privilege(cdc_role, format('public.%I', want), 'SELECT') THEN
    RAISE EXCEPTION 'integration: missing SELECT on outbox';
  END IF;
  IF has_table_privilege(cdc_role, format('public.%I', want), 'INSERT') THEN
    RAISE EXCEPTION 'integration: CDC must be SELECT-only';
  END IF;
  IF has_table_privilege(cdc_role, format('public.%I', peer), 'SELECT') THEN
    RAISE EXCEPTION 'integration: peer SELECT must be denied';
  END IF;
END
$assert$;
SQL

# Login as CDC role: SELECT allowed on outbox, denied on peer.
out_rc=0
PGPASSWORD="$CDC_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$CDC_USER" -d "$DB_NAME" \
  -c "SELECT count(*) FROM public.${CDC_TABLE};" >/dev/null || out_rc=$?
if [[ "$out_rc" -ne 0 ]]; then
  echo "FAIL: CDC role could not SELECT its outbox" >&2
  exit 1
fi
peer_login_rc=0
set +e
PGPASSWORD="$CDC_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$CDC_USER" -d "$DB_NAME" \
  -c "SELECT count(*) FROM public.${CDC_PEER_TABLE};" >/dev/null 2>&1
peer_login_rc=$?
set -e
if [[ "$peer_login_rc" -eq 0 ]]; then
  echo "FAIL: CDC role could SELECT peer outbox" >&2
  exit 1
fi

echo "rerun verify as zero-mutation exact path"
before_pub="$(psql_db -Atc "SELECT coalesce(string_agg(pubname, ',' ORDER BY pubname), '') FROM pg_publication;")"
before_role="$(psql_db -At -v cdc_user="$CDC_USER" <<'SQL'
SELECT rolcanlogin::text || ':' || rolreplication::text
  FROM pg_roles WHERE rolname = :'cdc_user';
SQL
)"
run_cdc_sql "$VERIFY"
after_pub="$(psql_db -Atc "SELECT coalesce(string_agg(pubname, ',' ORDER BY pubname), '') FROM pg_publication;")"
after_role="$(psql_db -At -v cdc_user="$CDC_USER" <<'SQL'
SELECT rolcanlogin::text || ':' || rolreplication::text
  FROM pg_roles WHERE rolname = :'cdc_user';
SQL
)"
if [[ "$before_pub" != "$after_pub" || "$before_role" != "$after_role" ]]; then
  echo "FAIL: verify mutated catalog state" >&2
  exit 1
fi

echo "inject bad peer SELECT — verify must fail nonzero"
psql_db -v cdc_user="$CDC_USER" -v cdc_peer_table="$CDC_PEER_TABLE" <<'SQL'
SELECT format('GRANT SELECT ON TABLE public.%I TO %I', :'cdc_peer_table', :'cdc_user')\gexec
SQL
set +e
run_cdc_sql "$VERIFY" >/tmp/cdc_it_verify_peer.out 2>&1
peer_rc=$?
set -e
if [[ "$peer_rc" -eq 0 ]]; then
  echo "FAIL: verify succeeded with peer SELECT granted" >&2
  cat /tmp/cdc_it_verify_peer.out >&2
  exit 1
fi
psql_db -v cdc_user="$CDC_USER" -v cdc_peer_table="$CDC_PEER_TABLE" <<'SQL'
SELECT format('REVOKE SELECT ON TABLE public.%I FROM %I', :'cdc_peer_table', :'cdc_user')\gexec
SQL
run_cdc_sql "$VERIFY" >/dev/null

echo "inject bad publication membership — verify must fail nonzero"
psql_db \
  -v bootstrap_user="$BOOTSTRAP_USER" \
  -v cdc_publication="$CDC_PUBLICATION" \
  -v cdc_table="$CDC_TABLE" \
  -v cdc_peer_table="$CDC_PEER_TABLE" <<'SQL'
SELECT format('DROP PUBLICATION %I', :'cdc_publication')\gexec
SELECT format(
  'CREATE PUBLICATION %I FOR TABLE public.%I, public.%I WITH (publish = %L)',
  :'cdc_publication', :'cdc_table', :'cdc_peer_table', 'insert'
)\gexec
SELECT format('ALTER PUBLICATION %I OWNER TO %I', :'cdc_publication', :'bootstrap_user')\gexec
SQL
set +e
run_cdc_sql "$VERIFY" >/tmp/cdc_it_verify_pub.out 2>&1
pub_rc=$?
set -e
if [[ "$pub_rc" -eq 0 ]]; then
  echo "FAIL: verify succeeded with multi-table publication" >&2
  cat /tmp/cdc_it_verify_pub.out >&2
  exit 1
fi

echo "ok cdc_sql_integration_test db=${DB_NAME}"
exit 0
