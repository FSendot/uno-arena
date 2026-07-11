#!/usr/bin/env bash
# Context-owned Postgres bootstrap (ADR-0027) + CDC prerequisites (ADR-0015/0016).
# ONE admin psql session + ONE transaction (--single-transaction):
#   pg_advisory_xact_lock → empty/exact/drift inspection →
#   drift aborts unchanged; exact commits with zero schema mutations after CDC verify;
#   empty creates roles, SET ROLE bootstrap DDL, migration + meta,
#   exact catalog/version/checksum verify, runtime DML grants, CDC role/publication
#   grants (no logical slots), commit.
# Exact = exactly one schema_migrations row + exactly one schema_bootstrap_meta
# row matching expected version/checksum + exact table/view/matview/sequence/index
# sets + every public ordinary/partitioned table, view, materialized view,
# sequence, and ordinary/partitioned index (relkind i/I) owned by the bootstrap
# DDL role + exact CDC role/publication/grant shape (verified, not fingerprinted
# into schema catalogs). Explicit indexes exclude constraint-backed indexes via
# pg_constraint.conindid (OID join), never by *_pkey / *_key name suffixes.
# Indexes (including partitioned I) count toward empty/drift nonempty detection.
# Fingerprints are generated from embedded migrations (no hand-duplicated SQL).
# Debezium owns logical replication slot lifecycle; bootstrap never creates slots.
# Publication ownership stays with the bootstrap DDL role; CDC roles get
# CONNECT + schema USAGE + SELECT on their outbox table only (no DB CREATE).
set -euo pipefail

: "${PGHOST:?}"
: "${PGPORT:=5432}"
: "${PGDATABASE:?}"
: "${ADMIN_USER:?}"
: "${ADMIN_PASSWORD:?}"
: "${BOOTSTRAP_USER:?}"
: "${BOOTSTRAP_PASSWORD:?}"
: "${RUNTIME_USER:?}"
: "${RUNTIME_PASSWORD:?}"
: "${ADVISORY_LOCK_KEY:?}"
: "${EXPECTED_VERSION:?}"
: "${MIGRATION_FILE:?}"
: "${CONTEXT_NAME:?}"
: "${FINGERPRINT_ENV:=/bootstrap/fingerprints/${CONTEXT_NAME}.env}"
# CDC pipeline (Kafka-bound outbox). Room may also set CDC_REALTIME_*.
: "${CDC_USER:?}"
: "${CDC_PASSWORD:?}"
: "${CDC_PUBLICATION:?}"
: "${CDC_TABLE:?}"
CDC_PEER_TABLE="${CDC_PEER_TABLE:-}"
CDC_REALTIME_USER="${CDC_REALTIME_USER:-}"
CDC_REALTIME_PASSWORD="${CDC_REALTIME_PASSWORD:-}"
CDC_REALTIME_PUBLICATION="${CDC_REALTIME_PUBLICATION:-}"
CDC_REALTIME_TABLE="${CDC_REALTIME_TABLE:-}"

if [[ ! -f "$MIGRATION_FILE" ]]; then
  echo "migration file missing: $MIGRATION_FILE" >&2
  exit 1
fi
if [[ ! -f "$FINGERPRINT_ENV" ]]; then
  echo "fingerprint env missing: $FINGERPRINT_ENV" >&2
  exit 1
fi

if [[ -n "$CDC_REALTIME_USER" || -n "$CDC_REALTIME_PASSWORD" || -n "$CDC_REALTIME_PUBLICATION" || -n "$CDC_REALTIME_TABLE" ]]; then
  : "${CDC_REALTIME_USER:?}"
  : "${CDC_REALTIME_PASSWORD:?}"
  : "${CDC_REALTIME_PUBLICATION:?}"
  : "${CDC_REALTIME_TABLE:?}"
  if [[ "$CDC_REALTIME_USER" == "$CDC_USER" ]]; then
    echo "bootstrap failed: CDC_USER and CDC_REALTIME_USER must be distinct" >&2
    exit 1
  fi
  if [[ "$CDC_REALTIME_PUBLICATION" == "$CDC_PUBLICATION" ]]; then
    echo "bootstrap failed: CDC publications must be distinct" >&2
    exit 1
  fi
  if [[ "$CDC_REALTIME_TABLE" == "$CDC_TABLE" ]]; then
    echo "bootstrap failed: CDC tables must be distinct" >&2
    exit 1
  fi
fi

export PGHOST PGPORT PGDATABASE

# shellcheck disable=SC1090
source "$FINGERPRINT_ENV"
: "${EXPECTED_CHECKSUM:?}"
: "${EXPECTED_TABLES:?}"
EXPECTED_VIEWS="${EXPECTED_VIEWS:-}"
EXPECTED_MATERIALIZED_VIEWS="${EXPECTED_MATERIALIZED_VIEWS:-}"
EXPECTED_SEQUENCES="${EXPECTED_SEQUENCES:-}"
EXPECTED_INDEXES="${EXPECTED_INDEXES:-}"

FILE_CHECKSUM="$(sha256sum "$MIGRATION_FILE" | awk '{print $1}')"
if [[ "$FILE_CHECKSUM" != "$EXPECTED_CHECKSUM" ]]; then
  echo "bootstrap failed: migration checksum ${FILE_CHECKSUM} != fingerprint ${EXPECTED_CHECKSUM}" >&2
  exit 1
fi

psql_admin() {
  PGPASSWORD="$ADMIN_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$ADMIN_USER" -d "$PGDATABASE" "$@"
}

echo "bootstrap postgres context=${CONTEXT_NAME} db=${PGDATABASE} expected=${EXPECTED_VERSION} checksum=${EXPECTED_CHECKSUM:0:12}…"

final="$(mktemp)"
trap 'rm -f "$final"' EXIT

cat >"$final" <<SQL
SELECT pg_advisory_xact_lock(${ADVISORY_LOCK_KEY});

CREATE TEMP TABLE _bootstrap_gate (action text NOT NULL);

DO \$\$
DECLARE
  table_names text[];
  view_names text[];
  matview_names text[];
  sequence_names text[];
  index_names text[];
  expected_tables text[] := string_to_array('${EXPECTED_TABLES}', ',');
  expected_views text[] := CASE WHEN '${EXPECTED_VIEWS}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_VIEWS}', ',') END;
  expected_matviews text[] := CASE WHEN '${EXPECTED_MATERIALIZED_VIEWS}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_MATERIALIZED_VIEWS}', ',') END;
  expected_sequences text[] := CASE WHEN '${EXPECTED_SEQUENCES}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_SEQUENCES}', ',') END;
  expected_indexes text[] := CASE WHEN '${EXPECTED_INDEXES}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_INDEXES}', ',') END;
  user_object_count integer;
  version_count integer;
  meta_count integer;
  found_version text;
  found_meta_version text;
  found_checksum text;
  action text;
  obj record;
BEGIN
  SELECT coalesce(array_agg(tablename ORDER BY tablename), ARRAY[]::text[])
    INTO table_names FROM pg_tables WHERE schemaname = 'public';
  SELECT coalesce(array_agg(viewname ORDER BY viewname), ARRAY[]::text[])
    INTO view_names FROM pg_views WHERE schemaname = 'public';
  SELECT coalesce(array_agg(matviewname ORDER BY matviewname), ARRAY[]::text[])
    INTO matview_names FROM pg_matviews WHERE schemaname = 'public';
  SELECT coalesce(array_agg(sequencename ORDER BY sequencename), ARRAY[]::text[])
    INTO sequence_names FROM pg_sequences WHERE schemaname = 'public';
  -- Explicit CREATE INDEX set only (ordinary i + partitioned I); exclude
  -- constraint-backed indexes by OID (pg_constraint.conindid = pg_index.indexrelid),
  -- never by name suffix.
  SELECT coalesce(array_agg(i.relname ORDER BY i.relname), ARRAY[]::text[])
    INTO index_names
    FROM pg_catalog.pg_class i
    JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
    JOIN pg_catalog.pg_index ix ON ix.indexrelid = i.oid
   WHERE n.nspname = 'public'
     AND i.relkind IN ('i', 'I')
     AND NOT EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conindid = ix.indexrelid
     );

  user_object_count := coalesce(array_length(table_names, 1), 0)
                     + coalesce(array_length(view_names, 1), 0)
                     + coalesce(array_length(matview_names, 1), 0)
                     + coalesce(array_length(sequence_names, 1), 0)
                     + coalesce(array_length(index_names, 1), 0);

  IF user_object_count = 0 THEN
    action := 'apply';
  ELSE
    IF to_regclass('public.schema_migrations') IS NULL
       OR to_regclass('public.schema_bootstrap_meta') IS NULL THEN
      RAISE EXCEPTION 'bootstrap fail: unexpected nonempty schema (missing version/meta)';
    END IF;
    SELECT count(*) INTO version_count FROM schema_migrations;
    SELECT count(*) INTO meta_count FROM schema_bootstrap_meta;
    IF version_count <> 1 OR meta_count <> 1 THEN
      RAISE EXCEPTION 'bootstrap fail: exact requires exactly one schema_migrations row and exactly one schema_bootstrap_meta row (got migrations=% meta=%)',
        version_count, meta_count;
    END IF;
    SELECT version INTO found_version FROM schema_migrations;
    SELECT version, checksum INTO found_meta_version, found_checksum FROM schema_bootstrap_meta;
    IF found_version IS NOT DISTINCT FROM '${EXPECTED_VERSION}'
       AND found_meta_version IS NOT DISTINCT FROM '${EXPECTED_VERSION}'
       AND found_checksum IS NOT DISTINCT FROM '${EXPECTED_CHECKSUM}'
       AND table_names IS NOT DISTINCT FROM expected_tables
       AND view_names IS NOT DISTINCT FROM expected_views
       AND matview_names IS NOT DISTINCT FROM expected_matviews
       AND sequence_names IS NOT DISTINCT FROM expected_sequences
       AND index_names IS NOT DISTINCT FROM expected_indexes THEN
      -- Exact/no-op requires full ownership before accepting (rollback on drift).
      FOR obj IN
        SELECT c.relname, c.relkind, pg_catalog.pg_get_userbyid(c.relowner) AS owner
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
           AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'i', 'I')
      LOOP
        IF obj.owner IS DISTINCT FROM '${BOOTSTRAP_USER}' THEN
          RAISE EXCEPTION 'bootstrap fail: ownership drift % (relkind=%) owner=% expected bootstrap role %',
            obj.relname, obj.relkind, obj.owner, '${BOOTSTRAP_USER}';
        END IF;
      END LOOP;
      action := 'noop';
    ELSE
      RAISE EXCEPTION 'bootstrap fail: schema drift tables=% views=% matviews=% sequences=% indexes=% ver=% meta_ver=% cksum=%',
        table_names, view_names, matview_names, sequence_names, index_names, found_version, found_meta_version, found_checksum;
    END IF;
  END IF;

  INSERT INTO _bootstrap_gate(action) VALUES (action);
END
\$\$;

-- psql \\if requires a boolean variable, not an equality expression.
SELECT
  (action = 'apply') AS do_apply,
  (action = 'noop') AS do_exact,
  (action IS DISTINCT FROM 'apply' AND action IS DISTINCT FROM 'noop') AS do_fail
FROM _bootstrap_gate \\gset

\\if :do_apply
\\echo applying migration ${MIGRATION_FILE} via SET ROLE bootstrap DDL
\\i /bootstrap/sql/postgres_ensure_roles.sql
SET ROLE :"bootstrap_user";
\\i ${MIGRATION_FILE}

CREATE TABLE schema_bootstrap_meta (
  version TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO schema_bootstrap_meta (version, checksum)
VALUES ('${EXPECTED_VERSION}', '${EXPECTED_CHECKSUM}');

DO \$\$
DECLARE
  table_names text[];
  view_names text[];
  matview_names text[];
  sequence_names text[];
  index_names text[];
  expected_tables text[] := string_to_array('${EXPECTED_TABLES}', ',');
  expected_views text[] := CASE WHEN '${EXPECTED_VIEWS}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_VIEWS}', ',') END;
  expected_matviews text[] := CASE WHEN '${EXPECTED_MATERIALIZED_VIEWS}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_MATERIALIZED_VIEWS}', ',') END;
  expected_sequences text[] := CASE WHEN '${EXPECTED_SEQUENCES}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_SEQUENCES}', ',') END;
  expected_indexes text[] := CASE WHEN '${EXPECTED_INDEXES}' = '' THEN ARRAY[]::text[] ELSE string_to_array('${EXPECTED_INDEXES}', ',') END;
  version_count integer;
  meta_count integer;
  found_version text;
  found_meta_version text;
  found_checksum text;
  obj record;
BEGIN
  SELECT coalesce(array_agg(tablename ORDER BY tablename), ARRAY[]::text[])
    INTO table_names FROM pg_tables WHERE schemaname = 'public';
  SELECT coalesce(array_agg(viewname ORDER BY viewname), ARRAY[]::text[])
    INTO view_names FROM pg_views WHERE schemaname = 'public';
  SELECT coalesce(array_agg(matviewname ORDER BY matviewname), ARRAY[]::text[])
    INTO matview_names FROM pg_matviews WHERE schemaname = 'public';
  SELECT coalesce(array_agg(sequencename ORDER BY sequencename), ARRAY[]::text[])
    INTO sequence_names FROM pg_sequences WHERE schemaname = 'public';
  SELECT coalesce(array_agg(i.relname ORDER BY i.relname), ARRAY[]::text[])
    INTO index_names
    FROM pg_catalog.pg_class i
    JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
    JOIN pg_catalog.pg_index ix ON ix.indexrelid = i.oid
   WHERE n.nspname = 'public'
     AND i.relkind IN ('i', 'I')
     AND NOT EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conindid = ix.indexrelid
     );
  SELECT count(*) INTO version_count FROM schema_migrations;
  SELECT count(*) INTO meta_count FROM schema_bootstrap_meta;
  IF version_count <> 1 OR meta_count <> 1 THEN
    RAISE EXCEPTION 'post-apply exact-one-row fail migrations=% meta=%', version_count, meta_count;
  END IF;
  SELECT version INTO found_version FROM schema_migrations;
  SELECT version, checksum INTO found_meta_version, found_checksum FROM schema_bootstrap_meta;

  IF found_version IS DISTINCT FROM '${EXPECTED_VERSION}'
     OR found_meta_version IS DISTINCT FROM '${EXPECTED_VERSION}' THEN
    RAISE EXCEPTION 'post-apply version mismatch migrations=% meta=%', found_version, found_meta_version;
  END IF;
  IF found_checksum IS DISTINCT FROM '${EXPECTED_CHECKSUM}' THEN
    RAISE EXCEPTION 'post-apply checksum mismatch found=%', found_checksum;
  END IF;
  IF table_names IS DISTINCT FROM expected_tables
     OR view_names IS DISTINCT FROM expected_views
     OR matview_names IS DISTINCT FROM expected_matviews
     OR sequence_names IS DISTINCT FROM expected_sequences
     OR index_names IS DISTINCT FROM expected_indexes THEN
    RAISE EXCEPTION 'post-apply catalog mismatch tables=% views=% matviews=% sequences=% indexes=%',
      table_names, view_names, matview_names, sequence_names, index_names;
  END IF;

  -- Every public migrated/bootstrap object must be owned by the bootstrap DDL role.
  -- Includes ordinary (r) and partitioned (p) tables, views (v), matviews (m),
  -- sequences (S), and ordinary (i) / partitioned (I) indexes.
  FOR obj IN
    SELECT c.relname, c.relkind, pg_catalog.pg_get_userbyid(c.relowner) AS owner
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'i', 'I')
  LOOP
    IF obj.owner IS DISTINCT FROM current_user THEN
      RAISE EXCEPTION 'post-apply ownership % (%) != bootstrap role %', obj.relname, obj.owner, current_user;
    END IF;
  END LOOP;
END
\$\$;

RESET ROLE;
\\i /bootstrap/sql/postgres_grant_runtime.sql
\\set cdc_user '$CDC_USER'
\\set cdc_password '$CDC_PASSWORD'
\\set cdc_publication '$CDC_PUBLICATION'
\\set cdc_table '$CDC_TABLE'
\\set cdc_peer_table '$CDC_PEER_TABLE'
\\i /bootstrap/sql/postgres_cdc_apply.sql
\\i /bootstrap/sql/postgres_cdc_verify.sql
SQL

if [[ -n "$CDC_REALTIME_USER" ]]; then
  cat >>"$final" <<SQL
\\set cdc_user '$CDC_REALTIME_USER'
\\set cdc_password '$CDC_REALTIME_PASSWORD'
\\set cdc_publication '$CDC_REALTIME_PUBLICATION'
\\set cdc_table '$CDC_REALTIME_TABLE'
\\set cdc_peer_table '$CDC_TABLE'
\\i /bootstrap/sql/postgres_cdc_apply.sql
\\i /bootstrap/sql/postgres_cdc_verify.sql
SQL
fi

# Quoted heredoc: backslashes are literal (no collapse). Emit one backslash for psql.
cat >>"$final" <<'SQL'
\elif :do_exact
\echo bootstrap no-op: exact version/checksum/catalog (zero schema mutations); verifying CDC
SQL

# Exact path: CDC structural verify only (no CREATE/ALTER/GRANT).
cat >>"$final" <<SQL
\\set cdc_user '$CDC_USER'
\\set cdc_password '$CDC_PASSWORD'
\\set cdc_publication '$CDC_PUBLICATION'
\\set cdc_table '$CDC_TABLE'
\\set cdc_peer_table '$CDC_PEER_TABLE'
\\i /bootstrap/sql/postgres_cdc_verify.sql
SQL

if [[ -n "$CDC_REALTIME_USER" ]]; then
  cat >>"$final" <<SQL
\\set cdc_user '$CDC_REALTIME_USER'
\\set cdc_password '$CDC_REALTIME_PASSWORD'
\\set cdc_publication '$CDC_REALTIME_PUBLICATION'
\\set cdc_table '$CDC_REALTIME_TABLE'
\\set cdc_peer_table '$CDC_TABLE'
\\i /bootstrap/sql/postgres_cdc_verify.sql
SQL
fi

# Quoted heredoc: single-backslash metacommands + raw $$ (not \$\$).
cat >>"$final" <<'SQL'
\elif :do_fail
DO $$ BEGIN RAISE EXCEPTION 'bootstrap fail: unknown action'; END $$;
\endif
SQL

# Single admin session: lock, gate, optional mutate, commit. No pre-gate role mutation.
psql_admin --single-transaction \
  -v bootstrap_user="$BOOTSTRAP_USER" \
  -v bootstrap_password="$BOOTSTRAP_PASSWORD" \
  -v runtime_user="$RUNTIME_USER" \
  -v runtime_password="$RUNTIME_PASSWORD" \
  -f "$final"

echo "bootstrap postgres context=${CONTEXT_NAME} ok version=${EXPECTED_VERSION}"
