-- Apply one least-privilege CDC pipeline after context schema exists.
-- Does not create logical replication slots (Debezium owns slot lifecycle).
-- Publication is owned by the bootstrap DDL role. The CDC LOGIN REPLICATION
-- role receives CONNECT, schema USAGE, and SELECT on the outbox table only —
-- never CREATE on the database (ownership transfer does not require it) and
-- never default sequence privileges.
-- psql variables:
--   bootstrap_user, cdc_user, cdc_password, cdc_publication, cdc_table
--   cdc_peer_table — optional sibling outbox to keep revoked (Room dual path)
-- Values read inside DO blocks are staged with set_config outside the block:
-- psql :'vars' do not interpolate inside dollar-quoted DO bodies.

SELECT format(
  'CREATE ROLE %I LOGIN REPLICATION PASSWORD %L',
  :'cdc_user', :'cdc_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user')\gexec

SELECT format(
  'ALTER ROLE %I LOGIN REPLICATION PASSWORD %L',
  :'cdc_user', :'cdc_password'
)
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user')\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'cdc_user')\gexec

-- PG14 grants CREATE on public to PUBLIC by default; strip it so the CDC role
-- cannot inherit schema CREATE. Full bootstrap also revokes PUBLIC in grant_runtime.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"cdc_user";
REVOKE CREATE ON SCHEMA public FROM :"cdc_user";

SELECT format('GRANT SELECT ON TABLE public.%I TO %I', :'cdc_table', :'cdc_user')\gexec

SELECT format(
  'CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish = %L)',
  :'cdc_publication', :'cdc_table', 'insert'
)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_publication WHERE pubname = :'cdc_publication'
)\gexec

-- Admin may create the publication; reassign to bootstrap DDL without granting
-- database CREATE to the CDC role.
SELECT format(
  'ALTER PUBLICATION %I OWNER TO %I',
  :'cdc_publication', :'bootstrap_user'
)
WHERE EXISTS (
  SELECT 1 FROM pg_publication WHERE pubname = :'cdc_publication'
)\gexec

-- Session-local (is_local=false): survives autocommit statement boundaries;
-- production bootstrap still wraps these in --single-transaction.
SELECT set_config('uno_arena.cdc_user', :'cdc_user', false);
SELECT set_config('uno_arena.cdc_publication', :'cdc_publication', false);
SELECT set_config('uno_arena.cdc_table', :'cdc_table', false);
SELECT set_config('uno_arena.cdc_peer_table', :'cdc_peer_table', false);
SELECT set_config('uno_arena.bootstrap_user', :'bootstrap_user', false);

DO $cdc$
DECLARE
  pub name := current_setting('uno_arena.cdc_publication');
  want name := current_setting('uno_arena.cdc_table');
  peer text := NULLIF(trim(both FROM current_setting('uno_arena.cdc_peer_table')), '');
  cdc_role name := current_setting('uno_arena.cdc_user');
  boot name := current_setting('uno_arena.bootstrap_user');
  tables text[];
  pub_owner text;
BEGIN
  SELECT coalesce(array_agg(pt.tablename ORDER BY pt.tablename), ARRAY[]::text[])
    INTO tables
    FROM pg_publication_tables pt
   WHERE pt.pubname = pub;

  IF tables IS DISTINCT FROM ARRAY[want::text] THEN
    EXECUTE format('DROP PUBLICATION %I', pub);
    EXECUTE format(
      'CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish = %L)',
      pub, want, 'insert'
    );
  END IF;

  SELECT pg_catalog.pg_get_userbyid(p.pubowner) INTO pub_owner
    FROM pg_publication p WHERE p.pubname = pub;
  IF pub_owner IS DISTINCT FROM boot THEN
    EXECUTE format('ALTER PUBLICATION %I OWNER TO %I', pub, boot);
  END IF;

  IF peer IS NOT NULL THEN
    IF to_regclass(format('public.%I', peer)) IS NOT NULL
       AND has_table_privilege(cdc_role, format('public.%I', peer), 'SELECT') THEN
      EXECUTE format('REVOKE SELECT ON TABLE public.%I FROM %I', peer, cdc_role);
    END IF;
  END IF;
END
$cdc$;
