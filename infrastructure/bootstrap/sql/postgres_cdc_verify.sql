-- Verify one CDC pipeline is exact (zero mutations). Fail closed on drift.
-- Publication owner must be the bootstrap DDL role. CDC role must remain
-- LOGIN REPLICATION with CONNECT + schema USAGE + SELECT-only on its outbox —
-- no database CREATE, no schema CREATE, no peer outbox SELECT.
-- Runtime must remain non-replication.
-- psql variables:
--   bootstrap_user, runtime_user, cdc_user, cdc_publication, cdc_table
--   cdc_peer_table — optional sibling outbox that must remain unreadable
-- Values read inside DO are staged with set_config (psql :'vars' do not
-- interpolate inside dollar-quoted DO bodies).

-- Session-local (is_local=false): survives autocommit statement boundaries.
SELECT set_config('uno_arena.cdc_user', :'cdc_user', false);
SELECT set_config('uno_arena.cdc_publication', :'cdc_publication', false);
SELECT set_config('uno_arena.cdc_table', :'cdc_table', false);
SELECT set_config('uno_arena.cdc_peer_table', :'cdc_peer_table', false);
SELECT set_config('uno_arena.bootstrap_user', :'bootstrap_user', false);
SELECT set_config('uno_arena.runtime_user', :'runtime_user', false);

DO $cdc_verify$
DECLARE
  pub name := current_setting('uno_arena.cdc_publication');
  want name := current_setting('uno_arena.cdc_table');
  peer text := NULLIF(trim(both FROM current_setting('uno_arena.cdc_peer_table')), '');
  cdc_role name := current_setting('uno_arena.cdc_user');
  boot name := current_setting('uno_arena.bootstrap_user');
  runtime_role name := current_setting('uno_arena.runtime_user');
  tables text[];
  can_login boolean;
  can_repl boolean;
  is_super boolean;
  pub_owner text;
  runtime_repl boolean;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = cdc_role) THEN
    RAISE EXCEPTION 'cdc verify fail: missing role %', cdc_role;
  END IF;

  SELECT r.rolcanlogin, r.rolreplication, r.rolsuper
    INTO can_login, can_repl, is_super
    FROM pg_roles r
   WHERE r.rolname = cdc_role;

  IF NOT can_login OR NOT can_repl THEN
    RAISE EXCEPTION 'cdc verify fail: role % must be LOGIN REPLICATION (login=% is_repl=%)',
      cdc_role, can_login, can_repl;
  END IF;
  IF is_super THEN
    RAISE EXCEPTION 'cdc verify fail: role % must not be superuser', cdc_role;
  END IF;

  SELECT r.rolreplication INTO runtime_repl
    FROM pg_roles r WHERE r.rolname = runtime_role;
  IF coalesce(runtime_repl, false) THEN
    RAISE EXCEPTION 'cdc verify fail: runtime role % must not have REPLICATION', runtime_role;
  END IF;

  IF NOT has_database_privilege(cdc_role, current_database(), 'CONNECT') THEN
    RAISE EXCEPTION 'cdc verify fail: % missing CONNECT on %', cdc_role, current_database();
  END IF;
  IF has_database_privilege(cdc_role, current_database(), 'CREATE') THEN
    RAISE EXCEPTION 'cdc verify fail: % must not have CREATE on database %',
      cdc_role, current_database();
  END IF;
  IF NOT has_schema_privilege(cdc_role, 'public', 'USAGE') THEN
    RAISE EXCEPTION 'cdc verify fail: % missing USAGE on public', cdc_role;
  END IF;
  IF has_schema_privilege(cdc_role, 'public', 'CREATE') THEN
    RAISE EXCEPTION 'cdc verify fail: % must not have CREATE on public', cdc_role;
  END IF;
  IF to_regclass(format('public.%I', want)) IS NULL THEN
    RAISE EXCEPTION 'cdc verify fail: missing table public.%', want;
  END IF;
  IF NOT has_table_privilege(cdc_role, format('public.%I', want), 'SELECT') THEN
    RAISE EXCEPTION 'cdc verify fail: % missing SELECT on %', cdc_role, want;
  END IF;
  IF has_table_privilege(cdc_role, format('public.%I', want), 'INSERT')
     OR has_table_privilege(cdc_role, format('public.%I', want), 'UPDATE')
     OR has_table_privilege(cdc_role, format('public.%I', want), 'DELETE') THEN
    RAISE EXCEPTION 'cdc verify fail: % must be SELECT-only on %', cdc_role, want;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = boot) THEN
    RAISE EXCEPTION 'cdc verify fail: missing bootstrap role %', boot;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = pub) THEN
    RAISE EXCEPTION 'cdc verify fail: missing publication %', pub;
  END IF;

  SELECT pg_catalog.pg_get_userbyid(p.pubowner) INTO pub_owner
    FROM pg_publication p WHERE p.pubname = pub;
  IF pub_owner IS DISTINCT FROM boot THEN
    RAISE EXCEPTION 'cdc verify fail: publication % owner=% expected bootstrap role %',
      pub, pub_owner, boot;
  END IF;

  SELECT coalesce(array_agg(pt.tablename ORDER BY pt.tablename), ARRAY[]::text[])
    INTO tables
    FROM pg_publication_tables pt
   WHERE pt.pubname = pub;
  IF tables IS DISTINCT FROM ARRAY[want::text] THEN
    RAISE EXCEPTION 'cdc verify fail: publication % tables=% expected=[%]', pub, tables, want;
  END IF;

  IF peer IS NOT NULL THEN
    IF to_regclass(format('public.%I', peer)) IS NOT NULL
       AND has_table_privilege(cdc_role, format('public.%I', peer), 'SELECT') THEN
      RAISE EXCEPTION 'cdc verify fail: % must not SELECT peer outbox %', cdc_role, peer;
    END IF;
  END IF;
END
$cdc_verify$;
