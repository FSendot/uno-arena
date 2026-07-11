-- Infrastructure-owned role bootstrap for a fresh context database.
-- Does not rewrite service migrations. Local-only passwords come from -v vars.
-- psql variables: bootstrap_user, bootstrap_password, runtime_user, runtime_password

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'bootstrap_user', :'bootstrap_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'bootstrap_user')\gexec

SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'bootstrap_user', :'bootstrap_password')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'bootstrap_user')\gexec

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'runtime_user', :'runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user')\gexec

SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'runtime_user', :'runtime_password')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user')\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'bootstrap_user')\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user')\gexec

GRANT USAGE, CREATE ON SCHEMA public TO :"bootstrap_user";
GRANT USAGE ON SCHEMA public TO :"runtime_user";
-- Runtime must never hold DDL on public.
REVOKE CREATE ON SCHEMA public FROM :"runtime_user";
