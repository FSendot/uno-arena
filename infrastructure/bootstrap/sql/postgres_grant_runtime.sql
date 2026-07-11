-- Grant DML-only privileges to the runtime role after schema exists.
-- psql variables: runtime_user, bootstrap_user

GRANT USAGE ON SCHEMA public TO :"runtime_user";
REVOKE CREATE ON SCHEMA public FROM :"runtime_user";

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :"runtime_user";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO :"runtime_user";

ALTER DEFAULT PRIVILEGES FOR ROLE :"bootstrap_user" IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"runtime_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"bootstrap_user" IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO :"runtime_user";

-- Explicitly deny DDL-ish rights that might exist from PUBLIC defaults.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"bootstrap_user";
GRANT USAGE ON SCHEMA public TO :"runtime_user";
GRANT CREATE ON SCHEMA public TO :"bootstrap_user";
