# Context-owned store bootstrap image

Build from the repository root (embeds `services/*/migrations` as the schema
source of truth; wrappers under `sql/` only add roles / empty-store gates).
Expected catalogs are generated at image build from those migrations
(`lib/fingerprint.rb` → `/bootstrap/fingerprints/*.env`); SQL is not hand-duplicated.

```bash
docker build -f infrastructure/bootstrap/Dockerfile -t uno-arena/bootstrap:local .
```

Or via kind helper:

```bash
make kind-build-load-bootstrap
```

Entrypoint modes: `postgres` | `clickhouse`.

## Postgres gate (ADR-0027) + CDC prerequisites (ADR-0015/0016)

One admin `psql --single-transaction` session (no pre-gate role/password/grant
mutation; no separate bootstrap login for the gate):

1. `pg_advisory_xact_lock`
2. Inspect empty / exact / drift. Drift raises and aborts unchanged. Exact
   commits with **zero schema mutations** only after full ownership validation
   and CDC role/publication/grant verification.
3. Empty only: create roles → `SET ROLE` bootstrap DDL → migration +
   `schema_bootstrap_meta` → verify exact catalog/version/checksum (every
   public ordinary/partitioned table, view, materialized view, sequence, and
   ordinary/partitioned index owned by bootstrap; runtime has no DDL) →
   runtime DML grants → least-privilege CDC `LOGIN REPLICATION` role(s),
   table-filtered publication(s) owned by the bootstrap DDL role, SELECT-only
   outbox grants (CONNECT + schema USAGE; no database CREATE for CDC) → commit.
   Room creates two CDC roles/publications (`integration_outbox_events` vs
   `realtime_outbox_events`) so Kafka and Redis pipelines cannot cross-read.
   Bootstrap never creates logical replication slots.
4. Exact requires **exactly one** `schema_migrations` row and **exactly one**
   `schema_bootstrap_meta` row matching expected version/checksum plus exact
   tables/views/matviews/sequences/indexes (full set equality; unexpected
   indexes fail). Index discovery includes `relkind` `i` and partitioned `I`.
   Constraint-backed indexes are excluded by joining `pg_constraint.conindid`
   to `pg_index.indexrelid` (never by `_pkey`/`_key` name suffix). Extra
   metadata rows fail closed. Ownership drift on exact raises and rolls back
   with no mutation. CDC shape drift fails closed without rewriting schema
   fingerprints.
5. Branch control uses boolean `\gset` variables (`do_apply` / `do_exact` /
   `do_fail`) consumed by `\if` — not expression-style `\if`.
6. Any failure rolls back the transaction. Views/matviews/sequences/**indexes**
   (including partitioned) count as nonempty.

## ClickHouse gate (ADR-0028)

Validate empty/exact/drift **before** users/grants/DDL. Exact → zero mutations.
Exact/no-op requires the completion marker row (`schema_bootstrap_meta`) **and**
exact table and view catalogs with **no** default-DB in-progress sentinel.
Catalog reads fail closed: each list query is captured via
`raw="$(list_…)" || exit 1` with `shopt -s inherit_errexit` (not `mapfile` +
process substitution, which discards producer status). A read error exits
before any mutation.

Empty apply is best-effort (ClickHouse has no cross-DDL transactions). Order:

1. **First mutation:** admin-owned `_bootstrap_in_progress` sentinel in the
   existing `default` database (before `CREATE DATABASE analytics` or any
   user/grant). Preflight rejects if this sentinel already exists.
2. `CREATE DATABASE analytics` / create users / bootstrap grants
3. Split the fingerprint-checked migration and POST **one statement per HTTP
   request** under bootstrap credentials (ClickHouse rejects multi-statement
   bodies with Code 62; do not use `multiquery=1`). Stop at first failure.
   Then create empty `schema_bootstrap_meta` table
4. Post-apply exact table **and** view verification (default sentinel still
   present; exact/no-op requires its absence)
5. Runtime DML grants / DDL revoke
6. DROP default sentinel
7. **Final mutation only:** insert the completion marker

### Nontransactional recovery

If any step after the sentinel fails, the next run sees the default sentinel
and/or nonempty analytics without an exact completion marker and **fails
closed**. There is no in-place repair of partial users/grants/DDL: disposable
cleanup is `make kind-reset`.

## Offline fingerprint / structure tests

```bash
ruby infrastructure/bootstrap/tests/fingerprint_test.rb
ruby infrastructure/bootstrap/tests/psql_gate_structure_test.rb
ruby infrastructure/bootstrap/tests/cdc_structure_test.rb
bash infrastructure/bootstrap/tests/postgres_bootstrap_fail_closed_test.sh
bash infrastructure/bootstrap/tests/postgres_bootstrap_render_test.sh
bash infrastructure/bootstrap/tests/cdc_sql_integration_test.sh
bash infrastructure/bootstrap/tests/clickhouse_catalog_fail_closed_test.sh
ruby infrastructure/bootstrap/tests/clickhouse_sql_split_test.rb
```