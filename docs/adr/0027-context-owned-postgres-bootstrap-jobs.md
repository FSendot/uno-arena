# Context-Owned Postgres Bootstrap Jobs

## Status
Accepted

## Context
Identity, Room Gameplay, Tournament Orchestration, and Ranking each own an independent Postgres database. Application replicas may start concurrently, and this project has not yet deployed persistent state, so the first production-capable implementation needs deterministic empty-database initialization without introducing an upgrade-migration history that is not currently required.

## Decision
Each Postgres-backed bounded context owns a dedicated Kubernetes bootstrap Job and its schema definition. The Job connects with an admin credential for a single advisory-locked transaction: it inspects empty/exact/drift before any role, password, grant, or schema mutation. Drift fails unchanged; exact commits with zero mutations. Empty initialization creates the dedicated bootstrap DDL role, applies schema via `SET ROLE` so migration objects are attributable to that role, stamps checksum metadata, verifies the exact catalog, grants runtime DML only, and commits atomically. Runtime credentials never receive DDL.

An empty database is initialized transactionally and stamped with the expected schema version. Exact means exactly one `schema_migrations` row and exactly one `schema_bootstrap_meta` row matching the expected version/checksum plus the expected catalog; extra metadata rows fail closed. A database already at that exact state is a successful no-op. Any non-empty, partially initialized, newer, older, or otherwise unexpected schema fails the Job without modifying or dropping data. Resetting the current disposable environment is an explicit operator action that recreates the database or volume before rerunning the Job; the Job never performs a destructive reset.

Application and worker pods use separate runtime credentials without DDL privileges. On startup and readiness they verify the expected schema version and required capabilities; they do not create, alter, or repair schema. Workloads remain unavailable until the bootstrap Job succeeds. Upgrade migrations, rollback migrations, and preservation of previously deployed data are intentionally outside the current pre-deployment scope and require a later decision before the first stateful upgrade.

## Consequences
Concurrent pod startup cannot race schema creation, schema ownership stays within its bounded context, and runtime compromise does not grant DDL authority. Kubernetes deployment ordering must run and await each bootstrap Job before releasing the corresponding service and CDC connector. Bootstrap credentials, advisory-lock keys, schema metadata, Job retries, and failure diagnostics become operational concerns. Local `kind` uses the same Job behavior against disposable volumes, preserving deployment parity without pretending an upgrade path already exists.
