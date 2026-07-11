# Store-Specific Initialization

## Status
Accepted

## Context
The production architecture uses ClickHouse, KurrentDB, and Redis in addition to context-owned Postgres databases. Applying the Postgres bootstrap mechanism uniformly would invent schema semantics that these stores do not share and would blur their different authority and recovery models.

## Decision
Analytics owns a dedicated Kubernetes ClickHouse bootstrap Job and DDL credential. The Job initializes an empty Analytics database with its tables, materialized views, retention policies, and an exact schema-version marker. Exact-current means exactly one `schema_migrations` row and exactly one `schema_bootstrap_meta` row matching the expected version/checksum/catalog and is a no-op. Any partial, older, newer, extra-metadata, or otherwise unexpected non-empty schema fails unchanged. Runtime Analytics credentials cannot execute DDL. As with Postgres, the current pre-deployment path has no upgrade migration: an explicit operator reset recreates disposable storage before bootstrap.

Game Integrity has no schema bootstrap Job. KurrentDB streams are created lazily by the Game Integrity adapter on the first append and every append uses the documented expected revision. Deployment provisions server-level identity, TLS, authorization, storage, and retention policy; Game Integrity startup/readiness validates those capabilities, the configured stream policy, and access to the envelope-encryption provider. It does not create administrative users or weaken server policy.

Redis has no schema bootstrap Job and remains non-authoritative. Each owning service uses a versioned, context-specific key prefix, validates Redis capabilities, and loads its Lua scripts safely during startup. Clustered deployments load scripts on every target node and recover from `NOSCRIPT` by reloading before retrying the idempotent operation. Unknown keyspace versions fail readiness rather than being silently interpreted. Redis loss is recovered through the documented Postgres, Kafka, or safe-projection rebuild path.

## Consequences
Initialization follows each store's real consistency model. Analytics schema ownership and least privilege remain explicit; KurrentDB keeps append-only, expected-revision stream semantics; Redis stays disposable and rebuildable. ClickHouse bootstrap failures block Analytics without affecting gameplay. KurrentDB capability or key-provider failure blocks Game Integrity writes. Redis capability/script failure blocks only the flows that require that Redis adapter and never promotes local memory or another store to authoritative state. Store-specific readiness and integration tests are required in both local `kind` and production-like lanes.
