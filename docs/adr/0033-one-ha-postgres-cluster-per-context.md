# One HA Postgres Cluster per Context

## Status
Accepted

## Context
Identity, Room Gameplay, Tournament Orchestration, and Ranking require independent persistence boundaries and production availability without reintroducing application-managed database sharding. A single unreplicated Postgres pod would make each context a node-level single point of failure.

## Decision
Each Postgres-backed bounded context owns exactly one HA database cluster: one writable primary and two synchronous standbys distributed across distinct Kubernetes nodes and, where available, failure zones. Replicas are copies of the same context database boundary, not additional shards or databases exposed to application routing.

Production commits use synchronous commit and require WAL flush on the primary plus at least one synchronous standby. If that acknowledgment quorum is unavailable, writes fail closed. A fenced failover mechanism promotes exactly one standby and exposes it through a stable context-specific read-write endpoint. Application and worker pools connect only to that endpoint for authoritative operations, discard broken connections after promotion, and reconcile unknown outcomes through command/event idempotency rather than blindly replaying writes.

Authoritative Identity session validation, Room commands and deadlines, Tournament registration/advancement, Ranking updates, bootstrap/schema checks, and outbox writes always use the read-write endpoint. Standby endpoints may serve only explicitly stale-tolerant reporting or rebuild reads whose contracts permit lag; they never authorize mutations or participate in a read-then-write decision. CDC connects through a promotion-aware writer path and must resume from durable source offsets after failover without skipping committed outbox records; failover tests must prove duplicate-safe recovery.

Local Docker Compose and `kind` use one Postgres instance per context for bounded resources. That preserves database ownership, schemas, transactions, and endpoints but makes no HA, synchronous-replication, or automatic-failover claim.

## Consequences
A single Postgres pod/node failure can be recovered without losing an acknowledged commit, while each bounded context retains one physical persistence boundary. Synchronous replication adds write latency and may stop writes during correlated failures or maintenance rather than accept an RPO violation. Primary/standby health, replication lag, quorum, failover fencing, endpoint convergence, connection-pool recovery, CDC resume position, and unknown-outcome reconciliation become production concerns. Provider choice remains open as long as it satisfies this topology and behavior.

Replication is not backup. Cluster-wide corruption, deletion, or loss recovers through the encrypted WAL archive, daily base backup, PITR objectives, and quarterly isolated restore drills in ADR-0034.
