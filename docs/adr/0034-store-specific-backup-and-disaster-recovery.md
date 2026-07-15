# Store-Specific Backup and Disaster Recovery

## Status
Accepted

## Context
High availability protects against individual process, pod, node, and limited replica failures, but it does not protect against operator error, corruption, credential compromise, cluster loss, or destruction replicated to every copy. The stores have different authority and rebuild characteristics, so one generic backup objective would either under-protect authoritative state or over-protect disposable projections.

## Decision
Production uses these recovery objectives:

- Each context-owned Postgres cluster continuously archives encrypted WAL and creates an encrypted daily base backup in an isolated failure/security domain. Point-in-time recovery targets RPO no greater than 5 minutes and RTO no greater than 30 minutes.
- KurrentDB and the Game Integrity envelope-key metadata/provider configuration use encrypted continuous backup or replication sufficient for RPO no greater than 5 minutes and RTO no greater than 60 minutes. Historical wrapping-key versions must remain recoverable and access-controlled; restoring ciphertext without its authorized key history is a failed restore.
- ClickHouse creates an encrypted daily backup with RPO no greater than 24 hours and RTO no greater than 4 hours. Missing derived intervals may be rebuilt from retained safe events and context-owned backfill sources.
- Redis has no authoritative RPO or backup dependency. Persistence/replication may accelerate restart, but correctness recovery rebuilds timers, projections, caches, rate-limit state, and streams from their documented Postgres, Kafka, or sanitized-snapshot sources.

Backups are encrypted in transit and at rest, use credentials separate from runtime/DDL roles, and reside outside the primary cluster failure domain. Production performs an automated quarterly restore drill into isolated infrastructure. A drill validates decryptability, schema/version compatibility, point-in-time selection, context invariants, KurrentDB commitments/replay, key-history access, CDC/checkpoint reconciliation, measured RPO/RTO, and secure teardown. Backup-upload success alone is not recovery evidence.

Local `kind` uses disposable storage and makes no backup, HA, RPO, or RTO claim. It may exercise restore tooling with fixtures, but production objectives are verified in an isolated production-like lane.

## Consequences
Authoritative contexts have explicit recovery bounds, while Redis remains rebuildable rather than becoming accidental truth. Backup storage, WAL/archive lag, cross-failure-domain bandwidth, encryption keys, restore automation, quarterly drills, and evidence retention become production responsibilities. A Game Integrity restore is unavailable if historical wrapping keys are unavailable even when KurrentDB bytes survived. Provider choice remains open if it meets the objectives and produces verifiable restore evidence.
