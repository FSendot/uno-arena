# Transactional Consumer Idempotency and Checkpoints

## Status
Accepted

## Context
Kafka and the CDC pipelines deliver at least once. A consumer can crash after updating its owned state but before committing the Kafka offset, and DLQ replay can reintroduce records long after ordinary processing. Unbounded event-ID tables would eventually dominate consumer stores, while committing offsets before state would silently lose effects.

## Decision
Every consumer uses the idempotency key declared by the owning AsyncAPI channel. For a durable consumer, the domain/projection mutation, the idempotency record or domain uniqueness constraint, and any aggregate sequence/version checkpoint commit atomically in the consumer's owned store. The Kafka source offset is committed only after that owned-store transaction succeeds. A crash between those commits produces a duplicate delivery, not a lost effect.

Ordered aggregate streams additionally persist the last applied aggregate sequence or contract-defined version. An exact duplicate at or below the checkpoint is ignored only when its idempotency key and immutable payload identity agree. A gap, a conflicting payload for an existing key/version, or an impossible regression quarantines that aggregate under ADR-0017; unrelated aggregate keys continue. Releasing quarantine requires replay, rebuild, or reconciliation to restore a contiguous checkpoint.

Contract-key dedupe records are retained for at least the Kafka source-retention period plus the maximum configured DLQ/replay window. A cleanup job may remove them only after that bound and only when no active quarantine or replay references them. Flows requiring indefinite replay use compact aggregate checkpoints, immutable domain uniqueness constraints, or durable upstream business keys instead of retaining every event ID forever. Redis projections atomically update projection state, idempotency/sequence state, and outgoing stream entries in one transaction or Lua script; ClickHouse ingestion uses its contract key and versioned replacement/deduplication strategy before a source offset is committed.

## Consequences
Consumer restarts and broker redelivery are safe without distributed transactions between Kafka and context stores. Idempotency storage stays bounded for finite-replay topics, while permanent replay remains provable from compact checkpoints or domain constraints. Every contract must declare a usable partition key and idempotency key, and ordered flows must expose a monotonic aggregate sequence/version. Transaction retries, uniqueness conflicts, checkpoint gaps, dedupe retention, cleanup lag, quarantine, and offset-commit failures become explicit operational signals.
