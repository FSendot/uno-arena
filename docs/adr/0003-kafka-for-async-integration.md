# Kafka for Async Integration

## Status
Accepted

## Context
The system needs asynchronous integration across bounded contexts, but not every stream has the same volume or purpose.

## Decision
Kafka is used for asynchronous integration between bounded contexts. Topics are bounded-context-specific. High-volume projection or sanitized-metrics streams are separated from low-volume business streams. Events carry per-event schema versions. Client-facing realtime player and spectator delivery is a separate Redis Streams concern and is not carried by Kafka.

Ordered keyed topics follow ADR-0030: partition counts are immutable after creation, initial counts are load-tested before deployment, and later expansion uses a versioned replacement topic with a durable producer watermark and consumer drain rather than an in-place increase.

Production broker and producer durability follows ADR-0031: replication factor 3, minimum two in-sync replicas, all-replica acknowledgments, idempotent production, rack-aware placement, and no unclean leader election. Single-replica local topologies make no HA claim.

Topic retention and post-expiry recovery follow ADR-0032. Kafka provides bounded operational replay by stream class; it is never the permanent domain or audit store.

## Consequences
Integration remains decoupled while topic ownership stays clear. High-volume read-side traffic can scale independently from business events. Schema evolution stays explicit per event type and version.
