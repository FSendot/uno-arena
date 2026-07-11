# LSN-Gated Outbox Partition Reclamation

## Status
Accepted

## Context
Transactional outboxes can receive hundreds of thousands of rows per second during peak gameplay. Keeping every relay row indefinitely would turn non-authoritative delivery staging into unbounded authoritative-database growth, while deleting rows by age alone could remove evidence before a stalled CDC pipeline has delivered it.

## Decision
Partition every Postgres transactional outbox by a server-assigned insertion time. A database-owned rotation procedure seals a closed partition so no later transaction can insert into it, then records a PostgreSQL WAL high-water LSN after the seal. Application request handlers only append to the current partition and never delete or mark rows as published.

An operational reclamation controller may drop a sealed partition only after both conditions hold: the pipeline's durably committed source offset is beyond the partition's recorded WAL high-water LSN, and the configured minimum safety-retention window has elapsed. Kafka-bound outboxes use their owning context's Kafka Connect/Debezium source offset. Room Gameplay's realtime outbox is evaluated independently against the durable Debezium Server PostgreSQL-to-Redis source offset. Connector offset stores are durable production state.

If the partition cannot be proven sealed, an offset is absent or incomparable, a connector is unhealthy, or the safety window has not elapsed, reclamation stops and alerts. Operators may not override this with an age-only deletion path. WAL/replication-slot retention remains the recovery mechanism until a connector commits delivery; Kafka or the Redis player-feed stream becomes the downstream recovery surface after that commit.

## Consequences
Outbox storage remains bounded without application polling, per-row publish flags, or synchronous broker writes. Rotation, high-water metadata, connector offset durability, safety-window policy, and cleanup lag become production concerns. Room's Kafka and Redis outboxes can advance and be reclaimed independently. Cleanup failure consumes database capacity and must trigger backpressure or incident response before storage exhaustion, but it cannot sacrifice unconfirmed events. Dropping an outbox partition never deletes authoritative aggregate state or immutable Game Integrity history.
