# Immutable Partition Counts for Ordered Kafka Topics

## Status
Accepted

## Context
Kafka maps a keyed record to a partition using the topic's current partition count. Increasing that count can move an existing aggregate key to a different partition while older records remain in the original partition, invalidating the per-aggregate ordering and checkpoint assumptions used by durable consumers.

## Decision
Treat the partition count of every ordered keyed topic as immutable after topic creation. Partition keys remain those declared in AsyncAPI. Initial production-scale planning targets are 256 partitions for the high-volume `room.spectator-safe.events` and `room.gameplay.metrics` topics and 32 partitions for lower-volume business topics. These are pre-deployment planning values, not hard-coded application constants; load tests may change them before a topic is first created. Local `kind` uses reduced counts appropriate to its brokers while preserving keys, immutability enforcement, and the cutover protocol.

Capacity expansion after creation uses a versioned replacement topic, not an in-place partition increase. Producers switch destination through a durable topology epoch and record a source-specific cutover watermark after the old destination is sealed. New-topic records may accumulate but are not applied until the producer's old pipeline has passed that watermark and the old consumer group has drained to zero lag. Consumers then release the replacement-topic buffer in aggregate sequence/version order using ADR-0029 checkpoints. A gap or conflicting overlap quarantines only the affected aggregate.

The replacement topic uses a new topology name such as `<logical-topic>.v2`; payload schema versioning remains independent. Producer adapters, connector routing, ACLs, DLQs, dashboards, and consumer groups are provisioned for the new physical topic before the epoch switch. The old topic remains read-only through the rollback/verification window and, under ADR-0032, for at least seven days after verified cutover with no active replay/quarantine/rollback reference. In-place partition increases are prohibited by infrastructure policy for ordered topics.

## Consequences
Aggregate order does not depend on racing records across old and newly created partitions. Pre-sizing creates some idle partitions for low initial load and makes load testing, broker storage, file descriptors, leaders, and consumer concurrency part of capacity planning. Later expansion is operationally heavier because it requires a replacement topic, buffered cutover, watermark proof, and drain verification. Schema evolution does not automatically require a topology version, and topology changes do not justify changing event identity or business semantics.
