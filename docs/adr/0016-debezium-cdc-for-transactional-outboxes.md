# Debezium CDC for Transactional Outboxes

## Status
Accepted

## Context
Identity, Room Gameplay, Tournament Orchestration, and Ranking commit authoritative Postgres state together with integration events. At the target scale, application polling of outbox tables would add recurring scans, row claims, and write contention to databases already serving aggregate transactions.

## Decision
Use a Debezium PostgreSQL connector with the Outbox Event Router single-message transformation for Kafka-bound transactional outboxes. Each context writes a standardized outbox record in the same transaction as its authoritative state. Debezium captures inserts from PostgreSQL WAL and routes the event ID, aggregate/partition key, event type, schema version, tracing metadata, and JSON payload to the context-owned AsyncAPI topic. Canonical event time remains `occurredAt` inside that JSON envelope; it is not mapped from a nonexistent relational `occurred_at` column. Run one connector per context-owned Postgres database. Delivery is at least once; consumers remain idempotent by the documented event or business keys.

Redis Streams realtime delivery is outside the Kafka Connect/Outbox Event Router path. Under ADR-0015, Room Gameplay uses a separate Debezium Server PostgreSQL-to-Redis pipeline for its transactional realtime outbox, while Spectator View atomically updates its Redis projection and spectator stream inside Redis.

Kafka-bound outbox tables follow ADR-0026: time partitions are sealed with a recorded WAL high-water LSN and reclaimed only after the owning connector's durable source offset has advanced beyond that LSN plus the configured safety-retention window.

## Consequences
Authoritative transactions do not wait for Kafka and do not compete with polling relays. Kafka Connect and Debezium Server availability, PostgreSQL logical-replication slots, WAL retention, connector offsets, and capture lag become production infrastructure that must be monitored and capacity-planned. Kafka-bound context outbox tables share the logical field contract expected by the router, while the Room realtime outbox additionally carries the target player-feed stream name. Connector retries can duplicate delivery, so consumer deduplication remains mandatory.
