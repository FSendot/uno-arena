# Redis Streams for Realtime SSE Delivery

## Status
Accepted

## Context
The assignment requires room updates to reach an independently scaled, stateless SSE broadcaster tier through Redis Streams. Realtime client delivery and cross-context integration have different scaling, recovery, and latency characteristics and therefore use separate transports.

## Decision
Use Redis Streams for ordered realtime delivery to the BFF. Room Gameplay commits player-safe entries to a `realtime_outbox` in the same Postgres transaction as room state; a dedicated Debezium Server PostgreSQL source with the Redis sink captures those rows from WAL and routes them to the room's player-feed stream. Run this CDC route for Room Gameplay's context-owned database and keep it separate from the Kafka-bound Outbox Event Router connector.

The realtime outbox follows ADR-0026. Its sealed time partitions are reclaimed only after the Debezium Server source offset has durably advanced beyond the partition's WAL high-water LSN and the safety-retention window has elapsed; Kafka connector progress cannot authorize realtime-outbox cleanup.

Spectator View publishes only its materialized spectator-safe projection entries. Because the projection and outgoing feed share Redis, it updates projection state, dedupe/sequence state, and the spectator stream atomically with a Redis transaction or Lua script. Gateway instances consume the player and spectator streams and translate them to SSE; trimmed history requires snapshot resynchronization. Kafka remains the integration bus for business events and cross-context projection inputs such as `room.spectator-safe.events` and `room.gameplay.metrics`.

## Consequences
The SSE tier remains stateless and independently scalable, player and spectator privacy boundaries stay owned by their source contexts, and realtime delivery load does not share Kafka consumer pressure with tournament, ranking, or analytics workflows. Room feed publication survives an application crash between Postgres commit and Redis delivery without polling or a dual write. The additional Debezium Server instances, logical-replication slots, Redis sink offsets, and capture lag are production infrastructure. Delivery can repeat after recovery, so feed consumers dedupe by event identity and sequence. Redis stream history is resumable delivery state, not authoritative room or spectator domain state; Postgres and rebuildable projections remain the recovery sources.
