# 05 Capacity Sketch

This sketch is intentionally high level. It captures the dominant loads and the scaling knobs that matter most in the accepted architecture.

## Workload Drivers

- Room Gameplay: command bursts, turn serialization, SSE fan-out, timer recovery
- Game Integrity: append latency and replay throughput
- Tournament Orchestration: large fan-out during provisioning and round closure
- Ranking: async result ingestion and leaderboard reads
- Spectator View: high read fan-out, privacy-safe projection rebuilds
- Analytics: batch and near-real-time event ingestion into ClickHouse

## Capacity Targets

These are initial design targets tied to the assignment scale.

- Tournament capacity: up to 1,000,000 registered players.
- First-round surge: 100,000+ simultaneous matches/rooms transitioning to active within seconds.
- Tournament room size assumption: average 10 active players per first-round room.
- Spectator multiplier: 10:1 stress assumption for public rooms, so spectator connections can be an order of magnitude larger than player connections.
- Gameplay command rate: low single-digit accepted commands per active room per second on average, with localized bursts.
- Round completion burst: up to 100,000 `MatchCompleted` events arriving near the end of a large round.
- SSE should support long-lived connections with predictable reconnect behavior; gateway load scales by connection count, not just room count.
- Timer expiry should recover correctly after worker restarts or Redis loss.
- Replay and audit should remain available under gameplay load.
- Tournament provisioning should scale horizontally by round size.
- Spectator reads should scale independently from gameplay writes.

## Scaling Levers

- BFF scales horizontally because it is stateless apart from connection handling.
- Room Gameplay scales by room shard or room key partition.
- Game Integrity scales by append volume and replay workload.
- Tournament Orchestration scales by shardable provisioning workers and async consumers.
- Ranking scales by consumer lag and leaderboard cache locality.
- Spectator View scales by Redis projection capacity and SSE fan-out.
- Analytics scales by ClickHouse ingestion and query pattern, not by transactional write pressure.

## Surge Handling

- Tournament Orchestration records the round kickoff once, then fans out provisioning batches by deterministic slot ranges.
- Sharded provisioning workers create rooms through Room Gameplay using idempotency keys based on `(tournamentId, roundNumber, slotId)`.
- Provisioning batches are partitioned by tournament/round/slot range so 100,000+ first-round rooms can be created by many workers without a single room-creation coordinator.
- Workers use rate-limited enqueue and bounded concurrency per Room Gameplay shard; if a shard or broker partition slows down, only that slot range backs up.
- Failed provisioning work is retried with the same slot keys, then dead-lettered or quarantined as a batch so completed slots are not recreated.
- Kafka buffers cross-context completion and projection bursts so Room Gameplay does not wait for Ranking, Analytics, or bracket projections.
- Round-end `MatchCompleted` bursts are partitioned by tournament/round/slot and consumed by Tournament Orchestration consumer groups; Analytics and Ranking use independent consumer groups so reporting lag cannot push back into gameplay writers.
- Analytics ingests completion and metric bursts into ClickHouse through async projection workers. It may lag seconds to minutes under the 100,000-result burst, but bracket advancement and gameplay commits do not wait for analytics ingestion.
- Bracket and standings projections carry round/version markers; clients may see a seconds-fresh projection while Tournament Orchestration keeps the authoritative advancement state in Postgres.
- Spectator fanout is isolated through Spectator View and the BFF; backpressure on spectator delivery must not slow Room Gameplay commits.
- Public spectator updates may coalesce non-critical changes under load, while player feeds remain ordered and low latency.

## Hot Path Priorities

1. Accept and validate gameplay commands.
2. Confirm durable append before broadcast.
3. Deliver SSE updates with minimal lag.
4. Recover timers and reconnect windows correctly.
5. Keep tournament advancement and ranking asynchronous.

## Failure Sensitivity

- Redis failure is acceptable for caches and scheduling indexes if durable state exists elsewhere.
- EventStoreDB failure is not acceptable for Game Integrity writes.
- Postgres failure in authoritative contexts is a write outage.
- SSE disconnects are recoverable if sequence markers and durable state are intact.

## Local and Test Topology

- Local runs should collapse services where helpful, but preserve the logical boundaries.
- Tests should cover:
  - command idempotency
  - sequence-number rejection
  - durable timer recovery
  - tournament advancement from async completion events
  - privacy-safe spectator projection rebuilds

## NFR Budget Sketch

- Availability: gameplay and tournament paths should fail closed on authoritative write loss rather than inventing state.
- Consistency: command acceptance must be deterministic and replayable.
- Privacy: spectator projections must not leak private hand data.
- Observability: every slow path must be attributable by correlation and aggregate identifiers.
