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
- Peak authenticated player connections: `100,000 rooms x 10 players ~= 1,000,000` concurrent player SSE streams during the first round. The 60-second reconnect window can temporarily overlap old and new connections, so the gateway budget reserves another 10-20% for short-lived reconnect churn rather than treating 1,000,000 as only a registration count.
- Spectator multiplier: 10:1 stress assumption for public rooms, so spectator connections can be an order of magnitude larger than player connections.
- Peak spectator connections: up to `~10,000,000` spectator SSE streams under the 10:1 stress assumption. Spectator delivery can coalesce non-critical updates and lag behind player delivery.
- Gameplay command rate: design around `~2 accepted commands/room/second` sustained and `~5 commands/room/second` localized burst before throttling. At 100,000 active rooms this is `~200,000 accepted commands/second` sustained at the BFF and Room Gameplay boundary, with burst admission pressure up to `~500,000 commands/second`.
- Player/spectator projection events: a normal accepted command can emit one player-feed event and one spectator-safe event, so the gameplay projection topics should be sized in the same order as accepted commands: `~200,000 events/second` sustained and up to `~500,000 events/second` burst before coalescing and backpressure.
- Round completion burst: up to 100,000 `MatchCompleted` events arriving near the end of a large round. If they compress into a one-minute window, Tournament Orchestration sees `~1,700 room.match.completed events/second`; the design target rounds this to `~2,000 events/second` with dedupe and consumer lag budgets.
- Game completion burst: best-of-three matches can produce up to `~300,000 GameCompleted` events across the first round. If a large tail compresses into a few minutes, Ranking and Analytics should tolerate `~1,000-5,000 room.game.completed events/second` without coupling back to Room Gameplay.
- Analytics ingestion: `room.gameplay.metrics` may track accepted commands at `~200,000 rows/second` sustained during the hottest period, with async buffering into ClickHouse and lag measured in seconds to minutes.
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

## Storage Growth Sketch

These figures are deliberately order-of-magnitude sizing inputs. They justify partitioning, retention, and projection rebuild choices without committing to a cloud SKU.

| Component | Estimate at first-round scale | Architectural implication |
| --- | --- | --- |
| Game Integrity / EventStoreDB | `~1 KB/event x 50 events/game x up to 3 games/match x 100,000 matches ~= 15 GB` raw game-log data for the first round. Metadata, indexes, stream headers, and replication can make this `~30-50 GB` effective storage before retention/compaction policy. | EventStoreDB is partitioned by room/game stream and isolated from projection load; replay/audit reads must not share the gameplay write path. |
| Room Gameplay / Postgres snapshots | `~12-24 KB/active room` for roster, hands summary, sequence, timer deadlines, dedupe markers, and small outbox pointers gives `~1.2-2.4 GB` for 100,000 active rooms. A delayed outbox can add tens of GB during a prolonged relay outage, so outbox lag is an operational limit. | Room state is sharded by `roomId`; Redis cannot replace Postgres because timer deadlines and sequence state must survive restart/loss. |
| Tournament Orchestration / Postgres | `~2-4 KB/slot/result` across 100,000 first-round slots gives `~200-400 MB` for the initial round, with subsequent rounds smaller by the advancement ratio. Full tournament state remains in the low-GB order for this scale. | Sharded provisioning workers and idempotency keys avoid one giant coordinator while keeping authoritative bracket state transactional. |
| Spectator View / Redis projection | `~5-20 KB/public room projection x 100,000 rooms ~= 0.5-2 GB` of hot projection data, excluding connection buffers. | Spectator projections can be rebuilt and scaled separately from gameplay writes; public stream coalescing protects Redis and BFF fanout. |
| Analytics / ClickHouse | At `~200,000 gameplay metric rows/second`, one peak hour is `~720 million rows`. With `~0.5-1 KB/row` before columnar compression, this is `~360-720 GB` raw and often `~40-150 GB` compressed depending on cardinality and retention. | Analytics must be async, partitioned by time/tournament, and allowed to lag; it cannot sit on the synchronous gameplay path. |

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

## Capacity-to-Architecture Trace

- `~200,000 accepted gameplay commands/second` is why Room Gameplay is partitioned by `roomId`, uses room-local serialization, and never waits for Ranking, Analytics, or spectator fanout.
- `~200,000-500,000 projection events/second` is why Kafka topics are split by purpose (`room.player-feed.events`, `room.spectator-safe.events`, `room.gameplay.metrics`) instead of using one catch-all stream.
- `~2,000 MatchCompleted events/second` at round end is why Tournament Orchestration consumes results asynchronously, dedupes by `(roomId, completionVersion)`, and advances rounds from Postgres state rather than from live Room Gameplay calls.
- `~10,000,000 possible spectator streams` is why Spectator View owns a separate Redis projection and BFF delivery can shed or coalesce spectator/reporting traffic before player command writes.
- `~30-50 GB` effective EventStoreDB growth per large first round is why immutable Game Integrity logs have explicit retention/audit access rules and are not used as the public read model.
