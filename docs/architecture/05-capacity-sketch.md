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
- State-changing command volume also drives authoritative Identity validation reads. Identity Postgres indexes, connection pools, and primary read capacity must be sized in the same order as admitted commands; cached-positive authorization is not a scaling shortcut.
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
- Istio Ambient scales the secure overlay per node through `ztunnel`, avoiding a sidecar container for every application/worker pod. Node tunnel capacity and connection churn are explicit sizing inputs.
- Identity validation scales through indexed token-hash lookups, bounded connection pools, database capacity, and stateless Identity replicas; Redis reduces secondary lookup work and rejects known-invalid sessions early but cannot replace strong mutation validation.
- Each Postgres context capacity plan includes one primary and two synchronous standbys, cross-failure-domain replication traffic, primary-plus-one commit latency, failover headroom, and connection storms after promotion. Read replicas do not scale authoritative mutation decisions.
- Recovery capacity includes continuous encrypted Postgres WAL archive, daily base backups, KurrentDB/key-history recovery material, daily ClickHouse backups, isolated restore infrastructure, and enough network/IO headroom to meet 30m/60m/4h RTOs without consuming live-cluster capacity.
- Identity replicas cache OIDC discovery/JWKS material with bounded refresh and rotation behavior. Authentication-provider latency affects login/provisioning, while ordinary command validation uses the internal authoritative session path after login.
- Room Gameplay uses native Postgres table partitioning and indexes by `roomId` inside its context-owned database; service replicas remain stateless executors.
- Game Integrity scales by append volume and replay workload.
- Game Integrity capacity includes per-event envelope encryption and KMS/HSM data-key unwrap/cache budgets; key-provider latency cannot be allowed to serialize unrelated game streams.
- Tournament Orchestration scales by shardable provisioning workers and async consumers.
- Ranking scales by consumer lag and leaderboard cache locality.
- Spectator View scales by Redis projection capacity and SSE fan-out.
- Analytics scales by ClickHouse ingestion and query pattern, not by transactional write pressure.

## Surge Handling

- Tournament Orchestration records the round kickoff once, then fans out provisioning batches by deterministic slot ranges.
- Sharded provisioning workers create rooms through Room Gameplay using idempotency keys based on `(tournamentId, roundNumber, slotId)`.
- Provisioning batches are partitioned by tournament/round/slot range so 100,000+ first-round rooms can be created by many workers without a single room-creation coordinator.
- Workers use rate-limited enqueue and bounded concurrency per deterministic slot range; if a worker or broker partition slows down, only that range backs up.
- Failed provisioning work is retried with the same slot keys, then dead-lettered or quarantined as a batch so completed slots are not recreated.
- Consumer-owned DLQs isolate poison events by consumer group. Aggregate-scoped quarantine prevents one bad ordered key from stopping healthy room, tournament, rating, or projection keys in the same consumer fleet.
- Consumer dedupe storage is capacity-bounded by source retention plus the maximum DLQ/replay window; indefinite replay uses aggregate checkpoints/domain uniqueness rather than per-event retention.
- Kafka buffers cross-context completion and projection bursts so Room Gameplay does not wait for Ranking, Analytics, or bracket projections.
- Initial production topic sizing plans 256 immutable partitions for high-volume spectator-safe/gameplay-metric streams and 32 for lower-volume business streams. Broker and consumer load tests may revise those counts before topic creation, never afterward; local `kind` deliberately uses smaller pre-created counts.
- Production broker capacity includes three-way replication, minimum ISR 2, rack/zone-aware leader distribution, replica recovery/reassignment bandwidth, and Kafka Connect internal topics. Throughput budgets use `acks=all`, not single-replica benchmark numbers.
- Broker storage sizing applies measured compressed bytes/second, immutable partition count, RF3, and retention class: 6h spectator-safe, 24h metrics/Identity controls, 7d business, and 30d DLQ. DLQ sizing uses failure budgets rather than full source throughput.
- Debezium reads committed outbox inserts from PostgreSQL WAL, avoiding polling scans on hot authoritative tables. Room Gameplay's context database has a Kafka Connect/Event Router connector for cross-context events and a separate Debezium Server/Redis sink for player feeds.
- High-volume outboxes use short, server-time partitions. Sealed partitions are reclaimed in bulk only after the corresponding durable connector offset passes the recorded WAL high-water LSN and a safety window; cleanup never performs per-row publish updates or age-only deletion.
- Native Room table partitions, connection pools, replication-slot throughput, and connector tasks are capacity-planned together without introducing another authoritative database.
- Pessimistic row locks serialize only commands targeting the same Room. With the assumed per-room command rate, contention is aggregate-local; hot-Room lock duration is dominated by Game Integrity latency and bounded by strict deadlines.
- Timer workers scale by stable Redis bucket and timer family. Atomic Lua claims take bounded batches from sorted sets, so one slow bucket or timer family does not serialize the global deadline workload.
- Round-end `MatchCompleted` bursts are partitioned by tournament/round/slot and consumed by Tournament Orchestration consumer groups; Analytics and Ranking use independent consumer groups so reporting lag cannot push back into gameplay writers.
- Analytics ingests completion and metric bursts into ClickHouse through async projection workers. It may lag seconds to minutes under the 100,000-result burst, but bracket advancement and gameplay commits do not wait for analytics ingestion.
- Bracket and standings projections carry round/version markers; clients may see a seconds-fresh projection while Tournament Orchestration keeps the authoritative advancement state in Postgres.
- Spectator fanout is isolated through Spectator View and the BFF; backpressure on spectator delivery must not slow Room Gameplay commits.
- Public spectator updates may coalesce non-critical changes under load, while player feeds remain ordered and low latency.

## Storage Growth Sketch

These figures are deliberately order-of-magnitude sizing inputs. They justify partitioning, retention, and projection rebuild choices without committing to a cloud SKU.

| Component | Estimate at first-round scale | Architectural implication |
| --- | --- | --- |
| Game Integrity / KurrentDB | `~1 KB/event x 50 events/game x up to 3 games/match x 100,000 matches ~= 15 GB` raw game-log data for the first round. Metadata, indexes, stream headers, and replication can make this `~30-50 GB` effective storage before retention/compaction policy. | KurrentDB is partitioned by room/game stream and isolated from projection load; replay/audit reads must not share the gameplay write path. |
| Room Gameplay / Postgres snapshots | `~12-24 KB/active room` for roster, hands summary, sequence, timer deadlines, dedupe markers, and small outbox pointers gives `~1.2-2.4 GB` for 100,000 active rooms. A delayed outbox can add tens of GB during a prolonged relay outage, so outbox lag is an operational limit. | Room tables may be natively partitioned and indexed by `roomId` inside the context-owned database; Redis cannot replace Postgres because timer deadlines and sequence state must survive restart/loss. |
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
- Redis timer-index loss pauses dispatch until sorted sets are rebuilt from open Postgres deadline rows; already claimed leases can repeat but cannot duplicate domain effects after Room revalidation.
- KurrentDB failure is not acceptable for Game Integrity writes.
- KMS/HSM or encryption failure is also a Game Integrity write outage; plaintext fallback is forbidden. Replay/export can be independently unavailable when historical keys cannot be authorized or loaded.
- Postgres failure in authoritative contexts is a write outage.
- Loss of one Postgres replica remains serviceable while primary-plus-one quorum exists. Loss of quorum stops writes; fenced promotion restores the stable writer endpoint before pools/CDC resume, and unknown outcomes reconcile through idempotency rather than blind retry.
- Cluster-wide loss or corruption invokes store-specific restore: Postgres PITR, KurrentDB plus key history, or ClickHouse backup/backfill. Redis loss invokes rebuild, not backup restore.
- Identity Postgres failure is also an authorization outage for new state-changing commands; BFF fails closed rather than using a stale positive cache.
- OIDC provider or JWKS validation failure blocks new authentication but does not invalidate already established internal sessions solely because discovery is temporarily unavailable; session validation still follows authoritative Identity policy until token/session expiry or revocation.
- Identity Postgres failure is an Identity-wide authoritative validation outage; mutations fail closed rather than using cached-positive authorization.
- Room Gameplay Postgres failure is a Room-wide write/read outage; services fail closed and never recreate authoritative rooms in another context or store.
- Excessive Game Integrity latency or competing actions can time out one Room's lock queue without blocking unrelated Room rows; timeout outcomes are explicit and retryable only after the client reconciles current state.
- Debezium/Kafka Connect failure delays integration publication but does not roll back committed aggregate state; replication-slot lag and retained WAL must stay within explicit operational limits.
- Debezium Server/Redis sink failure delays player-feed delivery while room state remains committed; clients reconcile from the authoritative snapshot, and WAL/realtime-outbox lag must remain bounded.
- A stalled or unverifiable connector also halts reclamation for its outbox. Storage-growth alarms and admission backpressure must engage before a context database exhausts space; operators do not bypass the offset gate.
- Kafka poison events consume only the configured retry budget; terminal failures move to the owning consumer's DLQ. Ordered aggregate quarantine prevents sequence skipping without creating a partition-wide indefinite stall.
- Topic-capacity expansion provisions a versioned replacement and buffers its records until the producer's durable old-epoch watermark is published and the old consumer group drains; an in-place partition increase is forbidden.
- A broker/zone loss may reduce capacity but acknowledged records remain on in-sync replicas. If ISR falls below two, publication stops and Postgres outboxes/consumer retries absorb pressure rather than accepting weakly replicated writes.
- SSE disconnects are recoverable if sequence markers and durable state are intact.
- Ambient control-plane or ztunnel disruption can block new east-west connections under strict mTLS; existing and new connection behavior must be load/failure tested without plaintext fallback.
- Local single-broker Kafka loss is a complete local broker outage and is not evidence of production availability; multi-broker failure behavior is tested in a production-like lane.
- A consumer lagging toward expiry triggers recovery escalation before data ages out. Crossing the window requires snapshot/backfill and checkpoint reconciliation; it is not treated as ordinary lag recovery.

## Local and Test Topology

- Local runs should collapse services where helpful, but preserve the logical boundaries.
- Local Postgres remains one instance per context and does not simulate synchronous quorum or automatic failover; those tests run in a production-like multi-node lane.
- Local stores remain disposable and do not prove backup RPO/RTO; fixture-based tooling checks may run locally, while timed restore drills require isolated production-like infrastructure.
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
- The same write rate is why Kafka-bound outboxes use WAL-based Debezium capture from Room Gameplay's context database rather than application polling workers against shared outbox indexes.
- The same rate requires bulk partition reclamation gated by durable CDC offsets; retaining relay rows indefinitely or updating them individually is outside the capacity model.
- Native table-partition skew, hot rooms, connection-pool pressure, WAL generation, and CDC lag are the Room scaling signals.
- Identity validation QPS/latency, index performance, pool saturation, primary capacity, and connector lag are the corresponding signals for Identity capacity.
- `~200,000-500,000 projection events/second` is why cross-context Kafka topics remain split by purpose (`room.spectator-safe.events`, `room.gameplay.metrics`) while latency-sensitive player and spectator SSE delivery uses independently scalable Redis Streams rather than the business-event bus.
- That projection rate is also why those two Kafka topics start with a 256-partition planning class, while lower-volume business topics start at 32; measured per-partition throughput and broker limits decide the final pre-creation counts.
- `~2,000 MatchCompleted events/second` at round end is why Tournament Orchestration consumes results asynchronously, dedupes by `(roomId, completionVersion)`, and advances rounds from Postgres state rather than from live Room Gameplay calls.
- `~10,000,000 possible spectator streams` is why Spectator View owns a separate Redis projection and BFF delivery can shed or coalesce spectator/reporting traffic before player command writes.
- `~30-50 GB` effective KurrentDB growth per large first round is why immutable Game Integrity logs have explicit retention/audit access rules and are not used as the public read model.
- Ciphertext, nonce, key-version metadata, and cryptographic commitments add storage/CPU overhead that must be included in the KurrentDB and Game Integrity sizing/load tests.
