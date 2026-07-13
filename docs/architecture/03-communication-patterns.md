# 03 Communication Patterns

## External Pattern

Clients use the BFF only.

- Commands are sent via compact REST envelopes.
- Realtime updates are received via SSE from the same logical gateway.
- Clients never call Room Gameplay, Tournament Orchestration, or any other microservice directly.
- This implementation uses the repo-owned simple CLI as the sole client/test interface; a graphical interface is deferred and must preserve the BFF-only REST/SSE boundary.
- OpenAPI should define the BFF HTTP surface, including player/spectator snapshot routes (player hand/`discardTop` as camelCase `Card` DTOs), SessionBearer auth requirements, spectator invalid-token `401`, and SSE `409 snapshot_required`.
- AsyncAPI should define the Kafka topic contracts (including `tournament.completed` and `ranking.leaderboard_snapshot_published`). The Kafka envelope is authoritative for production; the offline HTTP bridge is a documented destination-specific transform of the same event names and canonical domain fields, not an identical body. SSE framing belongs in OpenAPI, not AsyncAPI.
- Production requires Kafka adapters for those topics. Durable consuming services implement them when `KAFKA_BROKERS` is configured; the offline capability path retains HTTP bridges and does not claim Kafka is wired. The clean kind CDC and recovery lanes are live-proven; individual status/structure checks still make no delivery claim.

### Command Envelope

```json
{
  "commandId": "cmd_01HXABC123",
  "type": "PlayCard",
  "expectedSequenceNumber": 42,
  "payload": {
    "roomId": "room_123",
    "gameId": "game_456",
    "cardId": "red-7"
  }
}
```

- `commandId` is the idempotency key.
- `type` is the command name from the existing catalog.
- `expectedSequenceNumber` is required for mutations of an existing room aggregate.
- `CreateRoom` is the main exception because no room aggregate exists yet.
- The public tournament catalog contains only `CreateTournament`, `RegisterPlayer`, and `CloseRegistration`. Tournament-owned `SeedRound`, `ProvisionRoundMatches`, `CompleteRound`, and `CompleteTournament` policy operations never cross the BFF boundary.

## BFF Responsibilities

- Authenticate the request with Identity.
- Identity authenticates through its provider-neutral OIDC adapter and exposes only internal principals; the BFF never interprets provider claims directly.
- For every state-changing command, require authoritative Identity Postgres validation; cached-positive session state cannot authorize mutation.
- Validate envelope shape and route it to the correct bounded context.
- Preserve correlation headers across all internal hops.
- Fan out SSE updates for player streams and spectator streams.
- Close streams on `SessionInvalidated` or equivalent control events.
- Close spectator streams when Room Gameplay reaches `RoomCompleted` or `RoomCancelled`.
- Emit or forward structured operational/security audit records for rejected commands without treating them as domain events.

## Rate Limiting

Rate limiting is multi-layered and happens after authentication where player or room scope is needed.

| Layer | Deployable | Scope source | Store / mechanism | Failure behavior |
| --- | --- | --- | --- | --- |
| Edge abuse | Realtime Gateway / BFF | IP address, user agent, unauthenticated route | local counters plus Redis-backed buckets | reject before auth/service dispatch with a retryable throttling response |
| Authenticated user commands | BFF after Identity validation | `PlayerId`, `SessionId`, roles from Identity claims/introspection | Redis token bucket keyed by user/session/action family | reject before bounded-context command dispatch; structured operational/security audit record only; no domain event and no Game Integrity append |
| Room gameplay actions | Room Gameplay command middleware | authenticated `PlayerId`, `roomId`, command `type`, room membership | Redis acceleration plus room-local guard in the command transaction | reject before mutation, append, or broadcast; structured operational/security audit record only; suspicious bursts are audit/ops signals |
| Tournament actions | Tournament Orchestration command middleware | authenticated `PlayerId`, `tournamentId`, role/eligibility | Redis bucket keyed by tournament/action plus Postgres eligibility checks for authoritative gates | reject or queue according to tournament policy; provisioning workers use backpressure rather than client retries |
| Adaptive surge protection | BFF, Room Gameplay, Tournament workers | consumer lag, timer lag, queue depth, database and worker-partition health | dynamic limits driven by metrics, with conservative defaults | shed non-critical spectator/reporting traffic before player command writes |

Redis accelerates counters but does not become authority for sessions, room state, or tournament state. Rate-limit rejections are command rejections, not domain events, unless the business process explicitly removes or sanctions a player. Every rejected command still emits a structured operational/security audit record with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason, submitted/current sequence when applicable, and timestamp, and never appends a Game Integrity entry.

## Internal Command and Event Flow

- Room Gameplay accepts the command and performs domain validation.
- For an existing Room, the Room repository begins a `READ COMMITTED` transaction in the context-owned Postgres database and locks the Room aggregate row with `SELECT ... FOR UPDATE` before validating sequence, dedupe, membership, timers, or game rules.
- If the command is rejected, Room Gameplay emits a structured operational/security audit record and returns the rejection outcome. No domain event is published and Game Integrity is not called.
- If the command is valid, Room Gameplay requests an append from Game Integrity.
- Game Integrity confirms the durable append.
- Only then does Room Gameplay commit snapshot, deadlines, command outcome, integration/realtime outboxes, and integrity offset; CDC subsequently publishes the resulting facts outward.
- Lock wait, statement, Game Integrity call, and total transaction deadlines are bounded. Game Integrity failure rolls back; a local commit failure after append confirmation enters the integrity-log reconciliation path.
- Downstream projections consume the published facts asynchronously.
- When an Uno window opens, published player/spectator-safe facts include absolute UTC `expiresAt` and the opening room sequence; CLI countdown remains advisory and the server exclusively decides timeliness.

## Kafka Topology

Kafka topics are bounded-context-specific.

Ordered topic partition counts are immutable after creation. Production planning starts at 256 partitions for `room.spectator-safe.events` and `room.gameplay.metrics`, and 32 for lower-volume business topics; load tests may revise these values before first creation. Local `kind` uses reduced pre-creation counts while preserving the same partition keys and cutover behavior. Expansion creates a versioned physical topic. The producer seals the old topology epoch and records a durable source-specific watermark, new records buffer on the replacement topic, and consumers apply them only after the old publication pipeline passes the watermark and the old group reaches zero lag. Payload schema versions are independent from physical topic topology versions.

Production topics use replication factor 3 and `min.insync.replicas=2` across at least three rack/zone-aware brokers. All application, CDC, DLQ, replay, and administration producers use `acks=all` with idempotence and ordering-safe retries; unclean leader election is disabled. Connect config/offset/status topics, DLQs, and versioned replacements use the same durability floor. Local single-replica Kafka verifies behavior only and is rejected by production configuration validation.

Production retention defaults are 6 hours for `room.spectator-safe.events`, 24 hours for `room.gameplay.metrics` and `identity.session.invalidated`, 7 days for other business/low-volume projection topics (including rebuild-request control topics), and 30 days for consumer-owned DLQs. A superseded topology topic remains for at least 7 days after verified cutover and until replay/quarantine/rollback references clear. Kafka is not an audit store: expiry routes recovery to context-owned sanitized snapshot/backfill APIs triggered by bounded rebuild-request topics (`spectator.projection.rebuild_requested`, `analytics.projection.rebuild_requested`), and consumers never advance across an unexplained gap. Local `kind` may shorten retention for disk limits without claiming those recovery windows.

- Each context owns its own topics and consumers.
- Business streams are separated from projection streams.
- A single catch-all event bus is not the target design.
- Every event should carry an explicit `schemaVersion`.

Examples:

- `room.game.completed`
- `room.match.completed`
- `room.spectator-safe.events`
- `room.gameplay.metrics`
- `identity.session.invalidated`
- `tournament.match.assigned`
- `tournament.match.result_recorded`
- `tournament.players.advanced`
- `tournament.round.completed`
- `tournament.completed`
- `ranking.player_rating_updated`
- `ranking.leaderboard_snapshot_published`
- `spectator.room_projection.updated`
- `spectator.projection.rebuild_requested`
- `analytics.public_projection.updated`
- `analytics.projection.rebuild_requested`

## Transactional Outbox CDC

Postgres-backed contexts publish Kafka integration events through Debezium rather than application polling or synchronous broker writes.

- Identity, Room Gameplay, Tournament Orchestration, and Ranking insert an outbox row in the same transaction as authoritative state.
- Outbox records share the logical router fields: event ID, aggregate type, aggregate/partition key, event type, destination topic, JSON payload, schema version, correlation/causation IDs, and tracing context. Canonical event time is `occurredAt` inside the JSON envelope; the relational outbox does not invent a separate `occurred_at` router column.
- A Debezium PostgreSQL connector captures outbox inserts from WAL and applies the Outbox Event Router transformation. Each context database owns one connector.
- The router uses the documented aggregate key as the Kafka message key so related events preserve partition ordering.
- Delivery is at least once. Consumers persist event IDs or documented business keys before committing offsets; duplicate delivery is a normal recovery case.
- Outbox rows use server-assigned insertion time for physical partition maintenance, independently of canonical payload `occurredAt`. A database-owned rotation procedure seals a closed partition and records its WAL high-water LSN.
- Reclamation is operational maintenance, not event publication: a sealed partition is dropped only when its pipeline's durable source offset is beyond the recorded high-water LSN and the minimum safety window has elapsed. Missing proof stops cleanup and alerts.
- Connector health, replication-slot retention, WAL growth, connector offsets, capture lag, and oldest pending outbox age are production signals.
- Redis Streams delivery does not pass through this Kafka-bound CDC route. Room player feeds use a separate Debezium Server PostgreSQL-to-Redis CDC pipeline and their partitions are gated by that pipeline's own offset; Kafka progress cannot authorize their cleanup. Spectator View publishes its projection and spectator stream atomically in Redis.

## Consumer Retry, DLQ, and Quarantine

- Retry budgets and backoff are consumer-owned and configurable by failure class.
- A terminal processing failure is published to `<source-topic>.<consumer>.dlq` with the unchanged source envelope plus sanitized operational metadata. The consumer waits for the DLQ acknowledgment before committing the source offset.
- DLQ publication is itself at least once; duplicate DLQ records are deduped by source topic/partition/offset and consumer group.
- Ordered consumers persist aggregate quarantine state. Later records for the affected aggregate key are held or routed to quarantine until replay/rebuild restores the missing sequence; unrelated aggregate keys continue.
- Spectator/Analytics post-retention rebuild uses Kafka rebuild-request topics plus context-owned internal snapshot/backfill APIs (ADR-0039; implemented and live-proven in kind). Held post-gap records remain owned by the consumer quarantine/DLQ; after an atomic snapshot/generation rebuild they are replayed in sequence, and quarantine releases only after continuity is proven. Gaps are never silently skipped. Rebuilder Deployments are enabled in kind only and remain disabled by default in staging/production.
- On success, consumers atomically commit their owned-state mutation, the channel's documented idempotency key, and any aggregate sequence/version checkpoint before committing the Kafka offset.
- Exact duplicates are no-ops only when key/version and immutable payload identity agree. Sequence gaps, regressions, and conflicting duplicates quarantine the affected aggregate.
- Dedupe rows remain for at least source-topic retention plus the maximum DLQ/replay window. Indefinite replay relies on compact aggregate checkpoints or domain uniqueness constraints rather than an unbounded event-ID table.
- Schema/privacy violations can bypass transient retries and enter quarantine immediately. Dependency and timeout failures use bounded retry first.
- DLQ and quarantine records are operational artifacts, never domain events or Game Integrity entries.

## Integration Table

| From | To | Pattern | Rationale | Failure semantics |
| --- | --- | --- | --- | --- |
| Client | Realtime Gateway / BFF | REST commands + SSE streams | Single external boundary; compact commands and realtime updates | REST retries use `commandId`; SSE resumes from last event id/sequence |
| Realtime Gateway / BFF | Room Gameplay | `GET /v1/rooms` → `GET /internal/v1/rooms/public-list` | Canonical public room list / casual matchmaking read for CLI; BFF-only edge | Gateway↔Room credential; public summaries only; opaque Room HMAC cursor; default 50 / max 100; no player bearer; rooms are joinable only after their assigned pod is Ready |
| Realtime Gateway / BFF | Redis rate-limit buckets | Per-IP and authenticated per-user throttling | Protect public edge before services receive abusive traffic | Fail closed for abusive bursts; fail open only for low-risk spectator/reporting reads if policy allows |
| BFF | Identity | OIDC-backed authentication plus synchronous authoritative Postgres validation for mutations | Keep provider claims inside Identity and establish the authorization linearization point with a signed internal principal | Fail closed on provider, discovery/JWKS, Identity, or Postgres failure; cache may reject early but never authorize positively |
| Identity | BFF | `identity.session.invalidated` control event | Close old SSE streams for single-active-session | Broadcast to gateway instances; stale streams are closed on receipt and old commands are rejected |
| Postgres-backed context | Debezium / Kafka | Transactional outbox captured from WAL and routed by the Outbox Event Router | Publish committed cross-context facts without polling or dual writes in request handlers | At-least-once delivery; connector resumes from WAL position; consumers dedupe by event/business key |
| Kafka consumer | Consumer-owned DLQ | `<source-topic>.<consumer>.dlq` after bounded retries | Keep failure ownership and replay policy inside the consuming context | Publish original envelope + failure metadata, await acknowledgment, then commit source offset; ordered aggregates remain quarantined until contiguous recovery |
| BFF | Room Gameplay router → dedicated Room pod | Synchronous `roomId` routing + pessimistic Room row lock | Preserve one stable bounded-context endpoint while every active room has one exclusive state-machine executor | Router forwards only to the ready pod assigned to `roomId`; idempotent by `commandId`; stale by `expectedSequenceNumber`; bounded `SELECT ... FOR UPDATE`; rejected commands emit audit records only and never append Game Integrity; no blind retries after unknown mutation |
| Room runtime controller | Kubernetes API | Reconcile durable Room lifecycle assignment to one pod per active room | Satisfy pod-per-room topology without creating a Service per room or exposing Kubernetes placement to the BFF | Controller retries idempotently; replacement increments the durable generation before routing switches; terminal rooms are torn down without deleting authoritative Room state |
| Room router | Room state-machine pod | Forward room-scoped commands and snapshots to the Ready assigned generation | Keep Kubernetes scheduling outside client request transactions while preserving one Room boundary | Before readiness return `503 room_starting` + `Retry-After`; never blindly retry an unknown mutation; terminal durability precedes endpoint removal |
| Room state-machine pod | Room mutation worker | One bounded queue for every mutation targeting that room | Enforce exclusive state-machine execution before database contention while retaining Postgres as durable authority | One worker; player/timer/reconnect/automatic commands share order; overflow rejects before admission as `429 room_busy`; admitted unknown outcomes reconcile by `commandId` |
| Terminal Room transaction | Room runtime controller | Mark assignment terminal, then delete the dedicated pod | Release execution capacity without deleting authoritative results or coupling SSE closure to Kubernetes deletion | `completed`/`cancelled` commits first; terminal mutations reject without pod recreation; final reads use Postgres/projections; SSE closes from committed terminal events |
| Room router / runtime controller / state-machine pod | Kubernetes API and direct pod network | Separate endpoint observation, pod lifecycle, and gameplay execution identities | Keep request serving from gaining workload mutation authority and keep room pods from discovering/mutating peers | Namespace-scoped RBAC; router `get/list/watch` only; controller pod lifecycle only; room pod no Kubernetes API; direct pod-IP forwarding requires Room mesh identity plus scoped runtime credential |
| Room Gameplay | Redis rate-limit buckets | Per-room/per-user action limits after membership validation | Stop command floods without moving room truth to Redis | Rejection happens before domain mutation, Game Integrity append, or broadcast; audit record only |
| Room Gameplay | Room Timer Worker | Durable Uno deadline plus partitioned Redis sorted-set index and visibility lease | Own the 5-second Uno window without process-local timers | Room persists absolute UTC `expiresAt` with `(roomId, gameId, playerId, triggeringGameEventId)` and opening room sequence; Lua claim is at least once; Room locks/rechecks, appends, publishes, and ignores duplicate or stale expiry |
| Room Gameplay | Room Timer Worker | Durable reconnect deadline plus partitioned Redis sorted-set index and visibility lease | Own the 60-second reconnect window across restarts | Room persists `(roomId, playerId, disconnectVersion)`; Lua claim triggers `ForfeitPlayer`; Room rechecks Postgres; success/stale ack removes lease and expired leases retry |
| Room Gameplay | Game Integrity | Synchronous append/deck API | Log-before-broadcast requires durable append before publication | If append fails, command fails before broadcast; if local commit fails after append, recovery reconciles from KurrentDB |
| Room Gameplay | Debezium Server → Redis Streams → BFF/player feed | Transactional realtime outbox + WAL CDC + snapshot query | Room commits ordered player-safe entries with room state; one Redis-sink pipeline for the Room context database delivers them without polling or a dual write | At-least-once feed delivery; dedupe by event/sequence; if history is trimmed, fetch the authoritative player snapshot; no raw Game Integrity access |
| Room Gameplay | Spectator View | `room.spectator-safe.events` | Spectator projection is rebuilt from already safe room facts | Consumers dedupe by event id and sequence; unsafe events are dropped; new admission allowed in `waiting`/`locked`/`in_progress`; denied in `completed`/`cancelled`; existing streams close on `RoomCompleted`/`RoomCancelled` for the complete match/room |
| Spectator live consumer / ops | Spectator projection-rebuilder | `spectator.projection.rebuild_requested` | Bounded post-retention/quarantine rebuild without outbox polling | Keyed by `roomId`; idempotent by `(recoveryJobId, roomId, failedCheckpoint)`; worker DLQ is `spectator.projection.rebuild_requested.spectator-view.dlq`; live kind proof passed, kind-only Deployment enabled |
| Spectator projection-rebuilder | Room Gameplay | `GET /internal/v1/rooms/{roomId}/spectator-recovery-snapshot` | Authoritative sanitized recovery source after Kafka expiry | Credential `ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL`; response is `SnapshotSanitized`-compatible public state + sequence + `resumeCheckpoint` only; Redis Lua CAS/fenced generation swap + atomic idempotency/quarantine release must not regress live apply |
| Spectator View | BFF/spectator feed | Atomic Redis projection update + Redis Streams | Spectator View updates projection, dedupe/sequence state, and outgoing spectator entry in one Redis transaction or Lua script | Resume from spectator stream sequence; if history is trimmed, fetch the spectator snapshot; never fall back to player or Game Integrity data |
| Room Gameplay | Analytics | `room.gameplay.metrics` | Gameplay-level metrics without private facts | Ad-hoc metrics are anonymized; public tournament metrics may be more granular |
| Room Gameplay | Ranking | `room.game.completed` | Casual Elo source for completed non-abandoned ad-hoc games | Ranking filters by `roomType` and `isAbandoned`, and dedupes by `(playerId, gameId)` |
| Room Gameplay | Tournament Orchestration | `room.match.completed` | Tournament advancement consumes authoritative match facts asynchronously | Tournament validates room-slot mapping and dedupes by `(roomId, completionVersion)` |
| Tournament Orchestration | Room Gameplay | Internal room provisioning command | Tournament owns fanout; Room Gameplay owns room creation | Idempotent by `(tournamentId, roundNumber, slotId)`; retries/DLQ by provisioning batch |
| Tournament Orchestration | Redis rate-limit/backpressure buckets | Per-tournament command limits and worker admission control | Avoid thundering-herd fanout overwhelming Room Gameplay or Kafka | Workers slow or quarantine batches; authoritative tournament state stays in Postgres |
| Tournament Orchestration | Ranking | `tournament.players.advanced`, `tournament.completed` | Advancement depth and ordered final standings feed a separate tournament-placement rating; Tournament publishes no delta | Ranking applies each fact atomically, uses the upstream `eventId` as `placementEventId`, and isolates terminal failures in the source-topic Ranking DLQ without tournament-wide quarantine |
| Tournament Orchestration | Analytics | Public tournament events | Public participation/progression stats | Analytics lag does not block round advancement |
| Ranking | Analytics | Public rating facts/snapshots | Public reporting without querying Ranking DB | Analytics dedupes by upstream event id |
| Analytics live consumer / ops | Analytics projection-rebuilder | `analytics.projection.rebuild_requested` | Bounded post-retention/quarantine backfill without outbox polling | Keyed by `recoveryJobId`; idempotent by `(recoveryJobId, sourceTopic, pageCursor)`; paired checkpoint and/or occurredAt range required; worker DLQ is `analytics.projection.rebuild_requested.analytics.dlq`; live kind proof passed, kind-only Deployment enabled |
| Analytics projection-rebuilder | Room / Tournament / Ranking | `POST /internal/v1/rooms/analytics-backfill`, `POST /internal/v1/tournaments/analytics-backfill`, `POST /internal/v1/rankings/analytics-backfill` | Context-owned safe backfill of the nine Analytics-consumed topics | Scoped Analytics credentials vs producer API secrets; producer-owned HMAC cursors; producer page default 100 / hard max 1000 (worker default 1000); read-only append-only outbox pages; durable ClickHouse generation/lease/clone + active/building dual-write; ClickHouse non-transactional check-then-act acknowledged |

## Fallback Modeling Rule

Not every failure deserves a domain event. The architecture distinguishes operational fallback from business fallback:

- Operational fallback: broker redelivery, transient HTTP timeout, consumer lag, worker restart, and DLQ counters. These are logs, metrics, traces, or runbook signals.
- Business fallback: the saga or projection state changes because the system cannot continue automatically. These changes are modeled as named events.

Examples of business fallback events:

- `TournamentProvisioningBatchRetried`
- `TournamentProvisioningBatchQuarantined`
- `TournamentResultQuarantined`
- `RoomStateReconciled`
- `ProjectionEventQuarantined`

This keeps Kafka-heavy saga flows explicit without polluting the domain event catalog with every infrastructure retry.

## Fallback Matrix

| Flow step | Retry path | Business fallback event | Stop condition |
| --- | --- | --- | --- |
| Tournament provisioning batch -> Room Gameplay | Retry with same `(tournamentId, roundNumber, slotId)` keys | `TournamentProvisioningBatchRetried`, then `TournamentProvisioningBatchQuarantined` if unrecoverable | Batch is complete, quarantined, or explicitly cancelled |
| Room Gameplay -> Game Integrity append | Retry only if append idempotency can prove no duplicate log entry | none for simple append failure; command fails before broadcast | Append confirmed or command rejected/failed |
| Game Integrity append confirmed but Room Gameplay local commit fails | Rebuild room snapshot from KurrentDB log offset | `RoomStateReconciled` | Snapshot/outbox catches up to confirmed log offset |
| Room timer worker -> Room Gameplay expiry | Retry the same stable timer key after visibility lease expiry | none unless the room state changes through `UnoWindowExpired` or `PlayerForfeited` | Expiry applied once, definitively stale/duplicate acknowledged, or lease returned for retry |
| Room Gameplay -> Tournament Orchestration `MatchCompleted` | Kafka redelivery; Tournament dedupes by `(roomId, completionVersion)` | `TournamentResultQuarantined` for conflicting slot/result | Result recorded, duplicate ignored, or quarantined |
| Room Gameplay -> Ranking `GameCompleted` | Kafka redelivery; Ranking dedupes by `(playerId, gameId)` | none unless a future rating correction policy is introduced | Rating applied, ignored by eligibility filter, or duplicate ignored |
| Projection worker consumes unsafe/private event | Drop and flag event | `ProjectionEventQuarantined` when policy/schema violation must be reviewed | Event enters the consumer-owned DLQ; affected aggregate is quarantined, while unrelated aggregate keys continue |
| Spectator/Analytics exceed Kafka retention or need contiguous rebuild | Emit rebuild-request; worker calls context-owned snapshot/backfill API; replay held quarantine records | `ProjectionEventQuarantined` remains until continuity proven | Quarantine releases only after snapshot/backfill + held-record continuity; never skip a gap; rebuild job failures enter worker-owned DLQ |
| Any Kafka consumer exhausts retry budget | Publish to consumer-owned DLQ | Existing business fallback event only when the domain process itself changes | DLQ acknowledged before source offset commit; ordered aggregate quarantined, unrelated keys continue |
| BFF receives `SessionInvalidated` | Broadcast to all gateway instances; close matching streams | none unless stream close fails repeatedly and becomes operational incident | Old streams closed or old commands rejected by session checks |

The successful Identity Postgres validation is the authorization linearization point. A command admitted before a concurrent revocation commit may finish. Once revocation commits, every later validation of the superseded session rejects even when Redis or gateway invalidation state is stale.

## SSE Pattern

- SSE is the realtime delivery layer for room state, spectator projections, and control messages.
- Streams should be resumable using the last known sequence marker.
- Stream closure is a first-class control signal, not a side effect of random disconnect handling.
- `SessionInvalidated` must terminate live streams through the BFF control plane.
- Spectator SSE admission is allowed only while the room is `waiting`, `locked`, or `in_progress` subject to public/private authorization.
- Spectator admission is denied after `RoomCompleted` or `RoomCancelled`, and existing spectator streams close at that terminal room/match state (not at individual game end in a best-of-three).
- Uno window SSE payloads expose absolute UTC `expiresAt` and the opening room sequence; CLI display is advisory and server timing is exclusive.

## Internal APIs

- Game Integrity exposes an internal-only audit and replay API.
- Room Gameplay exposes internal calls to the integrity service before broadcast.
- Room Gameplay exposes internal spectator-recovery-snapshot and analytics-backfill APIs for Spectator/Analytics projection rebuilders (ADR-0039); Tournament and Ranking expose matching analytics-backfill APIs for their Analytics-consumed topics.
- Tournament Orchestration consumes room completion facts asynchronously.
- Analytics and Spectator View consume published safe facts and, for post-retention recovery, call those context-owned APIs rather than scraping internal databases or polling outboxes.
- Rebuild-request topics and internal recovery APIs are not BFF/OpenAPI surfaces.

## Contract Intent

- OpenAPI is for command envelopes, snapshot responses, status responses, SSE stream endpoints, and control-plane endpoints on the BFF, including SSE `409 snapshot_required`. SSE wire framing (`id`/`event`/`data`) is documented in OpenAPI, not AsyncAPI.
- AsyncAPI is for Kafka topics and projection-event consumers only. It does not define SSE wire frames. The Kafka envelope is authoritative; offline HTTP bridges apply documented transforms of canonical fields and must not be described as identical Kafka bodies.
- The contract documents should track `schemaVersion` and compatibility expectations per bounded context.

## Representative Event Payloads

`room.match.completed` is produced by Room Gameplay and consumed by Tournament Orchestration and Analytics. It carries ranked match facts, not advancement decisions.

```json
{
  "eventId": "evt_...",
  "eventType": "MatchCompleted",
  "schemaVersion": 1,
  "correlationId": "corr_...",
  "roomId": "room_123",
  "tournamentId": "t_1",
  "roundNumber": 1,
  "slotId": "slot_99",
  "completionVersion": 18,
  "players": [
    {
      "playerId": "p1",
      "matchWins": 2,
      "cumulativeCardPoints": 12,
      "finalGameCompletedAt": "2026-05-17T12:00:00Z",
      "forfeited": false
    }
  ],
  "forfeits": [],
  "isAbandoned": false,
  "occurredAt": "2026-05-17T12:00:01Z"
}
```

`room.game.completed` is produced by Room Gameplay and consumed by Ranking only for casual Elo when `roomType=ad_hoc` and `isAbandoned=false`. The Ranking bridge body requires authoritative participants (placement, card points, outcome), not winner-only facts.

```json
{
  "eventId": "evt_...",
  "eventType": "GameCompleted",
  "schemaVersion": 1,
  "correlationId": "corr_...",
  "commandId": "cmd_...",
  "roomId": "room_123",
  "gameId": "game_456",
  "roomType": "ad_hoc",
  "completionReason": "normal",
  "isAbandoned": false,
  "authoritative": true,
  "completed": true,
  "placementOrder": ["p1", "p2", "p3"],
  "participants": [
    {"playerId": "p1", "placement": 1, "cardPoints": 0, "outcome": "won"},
    {"playerId": "p2", "placement": 2, "cardPoints": 12, "outcome": "placed"},
    {"playerId": "p3", "placement": 3, "cardPoints": 40, "outcome": "placed"}
  ],
  "occurredAt": "2026-05-17T12:00:01Z"
}
```

`tournament.players.advanced` is produced by Tournament Orchestration after it validates a room result and applies the advancement rule.

For Ranking, `roundNumber` is the achieved advancement depth of every player in `advancingPlayerIds`. Ranking calculates the rating effect; the event never carries a delta.

```json
{
  "eventId": "evt_...",
  "eventType": "PlayersAdvanced",
  "schemaVersion": 1,
  "tournamentId": "t_1",
  "roundNumber": 1,
  "sourceSlotId": "slot_99",
  "advancingPlayerIds": ["p1", "p2", "p3"],
  "rule": "match_wins_card_points_completion_time",
  "occurredAt": "2026-05-17T12:00:02Z"
}
```

`tournament.completed` publishes complete final-room standings in placement order. Array position is the final placement (`1` for the first entry); Ranking again calculates the rating effect and uses the event `eventId` as each player's `placementEventId`.

```json
{
  "eventId": "evt_final_...",
  "eventType": "TournamentCompleted",
  "schemaVersion": 1,
  "tournamentId": "t_1",
  "finalStandings": ["p1", "p2", "p3"],
  "completionReason": "final_room_completed",
  "occurredAt": "2026-05-17T12:30:00Z"
}
```

`identity.session.invalidated` is produced by Identity and consumed by BFF/gateway instances and service-side caches.

```json
{
  "eventId": "evt_...",
  "eventType": "SessionInvalidated",
  "schemaVersion": 1,
  "playerId": "p1",
  "sessionId": "session_old",
  "reason": "new_login",
  "occurredAt": "2026-05-17T12:00:03Z"
}
```

`room.gameplay.metrics` is produced by Room Gameplay after applying its metrics privacy policy. Ad-hoc metrics are anonymized; public tournament metrics may include public identifiers.

```json
{
  "eventId": "evt_...",
  "eventType": "GameplayMetric",
  "schemaVersion": 1,
  "roomId": "room_123",
  "gameId": "game_456",
  "visibility": "public_tournament",
  "metricType": "card_played",
  "publicCard": "red-7",
  "occurredAt": "2026-05-17T12:00:04Z"
}
```

Uno window facts on player and spectator-safe streams expose absolute UTC `expiresAt` and `openingSequence` (the opening room sequence). They travel with the accepted gameplay publication that opened the window (for example after `CardPlayed`), not as a separate Game Integrity-only concern. CLI countdown is advisory; the server exclusively decides timeliness. Spectator ingest may accept `openingRoomSequence` as an alias; producers emit `openingSequence`.

```json
{
  "eventId": "evt_...",
  "eventType": "CardPlayed",
  "schemaVersion": 1,
  "roomId": "room_123",
  "gameId": "game_456",
  "roomSequence": 42,
  "unoWindow": {
    "playerId": "p1",
    "triggeringGameEventId": "evt_play_...",
    "openingSequence": 42,
    "expiresAt": "2026-05-17T12:00:09Z"
  },
  "occurredAt": "2026-05-17T12:00:04Z"
}
```

## Reliability Rules

- Commands must be idempotent at the envelope layer.
- Existing room mutations must be sequence-checked.
- Rejected commands emit structured operational/security audit records only; they never publish domain events or append Game Integrity entries.
- Duplicate event delivery must be safe for consumers.
- Projection rebuilds must be possible from authoritative upstream facts.

## Transport and Network Rules

- Production uses Istio Ambient for strict east-west L4 mTLS, SPIFFE-backed Kubernetes service-account identities, L4 authorization, and transport telemetry without per-pod sidecars.
- Enrolled namespaces use default-deny policy with explicit service-to-service and operator allowances. Plaintext traffic from outside the mesh is rejected.
- L7 waypoint proxies are not deployed by default; HTTP authorization, domain policy, and correlation remain application responsibilities.
- Service credentials remain scoped per bounded context and are verified in addition to transport identity where the internal API contract requires them.
- Correlation headers should be propagated end to end.
- Retry behavior should be conservative and explicit, especially around command handling.
