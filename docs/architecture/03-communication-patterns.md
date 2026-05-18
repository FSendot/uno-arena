# 03 Communication Patterns

## External Pattern

Clients use the BFF only.

- Commands are sent via compact REST envelopes.
- Realtime updates are received via SSE from the same logical gateway.
- Clients never call Room Gameplay, Tournament Orchestration, or any other microservice directly.
- OpenAPI should define the BFF HTTP surface.
- AsyncAPI should define the event and streaming contract surface.

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

## BFF Responsibilities

- Authenticate the request with Identity.
- Validate envelope shape and route it to the correct bounded context.
- Preserve correlation headers across all internal hops.
- Fan out SSE updates for player streams and spectator streams.
- Close streams on `SessionInvalidated` or equivalent control events.

## Rate Limiting

Rate limiting is multi-layered and happens after authentication where player or room scope is needed.

| Layer | Deployable | Scope source | Store / mechanism | Failure behavior |
| --- | --- | --- | --- | --- |
| Edge abuse | Realtime Gateway / BFF | IP address, user agent, unauthenticated route | local counters plus Redis-backed buckets | reject before auth/service dispatch with a retryable throttling response |
| Authenticated user commands | BFF after Identity validation | `PlayerId`, `SessionId`, roles from Identity claims/introspection | Redis token bucket keyed by user/session/action family | reject before bounded-context command dispatch; no domain event is emitted |
| Room gameplay actions | Room Gameplay command middleware | authenticated `PlayerId`, `roomId`, command `type`, room membership | Redis acceleration plus room-local guard in the command transaction | reject before mutation, append, or broadcast; suspicious bursts are audit/ops signals |
| Tournament actions | Tournament Orchestration command middleware | authenticated `PlayerId`, `tournamentId`, role/eligibility | Redis bucket keyed by tournament/action plus Postgres eligibility checks for authoritative gates | reject or queue according to tournament policy; provisioning workers use backpressure rather than client retries |
| Adaptive surge protection | BFF, Room Gameplay, Tournament workers | consumer lag, timer lag, queue depth, shard health | dynamic limits driven by metrics, with conservative defaults | shed non-critical spectator/reporting traffic before player command writes |

Redis accelerates counters but does not become authority for sessions, room state, or tournament state. Rate-limit rejections are command rejections, not domain events, unless the business process explicitly removes or sanctions a player.

## Internal Command and Event Flow

- Room Gameplay accepts the command and performs domain validation.
- If the command is valid, Room Gameplay requests an append from Game Integrity.
- Game Integrity confirms the durable append.
- Only then does Room Gameplay publish the resulting room state outward.
- Downstream projections consume the published facts asynchronously.

## Kafka Topology

Kafka topics are bounded-context-specific.

- Each context owns its own topics and consumers.
- Business streams are separated from projection streams.
- A single catch-all event bus is not the target design.
- Every event should carry an explicit `schemaVersion`.

Examples:

- `room.game.completed`
- `room.match.completed`
- `room.player-feed.events`
- `room.spectator-safe.events`
- `room.gameplay.metrics`
- `identity.session.invalidated`
- `tournament.match.assigned`
- `tournament.match.result_recorded`
- `tournament.players.advanced`
- `tournament.round.completed`
- `ranking.player_rating_updated`
- `spectator.room_projection.updated`
- `analytics.public_projection.updated`

## Integration Table

| From | To | Pattern | Rationale | Failure semantics |
| --- | --- | --- | --- | --- |
| Client | Realtime Gateway / BFF | REST commands + SSE streams | Single external boundary; compact commands and realtime updates | REST retries use `commandId`; SSE resumes from last event id/sequence |
| Realtime Gateway / BFF | Redis rate-limit buckets | Per-IP and authenticated per-user throttling | Protect public edge before services receive abusive traffic | Fail closed for abusive bursts; fail open only for low-risk spectator/reporting reads if policy allows |
| BFF | Identity | Synchronous validation/read | Establish internal `PlayerId`, `SessionId`, roles, and eligibility | Fail closed if identity cannot be validated |
| Identity | BFF | `identity.session.invalidated` control event | Close old SSE streams for single-active-session | Broadcast to gateway instances; stale streams are closed on receipt and old commands are rejected |
| BFF | Room Gameplay | Synchronous command dispatch | Room commands need immediate validation/rejection | Idempotent by `commandId`; stale by `expectedSequenceNumber`; no blind retries after unknown mutation |
| Room Gameplay | Redis rate-limit buckets | Per-room/per-user action limits after membership validation | Stop command floods without moving room truth to Redis | Rejection happens before domain mutation, Game Integrity append, or broadcast |
| Room Gameplay | Room Timer Worker | Durable Uno deadline plus Redis scheduling index | Own the 5-second Uno window without process-local timers | Room persists `(roomId, gameId, playerId, triggeringGameEventId)`; worker triggers `ExpireUnoWindow`; Room rechecks, appends, publishes, and ignores duplicate or stale expiry |
| Room Gameplay | Room Timer Worker | Durable reconnect deadline plus Redis scheduling index | Own the 60-second reconnect window across restarts | Room persists `(roomId, playerId, disconnectVersion)`; worker triggers `ForfeitPlayer`; Room rechecks, appends, publishes, and ignores duplicate or stale expiry |
| Room Gameplay | Game Integrity | Synchronous append/deck API | Log-before-broadcast requires durable append before publication | If append fails, command fails before broadcast; if local commit fails after append, recovery reconciles from EventStoreDB |
| Room Gameplay | BFF/player feed | Sanitized stream/query | Players receive public state plus only their own private hand/draw facts | Resume from sequence/log offset; no raw Game Integrity access |
| Room Gameplay | Spectator View | `room.spectator-safe.events` | Spectator projection is rebuilt from already safe room facts | Consumers dedupe by event id and sequence; unsafe events are dropped |
| Room Gameplay | Analytics | `room.gameplay.metrics` | Gameplay-level metrics without private facts | Ad-hoc metrics are anonymized; public tournament metrics may be more granular |
| Room Gameplay | Ranking | `room.game.completed` | Casual Elo source for completed non-abandoned ad-hoc games | Ranking filters by `roomType`, `isAbandoned`, and processed event id |
| Room Gameplay | Tournament Orchestration | `room.match.completed` | Tournament advancement consumes authoritative match facts asynchronously | Tournament validates room-slot mapping and dedupes by `(roomId, completionVersion)` |
| Tournament Orchestration | Room Gameplay | Internal room provisioning command | Tournament owns fanout; Room Gameplay owns room creation | Idempotent by `(tournamentId, roundNumber, slotId)`; retries/DLQ by provisioning batch |
| Tournament Orchestration | Redis rate-limit/backpressure buckets | Per-tournament command limits and worker admission control | Avoid thundering-herd fanout overwhelming Room Gameplay or Kafka | Workers slow or quarantine batches; authoritative tournament state stays in Postgres |
| Tournament Orchestration | Ranking | Tournament placement events | Tournament placement rating is not casual Elo | Ranking dedupes by `(playerId, tournamentId, placementEventId)` |
| Tournament Orchestration | Analytics | Public tournament events | Public participation/progression stats | Analytics lag does not block round advancement |
| Ranking | Analytics | Public rating facts/snapshots | Public reporting without querying Ranking DB | Analytics dedupes by upstream event id |

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
| Game Integrity append confirmed but Room Gameplay local commit fails | Rebuild room snapshot from EventStoreDB log offset | `RoomStateReconciled` | Snapshot/outbox catches up to confirmed log offset |
| Room timer worker -> Room Gameplay expiry | Retry with same timer key | none unless the room state changes through `UnoWindowExpired` or `PlayerForfeited` | Expiry applied once, ignored as duplicate, or ignored as stale |
| Room Gameplay -> Tournament Orchestration `MatchCompleted` | Kafka redelivery; Tournament dedupes by `(roomId, completionVersion)` | `TournamentResultQuarantined` for conflicting slot/result | Result recorded, duplicate ignored, or quarantined |
| Room Gameplay -> Ranking `GameCompleted` | Kafka redelivery; Ranking dedupes by `(playerId, gameId)` | none unless a future rating correction policy is introduced | Rating applied, ignored by eligibility filter, or duplicate ignored |
| Projection worker consumes unsafe/private event | Drop and flag event | `ProjectionEventQuarantined` when policy/schema violation must be reviewed | Event dropped/quarantined; projection continues from next safe event |
| BFF receives `SessionInvalidated` | Broadcast to all gateway instances; close matching streams | none unless stream close fails repeatedly and becomes operational incident | Old streams closed or old commands rejected by session checks |

## SSE Pattern

- SSE is the realtime delivery layer for room state, spectator projections, and control messages.
- Streams should be resumable using the last known sequence marker.
- Stream closure is a first-class control signal, not a side effect of random disconnect handling.
- `SessionInvalidated` must terminate live streams through the BFF control plane.

## Internal APIs

- Game Integrity exposes an internal-only audit and replay API.
- Room Gameplay exposes internal calls to the integrity service before broadcast.
- Tournament Orchestration consumes room completion facts asynchronously.
- Analytics and Spectator View should consume published safe facts, not scrape internal databases.

## Contract Intent

- OpenAPI is for command envelopes, status responses, and control-plane endpoints on the BFF.
- AsyncAPI is for Kafka topics, projection-event consumers, and SSE payload contracts where schema discipline matters.
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
      "finalGameCompletedAt": "2026-05-17T12:00:00Z"
    }
  ],
  "forfeits": [],
  "isAbandoned": false,
  "occurredAt": "2026-05-17T12:00:01Z"
}
```

`room.game.completed` is produced by Room Gameplay and consumed by Ranking only for casual Elo when `roomType=ad_hoc` and `isAbandoned=false`.

```json
{
  "eventId": "evt_...",
  "eventType": "GameCompleted",
  "schemaVersion": 1,
  "roomId": "room_123",
  "gameId": "game_456",
  "roomType": "ad_hoc",
  "completionReason": "normal",
  "isAbandoned": false,
  "placementOrder": ["p1", "p2", "p3"],
  "occurredAt": "2026-05-17T12:00:01Z"
}
```

`tournament.players.advanced` is produced by Tournament Orchestration after it validates a room result and applies the advancement rule.

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

## Reliability Rules

- Commands must be idempotent at the envelope layer.
- Existing room mutations must be sequence-checked.
- Duplicate event delivery must be safe for consumers.
- Projection rebuilds must be possible from authoritative upstream facts.

## Transport and Network Rules

- No service mesh is required.
- Private networking is sufficient for service-to-service calls.
- Service credentials should be scoped per bounded context.
- Correlation headers should be propagated end to end.
- Retry behavior should be conservative and explicit, especially around command handling.
