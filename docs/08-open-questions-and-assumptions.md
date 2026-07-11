# Open Questions and Assumptions

This document separates assumptions made for modeling purposes from requirements that appear validated by the assignment statement or settled product decisions.

## Validated Requirements

- A spectator capability exists and must be modeled explicitly.
- The language must distinguish `game`, `match`, `round`, and `tournament`.
- Tournament progression, Elo updates, and room completion must be described as event flows.
- Edge cases must include concurrent actions, disconnections, stale/replayed commands, partial failures, security/abuse, and spectator privacy violations.
- The submission should be provided as markdown documents with a root index.
- The Uno challenge window is fixed at 5 seconds and closes earlier if the next player begins their turn.
- The reconnection window is fixed at 60 seconds with no bot substitution.
- Tournament non-final matches advance the top 3 players by match wins, then lowest cumulative card points, then earliest final-game completion time.
- Casual Elo is updated once per completed non-abandoned ad-hoc game; tournament play uses a separate placement rating.
- A new login invalidates the player's previous active session.
- Rejected commands never produce domain events or Game Integrity log entries. They emit structured operational/security audit records that include `commandId`, `correlationId`, session/player identity, room/tournament identity when known, rejection reason, submitted and current sequence numbers when applicable, and timestamp.
- Spectators may establish a new spectator connection while a room is `waiting`, `locked`, or `in_progress`, subject to public/private room authorization. Admission is denied after `RoomCompleted` or `RoomCancelled`, and existing spectator streams close at that terminal room/match state. Terminal here means the complete match/room lifecycle, not the end of an individual game inside a best-of-three match.
- If an ad-hoc host leaves before lock/start, the remaining player in the lowest occupied seat becomes host deterministically. If nobody remains, the room cancels immediately. After lock/start, host reassignment has no gameplay authority.
- Uno windows publish an absolute UTC `expiresAt` and the room sequence at which the window opened (`openingSequence` on the wire). Client countdown or display is advisory only. The server exclusively decides timeliness; SSE updates and command results correct clients.
- This implementation uses the repo-owned simple CLI as the sole client and test interface. A graphical interface is explicitly deferred for later refactoring and does not change the BFF-only external boundary.
- BFF clients resync via `GET /v1/rooms/{roomId}/snapshot` or `GET /v1/spectator/rooms/{roomId}/snapshot` after SSE `409 snapshot_required`.
- Current capability-mode adapters (`GATEWAY_CAPABILITY_MODE` / `ROOM_CAPABILITY_MODE` / `IDENTITY_CAPABILITY_MODE` / `ANALYTICS_CAPABILITY_MODE` / `SPECTATOR_CAPABILITY_MODE`, GI explicit memory, HTTP bridge transforms) are distinct from required durable adapters. Identity durable Postgres + OIDC are implemented when `DATABASE_URL` is set; local CDC prerequisites exist, but Debezium CDC delivery for Identity outbox remains pending. Room durable Postgres + Redis timer index are implemented when `DATABASE_URL` + `REDIS_URL` are set; Debezium Kafka connector and Redis sink delivery for Room dual outboxes remain pending. Analytics durable ClickHouse HTTP/store is implemented when `CLICKHOUSE_URL` plus scoped credentials are set; Kafka ingestion for Analytics remains pending. Spectator durable Redis projection + Redis-backed SSE are implemented when `REDIS_URL` plus scoped credentials are set; Kafka consumer for `room.spectator-safe.events` remains pending (HTTP internal ingest is a test/ops bridge). Gateway durable Redis rate limiting and direct player/spectator LiveFeed SSE are implemented when `REDIS_URL`, `GATEWAY_PLAYER_FEED_REDIS_URL`, and `GATEWAY_SPECTATOR_REDIS_URL` are configured; non-capability rollout remains blocked when those adapters are absent or unreachable. Gateway Kafka `SessionInvalidated` consumption and the Room Debezium player-feed sink remain pending. Game Integrity durable KurrentDB is implemented separately (see ADRs 0024/0035).

## Assumptions

These assumptions were used to make the domain model concrete where the brief does not define exact rules.

### Gameplay assumptions

- A room hosts exactly one best-of-three match.
- A match is decided when one player wins two games, while tournament advancement still records the ranked top 3 for non-final matches.
- Standard Uno-like turn, draw, wild-color, and Uno-call mechanics apply.
- Draw-card stacking is enabled. The player targeted by a pending draw penalty may stack a `Draw Two` or legally playable `Wild Draw Four`; penalties accumulate and transfer until a targeted player draws the full total and forfeits the turn.
- Exact-match jump-ins are enabled. Outside mandatory-resolution states, any player may play out of turn when the card matches the discard by both color and rank or action symbol; the jumper becomes the acting player and play resumes after their seat.
- Rejected stale commands are treated as API outcomes rather than business events; observability uses the structured operational/security audit record described above, not the Game Integrity log.

These two optional-rule decisions use [*UNO - How to Play Correctly!*](https://www.youtube.com/watch?v=rC-DYC3ZELM) as their rule source. The video's `01:33` “Special Cards” chapter states that a `Draw Two` may be played on another `Draw Two`; its transcript does not define mixed draw-card stacking or mention jump-ins. Per the product-owner fallback, unmentioned optional-rule behavior is enabled, so mixed draw-card stacking and exact-match jump-ins are part of the implementation baseline.

### Tournament assumptions

- Tournament matches reuse the same room gameplay model as ad-hoc matches.
- Tournament advancement is calculated by Tournament Orchestration from authoritative `MatchCompleted` facts, including match wins, card points, completion time, and forfeit/abandonment markers.
- A round may advance players only after every assigned match reaches a terminal outcome.

### Ranking assumptions

- Casual Elo is updated once per completed non-abandoned ad-hoc game.
- Tournament placement rating is updated separately from tournament placement and final standing facts.

### Spectator assumptions

- Spectators can observe public room and tournament state in near real time while the room/match is non-terminal (`waiting`, `locked`, or `in_progress`).
- Spectators must never receive private hand data or player-authentication data.
- Spectator projections may lag slightly behind authoritative room state.
- Public rooms allow anonymous-tolerant spectator admission; private rooms require an authorized invite, participant, or operator context.

### Analytics assumptions

- Public analytics are derived and non-authoritative.
- Ad-hoc gameplay metrics are anonymized before they enter public analytics.
- Public tournament metrics may include already-public tournament facts, but not hidden card, hand, deck, session, or audit data.

### Client-interface assumptions

- The repo-owned simple CLI is the only client and automated test driver for this implementation checkpoint.
- Graphical UI work is deferred and must continue to speak only to the BFF; it must not introduce direct microservice access.

## Connection-Semantics Assumptions

These assumptions are included because the assignment asks to surface connection-related scope decisions.

- Client commands may be retried by the network or by reconnecting clients, so idempotency keys are required.
- Clients may disconnect temporarily and later rejoin with the same authenticated identity.
- Room commands use a room-scoped sequence number so clients can detect stale state.
- Spectator and player channels are logically distinct even if they share transport technology.
- Late-arriving events are possible across contexts, so downstream consumers must be idempotent and order-aware.
- CLI countdown and local clocks are advisory. Authoritative Uno windows use server-owned absolute UTC `expiresAt` plus the room sequence at which the window opened. Reconnect deadlines remain server-owned absolute UTC timestamps keyed by existing `roomId`/`playerId`/`disconnectVersion` semantics and do not gain room-sequence markers.

## Open Questions

No product-policy questions remain open for the previously unresolved rejection-audit, spectator-admission, host-reassignment, Uno-deadline display, or client-interface scope items. Those decisions are recorded under Validated Requirements above.

## Why These Assumptions Matter

These assumptions mostly affect:

- invariant design inside the Room aggregate
- the event catalog for completion and forfeits
- how casual Elo `GameCompleted` events are kept separate from tournament placement events
- what data Spectator View is permitted to publish
- how rejected commands are observed without polluting domain or Game Integrity streams
- how the CLI client presents advisory timers without claiming authority over server deadlines

The model is internally consistent under the validated requirements and assumptions above.
