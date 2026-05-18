# 02 Bounded Context Architecture

## 1. Identity

**Responsibility**

Authenticate players, maintain internal session state and ACLs, and expose revocation state to the BFF and downstream services.

**Owns**

- player identity and external IdP linkage
- active session state
- session invalidation
- ACL and eligibility checks

**Notes**

- Postgres is authoritative.
- Redis may cache active session lookups.
- `SessionInvalidated` must be able to close open SSE streams through the BFF control channel.

**Interfaces**

- Synchronous: internal session validation and eligibility APIs used by the BFF and selected services.
- Asynchronous: `identity.session.invalidated`, produced by Identity and consumed by BFF/gateway instances and service-side invalidation caches.
- External dependency: external IdP behind an anti-corruption layer that maps provider claims to `PlayerId`, `SessionId`, roles, and eligibility.

**Dependencies**

- Upstream: external IdP, translated through the Identity anti-corruption layer.
- Downstream: BFF/gateway instances and service-side authorization caches consume Identity's published session language.
- Contract ownership: Identity owns session validity, `PlayerId`, `SessionId`, roles, eligibility, and `identity.session.invalidated`.

## 2. Room Gameplay

**Responsibility**

Own the room lifecycle, Uno rules, turn sequencing, command validation, and operational room state.

**Owns**

- room status and roster
- sequence-number validation
- hand and turn legality
- penalty windows and timers
- player feed read API and stream
- sanitized gameplay metrics policy

**Notes**

- Room Gameplay is the rule engine.
- It calls Game Integrity before public broadcast.
- Postgres holds the operational snapshot and timer deadlines.
- Redis may accelerate timer dispatch, but does not own room truth.

**Interfaces**

- Synchronous through BFF: `POST /v1/rooms`, `POST /v1/rooms/{roomId}/commands`, player snapshot/read APIs for reconnect.
- Internal synchronous: append/deck calls to Game Integrity before publication.
- Asynchronous: room business streams such as `room.game.completed` and `room.match.completed`; high-volume sanitized projection/metrics streams such as `room.player-feed.events`, `room.spectator-safe.events`, and `room.gameplay.metrics`.
- Internal commands: `ExpireUnoWindow`, `ForfeitPlayer`, `SkipDisconnectedTurn`, and match lifecycle policy commands.

**Dependencies**

- Upstream: BFF for authenticated command envelopes; Identity for session and eligibility facts.
- Internal peer: Game Integrity owns append-only technical history and deck/draw confirmation before Room Gameplay may publish.
- Downstream: Tournament Orchestration consumes match results; Ranking consumes eligible casual game results; Spectator View and Analytics consume sanitized/public facts.
- Contract ownership: Room Gameplay owns the room command result, room sequence, gameplay events, match facts, timer outcomes, and sanitized projection facts.

## 3. Game Integrity

**Responsibility**

Own technical integrity only: append-only game history, replay, auditability, and the authoritative draw/order log per room or game.

**Owns**

- append-only event history
- replay position and log offsets
- audit export
- deterministic recovery inputs

**Notes**

- EventStoreDB is the authoritative store.
- The service is internal-only.
- Audit and replay APIs are not public gameplay APIs.

**Interfaces**

- Internal synchronous: append log entry, initialize deck, confirm draw/order operations for Room Gameplay.
- Internal operator/compliance: replay and audit export by `roomId`/`gameId` with strict authorization.
- Asynchronous: optional append-confirmed integration stream for internal recovery/projection tooling; not used as the public gameplay stream.

**Dependencies**

- Upstream: Room Gameplay is the only normal writer of gameplay append/deck requests.
- Downstream: authorized replay, audit, and reconciliation tooling may read internal exports.
- Contract ownership: Game Integrity owns expected-revision append semantics, immutable log offsets, replay contracts, and audit export rules.

## 4. Tournament Orchestration

**Responsibility**

Own tournament lifecycle, registration, provisioning, room assignment, round closure, and bracket advancement.

**Owns**

- tournament lifecycle
- registration and eligibility checks
- sharded room provisioning
- async consumption of `MatchCompleted`
- advancement state
- final ranking placement facts

**Notes**

- Postgres is authoritative.
- Tournament calculates `PlayersAdvanced`; Room Gameplay does not.
- Redis can hold a bracket projection for fast read access.

**Interfaces**

- Synchronous through BFF: tournament creation, registration, compact tournament command envelopes, bracket/read APIs.
- Internal synchronous: provisioning workers call Room Gameplay idempotently to create tournament rooms using `(tournamentId, roundNumber, slotId)`.
- Asynchronous input: `room.match.completed`.
- Asynchronous output: `tournament.match.assigned`, `tournament.match.result_recorded`, `tournament.players.advanced`, `tournament.round.completed`, `tournament.completed`.

**Dependencies**

- Upstream: BFF for tournament commands; Identity for registration eligibility; Room Gameplay for authoritative match facts.
- Downstream: Room Gameplay receives idempotent provisioning commands; Ranking consumes placement facts; Analytics consumes public tournament metrics.
- Contract ownership: Tournament Orchestration owns registration state, round lifecycle, room-slot assignment, advancement decisions, bracket state, and tournament completion facts.

## 5. Ranking

**Responsibility**

Maintain persistent ranking state and rating history.

**Owns**

- casual Elo or equivalent competitive rating
- tournament placement rating
- rating history
- leaderboard projection

**Notes**

- Postgres is authoritative.
- Redis is a cache for leaderboard reads.
- Updates are async and derived from authoritative room or tournament results.

**Interfaces**

- Synchronous through BFF: leaderboard and rating-history queries.
- Asynchronous input: eligible `room.game.completed` for casual Elo and tournament placement facts from Tournament Orchestration.
- Asynchronous output: `ranking.player_rating_updated`, `ranking.leaderboard_snapshot_published`.

**Dependencies**

- Upstream: Room Gameplay publishes completed, non-abandoned ad-hoc game results; Tournament Orchestration publishes tournament placement facts.
- Downstream: BFF reads leaderboards/rating history; Analytics may consume public rating facts.
- Contract ownership: Ranking owns rating rules, rating history, public leaderboard snapshots, and the separation between casual Elo and tournament placement rating.

## 6. Spectator View

**Responsibility**

Serve privacy-filtered room projections to anonymous observers and read-only consumers.

**Owns**

- room spectator projection
- privacy filtering rules
- public stream shaping

**Notes**

- Redis is the materialized projection store.
- It is rebuilt from committed safe events and sanitized snapshots.
- It never becomes the source of truth for private gameplay data.

**Interfaces**

- Synchronous through BFF: spectator room snapshot query for initial load or reconnect.
- Asynchronous input: `room.spectator-safe.events`.
- Asynchronous output: `spectator.room_projection.updated`.

**Dependencies**

- Upstream: Room Gameplay publishes already safe room facts and sanitized snapshots.
- Downstream: BFF reads spectator projections and streams them to spectator clients.
- Contract ownership: Spectator View owns the projection schema and visibility policy; it does not accept raw private gameplay or Game Integrity log data.

## 7. Analytics and Public Read Models

**Responsibility**

Provide non-authoritative analytics, public aggregates, and derived reporting views.

**Owns**

- ClickHouse analytics models
- ad-hoc anonymized metrics
- public tournament metrics
- coarse-grained operational reporting

**Notes**

- This is a narrow bounded context, not a generic reporting bucket.
- It consumes sanitized/public events, including `room.gameplay.metrics`.
- Its outputs are derived and non-authoritative.

**Interfaces**

- Synchronous through BFF: public reporting/statistics queries.
- Asynchronous input: anonymized ad-hoc gameplay metrics, public tournament gameplay metrics, tournament lifecycle/advancement facts, and public rating facts.
- Asynchronous output: optional public analytics refresh notifications.

**Dependencies**

- Upstream: Room Gameplay, Tournament Orchestration, and Ranking publish sanitized or public facts.
- Downstream: BFF and public reporting consumers query derived read models.
- Contract ownership: Analytics owns public reporting schemas and ingestion idempotency, but not gameplay, advancement, rating, privacy, or audit decisions.

## Context Map

```mermaid
flowchart LR
    Id["Identity"]
    Room["Room Gameplay"]
    Int["Game Integrity"]
    Tour["Tournament Orchestration"]
    Rank["Ranking"]
    Spec["Spectator View"]
    Ana["Analytics and Public Read Models"]

    Id -->|sessions, ACLs, eligibility| Room
    Id -->|sessions, ACLs, eligibility| Tour
    Room -->|append requests, replay confirmation| Int
    Int -->|durable append confirmation| Room
    Room -->|completed room results| Tour
    Room -->|completed casual results| Rank
    Room -->|sanitized room facts| Spec
    Room -->|sanitized public metrics| Ana
    Tour -->|advancement facts| Rank
    Tour -->|public tournament metrics| Ana
    Spec -->|read-only public projection| BFF["Realtime Gateway / BFF"]
    Ana -->|reporting views| BFF
```

## Ownership Boundary Summary

- Identity owns whether a player is allowed to act.
- Room Gameplay owns whether the action is legal in the room.
- Game Integrity owns whether the technical history is append-only and replayable.
- Tournament Orchestration owns bracket progression.
- Ranking owns long-lived rating state.
- Spectator View owns privacy-filtered reads.
- Analytics owns public, non-authoritative derived reporting.
