# 04 Persistence by Context

## Identity

**Authoritative store**

- Postgres for sessions, ACLs, and session invalidation state

**Cache**

- Redis for active-session lookups and short-lived authorization acceleration

**Persistence rules**

- The source of truth is Postgres.
- Redis may be evicted and rebuilt.
- Session invalidation must remain durable even if the cache is cold.
- Consistency is strong for session creation, supersession, revocation, and ACL updates.
- The live invalidation path stores durable revocation state first, then publishes `identity.session.invalidated`; gateway instances that miss the event still reject old commands on the next session validation.
- Session and ACL retention follows security/audit needs and should keep enough history to investigate takeover or duplicate-session disputes.

## Room Gameplay

**Authoritative store**

- Postgres for operational room snapshots
- Postgres for durable deadline records and room-state versioning

**Supporting stores**

- Redis as a scheduling index for timers
- Outbox table for reliable publication
- Read API / stream for the player feed

**Persistence rules**

- The room snapshot is the operational truth for gameplay.
- Timer deadlines are durable in Postgres, not Redis.
- Redis can accelerate dispatch, but it cannot define room truth.
- The outbox bridges committed room state to downstream consumers.
- Consistency is strong inside one room aggregate: command dedupe, sequence-number checks, hand mutation, turn advancement, match score, timer deadline writes, and outbox rows are committed in the same room transaction after Game Integrity append confirmation.
- Read models include the player feed and reconnect snapshot. Players may read their own private hand state plus public room state; spectators must use Spectator View instead.
- Retention keeps operational snapshots until the room/match can no longer affect reconnect, tournament advancement, rating, or dispute windows; immutable audit history remains in Game Integrity.

## Game Integrity

**Authoritative store**

- EventStoreDB as the append-only per-room or per-game technical log

**Supporting APIs**

- Internal audit export
- Internal replay API

**Persistence rules**

- Appends are immutable.
- Replay is deterministic from the log.
- The store is not a cache and should not be treated like one.
- Consistency is strong per room/game stream by expected revision; stale or replayed append attempts are rejected.
- The immutable game log is retained for dispute resolution, replay verification, tournament audit, and integrity investigations.
- Authorized operators, automated replay jobs, and compliance/audit tooling may query or export the log through internal-only APIs guarded by service credentials, role checks, correlation logging, and break-glass procedures for exceptional access.
- Spectator View, Analytics, and public clients cannot query raw Game Integrity logs and must consume sanitized/public streams.

## Tournament Orchestration

**Authoritative store**

- Postgres for tournament state, registration, round progress, and advancement records

**Supporting store**

- Redis for bracket projection and fast read access

**Persistence rules**

- Tournament advancement state is durable in Postgres.
- Sharded provisioning workers can scale independently from the read projection.
- Tournament processing is async around `MatchCompleted`.
- Consistency is strong per tournament, round, and slot assignment. Async room results are deduped before they update advancement state.
- Redis bracket projections are read models only; they are rebuilt from Postgres tournament state and published tournament events.
- Retention keeps registration, room-slot assignments, match results, advancement decisions, tie-break inputs, and final standings for audit and bracket reconstruction.

## Ranking

**Authoritative store**

- Postgres for rating history and authoritative ranking records

**Cache**

- Redis for leaderboard reads

**Persistence rules**

- Rank updates are derived from authoritative game or tournament results.
- The cache can be rebuilt.
- Ranking writes should not block gameplay.
- Consistency is eventual relative to Room Gameplay and Tournament Orchestration. Each rating application is idempotent by upstream event/player keys.
- Read models include Redis leaderboard snapshots and rating-history queries; stale leaderboards are acceptable while authoritative Postgres rating history catches up.
- Retention keeps rating history and upstream event references so duplicate or corrected inputs can be audited without recomputing from lossy aggregates.

## Spectator View

**Projection store**

- Redis materialized projection

**Inputs**

- committed safe events
- sanitized snapshots

**Persistence rules**

- The projection is rebuilt from durable upstream facts.
- Spectator View owns privacy filtering.
- It should never query private gameplay state directly.
- Redis may use persistence/replication for faster recovery, but the durable source is the upstream Room Gameplay/Game Integrity-backed event history and sanitized snapshots.
- Consistency is eventual and versioned by room sequence. The projection may lag briefly but must never include private hand, hidden deck, or player-only action data.
- Read models are room spectator snapshots and incremental SSE payloads served through the BFF.
- Projection retention follows active-room and replay needs; lost Redis state is recovered by replaying safe events and sanitized snapshots, not by querying private stores.

## Analytics and Public Read Models

**Authoritative store**

- ClickHouse

**Inputs**

- sanitized/public events
- `room.gameplay.metrics`
- public tournament metrics

**Persistence rules**

- Analytics is derived and non-authoritative.
- The model can be rebuilt or backfilled from safe upstream facts.
- More granular public tournament metrics are allowed than ad-hoc anonymized analytics, as long as they remain public and non-authoritative.
- Consistency is eventual and optimized for ingestion/query throughput, not transactional decisions.
- Read models include public aggregate metrics, public tournament reporting, and anonymized ad-hoc gameplay statistics.
- Retention is analytics-oriented and may aggregate or anonymize older ad-hoc data; it must not become the audit store for gameplay, tournaments, ratings, or privacy enforcement.

## Storage Summary

| Context | Authoritative | Cache / Index | Notes |
| --- | --- | --- | --- |
| Identity | Postgres | Redis | Session and ACL truth stays in Postgres |
| Room Gameplay | Postgres | Redis, outbox | Redis is only a timer index |
| Game Integrity | EventStoreDB | none | Append-only by design |
| Tournament Orchestration | Postgres | Redis | Redis holds bracket projection |
| Ranking | Postgres | Redis | Async updates, cacheable reads |
| Spectator View | Redis projection | none | Rebuilt from safe facts; not source of domain truth |
| Analytics and Public Read Models | ClickHouse | none | Derived reporting only |
