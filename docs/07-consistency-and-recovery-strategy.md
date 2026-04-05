# Consistency and Recovery Strategy

This document focuses on domain-level consistency rather than transport or infrastructure details.

## Consistency Model by Context

- **Room Gameplay**: strong consistency inside one room aggregate. Turn decisions, hand changes, and match score updates are synchronous and atomic at the room boundary.
- **Game Integrity**: strong consistency inside deck and log aggregates. Deck order and append-only log semantics must not be eventually consistent.
- **Tournament Orchestration**: strong consistency per tournament aggregate and per round aggregate; eventual consistency with room outcomes arriving asynchronously.
- **Ranking**: eventual consistency relative to Room Gameplay.
- **Spectator View**: eventual consistency and strictly filtered visibility.

## Retries

- Cross-context consumers retry processing of authoritative events until success.
- Retries must reuse the same event identity and business idempotency keys.
- A retry must never produce a second winner advancement or a second rating application.

## Deduplication

Deduplication is required in two places:

1. **Command side**
   Player-originated commands carry an idempotency key so duplicate submissions caused by reconnects or network retries are recognized.
2. **Event-consumer side**
   Downstream contexts store processed event identities or business keys such as `(roomId, completionVersion)` or `(playerId, matchId)`.

## Compensation and Saga Decisions

The preferred business strategy is:

- do not roll back already-completed room gameplay
- coordinate downstream consequences with orchestration and idempotent consumers
- compensate only by emitting new authoritative business facts

Examples:

- If Ranking applies late, no compensation is needed; it simply catches up.
- If Tournament Orchestration detects a conflicting room result, it emits `TournamentResultQuarantined` rather than overwriting the slot.
- If a player was advanced incorrectly due to a validated but later-disputed result, the correction should occur through explicit adjudication events, not silent mutation of history.

## How Invariant Violations Are Prevented

- room-level invariants are checked synchronously before event commit
- stale sequence numbers block concurrent conflicting actions
- deck and log invariants are protected by single-writer semantics per aggregate
- round invariants reject results from unassigned rooms
- ranking invariants reject duplicate rated match applications
- spectator invariants block private data from entering the projection

## How Invariant Violations Are Detected

- audit replay from Game Integrity can detect impossible or inconsistent histories
- Tournament Orchestration can detect duplicate or conflicting results for the same slot
- Ranking can detect duplicate match application attempts
- Spectator View can detect events that violate its visibility policy and drop them

## Recovery Paths

## Room Recovery

- rebuild the room from committed gameplay events and integrity log position
- resume from the latest accepted sequence number
- reject client commands that target an older state after recovery

## Tournament Recovery

- recompute round completion by replaying authoritative match outcomes
- reissue idempotent advancement commands for slots missing downstream effects

## Ranking Recovery

- replay completed rated matches not yet marked as applied for a given player
- recompute leaderboard snapshots from durable rating history if needed

## Spectator Recovery

- rebuild projections from spectator-safe upstream events
- re-filter historical events if the visibility policy changes

## Business-Level Trade-off

The design prefers:

- strong consistency for rules inside a room
- eventual consistency across contexts
- immutable history plus explicit corrective facts instead of distributed rollback

That trade-off keeps gameplay responsive while preserving auditability and recoverability.
