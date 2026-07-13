# Consistency and Recovery Strategy

This document focuses on domain-level consistency rather than transport or infrastructure details.

## Consistency Model by Context

- **Room Gameplay**: strong consistency inside one room aggregate. Turn decisions, hand changes, and match score updates are synchronous and atomic at the room boundary.
- **Game Integrity**: strong consistency inside deck and log aggregates. Deck order and append-only log semantics must not be eventually consistent.
- **Tournament Orchestration**: strong consistency per tournament aggregate and per round aggregate; eventual consistency with room outcomes arriving asynchronously.
- **Ranking**: eventual consistency relative to Room Gameplay for casual Elo and relative to Tournament Orchestration for tournament-placement rating.
- **Spectator View**: eventual consistency and strictly filtered visibility.
- **Analytics and Public Read Models**: eventual consistency from sanitized/public upstream events. Analytics projections can be rebuilt and never serve as the source of truth for gameplay, advancement, ratings, privacy, or audit.

## Retries

- Cross-context consumers retry processing of authoritative events until success.
- Retries must reuse the same event identity and business idempotency keys.
- A retry must never produce a second advancement decision or a second rating application.

## Deduplication

Deduplication is required in two places:

1. **Command side**
   Player-originated commands carry an idempotency key so duplicate submissions caused by reconnects or network retries are recognized.
2. **Event-consumer side**
   Downstream contexts store processed event identities or business keys such as `(roomId, completionVersion)`, `(playerId, gameId)`, or `(playerId, tournamentId, placementEventId)`.

## Compensation and Saga Decisions

The preferred business strategy is:

- do not roll back already-completed room gameplay
- coordinate downstream consequences with orchestration and idempotent consumers
- compensate only by emitting new authoritative business facts

Examples:

- If Ranking applies late, no compensation is needed; it simply catches up.
- If a tournament provisioning worker fails transiently, Tournament Orchestration retries the deterministic batch and may emit `TournamentProvisioningBatchRetried` when retry becomes part of saga state.
- If tournament provisioning exhausts retries or detects conflicting room assignments, it emits `TournamentProvisioningBatchQuarantined` and blocks the affected round from starting until explicit reconciliation.
- If Tournament Orchestration detects a conflicting room result, it emits `TournamentResultQuarantined` rather than overwriting the slot.
- If Room Gameplay has an append confirmed by Game Integrity but fails before committing its operational snapshot, recovery emits `RoomStateReconciled` after rebuilding the missing state from the log offset.
- A non-terminal best-of-three `GameCompleted` transaction persists a deterministic next-game continuation. Room-owned timer workers claim it with `FOR UPDATE SKIP LOCKED`; lease expiry recovers worker crashes and the successful `StartNextGame` commit removes it atomically.
- For an uncertain Game Integrity append, Room replays first. It re-appends the exact persisted event only when replay is successful, contains no matching event, and reports exactly the original expected revision. Replay errors, identity mismatch, or an advanced revision remain fail-closed for operator reconciliation.
- If a player was advanced incorrectly due to a validated but later-disputed result, the correction should occur through explicit adjudication events, not silent mutation of history.
- If an Uno challenge deadline expires, the room emits `UnoWindowExpired` exactly once using `(roomId, gameId, playerId, triggeringGameEventId)`; retries cannot close a newer window or apply penalties twice. The published window includes absolute UTC `expiresAt` and the opening room sequence so clients can display an advisory countdown without owning timeliness.
- If a 60-second reconnect deadline expires, the room emits `PlayerForfeited` exactly once using `(roomId, playerId, disconnectVersion)`; retries cannot duplicate the forfeit.

## How Invariant Violations Are Prevented

- room-level invariants are checked synchronously before event commit
- stale sequence numbers block concurrent conflicting actions
- rejected commands never mutate state, never emit domain events, and never append Game Integrity entries; they emit structured operational/security audit records only
- Uno-window expiry commands re-check persisted absolute UTC `expiresAt` and opening room sequence before emitting `UnoWindowExpired`, so delayed timer delivery or advisory client countdown cannot close a window that was already resolved or superseded
- deck and log invariants are protected by single-writer semantics per aggregate
- round invariants reject results from unassigned rooms
- ranking invariants reject duplicate casual-game Elo applications and duplicate tournament-placement applications
- spectator invariants block private data from entering the projection; new admission is allowed only in `waiting`, `locked`, or `in_progress`, and denied with stream close after `RoomCompleted` or `RoomCancelled`
- ad-hoc host leave before lock/start reassigns to the lowest occupied seat or cancels immediately when empty; after lock/start host labels have no gameplay authority
- session invariants reject commands from a previous active session after a new login invalidates it
- rate-limit and input-validation policies reject abusive or malformed commands before domain mutation
- sensitive published events that cross trust boundaries carry integrity protection, such as signatures or verifiable event identity

## How Invariant Violations Are Detected

- audit replay from Game Integrity can detect impossible or inconsistent histories
- Tournament Orchestration can detect duplicate or conflicting results for the same slot
- Ranking can detect duplicate casual-game or tournament-placement application attempts
- Spectator View can detect events that violate its visibility policy and drop them
- Analytics can detect private or unsanitized facts that violate its projection policy and drop them before public reporting
- command-security checks can detect forged future sequence numbers and replayed old sequence numbers
- structured operational/security audit records capture rejected-command attempts with `commandId`, `correlationId`, session/player, room/tournament, rejection reason, submitted/current sequence, and timestamp

## Recovery Paths

## Room Recovery

- rebuild the room from committed gameplay events and integrity log position
- resume from the latest accepted sequence number
- recover any open Uno deadline from persisted room state, including absolute UTC `expiresAt` and opening room sequence, and reissue the idempotent expiry command if the deadline has passed
- reject client commands that target an older state after recovery and emit structured operational/security audit records for those rejections without appending Game Integrity entries
- close spectator streams and deny new spectator admission once the recovered room is terminal (`RoomCompleted` or `RoomCancelled`)

## Tournament Recovery

- recompute round completion by replaying authoritative match outcomes
- reissue idempotent advancement commands for slots missing downstream effects
- retry deterministic provisioning batches by `(tournamentId, roundNumber, batchId)` without creating duplicate rooms
- quarantine provisioning batches or match results that cannot be reconciled automatically

## Ranking Recovery

- replay completed non-abandoned casual games not yet marked as applied for a given player
- ignore abandoned casual games and tournament games for casual Elo
- replay tournament placement facts separately from casual Elo events
- recompute leaderboard snapshots from durable rating history if needed

## Spectator Recovery

- rebuild projections from spectator-safe upstream events while Kafka history remains available
- after retention expiry or quarantine requiring rebuild, emit `spectator.projection.rebuild_requested` and fetch Room `GET /internal/v1/rooms/{roomId}/spectator-recovery-snapshot` with `ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL`; Redis Lua CAS/fence generation-swap so newer live apply wins, recording `(recoveryJobId, roomId, failedCheckpoint)` idempotency atomically with fenced quarantine release
- replay bounded held post-gap quarantine/DLQ records in sequence for that `roomId` only (max 1000); release quarantine only after continuity is proven; never silently skip a gap
- rebuilder Deployment is enabled only in kind after the live recovery proof; default/staging/production remain disabled
- re-filter historical events if the visibility policy changes
- restore admission rules so new connections are allowed only while the projected room is `waiting`, `locked`, or `in_progress`, and close streams for terminal rooms

## Analytics Recovery

- replay sanitized gameplay metrics, public tournament facts, and public rating facts from their upstream topics while Kafka history remains available
- after retention expiry or quarantine requiring rebuild, emit `analytics.projection.rebuild_requested` and page Room/Tournament/Ranking `POST .../analytics-backfill` APIs (producer-owned HMAC cursor; paired range required; producer default 100 / hard max 1000; worker default page 1000; read-only append-only outbox)
- apply under a durable ClickHouse rebuilding generation/lease (deterministic generation, lease readback, active/building dual-write, server-side clone) so concurrent live ingest joins the fence; claim continuity only after every requested page/checkpoint is reconciled
- acknowledge ClickHouse non-transactional projection-before-marker check-then-act; same-generation redelivery is idempotent via FINAL processed markers
- rebuilder Deployment is enabled only in kind after the live recovery proof; default/staging/production remain disabled
- rebuild public reporting projections without changing source-of-truth contexts
- drop or quarantine events that do not satisfy the analytics anonymization and public-data policy

## Business-Level Trade-off

The design prefers:

- strong consistency for rules inside a room
- eventual consistency across contexts
- immutable history plus explicit corrective facts instead of distributed rollback
- derived analytics that can lag or rebuild without blocking live gameplay, tournament advancement, or rating decisions

That trade-off keeps gameplay responsive while preserving auditability and recoverability.
