# 04 Persistence by Context

This document records the **required durable persistence architecture** per bounded context. Postgres, Redis, KurrentDB 26.0.3 LTS, Kafka (as the async transport paired with Debezium-captured transactional outboxes), and ClickHouse remain the settled technology choices.

For every Postgres-backed command that emits a contracted integration fact, authoritative state and its Kafka-bound outbox record commit together. Debezium captures inserts from PostgreSQL WAL and the Outbox Event Router maps standardized logical fields to the context-owned AsyncAPI topic. Each context-owned Postgres database has one Kafka-bound connector. Room Gameplay also commits a realtime outbox captured by a separate Debezium Server Redis sink for its database; Spectator View updates its Redis projection and spectator stream atomically. Redis Streams remain non-authoritative delivery state. Postgres outboxes are time-partitioned and reclaimed only through the LSN/offset safety gate in ADR-0026.

The current offline-friendly capability implementation uses `GATEWAY_CAPABILITY_MODE` / `ROOM_CAPABILITY_MODE` / `ANALYTICS_CAPABILITY_MODE` / `SPECTATOR_CAPABILITY_MODE` (real HTTP service paths with bounded in-memory limiters / memory session repo / Analytics memory / Spectator memory) plus explicit Game Integrity memory and HTTP bridges so services can run without durable adapters. Isolated-test fakes remain behind `*_ALLOW_FAKES` only. That offline mode is intentional and must not be confused with the configured durable path. Context-owned migrations document the durable schemas. Identity/Room/Ranking/Tournament Postgres, Analytics ClickHouse, Game Integrity KurrentDB, Spectator/Ranking/Tournament Redis projections, Room Redis timers, and Gateway Redis admission/LiveFeed adapters are implemented when their context-owned connections are set. The declared Gateway, Tournament, Ranking, Spectator, and Analytics Kafka consumers are implemented when `KAFKA_BROKERS` is set. Room/Tournament/Ranking analytics-backfill and Room spectator-recovery-snapshot APIs plus Spectator/Analytics projection-rebuilder workers are implemented. A clean ARM64 kind foundation and all eight services have deployed with live Kafka Connect/Server CDC; both full recovery-worker lanes passed. Spectator/Analytics rebuilders are therefore enabled only in kind and remain disabled in default/staging/production values.

Each Postgres-backed context initializes an empty database through its own Kubernetes bootstrap Job. The Job serializes with a context-specific advisory lock inside one admin transaction, decides empty/exact/drift before any role or schema mutation, writes the expected schema-version metadata only on empty apply (DDL attributable to the bootstrap role), and leaves runtime credentials without DDL. Exact requires exactly one migrations row and exactly one bootstrap-meta row matching version/checksum/catalog and is a no-op; any other non-empty state fails unchanged. Until a later stateful-upgrade decision exists, resets are explicit database/volume recreation followed by bootstrap.

Each such context database is one HA cluster with one primary and two synchronous standbys. A commit requires WAL flush on the primary and at least one standby. Authoritative reads/writes, schema bootstrap, and outbox CDC use the fenced stable writer path. Replicas are not shards and may serve only explicitly stale-tolerant reads. After promotion, pools reconnect and unknown write outcomes reconcile through existing idempotency keys/checkpoints; CDC resumes from durable offsets and may redeliver but may not skip committed rows.

Replication does not replace backup. Each Postgres context continuously archives encrypted WAL and creates an encrypted daily base backup outside the cluster failure domain, targeting PITR RPO ≤5 minutes and RTO ≤30 minutes. KurrentDB plus Game Integrity key history targets RPO ≤5 minutes/RTO ≤60 minutes; ClickHouse daily backup targets RPO ≤24 hours/RTO ≤4 hours; Redis correctness recovery is rebuild-only. Quarterly isolated restore drills prove these objectives.

Non-Postgres stores follow their own model. Analytics owns an empty-or-exact-current ClickHouse bootstrap Job and separate DDL credential. Game Integrity creates KurrentDB streams lazily with expected revisions after validating deployment-owned server policy and encryption-key access. Redis owners use versioned context key prefixes, validate capabilities, and load/reload Lua scripts safely; Redis has no schema Job and remains rebuildable.

Every durable Kafka consumer commits its owned-state mutation, AsyncAPI idempotency key, and ordered aggregate checkpoint atomically before acknowledging the source offset. Finite-replay dedupe records remain for at least source retention plus the maximum DLQ/replay window. Permanent replay uses aggregate checkpoints or domain uniqueness constraints; active quarantine/replay references prevent cleanup.

Kafka provides bounded operational replay only. Spectator-safe expiry recovers through `spectator.projection.rebuild_requested` plus Room's internal sanitized spectator-recovery-snapshot API; metrics and other Analytics source expiry recover through `analytics.projection.rebuild_requested` plus Room/Tournament/Ranking internal analytics-backfill APIs; Identity Postgres remains authoritative after invalidation controls expire; business replay beyond seven days uses the producing context's retained state/history. None of these Kafka windows replace context-specific audit retention.

## Identity

**Authoritative store**

- Postgres for external identity mappings, sessions, ACLs, and session invalidation state

**Cache**

- Redis for active-session lookups and short-lived authorization acceleration

**Persistence rules**

- The source of truth is Postgres.
- Canonical IdP issuer/subject and opaque session-token hashes use unique indexed lookups in Identity's context-owned Postgres database.
- External provider tokens are not persisted as domain session authority. Identity stores the accepted issuer/subject linkage and internal session/token hash required for authoritative validation.
- Token secrets are stored only as hashes. Missing or invalid sessions and database failures fail closed.
- Redis may be evicted and rebuilt.
- Redis persistence may shorten restart time but is never the authoritative backup; session/cache truth recovers from Identity Postgres and normal invalidation/validation paths.
- Session invalidation must remain durable even if the cache is cold.
- Positive cache entries are never sufficient authorization for state-changing commands; the authoritative validation read is served from Identity Postgres.
- Consistency is strong for session creation, supersession, revocation, and ACL updates.
- The live invalidation path stores durable revocation state first, then publishes `identity.session.invalidated`; gateway instances that miss the event still reject old commands on the next session validation.
- The invalidation outbox is captured by the Identity database's Debezium connector; Identity does not poll the table or synchronously publish Kafka from the login transaction.
- Identity outbox partitions are reclaimed only after the Identity connector's durable offset passes their sealed WAL high-water LSN plus the safety window.
- Identity owns one database connection pool and one Debezium connector for its context-owned Postgres database.
- Redis and gateway-local invalidation state optimize early rejection and stream closure. They may lag without allowing a mutation to bypass the authoritative Postgres check.
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
- Durable Room lifecycle assignments drive the Room-owned runtime controller. The stable Room router uses those assignments plus ready pod observations to locate the one exclusive executor for each active `roomId`; Kubernetes placement is never authoritative room state.
- Controller replicas claim bounded due assignments with `FOR UPDATE SKIP LOCKED` and expiring leases. A deterministic `(roomId, generation)` pod identity makes create acknowledgment crash-safe; generation transitions remain serialized on the assignment row.
- Each assignment has a monotonically increasing generation. Every Room mutation locks and validates the caller pod's `roomId` and generation in the same authoritative transaction; a stale generation fails closed before Game Integrity append or state/outbox mutation.
- `completed`/`cancelled` Room state and terminal runtime status commit together. Pod deletion is subsequent and disposable; terminal snapshots/results remain durable and readable without recreating an executor.
- All Room state and outbox rows commit in Room Gameplay's context-owned Postgres database. Native table partitioning and indexes may use `roomId` to control hot-table size and access paths.
- Room-owned PgBouncer uses transaction-pooling mode between dedicated room pods and the same physical database. Room pods use lazy zero-minimum, one-maximum client pools with short idle lifetime; transaction-scoped locks and atomic state/outbox commits remain unchanged.
- PgBouncer capacity is explicit per environment: replicas and client admission are sized against active runtimes, while default plus reserve backend pools are bounded against Room Postgres connection capacity. Staging/production require SCRAM-SHA-256 and end-to-end application-layer database TLS. Every Kubernetes environment uses strict mesh mTLS; kind retains a lightweight disposable database-TLS/credential profile without permitting plaintext east-west mesh transport.
- Timer deadlines are durable in Postgres, not Redis.
- Non-terminal game completion writes `next_game_continuations` in the same Room transaction. Existing Room timer workers claim bounded Postgres batches with visibility leases and deterministic identities; successful next-game commit deletes the row, while expired leases and retryable failures remain recoverable without client redelivery.
- Redis can accelerate dispatch, but it cannot define room truth.
- Redis scheduling uses sorted-set buckets partitioned by stable `roomId` hash and timer family. Lua claims move due entries into visibility-leased in-flight sets; workers acknowledge success/stale outcomes and a reaper retries expired leases.
- Redis scheduling indexes are rebuilt from open Postgres deadlines after loss. Rebuild and ordinary duplicate delivery use the same stable timer identities and Room-side revalidation.
- Room Postgres snapshots, deadlines, dedupe state, and outboxes recover through context PITR; Redis timers and feeds are rebuilt/reconciled after restore.
- Only the `room-timer` role rebuilds the Redis timer index, guarded by a context-wide rebuild lease. Each timer worker snapshots a Postgres-owned completion generation before claiming; successful completion increments that generation atomically, so sibling workers observe the completed cohort without comparing pod clocks. Only bounded `room-integrity-reconciler` replicas claim Game Integrity repair markers with `FOR UPDATE SKIP LOCKED`; dedicated room runtimes and routers perform neither global operation.
- The outbox bridges committed room state to downstream consumers.
- Room Gameplay owns one database connection pool, one Kafka-bound Debezium connector, and one separate Redis-bound Debezium Server pipeline for its context-owned Postgres database.
- Room's integration and realtime outboxes rotate and reclaim independently. Each sealed partition requires proof from its own pipeline offset; progress in one pipeline cannot authorize cleanup in the other.
- An unavailable Room Gameplay database fails the bounded context closed rather than rerouting or recreating authoritative state elsewhere.
- The same room transaction writes player-safe entries to a separate realtime outbox. A Debezium Server Redis sink routes those rows to per-room player-feed streams, with duplicate-safe event identities and sequences.
- Consistency is strong inside one room aggregate: command dedupe, sequence-number checks, hand mutation, turn advancement, match score, timer deadline writes, and outbox rows are committed in the same room transaction after Game Integrity append confirmation.
- Existing Room mutations use `READ COMMITTED` with `SELECT ... FOR UPDATE` on the aggregate row. The lock is retained through Game Integrity confirmation and the local commit; all Room-owned child rows follow a consistent lock order.
- Lock/statement/internal-call deadlines are bounded. Game Integrity failure rolls back the transaction, while append-confirmed/local-commit-failed recovery reconciles from KurrentDB.
- When an uncertain append replays with no matching event at exactly the persisted expected revision, reconciliation safely re-appends that exact deterministic event before normal finalize/repair. Any replay error, identity drift, or advanced revision stays fail-closed.
- Read models include the player feed and reconnect snapshot. Players may read their own private hand state plus public room state; spectators must use Spectator View instead.
- Public room listing (BFF `GET /v1/rooms` → Room `GET /internal/v1/rooms/public-list`) is a bounded keyset read over authoritative `rooms` snapshot columns (`visibility='public'`, status filter, `room_id ASC`) using `rooms_public_list_idx`; never OFFSET/full scan. Opaque HMAC cursor (`ROOM_PUBLIC_LIST_CURSOR_SECRET`, API-only) is bound to the status filter; default 50 / max 100 with one-row lookahead.
- Retention keeps operational snapshots until the room/match can no longer affect reconnect, tournament advancement, rating, or dispute windows; immutable committed gameplay/integrity history remains in Game Integrity. Structured rejected-command audit records live outside Game Integrity in operational/security observability.
- Context-owned recovery APIs (ADR-0039, implemented): `GET /internal/v1/rooms/{roomId}/spectator-recovery-snapshot` exports sanitized public state + `resumeCheckpoint`; `POST /internal/v1/rooms/analytics-backfill` pages read-only append-only integration-outbox rows (HMAC cursor; default 100 / max 1000) for Analytics-consumed Room topics. Neither path polls or mutates outbox publish state.

## Game Integrity

**Authoritative store**

- KurrentDB 26.0.3 LTS as the append-only per-room or per-game technical log

**Supporting APIs**

- Internal audit export
- Internal replay API

**Persistence rules**

- Appends are immutable.
- Streams are created lazily by the official Go adapter on first append; every append supplies the exact expected revision. Startup/readiness validates KurrentDB policy, credentials, transport security, and key-provider access rather than running schema DDL.
- A first-append deadline, connection, internal-server, aborted, or unavailable response is an unknown outcome, not permission to replay the write. Game Integrity reads, authenticates, decrypts, and compares the committed first event: exact logical identity and plaintext confirms success, a different candidate with the deterministic event UUID returns `conflicting_duplicate`, and absent/unreadable/tampered state preserves the original error and fails closed.
- Replay-sensitive event payloads are AES-256-GCM ciphertext under a per-game data key; KurrentDB retains readable event/stream/revision metadata, key version/nonce, and seed/order commitments only.
- Replay is deterministic from the log.
- The store is not a cache and should not be treated like one.
- Consistency is strong per room/game stream by expected revision; stale or replayed append attempts are rejected.
- The immutable game log holds committed gameplay/integrity history only and is retained for dispute resolution, replay verification, tournament audit, and integrity investigations. Structured rejected-command audit records are not Game Integrity entries; they live in operational/security observability.
- Authorized operators, automated replay jobs, and compliance/audit tooling may query or export the log through internal-only APIs guarded by service credentials, role checks, correlation logging, and break-glass procedures for exceptional access.
- Decryption additionally requires scoped access to the versioned key-encryption provider and emits a durable decrypt/export audit record. Storage/backup access without key-provider authorization yields ciphertext only.
- Wrapping-key rotation preserves immutable events and historical decryptability. Loss of required historical key material is treated as permanent audit-data loss, not a reason to bypass encryption.
- KurrentDB bytes and historical wrapping-key access are restored and verified together; either one alone is an incomplete Game Integrity recovery.
- Spectator View, Analytics, and public clients cannot query raw Game Integrity logs and must consume sanitized/public streams.

## Tournament Orchestration

**Authoritative store**

- Postgres for tournament state, registration, round progress, and advancement records
- Durable production path: context-owned pgxpool writer, exact schema/bootstrap readiness, transactional aggregate commit (authoritative rows + command idempotency + append-only outbox when a contracted integration fact is emitted) under tournament-row `FOR UPDATE` / create advisory lock. Outbox is Debezium CDC input only — the durable runtime never polls or marks rows. Explicit `CloseRegistration` and capacity-triggered auto-close atomically create the deterministic round-1 seeding job. `WORKER_ROLE=tournament-seeding` chunks/finalizes every round without whole-aggregate rewrite; each finalized round schedules bounded provisioning work. `WORKER_ROLE=tournament-provisioning` creates the Room assignments, and Tournament-owned completion processing atomically schedules the next round or completes the final round and emits `TournamentCompleted`. These worker Deployments are kind-enabled and disabled by default in staging/production.

**Supporting store**

- Redis for the implemented bracket projection and bounded fast reads

**Persistence rules**

- Tournament advancement state is durable in Postgres.
- Sharded all-round seeding, provisioning, and completion workers can scale independently from the read projection (worker Deployments disabled outside kind until ops enable them).
- Public BracketPage excludes pending seeding rounds/slots until finalization; projectionVersion bumps once on seeding completion.
- Tournament processing is async around `MatchCompleted`. Durable franz-go consumption of `room.match.completed` is implemented when `KAFKA_BROKERS` is set.
- Consistency is strong per tournament, round, and slot assignment. Async room results are deduped before they update advancement state.
- Tournament result/advancement changes, `(roomId, completionVersion)` dedupe, and the tournament/slot checkpoint commit in one Postgres transaction before the Kafka offset.
- Redis bracket projections are read models only; they are rebuilt from Postgres tournament state and published tournament events. Physical values are chunked by round/provisioning batch, and public reads return only a 100-slot default / 1,000-slot maximum live-keyset page plus compact summary metadata. Stable round/slot cursor identity survives state updates; each page reports its projection version/time.
- Public standings reads are Postgres one-statement snapshots (`registeredCount` = SUM of 64 registration shards; `finalStandings` from recorded final-round `ranked_result`, max 10) and never hydrate the whole tournament aggregate.
- Contracted tournament lifecycle, result, and advancement outbox rows are routed to Kafka through the Tournament database's Debezium connector. `CreateTournament` intentionally creates authoritative state without an integration-outbox row; publication begins with the contracted downstream integration facts. Kind Connect delivery is live-proven.
- Tournament outbox partitions use the connector-offset reclamation gate; an end-of-round burst cannot be age-deleted while connector progress is unproven.
- Retention keeps registration, room-slot assignments, match results, advancement decisions, tie-break inputs, and final standings for audit and bracket reconstruction.
- Context-owned Analytics backfill (ADR-0039, implemented): `POST /internal/v1/tournaments/analytics-backfill` pages read-only append-only outbox rows (HMAC cursor; default 100 / max 1000) for Analytics-consumed Tournament topics; never mutates outbox publish state.

## Ranking

**Authoritative store**

- Postgres for rating history and authoritative ranking records
- Durable production adapter (`services/ranking/src/store`) commits rating mutations, idempotency keys, stable responses, and append-only outbox rows in one writer transaction; Redis is not required for authoritative correctness
- Durable multi-topic franz-go consumers for `room.game.completed`, `tournament.players.advanced`, and `tournament.completed` (per-source Ranking DLQs) are implemented when `KAFKA_BROKERS` is set

**Cache**

- Redis for implemented leaderboard reads (non-authoritative and rebuildable)

**Persistence rules**

- Rank updates are derived from authoritative game or tournament results.
- Tournament-placement updates consume `PlayersAdvanced` as advancement-depth facts and `TournamentCompleted.finalStandings` as final-placement facts. Tournament never supplies a rating delta; Ranking calculates it.
- Tournament Placement Rating starts at zero, never decreases, and does not reset between tournaments. Seasonal or decaying views, if introduced later, are separate projections and cannot rewrite lifetime achievement history.
- Advancement awards are fixed at 10 points per accepted `PlayersAdvanced` event. `roundNumber` is stored in history for traceability, while replay-safe accumulation across successive rounds represents advancement depth.
- Final-placement awards are top-heavy and apply once from ordered `TournamentCompleted.finalStandings`; they supplement rather than replace previously earned advancement points.
- Ranking maps final places first through tenth to `100, 70, 50, 35, 25, 20, 15, 10, 5, 0` points. A smaller final uses the prefix of the same stable table.
- A zero-point final standing still commits idempotency, history, stable outcome, and a zero-delta rating-update outbox row. It does not by itself publish a leaderboard snapshot because the stored score is unchanged.
- A tournament-performance fact is one bounded multi-player Postgres transaction (at most three advancement players or ten finalists). Stable player-ID lock ordering covers every affected aggregate; ratings, history, idempotency, stable outcome, and outbox rows commit together before the Kafka offset.
- The same transaction persists the AsyncAPI event key plus immutable payload fingerprint: `(tournamentId, roundNumber, sourceSlotId)` for advancement and `eventId` for completion. Exact duplicates reuse the stable outcome; conflicting reuse is event-local DLQ failure before any player mutation.
- Tournament awards are additive and commutative. A failed event waits in its consumer-owned DLQ without quarantining the whole tournament; later valid facts may apply, and eventual replay of the repaired event converges to the same total.
- The cache can be rebuilt.
- Ranking writes should not block gameplay.
- Consistency is eventual relative to Room Gameplay and Tournament Orchestration. Each rating application is idempotent by upstream event/player keys.
- Rating history, the contract business key, and the applicable player/game or placement checkpoint commit together before the Kafka offset. Tournament performance uses `(playerId, tournamentId, placementEventId)`, with `placementEventId` equal to the upstream tournament event `eventId`.
- Read models include a complete incrementally maintained Redis sorted-set leaderboard and rating-history queries; stale leaderboards are acceptable while authoritative Postgres rating history catches up. Public reads are live keyset pages of 100 entries by default and at most 500, ordered from the cursor's `(rating, playerId)` boundary. Each page reports projection version/time; cross-page version drift is visible but does not require frozen million-player Redis generations. Published leaderboard snapshots are bounded top-100 views, never million-entry event payloads. Authoritative HTTP leaderboard/history fallback reads are served from Postgres.
- Score-changing transactions advance a durable dirty version for the affected board. A Ranking-owned snapshot worker claims unpublished versions and emits no more than one coalesced top-100 snapshot per board every 15 seconds; its published-version checkpoint commits with the snapshot outbox row. Zero-delta tournament facts leave the dirty version unchanged.
- Rating update and leaderboard-snapshot outbox rows are routed through the Ranking database's Debezium connector; kind Connect delivery is live-proven.
- Ranking outbox partitions use the connector-offset reclamation gate and remain separate from authoritative rating history retention.
- Retention keeps rating history and upstream event references so duplicate or corrected inputs can be audited without recomputing from lossy aggregates.
- Context-owned Analytics backfill (ADR-0039, implemented): `POST /internal/v1/rankings/analytics-backfill` pages read-only append-only outbox rows (HMAC cursor; default 100 / max 1000) for `ranking.player_rating_updated` (requires `projectionVersion`) and `ranking.leaderboard_snapshot_published`; never mutates outbox publish state.

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
- Projection state, upstream dedupe/sequence state, and the outgoing spectator stream entry are updated atomically in Redis so the published feed never outruns the privacy-filtered projection.
- Consistency is eventual and versioned by room sequence. The projection may lag briefly but must never include private hand, hidden deck, or player-only action data.
- Read models are room spectator snapshots and incremental SSE payloads served through the BFF.
- Projection retention follows active-room and replay needs; lost Redis state is recovered by replaying safe events and sanitized snapshots, not by querying private stores.
- Post-retention/quarantine rebuild (ADR-0039, implemented and live-proven in kind): Spectator consumes `spectator.projection.rebuild_requested` keyed by `roomId`, fetches Room's spectator-recovery-snapshot with `ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL`, CAS/fences Redis generation-swap against current room sequence/generation (newer live apply wins), records `(recoveryJobId, roomId, failedCheckpoint)` idempotency and releases quarantine atomically only after continuity through held post-gap records is proven.
- **Durable adapter status:** Spectator Redis projection + Redis-backed spectator SSE are implemented when `REDIS_URL` plus scoped `SPECTATOR_VIEW_INTERNAL_CREDENTIAL` are set (`/ready` pings Redis; dedicated key prefix; Lua CAS apply/rebuild). Capability memory + process-local StreamHub remain behind explicit non-prod `SPECTATOR_CAPABILITY_MODE`. Durable Kafka consumer for `room.spectator-safe.events` is implemented when `KAFKA_BROKERS` is set; HTTP internal ingest is a test/ops bridge only and is not the production input path. Projection-rebuilder worker binary and rebuild-request admission are implemented; `projectionRebuilder.enabled=true` only in kind after its live proof, and false in default/staging/production.

## Analytics and Public Read Models

**Authoritative store**

- ClickHouse

**Inputs**

- sanitized/public events
- `room.gameplay.metrics`
- public tournament metrics

**Persistence rules**

- Analytics is derived and non-authoritative.
- An Analytics-owned ClickHouse bootstrap Job creates tables, materialized views, retention policies, and the exact schema-version marker with a DDL credential unavailable to runtime pods.
- Empty and exact-current ClickHouse states are accepted; any other non-empty state fails unchanged until a future stateful-upgrade decision exists.
- **Durable adapter status:** Analytics ClickHouse HTTP/store is implemented when `CLICKHOUSE_URL` plus scoped runtime credentials are set (`/ready` verifies schema; generation-based rebuild). Capability memory remains behind explicit non-prod `ANALYTICS_CAPABILITY_MODE`. Durable Kafka ingestion of the nine configured AsyncAPI topics is implemented when `KAFKA_BROKERS` is set. Projection-rebuilder worker binary, rebuild-request admission, and durable ClickHouse recovery (generation/lease/checkpoint, dual-write, server-side clone) are implemented; `projectionRebuilder.enabled=true` only in kind after its live proof, and false in default/staging/production.
- Encrypted daily ClickHouse backup targets RPO ≤24 hours/RTO ≤4 hours; safe retained events and context backfills repair derived intervals beyond the backup point.
- The model can be rebuilt or backfilled from safe upstream facts.
- Post-retention/quarantine rebuild (ADR-0039, implemented and live-proven in kind): Analytics consumes `analytics.projection.rebuild_requested` keyed by `recoveryJobId`, pages Room/Tournament/Ranking analytics-backfill APIs (producer-owned HMAC cursor; paired range; producer default 100 / max 1000; worker default page 1000), applies under a durable ClickHouse-visible rebuilding generation/lease/checkpoint so live ingest dual-writes into the fenced generation, and claims continuity only after every requested page/checkpoint is reconciled. Process-local mutex alone is insufficient. ClickHouse non-transactional projection-before-marker check-then-act remains; same-generation redelivery is idempotent via FINAL processed markers.
- More granular public tournament metrics are allowed than ad-hoc anonymized analytics, as long as they remain public and non-authoritative.
- Consistency is eventual and optimized for ingestion/query throughput, not transactional decisions.
- Analytics applies the declared event/business key through its versioned ClickHouse deduplication/replacement strategy before acknowledging the source offset; conflicting duplicates are quarantined rather than counted twice.
- Read models include public aggregate metrics, public tournament reporting, and anonymized ad-hoc gameplay statistics.
- Retention is analytics-oriented and may aggregate or anonymize older ad-hoc data; it must not become the audit store for gameplay, tournaments, ratings, or privacy enforcement.

## Storage Summary

| Context | Authoritative | Cache / Index | Notes |
| --- | --- | --- | --- |
| Identity | One context-owned HA Postgres database cluster | Redis | Identity/session/ACL truth stays inside the Identity persistence boundary; authoritative validation uses the writer endpoint |
| Room Gameplay | One context-owned HA Postgres database cluster | Redis, integration/realtime outboxes | Room truth stays inside the Room Gameplay persistence boundary; Redis is non-authoritative |
| Game Integrity | KurrentDB 26.0.3 LTS | none | Append-only by design; deployment selects reviewed AMD64/ARM64 digests, and the experimental ARM64 image is local-only |
| Tournament Orchestration | One context-owned HA Postgres database cluster | Redis | Redis holds bracket projection |
| Ranking | One context-owned HA Postgres database cluster | Redis | Async updates, cacheable reads |
| Spectator View | Redis projection | none | Rebuilt from safe facts; not source of domain truth |
| Analytics and Public Read Models | ClickHouse | none | Derived reporting only |
