# Design Changelog

This changelog records design-package updates made while shaping the architecture checkpoint. It is limited to changes that affect traceability between the original design deliverables and the architecture.

## Runtime Routing and Client Parity Contract (2026-07-13)

- Stale room-sequence command outcomes use HTTP `409 Conflict` while preserving the structured rejected command result. Accepted commands and non-stale domain rejections remain HTTP `200`.
- ADR-0042 defines event-driven tournament assignment readiness: Room atomically emits `RoomRuntimeReady` on first runtime readiness, Tournament withholds `roomId` until consuming it, and later runtime replacement uses bounded client `room_starting` retry without revoking the assignment.
- ADR-0043 defines authorization-first Room routing: the stable boundary validates the original route-specific caller and BFF-derived player principal before pod lookup, then upgrades only authorized traffic to the scoped router credential and pinned generation.
- The CLI uses a configurable 60-second default budget for safe `room_starting` reads, honoring `Retry-After` with jitter and a five-second interval cap while never retrying mutations after unknown outcomes.
- ADR-0044 separates Room maintenance roles: dedicated runtimes and routers perform no global scans; the timer worker rebuilds under one context lease, and bounded integrity-reconciler replicas claim repair work with `FOR UPDATE SKIP LOCKED`.

## Client Parity and Dedicated Room Runtime Topology (2026-07-13)

- Client playability and bot selection now apply the Room contract during a pending penalty: only an ordinarily legal `Draw Two`, or a `Wild Draw Four` when the hand contains no non-wild active-color card, is selectable. The server remains authoritative.
- `POST /v1/auth/logout` now authoritatively invalidates the UnoArena session through Identity's existing `SessionInvalidated(reason=logout)` outbox path. The CLI removes its mode-0600 session file only after success and retains it on network/upstream failure.
- ADR-0040 defines one lifecycle-bound bare Kubernetes state-machine Pod per active Room, a stable pod-IP router, Room-owned leaderless controllers, generation fencing, bounded mutation admission, separate service accounts, and terminal teardown after authoritative commit.
- ADR-0041 defines Room-owned PgBouncer transaction pooling and two lazy `max=1` pools per dedicated runtime: one for the aggregate transaction and one for the pre-append crash-recovery intent. The Helm/kind topology deploys two controller replicas, PgBouncer, scoped RBAC, and runtime pods without Kubernetes API credentials.
- The deployment contract keeps controller secret resolution to its Room database URL and passes only kubelet-resolved SecretKeyRef names to dedicated runtimes. Controller admission and PgBouncer client/backend capacity are environment-configurable; production overlays carry the 100,000-room starting budgets while kind stays intentionally small.
- Staging/production Room renders require PgBouncer SCRAM-SHA-256, client/server database TLS material, and workload-scoped strict Istio mTLS. The disposable kind overlay is the explicit local plaintext exception.
- Room Postgres now atomically persists desired/observed runtime assignments with the aggregate lifecycle. Existing-room traffic returns retryable `503 room_starting` until the current generation is Ready; queue saturation returns `429 room_busy`.
- Live clean-kind acceptance passed on ARM64: two rooms produced distinct Ready pods; deleting a Ready pod advanced its durable generation and produced a fenced replacement; stale-generation admission returned `401`; an existing-room `JoinRoom` traversed the router and committed in the assigned pod; Spectator recovery, CDC, durable adapters, and connector probes remained green. CLI logout invalidated a copied token (`401`) and persisted `SessionInvalidated(reason=logout)`.
- Status: client parity, authoritative logout, durable runtime assignment/fencing, router/controller/runtime roles, PgBouncer, and Helm/kind resources are implemented and live-proven in the disposable cluster.

## Clean ARM64 Kind Deployment and Recovery Acceptance (2026-07-12)

- The clean-deploy aggregate lane now validates, rebuilds the disposable cluster, builds/loads the bootstrap plus all eight service images, applies the foundation, deploys services in dependency order, and runs the live probe suite. The ARM64 foundation and all eight services have deployed successfully.
- Debezium Kafka Connect and Debezium Server CDC ran live. The current 3.6.0.Final exact ARM64 digests were rotated after the upstream registry republished its tags; node-native staging validates architecture and digest instead of trusting mutable tags or stale kind-import metadata.
- Removed the invalid Outbox Event Router `occurred_at` timestamp override. Canonical `occurredAt` is carried inside the JSON event payload; the standardized relational outbox has no separate timestamp column to map.
- `CreateTournament` intentionally creates authoritative tournament state without an integration-outbox event. Lifecycle publication begins at the contracted integration facts; no synthetic create topic/event was introduced.
- Spectator Kafka→Room recovery snapshot→Redis generation-swap and Analytics Kafka→producer-owned backfill→ClickHouse generation/lease recovery passed as live worker processes. Their `projectionRebuilder` Deployments are now enabled only in `values.kind.yaml`; default, staging, and production remain disabled.
- Local kind Identity deliberately enables a context-owned password test-account stub for dynamic CLI `register`/`seed`. Staging and production remain OIDC-only.
- Game Integrity fixed the first-write per-game DEK race across replicas. After Kurrent acknowledges an initial append, the repository anchors to the committed wrapper, decrypts the committed event, and compares its complete logical identity and exact plaintext with the candidate; exact retries remain idempotent while same-event-ID conflicts return `conflicting_duplicate`.
- Client Checkpoint Stage B interactive play, headless casual/room/tournament bot, JSONL, and Docker packaging are implemented; the fake-BFF suite passes 174+ checks. Live kind acceptance completed both a casual best-of-three and a tournament from registration through assignment, Room play, result consumption, lifecycle advancement, and terminal `TournamentCompleted`.
- Tournament lifecycle closure is now context-owned end to end: explicit `CloseRegistration` and capacity auto-close atomically create round 1; every seeding finalization schedules bounded provisioning; every non-final `CompleteRound` creates the next-round seeding job; final `CompleteRound` atomically completes the tournament and emits `TournamentCompleted`. The public client catalog remains closed to `CreateTournament`, `RegisterPlayer`, and `CloseRegistration`; internal seeding/provisioning/completion commands were not exposed.
- Room's Helm chart now injects the scoped Game Integrity audit credential required by its replay/reconciliation path, distinct from its ordinary append credential.
- Room best-of-three advancement is crash-safe: non-terminal `GameCompleted` commits a deterministic Postgres continuation, the existing Room timer worker claims bounded `FOR UPDATE SKIP LOCKED` batches with expiring leases/backoff, and accepted `StartNextGame` deletes the continuation atomically. A live kind proof paused the worker, observed the queued row, restarted the worker, and completed the match.
- Game Integrity uncertain no-write recovery now re-appends only when authoritative replay proves no matching event at exactly the persisted expected revision; replay failure, identity mismatch, or advanced revision remains fail-closed.

## Authoritative Draw-Pile Size for Client Checkpoint Interactive Board (2026-07-12)

- Game Integrity — `ReserveDeal` / `ReserveDraw` responses include authoritative `remaining` (draw-pile size after that reservation confirms). Exact duplicate reserves return the same value; never derived from client input. Stored on pending/confirmed reservation state for durable replay.
- Room Gameplay — `HTTPDealSource` parses `remaining` into `MaterialReservation.DrawPileSize`; `StartGame`/deal initializes `Game.drawPileSize`; accepted draw/UNO-penalty application sets it from the reservation; cancelled/failed/rejected reservations leave it unchanged; idempotent retries do not double-decrement. Persisted/restored/cloned with validation `>= 0`. Player-private and public/spectator snapshots expose count-only `drawPileSize`.
- Spectator View — allowlist + projection/export carry `drawPileSize` so public spectator snapshots match OpenAPI.
- `contracts/openapi/bff-v1.yaml` — `PlayerRoomSnapshot.game.drawPileSize` and `SpectatorRoomSnapshot.drawPileSize`.
- Client Checkpoint — Interactive turn board (§5.C) can render draw-pile size from BFF snapshots/feeds without deck order/seed/card identities.
- Status: **implemented** end-to-end in GI + Room + Spectator projection and the Stage B interactive CLI.

## Bounded Public Room Listing for Client Checkpoint (2026-07-12)

- `contracts/openapi/bff-v1.yaml` — Added public `GET /v1/rooms` (default `status=waiting`, limit 50/max 100, opaque cursor) returning `PublicRoomListPage` summaries only.
- Room Gameplay — Internal `GET /internal/v1/rooms/public-list` authenticated with Gateway↔Room scoped credential; producer-owned HMAC cursor (`ROOM_PUBLIC_LIST_CURSOR_SECRET`, API-only, excluded from timer worker); durable Postgres keyset on `(status, room_id)` via `rooms_public_list_idx` (visibility=public partial); capability memory repository parity; one-row lookahead for `nextCursor`.
- Gateway — Proxies safe query/response, validates upstream shape/bounds, preserves Room 400/401, maps network/decode/malformed to 502; no player bearer required.
- Architecture docs (`02`, `03`, `04`), `client-checkpoint/README.md`, Helm/kind local secret wiring.
- Status: public room-list seam and CLI `room list` / `play --casual` wiring **implemented**.

## Context-Owned Snapshot/Backfill APIs and Kafka Rebuild-Request Topics (2026-07-12)

- `docs/adr/0039-context-owned-snapshot-backfill-and-kafka-rebuild-requests.md` — Settled Spectator/Analytics post-retention recovery: Kafka-carried bounded rebuild requests keyed by recovery aggregate/job identity, plus context-owned internal snapshot/backfill APIs. No outbox polling, no public BFF route, no cloud object store for local kind, and no generic cross-context query service. Accepted decision unchanged; implementation status recorded.
- Room/Tournament/Ranking — Implemented `POST .../analytics-backfill` (producer-owned HMAC cursor; paired bounded range; page default 100 / hard max 1000; read-only append-only outbox pages). Room also serves `GET /internal/v1/rooms/{roomId}/spectator-recovery-snapshot` for Spectator recovery.
- Spectator — Implemented projection-rebuilder path: consume `spectator.projection.rebuild_requested`, fetch Room snapshot with scoped recovery credential, Redis Lua CAS/fenced generation-swap with atomic `(recoveryJobId, roomId, failedCheckpoint)` idempotency and fenced quarantine release after held-record continuity (max 1000).
- Analytics — Implemented durable ClickHouse recovery worker: consume `analytics.projection.rebuild_requested` with `(recoveryJobId, sourceTopic, pageCursor)` idempotency, page producer backfills (worker default page 1000), deterministic generation/lease readback, active/building dual-write, server-side clone; acknowledges non-transactional projection-before-marker check-then-act with same-generation FINAL idempotency.
- Helm — Live kind worker proofs passed; `projectionRebuilder.enabled=true` only in kind, while default/staging/production remain false.
- `contracts/asyncapi/kafka-v1.yaml` — Rebuild-request channels/schemas + worker-owned DLQ naming; `PlayerRatingUpdated.projectionVersion` required; Analytics rebuild requests document paired-range and pageCursor idempotency requirements.
- Architecture/consistency docs and ADR-0013 — Reconciled endpoints, credentials ownership, generation/lease/page chain, and kind-only enabled status.
- Status: context-owned backfill/snapshot APIs and Spectator/Analytics rebuilder workers **implemented and live-proven in kind**; default/staging/production Deployments remain disabled.

## Tournament Durable All-Round Lifecycle (2026-07-11, completed 2026-07-12)

- `services/tournament-orchestration/**` — Explicit close and capacity auto-close atomically schedule round 1. Durable seeding jobs use an immutable N/S/base/rem plan; `WORKER_ROLE=tournament-seeding` chunks players, inserts pending bracket slots, and finalizes each round exactly once. Finalization schedules bounded provisioning; non-final completion schedules the next round; final completion atomically marks the tournament completed and emits `TournamentCompleted`. Partial pending rounds/slots stay invisible on public BracketPage. `TournamentRoundSeeded` remains internal-only (no Kafka channel).
- Helm: Tournament seeding, provisioning, and completion workers are disabled by default/staging/production; kind enables 2 replicas of each. ADR-0013 updated.
- Status: durable registration closure, all-round seeding/provisioning, round completion, and tournament completion are **implemented**; the terminal tournament lifecycle is additionally **live-proven in kind** through the Stage B tournament bot flow.

## Bounded Leaderboard and Bracket Projections (2026-07-11)

- `docs/adr/0038-bounded-leaderboard-and-bracket-projections.md` — Rejected whole-population read responses and leaderboard event payloads for the one-million-user target.
- Public leaderboard reads use opaque cursors with 100 entries by default and 500 maximum; Ranking retains the complete board in an incremental Redis sorted set, while `LeaderboardSnapshotPublished` is a bounded ordered top-100 view.
- Score-changing transactions mark a durable board version dirty; a Ranking-owned worker publishes at most one coalesced top-100 snapshot per board every 15 seconds with a transactional published-version checkpoint. Zero-delta facts do not dirty the board.
- Public tournament bracket reads return compact summary metadata plus 100 slots by default and at most 1,000; Tournament Redis projection values are chunked by round/provisioning batch instead of one tournament-sized document.
- Public tournament standings reads return a compact projection (`finalStandings` ≤10) from Tournament Postgres without whole-aggregate hydration; Gateway proxies bracket/standings as the sole public boundary. Private-tournament visibility enforcement (participant/operator for private tournaments) remains an architecture gap and is not claimed as implemented.
- Leaderboard and bracket pagination use producer-owned live keyset cursors rather than frozen full-dataset Redis generations. Pages expose `projectionVersion` and `generatedAt`; each page is internally consistent, while cross-page drift is visible and may be restarted by clients that require one visual point in time.
- `contracts/openapi/bff-v1.yaml`, `contracts/asyncapi/kafka-v1.yaml`, glossary, command catalog, and architecture persistence/context docs now carry the same bounds and 15-second change-driven cadence.

## Tournament Final Standings + Ranking Multi-Topic Kafka Ingestion (2026-07-11)

- `services/tournament-orchestration/**` — `TournamentCompleted` outbox/Kafka envelope now publishes ordered `finalStandings` (no `championId`); HTTP/read champion remains derived from `finalStandings[0]`. Durable franz-go consumer for `room.match.completed` is kind-wired when `KAFKA_BROKERS` is set.
- `services/analytics/**` — Kafka parser accepts `tournament.completed.finalStandings` and the nine configured AsyncAPI source topics; durable multi-topic franz-go ingestion is kind-wired when `KAFKA_BROKERS` is set.
- `services/ranking/**` — ADR-0037 award policy (`+10` advancement; top-heavy final-placement table) is enforced in domain; durable multi-topic franz-go consumers ingest `room.game.completed`, `tournament.players.advanced`, and `tournament.completed` with per-source Ranking DLQs; tournament facts apply event-wide atomically with event-local failure isolation (no tournament-wide quarantine). HTTP tournament-placement paths reject caller-supplied `delta`.
- `services/gateway/**`, `services/spectator-view/**` — Durable franz-go consumers for `identity.session.invalidated` and `room.spectator-safe.events` are kind-wired when `KAFKA_BROKERS` is set; Gateway Redis SI admission supports cross-replica rejection after durable apply.
- `contracts/asyncapi/kafka-v1.yaml`, `docs/architecture/**` — Removed the stale AsyncAPI “Kafka remains unwired” claim; architecture status records implemented Kafka consumers and Debezium Connect/Server wiring. Ranking/Tournament Redis projections and the clean live lane landed later.
- Status: Tournament `finalStandings` producer, Analytics parsers, Ranking policy + multi-topic durable ingestion, HTTP `delta` rejection, declared Kafka consumers, and subsequent Redis projections are **implemented**; the clean kind lane passed.

## Tournament Rating Is Cumulative Achievement (2026-07-11)

- `docs/adr/0037-tournament-rating-is-cumulative-achievement.md` — Defined Tournament Placement Rating as a lifetime, monotonically non-decreasing achievement score rather than a second Elo system.
- Ranking may award only non-negative points from authoritative tournament-performance facts; poor finishes never erase prior achievement, the score does not reset between tournaments, and replay/idempotency prevents duplicate awards.
- Each accepted `PlayersAdvanced` fact contributes 10 points per listed player. `roundNumber` is retained for history but does not multiply the award, avoiding quadratic growth across deep tournaments.
- `TournamentCompleted.finalStandings` adds a top-heavy final-placement bonus so champion and podium results remain materially distinct after advancement has already rewarded reaching the final room.
- Final places first through tenth award `100, 70, 50, 35, 25, 20, 15, 10, 5, 0` points; smaller finals use the applicable prefix.
- Tenth-place `+0` remains an accepted, idempotent history result and emits a zero-delta rating fact, while unchanged scores do not trigger leaderboard snapshots.
- Seasonal, decaying, or current-form leaderboards remain separate future projections and cannot silently reinterpret lifetime tournament history.

## Tournament Performance Facts Feed Ranking (2026-07-11)

- `docs/adr/0036-tournament-performance-facts-feed-ranking.md` — Settled the Tournament Orchestration → Ranking published language: `PlayersAdvanced.roundNumber` supplies advancement depth, `TournamentCompleted.finalStandings` supplies the complete ordered final result, and the upstream event `eventId` is each affected player's `placementEventId`.
- `contracts/asyncapi/kafka-v1.yaml`, domain glossary, command catalog, and architecture communication/persistence docs — Made Ranking a consumer of both tournament performance facts and kept the rating delta exclusively Ranking-owned. Tournament Orchestration publishes facts, never rating adjustments.
- `TournamentCompleted.finalStandings[0]` is the sole champion definition; the redundant `championId` field was removed so completion facts cannot express contradictory winners.
- `PlayersAdvanced.advancingPlayerIds` is constrained to 1–3 unique players: normally three, with fewer permitted only for authoritative undersized/forfeit outcomes.
- Each tournament-performance fact applies atomically across all affected Ranking player aggregates under stable player-ID lock ordering; rating/history/idempotency/outbox changes cannot partially commit.
- Ranking persists the declared source-event business key and payload fingerprint in that transaction, preventing a new event ID or changed player list from repeating an award under the same contract key.
- Failure recovery is event-local: additive, commutative awards permit later valid facts while one unchanged envelope waits in its Ranking DLQ, so no tournament-wide quarantine is introduced.
- Ranking's numerical award policy is recorded separately in ADR-0037 so the published fact contract remains independent of rating weights.

## Phase 1 Durable Delivery Plane — Wire Values + Debezium Infra (2026-07-11)

- `services/room-gameplay/**` — Spectator-safe integration outbox `payload` is now the canonical AsyncAPI `SpectatorSafeEvent` JSON envelope (`schemaVersion`, `eventId`, `eventType`, `roomId`, `sequenceNumber`, `correlationId`, `causationId`, `occurredAt`, nested privacy-safe `payload`, optional top-level `unoWindow`). HTTP bridge `spectatorCanonicalBody` continues to unwrap nested `payload`/`data`.
- `services/tournament-orchestration/**` — Durable outbox payloads are canonical AsyncAPI envelopes for contract topics only (`tournament.match.assigned`, `tournament.match.result_recorded`, `tournament.players.advanced`, `tournament.round.completed`, `tournament.completed`). Non-contract facts stay internal and create **no** outbox row; undocumented `tournament.lifecycle` is no longer emitted.
- `services/analytics/**` — Added `SourceTopic` + policy bindings for declared consumers `room.match.completed` and `tournament.completed` → `TournamentStatistic`.
- `infrastructure/kind/manifests/80-debezium/**` — Debezium Kafka Connect **3.6.0.Final** (ARM64 digest-pinned) Deployment/Service + idempotent register Job for exactly four PostgreSQL Outbox Event Router connectors (identity, room integration, tournament, ranking). Existing publications/CDC users; Debezium-owned unique slots; pgoutput; publication autocreate disabled; snapshot never; schema-disabled JSON; no Heartbeat SMT.
- `infrastructure/kind/manifests/80-debezium-server/**` — Separate Debezium Server **3.6.0.Final** for Room realtime only (`room_cdc_realtime` / `realtime_outbox_events` → Redis DB2 extended format with unique offset key and `target_stream` routing).
- `services/gateway/src/bff/streams_redis.go` — RedisLiveFeed dual-decode: flat test form + Debezium Redis extended value JSON; unwraps outbox envelope into SSE data; retains opaque Redis IDs, audience isolation, sequence/dedupe, bounded replay, malformed fail-safe.
- Kind apply/wait/validate, image-load scripts, structure/status scripts, and explicit live Postgres→Kafka / Postgres→Redis probes (Makefile targets). Franz-go consumers were out of Phase 1 scope and landed in a later slice.
- Status: Debezium Connect + Server infrastructure, wire values, declared Kafka consumers, and live clean-cluster CDC proofs are **implemented**. Structure/status checks alone remain non-delivery evidence.

## Gateway Durable Redis Rate-Limit + LiveFeed SSE (2026-07-11)

- `services/gateway/**` — Implemented distributed Redis rate limiting (fail-closed 503 on adapter failure; 429 only on quota exhaustion) and direct Redis Streams LiveFeed consumption for player (`room:{roomId}:player`) and spectator (`spectator:v1:room:{roomId}:stream:{generation}`) SSE. Deep `LiveFeed`/`LiveSession` seam with Hub adapter for fakes/capability; Hub remains process-local control-state until durable Redis SI + Kafka `identity.session.invalidated` apply (cross-replica admission rejection after durable SI apply). Physical Redis stream IDs are opaque SSE resume markers; player resume markers must belong to the same `playerId`/`sessionId`. Per-subscription `eventId`/`sequence` dedupe and positive monotonically increasing sequence (gaps allowed); bounded `XRangeN` replay aligned with Redis stream history (overflow → `409 snapshot_required`). Empty `Last-Event-ID` live-tails only; unknown/trimmed/wrong-audience markers return `409 snapshot_required`. Redis failures during `BeginSession` map to `503 live_feed_unavailable`. `/ready` pings every configured Redis client. Gateway now dual-decodes flat and Debezium extended Redis entries (Phase 1). Kafka `identity.session.invalidated` consumer is kind-wired when `KAFKA_BROKERS` is set.
- `services/gateway/helm/gateway/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `values.schema.json`, `helm-test.sh`), kind deploy/port-forward/integration/adapter scripts, Makefile targets — Helm-only Gateway deploy for kind (ClusterIP; Redis DB 6 rate-limit, DB 2 player feeds, DB 5 spectator; `uno-arena-local-credentials`; no capability/fakes flags). Integration suite uses isolated Redis DBs 11/12/13.
- `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md` — Recorded Gateway Redis durable slice; Kafka SessionInvalidated status updated with later consumer slice.
- Status: Gateway durable Redis rate-limit + LiveFeed SSE **implemented** (including Debezium extended decode); Kafka SessionInvalidated consumer **implemented**; clean kind deployment passed.

## Kind Redis local AOF (same-pod restart) (2026-07-11)

- `infrastructure/kind/manifests/20-redis/redis.yaml` — Enabled local AOF
  (`appendonly yes`, `appendfsync everysec`, `dir /data`,
  `appenddirname appendonlydir`) with loading-aware readiness (`PONG`) while
  retaining disposable emptyDir. Redis is not authoritative; pod replace /
  kind reset may still lose data and require Kafka rebuild.
- `infrastructure/kind/scripts/test-redis-aof*.sh`, Makefile/`kind-validate`
  wiring, runbook/kind README — Structure checks plus live DB15 prefix
  acceptance (kill Redis PID 1; prefix-only cleanup). Spectator adapter
  wording clarifies pod-replacement survival ≠ Redis process durability.
- Status: same-pod Redis container restart durability via AOF **implemented**
  for kind; Redis remains disposable / rebuild-from-Kafka; Spectator Kafka
  consumer and live recovery lane are **implemented**.

## Spectator Durable Redis Projection + SSE (2026-07-11)

- `services/spectator-view/**` — Implemented durable Redis projection store (go-redis v9.14.1) with deep `SpectatorApplication` seam; capability memory retained behind explicit non-prod `SPECTATOR_CAPABILITY_MODE`. Exact domain export/restore (no fake-event rebuild), SHA-256 invite digests, Lua CAS apply (revision/sequence, outcome idempotency, single XADD per accepted sequence, terminal close signal) and generation-swap rebuild. Durable SSE reads Redis spectator streams (capability retains StreamHub). `/ready` pings Redis and requires scoped internal credential. HTTP internal ingest remains a test/ops bridge; Kafka consumer for `room.spectator-safe.events` is kind-wired when `KAFKA_BROKERS` is set.
- `services/spectator-view/helm/spectator-view/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `values.schema.json`, `helm-test.sh`), kind deploy/port-forward/integration/adapter scripts, Makefile targets — Helm-only Spectator deploy for kind (ClusterIP, Redis DB 5). Integration suite uses Redis DB 14 with per-run random prefix cleanup.
- `docs/architecture/04-persistence-by-context.md`, changelog/runbook/README status wording — Recorded durable Redis projection + SSE implemented; Kafka consumer status updated with later slice.
- Status: Spectator durable Redis projection + SSE and Kafka consumer **implemented**; full worker recovery proof passed in kind.

## Analytics Durable ClickHouse HTTP Adapter (2026-07-11)

- `services/analytics/**` — Implemented durable stdlib-HTTP ClickHouse store (no driver). Deep `AnalyticsApplication` seam returns store errors for 503; capability memory retained behind explicit non-prod `ANALYTICS_CAPABILITY_MODE`. Domain validate/sanitize before durable write; projection rows before `processed_events` outcome marker; queries use `FINAL`; generation-based rebuild activates only after complete generation; `/ready` verifies ClickHouse schema. Safe `unoarena_analytics_test_<hex>` integration harness.
- `services/analytics/helm/analytics/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `values.schema.json`, `helm-test.sh`), kind secrets for scoped Analytics credentials, deploy/port-forward/integration/adapter scripts, Makefile targets — Helm-only Analytics deploy for kind (ClusterIP). Existing ClickHouse bootstrap Job remains context-owned.
- `docs/architecture/04-persistence-by-context.md`, changelog/runbook/README status wording — Recorded durable ClickHouse HTTP/store implemented; Kafka ingestion status updated with later slice.
- Status: Analytics durable ClickHouse adapter and nine-topic Kafka ingestion **implemented**; full worker recovery proof passed in kind.

## Offline-Verifiable CDC Prerequisites + Kafka Retention/DLQ Policy (2026-07-11)

- `infrastructure/kind/manifests/10-postgres/**` — Every kind Postgres Deployment sets `wal_level=logical` with bounded local `max_replication_slots` / `max_wal_senders` while retaining disposable `emptyDir`.
- `infrastructure/bootstrap/**`, `infrastructure/kind/manifests/01-local-secrets.yaml`, `70-bootstrap/job-postgres-*.yaml` — Per-context least-privilege `LOGIN REPLICATION` CDC roles/passwords, table-filtered publications, and SELECT-only outbox grants applied/verified by bootstrap. Room splits Kafka (`integration_outbox_events`) from realtime (`realtime_outbox_events`) with distinct roles/publications. No logical slots in bootstrap; Debezium owns slot lifecycle. Exact re-run verifies CDC without poisoning schema fingerprints.
- `infrastructure/kind/scripts/render-kafka-topics.rb` — Kind-short `retention.ms` by ADR-0032 class; ADR-0017 consumer-owned DLQ topic scaffolding; Connect internal topics remain compacted; partition immutability checks preserved.
- `services/room-gameplay/src/store/postgres_persist.go`, `app/player_feed_stream.go` — `realtime_outbox_events.target_stream` is the deterministic per-room key `room:{roomId}:player` (validated/injective); Redis timer keys unchanged.
- `services/ranking/src/store/postgres.go` — `topicCasualIngest` corrected to AsyncAPI `room.game.completed`.
- Status: **CDC prerequisites, Kafka retention/DLQ policy, Debezium Connect/Server, and declared consumers implemented**; live clean-cluster delivery proofs passed. Structure/status targets alone still make no delivery claim.

## Ranking Durable Postgres Adapter (2026-07-11)

- `services/ranking/**` — Implemented durable pgxpool Ranking store (exact schema/bootstrap readiness, multi-participant casual Elo under stable `FOR UPDATE` player-id order, tournament placement biz-key + eventId dedupe, stable response persistence, append-only CDC outbox). Migration outbox is append-only (no `published_at` / unpublished index / app polling). Handlers use a deep `RatingApplication` seam; memory capability path retained behind explicit non-prod `RANKING_CAPABILITY_MODE`.
- `services/ranking/helm/ranking/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `values.schema.json`, `helm-test.sh`), kind secret `ranking-database-url`, deploy/port-forward/integration/adapter scripts, Makefile targets — Helm-only Ranking deploy for kind (ClusterIP).
- `docs/architecture/04-persistence-by-context.md` — Recorded durable adapter status and authoritative Postgres reads; the Redis leaderboard projection landed in a later slice.
- Status: Ranking durable adapter, connector, multi-topic Kafka consumers, and Redis leaderboard projection are **implemented**; clean kind deployment passed.

## Tournament Orchestration Durable Postgres Adapter (2026-07-11)

- `services/tournament-orchestration/**` — Implemented durable pgxpool TournamentRepository (exact schema/bootstrap readiness, complete aggregate hydrate/restore, transactional commit of authoritative rows + command idempotency + append-only CDC outbox under tournament `FOR UPDATE` / create advisory lock). Migration outbox is append-only (no `published_at` / unpublished index / app polling). Real HTTP `RoomProvisioner` for `POST /internal/v1/rooms/provision`. Fail-closed production wiring; provisioning worker stays disabled (no safe `WORKER_ROLE` loop); synchronous `ProcessProvisioningBatch` remains the provision path.
- `services/tournament-orchestration/helm/tournament-orchestration/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `values.schema.json`, `helm-test.sh`), kind secret `tournament-database-url`, deploy/port-forward/integration/adapter scripts, Makefile targets — Helm-only Tournament deploy for kind (ClusterIP); worker disabled.
- `docs/architecture/04-persistence-by-context.md` — Recorded durable adapter status; the Redis bracket projection landed in a later slice; Debezium Connect is kind-wired.
- Status: Tournament durable adapter, connector, `room.match.completed` consumer, and Redis bracket projection are **implemented**; clean kind deployment passed.

## Room Gameplay Durable Postgres + Redis Timer Slice (2026-07-11)

- `services/room-gameplay/**` — Implemented durable pgx SessionRepository + SessionUnitOfWork (ADR-0019 FOR UPDATE across Identity validate → domain apply → GI append → atomic dual outboxes). Migration rewritten with `integration_outbox_events` / `realtime_outbox_events` (no `published_at`), deadlines, bindings, provisions, stream highwater, pending audits, and GI reconciliation markers. Autonomous reconciliation marker (separate TX after confirmed GI append) + durable-API ReconciliationWorker; Redis sorted-set timer index (claim/ack/reaper/rebuild) + `WORKER_ROLE=room-timer` worker. `/ready` verifies schema fingerprint + Redis in durable mode. Capability keeps MemorySessionRepository + OutboxRetryWorker + MultiDestinationPublisher.
- `services/room-gameplay/helm/room-gameplay/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `helm-test.sh`), kind secrets `room-database-url`, deploy/integration scripts, Makefile targets — Helm-only Room deploy for kind (ClusterIP); timer worker enabled in kind.
- Status: Room durable adapter + timer index + GI reconciliation worker, Connect/Server CDC, and downstream consumers are **implemented**; live clean-cluster delivery passed.

## Identity Durable Postgres + OIDC Slice (2026-07-11)

- `services/identity/**` — Implemented durable pgxpool adapters (`DATABASE_URL`) for players/ACLs/`external_identities`/sessions and append-only Debezium-compatible `outbox_events` (no `published_at` / app polling). OIDC anti-corruption layer (stdlib discovery/JWKS/RS256) maps `(issuer, subject)` without persisting provider tokens. `/ready` verifies writer DB + schema fingerprint in durable mode; checkpoint HTTP invalidation worker remains capability-only (`IDENTITY_CAPABILITY_MODE`).
- `services/identity/migrations/001_init.sql`, `infrastructure/bootstrap/fingerprints/identity.*` — Baseline schema adds `external_identities` and outbox router columns; fingerprints regenerated.
- `services/identity/helm/identity/**` (`values.kind.yaml`, digest-strict `_helpers.tpl`, `helm-test.sh`), kind secrets/OIDC ConfigMap, `infrastructure/kind/scripts/deploy-identity.sh` / `test-identity-adapter.sh`, Makefile targets — Helm-only Identity deploy for kind (no static Deployment); ClusterIP only.
- Status: Identity durable adapter, Debezium connector, and Gateway SessionInvalidated consumer are **implemented**; live clean-cluster delivery passed.

## Game Integrity KurrentDB Adapter + Envelope Encryption (Slice 1) (2026-07-11)

- `services/game-integrity/src/**` — Implemented durable `KurrentStreamRepository` (official client v1.4.0) with exact expected-revision append, room/deck stream restore, AES-256-GCM envelope encryption via local-only versioned `dev` KeyProvider, dynamic `/ready` store+sentinel probe, cancel tombstones, context-propagated seams, decrypt/export audit records, and clean client Close lifecycle. Capability memory path retained.
- `.env.example`, `docker-compose.local.yml` GI env, `docker-compose.capability.yml` memory overlay, `services/game-integrity/helm/**` (incl. `values.kind.yaml`), `infrastructure/kind/manifests/01-local-secrets.yaml`, kind validators, `Makefile` `deps-game-integrity` / `test-game-integrity-integration` — Wired local-only versioned keyring + `DEPLOYMENT_ENV`; Helm kind binding uses `uno-arena-local-credentials`; staging/production stay KMS fail-closed without dev keys.
- `docs/architecture/02-bounded-context-architecture.md` — Recorded required `X-Audit-Actor` / `X-Audit-Reason` headers and durable decrypt/export audit distinction for replay vs export.

Does not claim Kafka integration or full cluster deployment.

## Kind Local Foundation and Context Bootstrap (Slice 0) (2026-07-11)

- `infrastructure/kind/**`, `infrastructure/bootstrap/**`, Makefile kind targets, `devops-checkpoint/local-runbook.md`, and `.env.example` — Added a disposable single-node `kind` foundation (namespace `uno-arena`, four separate Postgres instances, Kafka KRaft RF1, Redis, digest-pinned KurrentDB ARM64, ClickHouse, Keycloak start-dev) plus context-owned bootstrap Jobs that embed existing `services/*/migrations` without rewriting them, AsyncAPI-derived Kafka topic init (reduced high/business partitions, Connect internal topics only), and offline validation. Material corrections: atomic single-session Postgres gate with migration fingerprints, ClickHouse pre-mutation exact-state gate, topic drift asserts, apply datastore-then-Job ordering, literal reset pin, and strengthened offline validators. Reinspection: zero role mutation before the Postgres state gate (one admin transaction + `SET ROLE` bootstrap DDL), exact-one-row migrations/meta for Postgres and ClickHouse, Kafka cleanup/minISR fail-closed on empty/unparseable configs, and regression fixtures for those shapes. No service/adapter readiness claim.

## KurrentDB 26.0.3 LTS and Native ARM64 Local Image (2026-07-11)

- `docs/adr/0002-eventstoredb-for-game-integrity.md` - Marked the original product-name decision superseded while retaining its append-only Game Integrity contract.
- `docs/adr/0035-kurrentdb-26-lts-with-native-arm64-local-image.md` - Selected KurrentDB 26.0.3 LTS, the official Go client v1.4.0, stable AMD64 and official-publisher experimental ARM64 image digests, canonical `KURRENTDB_URL`, and mandatory native-ARM persistence/restart verification.
- `docker-compose.local.yml`, `docker-compose.capability.yml`, `.env.example`, `services/game-integrity/**`, `client-checkpoint/**`, `README.md`, and `docs/architecture/**` - Replaced the obsolete AMD64-only local pin and EventStore configuration names with the current KurrentDB image, paths, environment, readiness reason, topology, and documentation. Capability mode continues to omit the store intentionally because that lane uses explicit GI memory.

The ARM64 image is local-only and experimental; failure blocks readiness and never falls back to memory. The Game Integrity stream, expected-revision, encryption, replay, and audit contracts are unchanged.

## Store-Specific Backup and Disaster Recovery (2026-07-11)

- `docs/adr/0034-store-specific-backup-and-disaster-recovery.md` - Set Postgres PITR at RPO ≤5 minutes/RTO ≤30 minutes, KurrentDB plus Game Integrity key-history recovery at RPO ≤5 minutes/RTO ≤60 minutes, ClickHouse at RPO ≤24 hours/RTO ≤4 hours, and Redis as rebuild-only authoritative recovery.
- `docs/adr/0024-envelope-encryption-for-game-integrity-payloads.md`, `0033-one-ha-postgres-cluster-per-context.md`, and `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md` - Added isolated encrypted backup storage, separate credentials, continuous WAL/archive monitoring, quarterly restore drills, key-history verification, and production-like recovery tests.

Backup upload is not recovery evidence. Production objectives require a successful isolated restore, invariant/replay verification, measured RPO/RTO, and secure drill teardown; local `kind` remains disposable.

## One HA Postgres Cluster per Context (2026-07-11)

- `docs/adr/0033-one-ha-postgres-cluster-per-context.md` - Required one primary plus two synchronous standbys per Postgres-backed bounded context, acknowledgment by the primary and at least one standby, fenced promotion, stable read-write endpoints, and primary-only authoritative traffic.
- `docs/adr/0004-separate-physical-databases-per-bounded-context.md` and `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md` - Clarified that replicas are one context database boundary rather than shards and added fail-closed quorum, pool/CDC recovery, observability, and failover verification.

Local Docker Compose and `kind` retain one unreplicated Postgres instance per context for resource-bounded verification. They make no HA or automatic-failover claim.

## Kafka Retention Classes and Recovery Boundaries (2026-07-11)

- `docs/adr/0032-kafka-retention-classes-and-recovery-boundaries.md` - Set production defaults of 6 hours for spectator-safe events, 24 hours for gameplay metrics and Identity invalidations, 7 days for other business/projection topics and verified superseded topics, and 30 days for consumer-owned DLQs.
- `docs/adr/0003-kafka-for-async-integration.md`, `0017-consumer-owned-dlqs-and-aggregate-quarantine.md`, `0030-immutable-partition-counts-for-ordered-kafka-topics.md`, `contracts/asyncapi/kafka-v1.yaml`, and `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md` - Added post-expiry snapshot/backfill paths, deletion blockers, capacity inputs, observability, and recovery tests.

Kafka remains bounded operational replay, not permanent audit storage. Consumers that exceed retention enter an explicit context-owned snapshot/backfill path rather than skipping expired records.

## Kafka Production Durability Floor (2026-07-11)

- `docs/adr/0031-kafka-production-durability-floor.md` - Required at least three production brokers, replication factor 3, `min.insync.replicas=2`, `acks=all`, idempotent producers, rack/zone-aware replica placement, and disabled unclean leader election for domain, projection, DLQ, replacement, and Kafka Connect internal topics.
- `docs/adr/0003-kafka-for-async-integration.md`, `contracts/asyncapi/kafka-v1.yaml`, and `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `03-communication-patterns.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md` - Added fail-closed publish behavior, environment validation, broker/ISR observability, local limitations, and multi-broker failure verification.

Docker Compose and local `kind` may use replication factor 1 and `min.insync.replicas=1` for resource-bounded verification. Those settings are rejected in production and make no HA claim.

## Immutable Ordered Kafka Partition Counts (2026-07-11)

- `docs/adr/0030-immutable-partition-counts-for-ordered-kafka-topics.md` - Prohibited in-place partition-count increases for ordered keyed topics, set pre-deployment planning targets of 256 partitions for high-volume Room projection/metrics topics and 32 for lower-volume business topics, and defined versioned-topic expansion with a durable producer epoch/watermark and consumer drain.
- `docs/adr/0003-kafka-for-async-integration.md`, `contracts/asyncapi/kafka-v1.yaml`, and `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added topology-versus-schema versioning, infrastructure enforcement, sizing signals, cutover ordering, observability, and verification.

The planning counts remain adjustable until first topic creation. After creation, expansion requires a new physical topic and ordered cutover; existing topics are never repartitioned in place.

## Transactional Consumer Idempotency and Checkpoints (2026-07-11)

- `docs/adr/0029-transactional-consumer-idempotency-and-checkpoints.md` - Required consumer-owned state, contract idempotency keys, and aggregate checkpoints to commit atomically before Kafka offsets; ordered gaps/conflicts quarantine only the affected aggregate.
- `docs/adr/0017-consumer-owned-dlqs-and-aggregate-quarantine.md`, `contracts/asyncapi/kafka-v1.yaml`, and `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added duplicate/crash semantics, bounded dedupe retention, permanent-replay compaction, offset ordering, observability, and integration tests.

Dedupe rows remain for at least source retention plus the maximum DLQ/replay window. Indefinite replay uses aggregate checkpoints or domain uniqueness instead of an unbounded event-ID ledger.

## Store-Specific Initialization (2026-07-11)

- `docs/adr/0028-store-specific-initialization.md` - Assigned ClickHouse DDL to an Analytics-owned bootstrap Job, kept KurrentDB stream creation lazy and expected-revision guarded, and defined Redis initialization as versioned key-prefix/capability validation plus safe Lua loading rather than schema migration.
- `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md` - Added store-specific deployment ordering, credentials, readiness, recovery, and integration tests.

No generic cross-store migration mechanism is introduced. ClickHouse accepts only empty or exact-current state, KurrentDB validates deployment-owned policies, and Redis remains disposable and rebuildable.

## Context-Owned Postgres Bootstrap Jobs (2026-07-11)

- `docs/adr/0027-context-owned-postgres-bootstrap-jobs.md` - Assigned empty-database initialization to one Kubernetes bootstrap Job per Postgres-backed bounded context, serialized by a context-specific advisory lock and authenticated with a DDL-capable bootstrap credential separate from runtime credentials.
- `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md` - Added deployment ordering, schema-version readiness, least-privilege roles, fail-closed mismatch behavior, disposable reset semantics, and bootstrap verification.

The current pre-deployment path initializes an empty database or accepts the exact expected version. It never mutates an unexpected schema or silently drops data; upgrade migrations remain a later decision before the first stateful upgrade.

## LSN-Gated Outbox Partition Reclamation (2026-07-11)

- `docs/adr/0026-lsn-gated-outbox-partition-reclamation.md` - Required server-time-partitioned outboxes, database-owned partition sealing with a WAL high-water LSN, and fail-closed reclamation only after the corresponding CDC pipeline's durable source offset has passed the high-water mark plus a safety-retention window.
- `docs/adr/0015-redis-streams-for-realtime-sse-delivery.md`, `0016-debezium-cdc-for-transactional-outboxes.md`, and `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md` - Added independent Kafka/realtime cleanup gates, bounded-storage behavior, operational ownership, observability, backpressure, and recovery verification.

Age alone never authorizes outbox deletion. Missing or unhealthy connector offsets stop cleanup and alert rather than risking an unconfirmed event.

## Mandatory Istio Ambient Production mTLS (2026-07-11)

- `docs/adr/0009-no-service-mesh-as-required-component.md` - Marked the checkpoint-era optional-mesh decision superseded.
- `docs/adr/0025-istio-ambient-for-production-mtls.md` - Required Istio Ambient for production east-west L4 mTLS, SPIFFE workload identity, and default-deny authorization without per-pod sidecars. L7 waypoints remain opt-in for concrete needs.
- `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `03-communication-patterns.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md` - Added ambient enrollment, strict mTLS, service-account identities, policy ownership, capacity/failure signals, `kind` parity, and verification requirements.

Application authorization, Game Integrity envelope encryption, external ingress TLS, and encrypted persistent storage remain separate required controls.

## Game Integrity Payload Envelope Encryption (2026-07-11)

- `docs/adr/0024-envelope-encryption-for-game-integrity-payloads.md` - Required AES-256-GCM envelope encryption for deck seeds/order, private card material, reservations, and replay-sensitive Game Integrity payloads. Production keys are wrapped by a managed KMS/HSM provider; local/`kind` uses an explicit development provider.
- `docs/architecture/00-overview-and-traceability.md`, `02-bounded-context-architecture.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added encryption boundaries, readable commitments/metadata, key rotation/retention, audited decrypt/export, latency/failure behavior, observability, and security/recovery tests.

Transport mTLS and encrypted volumes/backups remain complementary. KurrentDB or storage access alone cannot reveal unreleased deck material.

## Provider-Neutral OIDC and Keycloak Reference IdP (2026-07-11)

- `docs/adr/0023-provider-neutral-oidc-with-keycloak-reference.md` - Selected a provider-neutral OIDC anti-corruption layer with Keycloak as the local/`kind` reference provider and an explicitly environment-gated test-account provisioning path.
- `docs/architecture/00-overview-and-traceability.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added OIDC validation/claim mapping, discovery/JWKS rotation, provider failure semantics, Keycloak topology, test provisioning gates, observability, and integration/security tests.

Production configuration rejects development issuers, insecure transport, direct password grants, and test-provisioning endpoints. Provider tokens and raw claims never cross the Identity boundary.

## Authoritative Session Validation for Mutations (2026-07-11)

- `docs/adr/0021-authoritative-session-validation-for-state-changing-commands.md` - Required every state-changing client command to pass authoritative Identity Postgres validation. Positive Redis or gateway cache entries may not authorize mutations.
- `docs/architecture/00-overview-and-traceability.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added the authorization linearization point, signed internal principal forwarding, fail-closed semantics, capacity/latency requirements, cache limits, observability, and revocation-race tests.

Commands admitted before a concurrent revocation commit may finish. Commands validated after the revocation commit are rejected regardless of stale cache state.

## Room Timer Scheduling with Redis Sorted Sets (2026-07-11)

- `docs/adr/0020-redis-sorted-sets-for-room-timer-scheduling.md` - Selected partitioned, per-timer-family Redis sorted sets with atomic Lua claims, visibility leases, lease reaping, Room-side Postgres revalidation, and index rebuild from authoritative open deadlines.
- `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added scheduler topology, stable timer identities, claim/ack/retry semantics, partition isolation, Redis-loss recovery, observability, and failure/rebuild tests.

Redis remains a disposable scheduling index. Postgres deadlines and Room aggregate state remain authoritative.

## Room Command Pessimistic Serialization (2026-07-11)

- `docs/adr/0019-room-commands-use-pessimistic-row-locking.md` - Selected `READ COMMITTED` plus `SELECT ... FOR UPDATE` on the Room aggregate row. Validation, Game Integrity confirmation, snapshot/deadline updates, command outcome, and both outboxes execute under one bounded Room transaction.
- `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added lock ordering, timeout budgets, stale-command timing, rollback/reconciliation behavior, contention observability, and concurrency/failure verification.

Contention remains local to one Room. All Room transactions and locks remain inside Room Gameplay's context-owned Postgres database.

## Consumer-Owned DLQs and Aggregate Quarantine (2026-07-11)

- `docs/adr/0017-consumer-owned-dlqs-and-aggregate-quarantine.md` - Assigned terminal Kafka processing failures to the consuming bounded context with one DLQ per source topic/consumer group. Ordered consumers quarantine only the affected aggregate until replay, rebuild, or reconciliation restores contiguous processing.
- `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added DLQ naming, retry-to-DLQ offset semantics, failure metadata, aggregate-scoped quarantine, continued processing for unrelated keys, and operational verification/observability requirements.

DLQ records wrap the unchanged original event with operational failure metadata. They are not domain events and never enter Game Integrity.

## Debezium CDC for Transactional Outboxes (2026-07-11)

- `docs/adr/0016-debezium-cdc-for-transactional-outboxes.md` - Selected PostgreSQL WAL-based CDC with Debezium's Outbox Event Router for Kafka-bound events. Each Postgres-backed context owns its transactional outbox and connector. Delivery remains at least once and consumers remain idempotent.
- `docs/architecture/00-overview-and-traceability.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Added the CDC publication path, standardized logical outbox fields, connector-per-context topology, WAL/replication-slot operational requirements, lag observability, and real-infrastructure verification expectations.

Realtime player and spectator Redis Streams remain separate from the Kafka-bound connector path under ADR-0015.

## Redis Streams for Realtime SSE Delivery (2026-07-11)

- `docs/adr/0003-kafka-for-async-integration.md`, `0005-redis-as-acceleration-and-projection-only.md`, `0015-redis-streams-for-realtime-sse-delivery.md` - Established the transport split: Redis Streams carries ordered player and spectator delivery to independently scaled, stateless BFF/SSE instances; Kafka carries cross-context business and projection events. Room Gameplay publishes its transactional realtime outbox through a dedicated Debezium Server PostgreSQL-to-Redis pipeline from its context-owned database; Spectator View atomically updates its Redis projection and outgoing stream in Redis.
- `contracts/asyncapi/kafka-v1.yaml` - Defines the bounded-context Kafka integration topics; realtime SSE delivery remains outside the Kafka contract.
- `docs/architecture/00-overview-and-traceability.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Aligned ownership, integration tables, persistence, capacity rationale, and command/recovery flows: Room Gameplay owns a durable realtime outbox captured to Redis Streams; Spectator View publishes only its atomically materialized privacy-filtered Redis projection; trimmed history requires snapshot resynchronization.

No bounded-context ownership changed. Room Gameplay still owns player-private feed shaping, Spectator View still owns spectator privacy and projection state, and the BFF remains a transport adapter rather than a filtering or authoritative state owner.

## Capability Stack Harness and Root-Context Service Images (2026-07-11)

- `services/*/Dockerfile` - All eight services build from the repository root context; copy only required module source and local `replace` modules (gateway + tournament-orchestration → `shared`; room-gameplay → `shared` + spectator-view; others → own `src`). `GOWORK=off`, static `CGO_ENABLED=0`; Go/Alpine pins unchanged (`golang:1.26.5-alpine3.24` / `alpine:3.24.1`).
- `docker-compose.local.yml` - Service `build.context` is `.` with `dockerfile: services/<svc>/Dockerfile`. Postgres named volumes renamed to `pg_*_data_v18` so prior PG16 `pg_*_data` volumes remain preserved and are not silently reused. Networking: non-internal `uno_edge` (gateway only, host publish) plus internal `uno_private` (all services); gateway attaches to both so `NetworkSettings.Ports` materializes (internal-only attachment left PortBindings set but Ports null).
- `docker-compose.capability.yml` - Project-scoped `uno_edge` + `uno_private` network names for `docker compose -p` isolation. Profiles `eventstore` as `architecture-eventstore` and clears GI `depends_on` so capability mode omits the amd64-only EventStore image (GI explicit memory); base compose alone keeps EventStore.
- `client-checkpoint/tests/test-compose-topology.sh` + `Makefile` `test-compose-topology` - Resolved-config checks (no stack): gateway-only edge, all services on private, concrete selected host port, capability project-scoped network names.
- `client-checkpoint/tests/lib/capability-compose.sh` - Harness `up` selects exact resolved `config --services` (fails if `eventstore` appears).
- `services/*/ci/*.gitlab-ci.yml` - `docker build -f services/<svc>/Dockerfile … .`; change rules include `shared/**` (and spectator-view for room-gameplay) where local modules apply, plus root `.dockerignore` for root-context build/test jobs.
- `.dockerignore` - Optional root ignore to keep root-context builds small.
- `client-checkpoint/tests/run-capability-stack.sh` + `lib/` + `fixtures/` - Repeatable bash/curl/python3 capability integration harness (unique project; concrete free localhost gateway port via Python socket, override with `CAP_BFF_HOST_PORT`; bind-conflict retries pick another free port, never published zero; discovery polls `compose ps` + `docker inspect` up to `CAP_READY_TIMEOUT_S` and must match the selected nonzero port; prints `compose ps` diagnostics on failure; raw `/health` then `/ready` wait; CLI/BFF-only paths). Teardown only for harness-started stacks; idempotent INT→EXIT cleanup; `CAP_SKIP_UP=1` preserves caller `UNOARENA_API_URL` and never downs external volumes unless `CAP_TEARDOWN_EXTERNAL=1`. Background streams use process groups; descendants reaped on exit. Control takeover requires exact `event: session_invalidated` + `data:` after subscribe. Spectator SSE subscribes before `DrawCard` and requires exact canonical `event: SnapshotSanitized` + non-empty `data:` (gateway forwards Room event type unchanged). Host registration is strict under unique `RUN_ID` (same as guest). Locked spectator snapshots assert `streamClosed=false`. Public-read fixture validates live leaderboard/analytics response schema. Phase 8 uses `CancelRoom` as the deterministic live terminal-denial representative (raw stream HTTP 403 + CLI evidence `403`/`spectator_denied`, bounded so denial cannot hang); `RoomCompleted` denial remains covered by room-gameplay/spectator-view domain and service tests. `KEEP_STACK=1` leaves the stack up.
- `client-checkpoint/tests/test-capability-helpers.sh` - Focused helper checks (bounded-run exit 7, PID/descendant reap, SSE frame proof, port-zero reject, terminal-denial evidence, public-read fixture validation) without booting the stack.
- `Makefile` - `make test-capability-stack`.
- `README.md`, `client-checkpoint/README.md`, `.env.example` - Document root-context image builds, `*_v18` volume migration/reset, edge+private compose topology, capability EventStore omission, and the capability harness.

## Verified Runtime Version Refresh (2026-07-11)

- `go.work`, `shared/go.mod`, `services/*/src/go.mod` - Language `go 1.26.0` with `toolchain go1.26.5`.
- `services/*/Dockerfile`, `services/*/ci/*.gitlab-ci.yml` - Builders/CI `golang:1.26.5-alpine3.24`; runtime/CI Alpine `alpine:3.24.1`.
- `docker-compose.local.yml`, `.env.example` - Infra pins: `postgres:18.4-alpine3.24`, `apache/kafka:4.3.1`, `eventstore/eventstore:24.10.14` (supported LTS patch; not Kurrent major; amd64-only — capability overlay omits it), `redis:8.8.0-alpine`, `clickhouse/clickhouse-server:26.6.1.1193`. Postgres mounts at `/var/lib/postgresql` on `*_v18` named volumes; prior PG16 `pg_*_data` volumes are preserved (reset = empty `*_v18`; migrate = dump/restore).
- `README.md` - Documented the verified pins, `*_v18` volume note, and capability EventStore omission. OpenAPI 3.0.3 / AsyncAPI 2.6.0 / JSON Schema draft unchanged (contract migrations, not runtime pins).

## Final Contract-Review Corrections

- `contracts/openapi/bff-v1.yaml` - Documented `GET /ready`; removed impossible command HTTP `409` (domain rejection is `200` + `status=rejected`); added actual auth/command/stream/read `429`/`502` responses; `schemaVersion` enum `1`; SSE terminals documented as `RoomCompleted`/`RoomCancelled` without an unemitted spectator-safe `SpectatorStreamsClose` claim.
- `contracts/asyncapi/kafka-v1.yaml` - Clarified Room→Analytics offline transforms (`GameCompleted`→`GameplayMetric` rewrite vs `room.gameplay.metrics`); removed unemitted spectator `SpectatorStreamsClose` eventType claim; added `tournament.completed` and `ranking.leaderboard_snapshot_published` channels/schemas.
- `services/identity` - SessionInvalidated HTTP control body matches Gateway versioned contract (`schemaVersion=1`, stable outbox `eventId`, `eventType`, path/body `sessionId`, reason when present).
- `client-checkpoint/**` - Advisory countdown accepts canonical `openingSequence` and legacy `openingRoomSequence`; `schemaVersion` must equal `1`; `--json` only meaningful for countdown; tests assert real `openingSequence` field.
- `Makefile` - `validate-yaml` includes `docker-compose.capability.yml`.
- `README.md`, room Helm values comments - Configured-mode readiness always blocked until Postgres adapter; credential matrix notes clarified.
- `docs/architecture/02-bounded-context-architecture.md`, `03-communication-patterns.md` - AsyncAPI is Kafka-only; SSE framing belongs in OpenAPI; topic lists include tournament/ranking completion/snapshot channels.

## Settled Uno Challenge Semantics Lock-In

- `docs/01-domain-glossary.md` - Deliverable 1: Challenge Window now states the settled rule — successful `CallUno` closes/resolves the window; later `ReportMissingUno` is rejected inactive with no challenger penalty/facts; valid challenges still make the target draw 2.
- `docs/raw/Design Assignment.md` - Raw Challenge Window (and scenario) wording updated to the same settled rule; the earlier “invalid challenge → challenger draws 2” deviation is marked superseded.
- `docs/03-aggregates-entities-value-objects.md` - Deliverable 3: replaced the stale “invalid challenger draws 2” claim with settled behavior — successful `CallUno` closes/resolves the window; later `ReportMissingUno` is rejected inactive with no challenger penalty; valid challenges still apply `cardsDrawn=2` to the target.
- `docs/04-commands-and-domain-events.md` - Deliverable 4: documented `CallUno` window resolution, post-call/`ReportMissingUno` rejection with no facts, and rejection catalog coverage for inactive challenges.
- `docs/05-domain-event-flow-narratives.md` - Deliverable 5: clarified that `CallUno` is a window-closing path alongside expiry and turn advance.
- `docs/06-edge-cases-and-failure-path-analysis.md` - Deliverable 6 §8.1: recorded post-`CallUno` challenge rejection without challenger draw penalty.
- `services/room-gameplay/src/game` - Removed unreachable `HasCalled` challenger-penalty branch from `ReportMissingUno` (window already closed after `CallUno`); valid missing-Uno target draw-2 retained.

No Design Checkpoint non-negotiable guarantee was weakened or dropped. The 5-second Uno window, server-authoritative timeliness, and 2-card penalty on a valid missing-Uno challenge remain; only the unreachable post-call challenger-penalty wording/code was corrected to match close-on-`CallUno` reject semantics.

## Settled Interface Sync and Capability Implementation Status

- `contracts/openapi/bff-v1.yaml` - Added BFF `GET /v1/rooms/{roomId}/snapshot` and `GET /v1/spectator/rooms/{roomId}/snapshot` with player/spectator snapshot schemas; player hand/`discardTop` use camelCase `Card` DTOs (`id`/`color`/`face`); documented SSE `409 snapshot_required`; recorded SessionBearer security requirements and spectator invalid-token `401`; recorded CLI-as-sole-client and rejection-audit-only notes on the BFF surface. Modeled SSE accurately: `id`/`event` are wire fields, `schemaVersion` is injected inside the JSON `data` body; listed applicable stream `400`/`401`/`409`/`429`/`502` without overclaiming wire exactness.
- `contracts/asyncapi/kafka-v1.yaml` - Declared the production Kafka envelope authoritative; documented offline HTTP bridge as a destination-specific transform (not identical body) with canonical metadata/field mappings; aligned `GameCompleted` / `MatchCompleted` / Uno window schemas (`participants`, `authoritative`, `completed`, `openingSequence`); fixed GameplayMetric visibility spelling to canonical `anonymized_adhoc`.
- `services/gateway` - Capability upstream readiness probes hit `/ready` (never `/health`); upstream not-ready blocks gateway `/ready`.
- `services/room-gameplay` - `game.Card` marshals camelCase `id`/`color`/`face` to match OpenAPI player snapshot DTOs.
- `README.md`, `client-checkpoint/README.md` - Documented CLI as sole capability interface with GUI deferred; snapshot routes and `409 snapshot_required`; capability mode (`GATEWAY_CAPABILITY_MODE` / `ROOM_CAPABILITY_MODE`) vs fake backends; cross-chart credential equality matrix without hardcoded secrets; Service port 8080; worker Deployments disabled pending `WORKER_ROLE` loops.
- `docker-compose.capability.yml`, `.env.example` - Capability overlay uses real-service capability modes + GI explicit memory (not `ALLOW_FAKES` backend fakes).
- `services/*/helm/**` - Service port 8080; peer URLs on `:8080`; timer/provisioning/rebuilder `enabled: false` by default/staging/production; readiness comments state Gateway Redis, Room Postgres, and GI EventStore intentionally block staging/production until durable adapters exist.
- `docs/08-open-questions-and-assumptions.md`, `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md` - Preserved settled architecture/technology requirements while recording snapshot/SSE contracts, spectator admission, absolute `expiresAt`/`openingSequence`, rejection audit-only, Kafka-authoritative vs HTTP-transform wording, and offline-vs-production adapter status.

### Capability implementation status (docs claim)

| Capability | Status in this checkpoint |
| --- | --- |
| BFF REST command envelopes + SSE (player/spectator/control) | Implemented (gateway; capability mode uses real HTTP upstreams) |
| BFF player/spectator snapshot routes + `409 snapshot_required` | Implemented; OpenAPI synchronized (`Card` DTOs + auth/`401`) |
| CLI as sole client/test interface; GUI deferred | Documented and enforced as scope |
| Rejected-command operational/security audit records only | Implemented and documented |
| Spectator admission `waiting`/`locked`/`in_progress`; terminal close on `RoomCompleted`/`RoomCancelled` | Implemented and documented |
| Absolute UTC Uno `expiresAt` + `openingSequence` | Implemented and documented |
| Room completion event names + canonical fields (`GameCompleted`, `MatchCompleted`, spectator-safe) | Kafka envelope authoritative in AsyncAPI; offline HTTP bridge is a documented transform (not identical body); durable Kafka adapters are **implemented** when `KAFKA_BROKERS` is set |
| Production Postgres adapters | **Implemented:** Identity + Room + Ranking + Tournament durable pgx adapters (`DATABASE_URL`, schema-gated `/ready`); Room also requires `REDIS_URL` for timer indexing. Kind Connect/Server CDC has live delivery proof |
| Production Kafka adapters | **Implemented** for declared consumers when `KAFKA_BROKERS` is set: Gateway `identity.session.invalidated`; Tournament `room.match.completed`; Ranking `room.game.completed` / `tournament.players.advanced` / `tournament.completed`; Spectator `room.spectator-safe.events`; Analytics nine configured topics. Clean kind CDC and recovery-worker lanes passed |
| Production Redis adapters | **Implemented:** Spectator projection + SSE/recovery fencing, Ranking leaderboard projection/pagination/rebuild, Tournament bracket projection/pagination/rebuild, Room timer index, and Gateway rate-limit + LiveFeed/session-invalidation admission. Kind Redis and Debezium Server delivery lanes pass |
| Production KurrentDB adapter | **Implemented** for Game Integrity (KurrentDB 26 + AES-256-GCM envelope encryption, ADR-0024/0035). Capability overlay still uses explicit memory. Staging/production stay fail-closed without managed KMS provider config |
| Production ClickHouse adapter | **Implemented** for Analytics (stdlib HTTP, generation-based rebuild, schema-gated `/ready`). Capability memory retained behind `ANALYTICS_CAPABILITY_MODE`. Kafka ingestion of nine configured topics **implemented** when `KAFKA_BROKERS` is set |
| Helm worker Deployments | Room timer, Tournament provisioning/seeding/completion, Ranking snapshotter, and Spectator/Analytics projection-rebuilder binaries/templates are **implemented**. Spectator/Analytics rebuilders are enabled only in kind after full live proofs; default/staging/production remain disabled |

No Design Checkpoint non-negotiable guarantee was weakened or dropped. `/ready` is not weakened to fake production readiness.

## Product Decisions from Grilling and Implementation-Client Scope

- `docs/08-open-questions-and-assumptions.md` - Deliverable 8: moved the four previously open product questions into validated requirements, recorded the CLI-only client scope, and left no residual open wording for those items.
- `docs/01-domain-glossary.md` - Deliverable 1: clarified Host pre-start authority, spectator admission against room/match terminal state, Uno window `expiresAt` plus room sequence, and command-rejection operational/security audit records.
- `docs/02-bounded-contexts-and-context-map.md` - Deliverable 2: documented spectator admission for `waiting`/`locked`/`in_progress`, terminal stream closure on `RoomCompleted`/`RoomCancelled`, and rejection-audit vs Game Integrity separation.
- `docs/03-aggregates-entities-value-objects.md` - Deliverable 3: added Room invariants for deterministic ad-hoc host reassignment, immediate empty-room cancel, post-lock host authority loss, and published Uno `expiresAt`.
- `docs/04-commands-and-domain-events.md` - Deliverable 4: recorded rejection outcomes as structured operational/security audit records only, host-leave causality, and Uno window publication of absolute UTC `expiresAt` with room sequence.
- `docs/05-domain-event-flow-narratives.md` - Deliverable 5: added host-leave and spectator-admission narratives and clarified server-authoritative Uno deadline correction via SSE/command results.
- `docs/06-edge-cases-and-failure-path-analysis.md` - Deliverable 6: replaced ambiguous host-timeout wording with deterministic lowest-seat host reassignment and immediate cancel when empty; aligned rejection and spectator edge cases with the settled decisions.
- `docs/07-consistency-and-recovery-strategy.md` - Deliverable 7: aligned recovery and invariant checks with rejection-audit records and absolute UTC Uno deadlines.
- `docs/architecture/00-overview-and-traceability.md`, `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` - Architecture package: recorded CLI-as-sole-client scope without changing the BFF-only boundary, spectator admission/terminal closure, rejection audit fields, host reassignment, and advisory client countdown semantics.
- `README.md` - Root index: noted the settled product decisions and CLI-only implementation-client scope.
- `client-checkpoint/README.md` - Client checkpoint: stated that the repo-owned simple CLI is the sole client/test interface for this implementation; graphical UI is deferred.

No Design Checkpoint non-negotiable guarantee was weakened or dropped. Rejected commands still do not mutate domain state or append Game Integrity entries; Spectator View remains privacy-filtered; Room Gameplay remains the owner of Uno and reconnect deadlines; and clients still reach the platform only through the BFF.

## DevOps Checkpoint Boundary Preservation

The DevOps Checkpoint preserves the Architecture Checkpoint service boundaries unchanged: its eight service placeholders map one-to-one to the architecture services and introduce no new bounded contexts or shared platform service.

## Optional Uno Rules Resolution

- `docs/01-domain-glossary.md` - Deliverable 1, Domain glossary: defined accumulated draw-card stacking and exact-match jump-ins as part of the ubiquitous language.
- `docs/03-aggregates-entities-value-objects.md` - Deliverable 3, Aggregates, entities, and value objects: added Room invariants for stack eligibility, accumulated penalty resolution, jump-in matching, and post-jump turn authority.
- `docs/04-commands-and-domain-events.md` - Deliverable 4, Commands and domain events catalog: expanded `PlayCard` and `DrawCard` causality with `playMode`, `PenaltyStackIncreased`, and `PenaltyStackResolved`, and documented invalid stack/jump-in rejection outcomes.
- `docs/05-domain-event-flow-narratives.md` - Deliverable 5, Domain event flow narratives: added the synchronous stack and jump-in decision path to room gameplay.
- `docs/06-edge-cases-and-failure-path-analysis.md` - Deliverable 6, Edge cases and failure-path analysis: defined serialization of competing jump-ins and turn actions, plus pending-penalty ownership and resolution behavior.
- `docs/08-open-questions-and-assumptions.md` - Deliverable 8, Open questions and assumptions: resolved the optional stacking/jump-in question using [*UNO - How to Play Correctly!*](https://www.youtube.com/watch?v=rC-DYC3ZELM) and the product-owner fallback for behavior the video does not mention.
- `docs/raw/Design Assignment.md` - Raw EventStorming artifact: synchronized the previously unresolved stacking/jump-in hotspot with the refined design decision.

The implementation baseline enables both rules. `Draw Two` and legally playable `Wild Draw Four` penalties may be stacked by the currently targeted player, including mixed stacks; the accumulated penalty transfers until a target draws the full total and forfeits the turn. Outside mandatory-resolution states, a player may jump in with an exact color-and-rank-or-symbol match, after which turn order continues from the jumper's seat. This changes Room Gameplay policy only and does not move ownership across bounded contexts.

## Architecture Evaluation Closure

- `docs/architecture/01-context-and-container-view.md` - Architecture deliverable 6.1, context and container view: annotated the Mermaid container diagram with an explicit `Trust Boundary / External Edge (DMZ)` between untrusted clients and private services/data stores.
- `docs/architecture/02-bounded-context-architecture.md` - Architecture deliverable 6.1, architecture of every bounded context: added endpoint-level authorization annotations for public BFF-routed APIs, internal service APIs, anonymous-tolerant spectator/public reads, player-only room reads, operator-only actions, and compliance-only audit paths.
- `docs/architecture/05-capacity-sketch.md` - Architecture deliverable 6.5, capacity sketch: added aggregate command/event rates, concurrent player/spectator connection counts, reconnect-overlap budget, storage growth estimates for EventStoreDB/Postgres/Redis/ClickHouse, and explicit links from those numbers to partitioning, Kafka topic split, async consumers, and projection isolation decisions.

No Design Checkpoint non-negotiable guarantee was weakened or dropped. The edits make the previously intended security boundary, authorization surface, and scale assumptions reviewable without changing bounded-context ownership: Room Gameplay still owns gameplay invariants, Game Integrity still owns immutable logs, Spectator View still owns privacy-filtered projections, and Ranking/Analytics remain downstream asynchronous consumers.

## Architecture Checkpoint Timer Update

- `docs/04-commands-and-domain-events.md` - Deliverable 4, Commands and domain events catalog: added internal Room Gameplay policy command `ExpireUnoWindow` and domain event `UnoWindowExpired` so the architecture has a named, traceable event for durable 5-second Uno timer expiry.
- `docs/02-bounded-contexts-and-context-map.md` - Deliverable 2, Bounded contexts and context map: added `UnoWindowExpired` to the events that may drive spectator-safe projection updates when a visible Uno window closes.
- `docs/05-domain-event-flow-narratives.md` - Deliverable 5, Domain event flow narratives: clarified that the Uno window can close through a persisted deadline and records `UnoWindowExpired` when no valid call or challenge resolved it first.
- `docs/07-consistency-and-recovery-strategy.md` - Deliverable 7, Consistency and recovery strategy: added the idempotency key and recovery rule for retrying Uno expiry without closing newer windows or applying duplicate side effects.
- `docs/06-edge-cases-and-failure-path-analysis.md` - Deliverable 6, Edge cases and failure-path analysis: clarified that reconnect deadlines are persisted with `disconnectVersion` so timer recovery cannot duplicate or misapply forfeits.
- `docs/raw/Design Assignment.md` - Raw EventStorming artifact: synchronized the in-match command/event table and narrative with `UnoWindowExpired`.
- `docs/architecture/*` - Architecture checkpoint: split the high-level architecture record into focused views covering traceability, containers, bounded-context architecture, communication, persistence, capacity, cross-cutting concerns, and sequence diagrams.

No Design Checkpoint non-negotiable guarantee was weakened or dropped. The 5-second Uno challenge window and 60-second reconnection window remain fixed, Room Gameplay remains the owner of room timer outcomes, Game Integrity remains technical-only, casual Elo remains limited to completed non-abandoned ad-hoc games, and tournament advancement remains based on top three match results with the documented tie-breaks.

## Architecture Checkpoint Boundary Alignment

- `docs/01-domain-glossary.md` - Deliverable 1, Domain glossary: preserved the corrected `game`, `match`, `round`, and `tournament` terminology as the source language for architecture contracts, including the distinction between one-game `GameCompleted`, best-of-three `MatchCompleted`, top-three non-final advancement, and the `<=10` final room rule.
- `docs/02-bounded-contexts-and-context-map.md` - Deliverable 2, Bounded contexts and context map: added `Analytics and Public Read Models` as a narrow downstream bounded context for public, derived, non-authoritative analytics; narrowed `Spectator View` back to room spectator projections so tournament bracket projection stays with Tournament Orchestration.
- `docs/03-aggregates-entities-value-objects.md` - Deliverable 3, Aggregates, entities, and value objects: added `PublicAnalyticsProjection` with anonymization/public-data invariants.
- `docs/04-commands-and-domain-events.md` - Deliverable 4, Commands and domain events catalog: moved tournament advancement ownership out of `MatchCompleted` payload wording and into Tournament Orchestration's `RecordMatchResult`; added analytics projection commands for public derived metrics.
- `docs/05-domain-event-flow-narratives.md` - Deliverable 5, Domain event flow narratives: clarified that Room Gameplay emits ranked match facts, while Tournament Orchestration calculates `PlayersAdvanced`; added analytics propagation from sanitized gameplay, tournament, and rating facts.
- `docs/07-consistency-and-recovery-strategy.md` - Deliverable 7, Consistency and recovery strategy: added Analytics as an eventually consistent rebuildable projection context.
- `docs/08-open-questions-and-assumptions.md` - Deliverable 8, Open questions and assumptions: updated tournament advancement assumptions and added analytics assumptions.
- `docs/raw/Design Assignment.md` - Raw EventStorming artifact: synchronized older Bracket Projection & Analytics wording with the final split between Spectator View and Analytics/Public Read Models, removed `advancingPlayerIds[3]` from the room-completion responsibility, and aligned casual-game completion with `GameCompleted`.

No Design Checkpoint non-negotiable guarantee was weakened or dropped. Tournament advancement is still top three by match wins, card-point tie-break, and completion-time tie-break; the runtime owner is now stated more precisely as Tournament Orchestration. Spectator privacy remains first-class and Analytics is explicitly downstream, sanitized, derived, and non-authoritative.

## Architecture Checkpoint Fallback Modeling

- `docs/04-commands-and-domain-events.md` - Deliverable 4, Commands and domain events catalog: added saga, reconciliation, and fallback commands/events for provisioning retry/quarantine, tournament result quarantine, room-state reconciliation, and unsafe projection quarantine.
- `docs/06-edge-cases-and-failure-path-analysis.md` - Deliverable 6, Edge cases and failure-path analysis: added the fallback behavior for dead-lettered tournament provisioning work.
- `docs/07-consistency-and-recovery-strategy.md` - Deliverable 7, Consistency and recovery strategy: clarified which fallback outcomes become domain events and how provisioning, result conflicts, and append/local-commit divergence recover.
- `docs/architecture/03-communication-patterns.md` - Architecture communication patterns: added a fallback modeling rule and a flow-by-flow fallback matrix.
- `docs/architecture/06-cross-cutting-concerns.md` - Architecture cross-cutting concerns: distinguished operational fallback signals from business fallback events.

## Architecture Compliance Review

- `docs/architecture/02-bounded-context-architecture.md` - Architecture deliverable 6.1, Architecture of every bounded context: added explicit dependency and contract-ownership sections for each bounded context so upstream/downstream relationships are visible in the per-context architecture, not only in the context map.
- `docs/architecture/03-communication-patterns.md` - Architecture deliverable 6.3, Communication patterns: added mandatory multi-layer rate limiting mapped to BFF, Room Gameplay, Tournament Orchestration, Redis acceleration, identity-derived scope, and failure behavior.
- `docs/architecture/04-persistence-by-context.md` - Architecture deliverable 6.4, Persistence layer per context: clarified consistency model, read models, retention, audit, and game-log access rules for each context.
- `docs/architecture/05-capacity-sketch.md` - Architecture deliverable 6.5, Capacity sketch: expanded the first-round room-provisioning surge and round-end completion burst mechanics with partitioning, consumer groups, backpressure, and acceptable analytics/projection lag.
- `docs/raw/Design Assignment.md` - Raw EventStorming artifact: aligned the casual-game completion flow with the corrected `GameCompleted` event name.

No Design Checkpoint non-negotiable guarantee was weakened or dropped. The edits only make runtime ownership, rate limiting, persistence, burst handling, and terminology traceability explicit for the architecture checkpoint.

## Submission Package Organization

- `README.md` - Root index: reorganized the repository entrypoint so reviewers can find the current design package, split architecture package, ADR decision log, raw EventStorming artifact, and this changelog.
- `docs/adr/*` - Decision log: added separate ADRs for the major architecture decisions: REST command envelope and SSE, EventStoreDB for Game Integrity, Kafka for async integration, separate physical databases, Redis usage, sharded tournament provisioning, projection-owned privacy filtering, ClickHouse analytics, and the checkpoint-era optional-mesh posture later superseded by mandatory Istio Ambient production mTLS.
- `docs/architecture/*` - Architecture package: added focused architecture deliverables for overview/traceability, context/container views, bounded-context architecture, communication patterns, persistence, capacity, cross-cutting concerns, and sequence diagrams.

These organization changes do not alter domain behavior. They make the architecture checkpoint easier to grade and preserve traceability from the root README to the design package, architecture package, ADRs, and design changelog.
