# Design Changelog

This changelog records design-package updates made while shaping the architecture checkpoint. It is limited to changes that affect traceability between the original design deliverables and the architecture.

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
- `contracts/asyncapi/kafka-v1.yaml` - Aligned `RoomFeedEvent.sequenceNumber` with producers; clarified Room→Analytics offline transforms (`GameCompleted`→`GameplayMetric` rewrite vs `room.gameplay.metrics`); removed unemitted spectator `SpectatorStreamsClose` eventType claim; added `tournament.completed` and `ranking.leaderboard_snapshot_published` channels/schemas.
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
| Room completion event names + canonical fields (`GameCompleted`, `MatchCompleted`, spectator-safe) | Kafka envelope authoritative in AsyncAPI; offline HTTP bridge is a documented transform (not identical body); Kafka adapters **not** claimed wired |
| Production Postgres adapters | Required; **not** implemented — Room configured-mode `/ready` is always `postgres_adapter_blocked` until a durable Postgres session adapter exists (`DATABASE_URL` does not unblock); staging/production rollout blocked |
| Production Kafka adapters | Required; **not** implemented — offline HTTP bridges only |
| Production Redis adapters | Required; **not** implemented — Gateway `/ready` stays not-ready with `REDIS_URL` and no rate-limit adapter; staging/production rollout blocked |
| Production EventStoreDB adapter | Required; **not** implemented — capability overlay uses explicit memory; with `EVENTSTORE_URL` set, `/ready` reports `eventstore_adapter_blocked`; staging/production rollout blocked |
| Production ClickHouse adapter | Required; **not** claimed implemented (offline memory analytics store) |
| Helm worker Deployments (timer / provisioning / projection-rebuilder) | **Blocked / disabled** (`enabled: false`) — binaries do not implement `WORKER_ROLE` loops; production worker adapters absent |

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
- `docs/adr/*` - Decision log: added separate ADRs for the major architecture decisions: REST command envelope and SSE, EventStoreDB for Game Integrity, Kafka for async integration, separate physical databases, Redis usage, sharded tournament provisioning, projection-owned privacy filtering, ClickHouse analytics, and no required service mesh.
- `docs/architecture/*` - Architecture package: added focused architecture deliverables for overview/traceability, context/container views, bounded-context architecture, communication patterns, persistence, capacity, cross-cutting concerns, and sequence diagrams.

These organization changes do not alter domain behavior. They make the architecture checkpoint easier to grade and preserve traceability from the root README to the design package, architecture package, ADRs, and design changelog.
