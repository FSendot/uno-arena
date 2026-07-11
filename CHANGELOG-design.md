# Design Changelog

This changelog records design-package updates made while shaping the architecture checkpoint. It is limited to changes that affect traceability between the original design deliverables and the architecture.

## DevOps Checkpoint Boundary Preservation

The DevOps Checkpoint preserves the Architecture Checkpoint service boundaries unchanged: its eight service placeholders map one-to-one to the architecture services and introduce no new bounded contexts or shared platform service.

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
