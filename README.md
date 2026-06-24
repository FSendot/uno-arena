# UnoArena - Design and Architecture Assignments

This repository contains the current UnoArena design package and the architecture checkpoint deliverables. The documents are organized so the domain model, architecture views, ADRs, and design changelog can be reviewed section by section.

## Design Package

- [01. Domain Glossary](./docs/01-domain-glossary.md)
- [02. Bounded Contexts and Context Map](./docs/02-bounded-contexts-and-context-map.md)
- [03. Aggregates, Entities, and Value Objects](./docs/03-aggregates-entities-value-objects.md)
- [04. Commands and Domain Events Catalog](./docs/04-commands-and-domain-events.md)
- [05. Domain Event Flow Narratives](./docs/05-domain-event-flow-narratives.md)
- [06. Edge Cases and Failure-Path Analysis](./docs/06-edge-cases-and-failure-path-analysis.md)
- [07. Consistency and Recovery Strategy](./docs/07-consistency-and-recovery-strategy.md)
- [08. Open Questions and Assumptions](./docs/08-open-questions-and-assumptions.md)
- [EventStorming Raw Artifact](./docs/raw/Design%20Assignment.md)

## Architecture Package

- [00. Overview and Traceability](./docs/architecture/00-overview-and-traceability.md)
- [01. Context and Container View](./docs/architecture/01-context-and-container-view.md)
- [02. Bounded Context Architecture](./docs/architecture/02-bounded-context-architecture.md)
- [03. Communication Patterns](./docs/architecture/03-communication-patterns.md)
- [04. Persistence by Context](./docs/architecture/04-persistence-by-context.md)
- [05. Capacity Sketch](./docs/architecture/05-capacity-sketch.md)
- [06. Cross-Cutting Concerns](./docs/architecture/06-cross-cutting-concerns.md)
- [07. Sequence Diagrams](./docs/architecture/07-sequence-diagrams.md)

## Decision Log

- [ADR-0001. REST Command Envelope and SSE](./docs/adr/0001-rest-command-envelope-and-sse.md)
- [ADR-0002. EventStoreDB for Game Integrity](./docs/adr/0002-eventstoredb-for-game-integrity.md)
- [ADR-0003. Kafka for Async Integration](./docs/adr/0003-kafka-for-async-integration.md)
- [ADR-0004. Separate Physical Databases per Bounded Context](./docs/adr/0004-separate-physical-databases-per-bounded-context.md)
- [ADR-0005. Redis as Acceleration and Projection Only](./docs/adr/0005-redis-as-acceleration-and-projection-only.md)
- [ADR-0006. Sharded Tournament Provisioning](./docs/adr/0006-sharded-tournament-provisioning.md)
- [ADR-0007. Projections Own Privacy Filtering and BFF Only Routes](./docs/adr/0007-projections-own-privacy-filtering-and-bff-only-routes.md)
- [ADR-0008. Analytics Public Read Models with ClickHouse](./docs/adr/0008-analytics-public-read-models-with-clickhouse.md)
- [ADR-0009. No Service Mesh as Required Component](./docs/adr/0009-no-service-mesh-as-required-component.md)
- [ADR-0010. Helm from Pipeline as Deploy Model](./docs/adr/0010-helm-from-pipeline-as-deploy-model.md)
- [ADR-0011. Image Versioning: Branch-SHA Tag with Digest Pinning](./docs/adr/0011-image-versioning-branch-sha-with-digest-pinning.md)
- [ADR-0012. Monorepo Change Detection via rules:changes per Service Fragment](./docs/adr/0012-monorepo-change-detection-via-rules-changes.md)
- [ADR-0013. Infrastructure Workers Bundled with Their Owning Service](./docs/adr/0013-workers-bundled-with-owning-service.md)
- [ADR-0014. Async Contract Check: Room Gameplay to Spectator View](./docs/adr/0014-async-contract-check-room-gameplay-to-spectator-view.md)

## Change Tracking

- [Design Changelog](./CHANGELOG-design.md)

## Scope Notes

- The design package defines domain language, bounded contexts, aggregates, commands, events, edge cases, consistency, and assumptions.
- The architecture package translates those design decisions into deployable services, interfaces, persistence, communication patterns, capacity reasoning, and operational constraints.
- Architectural adapters such as REST, SSE, Kafka, Redis, EventStoreDB, ClickHouse, and service deployment choices are documented where they affect assignment invariants or scaling constraints.
- The modeling assumes Uno rooms support ad-hoc play and tournament-assigned play, and that tournament matches are best-of-three series.

## Suggested Reading Order

1. Start with the glossary to align on the language.
2. Read the bounded contexts and context map to understand ownership and integration boundaries.
3. Review aggregates and invariants before reading the command/event catalog.
4. Use the event-flow and failure-path documents to validate the model under realistic conditions.
5. Read the architecture overview and traceability document before the detailed architecture views.
6. Review the ADRs for the major technology and boundary decisions.
7. Review the raw EventStorming artifact for the event-first discovery tables, hotspots, and narrative flow evidence.
