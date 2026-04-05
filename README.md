# UnoArena - First Assignment

This repository contains the first assignment deliverables for the UnoArena domain modeling exercise. The documents are organized by required deliverable so the submission can be reviewed section by section.

## Index

- [01. Domain Glossary](./docs/01-domain-glossary.md)
- [02. Bounded Contexts and Context Map](./docs/02-bounded-contexts-and-context-map.md)
- [03. Aggregates, Entities, and Value Objects](./docs/03-aggregates-entities-value-objects.md)
- [04. Commands and Domain Events Catalog](./docs/04-commands-and-domain-events.md)
- [05. Domain Event Flow Narratives](./docs/05-domain-event-flow-narratives.md)
- [06. Edge Cases and Failure-Path Analysis](./docs/06-edge-cases-and-failure-path-analysis.md)
- [07. Consistency and Recovery Strategy](./docs/07-consistency-and-recovery-strategy.md)
- [08. Open Questions and Assumptions](./docs/08-open-questions-and-assumptions.md)

## Scope Notes

- The focus is domain modeling, not deployment or infrastructure design.
- Architectural adapters such as REST, SSE, Kafka, Redis, and Kubernetes are intentionally treated as implementation concerns unless they affect domain boundaries or invariants.
- The modeling assumes Uno rooms support ad-hoc play and tournament-assigned play, and that tournament matches are best-of-three series.

## Suggested Reading Order

1. Start with the glossary to align on the language.
2. Read the bounded contexts and context map to understand ownership and integration boundaries.
3. Review aggregates and invariants before reading the command/event catalog.
4. Use the event-flow and failure-path documents to validate the model under realistic conditions.
