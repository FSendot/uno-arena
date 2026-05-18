# Kafka for Async Integration

## Status
Accepted

## Context
The system needs asynchronous integration across bounded contexts, but not every stream has the same volume or purpose.

## Decision
Kafka is used for asynchronous integration between bounded contexts. Topics are bounded-context-specific. High-volume projection or sanitized-metrics streams are separated from low-volume business streams. Events carry per-event schema versions.

## Consequences
Integration remains decoupled while topic ownership stays clear. High-volume read-side traffic can scale independently from business events. Schema evolution stays explicit per event type and version.
