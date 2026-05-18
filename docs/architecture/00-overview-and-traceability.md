# 00 Overview and Traceability

This architecture package captures the accepted target design for UnoArena and splits it into focused views:

- `01-context-and-container-view.md`
- `02-bounded-context-architecture.md`
- `03-communication-patterns.md`
- `04-persistence-by-context.md`
- `05-capacity-sketch.md`
- `06-cross-cutting-concerns.md`
- `07-sequence-diagrams.md`

It is intentionally traceable rather than expansive. The goal is to show how the accepted decisions fit together, what each bounded context owns, which stores are authoritative, and how commands and events move across boundaries.

## Design Principles

- The BFF is the only external boundary.
- Clients use REST command envelopes and SSE for realtime updates.
- Gameplay rules live in Room Gameplay; technical integrity lives in Game Integrity.
- Postgres is the authoritative store where a context needs transactional state.
- Redis is a cache or scheduling index only when justified.
- Kafka carries bounded-context-specific event streams, not a single undifferentiated bus.
- Spectator-safe data is derived, filtered, and privacy-aware by design.
- Realtime recovery and auditability are first-class requirements, not afterthoughts.
- OpenAPI describes the BFF command surface; AsyncAPI describes the event and stream contracts.

## Decision Traceability

| Accepted decision | Where it is reflected |
| --- | --- |
| BFF is the only external boundary; clients never call microservices directly | `01-context-and-container-view.md`, `03-communication-patterns.md` |
| REST command envelope + SSE realtime, one logical Realtime Gateway/BFF | `01-context-and-container-view.md`, `03-communication-patterns.md` |
| Compact command endpoint maps to the existing command catalog; `commandId` idempotency key, `type` command name, `expectedSequenceNumber` for room mutations | `03-communication-patterns.md`, `07-sequence-diagrams.md` |
| Game Integrity is a separate BC/service; EventStoreDB is authoritative append-only per room/game; internal-only audit/replay API | `02-bounded-context-architecture.md`, `04-persistence-by-context.md`, `03-communication-patterns.md` |
| Room Gameplay owns Uno rules and calls Game Integrity before broadcast | `02-bounded-context-architecture.md`, `07-sequence-diagrams.md` |
| Room Gameplay uses Postgres snapshots, outbox, player feed read API/stream, and durable timers with Redis only as an index | `04-persistence-by-context.md`, `05-capacity-sketch.md` |
| Tournament Orchestration uses Postgres, sharded workers, async `MatchCompleted` consumption, and Redis bracket projection | `02-bounded-context-architecture.md`, `04-persistence-by-context.md`, `07-sequence-diagrams.md` |
| Ranking is async with Postgres authoritative and Redis leaderboard cache | `02-bounded-context-architecture.md`, `04-persistence-by-context.md` |
| Identity uses external IdP plus internal session/ACL, Postgres authoritative with Redis cache, and `SessionInvalidated` closes SSE through BFF | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md` |
| Spectator View is a Redis materialized projection rebuilt from committed safe events and sanitized snapshots, and owns privacy filtering | `02-bounded-context-architecture.md`, `04-persistence-by-context.md` |
| Analytics and Public Read Models is a narrow BC with ClickHouse and public derived, non-authoritative analytics | `02-bounded-context-architecture.md`, `04-persistence-by-context.md` |
| Kafka topics are bounded-context-specific; event schema versions are explicit | `03-communication-patterns.md` |
| No service mesh is required; private networking, service credentials, and correlation headers are enough | `06-cross-cutting-concerns.md` |
| OpenAPI and AsyncAPI are the contract intent for HTTP and event surfaces | `03-communication-patterns.md`, `06-cross-cutting-concerns.md` |

## Architectural Qualities

### Security and privacy

- Private player state never leaks into spectator projections.
- Session invalidation must be enforceable even while SSE streams are open.
- Internal service calls should rely on service identity, least privilege, and correlation headers.
- Audit and replay interfaces are internal-only.

### Reliability

- Accepted gameplay commands must be durably confirmed before broadcast.
- Realtime fan-out should survive worker restarts without corrupting room state.
- Timers must recover from Redis loss because Redis is not authoritative.
- Tournament advancement must be replayable and idempotent.

### Operability

- Every command and event should be traceable by `correlationId`, `commandId`, and aggregate identifiers.
- Each bounded context owns its own operational datastore and migration cadence.
- Projection rebuilds should be possible from durable upstream facts.

## NFR Targets

These are design targets for implementation and review, not product promises.

- Player command response should target p95 150-250 ms from BFF receipt to accepted/rejected command outcome under normal regional load.
- Player SSE delivery should target p95 under 250 ms after authoritative commit.
- Spectator SSE delivery should target p95 500 ms-1 s, with controlled coalescing allowed under load.
- Game Integrity append should target p95 under 50 ms inside the active region.
- Session invalidation should close superseded SSE streams within 2 seconds of `SessionInvalidated` publication.
- Tournament bracket projections should be seconds-fresh and explicitly versioned; they must not block authoritative advancement.
- Ranking and analytics are eventually consistent and may lag seconds to minutes during large bursts.
- Gameplay and tournament writes must remain consistent under retries and duplicated submissions.
- Replay and audit paths must be available without depending on live gameplay services.
- Privacy controls must be enforced in the projection layer, not by client convention.
