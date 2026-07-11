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
- This implementation uses the repo-owned simple CLI as the sole client/test interface; a graphical interface is deferred and must preserve the BFF-only REST/SSE boundary.
- Gameplay rules live in Room Gameplay; technical integrity lives in Game Integrity.
- Rejected commands never produce domain events or Game Integrity log entries; they emit structured operational/security audit records.
- Postgres is the authoritative store where a context needs transactional state.
- Redis is a cache or scheduling index only when justified.
- Kafka carries bounded-context-specific event streams, not a single undifferentiated bus.
- Spectator-safe data is derived, filtered, and privacy-aware by design.
- Spectators may connect while a room is `waiting`, `locked`, or `in_progress`; admission is denied in `completed`/`cancelled`, and terminal room/match state closes existing spectator streams.
- Ad-hoc host leave before lock/start reassigns to the lowest occupied seat or cancels immediately if empty; after lock/start the host label has no gameplay authority.
- Uno windows publish absolute UTC `expiresAt` plus the opening room sequence; CLI countdown is advisory and the server exclusively decides timeliness.
- Realtime recovery and auditability are first-class requirements, not afterthoughts.
- OpenAPI describes the BFF command, snapshot, and SSE surface (including `409 snapshot_required`); AsyncAPI describes the event and stream contracts.
- **Capability status:** architecture still requires Postgres, Kafka, Redis, EventStoreDB, and ClickHouse adapters per context. Capability mode uses real HTTP service paths with bounded memory limiters/session repo and explicit GI memory; it must not be read as claiming those production adapters are wired. Gateway Redis, Room Postgres, and GI EventStore `/ready` checks intentionally block staging/production until durable adapters exist.

## Decision Traceability

| Accepted decision | Where it is reflected |
| --- | --- |
| BFF is the only external boundary; clients never call microservices directly | `01-context-and-container-view.md`, `03-communication-patterns.md` |
| Repo-owned simple CLI is the sole client/test interface for this implementation; graphical UI deferred without changing BFF-only REST/SSE boundary | `01-context-and-container-view.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `client-checkpoint/README.md` |
| REST command envelope + SSE realtime, one logical Realtime Gateway/BFF | `01-context-and-container-view.md`, `03-communication-patterns.md` |
| Compact command endpoint maps to the existing command catalog; `commandId` idempotency key, `type` command name, `expectedSequenceNumber` for room mutations | `03-communication-patterns.md`, `07-sequence-diagrams.md` |
| Rejected commands emit structured operational/security audit records only; no domain events and no Game Integrity append | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` |
| Spectators may connect in `waiting`/`locked`/`in_progress`; denied and streams close after `RoomCompleted`/`RoomCancelled` for the complete match/room | `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `07-sequence-diagrams.md` |
| Ad-hoc host leave before lock/start reassigns to lowest occupied seat or cancels immediately; post-lock host has no gameplay authority | `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `07-sequence-diagrams.md` |
| Uno windows publish absolute UTC `expiresAt` and opening room sequence; CLI countdown is advisory and server decides timing | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` |
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
| OpenAPI and AsyncAPI are the contract intent for HTTP and event surfaces | `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `contracts/openapi/bff-v1.yaml`, `contracts/asyncapi/kafka-v1.yaml` |
| BFF player/spectator snapshot routes and SSE `409 snapshot_required` for unknown/evicted Last-Event-ID | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `contracts/openapi/bff-v1.yaml` |
| Offline capability mode (real HTTP paths + bounded memory / GI memory) is distinct from required production Postgres/Kafka/Redis/EventStoreDB/ClickHouse adapters; Kafka envelope authoritative vs HTTP bridge transform | `01-context-and-container-view.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `README.md`, `CHANGELOG-design.md` |
| Helm Service/peer port 8080; worker Deployments disabled until WORKER_ROLE loops exist; `/ready` not weakened for missing durable adapters | `README.md`, `services/*/helm/**`, `06-cross-cutting-concerns.md` |

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
- Rejected commands must leave structured operational/security audit records with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason, submitted/current sequence when applicable, and timestamp.
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
