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

## DevOps Checkpoint

- [DevOps Checkpoint README](./devops-checkpoint/README.md)
- [Local GitLab Runner and Staging Runbook](./devops-checkpoint/local-runbook.md)

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
- **Capability status:** capability mode (`GATEWAY_CAPABILITY_MODE` / `ROOM_CAPABILITY_MODE` via `docker-compose.capability.yml`) uses real service HTTP paths with bounded in-memory limiters / memory session repo, plus explicit Game Integrity memory. `GATEWAY_ALLOW_FAKES` / `ROOM_ALLOW_FAKES` remain isolated-test fakes only. Production Postgres, Kafka, Redis, EventStoreDB, and ClickHouse adapters are **not** claimed as implemented. Those stores remain required architecture targets (see ADRs and `docs/architecture/04-persistence-by-context.md`); migrations under `services/*/migrations/` describe intended schemas, not a wired production runtime. Room **configured mode** `/ready` is always blocked with `postgres_adapter_blocked` until a durable Postgres session adapter exists (setting `DATABASE_URL` does not unblock). Gateway Redis and GI EventStore readiness likewise block staging/production rollout until durable adapters exist — do not weaken `/ready`.
- Helm charts expose Service port **8080** (matching container/targetPort and peer URLs). Worker Deployments (timer / provisioning / projection-rebuilder) default to `enabled: false` because binaries do not implement `WORKER_ROLE` loops and production worker adapters are absent.
- Cross-chart credential equality and producer-scope matrix (secret *values* must match across producer/consumer charts; never commit secret material): see the table under Local foundation below and matching comments in `services/*/helm/*/values.yaml`. Each row is a required equality of secret *values*; chart-local key names differ.
- The modeling assumes Uno rooms support ad-hoc play and tournament-assigned play, and that tournament matches are best-of-three series.
- Settled product decisions cover rejected-command operational/security audit records (no domain events or Game Integrity entries), spectator admission through `waiting`/`locked`/`in_progress` with terminal room/match closure on `RoomCompleted`/`RoomCancelled` (not individual `GameCompleted`), deterministic ad-hoc host reassignment before lock/start, and server-authoritative Uno `expiresAt` plus `openingSequence` with advisory client countdown.
- BFF contracts include player/spectator snapshot routes and SSE `409 snapshot_required` when `Last-Event-ID` is unknown or evicted.
- This implementation uses the repo-owned simple CLI under `client-checkpoint/` as the sole client and test interface. A graphical interface is deferred for later refactoring and does not change the BFF-only external boundary.

## Client Checkpoint

- [Client Checkpoint CLI](./client-checkpoint/README.md)

## Local foundation (contracts + topology)

Offline-friendly foundation targets (no automatic module or image fetch):

```bash
make check
# or individually:
make fmt-shared test-shared validate-yaml
```

Verified runtime pins (as of 2026-07-11; see `.env.example` and service Dockerfiles/CI): Go `1.26.0` with toolchain `go1.26.5`; builders `golang:1.26.5-alpine3.24`; runtime/CI Alpine `3.24.1`; local infra `postgres:18.4-alpine3.24`, `apache/kafka:4.3.1`, `eventstore/eventstore:24.10.14`, `redis:8.8.0-alpine`, `clickhouse/clickhouse-server:26.6.1.1193`. OpenAPI 3.0.3 / AsyncAPI 2.6.0 / JSON Schema draft stay unchanged (contract migrations, not runtime pins).

Local Compose topology uses two bridges: a non-internal **edge** network (`uno_edge`, only gateway) for host port publish, and an **internal private** network (`uno_private`, all services including gateway) for interservice traffic. Backends and infra must not attach to edge — a container on an internal-only network does not materialize `NetworkSettings.Ports` even when `HostConfig.PortBindings` is set. The capability overlay project-scopes both network names (`<project>_edge` / `<project>_private`) for `docker compose -p` isolation. Copy `.env.example` before overriding image tags. Postgres 18 uses named volumes `pg_*_data_v18` mounted at `/var/lib/postgresql`; prior PG16 volumes (`pg_*_data`) stay preserved and are not reused — reset with empty `*_v18` volumes or migrate via dump/restore (compose does not delete volumes). `eventstore/eventstore:24.10.14` is amd64-only and remains in base architecture mode; the capability overlay profiles it out (`architecture-eventstore`) because GI uses explicit memory:

```bash
cp .env.example .env
docker compose -f docker-compose.local.yml --env-file .env config
# capability overlay (real HTTP paths; GI memory; EventStore omitted):
docker compose -f docker-compose.local.yml -f docker-compose.capability.yml --env-file .env config
docker compose -f docker-compose.local.yml -f docker-compose.capability.yml --env-file .env config --services
make validate-yaml validate-compose test-compose-topology
# start when images are already available / built locally (this slice does not pull):
# docker compose -f docker-compose.local.yml --env-file .env up -d --no-build
```

Service images build from the **repository root** context (`docker build -f services/<svc>/Dockerfile .`) so local `replace` modules resolve (`shared` for gateway/tournament; `shared` + spectator-view for room-gameplay). Compose `build.context` is `.` for every service. `GOWORK=off` and `CGO_ENABLED=0` produce static binaries; Go/Alpine pins stay at the verified versions above.

Repeatable capability integration (isolated compose project, CLI/BFF only;
teardown only for stacks this harness started):

```bash
make test-capability-stack
# KEEP_STACK=1 make test-capability-stack   # leave harness-started stack up
# CAP_SKIP_UP=1 UNOARENA_API_URL=http://127.0.0.1:8080 make test-capability-stack
```

Contracts live under `contracts/openapi/bff-v1.yaml` and `contracts/asyncapi/kafka-v1.yaml`. Shared stdlib helpers live under `shared/`. Context-owned migrations live under `services/*/migrations/`.

### Cross-chart credential equality / producer-scope matrix

Helm uses `existingSecret` + `secretKeyRef` only (no hardcoded secrets). Operators must place **equal secret values** where the matrix says “same value”; secret *key names* stay chart-local. Mismatched values break producer auth even when key names look related.

| Producer scope | Chart secret key (example) | Must equal (consumer chart key) | Notes |
| --- | --- | --- | --- |
| Gateway → Identity / Room / Tournament / Spectator | `gateway-service-credential` | `identity-internal-credential`, `room-service-credential`, `tournament-internal-credential`, `spectator-view-internal-credential` | One Gateway service credential value shared to each peer’s internal audience key |
| Gateway → Identity (explicit) | `gateway-identity-credential` | `identity-internal-credential` | Same value as Gateway→Identity above with current Identity binary |
| Room → Gateway | `room-to-gateway-credential` | `gateway-room-credential` | Room producer into Gateway control/ingest |
| Room → Identity | `room-identity-credential` | `identity-internal-credential` | Distinct from Gateway→Identity only if operators choose separate values; default local compose may share |
| Room → Game Integrity | `game-integrity-internal-credential` (room chart) | `game-integrity-internal-credential` (GI chart) | Same key name, equal values across charts |
| Room → Ranking | `ranking-internal-credential` (room) | `ranking-internal-credential` (ranking) | Same key name, equal values |
| Room → Analytics | `analytics-room-credential` (room) | `analytics-room-credential` (analytics) | Room→Analytics offline HTTP bridge credential |
| Room timer only | `room-timer-service-credential` | (Room timer audience only; not Gateway) | Never reused as Gateway producer credential |
| Ranking → Analytics | `analytics-ranking-credential` | analytics chart only | Ranking producer scope |
| Tournament → Analytics | `analytics-tournament-credential` | analytics chart only | Tournament producer scope |
| GI audit / Analytics ops | `game-integrity-audit-credential` / `analytics-ops-credential` | operator/compliance scope; not gameplay producers | Not interchangeable with room/ranking/tournament producer credentials |

Kubernetes Services and peer URLs use port **8080** consistently.

## Suggested Reading Order

1. Start with the glossary to align on the language.
2. Read the bounded contexts and context map to understand ownership and integration boundaries.
3. Review aggregates and invariants before reading the command/event catalog.
4. Use the event-flow and failure-path documents to validate the model under realistic conditions.
5. Read open questions and assumptions for settled product decisions and remaining scope notes.
6. Read the architecture overview and traceability document before the detailed architecture views.
7. Review the ADRs for the major technology and boundary decisions.
8. Review the raw EventStorming artifact for the event-first discovery tables, hotspots, and narrative flow evidence.
