# 06 Cross-Cutting Concerns

## Security

- Use the external IdP for user authentication.
- Enforce authorization in Identity and the BFF before command dispatch.
- Scope service credentials per bounded context.
- Keep internal audit and replay APIs private.
- Treat spectator traffic as untrusted by default.
- Admit spectators only while a room is `waiting`, `locked`, or `in_progress` subject to public/private authorization; deny new connections and close existing streams after `RoomCompleted` or `RoomCancelled`.
- Keep microservices off the public internet; the BFF is the only external surface.
- Use the repo-owned simple CLI as the sole client/test interface for this implementation; deferred graphical clients must preserve the BFF-only REST/SSE boundary.

## Privacy

- Spectator View owns the privacy filter.
- Private hands, hidden deck state, and player-only decisions never cross into spectator projections.
- Public analytics must stay anonymized or explicitly public.
- Sanitized events are better than ad-hoc client filtering.

## Threat Model

Primary threats:

- stale or replayed commands
- session hijacking or displaced sessions
- spectator data leakage
- duplicate event delivery
- timer manipulation or replay
- cross-service contract drift

Mitigations:

- `commandId` idempotency keys
- `expectedSequenceNumber` checks
- `SessionInvalidated` stream closure
- privacy-filtered projections
- schema versioning on events
- durable timer state outside Redis, with absolute UTC `expiresAt` and opening room sequence as server authority
- structured operational/security audit records for rejected commands
- spectator admission limited to non-terminal room states, with terminal stream closure
- bounded-context-specific Kafka topics

## Observability

- Propagate `correlationId` across the BFF, services, workers, and consumers.
- Include `commandId`, `eventId`, `causationId`, `roomId`, `gameId`, `tournamentId`, and `schemaVersion` where applicable.
- Log command acceptance, rejection, append confirmation, and broadcast completion.
- Trace internal hops from envelope to durable append to downstream publication.
- Expose metrics for SSE connection counts, command latency, append latency, consumer lag, timer lag, and projection rebuild duration.
- Emit structured operational/security audit records for every rejected command with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason, submitted/current sequence when applicable, and timestamp. These records are not domain events and are not Game Integrity entries.
- Track tournament provisioning batch success/failure, DLQ depth, round projection lag, and session invalidation close latency.

## Error Handling

- Stale room mutations should be rejected cleanly, not retried blindly.
- Rejected commands emit structured operational/security audit records only; they never produce domain events or Game Integrity entries.
- Timer and reconnect handlers should be idempotent and decide Uno openness from persisted absolute UTC `expiresAt` plus opening room sequence.
- Projection consumers should tolerate duplicate or out-of-order delivery where the upstream contract allows it.
- Replay or audit failures must not take down live gameplay.
- Saga fallback events should be emitted only when fallback changes business process state, such as quarantine, reconciliation, or recorded retry state.
- Infrastructure fallback such as broker redelivery, DLQ depth, worker restarts, and transient timeouts should be observable through metrics/traces unless it changes domain state.

## Schema and Contract Evolution

- Every event should declare `schemaVersion`.
- Bounded contexts should evolve their own contracts independently.
- Kafka event names and SSE payloads should be versioned deliberately.
- Public analytics schemas may evolve separately from gameplay schemas.

## Deployment and Networking

- No service mesh is required for this design.
- Private networking is sufficient for service-to-service calls.
- Use service credentials and least privilege instead of mesh-heavy policy layers.
- Keep the BFF and stateful services close enough to reduce tail latency where practical.

## OpenAPI and AsyncAPI Intent

- OpenAPI under `contracts/openapi/bff-v1.yaml` formalizes the BFF command envelope, snapshot routes, status/error response shape (including `409 snapshot_required`), SSE endpoints, and versioned resources.
- Command envelopes should use a discriminated schema where `type` maps to the design command catalog and `payload` is a `oneOf` by command type.
- AsyncAPI under `contracts/asyncapi/kafka-v1.yaml` formalizes topic names, event schemas (including Room `GameCompleted`/`MatchCompleted` bodies), producers, consumers, partition keys, idempotency keys, and schema-version policy. The Kafka envelope is authoritative; offline HTTP bridges are documented destination-specific transforms of the same event names and canonical fields and do not claim Kafka is wired.
- Scope service credentials per bounded context. Exact cross-chart credential equality and producer-scope requirements are documented in root `README.md` and Helm `values.yaml` comments (secret key names only; no hardcoded secret values).
- Helm Kubernetes Services and peer URLs use port 8080 consistently. Worker Deployments (Room timer, Tournament provisioning, Spectator/Analytics projection rebuilders) stay `enabled: false` until binaries implement `WORKER_ROLE` loops and production worker adapters exist — enabling them today would launch duplicate HTTP servers.
- Do not weaken `/ready` for staging/production: Gateway Redis rate-limit, Room Postgres session, and Game Integrity EventStore adapters intentionally keep those environments not-ready until durable adapters are implemented.

## Testing Strategy

- Contract tests for command envelopes, snapshot schemas, and SSE payloads, including Uno `expiresAt` plus `openingSequence` and `409 snapshot_required`.
- Integration tests for room append-before-broadcast behavior.
- Rejection tests that assert structured operational/security audit records and the absence of domain events or Game Integrity appends.
- Replay tests for Game Integrity.
- Projection tests for Spectator View privacy filtering.
- Spectator admission tests for `waiting`/`locked`/`in_progress`, denial after `RoomCompleted`/`RoomCancelled`, and terminal stream closure.
- Host-leave tests for ad-hoc lowest-occupied-seat reassignment or immediate cancel before lock/start, and no host gameplay authority after lock/start.
- End-to-end tests for session invalidation closing live streams.
- This implementation drives those checks through the repo-owned simple CLI as the sole client/test interface; graphical UI testing is deferred and must still use only the BFF REST/SSE boundary.
- CLI countdown display for Uno windows is advisory; tests assert server `expiresAt`, `openingSequence`, SSE corrections, and command outcomes rather than local clock authority.
- Offline capability adapters (capability-mode memory session / in-memory limiters / GI memory / HTTP bridge transforms) are tested explicitly; production Postgres/Kafka/Redis/EventStoreDB/ClickHouse adapters remain architecture requirements and are not claimed as wired by those offline adapters.
