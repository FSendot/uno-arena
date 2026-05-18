# 06 Cross-Cutting Concerns

## Security

- Use the external IdP for user authentication.
- Enforce authorization in Identity and the BFF before command dispatch.
- Scope service credentials per bounded context.
- Keep internal audit and replay APIs private.
- Treat spectator traffic as untrusted by default.
- Keep microservices off the public internet; the BFF is the only external surface.

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
- durable timer state outside Redis
- bounded-context-specific Kafka topics

## Observability

- Propagate `correlationId` across the BFF, services, workers, and consumers.
- Include `commandId`, `eventId`, `causationId`, `roomId`, `gameId`, `tournamentId`, and `schemaVersion` where applicable.
- Log command acceptance, rejection, append confirmation, and broadcast completion.
- Trace internal hops from envelope to durable append to downstream publication.
- Expose metrics for SSE connection counts, command latency, append latency, consumer lag, timer lag, and projection rebuild duration.
- Record structured reasons for rejection, especially stale sequence numbers and invalid session state.
- Track tournament provisioning batch success/failure, DLQ depth, round projection lag, and session invalidation close latency.

## Error Handling

- Stale room mutations should be rejected cleanly, not retried blindly.
- Timer and reconnect handlers should be idempotent.
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

- Full OpenAPI is a follow-up artifact, not part of this checkpoint implementation.
- OpenAPI should formalize the BFF command envelope, status/error response shape, SSE endpoints, and versioned resources.
- Command envelopes should use a discriminated schema where `type` maps to the design command catalog and `payload` is a `oneOf` by command type.
- AsyncAPI should formalize topic names, event schemas, producers, consumers, partition keys, idempotency keys, and schema-version policy.

## Testing Strategy

- Contract tests for command envelopes and SSE payloads.
- Integration tests for room append-before-broadcast behavior.
- Replay tests for Game Integrity.
- Projection tests for Spectator View privacy filtering.
- End-to-end tests for session invalidation closing live streams.
