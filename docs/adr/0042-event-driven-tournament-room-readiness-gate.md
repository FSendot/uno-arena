# Event-Driven Tournament Room Readiness Gate

## Status

Accepted

## Decision

Room Gameplay atomically records a `RoomRuntimeReady` integration fact in its context-owned outbox when the controller first marks a tournament Room runtime generation Ready. Tournament Orchestration keeps the corresponding match assignment internally `provisioning` and does not expose its `roomId` to players until it consumes that fact. Initial readiness therefore joins asynchronously through Kafka rather than blocking Room provisioning calls or polling every Room.

Runtime replacement does not revoke an already-visible tournament assignment. During a replacement, the Room router returns `503 room_starting` with `Retry-After`, and clients reconcile through bounded retry. Casual room clients use the same bounded startup retry after asynchronous room creation.

## Consequences

Tournament assignment visibility means “the assigned Room became playable at least once,” not “its current pod can never be temporarily unavailable.” The readiness fact is an integration signal, not a gameplay domain event, and Room Postgres remains authoritative for both room lifecycle and runtime generation.

## Rejected alternatives

- Blocking the provisioning request until Kubernetes readiness couples tournament workers to scheduler latency and holds requests during large surges.
- Polling Room readiness from Tournament Orchestration creates cross-context read pressure proportional to the number of first-round rooms.
- Exposing `roomId` immediately makes assignment consumers race the initial runtime admission path.
