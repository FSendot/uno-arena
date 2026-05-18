# REST Command Envelope and SSE

## Status
Accepted

## Context
The client must interact through a BFF, and the system needs a compact command entrypoint with idempotency, concurrency control, and realtime updates.

## Decision
Clients submit commands through the BFF only, using `POST /v1/rooms/{roomId}/commands` as a compact REST command envelope. The envelope carries `commandId` as the idempotency key, `type` from the command catalog, `expectedSequenceNumber` for existing room mutations, and the command payload. Realtime updates are delivered over SSE. The BFF routes requests and authorizes them, but it does not own domain logic.

## Consequences
Command handling stays explicit and replayable, while the BFF remains a routing and authorization layer. `commandId` and `expectedSequenceNumber` provide safe retries and mutation ordering. Realtime behavior depends on SSE delivery rather than polling.
