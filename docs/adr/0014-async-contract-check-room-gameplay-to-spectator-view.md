# Async Contract Check: Room Gameplay to Spectator View on room.spectator-safe.events

## Status
Accepted

## Context
The checkpoint requires at least one contract check in the pipeline to demonstrate where real contract testing would live. The architecture has multiple integration boundaries — synchronous (BFF to Identity) and asynchronous (Room Gameplay to several consumers via Kafka). The most illustrative async boundary for a schema compatibility check is `room.spectator-safe.events`, produced by Room Gameplay and consumed by Spectator View. This event is architecturally significant: it is the privacy-filtered projection feed that Spectator View materializes into Redis, and its schema correctness directly affects what spectators see.

## Decision
A JSON Schema compatibility check is wired into the `test` stage of both Room Gameplay (producer) and Spectator View (consumer). Room Gameplay owns the schema definition under `services/room-gameplay/contracts/room.spectator-safe.events.schema.json`. Spectator View's test stage validates its consumer stub against that schema. The check operates on placeholder payloads — it does not require a running Kafka broker. This is traceable to ADR-0003 (Kafka for async integration).

## Consequences
The contract check seam is in place for both the producer and consumer, even though neither service has real domain behavior in this checkpoint. When real services replace the placeholders, the schema check becomes the enforcement point for backward compatibility. A schema change by Room Gameplay that breaks Spectator View's consumer stub will fail both services' `test` stages and block their pipelines — demonstrating cross-service contract blocking as required by the checkpoint's fail-fast constraint. Choosing this pair over the BFF → Identity sync boundary gives a more interesting and harder-to-test case, since async schema drift is typically less visible than a synchronous API mismatch.
