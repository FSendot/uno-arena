# Async Contract Check: Room Gameplay to Spectator View on room.spectator-safe.events

## Status
Accepted

## Context
The checkpoint requires at least one contract check in the pipeline to demonstrate where real contract testing would live. The architecture has multiple integration boundaries — synchronous (BFF to Identity) and asynchronous (Room Gameplay to several consumers via Kafka). The most illustrative async boundary for a schema compatibility check is `room.spectator-safe.events`, produced by Room Gameplay and consumed by Spectator View.

Room Gameplay emits a sanitized spectator-safe upstream fact / projection input on that topic. It does not own final privacy filtering for spectators. Per ADR-0007, Spectator View owns privacy filtering and the spectator projection; per ADR-0015, Spectator View materializes that projection in Redis and publishes the outgoing spectator Redis stream atomically with projection and dedupe/sequence state. Schema correctness on the Kafka input therefore affects what Spectator View may safely project and stream, without making Room the owner of the final spectator privacy boundary.

## Decision
A schema-backed compatibility check is wired into the `test` stage of both Room Gameplay (producer) and Spectator View (consumer). Room Gameplay owns the JSON Schema definition under `services/room-gameplay/contracts/room.spectator-safe.events.schema.json`. Both tests load that file and enforce the keywords this contract uses: required fields, field types, `sequenceNumber` minimum, and rejected extra top-level properties. Spectator View's test intentionally reads the producer-owned schema via a cross-service relative path so contract drift is visible to the consumer pipeline. The check operates on placeholder payloads — it does not require a running Kafka broker or a general-purpose JSON Schema engine. This is traceable to ADR-0003 (Kafka for async integration), ADR-0007 (projections own privacy filtering), and ADR-0015 (Spectator View owns Redis projection plus outgoing spectator stream).

## Consequences
The contract check seam is in place for both the producer and consumer, even though neither service has real domain behavior in this checkpoint. When real services replace the placeholders, the schema check becomes the enforcement point for backward compatibility of the Room→Spectator Kafka input. A schema change by Room Gameplay that breaks Spectator View's consumer stub will fail both services' `test` stages and block their pipelines — demonstrating cross-service contract blocking as required by the checkpoint's fail-fast constraint. Choosing this pair over the BFF → Identity sync boundary gives a more interesting and harder-to-test case, since async schema drift is typically less visible than a synchronous API mismatch. Privacy filtering, projection materialization, and the outgoing spectator Redis stream remain Spectator View responsibilities.
