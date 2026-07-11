# EventStoreDB for Game Integrity

## Status
Superseded by ADR-0035

## Context
The platform needs an immutable technical log that can support integrity checks, replay, and audit without becoming the general business event stream.

## Decision
`Game Integrity` uses EventStoreDB as an immutable append-only stream per room or game. The stream is for technical integrity only: replay, audit, and verification of append order through expected revision. It remains internal-only. Kafka is not the audit log.

ADR-0035 retains this storage contract under the product's current KurrentDB name and selects version 26.0.3 LTS with architecture-specific pinned images.

## Consequences
Game integrity events stay durable, replayable, and isolated from business integration traffic. Expected revision protects append ordering. Audit and replay can be implemented without making Kafka the source of truth for integrity.
