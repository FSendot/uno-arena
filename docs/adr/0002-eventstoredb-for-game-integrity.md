# EventStoreDB for Game Integrity

## Status
Accepted

## Context
The platform needs an immutable technical log that can support integrity checks, replay, and audit without becoming the general business event stream.

## Decision
`Game Integrity` uses EventStoreDB as an immutable append-only stream per room or game. The stream is for technical integrity only: replay, audit, and verification of append order through expected revision. It remains internal-only. Kafka is not the audit log.

## Consequences
Game integrity events stay durable, replayable, and isolated from business integration traffic. Expected revision protects append ordering. Audit and replay can be implemented without making Kafka the source of truth for integrity.
