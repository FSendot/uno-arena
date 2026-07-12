# Kafka Retention Classes and Recovery Boundaries

## Status
Accepted

## Context
At the target event rates, retaining every Kafka record indefinitely would create unbounded replicated broker storage. Different streams have different replay value: spectator delivery can recover from sanitized snapshots, metrics tolerate delayed backfill, business processes need a longer operational replay window, and DLQs need time for investigation. Kafka is not the authoritative audit store.

## Decision
Use production retention classes with these defaults:

- `room.spectator-safe.events`: 6 hours. Consumers that fall behind this boundary rebuild from a sanitized Room/Spectator snapshot and resume from its sequence.
- `room.gameplay.metrics`: 24 hours. Analytics outages beyond this boundary use a safe backfill path; analytics may remain incomplete or delayed without blocking gameplay.
- `identity.session.invalidated`: 24 hours. Identity Postgres remains authoritative for session validity, so expired control events never authorize a stale session.
- all other business and low-volume projection topics: 7 days.
- consumer-owned DLQs: 30 days from DLQ publication, subject to the documented privacy/sanitization rules.
- a superseded topology topic: at least 7 days after cutover has been verified, with no active consumer, replay, quarantine, or rollback reference.

These are environment-configured defaults and may be revised before first deployment from measured throughput, storage, and recovery objectives. Reducing an active topic's retention is a controlled change that verifies consumer lag and recovery readiness first. Local `kind` may use shorter retention or byte caps for bounded resources and makes no production recovery-window claim.

Kafka retention never defines domain or audit retention. Postgres context history, KurrentDB, ClickHouse retention, and sanitized snapshots remain the documented recovery/audit sources. Topic expiry is observable; a consumer that exceeds its source window enters explicit snapshot/backfill recovery rather than skipping ahead as if processing were complete. The concrete post-retention path is settled in ADR-0039: Kafka rebuild-request topics plus context-owned internal snapshot/backfill APIs, with no application outbox polling.

## Consequences
Broker storage can be sized by topic class, partition count, replication factor, and measured bytes per second. High-volume topics remain deliberately short-lived, while business replay and failure investigation receive longer windows. Consumer dedupe retention under ADR-0029 uses the applicable source class plus the maximum DLQ/replay window unless compact aggregate checkpoints or domain uniqueness provide indefinite replay safety. Retention pressure, oldest consumer lag, time-to-expiry, snapshot/backfill success, DLQ age, and deletion blockers become required operational signals.
