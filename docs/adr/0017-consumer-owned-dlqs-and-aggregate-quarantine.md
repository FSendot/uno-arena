# Consumer-Owned DLQs and Aggregate Quarantine

## Status
Accepted

## Context
Kafka topics have multiple independent consumer groups. One event can be valid and successfully processed by one bounded context while another consumer fails because of its own schema, dependency, projection, or policy problem. A producer-owned or topic-wide DLQ would blur failure ownership and couple unrelated recovery workflows.

## Decision
Each consuming bounded context owns a DLQ per source topic and consumer group, named `<source-topic>.<consumer>.dlq`. After bounded retries, the consumer publishes the unchanged original event envelope plus operational failure metadata to its DLQ, waits for broker acknowledgment, and only then commits the source offset. Failure metadata includes the consumer, attempt count, failure classification, first/last failure timestamps, correlation/trace identifiers, and a sanitized error summary.

DLQ records use the 30-day production retention class in ADR-0032. Expiry is blocked while an active replay, quarantine, legal/security hold, or incident reference requires the record; any longer hold uses an explicitly access-controlled operational archive rather than silently extending every Kafka DLQ.

Consumers that require aggregate ordering also quarantine the affected aggregate key after a terminal processing failure. They do not apply later events for that aggregate until replay, rebuild, or operator reconciliation restores a contiguous sequence. Unrelated aggregate keys and other consumer groups continue processing.

Under ADR-0029, successful consumers atomically commit their owned-state mutation, contract idempotency key, and aggregate checkpoint before committing the Kafka offset. Sequence gaps and conflicting duplicates enter this same aggregate-quarantine path. DLQ and replay retention therefore constrain dedupe-record retention.

## Consequences
Failure recovery remains owned by the context that failed without blocking consumers that processed the event successfully. DLQ writes and source-offset commits must be coordinated so a crash can cause duplicate DLQ records but cannot silently discard the source event. DLQ tooling, retention, access control, replay, aggregate quarantine state, and alerts become part of each consumer's production operations. DLQ records are operational artifacts, not domain events or Game Integrity entries.
