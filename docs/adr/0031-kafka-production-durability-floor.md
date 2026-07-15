# Kafka Production Durability Floor

## Status
Accepted

## Context
Transactional outboxes protect events until CDC publishes them, but weak broker acknowledgment or replica policy could acknowledge a record that is then lost during a broker or node failure. Consumer offsets, connector offsets, DLQs, and topology-cutover buffers are also production state and require the same failure assumptions as domain topics.

## Decision
Production Kafka runs with at least three brokers across distinct Kubernetes nodes and, where available, distinct failure zones. Every domain, projection, DLQ, replacement, and Kafka Connect internal topic uses replication factor 3 and `min.insync.replicas=2`. Unclean leader election is disabled and replica placement is rack/zone aware.

All application, Debezium/Kafka Connect, DLQ, replay, and administrative producers require `acks=all` and idempotent production. Producer retries remain enabled with ordering-safe in-flight limits; an insufficient in-sync replica set fails publication rather than weakening acknowledgment. Kafka Connect config, offset, and status topics use replication factor 3, and connector producer overrides must preserve the same acknowledgment/idempotence policy.

Local `kind` may use a single broker, replication factor 1, and `min.insync.replicas=1` to keep local resource use bounded. This is an explicit non-HA topology: it verifies contracts, ordering, retries, CDC, and recovery behavior but makes no broker-failure durability claim. Environment validation rejects local durability settings in production.

## Consequences
A single broker or node loss does not lose acknowledged records while two in-sync replicas remain. Broker and zone capacity, replica-reassignment traffic, under-replicated partitions, ISR shrink, produce failures, and leader distribution become required operational signals. During a second correlated failure or maintenance event, publication may stop instead of accepting under-replicated writes; Postgres outboxes and consumer retries retain pressure until Kafka recovers. Production load and failure tests require a multi-broker topology outside the local `kind` lane.
