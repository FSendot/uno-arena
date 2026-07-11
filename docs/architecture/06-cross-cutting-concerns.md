# 06 Cross-Cutting Concerns

## Security

- Use the external IdP for user authentication.
- Use provider-neutral OIDC validation inside Identity; the BFF and downstream contexts never parse or trust provider claims directly.
- Allow Keycloak test provisioning/direct credential flows only in explicitly non-production environments; production readiness rejects those flags and insecure issuer/TLS/JWKS configuration.
- Enforce authorization in Identity and the BFF before command dispatch.
- Scope service credentials per bounded context.
- Keep internal audit and replay APIs private.
- Envelope-encrypt Game Integrity seeds, deck order, private card material, reservations, and replay-sensitive payloads before storage; mesh mTLS and disk encryption do not replace this control.
- Grant KMS/HSM decrypt capability only to scoped Game Integrity replay/audit identities, with break-glass authorization and durable decrypt/export audit records.
- Treat spectator traffic as untrusted by default.
- Admit spectators only while a room is `waiting`, `locked`, or `in_progress` subject to public/private authorization; deny new connections and close existing streams after `RoomCompleted` or `RoomCancelled`.
- Keep microservices off the public internet; the BFF is the only external surface.
- Enroll production application, worker, CDC, and datastore namespaces in Istio Ambient; require strict mTLS and dedicated Kubernetes service accounts with default-deny L4 authorization.
- Use the repo-owned simple CLI as the sole client/test interface for this implementation; deferred graphical clients must preserve the BFF-only REST/SSE boundary.

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
- authoritative Identity Postgres validation for each state-changing command; no cached-positive authorization fallback
- privacy-filtered projections
- schema versioning on events
- durable timer state outside Redis, with absolute UTC `expiresAt` and opening room sequence as server authority
- structured operational/security audit records for rejected commands
- spectator admission limited to non-terminal room states, with terminal stream closure
- bounded-context-specific Kafka topics
- least-privilege PostgreSQL logical-replication users and isolated Debezium connector credentials

## Observability

- Propagate `correlationId` across the BFF, services, workers, and consumers.
- Include `commandId`, `eventId`, `causationId`, `roomId`, `gameId`, `tournamentId`, and `schemaVersion` where applicable.
- Log command acceptance, rejection, append confirmation, and broadcast completion.
- Trace internal hops from envelope to durable append to downstream publication.
- Expose metrics for SSE connection counts, command latency, append latency, consumer lag, timer lag, and projection rebuild duration.
- Emit structured operational/security audit records for every rejected command with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason, submitted/current sequence when applicable, and timestamp. These records are not domain events and are not Game Integrity entries.
- Track tournament provisioning batch success/failure, DLQ depth, round projection lag, and session invalidation close latency.
- Track Debezium connector availability, source WAL/replication-slot retention, connector offsets, capture lag, and oldest committed outbox event not yet visible in Kafka.
- Track the Room realtime Debezium Server Redis sink separately: replication-slot/WAL lag, Redis sink acknowledgments, offset durability, oldest undelivered player-feed entry, and duplicate/sequence-gap counts at gateways.
- Track each outbox's active/sealed partition sizes, seal high-water LSN, durable source offset, offset-to-high-water distance, safety-window age, reclaimable backlog, cleanup failures, and projected storage exhaustion.
- Track retries, DLQ publish failures, DLQ depth/oldest age, quarantined aggregate count/oldest age, replay attempts, and reconciliation outcomes per consumer group.
- Track idempotency-key conflicts, aggregate checkpoint gaps/regressions, owned-store commit failures, post-state/pre-offset duplicate deliveries, dedupe retention/cleanup lag, and cleanup blocked by replay/quarantine references.
- Track topic partition count against its immutable declaration, leader/replica distribution, per-partition throughput/size/lag/skew, topology epoch, cutover watermark, old-group drain, replacement buffer, and cutover quarantine.
- Track broker and failure-zone availability, ISR shrink, under-replicated/offline partitions, leader imbalance, replica reassignment/recovery, `acks=all` failures, idempotent-producer errors, and Connect internal-topic replication.
- Track bytes and time retained per topic class, oldest unconsumed/DLQ record, time-to-expiry, retention-policy drift, active replay/quarantine/rollback holds, snapshot/backfill activation, and checkpoint reconciliation outcome.
- Track each Postgres cluster's primary/standby health, sync state and flush lag, quorum loss, WAL growth, fenced promotions, writer-endpoint convergence, pool reconnect storms, unknown commit outcomes, and CDC offset/resume gaps.
- Track WAL/archive and backup age, upload/encryption failures, restore-point coverage, backup storage growth, historical key recoverability, last successful drill/evidence age, measured RPO/RTO, invariant/replay failures, and secure teardown completion.
- Track Room request distribution, native table-partition skew, hot rooms, database pool saturation/availability, and both CDC pipelines for the context database.
- Track Room row-lock wait and hold percentiles, lock/statement timeout counts, transaction duration, Game Integrity latency inside the lock, and reconciliation after append-confirmed/local-commit-failed cases.
- Track Room timer sorted-set and in-flight depth by bucket/type, oldest overdue score, claim batch latency, lease expiry/retry rate, definitively stale acknowledgments, and Postgres-to-Redis rebuild progress.
- Track authoritative Identity validation p50/p95/p99, Postgres partition/pool saturation, fail-closed timeout/error rate, stale-cache early rejections, superseded-session attempts, and validation-to-revocation race outcomes.
- Track Identity database pool/index saturation, validation lookup failures, database availability, and Debezium lag.
- Track OIDC discovery/JWKS refresh age/failures, provider latency/errors, issuer/audience/signature/nonce rejection counts, claim-mapping errors, Keycloak realm readiness, and production test-provisioning gate violations.
- Track Game Integrity encryption/decryption latency and failures, key-provider availability, active/historical key versions, rotation/rewrap progress, decrypt/export requests and denials, and commitment mismatches.
- Track istiod/CNI/ztunnel availability, certificate age/rotation failures, mTLS handshakes, denied connections, service-account identity mismatches, HBONE connection/throughput saturation, and policy rollout drift.

## Error Handling

- Stale room mutations should be rejected cleanly, not retried blindly.
- Identity/Postgres validation failure rejects state-changing commands; cached-positive sessions cannot be used as an availability fallback.
- Existing Room mutations acquire the aggregate row lock before sequence and rule validation. Lock timeout is an explicit transient failure, not permission to bypass serialization or append optimistically.
- Rejected commands emit structured operational/security audit records only; they never produce domain events or Game Integrity entries.
- Timer and reconnect handlers should be idempotent and decide Uno openness from persisted absolute UTC `expiresAt` plus opening room sequence.
- Timer workers treat Redis claims as at-least-once hints: only Room's locked Postgres transaction can decide whether a deadline is current, stale, duplicate, or applicable.
- Projection consumers should tolerate duplicate or out-of-order delivery where the upstream contract allows it.
- Replay or audit failures must not take down live gameplay.
- Encryption failure must fail a Game Integrity write before append; plaintext fallback is forbidden. Replay/audit key failure remains isolated from already-running gameplay unless the unavailable key is required for a live game operation.
- Saga fallback events should be emitted only when fallback changes business process state, such as quarantine, reconciliation, or recorded retry state.
- Infrastructure fallback such as broker redelivery, DLQ depth, worker restarts, and transient timeouts should be observable through metrics/traces unless it changes domain state.
- A consumer commits a poison source offset only after its consumer-owned DLQ acknowledges the original envelope and failure metadata. Ordered aggregates remain quarantined until replay/rebuild proves continuity.
- A successful consumer commits owned state, the contract idempotency key, and its sequence/version checkpoint before committing the Kafka offset. A duplicate is ignored only when immutable identity agrees; gaps or conflicts quarantine the aggregate.
- Outbox reclamation fails closed when a partition is not sealed, its WAL high-water LSN is missing, the matching source offset is absent/incomparable, the connector is unhealthy, or the safety window remains open. Age-only deletion is forbidden.

## Schema and Contract Evolution

- Every event should declare `schemaVersion`.
- Bounded contexts should evolve their own contracts independently.
- Kafka event names and SSE payloads should be versioned deliberately.
- Public analytics schemas may evolve separately from gameplay schemas.
- The standardized outbox router columns are infrastructure metadata; the JSON payload and AsyncAPI schema remain owned and versioned by the producing bounded context.

## Deployment and Networking

- Istio Ambient is required for production east-west L4 mTLS and workload identity. Enroll only intended namespaces and enforce strict mode plus explicit L4 authorization policies.
- Use per-node `ztunnel`; do not inject sidecars. Deploy L7 waypoint proxies only for an approved, concrete L7 routing or policy requirement.
- Retain application service credentials, authorization, correlation, Kubernetes NetworkPolicy, and least privilege; mesh identity complements rather than replaces them.
- Keep the BFF and stateful services close enough to reduce tail latency where practical.
- Deploy Keycloak only as the local/`kind` reference IdP. Production may use a managed standards-compliant provider configured through issuer/audience/JWKS settings without changing Identity's internal contract.
- Run Kafka Connect/Debezium privately, with one Kafka-bound PostgreSQL connector per context-owned database. Run one separate Debezium Server PostgreSQL-to-Redis pipeline for Room Gameplay's realtime outbox. Each replication identity is limited to logical replication plus its owned outbox table.
- Admission policy rejects in-place partition increases for ordered topics. A replacement topic, ACLs, connector routes, DLQ, dashboards, and consumer group must exist before a topology-epoch switch; old-topic deletion is a separate explicit retention action.
- Production admission requires at least three brokers, RF3, minimum ISR2, rack/zone placement, `acks=all`, idempotent producers, and disabled unclean election for every topic class. It rejects the RF1/min-ISR1 settings permitted in local Docker Compose and `kind`.
- Production topic policy applies the declared retention class and blocks deletion/reduction while consumer lag or replay/quarantine/rollback references would cross the new boundary. Local retention overrides are allowed only outside production.
- Run the outbox rotation/reclamation controller as an operational worker with database partition-maintenance rights and read-only access to the matching durable connector offset. It has no authority to mutate aggregate tables, fabricate offsets, or bypass the safety gate.
- Configure exactly one authoritative database endpoint per bounded context. Contexts own their schema definition, bootstrap lifecycle, connection pool, native table partitioning, and CDC configuration; no service receives another context's database credentials. Upgrade migrations require a later decision before the first stateful upgrade.
- The production endpoint represents one primary plus two synchronous standbys, requires primary-plus-one WAL-flush acknowledgment, and fences promotion to one writer. Runtime/CDC clients use promotion-aware writer discovery; stale standby endpoints are never used for authoritative decisions. Local single-instance endpoints are rejected as production HA configuration.
- Backup identities are separate from runtime/DDL roles and write encrypted material outside the primary cluster failure domain. Quarterly restore Jobs run only in isolated infrastructure with scoped decrypt access, record evidence, and securely destroy restored data after verification.
- Run one context-owned bootstrap Job before each Postgres-backed service and its CDC connector. The Job uses one admin advisory-locked transaction that mutates roles only after an empty-state decision, applies DDL through the dedicated bootstrap role, and grants runtime DML without DDL; exact/drift states receive zero mutations. Deployment fails on any schema version other than empty or exact-current (exactly one migrations row and one bootstrap-meta row), and never resets data implicitly.
- Run the Analytics-owned ClickHouse bootstrap Job before Analytics workloads. Runtime credentials have no DDL privilege and unexpected non-empty schema fails unchanged.
- Do not run schema Jobs for KurrentDB or Redis. Game Integrity validates deployment-provisioned KurrentDB policy and lazily creates expected-revision streams. Redis owners validate versioned key prefixes/capabilities and load scripts on every target node, with safe `NOSCRIPT` reload.

## OpenAPI and AsyncAPI Intent

- OpenAPI under `contracts/openapi/bff-v1.yaml` formalizes the BFF command envelope, snapshot routes, status/error response shape (including `409 snapshot_required`), SSE endpoints, and versioned resources.
- Command envelopes should use a discriminated schema where `type` maps to the design command catalog and `payload` is a `oneOf` by command type.
- AsyncAPI under `contracts/asyncapi/kafka-v1.yaml` formalizes topic names, event schemas (including Room `GameCompleted`/`MatchCompleted` bodies), producers, consumers, partition keys, idempotency keys, and schema-version policy. The Kafka envelope is authoritative; offline HTTP bridges are documented destination-specific transforms of the same event names and canonical fields and do not claim Kafka is wired.
- Scope service credentials per bounded context. Exact cross-chart credential equality and producer-scope requirements are documented in root `README.md` and Helm `values.yaml` comments (secret key names only; no hardcoded secret values).
- Helm Kubernetes Services and peer URLs use port 8080 consistently. Worker Deployments (Room timer, Tournament provisioning, Spectator/Analytics projection rebuilders) stay `enabled: false` until binaries implement `WORKER_ROLE` loops and production worker adapters exist — enabling them today would launch duplicate HTTP servers.
- Do not weaken `/ready` outside explicit capability mode: configured Gateway requires Redis rate-limit + LiveFeed clients (ping every configured Redis client); Room Postgres session and Game Integrity KurrentDB adapters intentionally keep the service not-ready until durable stores are reachable. Gateway Kafka SessionInvalidated consumption and Debezium player-feed delivery remain pending.

## Testing Strategy

- Contract tests for command envelopes, snapshot schemas, and SSE payloads, including Uno `expiresAt` plus `openingSequence` and `409 snapshot_required`.
- Integration tests for room append-before-broadcast behavior.
- Rejection tests that assert structured operational/security audit records and the absence of domain events or Game Integrity appends.
- Replay tests for Game Integrity.
- Projection tests for Spectator View privacy filtering.
- Spectator admission tests for `waiting`/`locked`/`in_progress`, denial after `RoomCompleted`/`RoomCancelled`, and terminal stream closure.
- Host-leave tests for ad-hoc lowest-occupied-seat reassignment or immediate cancel before lock/start, and no host gameplay authority after lock/start.
- End-to-end tests for session invalidation closing live streams.
- Concurrency tests linearize command authorization against new-login/revocation commits: pre-revocation admissions may finish, while every post-commit validation rejects even with deliberately stale Redis/gateway cache entries.
- Identity persistence tests prove unique issuer/subject mapping, indexed token-hash lookup, single-active-session replacement, revocation ordering, database-outage fail-closed behavior, and no cross-context database access.
- OIDC tests use a real Keycloak realm locally to prove discovery/JWKS validation, issuer/audience/signature/expiry/nonce handling, subject mapping, signing-key rotation, provider outage behavior, and production rejection of test-only provisioning/direct-grant settings.
- Game Integrity security tests inspect KurrentDB records/backups for absent plaintext seed/private-card material, verify commitment/replay correctness, exercise KMS outage and key rotation, prove historical-key retention, and assert authorization plus durable audit for every decrypt/export.
- Mesh integration tests verify strict mTLS, expected SPIFFE/service-account identities, default-deny behavior, explicit allowed flows, plaintext rejection, certificate rotation, ztunnel disruption/recovery, and absence of sidecars. The `kind` lane verifies behavior, not target-scale throughput.
- This implementation drives those checks through the repo-owned simple CLI as the sole client/test interface; graphical UI testing is deferred and must still use only the BFF REST/SSE boundary.
- CLI countdown display for Uno windows is advisory; tests assert server `expiresAt`, `openingSequence`, SSE corrections, and command outcomes rather than local clock authority.
- Offline capability adapters (capability-mode memory session / in-memory limiters / GI memory / Analytics memory / HTTP bridge transforms) are tested explicitly; durable Postgres/Kafka/Redis/KurrentDB adapters and Analytics Kafka ingestion remain architecture requirements not claimed as wired by those offline adapters. Analytics durable ClickHouse HTTP/store is implemented separately when configured.
- Production integration tests use real PostgreSQL logical replication, Kafka Connect, Debezium Outbox Event Router, and Kafka to prove commit-to-topic delivery, connector restart recovery, partition-key ordering, and duplicate-safe consumers.
- Realtime integration tests use real PostgreSQL logical replication, Debezium Server's Redis sink, and Redis Streams to prove commit-to-feed delivery, connector restart recovery, room stream routing, duplicate/sequence handling, and snapshot fallback after trimming.
- Outbox lifecycle tests seal time partitions under concurrent writers, record WAL high-water LSNs, prove cleanup remains blocked before matching durable offsets and safety windows, prove independent Room Kafka/Redis gates, and verify connector outage or missing/corrupt offset fails cleanup closed.
- Postgres bootstrap tests race Job retries to prove advisory-lock serialization, transactional empty-schema creation, exact-version no-op, rejection of partial/older/newer schemas without mutation, runtime-role DDL denial, and service readiness failure before successful bootstrap.
- Postgres HA tests lose a standby, lose the primary, and lose synchronous quorum to prove acknowledged commits survive, insufficient quorum fails writes, only one primary is writable, writer endpoints converge, pools recover without blind mutation retry, authoritative reads never hit stale replicas, and CDC resumes without skipping outbox records.
- Quarterly DR tests restore each Postgres context to a selected point and meet 5m/30m, restore KurrentDB with historical key access and meet 5m/60m, restore/backfill ClickHouse and meet 24h/4h, rebuild Redis from authoritative sources, verify schemas/invariants/commitments/checkpoints, and produce auditable teardown evidence.
- ClickHouse tests prove empty bootstrap, exact-version no-op, partial/older/newer rejection without mutation, and runtime-role DDL denial. KurrentDB tests prove lazy stream creation, event-ID idempotency, exact expected-revision conflicts, policy/readiness failure, encryption-key gating, ARM64 persistence, and restart recovery. Redis tests prove prefix/version validation, every-node script load, safe `NOSCRIPT` recovery, and rebuild after flush.
- Kafka failure-path tests inject transient and terminal consumer failures to prove bounded retries, DLQ-before-offset ordering, duplicate-safe DLQ publication, unrelated-key progress, aggregate quarantine, and successful replay/rebuild release.
- Consumer transaction tests crash after owned-store commit but before offset commit, replay exact duplicates, inject key/payload conflicts and sequence gaps, verify aggregate-only quarantine, and prove dedupe cleanup respects source retention plus the maximum replay/DLQ window and active references.
- Kafka topology tests assert environment-declared partition counts, reject in-place expansion, load-test the production 256/32 planning classes outside `kind`, and exercise versioned cutover in `kind` with reduced counts, new-topic buffering, durable producer watermark proof, old-group zero lag, checkpoint-ordered release, duplicate overlap, and aggregate-only gap quarantine.
- Kafka durability tests use at least three brokers to lose one broker/zone during produce and consume, verify acknowledged records survive, verify ISR-below-two fails publication, reject unclean election/config drift, and confirm domain, DLQ, replacement, and Connect internal topics retain RF3. The single-broker `kind` lane is excluded from HA assertions.
- Retention tests force each class across its boundary, verify spectator snapshot and Analytics backfill recovery, prove Identity remains authoritative after control expiry, reject checkpoint skips, retain DLQs/superseded topics while referenced, and validate local shortened windows make no production recovery claim.
- Room persistence tests prove all aggregate state and both outboxes use the Room context database, native partition pruning/index use where configured, transaction atomicity, and database-outage fail-closed behavior.
- Room concurrency tests race turn, jump-in, reconnect, and timer commands against one aggregate to prove one integrity append/commit order, stale rejection before append, bounded lock timeout, Game Integrity rollback, and append-confirmed commit-failure reconciliation.
- Timer integration tests use real Redis Lua claims and Postgres deadlines to prove bounded batches, exclusive leases, worker-crash reaping, duplicate/stale safety, bucket isolation, Redis flush rebuild, and overdue recovery.
