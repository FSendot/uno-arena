# 00 Overview and Traceability

This architecture package captures the accepted target design for UnoArena and splits it into focused views:

- `01-context-and-container-view.md`
- `02-bounded-context-architecture.md`
- `03-communication-patterns.md`
- `04-persistence-by-context.md`
- `05-capacity-sketch.md`
- `06-cross-cutting-concerns.md`
- `07-sequence-diagrams.md`

It is intentionally traceable rather than expansive. The goal is to show how the accepted decisions fit together, what each bounded context owns, which stores are authoritative, and how commands and events move across boundaries.

## Design Principles

- The BFF is the only external boundary.
- Clients use REST command envelopes and SSE for realtime updates.
- This implementation uses the repo-owned simple CLI as the sole client/test interface; a graphical interface is deferred and must preserve the BFF-only REST/SSE boundary.
- Gameplay rules live in Room Gameplay; technical integrity lives in Game Integrity.
- Rejected commands never produce domain events or Game Integrity log entries; they emit structured operational/security audit records.
- Postgres is the authoritative store where a context needs transactional state.
- Redis is a cache or scheduling index only when justified.
- Kafka carries bounded-context-specific event streams, not a single undifferentiated bus.
- Postgres-backed contexts publish Kafka integration events through transactional outboxes captured from WAL by Debezium; application request paths do not poll or synchronously publish those events.
- Each bounded context owns one physical authoritative database; native table partitioning and indexes may organize high-volume data without crossing that context boundary.
- Each active room executes in its own lifecycle-bound Kubernetes state-machine pod.
- Spectator-safe data is derived, filtered, and privacy-aware by design.
- Spectators may connect while a room is `waiting`, `locked`, or `in_progress`; admission is denied in `completed`/`cancelled`, and terminal room/match state closes existing spectator streams.
- Ad-hoc host leave before lock/start reassigns to the lowest occupied seat or cancels immediately if empty; after lock/start the host label has no gameplay authority.
- Uno windows publish absolute UTC `expiresAt` plus the opening room sequence; CLI countdown is advisory and the server exclusively decides timeliness.
- Realtime recovery and auditability are first-class requirements, not afterthoughts.
- OpenAPI describes the BFF command, snapshot, and SSE surface (including `409 snapshot_required`); AsyncAPI describes the event and stream contracts.
- **Capability status:** Capability mode uses real HTTP service paths with bounded memory limiters/session repo and explicit GI/Analytics memory; it must not be read as the durable path. Identity/Room/Ranking/Tournament Postgres, GI KurrentDB, Analytics ClickHouse HTTP, Spectator/Ranking/Tournament Redis projections, Room Redis timers, Gateway Redis rate-limit + LiveFeed SSE, and the declared franz-go consumers are implemented. A clean ARM64 kind lane has deployed the foundation and all eight services with live Connect/Server CDC. Spectator and Analytics full recovery-worker proofs passed, so their rebuilders are enabled only in kind and remain disabled in default/staging/production values.

## Decision Traceability

| Accepted decision | Where it is reflected |
| --- | --- |
| BFF is the only external boundary; clients never call microservices directly | `01-context-and-container-view.md`, `03-communication-patterns.md` |
| Repo-owned simple CLI is the sole client/test interface for this implementation; graphical UI deferred without changing BFF-only REST/SSE boundary | `01-context-and-container-view.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `client-checkpoint/README.md` |
| REST command envelope + SSE realtime, one logical Realtime Gateway/BFF | `01-context-and-container-view.md`, `03-communication-patterns.md` |
| Compact command endpoint maps to the existing command catalog; `commandId` idempotency key, `type` command name, `expectedSequenceNumber` for room mutations | `03-communication-patterns.md`, `07-sequence-diagrams.md` |
| Rejected commands emit structured operational/security audit records only; no domain events and no Game Integrity append | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` |
| Spectators may connect in `waiting`/`locked`/`in_progress`; denied and streams close after `RoomCompleted`/`RoomCancelled` for the complete match/room | `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `07-sequence-diagrams.md` |
| Ad-hoc host leave before lock/start reassigns to lowest occupied seat or cancels immediately; post-lock host has no gameplay authority | `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `07-sequence-diagrams.md` |
| Uno windows publish absolute UTC `expiresAt` and opening room sequence; CLI countdown is advisory and server decides timing | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `07-sequence-diagrams.md` |
| Game Integrity is a separate BC/service; KurrentDB 26.0.3 LTS is authoritative append-only per room/game; internal-only audit/replay API | `02-bounded-context-architecture.md`, `04-persistence-by-context.md`, `03-communication-patterns.md`, `docs/adr/0035-kurrentdb-26-lts-with-architecture-selected-images.md` |
| Replay-sensitive Game Integrity payloads use per-game envelope encryption; storage administrators cannot read seeds without audited key-provider authorization | `02-bounded-context-architecture.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md`, `docs/adr/0024-envelope-encryption-for-game-integrity-payloads.md` |
| Room Gameplay owns Uno rules and calls Game Integrity before broadcast | `02-bounded-context-architecture.md`, `07-sequence-diagrams.md` |
| One dedicated Kubernetes state-machine pod executes each active room | `01-context-and-container-view.md`, `02-bounded-context-architecture.md`, `05-capacity-sketch.md`, `docs/adr/0040-dedicated-state-machine-pod-per-room.md` |
| Room Gameplay commits Postgres snapshots plus integration/realtime outboxes; Debezium routes the realtime outbox to ordered Redis player feeds; timers remain durable in Postgres with Redis only as a scheduling index | `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `docs/adr/0015-redis-streams-for-realtime-sse-delivery.md` |
| Existing Room mutations serialize with `SELECT ... FOR UPDATE` through Game Integrity confirmation and the local Postgres commit | `03-communication-patterns.md`, `04-persistence-by-context.md`, `07-sequence-diagrams.md`, `docs/adr/0019-room-commands-use-pessimistic-row-locking.md` |
| Room timer workers claim partitioned Redis sorted-set entries with visibility leases and revalidate authoritative Postgres deadlines before mutation | `03-communication-patterns.md`, `04-persistence-by-context.md`, `07-sequence-diagrams.md`, `docs/adr/0020-redis-sorted-sets-for-room-timer-scheduling.md` |
| Tournament Orchestration uses Postgres, sharded workers, async `MatchCompleted` consumption, and Redis bracket projection | `02-bounded-context-architecture.md`, `04-persistence-by-context.md`, `07-sequence-diagrams.md` |
| Ranking is async with Postgres authoritative and Redis leaderboard cache | `02-bounded-context-architecture.md`, `04-persistence-by-context.md` |
| Identity uses external IdP plus internal session/ACL, Postgres authoritative with Redis cache, and `SessionInvalidated` closes SSE through BFF | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md` |
| Every state-changing command uses authoritative Identity Postgres validation; positive Redis/gateway cache entries cannot authorize mutation | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `docs/adr/0021-authoritative-session-validation-for-state-changing-commands.md` |
| Identity uses a provider-neutral OIDC anti-corruption layer; Keycloak is the local/`kind` reference and test provisioning is non-production only | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `docs/adr/0023-provider-neutral-oidc-with-keycloak-reference.md` |
| Spectator View is a Redis materialized projection rebuilt from committed safe events and sanitized snapshots, and owns privacy filtering | `02-bounded-context-architecture.md`, `04-persistence-by-context.md` |
| Analytics and Public Read Models is a narrow BC with ClickHouse and public derived, non-authoritative analytics | `02-bounded-context-architecture.md`, `04-persistence-by-context.md` |
| Kafka topics are bounded-context-specific; event schema versions are explicit | `03-communication-patterns.md` |
| Debezium PostgreSQL CDC and the Outbox Event Router publish context-owned transactional outboxes to Kafka | `03-communication-patterns.md`, `04-persistence-by-context.md`, `docs/adr/0016-debezium-cdc-for-transactional-outboxes.md` |
| Time-partitioned outboxes are reclaimed only after the corresponding CDC source offset passes the sealed partition's WAL high-water LSN plus a safety window | `03-communication-patterns.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md`, `docs/adr/0026-lsn-gated-outbox-partition-reclamation.md` |
| Each Postgres-backed context initializes an empty database through its own advisory-locked Kubernetes bootstrap Job; runtime pods validate schema version but hold no DDL privilege | `01-context-and-container-view.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md`, `docs/adr/0027-context-owned-postgres-bootstrap-jobs.md` |
| Each Postgres-backed context owns one HA database boundary with one primary, two synchronous standbys, primary-plus-one acknowledgment, fenced promotion, and a stable writer endpoint | `01-context-and-container-view.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `docs/adr/0033-one-ha-postgres-cluster-per-context.md` |
| Store-specific DR uses Postgres PITR (5m/30m), KurrentDB plus key-history recovery (5m/60m), ClickHouse backup/backfill (24h/4h), rebuild-only Redis, and quarterly isolated restore drills | `01-context-and-container-view.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `docs/adr/0034-store-specific-backup-and-disaster-recovery.md` |
| Analytics bootstraps ClickHouse explicitly; Game Integrity lazily creates expected-revision streams; Redis owners validate versioned keyspaces and load Lua without treating Redis as schema-managed truth | `01-context-and-container-view.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md`, `docs/adr/0028-store-specific-initialization.md` |
| Kafka DLQs are owned per consumer group; ordered consumers quarantine only the affected aggregate until contiguous recovery | `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `docs/adr/0017-consumer-owned-dlqs-and-aggregate-quarantine.md` |
| Consumers atomically commit owned state, the AsyncAPI idempotency key, and ordered aggregate checkpoints before committing Kafka offsets | `03-communication-patterns.md`, `04-persistence-by-context.md`, `06-cross-cutting-concerns.md`, `docs/adr/0029-transactional-consumer-idempotency-and-checkpoints.md` |
| Ordered Kafka topic partition counts are immutable after creation; capacity expansion uses a versioned topic, durable producer watermark, old-group drain, and checkpoint-ordered release | `03-communication-patterns.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `docs/adr/0030-immutable-partition-counts-for-ordered-kafka-topics.md` |
| Production Kafka acknowledges only replicated writes: RF3, minimum ISR2, `acks=all`, idempotent producers, rack-aware placement, and disabled unclean leader election | `01-context-and-container-view.md`, `03-communication-patterns.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `docs/adr/0031-kafka-production-durability-floor.md` |
| Kafka retention is classed by recovery need: 6h spectator-safe, 24h metrics/Identity invalidations, 7d business/superseded topics, and 30d DLQs; authoritative stores own long-term recovery | `03-communication-patterns.md`, `04-persistence-by-context.md`, `05-capacity-sketch.md`, `06-cross-cutting-concerns.md`, `docs/adr/0032-kafka-retention-classes-and-recovery-boundaries.md` |
| Spectator/Analytics post-retention recovery uses Kafka rebuild-request topics plus context-owned internal snapshot/backfill APIs; live kind proofs passed and Deployments are kind-only enabled | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `contracts/asyncapi/kafka-v1.yaml`, `docs/adr/0039-context-owned-snapshot-backfill-and-kafka-rebuild-requests.md` |
| Redis Streams carries ordered realtime player/spectator delivery; Room uses Debezium Server CDC to Redis while Spectator View publishes atomically inside Redis; Kafka remains the cross-context integration bus | `03-communication-patterns.md`, `docs/adr/0015-redis-streams-for-realtime-sse-delivery.md` |
| Istio Ambient provides mandatory production east-west L4 mTLS and SPIFFE workload identity without per-pod sidecars; local `kind` verifies the same L4 security path, and L7 waypoints are opt-in | `01-context-and-container-view.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `docs/adr/0025-istio-ambient-for-production-mtls.md` |
| A disposable empty `kind` cluster is the sole local deployment and live-integration environment; offline renders and capability/fake-BFF checks are process-level preflight evidence only, not topology or deployment evidence | `01-context-and-container-view.md`, `06-cross-cutting-concerns.md`, `infrastructure/kind/README.md`, `docs/adr/0045-kind-as-empty-cluster-deployment-acceptance.md` |
| Centralized application logs flow through Alloy into Loki TSDB backed by the same S3-compatible contract locally and in production; kind supplies disposable MinIO | `01-context-and-container-view.md`, `06-cross-cutting-concerns.md`, `docs/adr/0046-loki-tsdb-on-s3-compatible-object-storage.md` |
| OpenTelemetry traces flow through Alloy into Tempo/S3, Prometheus scrapes OpenTelemetry-exported metrics, and live acceptance proves three context-owned business counters plus the canonical CDC trace | `01-context-and-container-view.md`, `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `docs/adr/0047-opentelemetry-alloy-and-tempo-for-distributed-tracing.md` |
| OpenAPI and AsyncAPI are the contract intent for HTTP and event surfaces | `03-communication-patterns.md`, `06-cross-cutting-concerns.md`, `contracts/openapi/bff-v1.yaml`, `contracts/asyncapi/kafka-v1.yaml` |
| BFF player/spectator snapshot routes and SSE `409 snapshot_required` for unknown/evicted Last-Event-ID | `02-bounded-context-architecture.md`, `03-communication-patterns.md`, `contracts/openapi/bff-v1.yaml` |
| Offline capability mode (real HTTP paths + bounded memory / GI memory) is distinct from required Postgres/Kafka/Redis/KurrentDB/ClickHouse adapters; Kafka envelope authoritative vs HTTP bridge transform | `01-context-and-container-view.md`, `03-communication-patterns.md`, `04-persistence-by-context.md`, `README.md`, `CHANGELOG-design.md` |
| Helm Service/peer port 8080; Room/Tournament workers gated by env; Spectator/Analytics projection rebuilders enabled only in kind after live proofs; `/ready` not weakened for missing durable adapters | `README.md`, `services/*/helm/**`, `06-cross-cutting-concerns.md` |

## Architectural Qualities

### Security and privacy

- Private player state never leaks into spectator projections.
- Session invalidation must be enforceable even while SSE streams are open.
- Internal service calls should rely on service identity, least privilege, and correlation headers.
- Audit and replay interfaces are internal-only.

### Reliability

- Accepted gameplay commands must be durably confirmed before broadcast.
- Realtime fan-out should survive worker restarts without corrupting room state.
- Timers must recover from Redis loss because Redis is not authoritative.
- Tournament advancement must be replayable and idempotent.
- CDC connector interruption must not lose committed outbox events; recovery resumes from the recorded WAL position and consumers tolerate duplicate delivery.
- Outbox cleanup fails closed when partition sealing or connector progress cannot be proven; age alone never authorizes deletion.
- Service startup fails closed on absent or unexpected Postgres schema versions; runtime pods never race or repair DDL.
- Postgres writes fail when primary-plus-one synchronous acknowledgment is unavailable; failover never routes authoritative reads to a stale standby or permits split brain.
- Cluster replication does not replace backup: corruption or cluster loss recovers from isolated encrypted archives whose RPO/RTO and key history are proven by restore drills.
- ClickHouse schema mismatch blocks Analytics; KurrentDB capability/policy failure blocks Game Integrity writes; Redis keyspace/script incompatibility blocks dependent flows without an authoritative fallback.
- A poison event cannot silently skip an ordered aggregate: terminal failure is durably written to the consumer-owned DLQ and the aggregate is quarantined until contiguous recovery.
- Consumer crashes after state commit but before offset commit cause harmless duplicate delivery; gaps or conflicting duplicates quarantine only their aggregate.
- Kafka capacity changes cannot repartition a live ordered topic; replacement-topic records remain buffered until the sealed old epoch is published and consumed completely.
- Kafka publication fails when fewer than two replicas remain in sync; producers never weaken acknowledgment to preserve availability.
- A consumer that exceeds Kafka retention enters an explicit sanitized snapshot or context-owned backfill path via rebuild-request topics and owning-context internal APIs; it never advances checkpoints across an unexplained gap.
- A Room Gameplay database outage affects the bounded context and fails closed; no fallback may silently create authoritative room state elsewhere.
- Competing commands for one Room serialize before Game Integrity append; lock waits and the internal append call are bounded so a hot or unhealthy Room cannot consume an unbounded worker pool.
- Timer claims are at least once and disposable; stable keys, visibility leases, and Room-side Postgres revalidation prevent duplicate or stale expiry effects.
- Identity validation fails closed for mutations. A revocation commit prevents all subsequently validated commands even if cache invalidation or stream closure lags.
- Identity database failures fail state-changing commands closed; cache state never substitutes for authoritative validation.
- OIDC issuer/JWKS/provider failures fail closed for authentication; production cannot start with test provisioning, direct password grants, development issuers, or insecure OIDC transport enabled.
- Game Integrity fails closed when encryption keys are unavailable; it never falls back to plaintext append. Historical replay fails explicitly if an authorized key version cannot be recovered.
- Enrolled workloads reject plaintext east-west traffic. Ambient control-plane or ztunnel failures fail according to strict mTLS policy rather than silently bypassing workload identity.

### Operability

- Every command and event should be traceable by `correlationId`, `commandId`, and aggregate identifiers.
- Rejected commands must leave structured operational/security audit records with `commandId`, `correlationId`, session/player, room/tournament when known, rejection reason, submitted/current sequence when applicable, and timestamp.
- Each bounded context owns its operational datastore and schema lifecycle; current Postgres deployment supports empty bootstrap only, with upgrade migrations deferred until before the first stateful upgrade.
- Projection rebuilds should be possible from durable upstream facts.
- Monitor Debezium connector health, replication-slot/WAL retention, capture lag, and oldest unpublished outbox age per context database.
- Monitor outbox partition age/size, seal high-water LSN, connector-offset distance, reclaimable partition count, cleanup failures, and projected exhaustion time per CDC pipeline.
- Monitor bootstrap Job outcome/duration/retries, advisory-lock wait, detected schema version, and workload readiness blocked by schema mismatch.
- Monitor Postgres primary/standby health, synchronous quorum and lag, WAL retention, fenced promotion, writer-endpoint convergence, pool recovery, unknown outcomes, and CDC resume position per context.
- Monitor backup/WAL archive age and failures, restore-point coverage, storage growth, key-history recoverability, last successful drill, achieved RPO/RTO, invariant/replay failures, and overdue evidence.
- Monitor ClickHouse bootstrap/version checks, KurrentDB capability/policy validation, and Redis capability/keyspace/script-load or `NOSCRIPT` recovery failures.
- Monitor retry rate, consumer-owned DLQ depth/age, aggregate quarantine count/age, and replay outcomes per consumer group.
- Monitor idempotency conflicts, aggregate checkpoint gaps/regressions, owned-store-to-offset commit failures, dedupe retention/cleanup lag, and retained replay references.
- Monitor partitions/leaders per broker, per-partition bytes/rate/lag/skew, producer topology epoch, cutover watermark progress, old-topic drain lag, new-topic buffer depth, and quarantined cutover gaps.
- Monitor broker/zone availability, ISR shrink, under-replicated/offline partitions, unclean-election configuration drift, produce acknowledgment failures, replica reassignment, and Kafka Connect internal-topic replication.
- Monitor storage by retention class, oldest consumer/DLQ age, time-to-expiry, retention/config drift, snapshot/backfill entries and outcomes, and topic deletion blockers.
- Monitor Room database pool saturation/latency/error rate, table-partition skew, hot-room contention, and CDC lag.
- Monitor Room row-lock wait/hold duration, lock timeout rate, queued commands per hot Room, Game Integrity latency while locked, and append-confirmed/local-commit-failed reconciliation count.
- Monitor scheduled/in-flight timer depth by partition/type, oldest overdue deadline, claim/ack/retry/lease-expiry rates, Redis latency, and deadline-index rebuild lag.
- Monitor authoritative validation latency/error/timeout rate, Identity database pool and index load, cache early-rejection hit rate, revocation-to-stream-close latency, and rejected superseded-session commands.
- Monitor OIDC discovery/JWKS refresh success and age, issuer/audience/claim rejection rates, provider latency/errors, key rotations, callback failures, and test-provisioning gate status.
- Monitor Game Integrity encrypt/decrypt latency/errors, KMS/HSM availability, active/historical key versions, rotation age, decrypt/export audit outcomes, and seed/order commitment verification failures.
- Monitor Istio control-plane/CNI/ztunnel health, certificate issuance/rotation, mTLS handshake failures, denied L4 flows, per-node tunnel saturation, and policy/config rollout status.

## NFR Targets

These are design targets for implementation and review, not product promises.

- Player command response should target p95 150-250 ms from BFF receipt to accepted/rejected command outcome under normal regional load.
- Player SSE delivery should target p95 under 250 ms after authoritative commit.
- Spectator SSE delivery should target p95 500 ms-1 s, with controlled coalescing allowed under load.
- Game Integrity append should target p95 under 50 ms inside the active region.
- Session invalidation should close superseded SSE streams within 2 seconds of `SessionInvalidated` publication.
- Tournament bracket projections should be seconds-fresh and explicitly versioned; they must not block authoritative advancement.
- Ranking and analytics are eventually consistent and may lag seconds to minutes during large bursts.
- Gameplay and tournament writes must remain consistent under retries and duplicated submissions.
- Replay and audit paths must be available without depending on live gameplay services.
- Privacy controls must be enforced in the projection layer, not by client convention.
