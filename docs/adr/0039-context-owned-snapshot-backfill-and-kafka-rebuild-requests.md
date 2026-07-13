# Context-Owned Snapshot/Backfill APIs and Kafka Rebuild-Request Topics

## Status
Accepted

## Context
ADR-0032 already requires post-retention recovery: Spectator rebuilds from a sanitized Room snapshot and resumes from its sequence; Analytics uses a safe context-owned backfill and may remain delayed without blocking gameplay. ADR-0017/0029 already require consumer-owned DLQs, aggregate quarantine, and no checkpoint advance across unexplained gaps. ADR-0013 already names Spectator/Analytics projection-rebuilder workers, but they remain disabled because the recovery **source and admission** path was never specified.

Application polling of transactional outboxes is rejected (ADR-0016). Kafka retention stays bounded, so recovery cannot depend on indefinite broker history or a cloud object store for local `kind`. Cross-context database access and a generic shared query/backfill service would violate one-database-per-context ownership and privacy rules (ADR-0007, ADR-0008, ADR-0014).

## Decision
Settle Spectator and Analytics recovery on **Kafka-carried bounded rebuild requests** plus **context-owned internal HTTP snapshot/backfill APIs**. No public BFF route is added.

**Implementation status (decision unchanged):** Room/Tournament/Ranking context-owned analytics-backfill APIs and Room's spectator-recovery-snapshot API are implemented. Spectator projection-rebuilder (Room snapshot fetch + Redis fenced rebuild) and Analytics projection-rebuilder (durable ClickHouse recovery worker + producer backfill clients) are implemented. Their full live kind recovery lanes have passed, so `projectionRebuilder.enabled=true` is now limited to each `values.kind.yaml`. Default, staging, and production remain disabled until operators explicitly enable and capacity-plan them for those environments.

### 1. Transport and ownership

- Live consumers and operators emit bounded rebuild-request messages to Kafka. Workers never poll outboxes.
- Request partition keys are the recovery aggregate/job identity (`roomId` for Spectator; `recoveryJobId` for Analytics).
- Each consuming context owns its request topic and a worker-owned DLQ named `<request-topic>.<consumer>.dlq`.
- Request and DLQ payloads are compact control envelopes. They never embed unbounded event arrays.

Exact topics:

| Topic | Producer | Consumer | Partition key | Idempotency key | Partitions (prod plan) | Retention class |
| --- | --- | --- | --- | --- | --- | --- |
| `spectator.projection.rebuild_requested` | Spectator live consumer / operator tooling | Spectator projection-rebuilder worker | `roomId` | `(recoveryJobId, roomId, failedCheckpoint)` | 32 | business (7d) |
| `spectator.projection.rebuild_requested.spectator-view.dlq` | Spectator projection-rebuilder | ops / replay tooling | `roomId` | source topic/partition/offset + consumer group | 32 | DLQ (30d) |
| `analytics.projection.rebuild_requested` | Analytics live consumer / operator tooling | Analytics projection-rebuilder worker | `recoveryJobId` | `(recoveryJobId, sourceTopic, pageCursor)` | 32 | business (7d) |
| `analytics.projection.rebuild_requested.analytics.dlq` | Analytics projection-rebuilder | ops / replay tooling | `recoveryJobId` | source topic/partition/offset + consumer group | 32 | DLQ (30d) |

### 2. Shared request envelope

Every rebuild-request message carries EventMetadata (`schemaVersion`, `eventId`, `eventType`, `correlationId`, optional `causationId`, `occurredAt`) plus:

- `recoveryJobId` (stable identity for the whole recovery; chunking reuses it)
- `expectedSourceTopic`
- expected checkpoint/range fields for that consumer
- bounded `attempt` / retry metadata (not an event array)

Workers apply bounded retries, then publish the unchanged request envelope plus sanitized failure metadata to their worker-owned DLQ, await acknowledgment, and only then commit the request-topic offset.

### 3. Spectator recovery

**Request body fields (in addition to the shared envelope):** `roomId`, `failedCheckpoint` (the failed or quarantined room sequence/checkpoint that blocked continuity).

**Worker path:**

1. Consume `spectator.projection.rebuild_requested`.
2. Call Room-owned `GET /internal/v1/rooms/{roomId}/spectator-recovery-snapshot` with Spectator service credential (`ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL` on both Room API and Spectator rebuilder), `failedCheckpoint`, `recoveryJobId`, and `schemaVersion`.
3. Atomically rebuild the Redis projection via generation-swap that is CAS/fenced against the current room sequence/generation. A concurrent newer live apply wins; the rebuild must not regress an already-advanced live projection. The same Redis Lua transaction records the declared `(recoveryJobId, roomId, failedCheckpoint)` idempotency marker and, when continuity is proven, performs fenced quarantine release.
4. Resume from the snapshot's `resumeCheckpoint`.
5. Replay **bounded held post-gap records** already retained by the consumer-owned DLQ/quarantine for that `roomId`, in sequence (held-event bound 1000). Unrelated rooms continue uninterrupted.
6. Release quarantine only after contiguous continuity is proven from `resumeCheckpoint` through the held records. Never silently skip a gap. If continuity cannot be proven, keep quarantine and route the rebuild job to the worker DLQ.

**Room recovery-snapshot response contract:**

- Contains only `SnapshotSanitized`-compatible public spectator state, authoritative `sequenceNumber` / room sequence, and `resumeCheckpoint`.
- Must never include private hands, deck order, player-feed payloads, or Game Integrity data.
- Room remains the owner of sanitization for this export; Spectator still owns projection privacy policy when applying the snapshot.

### 4. Analytics recovery

**Request body fields (in addition to the shared envelope):** `sourceContext` (`room` | `tournament` | `ranking`), `expectedSourceTopic` (one of the nine Analytics-consumed AsyncAPI topics), at least one **paired** bounded range (`fromCheckpoint`/`toCheckpoint` and/or `fromOccurredAt`/`toOccurredAt`; halves of a pair are invalid alone), and opaque `pageCursor` (empty for the first page). Page identity for idempotency is `(recoveryJobId, sourceTopic, pageCursor)`.

**Worker path:**

1. Consume `analytics.projection.rebuild_requested`.
2. Call the owning producer's internal safe backfill API with Analytics service credential, opaque keyset cursor, and hard `limit` max **1000** (Analytics worker default page size is also 1000; producers default omitted `limit` to **100** and reject outside 1..1000).
3. Apply returned records under a durable ClickHouse-visible rebuilding generation/lease/checkpoint. Generation identity is deterministic per `recoveryJobId`. Lease acquire/renew uses epoch + owner-token readback after insert (FINAL winner). Live ingestion dual-writes into the active completed generation and every initializing/building recovery generation under the same durable fence so a separate worker pod cannot lose concurrent events. Process-local mutex alone is insufficient. Building generations are populated by server-side clone from the active generation, then marked building.
4. Persist page progress (`recoveryJobId`, cursor, checkpoint coverage) and request-idempotency disposition before requesting the next page.
5. Analytics may remain delayed, but it must not claim continuity for the requested range until every page/checkpoint in that range is reconciled. Gaps quarantine the affected Analytics aggregate/key rather than skipping.

**Known ClickHouse constraint (honest):** ClickHouse is non-transactional. Projection rows are written before `processed_events` (check-then-act crash window). Same-generation redelivery is made safe by ReplacingMergeTree logical keys plus FINAL processed-marker readback; conflicting fingerprints quarantine without a second projection write. Offset commit happens only after the processed marker succeeds.

**Context-owned backfill producers (no generic cross-context query service):**

| Owner | Endpoint | Topics served | Credential (API) | Cursor HMAC (API-only) |
| --- | --- | --- | --- | --- |
| Room Gameplay | `POST /internal/v1/rooms/analytics-backfill` | `room.gameplay.metrics`, `room.match.completed` | `ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL` | `ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET` |
| Tournament Orchestration | `POST /internal/v1/tournaments/analytics-backfill` | `tournament.match.assigned`, `tournament.match.result_recorded`, `tournament.players.advanced`, `tournament.round.completed`, `tournament.completed` | `TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL` | `TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET` |
| Ranking | `POST /internal/v1/rankings/analytics-backfill` | `ranking.player_rating_updated`, `ranking.leaderboard_snapshot_published` | `RANKING_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL` | `RANKING_ANALYTICS_BACKFILL_CURSOR_SECRET` |

Analytics rebuilder presents the matching scoped credentials (`ANALYTICS_ROOM_CREDENTIAL`, `ANALYTICS_TOURNAMENT_CREDENTIAL`, `ANALYTICS_RANKING_CREDENTIAL`) that equal the producer API secrets above. Cursor HMAC secrets stay on the owning producer API pods only (not on worker Deployments that do not serve backfill).

Backfill request body: `recoveryJobId`, `sourceTopic`, paired bounded range, opaque `cursor`, `limit` (1..1000; producer default 100), `schemaVersion`, correlation headers.

Backfill response body: ordered `records[]` of the same sanitized/public canonical AsyncAPI facts already allowed on those topics (never raw private state), `nextCursor` (omitted/empty when complete), and coverage markers (`fromCheckpoint`/`toCheckpoint` or equivalent) for the returned page.

**Source of truth for pages:** each producer reads its own append-only integration outbox as a **read-only** keyset source. Backfill never mutates outbox rows, never marks publish state, and never substitutes for Debezium CDC.

### 5. Workers and environments

- Projection-rebuilder Deployments remain long-running, HTTP-free, and consume only their rebuild-request topics (plus their worker DLQ publish path).
- They use existing scoped owning-service credentials to call internal snapshot/backfill APIs.
- Default/staging/production keep `projectionRebuilder.enabled=false`.
- Local `kind` enables both workers because their end-to-end Kafka→HTTP→Redis/ClickHouse recovery lanes passed. Kind remains disposable and introduces no cloud object store.
- One physical database per bounded context and existing ownership/security/privacy rules are unchanged: Room/Tournament/Ranking each own one Postgres; Analytics owns ClickHouse only; Spectator owns Redis projection only. No shared DB reads and no application outbox polling.

### 6. Relationship to Kafka retention

Kafka retention classes from ADR-0032 are unchanged. After broker expiry, recovery APIs/backfills are the authoritative post-retention path. Rebuild-request topics themselves stay bounded (business/DLQ classes) and never substitute for Room/Tournament/Ranking durable stores or ClickHouse/Redis rebuild state.

## Consequences
- The Spectator/Analytics projection-rebuilder source/admission path is settled in design/contracts and implemented in service code. Live kind recovery proofs authorize kind-only enablement; other overlays remain disabled by default.
- AsyncAPI carries rebuild-request channels and schemas; architecture documents the internal snapshot/backfill endpoints (not OpenAPI/BFF).
- Recovery scales by keyed jobs and bounded pages rather than whole-topic dumps or shared DB access.
- Online rebuild concurrency is fenced in Redis (Spectator CAS + atomic idempotency/quarantine release) and ClickHouse (Analytics deterministic generation/lease readback, active/building dual-write, server-side clone), preventing silent loss or regression under multi-replica workers while acknowledging ClickHouse's non-transactional check-then-act/idempotent same-generation constraint.
- Operators gain explicit request/DLQ topics for recovery jobs without embedding replay payloads in Kafka control messages.
