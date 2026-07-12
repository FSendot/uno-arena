-- Analytics ClickHouse schema (derived, non-authoritative).
-- Physical ownership: analytics database only. No joins to transactional stores.
-- Inputs: sanitized gameplay metrics, public tournament facts, public rating facts.
-- Privacy: no private hands, hidden deck order, private draw ids, session tokens, or raw audit.
-- Dedupe: processed_events keyed by (generation_id, topic, idempotency_key) with durable
--   outcome_json, event_id, and immutable payload_fingerprint (ADR-0029 contract keys).
-- Generations: public reads use only the latest completed active generation (FINAL).
-- rating_statistics ORDER BY retains every leaderboard row under one upstream
-- idempotency_key and isolates Ranking topics via source_topic.
-- Partitions: time (YYYYMM) and tournament_id where applicable (architecture/04, ADR 0008).
-- ClickHouse is non-transactional: writers insert projection rows before processed_events.
-- Crash window: projection insert without a processed marker redelivers into ReplacingMergeTree
--   logical rows keyed by (source_topic, idempotency_key) (version-replaced), then the marker is written last.
-- Offset commit happens only after the processed marker succeeds. No Kafka offset table.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.schema_migrations
(
    version String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;

-- ---------------------------------------------------------------------------
-- Projection generation bookkeeping for safe rebuild activation.
-- status: building (invisible to public reads) | complete (eligible for activation).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.projection_generations
(
    generation_id String,
    status LowCardinality(String),
    accepted_count UInt64 DEFAULT 0,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3),
    completed_at DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (generation_id)
SETTINGS index_granularity = 8192;

-- Singleton active-generation pointer. Public APIs read only this generation when complete.
CREATE TABLE IF NOT EXISTS analytics.active_generation
(
    singleton UInt8 DEFAULT 1,
    generation_id String,
    switched_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(switched_at)
ORDER BY (singleton)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Sanitized gameplay metrics (room.gameplay.metrics and public room facts).
-- Ad-hoc metrics must already be anonymized before insert.
-- Public tournament metrics may round-trip already-public player display facts.
-- Logical replace key includes source_topic + idempotency_key so crash-window
-- redelivery replaces without colliding across topics that share key strings.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.gameplay_metrics
(
    generation_id String,
    source_topic LowCardinality(String),
    event_id String,
    idempotency_key String,
    schema_version UInt16,
    correlation_id String,
    -- Public room/game identifiers only; never session or integrity audit payloads.
    room_id String,
    game_id String,
    tournament_id String DEFAULT '',
    -- public | anonymized_adhoc | public_tournament — never private
    visibility LowCardinality(String),
    metric_type LowCardinality(String),
    -- Public discard/top-card facts only when already spectator-safe.
    public_card_rank LowCardinality(String) DEFAULT '',
    public_card_color LowCardinality(String) DEFAULT '',
    -- Aggregate counters only (e.g. hand size), never hand contents.
    public_card_count_total UInt16 DEFAULT 0,
    room_sequence UInt64 DEFAULT 0,
    -- Already-public tournament player display facts only (empty for ad-hoc).
    public_player_id String DEFAULT '',
    display_name String DEFAULT '',
    occurred_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (generation_id, source_topic, idempotency_key)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Public tournament lifecycle / advancement / result projections.
-- Partitioned by month and tournament for bursty round-completion queries.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.tournament_statistics
(
    generation_id String,
    source_topic LowCardinality(String),
    event_id String,
    idempotency_key String,
    schema_version UInt16,
    correlation_id String,
    tournament_id String,
    round_number Int32 DEFAULT 0,
    slot_id String DEFAULT '',
    event_type LowCardinality(String),
    -- Public phase/status labels and aggregate counts only.
    phase LowCardinality(String) DEFAULT '',
    registered_count UInt32 DEFAULT 0,
    advancing_player_count UInt16 DEFAULT 0,
    -- Producer-filtered public JSON; nested keys validated by analytics allowlist.
    public_payload_json String DEFAULT '',
    occurred_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY (toYYYYMM(occurred_at), sipHash64(tournament_id) % 16)
ORDER BY (generation_id, tournament_id, source_topic, idempotency_key)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Public rating / leaderboard projection facts from Ranking.
-- ORDER BY includes player/snapshot identity so one upstream leaderboard event
-- retains every row; ingestion dedupe remains by (topic, idempotency_key).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.rating_statistics
(
    generation_id String,
    source_topic LowCardinality(String),
    event_id String,
    idempotency_key String,
    schema_version UInt16,
    correlation_id String,
    player_id String,
    source_type LowCardinality(String),
    previous_rating Int32,
    new_rating Int32,
    board_type LowCardinality(String) DEFAULT '',
    snapshot_id String DEFAULT '',
    occurred_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (generation_id, source_topic, idempotency_key, snapshot_id, player_id)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Ingestion dedupe / quarantine / ignored markers for upstream events.
-- Lookup key: (generation_id, topic, idempotency_key). Stores event_id + fingerprint.
-- disposition: applied | quarantined | ignored
-- outcome_json holds the byte-stable durable ApplyOutcome.
-- Writers MUST insert projection rows (if any) before this marker.
-- Conflicting fingerprint against an existing applied marker returns conflict without
-- replacing the first-wins marker (projection rows stay; no second marker write).
-- Conflicts are durably recorded in ingestion_conflicts before Apply returns quarantined.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.processed_events
(
    generation_id String,
    topic LowCardinality(String),
    idempotency_key String,
    event_id String,
    payload_fingerprint String,
    disposition LowCardinality(String) DEFAULT 'applied',
    outcome_json String,
    processed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (generation_id, topic, idempotency_key)
SETTINGS index_granularity = 8192;

-- Privacy-safe durable conflict audit (no raw payload). First-wins marker/projection
-- remain authoritative; this table records each conflicting fingerprint seen.
CREATE TABLE IF NOT EXISTS analytics.ingestion_conflicts
(
    generation_id String,
    topic LowCardinality(String),
    idempotency_key String,
    conflicting_fingerprint String,
    original_event_id String,
    seen_event_id String,
    first_marker_fingerprint String,
    outcome_json String,
    recorded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(recorded_at)
ORDER BY (generation_id, topic, idempotency_key, conflicting_fingerprint)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- ADR-0039 durable projection recovery (generation/lease/checkpoint).
-- Process-local mutex is never the correctness boundary; ClickHouse FINAL +
-- ReplacingMergeTree version columns fence multi-replica workers.
-- status: initializing | building | complete | failed | quarantined
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.recovery_jobs
(
    recovery_job_id String,
    source_context LowCardinality(String),
    source_topic LowCardinality(String),
    generation_id String,
    status LowCardinality(String),
    from_checkpoint String DEFAULT '',
    to_checkpoint String DEFAULT '',
    from_occurred_at DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC'),
    to_occurred_at DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC'),
    has_checkpoint_range UInt8 DEFAULT 0,
    has_occurred_range UInt8 DEFAULT 0,
    last_page_cursor String DEFAULT '',
    next_page_cursor String DEFAULT '',
    pages_completed UInt32 DEFAULT 0,
    accepted_count UInt64 DEFAULT 0,
    failure_code LowCardinality(String) DEFAULT '',
    failure_summary String DEFAULT '',
    quarantine_key String DEFAULT '',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (recovery_job_id)
SETTINGS index_granularity = 8192;

-- Worker lease contenders retained per owner_token. Winner is selected
-- deterministically: highest lease_epoch, then lexicographically smallest
-- owner_token. ReplacingMergeTree(updated_at) keeps the latest version per
-- (job, owner); every acquire/renew must advance lease_epoch.
CREATE TABLE IF NOT EXISTS analytics.recovery_leases
(
    recovery_job_id String,
    owner_token String,
    lease_epoch UInt64,
    expires_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (recovery_job_id, owner_token)
SETTINGS index_granularity = 8192;

-- Per-page progress keyed by AsyncAPI idempotency (recoveryJobId, sourceTopic, pageCursor).
CREATE TABLE IF NOT EXISTS analytics.recovery_page_checkpoints
(
    recovery_job_id String,
    source_topic LowCardinality(String),
    page_cursor String,
    page_index UInt32,
    next_page_cursor String DEFAULT '',
    from_checkpoint String DEFAULT '',
    to_checkpoint String DEFAULT '',
    records_applied UInt32 DEFAULT 0,
    quarantined_count UInt32 DEFAULT 0,
    status LowCardinality(String),
    generation_id String,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (recovery_job_id, source_topic, page_cursor)
SETTINGS index_granularity = 8192;

-- Declared request idempotency / follow-up publish fence before source offset commit.
CREATE TABLE IF NOT EXISTS analytics.recovery_request_idempotency
(
    recovery_job_id String,
    source_topic LowCardinality(String),
    page_cursor String,
    disposition LowCardinality(String),
    follow_up_event_id String DEFAULT '',
    generation_id String DEFAULT '',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (recovery_job_id, source_topic, page_cursor)
SETTINGS index_granularity = 8192;

-- Explicit insert is safe to re-run only if version row is replaced by engine semantics.
INSERT INTO analytics.schema_migrations (version, applied_at)
VALUES ('001_init', now64(3));
