-- Analytics ClickHouse schema (derived, non-authoritative).
-- Physical ownership: analytics database only. No joins to transactional stores.
-- Inputs: sanitized gameplay metrics, public tournament facts, public rating facts.
-- Privacy: no private hands, hidden deck order, private draw ids, session tokens, or raw audit.
-- Dedupe: processed_events keyed by event_id (docs/04 projection idempotency).
-- rating_statistics ORDER BY retains every leaderboard row under one upstream event_id.
-- Partitions: time (YYYYMM) and tournament_id where applicable (architecture/04, ADR 0008).

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.schema_migrations
(
    version String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;

-- ---------------------------------------------------------------------------
-- Sanitized gameplay metrics (room.gameplay.metrics and public room facts).
-- Ad-hoc metrics must already be anonymized before insert.
-- Public tournament metrics may round-trip already-public player display facts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.gameplay_metrics
(
    event_id String,
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
ORDER BY (event_id)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Public tournament lifecycle / advancement / result projections.
-- Partitioned by month and tournament for bursty round-completion queries.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.tournament_statistics
(
    event_id String,
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
ORDER BY (tournament_id, event_id)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Public rating / leaderboard projection facts from Ranking.
-- ORDER BY includes player/snapshot identity so one upstream leaderboard event
-- retains every row; ingestion dedupe remains by event_id in processed_events.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.rating_statistics
(
    event_id String,
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
ORDER BY (event_id, snapshot_id, player_id)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Ingestion dedupe / quarantine markers for unsafe or duplicate upstream events.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.processed_events
(
    event_id String,
    topic LowCardinality(String),
    disposition LowCardinality(String) DEFAULT 'applied',
    processed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (event_id)
SETTINGS index_granularity = 8192;

-- Explicit insert is safe to re-run only if version row is replaced by engine semantics.
INSERT INTO analytics.schema_migrations (version, applied_at)
VALUES ('001_init', now64(3));
