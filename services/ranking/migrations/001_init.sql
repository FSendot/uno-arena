-- Ranking authoritative schema (Postgres).
-- Physical ownership: this database only. No cross-context FKs.
-- Casual Elo and tournament-placement rating are separate streams (docs/03, docs/04).
-- Invariants: docs/07 idempotency keys; architecture/04 rating history + rebuildable cache.
-- Append-only outbox for Debezium CDC (ADR-0016/0026): no published_at / app polling.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- PlayerRating aggregate: separated casual Elo vs tournament placement.
-- Rating floor enforced in domain; CHECK documents non-negative floor bound.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_ratings (
    player_id TEXT PRIMARY KEY,
    -- Casual Elo: only from completed non-abandoned ad-hoc games.
    casual_elo INT NOT NULL,
    casual_games_played INT NOT NULL DEFAULT 0 CHECK (casual_games_played >= 0),
    -- Tournament placement rating: never updated from casual GameCompleted.
    tournament_placement_rating INT NOT NULL,
    tournament_events_applied INT NOT NULL DEFAULT 0 CHECK (tournament_events_applied >= 0),
    rating_floor INT NOT NULL DEFAULT 0 CHECK (rating_floor >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT player_ratings_casual_floor CHECK (casual_elo >= rating_floor),
    CONSTRAINT player_ratings_tournament_floor CHECK (tournament_placement_rating >= rating_floor)
);

COMMENT ON TABLE player_ratings IS
    'Authoritative PlayerRating. Casual Elo and tournament placement are independent value objects.';
COMMENT ON COLUMN player_ratings.casual_elo IS
    'Derived only from authoritative completed non-abandoned ad-hoc games.';
COMMENT ON COLUMN player_ratings.tournament_placement_rating IS
    'Updated only from tournament placement/standing facts — never from casual Elo events.';

CREATE INDEX IF NOT EXISTS player_ratings_casual_elo_idx
    ON player_ratings (casual_elo DESC);

CREATE INDEX IF NOT EXISTS player_ratings_tournament_idx
    ON player_ratings (tournament_placement_rating DESC);

-- ---------------------------------------------------------------------------
-- Immutable rating history for audit and leaderboard rebuild.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rating_history (
    history_id BIGSERIAL PRIMARY KEY,
    player_id TEXT NOT NULL REFERENCES player_ratings (player_id),
    source_type TEXT NOT NULL CHECK (source_type IN ('casual_elo', 'tournament_placement')),
    previous_rating INT NOT NULL,
    new_rating INT NOT NULL,
    delta INT NOT NULL,
    reason TEXT,
    -- Casual path keys
    game_id TEXT,
    room_id TEXT,
    -- Tournament path keys
    tournament_id TEXT,
    placement_event_id TEXT,
    placement INT,
    upstream_event_id TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rating_history_casual_keys CHECK (
        source_type <> 'casual_elo'
        OR (game_id IS NOT NULL AND tournament_id IS NULL AND placement_event_id IS NULL)
    ),
    CONSTRAINT rating_history_tournament_keys CHECK (
        source_type <> 'tournament_placement'
        OR (tournament_id IS NOT NULL AND placement_event_id IS NOT NULL AND game_id IS NULL)
    )
);

COMMENT ON TABLE rating_history IS
    'Durable rating applications. Leaderboard Redis snapshots rebuild from this history.';

CREATE INDEX IF NOT EXISTS rating_history_player_applied_idx
    ON rating_history (player_id, applied_at DESC);

CREATE INDEX IF NOT EXISTS rating_history_upstream_event_idx
    ON rating_history (upstream_event_id);

-- ---------------------------------------------------------------------------
-- Processed upstream business keys (docs/07).
-- Casual: (player_id, game_id). Tournament: (player_id, tournament_id, placement_event_id).
-- Also track raw event_id for at-least-once consumer dedupe.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_casual_elo_keys (
    player_id TEXT NOT NULL,
    game_id TEXT NOT NULL,
    upstream_event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, game_id)
);

COMMENT ON TABLE processed_casual_elo_keys IS
    'ApplyCasualEloUpdate idempotency. Same (playerId, gameId) cannot apply twice. '
    'One GameCompleted event may insert multiple participant rows sharing upstream_event_id; '
    'event-level dedupe is processed_upstream_events only.';

CREATE TABLE IF NOT EXISTS processed_tournament_placement_keys (
    player_id TEXT NOT NULL,
    tournament_id TEXT NOT NULL,
    placement_event_id TEXT NOT NULL,
    upstream_event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, tournament_id, placement_event_id)
);

COMMENT ON TABLE processed_tournament_placement_keys IS
    'ApplyTournamentPlacementUpdate idempotency by (playerId, tournamentId, placementEventId). '
    'Event-level dedupe is processed_upstream_events only.';

CREATE TABLE IF NOT EXISTS processed_upstream_events (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    consumer_group TEXT NOT NULL DEFAULT 'ranking',
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE processed_upstream_events IS
    'Single event-id dedupe for ranking consumers; complements business-key tables.';

-- Stable HTTP/command responses for exact eventId / gameId / placement replay.
CREATE TABLE IF NOT EXISTS ranking_command_responses (
    dedupe_kind TEXT NOT NULL CHECK (dedupe_kind IN ('event_id', 'game_id', 'tournament_placement')),
    dedupe_key TEXT NOT NULL,
    response_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dedupe_kind, dedupe_key)
);

COMMENT ON TABLE ranking_command_responses IS
    'Byte-stable accepted responses for eventId, gameId, and tournament placement replay.';

-- ---------------------------------------------------------------------------
-- Leaderboard snapshot metadata (authoritative generation record; Redis is cache).
-- PublishLeaderboardSnapshot may repeat safely.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    board_type TEXT NOT NULL CHECK (board_type IN ('casual_elo', 'tournament_placement')),
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    player_count INT NOT NULL DEFAULT 0 CHECK (player_count >= 0),
    -- Optional compact checksum / version marker for cache invalidation.
    content_version TEXT,
    published_event_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON TABLE leaderboard_snapshots IS
    'Snapshot generation metadata. Redis leaderboards are rebuildable from rating_history.';

CREATE INDEX IF NOT EXISTS leaderboard_snapshots_type_generated_idx
    ON leaderboard_snapshots (board_type, generated_at DESC);

-- ---------------------------------------------------------------------------
-- Append-only outbox for PlayerRatingUpdated (casual + tournament placement), snapshots.
-- Debezium CDC publishes; app never polls or marks published_at (ADR-0016/0026).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    player_id TEXT,
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);

COMMENT ON TABLE outbox_events IS
    'Append-only Debezium CDC input for public rating facts. No published_at / app polling.';

CREATE INDEX IF NOT EXISTS outbox_events_player_idx
    ON outbox_events (player_id, outbox_id);

INSERT INTO schema_migrations (version) VALUES ('001_init')
ON CONFLICT (version) DO NOTHING;
