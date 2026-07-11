-- Room Gameplay authoritative schema (Postgres).
-- Physical ownership: this database only. No cross-context FKs.
-- Invariants: docs/03 (Room), docs/04 (commands/events), docs/07 (deadlines/recovery),
-- architecture/04 (snapshot + durable timers + outbox).

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Room aggregate: operational truth for roster, status, and sequence.
-- Status moves waiting -> locked -> in_progress -> completed, or waiting -> cancelled.
-- sequence_number is the optimistic concurrency token for gameplay commands.
-- tournament_* columns are reference-by-identity only (no FK to Tournament DB).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rooms (
    room_id TEXT PRIMARY KEY,
    room_type TEXT NOT NULL CHECK (room_type IN ('ad_hoc', 'tournament')),
    status TEXT NOT NULL CHECK (status IN (
        'waiting', 'locked', 'in_progress', 'completed', 'cancelled'
    )),
    -- Protected: sequence only advances; never decreases for a live room.
    sequence_number BIGINT NOT NULL DEFAULT 0 CHECK (sequence_number >= 0),
    host_player_id TEXT,
    match_number INT NOT NULL DEFAULT 1 CHECK (match_number >= 1),
    -- Best-of-three match score snapshot (player_id -> wins); authoritative for CompleteMatch.
    match_score JSONB NOT NULL DEFAULT '{}'::jsonb,
    tournament_id TEXT,
    round_number INT,
    slot_id TEXT,
    -- Last Game Integrity log offset committed with this snapshot (recovery cursor).
    integrity_log_offset BIGINT NOT NULL DEFAULT 0 CHECK (integrity_log_offset >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT rooms_tournament_refs_consistent CHECK (
        (room_type = 'ad_hoc' AND tournament_id IS NULL AND round_number IS NULL AND slot_id IS NULL)
        OR (room_type = 'tournament' AND tournament_id IS NOT NULL AND round_number IS NOT NULL AND slot_id IS NOT NULL)
    ),
    CONSTRAINT rooms_terminal_completed_at CHECK (
        (status IN ('completed', 'cancelled') AND completed_at IS NOT NULL)
        OR (status NOT IN ('completed', 'cancelled'))
    )
);

COMMENT ON TABLE rooms IS
    'Room aggregate root. Strong consistency boundary for roster, turn authority, match score, and status.';
COMMENT ON COLUMN rooms.sequence_number IS
    'Optimistic concurrency / command serialization token. Gameplay mutations require expectedSequenceNumber match.';
COMMENT ON COLUMN rooms.status IS
    'Protected lifecycle: waiting|locked|in_progress|completed|cancelled. Terminal rooms reject gameplay commands.';
COMMENT ON COLUMN rooms.integrity_log_offset IS
    'Highest Game Integrity log offset reflected in this operational snapshot; used by RoomStateReconciled.';

CREATE UNIQUE INDEX IF NOT EXISTS rooms_tournament_slot_uidx
    ON rooms (tournament_id, round_number, slot_id)
    WHERE room_type = 'tournament';

CREATE INDEX IF NOT EXISTS rooms_status_idx
    ON rooms (status);

CREATE INDEX IF NOT EXISTS rooms_updated_at_idx
    ON rooms (updated_at);

-- ---------------------------------------------------------------------------
-- Roster / seats. No joins after lock (enforced in domain; seat rows remain).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS room_roster (
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    seat_number INT NOT NULL CHECK (seat_number >= 0),
    player_id TEXT NOT NULL,
    -- connected | disconnected | forfeited
    connection_status TEXT NOT NULL DEFAULT 'connected'
        CHECK (connection_status IN ('connected', 'disconnected', 'forfeited')),
    disconnect_version BIGINT NOT NULL DEFAULT 0 CHECK (disconnect_version >= 0),
    game_wins INT NOT NULL DEFAULT 0 CHECK (game_wins >= 0),
    cumulative_card_points INT NOT NULL DEFAULT 0,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    left_at TIMESTAMPTZ,
    PRIMARY KEY (room_id, seat_number),
    UNIQUE (room_id, player_id)
);

COMMENT ON TABLE room_roster IS
    'Seat entities inside the Room aggregate. player_id is identity-only (no FK to Identity DB).';
COMMENT ON COLUMN room_roster.disconnect_version IS
    'Monotonic per seat disconnect episode; pairs with reconnect_deadlines for single forfeit emission.';

CREATE INDEX IF NOT EXISTS room_roster_player_idx
    ON room_roster (player_id);

-- ---------------------------------------------------------------------------
-- Current game snapshot (at most one active game per room).
-- Private hand/deck facts live here for reconnect; spectators must not read this store.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS current_games (
    room_id TEXT PRIMARY KEY REFERENCES rooms (room_id) ON DELETE CASCADE,
    game_id TEXT NOT NULL,
    game_number INT NOT NULL CHECK (game_number >= 1),
    status TEXT NOT NULL CHECK (status IN ('active', 'completed', 'abandoned')),
    -- Room sequence at which this game snapshot was last written.
    snapshot_sequence BIGINT NOT NULL CHECK (snapshot_sequence >= 0),
    turn_order JSONB NOT NULL DEFAULT '[]'::jsonb,
    current_seat INT,
    active_color TEXT,
    direction INT NOT NULL DEFAULT 1 CHECK (direction IN (-1, 1)),
    penalty_stack INT NOT NULL DEFAULT 0 CHECK (penalty_stack >= 0),
    top_discard JSONB,
    -- Per-player private hands and public card counts; operational reconnect truth.
    hands JSONB NOT NULL DEFAULT '{}'::jsonb,
    card_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    placement_order JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (game_id)
);

COMMENT ON TABLE current_games IS
    'CurrentGame entity snapshot. One active game per room; hands are player-private operational state.';
COMMENT ON COLUMN current_games.snapshot_sequence IS
    'Room sequence captured with this game snapshot; must stay aligned with rooms.sequence_number on commit.';

CREATE INDEX IF NOT EXISTS current_games_status_idx
    ON current_games (status)
    WHERE status = 'active';

-- ---------------------------------------------------------------------------
-- Command idempotency with stable outcome (docs/04, docs/07).
-- Duplicate submissions return the previously decided outcome without re-mutation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_idempotency (
    command_id TEXT PRIMARY KEY,
    room_id TEXT,
    player_id TEXT,
    command_type TEXT NOT NULL,
    -- accepted | rejected | failed — outcome is immutable once written
    outcome_status TEXT NOT NULL CHECK (outcome_status IN ('accepted', 'rejected', 'failed')),
    outcome_body JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Sequence observed/applied when the outcome was decided (for stale-command audits).
    applied_sequence BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE command_idempotency IS
    'Command-side dedupe. Same command_id always returns the stored stable outcome; never re-applies.';

CREATE INDEX IF NOT EXISTS command_idempotency_room_idx
    ON command_idempotency (room_id, created_at);

-- ---------------------------------------------------------------------------
-- Durable Uno deadlines.
-- Idempotency key: (room_id, game_id, player_id, triggering_game_event_id).
-- Server owns absolute UTC expires_at and opening_room_sequence; client countdown is advisory.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uno_deadlines (
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    game_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    triggering_game_event_id TEXT NOT NULL,
    -- Absolute UTC expiry; ExpireUnoWindow rechecks this before emitting UnoWindowExpired.
    expires_at TIMESTAMPTZ NOT NULL,
    -- Room sequence when the window opened; required for stale-timer rejection.
    opening_room_sequence BIGINT NOT NULL CHECK (opening_room_sequence >= 0),
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'expired', 'closed_by_call', 'closed_by_challenge', 'closed_by_turn', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (room_id, game_id, player_id, triggering_game_event_id)
);

COMMENT ON TABLE uno_deadlines IS
    'Durable Uno challenge windows. Emit UnoWindowExpired at most once per primary key (docs/07).';
COMMENT ON COLUMN uno_deadlines.expires_at IS
    'Authoritative absolute UTC deadline; Redis may index dispatch but must not own truth.';
COMMENT ON COLUMN uno_deadlines.opening_room_sequence IS
    'Sequence at window open; delayed timers cannot close a superseded window.';

CREATE INDEX IF NOT EXISTS uno_deadlines_open_expires_idx
    ON uno_deadlines (expires_at)
    WHERE status = 'open';

-- ---------------------------------------------------------------------------
-- Durable reconnect / forfeit deadlines.
-- Idempotency key: (room_id, player_id, disconnect_version).
-- No opening-sequence field (docs/architecture/02 timer-command notes).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reconnect_deadlines (
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    player_id TEXT NOT NULL,
    disconnect_version BIGINT NOT NULL CHECK (disconnect_version >= 0),
    expires_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'forfeited', 'reconnected', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (room_id, player_id, disconnect_version)
);

COMMENT ON TABLE reconnect_deadlines IS
    '60s reconnect windows. PlayerForfeited emits once per (room, player, disconnectVersion).';

CREATE INDEX IF NOT EXISTS reconnect_deadlines_open_expires_idx
    ON reconnect_deadlines (expires_at)
    WHERE status = 'open';

-- ---------------------------------------------------------------------------
-- Transactional outbox: committed with room snapshot after Game Integrity append.
-- Carries event identity, topic, partition key, schema version, and integrity log offset.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    room_id TEXT,
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    -- Game Integrity offset associated with this published fact (recovery/ordering).
    integrity_log_offset BIGINT CHECK (integrity_log_offset IS NULL OR integrity_log_offset >= 0),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    UNIQUE (event_id)
);

COMMENT ON TABLE outbox_events IS
    'Reliable publication bridge. Same transaction as snapshot + deadlines after integrity append confirm.';
COMMENT ON COLUMN outbox_events.schema_version IS
    'Explicit event schema version for AsyncAPI/contract evolution.';
COMMENT ON COLUMN outbox_events.integrity_log_offset IS
    'Log offset committed with this outbox row; enables RoomStateReconciled catch-up.';

CREATE INDEX IF NOT EXISTS outbox_events_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_events_room_idx
    ON outbox_events (room_id, outbox_id);

-- ---------------------------------------------------------------------------
-- Processed reconciliation offsets.
-- ReconcileRoomStateFromIntegrityLog is idempotent by (room_id, log_offset).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_reconciliation_offsets (
    room_id TEXT NOT NULL,
    log_offset BIGINT NOT NULL CHECK (log_offset >= 0),
    reconciled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes TEXT,
    PRIMARY KEY (room_id, log_offset)
);

COMMENT ON TABLE processed_reconciliation_offsets IS
    'Idempotency for RoomStateReconciled recovery. Retries at the same offset are no-ops.';

INSERT INTO schema_migrations (version) VALUES ('001_init')
ON CONFLICT (version) DO NOTHING;
