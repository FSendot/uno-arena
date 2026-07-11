-- Room Gameplay authoritative schema (Postgres).
-- Physical ownership: this database only. No cross-context FKs.
-- Invariants: docs/03 (Room), docs/04 (commands/events), docs/07 (deadlines/recovery),
-- architecture/04 (snapshot + durable timers + dual outboxes), ADR-0019 (FOR UPDATE).
--
-- Lock order (application MUST acquire in this order while holding rooms FOR UPDATE):
--   1. rooms                (SELECT ... FOR UPDATE on room root first)
--   2. room_roster
--   3. current_games
--   4. command_idempotency
--   5. uno_deadlines
--   6. reconnect_deadlines
--   7. player_session_bindings
--   8. tournament_provisions
--   9. player_stream_highwater
--  10. pending_rejection_audits
--  11. pending_integrity_reconciliations
--  12. integration_outbox_events
--  13. realtime_outbox_events
--  14. processed_reconciliation_offsets
-- schema_bootstrap_meta is bootstrap-owned and is NOT created by this migration.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Room aggregate root. Strong consistency boundary (ADR-0019).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rooms (
    room_id TEXT PRIMARY KEY,
    room_type TEXT NOT NULL CHECK (room_type IN ('ad_hoc', 'tournament')),
    status TEXT NOT NULL CHECK (status IN (
        'waiting', 'locked', 'in_progress', 'completed', 'cancelled'
    )),
    visibility TEXT NOT NULL DEFAULT 'public'
        CHECK (visibility IN ('public', 'private')),
    capacity INT NOT NULL DEFAULT 10 CHECK (capacity >= 2 AND capacity <= 10),
    -- Protected: sequence only advances; never decreases for a live room.
    sequence_number BIGINT NOT NULL DEFAULT 0 CHECK (sequence_number >= 0),
    host_player_id TEXT,
    match_number INT NOT NULL DEFAULT 1 CHECK (match_number >= 1),
    -- Best-of-three match score snapshot (player_id -> wins).
    match_score JSONB NOT NULL DEFAULT '{}'::jsonb,
    tournament_id TEXT,
    round_number INT,
    slot_id TEXT,
    -- Last Game Integrity log offset committed with this snapshot (recovery cursor).
    integrity_log_offset BIGINT NOT NULL DEFAULT 0 CHECK (integrity_log_offset >= 0),
    turn_version BIGINT NOT NULL DEFAULT 0 CHECK (turn_version >= 0),
    game_completed_in_match BOOLEAN NOT NULL DEFAULT false,
    used_game_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    skipped_turns JSONB NOT NULL DEFAULT '[]'::jsonb,
    has_uno BOOLEAN NOT NULL DEFAULT false,
    -- Room-level Uno window (when has_uno); engine Uno may also live in current_games.
    uno_window JSONB,
    -- Active disconnect episodes + next disconnect version counters.
    disconnects JSONB NOT NULL DEFAULT '{}'::jsonb,
    next_disconnect_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Full match engine snapshot when a match is bound (exact round-trip).
    match_snapshot JSONB,
    -- Room-scoped command outcomes embedded for PriorOutcome (mirrors command_idempotency).
    outcomes JSONB NOT NULL DEFAULT '{}'::jsonb,
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
    'Room aggregate root. Lock with SELECT ... FOR UPDATE before child rows (lock order #1).';
COMMENT ON COLUMN rooms.sequence_number IS
    'Optimistic concurrency / command serialization token. Gameplay mutations require expectedSequenceNumber match.';
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
-- Roster / seats (lock order #2 after rooms).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS room_roster (
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    seat_number INT NOT NULL CHECK (seat_number >= 0),
    player_id TEXT NOT NULL,
    occupied BOOLEAN NOT NULL DEFAULT true,
    -- connected | disconnected | forfeited
    connection_status TEXT NOT NULL DEFAULT 'connected'
        CHECK (connection_status IN ('connected', 'disconnected', 'forfeited')),
    disconnect_version BIGINT NOT NULL DEFAULT 0 CHECK (disconnect_version >= 0),
    wins INT NOT NULL DEFAULT 0 CHECK (wins >= 0),
    points INT NOT NULL DEFAULT 0,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    left_at TIMESTAMPTZ,
    PRIMARY KEY (room_id, seat_number),
    UNIQUE (room_id, player_id)
);

COMMENT ON TABLE room_roster IS
    'Seat entities inside the Room aggregate. Acquire after rooms FOR UPDATE (lock order #2).';

CREATE INDEX IF NOT EXISTS room_roster_player_idx
    ON room_roster (player_id);

-- ---------------------------------------------------------------------------
-- Current game snapshot (lock order #3). Exact *game.Game restore via engine_state.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS current_games (
    room_id TEXT PRIMARY KEY REFERENCES rooms (room_id) ON DELETE CASCADE,
    game_id TEXT NOT NULL,
    game_number INT NOT NULL DEFAULT 1 CHECK (game_number >= 1),
    status TEXT NOT NULL CHECK (status IN ('active', 'completed', 'abandoned')),
    snapshot_sequence BIGINT NOT NULL CHECK (snapshot_sequence >= 0),
    turn_order JSONB NOT NULL DEFAULT '[]'::jsonb,
    current_seat INT,
    active_color TEXT,
    direction INT NOT NULL DEFAULT 1 CHECK (direction IN (-1, 1)),
    penalty_stack INT NOT NULL DEFAULT 0 CHECK (penalty_stack >= 0),
    top_discard JSONB,
    hands JSONB NOT NULL DEFAULT '{}'::jsonb,
    card_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    placement_order JSONB,
    -- Full engine snapshot for exact *game.Game round-trip (pending color, outcomes, uno, …).
    engine_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (game_id)
);

COMMENT ON TABLE current_games IS
    'CurrentGame entity snapshot. One row per room; engine_state restores *game.Game exactly.';

CREATE INDEX IF NOT EXISTS current_games_status_idx
    ON current_games (status)
    WHERE status = 'active';

-- ---------------------------------------------------------------------------
-- Command idempotency (lock order #4). room_id NULL for global / pre-room outcomes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_idempotency (
    command_id TEXT PRIMARY KEY,
    room_id TEXT,
    player_id TEXT,
    command_type TEXT NOT NULL DEFAULT '',
    outcome_status TEXT NOT NULL CHECK (outcome_status IN ('accepted', 'rejected', 'failed')),
    outcome_body JSONB NOT NULL DEFAULT '{}'::jsonb,
    applied_sequence BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE command_idempotency IS
    'Command-side dedupe. Same command_id always returns the stored stable outcome; never re-applies.';

CREATE INDEX IF NOT EXISTS command_idempotency_room_idx
    ON command_idempotency (room_id, created_at);

-- ---------------------------------------------------------------------------
-- Durable Uno deadlines (lock order #5). Redis indexes dispatch only.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS uno_deadlines (
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    game_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    triggering_game_event_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    opening_room_sequence BIGINT NOT NULL CHECK (opening_room_sequence >= 0),
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'expired', 'closed_by_call', 'closed_by_challenge', 'closed_by_turn', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (room_id, game_id, player_id, triggering_game_event_id)
);

COMMENT ON TABLE uno_deadlines IS
    'Durable Uno challenge windows. Postgres is authority; Redis is a non-authoritative index.';

CREATE INDEX IF NOT EXISTS uno_deadlines_open_expires_idx
    ON uno_deadlines (expires_at)
    WHERE status = 'open';

-- ---------------------------------------------------------------------------
-- Durable reconnect / forfeit deadlines (lock order #6).
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
-- Player session bindings (lock order #7).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_session_bindings (
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    player_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, player_id)
);

COMMENT ON TABLE player_session_bindings IS
    'Authoritative (room_id, player_id) -> session_id binding for player-feed audiences.';

-- ---------------------------------------------------------------------------
-- Tournament provisions (lock order #8).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournament_provisions (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    room_id TEXT NOT NULL REFERENCES rooms (room_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, slot_id)
);

COMMENT ON TABLE tournament_provisions IS
    'Idempotent tournament room provision key -> room_id.';

-- ---------------------------------------------------------------------------
-- Player-feed stream high-water (lock order #9).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_stream_highwater (
    room_id TEXT PRIMARY KEY REFERENCES rooms (room_id) ON DELETE CASCADE,
    sequence_number BIGINT NOT NULL DEFAULT 0 CHECK (sequence_number >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE player_stream_highwater IS
    'Last allocated player-feed sequence for the room (independent of room sequence).';

-- ---------------------------------------------------------------------------
-- Pending rejection audits awaiting sink delivery (lock order #10).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pending_rejection_audits (
    command_id TEXT PRIMARY KEY,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE pending_rejection_audits IS
    'Rejection audit records awaiting sink delivery; cleared after successful Record.';

-- ---------------------------------------------------------------------------
-- GI reconciliation intents (lock order #11).
-- Persisted BEFORE GI append; finalized with log_offset after append success.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pending_integrity_reconciliations (
    command_id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL,
    expected_revision BIGINT NOT NULL DEFAULT 0 CHECK (expected_revision >= 0),
    log_offset BIGINT CHECK (log_offset IS NULL OR log_offset >= 0),
    revision BIGINT CHECK (revision IS NULL OR revision >= 0),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'done', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

COMMENT ON TABLE pending_integrity_reconciliations IS
    'Durable GI reconciliation intent keyed by command_id. Inserted before Append; finalized with log_offset/revision after success; worker repairs pending intents.';

CREATE INDEX IF NOT EXISTS pending_integrity_reconciliations_pending_idx
    ON pending_integrity_reconciliations (status, created_at)
    WHERE status = 'pending';

-- ---------------------------------------------------------------------------
-- Integration outbox (Kafka via Debezium Outbox Event Router). NO published_at.
-- Append-only. Lock order #12.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration_outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    room_id TEXT,
    integrity_log_offset BIGINT CHECK (integrity_log_offset IS NULL OR integrity_log_offset >= 0),
    payload JSONB NOT NULL,
    correlation_id TEXT,
    causation_id TEXT,
    occurred_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);

COMMENT ON TABLE integration_outbox_events IS
    'Kafka-bound transactional outbox. Debezium Outbox Event Router; no app polling; no published_at.';

CREATE INDEX IF NOT EXISTS integration_outbox_events_room_idx
    ON integration_outbox_events (room_id, outbox_id);

CREATE INDEX IF NOT EXISTS integration_outbox_events_created_idx
    ON integration_outbox_events (created_at);

-- ---------------------------------------------------------------------------
-- Realtime outbox (Redis Streams via Debezium Server). NO published_at.
-- Append-only. Lock order #13.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS realtime_outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    target_stream TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    room_id TEXT,
    player_id TEXT,
    session_id TEXT,
    sequence_number BIGINT NOT NULL DEFAULT 0 CHECK (sequence_number >= 0),
    integrity_log_offset BIGINT CHECK (integrity_log_offset IS NULL OR integrity_log_offset >= 0),
    payload JSONB NOT NULL,
    correlation_id TEXT,
    causation_id TEXT,
    occurred_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);

COMMENT ON TABLE realtime_outbox_events IS
    'Redis-bound realtime outbox for player feeds. Debezium Server Redis sink; no app polling; no published_at.';

CREATE INDEX IF NOT EXISTS realtime_outbox_events_room_idx
    ON realtime_outbox_events (room_id, outbox_id);

CREATE INDEX IF NOT EXISTS realtime_outbox_events_created_idx
    ON realtime_outbox_events (created_at);

-- ---------------------------------------------------------------------------
-- Processed reconciliation offsets (lock order #14). Idempotent by (room, offset).
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
