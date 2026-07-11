-- Tournament Orchestration authoritative schema (Postgres).
-- Physical ownership: this database only. No cross-context FKs.
-- room_id values are reference-by-identity to Room Gameplay (docs/03).
-- Invariants: docs/03 (Tournament/Round), docs/04, docs/07, architecture/04.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Tournament aggregate root.
-- Phases: registration -> seeding -> in_progress -> completed | cancelled.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournaments (
    tournament_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL CHECK (phase IN (
        'registration', 'seeding', 'in_progress', 'completed', 'cancelled'
    )),
    capacity INT NOT NULL CHECK (capacity > 0),
    registered_count INT NOT NULL DEFAULT 0 CHECK (registered_count >= 0),
    rules JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT tournaments_capacity_bound CHECK (registered_count <= capacity)
);

COMMENT ON TABLE tournaments IS
    'Tournament aggregate. Strong consistency for lifecycle; async relative to room outcomes.';
COMMENT ON COLUMN tournaments.phase IS
    'Forward-only lifecycle. Registration cannot exceed capacity.';

CREATE INDEX IF NOT EXISTS tournaments_phase_idx
    ON tournaments (phase);

-- ---------------------------------------------------------------------------
-- Registrations. Idempotent by (tournament_id, player_id).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournament_registrations (
    tournament_id TEXT NOT NULL REFERENCES tournaments (tournament_id),
    player_id TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'registered'
        CHECK (status IN ('registered', 'withdrawn', 'eliminated', 'advanced_final')),
    PRIMARY KEY (tournament_id, player_id)
);

COMMENT ON TABLE tournament_registrations IS
    'Player registration records. player_id is identity-only (no FK to Identity DB).';

CREATE INDEX IF NOT EXISTS tournament_registrations_player_idx
    ON tournament_registrations (player_id);

-- ---------------------------------------------------------------------------
-- Rounds: elimination tiers owning bracket slots and completion state.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournament_rounds (
    tournament_id TEXT NOT NULL REFERENCES tournaments (tournament_id),
    round_number INT NOT NULL CHECK (round_number >= 1),
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'seeded', 'provisioning', 'in_progress', 'completed', 'blocked'
    )),
    is_final BOOLEAN NOT NULL DEFAULT false,
    seeded_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (tournament_id, round_number)
);

COMMENT ON TABLE tournament_rounds IS
    'Round aggregate. Completes only when every assigned match is terminal and advancement slots filled.';
COMMENT ON COLUMN tournament_rounds.status IS
    'blocked is used when provisioning or result quarantine prevents round start/completion.';

-- ---------------------------------------------------------------------------
-- Bracket slots and assigned matches (entities under Round).
-- room_id is reference-by-identity; never embed room state.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bracket_slots (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
        'pending', 'assigned', 'in_progress', 'result_recorded', 'advanced', 'quarantined', 'cancelled'
    )),
    seeded_player_ids TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, slot_id),
    FOREIGN KEY (tournament_id, round_number)
        REFERENCES tournament_rounds (tournament_id, round_number)
);

COMMENT ON TABLE bracket_slots IS
    'BracketSlot entity. Slot results may only come from the room assigned to this slot.';

CREATE TABLE IF NOT EXISTS assigned_matches (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    -- Room Gameplay identity only; uniqueness prevents duplicate room assignment.
    room_id TEXT NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    provisioning_batch_id TEXT,
    PRIMARY KEY (tournament_id, round_number, slot_id),
    UNIQUE (room_id),
    -- Composite unique enables match_results ownership FK on exact (slot, room_id).
    UNIQUE (tournament_id, round_number, slot_id, room_id),
    FOREIGN KEY (tournament_id, round_number, slot_id)
        REFERENCES bracket_slots (tournament_id, round_number, slot_id)
);

COMMENT ON TABLE assigned_matches IS
    'AssignedMatch entity. Duplicate room assignments are rejected by UNIQUE(room_id).';
COMMENT ON COLUMN assigned_matches.room_id IS
    'Reference-by-identity to Room Gameplay; no cross-context FK.';

CREATE INDEX IF NOT EXISTS assigned_matches_batch_idx
    ON assigned_matches (tournament_id, round_number, provisioning_batch_id);

-- ---------------------------------------------------------------------------
-- Match results with completion-version dedupe and quarantine.
-- RecordMatchResult / QuarantineTournamentResult idempotent by (room_id, completion_version).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_results (
    room_id TEXT NOT NULL,
    completion_version BIGINT NOT NULL CHECK (completion_version >= 0),
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    -- recorded | duplicate_ignored | quarantined
    disposition TEXT NOT NULL CHECK (disposition IN (
        'recorded', 'duplicate_ignored', 'quarantined'
    )),
    -- Ranked facts from MatchCompleted: wins, card points, completion time, forfeit markers.
    ranked_result JSONB NOT NULL DEFAULT '{}'::jsonb,
    quarantine_reason TEXT,
    source_event_id TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, completion_version),
    -- Ownership: result may only bind to the exact assigned (slot, room_id).
    FOREIGN KEY (tournament_id, round_number, slot_id, room_id)
        REFERENCES assigned_matches (tournament_id, round_number, slot_id, room_id),
    -- Enables advancement FK that keeps source result and slot consistent.
    UNIQUE (tournament_id, round_number, slot_id, room_id, completion_version),
    CONSTRAINT match_results_quarantine_reason CHECK (
        (disposition = 'quarantined' AND quarantine_reason IS NOT NULL)
        OR (disposition <> 'quarantined')
    )
);

COMMENT ON TABLE match_results IS
    'Consumed MatchCompleted facts. Duplicates ignored; conflicts quarantined — never overwrite advancement.';
COMMENT ON COLUMN match_results.completion_version IS
    'Business idempotency half-key with room_id (docs/04, docs/07).';

CREATE INDEX IF NOT EXISTS match_results_slot_idx
    ON match_results (tournament_id, round_number, slot_id);

CREATE INDEX IF NOT EXISTS match_results_quarantined_idx
    ON match_results (processed_at)
    WHERE disposition = 'quarantined';

-- ---------------------------------------------------------------------------
-- Advancement records. Once written for a slot, cannot be overwritten by duplicates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS advancement_records (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    -- Stable identity of the advancement decision for this slot.
    advancement_id TEXT NOT NULL,
    advancing_player_ids TEXT[] NOT NULL,
    -- Tie-break inputs retained for audit/reconstruction (docs/architecture/04 retention).
    tie_break_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_room_id TEXT NOT NULL,
    source_completion_version BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, slot_id),
    UNIQUE (advancement_id),
    FOREIGN KEY (tournament_id, round_number, slot_id)
        REFERENCES bracket_slots (tournament_id, round_number, slot_id),
    -- Source result must belong to this exact slot (result/slot consistency).
    FOREIGN KEY (tournament_id, round_number, slot_id, source_room_id, source_completion_version)
        REFERENCES match_results (tournament_id, round_number, slot_id, room_id, completion_version)
);

COMMENT ON TABLE advancement_records IS
    'PlayersAdvanced decisions. Primary key on slot prevents overwrite by duplicate completions.';

-- ---------------------------------------------------------------------------
-- Sharded provisioning batches: retries then quarantine (docs/07 saga).
-- Idempotent by (tournament_id, round_number, batch_id); retries by retry_attempt.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provisioning_batches (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    batch_id TEXT NOT NULL,
    shard_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'in_progress', 'completed', 'retried', 'quarantined', 'cancelled'
    )),
    retry_attempt INT NOT NULL DEFAULT 0 CHECK (retry_attempt >= 0),
    slot_id_from TEXT,
    slot_id_to TEXT,
    last_error TEXT,
    quarantine_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, batch_id),
    FOREIGN KEY (tournament_id, round_number)
        REFERENCES tournament_rounds (tournament_id, round_number),
    CONSTRAINT provisioning_batches_quarantine_reason CHECK (
        (status = 'quarantined' AND quarantine_reason IS NOT NULL)
        OR (status <> 'quarantined')
    )
);

COMMENT ON TABLE provisioning_batches IS
    'Deterministic provisioning work units. Retry mutates retry_attempt in place; quarantine blocks round start.';
COMMENT ON COLUMN provisioning_batches.retry_attempt IS
    'Saga retry counter. Command idempotency for RetryTournamentProvisioningBatch uses (tournament, round, batch, attempt).';

CREATE INDEX IF NOT EXISTS provisioning_batches_status_idx
    ON provisioning_batches (status)
    WHERE status IN ('pending', 'in_progress', 'retried', 'quarantined');
-- ---------------------------------------------------------------------------
-- Command idempotency (CreateTournament, RegisterPlayer, saga commands, etc.).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_idempotency (
    command_id TEXT PRIMARY KEY,
    tournament_id TEXT,
    player_id TEXT,
    command_type TEXT NOT NULL,
    outcome_status TEXT NOT NULL CHECK (outcome_status IN ('accepted', 'rejected', 'failed')),
    outcome_body JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE command_idempotency IS
    'Command-side dedupe with stable outcome for tournament and saga commands.';

-- ---------------------------------------------------------------------------
-- Outbox for tournament lifecycle, advancement, provisioning, and quarantine events.
-- Append-only Debezium CDC input (ADR-0016/0026): no published_at / app polling.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    tournament_id TEXT,
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE outbox_events IS
    'Append-only tournament facts for Debezium CDC. Never polled or marked published by the app.';
COMMENT ON COLUMN outbox_events.event_type IS
    'Logical event type for Outbox Event Router (tournament.* topics).';
COMMENT ON COLUMN outbox_events.schema_version IS
    'Explicit AsyncAPI schema version; must equal 1.';

CREATE INDEX IF NOT EXISTS outbox_events_tournament_idx
    ON outbox_events (tournament_id, outbox_id);

INSERT INTO schema_migrations (version) VALUES ('001_init')
ON CONFLICT (version) DO NOTHING;
