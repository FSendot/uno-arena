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
    visibility TEXT NOT NULL DEFAULT 'public'
        CHECK (visibility IN ('public', 'private')),
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
COMMENT ON COLUMN tournaments.visibility IS
    'public: anonymous bracket/standings reads allowed; private: participant or operator only.';

CREATE INDEX IF NOT EXISTS tournaments_phase_idx
    ON tournaments (phase);

-- ---------------------------------------------------------------------------
-- Registrations. Idempotent by (tournament_id, player_id).
-- shard_id binds the player to one of 64 registration quota shards (T-Reg).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournament_registrations (
    tournament_id TEXT NOT NULL REFERENCES tournaments (tournament_id),
    player_id TEXT NOT NULL,
    shard_id INT NOT NULL CHECK (shard_id >= 0 AND shard_id < 64),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'registered'
        CHECK (status IN ('registered', 'withdrawn', 'eliminated', 'advanced_final')),
    PRIMARY KEY (tournament_id, player_id)
);

COMMENT ON TABLE tournament_registrations IS
    'Player registration records. player_id is identity-only (no FK to Identity DB).';
COMMENT ON COLUMN tournament_registrations.shard_id IS
    'Registration quota shard 0..63 allocated at admission; preserved across legacy rewrite.';

CREATE INDEX IF NOT EXISTS tournament_registrations_player_idx
    ON tournament_registrations (player_id);

-- Keyset pagination for future bounded registration reads (tournament_id, registered_at, player_id).
CREATE INDEX IF NOT EXISTS tournament_registrations_keyset_idx
    ON tournament_registrations (tournament_id, registered_at, player_id);

-- ---------------------------------------------------------------------------
-- Fixed 64-way registration quota shards per tournament (T-Reg hot path).
-- Quotas are immutable after create and sum EXACTLY to tournaments.capacity.
-- Admission atomically UPDATE count=count+1 WHERE count<quota; never SUM-admit.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournament_registration_shards (
    tournament_id TEXT NOT NULL REFERENCES tournaments (tournament_id),
    shard_id INT NOT NULL CHECK (shard_id >= 0 AND shard_id < 64),
    quota INT NOT NULL CHECK (quota >= 0),
    count INT NOT NULL DEFAULT 0 CHECK (count >= 0),
    PRIMARY KEY (tournament_id, shard_id),
    CONSTRAINT tournament_registration_shards_count_bound CHECK (count <= quota)
);

COMMENT ON TABLE tournament_registration_shards IS
    'Sharded registration quotas. Per RegisterPlayer mutates one shard; global-full check is O(64) only on shard fill.';
COMMENT ON COLUMN tournament_registration_shards.quota IS
    'Immutable per-tournament shard capacity; floor(capacity/64)+remainder distribution.';
COMMENT ON COLUMN tournament_registration_shards.count IS
    'Current reservations in this shard; SUM(count) is authoritative registeredCount.';

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
-- Fixed 64-way progress shards per round (hot-path MatchCompleted counters).
-- shard_id = slot_index % 64. No single hot round counter is mutated per event.
-- SUM(assigned_count) must equal assigned matches after every legacy rewrite.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS round_progress_shards (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    shard_id INT NOT NULL CHECK (shard_id >= 0 AND shard_id < 64),
    assigned_count INT NOT NULL DEFAULT 0 CHECK (assigned_count >= 0),
    resolved_count INT NOT NULL DEFAULT 0 CHECK (resolved_count >= 0),
    quarantined_count INT NOT NULL DEFAULT 0 CHECK (quarantined_count >= 0),
    -- Authoritative advancing player count for CompleteRound remainingPlayers (O(64) SUM).
    advancing_count INT NOT NULL DEFAULT 0 CHECK (advancing_count >= 0),
    PRIMARY KEY (tournament_id, round_number, shard_id),
    FOREIGN KEY (tournament_id, round_number)
        REFERENCES tournament_rounds (tournament_id, round_number),
    -- Terminal: resolved+quarantined never exceed assigned; advancing bounded by resolved*PlayersPerRoom.
    CONSTRAINT round_progress_shards_terminal_bound CHECK (
        resolved_count + quarantined_count <= assigned_count
        AND advancing_count <= resolved_count * 10
    )
);

COMMENT ON TABLE round_progress_shards IS
    'Sharded round progress. Per MatchCompleted mutates one shard only; readiness is O(64) SUM.';
COMMENT ON COLUMN round_progress_shards.shard_id IS
    'Fixed shard 0..63; assignment is slot_index % 64.';
COMMENT ON COLUMN round_progress_shards.advancing_count IS
    'Players advanced on this shard; incremented only on first successful Record (not reject/dup/quarantine).';

-- Hint index for FindReadyRoundCandidate (status=in_progress); CompleteRound TX revalidates.
CREATE INDEX IF NOT EXISTS tournament_rounds_in_progress_idx
    ON tournament_rounds (tournament_id, round_number)
    WHERE status = 'in_progress';

-- ---------------------------------------------------------------------------
-- Bracket slots and assigned matches (entities under Round).
-- room_id is reference-by-identity; never embed room state.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bracket_slots (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    -- Stable keyset identity for bounded public pagination (ADR-0038).
    slot_index INT NOT NULL CHECK (slot_index >= 0),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
        'pending', 'assigned', 'in_progress', 'result_recorded', 'advanced', 'quarantined', 'cancelled'
    )),
    seeded_player_ids TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, slot_id),
    UNIQUE (tournament_id, round_number, slot_index),
    FOREIGN KEY (tournament_id, round_number)
        REFERENCES tournament_rounds (tournament_id, round_number)
);

COMMENT ON TABLE bracket_slots IS
    'BracketSlot entity. Slot results may only come from the room assigned to this slot.';
COMMENT ON COLUMN bracket_slots.slot_index IS
    'Durable zero-based slot identity within a round; public live keyset cursor boundary.';

-- Keyset pagination: (tournament_id, round_number, slot_index) for LoadBracketSlotPage.
CREATE INDEX IF NOT EXISTS bracket_slots_keyset_idx
    ON bracket_slots (tournament_id, round_number, slot_index);

-- ---------------------------------------------------------------------------
-- Normalized player→slot assignment index (scale: never scan seeded_player_ids arrays).
-- One player per round; one seat per (round, slot). Latest-round discovery via
-- (tournament_id, player_id, round_number DESC) LIMIT 1.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tournament_round_slot_players (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL CHECK (round_number >= 1),
    player_id TEXT NOT NULL,
    slot_id TEXT NOT NULL,
    seat_index INT NOT NULL CHECK (seat_index >= 0),
    PRIMARY KEY (tournament_id, round_number, player_id),
    UNIQUE (tournament_id, round_number, slot_id, seat_index),
    FOREIGN KEY (tournament_id, round_number, slot_id)
        REFERENCES bracket_slots (tournament_id, round_number, slot_id)
);

COMMENT ON TABLE tournament_round_slot_players IS
    'Normalized player-to-slot mapping. Assignment discovery uses indexed keys only.';
COMMENT ON COLUMN tournament_round_slot_players.seat_index IS
    'Zero-based ordinal within the slot seeded_player_ids array at seed time.';

CREATE INDEX IF NOT EXISTS tournament_round_slot_players_player_round_idx
    ON tournament_round_slot_players (tournament_id, player_id, round_number DESC);

CREATE TABLE IF NOT EXISTS assigned_matches (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL,
    slot_id TEXT NOT NULL,
    -- Room Gameplay identity only; uniqueness prevents duplicate room assignment.
    room_id TEXT NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    provisioning_batch_id TEXT,
    -- First RoomRuntimeReady observation. NULL keeps room_id internal/provisioning.
    runtime_ready_event_id TEXT UNIQUE,
    runtime_ready_generation BIGINT CHECK (runtime_ready_generation >= 1),
    runtime_ready_at TIMESTAMPTZ,
    PRIMARY KEY (tournament_id, round_number, slot_id),
    UNIQUE (room_id),
    -- Composite unique enables match_results ownership FK on exact (slot, room_id).
    UNIQUE (tournament_id, round_number, slot_id, room_id),
    FOREIGN KEY (tournament_id, round_number, slot_id)
        REFERENCES bracket_slots (tournament_id, round_number, slot_id),
    CONSTRAINT assigned_matches_runtime_ready_complete CHECK (
        (runtime_ready_event_id IS NULL AND runtime_ready_generation IS NULL AND runtime_ready_at IS NULL)
        OR
        (runtime_ready_event_id IS NOT NULL AND runtime_ready_generation IS NOT NULL AND runtime_ready_at IS NOT NULL)
    )
);

COMMENT ON TABLE assigned_matches IS
    'AssignedMatch entity. Duplicate room assignments are rejected by UNIQUE(room_id).';
COMMENT ON COLUMN assigned_matches.room_id IS
    'Reference-by-identity to Room Gameplay; no cross-context FK.';
COMMENT ON COLUMN assigned_matches.runtime_ready_event_id IS
    'Deduplicates the first RoomRuntimeReady event atomically with making room_id externally visible.';

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
-- Match-result quarantine ledger (sanitized conflict metadata).
-- Used when claimed identity cannot satisfy match_results ownership FK, or when
-- a conflict must be recorded without mutating disposition=recorded rows.
-- Never stores raw private payload.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_result_quarantines (
    quarantine_id TEXT PRIMARY KEY,
    source_event_id TEXT,
    tournament_id TEXT NOT NULL REFERENCES tournaments (tournament_id),
    claimed_room_id TEXT NOT NULL,
    claimed_round_number INT CHECK (claimed_round_number IS NULL OR claimed_round_number >= 1),
    claimed_slot_id TEXT,
    completion_version BIGINT NOT NULL CHECK (completion_version >= 0),
    fingerprint TEXT,
    reason TEXT NOT NULL,
    resolved_round_number INT CHECK (resolved_round_number IS NULL OR resolved_round_number >= 1),
    resolved_slot_id TEXT,
    affects_slot BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT match_result_quarantines_resolved_pair CHECK (
        (resolved_round_number IS NULL AND resolved_slot_id IS NULL)
        OR (resolved_round_number IS NOT NULL AND resolved_slot_id IS NOT NULL)
    )
);

COMMENT ON TABLE match_result_quarantines IS
    'Durable sanitized MatchCompleted / QuarantineTournamentResult conflict metadata. Does not require assigned(slot,room) FK.';
COMMENT ON COLUMN match_result_quarantines.affects_slot IS
    'true when quarantine binds a trustworthy resolved slot (blocks that slot/round publicly).';
COMMENT ON COLUMN match_result_quarantines.claimed_room_id IS
    'Business idempotency half-key with completion_version (docs/04 QuarantineTournamentResult).';

-- Business-key idempotency: exactly one ledger row per (roomId, completionVersion).
CREATE UNIQUE INDEX IF NOT EXISTS match_result_quarantines_business_key_uidx
    ON match_result_quarantines (claimed_room_id, completion_version);

CREATE INDEX IF NOT EXISTS match_result_quarantines_tournament_idx
    ON match_result_quarantines (tournament_id, created_at DESC);
CREATE INDEX IF NOT EXISTS match_result_quarantines_resolved_slot_idx
    ON match_result_quarantines (tournament_id, resolved_round_number, resolved_slot_id, created_at DESC)
    WHERE resolved_slot_id IS NOT NULL AND affects_slot = true;
CREATE INDEX IF NOT EXISTS match_result_quarantines_claimed_room_idx
    ON match_result_quarantines (tournament_id, claimed_room_id, completion_version);

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
-- Normalized advancing players (T4 scale/keyset source for later-round seeding).
-- One row per (tournament, source_round, player). Rank is stable per source slot ordinal.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS round_advancing_players (
    tournament_id TEXT NOT NULL,
    source_round_number INT NOT NULL CHECK (source_round_number >= 1),
    player_id TEXT NOT NULL,
    source_slot_id TEXT NOT NULL,
    advancement_rank INT NOT NULL CHECK (advancement_rank >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, source_round_number, player_id),
    UNIQUE (tournament_id, source_round_number, source_slot_id, advancement_rank),
    FOREIGN KEY (tournament_id, source_round_number, source_slot_id)
        REFERENCES bracket_slots (tournament_id, round_number, slot_id)
);

COMMENT ON TABLE round_advancing_players IS
    'Normalized advancement source for later-round seeding. Keyset (tournament_id, source_round_number, player_id).';
COMMENT ON COLUMN round_advancing_players.advancement_rank IS
    'Zero-based ordinal within the source slot advancing array (deterministic rebuild).';

-- Exact keyset supporting (tournament_id, source_round_number, player_id) ASC scans.
CREATE INDEX IF NOT EXISTS round_advancing_players_keyset_idx
    ON round_advancing_players (tournament_id, source_round_number, player_id);

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
    -- Worker visibility lease (ADR-0013). Cleared on domain rewrite / reap.
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    -- Heartbeat stamp for long Room-call batches (owner-checked renewals).
    lease_heartbeat_at TIMESTAMPTZ,
    -- Monotonic fence token; incremented on every claim/reclaim. Survives lease clear.
    -- May remain 0 while unleased; owner alone is insufficient across pod restart.
    lease_version BIGINT NOT NULL DEFAULT 0,
    -- Differential prepare/finalize timestamps (T5/T6); null until worker prepare/complete.
    prepared_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, batch_id),
    FOREIGN KEY (tournament_id, round_number)
        REFERENCES tournament_rounds (tournament_id, round_number),
    CONSTRAINT provisioning_batches_quarantine_reason CHECK (
        (status = 'quarantined' AND quarantine_reason IS NOT NULL)
        OR (status <> 'quarantined')
    ),
    -- lease_version may stay 0 (or any prior value) when lease columns are cleared.
    CONSTRAINT provisioning_batches_lease_consistency CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

COMMENT ON TABLE provisioning_batches IS
    'Deterministic provisioning work units. Retry mutates retry_attempt in place; quarantine blocks round start.';
COMMENT ON COLUMN provisioning_batches.retry_attempt IS
    'Saga retry counter. Command idempotency for RetryTournamentProvisioningBatch uses (tournament, round, batch, attempt).';
COMMENT ON COLUMN provisioning_batches.lease_owner IS
    'Claiming worker identity while status=in_progress. Aggregate rewrite preserves other active in_progress leases; clears completed/retried/quarantined/cancelled leases.';
COMMENT ON COLUMN provisioning_batches.lease_expires_at IS
    'Visibility deadline for in_progress claims; expired leases are reaped (tournament-first, bounded) or reclaimable via SKIP LOCKED.';
COMMENT ON COLUMN provisioning_batches.lease_heartbeat_at IS
    'Last owner-checked lease renewal during Room provision fanout; cleared with lease.';
COMMENT ON COLUMN provisioning_batches.lease_version IS
    'Fence token bumped atomically on claim/reclaim; prepare/heartbeat/finalize require exact match with owner+status+retry_attempt.';
COMMENT ON COLUMN provisioning_batches.prepared_at IS
    'Set when differential prepare inserts/confirms assigned_matches + match.assigned outbox for the batch range.';
COMMENT ON COLUMN provisioning_batches.completed_at IS
    'Set when differential success finalize marks the batch completed; cleared on non-completed statuses.';

-- Claimable work + expired in_progress leases (terminal completed/quarantined/cancelled excluded).
CREATE INDEX IF NOT EXISTS provisioning_batches_claimable_idx
    ON provisioning_batches (created_at)
    WHERE status IN ('pending', 'retried', 'in_progress');
CREATE INDEX IF NOT EXISTS provisioning_batches_lease_expires_idx
    ON provisioning_batches (lease_expires_at)
    WHERE status = 'in_progress' AND lease_expires_at IS NOT NULL;
-- Tournament-first claim: nonterminal tournaments with a provisioning round.
CREATE INDEX IF NOT EXISTS tournament_rounds_provisioning_idx
    ON tournament_rounds (tournament_id)
    WHERE status = 'provisioning';
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

-- Keyset Analytics backfill (ADR-0039): topic + outbox_id pagination without OFFSET.
CREATE INDEX IF NOT EXISTS outbox_events_topic_outbox_idx
    ON outbox_events (topic, outbox_id);

-- ---------------------------------------------------------------------------
-- ADR-0017 Kafka aggregate quarantine (Tournament-owned ordered consumer).
-- Keyed by (consumer_group, source_topic, aggregate_key=roomId).
-- Stores sanitized operational metadata only — never raw private payload/secrets.
-- released_at / release_note support later operator release/audit without an API here.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_consumer_quarantine (
    consumer_group TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    aggregate_key TEXT NOT NULL,
    classification TEXT NOT NULL,
    reason TEXT NOT NULL,
    source_partition INT,
    source_offset BIGINT,
    event_id TEXT,
    correlation_id TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    release_note TEXT,
    PRIMARY KEY (consumer_group, source_topic, aggregate_key),
    CONSTRAINT kafka_consumer_quarantine_release_consistency CHECK (
        (active = true AND released_at IS NULL)
        OR (active = false AND released_at IS NOT NULL)
    )
);

COMMENT ON TABLE kafka_consumer_quarantine IS
    'ADR-0017 ordered Kafka aggregate quarantine. Active rows block apply for that roomId until release.';
COMMENT ON COLUMN kafka_consumer_quarantine.aggregate_key IS
    'Ordering key for room.match.completed (roomId).';
COMMENT ON COLUMN kafka_consumer_quarantine.reason IS
    'Sanitized failure summary; never store secrets or raw private payload.';
COMMENT ON COLUMN kafka_consumer_quarantine.active IS
    'true while quarantine holds; false after operator/replay release (audit via released_at).';

CREATE INDEX IF NOT EXISTS kafka_consumer_quarantine_active_idx
    ON kafka_consumer_quarantine (consumer_group, source_topic)
    WHERE active = true;

-- ---------------------------------------------------------------------------
-- Bracket projection checkpoint (Postgres-backed until Redis projection lands).
-- Monotonic tournament-visible version; O(1) read — never derived from slot scan.
-- Bumped only when CommitRequest.ProjectionChanged is set (accepted bracket-visible
-- state changes with domain facts). Rejected and semantic no-ops never bump.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bracket_projection_versions (
    tournament_id TEXT PRIMARY KEY
        REFERENCES tournaments (tournament_id),
    projection_version BIGINT NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE bracket_projection_versions IS
    'Tournament-owned public bracket projectionVersion/generatedAt checkpoint (ADR-0038).';
COMMENT ON COLUMN bracket_projection_versions.projection_version IS
    'Monotonic version observed on BracketPage; bumped only on ProjectionChanged commits.';

-- ---------------------------------------------------------------------------
-- Sharded public projection checkpoints (hot-path MatchCompleted).
-- shard_id = slot_index % 64. Differential bumps one shard only; legacy may bump
-- the base bracket_projection_versions row. Public projectionVersion =
-- base.version + SUM(shard.version); missing shards count as zero.
-- Never DELETE/reset these rows during legacy rewrite (barrier ensures consistency).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bracket_projection_shards (
    tournament_id TEXT NOT NULL REFERENCES tournaments (tournament_id),
    shard_id INT NOT NULL CHECK (shard_id >= 0 AND shard_id < 64),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, shard_id)
);

COMMENT ON TABLE bracket_projection_shards IS
    'Per-slot-shard projection checkpoints. Hot MatchCompleted bumps only slot_index%64.';
COMMENT ON COLUMN bracket_projection_shards.version IS
    'Monotonic shard contribution to public projectionVersion (base + SUM(shards)).';

-- ---------------------------------------------------------------------------
-- Seeding jobs (durable bounded T3/T4). Kickoff schedules; worker chunks all rounds.
-- Unique (tournament_id, round_number) elects one job across command ids.
-- Immutable plan: player_count/slot_count/base_size/remainder from kickoff N.
-- Round 1 source=registrations (source_round_number NULL); later source=advancement.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS round_seeding_jobs (
    tournament_id TEXT NOT NULL
        REFERENCES tournaments (tournament_id),
    round_number INT NOT NULL CHECK (round_number >= 1),
    source TEXT NOT NULL DEFAULT 'registrations'
        CHECK (source IN ('registrations', 'advancement')),
    source_round_number INT CHECK (
        (source = 'registrations' AND round_number = 1 AND source_round_number IS NULL)
        OR (source = 'advancement' AND round_number > 1 AND source_round_number = round_number - 1)
    ),
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'in_progress', 'completed', 'quarantined', 'cancelled'
    )),
    player_count INT NOT NULL CHECK (player_count >= 1),
    slot_count INT NOT NULL CHECK (slot_count >= 1),
    base_size INT NOT NULL CHECK (base_size >= 0),
    remainder INT NOT NULL CHECK (remainder >= 0),
    next_slot_index INT NOT NULL DEFAULT 0 CHECK (next_slot_index >= 0),
    processed_player_count INT NOT NULL DEFAULT 0 CHECK (processed_player_count >= 0),
    last_player_id TEXT NOT NULL DEFAULT '',
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    -- Monotonic fence token; incremented on every claim/reclaim. Survives lease clear/reap.
    -- May remain 0 while unleased; owner alone is insufficient across expiry/reclaim.
    lease_version BIGINT NOT NULL DEFAULT 0,
    command_id TEXT NOT NULL,
    correlation_id TEXT,
    quarantine_reason TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number),
    -- Tournament-owned (not round-owned): survives legacy tournament_rounds delete/reinsert.
    -- Do not ON DELETE CASCADE — audit/job state must outlive round rewrites.
    CONSTRAINT round_seeding_jobs_plan_consistency CHECK (
        player_count = (base_size * slot_count) + remainder
        AND remainder < slot_count
    ),
    CONSTRAINT round_seeding_jobs_progress_bound CHECK (
        next_slot_index <= slot_count
        AND processed_player_count <= player_count
    ),
    CONSTRAINT round_seeding_jobs_quarantine_reason CHECK (
        (status = 'quarantined' AND quarantine_reason IS NOT NULL)
        OR (status <> 'quarantined')
    ),
    -- lease_version may stay 0 (or any prior value) when lease columns are cleared.
    CONSTRAINT round_seeding_jobs_lease_consistency CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT round_seeding_jobs_completed_at CHECK (
        (status = 'completed' AND completed_at IS NOT NULL)
        OR (status <> 'completed' AND completed_at IS NULL)
    )
);

COMMENT ON TABLE round_seeding_jobs IS
    'Durable seeding work for any round. Kickoff/CompleteRound inserts pending; worker claims/chunks; finalization completes. FK is tournament-owned so legacy round rewrites do not erase jobs.';
COMMENT ON COLUMN round_seeding_jobs.source IS
    'registrations for round 1; advancement for round>1 (source_round_number = round_number - 1).';
COMMENT ON COLUMN round_seeding_jobs.source_round_number IS
    'NULL for round-1 registrations; completed source round for advancement seeding.';
COMMENT ON COLUMN round_seeding_jobs.last_player_id IS
    'Keyset cursor: next chunk reads player_id > last_player_id ORDER BY player_id ASC.';
COMMENT ON COLUMN round_seeding_jobs.lease_version IS
    'Fence token bumped atomically on claim/reclaim; chunk/finalize/quarantine/cancel require exact match with owner+status=in_progress. Reap clears owner/expiry but preserves version.';
COMMENT ON COLUMN round_seeding_jobs.quarantine_reason IS
    'Allowlisted operational reason code only; never raw SQL/error text; never exposed on public BracketPage.';
COMMENT ON COLUMN round_seeding_jobs.completed_at IS
    'Set only on successful completion; NULL for pending/in_progress/quarantined/cancelled.';

-- Claimable pending + expired in_progress (terminal completed/quarantined/cancelled excluded).
CREATE INDEX IF NOT EXISTS round_seeding_jobs_claimable_idx
    ON round_seeding_jobs (created_at)
    WHERE status IN ('pending', 'in_progress');
CREATE INDEX IF NOT EXISTS round_seeding_jobs_lease_expires_idx
    ON round_seeding_jobs (lease_expires_at)
    WHERE status = 'in_progress' AND lease_expires_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Seeding batch ledger (audit/resume). One row per committed chunk (any round).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS round_seeding_batches (
    tournament_id TEXT NOT NULL,
    round_number INT NOT NULL CHECK (round_number >= 1),
    batch_index INT NOT NULL CHECK (batch_index >= 0),
    slot_index_from INT NOT NULL CHECK (slot_index_from >= 0),
    slot_index_to INT NOT NULL CHECK (slot_index_to >= slot_index_from),
    player_count INT NOT NULL CHECK (player_count >= 1),
    source_cursor_after TEXT NOT NULL DEFAULT '',
    source_cursor_to TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tournament_id, round_number, batch_index),
    FOREIGN KEY (tournament_id, round_number)
        REFERENCES round_seeding_jobs (tournament_id, round_number)
);

COMMENT ON TABLE round_seeding_batches IS
    'Append-only ledger of committed seeding chunks; supports resume/audit without rescanning.';
COMMENT ON COLUMN round_seeding_batches.checksum IS
    'Deterministic digest over ordered (slot_id, player_ids) for the chunk.';

INSERT INTO schema_migrations (version) VALUES ('001_init')
ON CONFLICT (version) DO NOTHING;
