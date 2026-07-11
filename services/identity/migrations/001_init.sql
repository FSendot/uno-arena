-- Identity authoritative schema (Postgres).
-- Empty-db baseline 001_init. No cross-context tables.
-- Outbox is append-only for Debezium CDC (no published_at / polling index).

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS players (
    player_id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS player_acls (
    player_id TEXT NOT NULL REFERENCES players (player_id),
    role TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, role)
);

-- Provider-neutral OIDC mapping: unique (issuer, subject) -> player (ADR-0023).
CREATE TABLE IF NOT EXISTS external_identities (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    player_id TEXT NOT NULL REFERENCES players (player_id),
    linked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer, subject)
);

CREATE INDEX IF NOT EXISTS external_identities_player_idx
    ON external_identities (player_id);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL REFERENCES players (player_id),
    token_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('active', 'invalidated', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    invalidated_at TIMESTAMPTZ,
    invalidation_reason TEXT,
    expires_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS sessions_one_active_per_player
    ON sessions (player_id)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS command_idempotency (
    command_id TEXT PRIMARY KEY,
    player_id TEXT,
    command_type TEXT NOT NULL,
    outcome_status TEXT NOT NULL,
    outcome_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: same commit as session mutation; Debezium publishes (ADR-0016).
-- Handlers append only — no published_at marking / app polling (ADR-0026).
CREATE TABLE IF NOT EXISTS outbox_events (
    outbox_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    player_id TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE outbox_events IS
    'Append-only SessionInvalidated bridge. Captured by Identity Debezium connector; never polled by the app.';
COMMENT ON COLUMN outbox_events.event_type IS
    'Logical event type for Outbox Event Router (SessionInvalidated).';
COMMENT ON COLUMN outbox_events.schema_version IS
    'Explicit AsyncAPI schema version; must equal 1.';

INSERT INTO schema_migrations (version) VALUES ('001_init')
ON CONFLICT (version) DO NOTHING;
