package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/platform/telemetry"
	"unoarena/services/identity/domain"
)

// SessionStore implements domain.SessionRepository against Postgres (CDC outbox).
type SessionStore struct {
	pool *pgxpool.Pool
}

func NewSessionStore(pool *pgxpool.Pool) *SessionStore {
	return &SessionStore{pool: pool}
}

func (s *SessionStore) Save(ctx context.Context, session domain.Session) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sessions (
			session_id, player_id, token_hash, status, created_at,
			invalidated_at, invalidation_reason, expires_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (session_id) DO UPDATE SET
			token_hash = EXCLUDED.token_hash,
			status = EXCLUDED.status,
			invalidated_at = EXCLUDED.invalidated_at,
			invalidation_reason = EXCLUDED.invalidation_reason,
			expires_at = EXCLUDED.expires_at
	`, session.ID.String(), session.PlayerID.String(), session.TokenHash, string(session.Status),
		session.CreatedAt.UTC(), session.InvalidatedAt, nullIfEmpty(session.InvalidationReason), nullableTime(session.ExpiresAt))
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	return nil
}

func (s *SessionStore) FindByID(ctx context.Context, id domain.SessionID) (domain.Session, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT session_id, player_id, token_hash, status, created_at,
			invalidated_at, invalidation_reason, expires_at
		FROM sessions WHERE session_id = $1
	`, id.String())
	sess, err := scanSession(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Session{}, false, nil
	}
	if err != nil {
		return domain.Session{}, false, domain.WrapUnavailable(err)
	}
	return sess, true, nil
}

func (s *SessionStore) FindByTokenHash(ctx context.Context, tokenHash string) (domain.Session, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT session_id, player_id, token_hash, status, created_at,
			invalidated_at, invalidation_reason, expires_at
		FROM sessions WHERE token_hash = $1
	`, tokenHash)
	sess, err := scanSession(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Session{}, false, nil
	}
	if err != nil {
		return domain.Session{}, false, domain.WrapUnavailable(err)
	}
	return sess, true, nil
}

func (s *SessionStore) FindActiveByPlayer(ctx context.Context, playerID domain.PlayerID) (domain.Session, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT session_id, player_id, token_hash, status, created_at,
			invalidated_at, invalidation_reason, expires_at
		FROM sessions WHERE player_id = $1 AND status = 'active'
	`, playerID.String())
	sess, err := scanSession(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Session{}, false, nil
	}
	if err != nil {
		return domain.Session{}, false, domain.WrapUnavailable(err)
	}
	return sess, true, nil
}

func (s *SessionStore) ReplaceActiveSession(ctx context.Context, previous *domain.Session, next domain.Session, eventFor func(domain.Session) domain.SessionInvalidatedEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	defer tx.Rollback(ctx)

	// Serialize all session replacements for this player, including the
	// no-prior-session race, by locking the player row before reading sessions.
	var lockedPlayer string
	err = tx.QueryRow(ctx, `
		SELECT player_id FROM players WHERE player_id = $1 FOR UPDATE
	`, next.PlayerID.String()).Scan(&lockedPlayer)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrPlayerNotFound
	}
	if err != nil {
		return domain.WrapUnavailable(err)
	}

	rows, err := tx.Query(ctx, `
		SELECT session_id, player_id, token_hash, status, created_at,
			invalidated_at, invalidation_reason, expires_at
		FROM sessions
		WHERE player_id = $1 AND status = 'active'
		FOR UPDATE
	`, next.PlayerID.String())
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	var active []domain.Session
	for rows.Next() {
		sess, scanErr := scanSession(rows)
		if scanErr != nil {
			rows.Close()
			return domain.WrapUnavailable(scanErr)
		}
		active = append(active, sess)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return domain.WrapUnavailable(err)
	}

	for _, cur := range active {
		inv := cur
		if previous != nil && cur.ID == previous.ID {
			inv = *previous
		} else {
			ts := next.CreatedAt
			inv.Status = domain.SessionStatusInvalidated
			inv.InvalidatedAt = &ts
			inv.InvalidationReason = domain.ReasonLoginTakeover
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sessions SET
				status = $2,
				invalidated_at = $3,
				invalidation_reason = $4
			WHERE session_id = $1 AND status = 'active'
		`, inv.ID.String(), string(inv.Status), inv.InvalidatedAt, nullIfEmpty(inv.InvalidationReason)); err != nil {
			return domain.WrapUnavailable(err)
		}
		if eventFor != nil {
			if err := insertOutbox(ctx, tx, eventFor(inv)); err != nil {
				return err
			}
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sessions (
			session_id, player_id, token_hash, status, created_at,
			invalidated_at, invalidation_reason, expires_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, next.ID.String(), next.PlayerID.String(), next.TokenHash, string(next.Status),
		next.CreatedAt.UTC(), next.InvalidatedAt, nullIfEmpty(next.InvalidationReason), nullableTime(next.ExpiresAt)); err != nil {
		return domain.WrapUnavailable(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.WrapUnavailable(err)
	}
	return nil
}

func (s *SessionStore) InvalidateWithOutbox(ctx context.Context, session domain.Session, event domain.SessionInvalidatedEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE sessions SET
			status = $2,
			invalidated_at = $3,
			invalidation_reason = $4
		WHERE session_id = $1 AND status = 'active'
	`, session.ID.String(), string(session.Status), session.InvalidatedAt, nullIfEmpty(session.InvalidationReason))
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		// Idempotent: already inactive or missing — check existence.
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = $1)`, session.ID.String()).Scan(&exists); err != nil {
			return domain.WrapUnavailable(err)
		}
		if !exists {
			return domain.ErrSessionNotFound
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.WrapUnavailable(err)
		}
		return nil
	}
	if err := insertOutbox(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WrapUnavailable(err)
	}
	return nil
}

func (s *SessionStore) ListPendingOutbox(context.Context, int) ([]domain.OutboxEntry, error) {
	return nil, ErrCapabilityOnly
}

func (s *SessionStore) MarkOutboxPublished(context.Context, string, time.Time) error {
	return ErrCapabilityOnly
}

func insertOutbox(ctx context.Context, tx pgx.Tx, event domain.SessionInvalidatedEvent) error {
	event = event.Normalize()
	traceparent, tracestate := telemetry.TraceContextHeaders(ctx)
	payload, err := json.Marshal(event.OutboxPayload())
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, topic, partition_key, schema_version, player_id, payload,
			traceparent, tracestate
		) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
	`, event.EventID, event.EventType, domain.OutboxTopicSessionInvalidated,
		event.PlayerID.String(), event.SchemaVersion, event.PlayerID.String(), string(payload),
		nullIfEmpty(traceparent), nullIfEmpty(tracestate))
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	return nil
}

func scanSession(row scannable) (domain.Session, error) {
	var (
		id, playerID, tokenHash, status string
		created                         time.Time
		invalidatedAt                   *time.Time
		reason                          *string
		expiresAt                       *time.Time
	)
	if err := row.Scan(&id, &playerID, &tokenHash, &status, &created, &invalidatedAt, &reason, &expiresAt); err != nil {
		return domain.Session{}, err
	}
	sess := domain.Session{
		ID:            domain.SessionID(id),
		PlayerID:      domain.PlayerID(playerID),
		TokenHash:     tokenHash,
		Status:        domain.SessionStatus(status),
		CreatedAt:     created.UTC(),
		InvalidatedAt: invalidatedAt,
	}
	if reason != nil {
		sess.InvalidationReason = *reason
	}
	if expiresAt != nil {
		sess.ExpiresAt = expiresAt.UTC()
	}
	return sess, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

var _ domain.SessionRepository = (*SessionStore)(nil)
