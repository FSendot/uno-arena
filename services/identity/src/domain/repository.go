package domain

import (
	"context"
	"time"
)

// PlayerRepository persists registered players and OIDC external mappings.
type PlayerRepository interface {
	Save(ctx context.Context, player Player) error
	FindByUsername(ctx context.Context, username string) (Player, bool, error)
	FindByID(ctx context.Context, id PlayerID) (Player, bool, error)
	// RegisterWithDefaultACL atomically inserts the player and default ACL role.
	RegisterWithDefaultACL(ctx context.Context, player Player, role string) error
	ListRoles(ctx context.Context, playerID PlayerID) ([]string, error)
	// LinkOrResolveExternal is race-safe: unique (issuer,subject)->player;
	// creates player+ACL+link in one TX when missing. acceptedRoles are already
	// normalized/allowlisted and must be reconciled into player_acls in the same TX.
	LinkOrResolveExternal(ctx context.Context, issuer, subject, preferredUsername string, newPlayer Player, acceptedRoles []string) (Player, []string, error)
}

// OutboxEntry is a durable pending SessionInvalidated record (capability/memory path).
// PublishedAt is capability-mode only; durable CDC never sets it.
type OutboxEntry struct {
	Event       SessionInvalidatedEvent
	CreatedAt   time.Time
	PublishedAt *time.Time
}

// SessionRepository persists sessions, enforces single-active lookup, and owns
// the transactional outbox for SessionInvalidated.
type SessionRepository interface {
	Save(ctx context.Context, session Session) error
	FindByID(ctx context.Context, id SessionID) (Session, bool, error)
	FindByTokenHash(ctx context.Context, tokenHash string) (Session, bool, error)
	FindActiveByPlayer(ctx context.Context, playerID PlayerID) (Session, bool, error)
	// ReplaceActiveSession atomically invalidates any active session for next.PlayerID,
	// installs next, and enqueues a pending outbox event for each invalidated session
	// via eventFor. On error, prior persisted state (including outbox) is unchanged.
	ReplaceActiveSession(ctx context.Context, previous *Session, next Session, eventFor func(Session) SessionInvalidatedEvent) error
	// InvalidateWithOutbox atomically persists session invalidation/expiry and a
	// pending SessionInvalidated outbox event in one commit, but only when the
	// persisted session is still active. If already inactive, it is a no-op
	// success (stable idempotent outcome) and must not enqueue another outbox row.
	InvalidateWithOutbox(ctx context.Context, session Session, event SessionInvalidatedEvent) error
	// ListPendingOutbox is capability/memory only — durable Postgres never polls.
	ListPendingOutbox(ctx context.Context, limit int) ([]OutboxEntry, error)
	// MarkOutboxPublished is capability/memory only — durable Postgres never marks.
	MarkOutboxPublished(ctx context.Context, eventID string, publishedAt time.Time) error
}
