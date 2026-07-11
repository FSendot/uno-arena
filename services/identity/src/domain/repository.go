package domain

import "time"

// PlayerRepository persists registered players.
type PlayerRepository interface {
	Save(player Player) error
	FindByUsername(username string) (Player, bool)
	FindByID(id PlayerID) (Player, bool)
}

// OutboxEntry is a durable pending SessionInvalidated record.
type OutboxEntry struct {
	Event       SessionInvalidatedEvent
	CreatedAt   time.Time
	PublishedAt *time.Time
}

// SessionRepository persists sessions, enforces single-active lookup, and owns
// the transactional outbox for SessionInvalidated.
type SessionRepository interface {
	Save(session Session) error
	FindByID(id SessionID) (Session, bool)
	FindByTokenHash(tokenHash string) (Session, bool)
	FindActiveByPlayer(playerID PlayerID) (Session, bool)
	// ReplaceActiveSession atomically invalidates any active session for next.PlayerID,
	// installs next, and enqueues a pending outbox event for each invalidated session
	// via eventFor. On error, prior persisted state (including outbox) is unchanged.
	ReplaceActiveSession(previous *Session, next Session, eventFor func(Session) SessionInvalidatedEvent) error
	// InvalidateWithOutbox atomically persists session invalidation/expiry and a
	// pending SessionInvalidated outbox event in one commit, but only when the
	// persisted session is still active. If already inactive, it is a no-op
	// success (stable idempotent outcome) and must not enqueue another outbox row.
	InvalidateWithOutbox(session Session, event SessionInvalidatedEvent) error
	ListPendingOutbox(limit int) ([]OutboxEntry, error)
	MarkOutboxPublished(eventID string, publishedAt time.Time) error
}
