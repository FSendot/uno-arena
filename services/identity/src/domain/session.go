package domain

import "time"

// SessionStatus mirrors the authoritative session lifecycle.
type SessionStatus string

const (
	SessionStatusActive      SessionStatus = "active"
	SessionStatusInvalidated SessionStatus = "invalidated"
	SessionStatusExpired     SessionStatus = "expired"
)

// Invalidation reasons for auditability / takeover investigation.
const (
	ReasonLoginTakeover = "login_takeover"
	ReasonLogout        = "logout"
	ReasonExpired       = "expired"
	ReasonAdmin         = "admin"
)

// Session is a single authenticated session for one player.
type Session struct {
	ID                 SessionID
	PlayerID           PlayerID
	TokenHash          string
	Status             SessionStatus
	CreatedAt          time.Time
	ExpiresAt          time.Time
	InvalidatedAt      *time.Time
	InvalidationReason string
}

// IsActiveAt reports whether the session authorizes commands at now.
func (s Session) IsActiveAt(now time.Time) error {
	switch s.Status {
	case SessionStatusInvalidated:
		return ErrSessionInvalid
	case SessionStatusExpired:
		return ErrSessionExpired
	case SessionStatusActive:
		if !s.ExpiresAt.IsZero() && !now.Before(s.ExpiresAt) {
			return ErrSessionExpired
		}
		return nil
	default:
		return ErrSessionInvalid
	}
}
