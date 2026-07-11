package domain

import "time"

// Clock is injectable for deterministic tests.
type Clock interface {
	Now() time.Time
}

// SystemClock uses wall time.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator creates opaque identifiers.
type IDGenerator interface {
	NewID(prefix string) string
}

// TokenGenerator issues opaque session tokens.
type TokenGenerator interface {
	NewToken() (string, error)
}

// SaltGenerator supplies password salts (optional; PasswordHasher may embed it).
type SaltGenerator interface {
	NewSalt() ([]byte, error)
}
