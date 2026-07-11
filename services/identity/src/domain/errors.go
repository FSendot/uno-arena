package domain

import (
	"errors"
	"fmt"
)

var (
	ErrUsernameTaken      = errors.New("username taken")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrSessionNotFound    = errors.New("session not found")
	ErrSessionInvalid     = errors.New("session invalid")
	ErrSessionExpired     = errors.New("session expired")
	ErrPlayerNotFound     = errors.New("player not found")
	ErrInvalidInput       = errors.New("invalid input")
	// ErrUnavailable signals infrastructure failure (DB/query) for HTTP 503 mapping.
	ErrUnavailable = errors.New("service unavailable")
	// ErrExternalIdentityConflict is a lost race on unique (issuer, subject).
	ErrExternalIdentityConflict = errors.New("external identity conflict")
	// ErrNotAllowed is a capability/feature gate rejection.
	ErrNotAllowed = errors.New("operation not allowed")
)

// WrapUnavailable annotates an infra error so HTTP can map it to 503.
func WrapUnavailable(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrUnavailable) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

// MapRepoError preserves known domain errors and wraps unknown infra failures.
func MapRepoError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrUnavailable),
		errors.Is(err, ErrUsernameTaken),
		errors.Is(err, ErrInvalidCredentials),
		errors.Is(err, ErrSessionNotFound),
		errors.Is(err, ErrSessionInvalid),
		errors.Is(err, ErrSessionExpired),
		errors.Is(err, ErrPlayerNotFound),
		errors.Is(err, ErrInvalidInput),
		errors.Is(err, ErrExternalIdentityConflict),
		errors.Is(err, ErrNotAllowed):
		return err
	default:
		return WrapUnavailable(err)
	}
}
