package store

import "errors"

// ErrCapabilityOnly is returned for memory/capability-only outbox polling methods.
// Durable mode relies on Debezium CDC; the app must never poll or mark published_at.
var ErrCapabilityOnly = errors.New("outbox polling is capability-mode only")

// ErrUnavailable wraps database/infra failures for fail-closed readiness/HTTP mapping.
var ErrUnavailable = errors.New("ranking store unavailable")

// ErrTournamentPerformanceConflict is returned when a tournament performance business key
// is reused with a different payload fingerprint. It is terminal (DLQ); no player mutation.
var ErrTournamentPerformanceConflict = errors.New("tournament performance business key conflict")

func wrapUnavailable(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrUnavailable, err)
}

// TournamentPerformanceConflictError carries fingerprint mismatch details for DLQ classification.
type TournamentPerformanceConflictError struct {
	SourceTopic         string
	BusinessKey         string
	ExistingFingerprint string
	IncomingFingerprint string
}

func (e *TournamentPerformanceConflictError) Error() string {
	if e == nil {
		return ErrTournamentPerformanceConflict.Error()
	}
	return ErrTournamentPerformanceConflict.Error() + ": topic=" + e.SourceTopic + " key=" + e.BusinessKey
}

func (e *TournamentPerformanceConflictError) Unwrap() error {
	return ErrTournamentPerformanceConflict
}

// IsTournamentPerformanceConflict reports a typed business-key fingerprint conflict.
func IsTournamentPerformanceConflict(err error) bool {
	return errors.Is(err, ErrTournamentPerformanceConflict)
}
