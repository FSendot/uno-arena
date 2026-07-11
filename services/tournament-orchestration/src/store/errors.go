package store

import "errors"

// ErrCapabilityOnly is returned for memory/capability-only outbox polling methods.
// Durable mode relies on Debezium CDC; the app must never poll or mark published_at.
var ErrCapabilityOnly = errors.New("outbox polling is capability-mode only")

// ErrUnavailable wraps database/infra failures for fail-closed readiness/HTTP mapping.
var ErrUnavailable = errors.New("tournament store unavailable")

func wrapUnavailable(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrUnavailable, err)
}
