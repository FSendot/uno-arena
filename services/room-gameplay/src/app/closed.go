package app

import (
	"context"
	"errors"
)

// ErrDependencyClosed is returned by fail-closed adapters when a required
// upstream dependency was not configured.
var ErrDependencyClosed = errors.New("dependency_not_configured")

// ClosedSessionValidator rejects all session validation (Identity unwired).
type ClosedSessionValidator struct{}

func (ClosedSessionValidator) Validate(context.Context, string, string) error {
	return ErrDependencyClosed
}

// ClosedGameIntegrity rejects all appends (Game Integrity unwired).
type ClosedGameIntegrity struct{}

func (ClosedGameIntegrity) Append(context.Context, AppendRequest) (AppendResult, error) {
	return AppendResult{}, ErrDependencyClosed
}

func (ClosedGameIntegrity) Replay(context.Context, string, int64) (ReplayResult, error) {
	return ReplayResult{}, ErrDependencyClosed
}

// ClosedDealSource rejects all deal/draw material operations (GI deals unwired).
type ClosedDealSource struct{}

func (ClosedDealSource) ReserveDeal(context.Context, string, string, string, []string) (MaterialReservation, error) {
	return MaterialReservation{}, ErrDependencyClosed
}

func (ClosedDealSource) ReserveDraw(context.Context, string, string, string, int) (MaterialReservation, error) {
	return MaterialReservation{}, ErrDependencyClosed
}

func (ClosedDealSource) Confirm(context.Context, string) error { return ErrDependencyClosed }

func (ClosedDealSource) Cancel(context.Context, string) error { return ErrDependencyClosed }

func (ClosedDealSource) ConfirmAt(context.Context, string, string, string) error {
	return ErrDependencyClosed
}

func (ClosedDealSource) CancelAt(context.Context, string, string, string) error {
	return ErrDependencyClosed
}
