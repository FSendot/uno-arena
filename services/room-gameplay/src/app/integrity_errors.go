package app

import "errors"

// ErrReconciliationPending means a pending intent already exists for this command+room.
// Callers must not Append again; return retryable unavailable and leave the intent untouched.
var ErrReconciliationPending = errors.New("reconciliation intent pending")

// ErrReconciliationDone means the intent is already done. Durable idempotency must have
// short-circuited earlier; reaching Begin again is fail-closed.
var ErrReconciliationDone = errors.New("reconciliation intent already done")

// ErrRetryableUnavailable marks a caller-visible retryable failure (e.g. pending intent).
var ErrRetryableUnavailable = errors.New("retryable unavailable")

// ErrIntegrityAppendDefinitive is a typed definitive no-write rejection from Game Integrity.
// Only explicit HTTP 4xx (and test doubles that wrap this) are definitive; transport,
// decode/read, context timeout, and 5xx must NOT wrap this sentinel.
var ErrIntegrityAppendDefinitive = errors.New("game integrity append definitive rejection")

// Reservation action values persisted in ReconciliationRepairBlob / DurableAcceptedCommit.
const (
	ReservationActionConfirm = "confirm"
	ReservationActionCancel  = "cancel"
)
