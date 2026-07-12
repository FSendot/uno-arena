package store

import (
	"errors"

	"unoarena/shared/envelope"
)

// ErrCapabilityOnly is returned for memory/capability-only outbox polling methods.
// Durable mode relies on Debezium CDC; the app must never poll or mark published_at.
var ErrCapabilityOnly = errors.New("outbox polling is capability-mode only")

// ErrUnavailable wraps database/infra failures for fail-closed readiness/HTTP mapping.
var ErrUnavailable = errors.New("tournament store unavailable")

// ErrProvisioningFence is returned when prepare/heartbeat/finalize lose the lease fence
// (owner, lease_version, retry_attempt, or status). It MUST NOT insert command_idempotency:
// a later fenced claimant may reuse the same attempt command id.
var ErrProvisioningFence = errors.New("provisioning_fence")

// ErrImmutableLedgerDrift is returned when match_result_quarantines already holds a
// business-key row whose immutable identity (tournament_id) disagrees with the writer.
var ErrImmutableLedgerDrift = errors.New("immutable_ledger_drift")

// ErrPriorCommandOutcome marks that command_idempotency already holds a canonical outcome.
// Prefer AsPriorCommandOutcome to extract the body; never treat a bare nil Commit/Finalize
// as success of a locally constructed loser response.
var ErrPriorCommandOutcome = errors.New("prior command outcome")

// PriorCommandOutcome is returned by Commit/FinalizeRegister when a prior outcome exists.
// The unit of work is rolled back (never commits local mutations on late prior discovery).
type PriorCommandOutcome struct {
	Outcome envelope.Result
}

func (e *PriorCommandOutcome) Error() string {
	if e == nil {
		return ErrPriorCommandOutcome.Error()
	}
	return ErrPriorCommandOutcome.Error()
}

func (e *PriorCommandOutcome) Unwrap() error { return ErrPriorCommandOutcome }

// AsPriorCommandOutcome extracts a canonical prior outcome from err, if present.
func AsPriorCommandOutcome(err error) (envelope.Result, bool) {
	var prior *PriorCommandOutcome
	if errors.As(err, &prior) && prior != nil {
		return prior.Outcome, true
	}
	return envelope.Result{}, false
}

func wrapUnavailable(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrUnavailable, err)
}
