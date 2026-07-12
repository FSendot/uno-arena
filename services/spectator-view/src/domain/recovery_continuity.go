package domain

import (
	"errors"
	"fmt"
)

// MaxHeldRecoveryEvents is the hard bound for post-gap held replay (ADR-0039).
const MaxHeldRecoveryEvents = 1000

var (
	// ErrHeldContinuityGap means held sequences are not contiguous after resumeCheckpoint.
	ErrHeldContinuityGap = errors.New("held recovery events have a sequence gap")
	// ErrHeldContinuityBound means held events exceed MaxHeldRecoveryEvents.
	ErrHeldContinuityBound = errors.New("held recovery events exceed hard bound")
	// ErrHeldContinuityInvalid means a held event is missing a usable sequence.
	ErrHeldContinuityInvalid = errors.New("held recovery event invalid")
	// ErrRecoverySnapshotInvalid means the bootstrap event is not a usable SnapshotSanitized.
	ErrRecoverySnapshotInvalid = errors.New("recovery snapshot event invalid")
	// ErrRecoveryApplyFailed means snapshot or held apply did not accept.
	ErrRecoveryApplyFailed = errors.New("recovery in-memory apply failed")
)

// ValidateHeldContinuity checks ascending room sequences with no gaps after resumeCheckpoint.
// Does not mutate Redis or projection state. Empty held is valid (snapshot-only continuity).
func ValidateHeldContinuity(resumeCheckpoint SequenceNumber, held []SpectatorSafeEvent) error {
	if len(held) > MaxHeldRecoveryEvents {
		return fmt.Errorf("%w: %d > %d", ErrHeldContinuityBound, len(held), MaxHeldRecoveryEvents)
	}
	expected := resumeCheckpoint + 1
	for i, evt := range held {
		if evt.Sequence < 1 {
			return fmt.Errorf("%w: index %d sequence %d", ErrHeldContinuityInvalid, i, evt.Sequence)
		}
		if evt.Sequence != expected {
			return fmt.Errorf("%w: index %d got seq %d want %d", ErrHeldContinuityGap, i, evt.Sequence, expected)
		}
		expected++
	}
	return nil
}

// RebuildFromRecoverySnapshot bootstraps from SnapshotSanitized at an arbitrary
// resumeCheckpoint, then applies held events. Live Apply rules are unchanged —
// this is a dedicated recovery entrypoint that never writes Redis.
//
// Continuity must be proven first; any non-accepted outcome fails closed without
// returning a mutated projection suitable for Redis commit.
func RebuildFromRecoverySnapshot(
	roomID RoomID,
	snapshot SpectatorSafeEvent,
	held []SpectatorSafeEvent,
) (*SpectatorRoomProjection, []ApplyOutcome, error) {
	if snapshot.EventType != EventSnapshotSanitized {
		return nil, nil, fmt.Errorf("%w: eventType=%s", ErrRecoverySnapshotInvalid, snapshot.EventType)
	}
	if snapshot.Sequence < 1 {
		return nil, nil, fmt.Errorf("%w: sequence must be >= 1", ErrRecoverySnapshotInvalid)
	}
	if snapshot.RoomID != "" && snapshot.RoomID != roomID {
		return nil, nil, fmt.Errorf("%w: room mismatch", ErrRecoverySnapshotInvalid)
	}
	snapshot.RoomID = roomID

	if err := ValidateHeldContinuity(snapshot.Sequence, held); err != nil {
		return nil, nil, err
	}

	p := NewSpectatorRoomProjection(roomID)
	outcomes := make([]ApplyOutcome, 0, 1+len(held))

	snapOut := p.bootstrapSanitizedSnapshot(snapshot)
	outcomes = append(outcomes, snapOut)
	if snapOut.Kind != OutcomeAccepted {
		return nil, outcomes, fmt.Errorf("%w: snapshot: %s", ErrRecoveryApplyFailed, snapOut.Kind)
	}

	for i, evt := range held {
		evt.RoomID = roomID
		out := p.Apply(evt)
		outcomes = append(outcomes, out)
		if out.Kind != OutcomeAccepted {
			return nil, outcomes, fmt.Errorf("%w: held[%d] eventId=%s kind=%s", ErrRecoveryApplyFailed, i, evt.EventID, out.Kind)
		}
	}
	return p, outcomes, nil
}

// bootstrapSanitizedSnapshot applies SnapshotSanitized onto an empty projection at
// any sequence >= 1. It reuses the same privacy/schema validation as Apply but
// skips the live "first event must be sequence 1" rule required for mid-stream recovery.
func (p *SpectatorRoomProjection) bootstrapSanitizedSnapshot(evt SpectatorSafeEvent) ApplyOutcome {
	if p.sequence != 0 {
		return p.quarantine(evt, Rejection{
			Code:    RejectOutOfOrderSequence,
			Message: "recovery bootstrap requires empty projection",
		})
	}
	if prior, ok := p.outcomes[evt.EventID]; ok {
		return duplicateOutcome(prior)
	}
	if rej := p.policy.ValidateEnvelope(evt); rej != nil {
		if rej.Code == RejectUnknownEventType {
			return p.drop(evt, *rej)
		}
		return p.quarantine(evt, *rej)
	}
	if evt.EventType != EventSnapshotSanitized {
		return p.quarantine(evt, Rejection{
			Code:    RejectInvalidSchema,
			Message: "recovery bootstrap requires SnapshotSanitized",
		})
	}
	if evt.RoomID != p.roomID {
		return p.quarantine(evt, Rejection{
			Code:    RejectRoomMismatch,
			Message: "event roomId does not match projection",
		})
	}
	if rej := p.policy.ValidatePayload(evt.Payload); rej != nil {
		return p.drop(evt, *rej)
	}

	p.applyPayload(evt)
	p.sequence = evt.Sequence
	facts := []Fact{
		newFact(FactSpectatorRoomProjectionUpdated, map[string]string{
			"eventId":  string(evt.EventID),
			"roomId":   string(p.roomID),
			"sequence": u64toa(uint64(p.sequence)),
			"status":   string(p.status),
		}),
	}
	out := ApplyOutcome{
		Kind:     OutcomeAccepted,
		EventID:  evt.EventID,
		Sequence: p.sequence,
		Facts:    facts,
	}
	p.outcomes[evt.EventID] = out
	return copyOutcome(out)
}
