package domain

import "fmt"

// ProvisionKickoffContext is the bounded durable ProvisionRoundMatches decision input.
// Store loads counts/contiguity without hydrating slots or the whole tournament.
type ProvisionKickoffContext struct {
	TournamentID    TournamentID
	Exists          bool
	Phase           TournamentPhase
	RoundNumber     int
	RoundStatus     RoundStatus // empty when round row absent
	SlotCount       int
	SlotsContiguous bool // true when slots exist as contiguous 0..SlotCount-1
	BatchSize       int
	RetryBudget     int
	// ExistingBatchesFilled is true when provisioning_batches already exist for the round.
	ExistingBatchesFilled bool
	// ExistingBatchPlanFingerprint is the immutable plan fingerprint when batches exist.
	ExistingBatchPlanFingerprint string
}

// ProvisionBatchPlan is the deterministic immutable batch partition for kickoff.
type ProvisionBatchPlan struct {
	SlotCount  int
	BatchSize  int
	BatchCount int
}

// ProvisionKickoffKind classifies durable ProvisionRoundMatches kickoff apply.
type ProvisionKickoffKind string

const (
	ProvisionKickoffReject      ProvisionKickoffKind = "reject"
	ProvisionKickoffSchedule    ProvisionKickoffKind = "schedule"
	ProvisionKickoffAlreadyDone ProvisionKickoffKind = "already_done" // provisioning+/matching plan
)

// ProvisionKickoffDecision is pure policy before durable kickoff apply.
type ProvisionKickoffDecision struct {
	Kind    ProvisionKickoffKind
	Outcome CommandOutcome
	Plan    ProvisionBatchPlan
}

// ComputeProvisionBatchPlan returns the deterministic batch partition.
// batchSize must be in [1, MaxProvisioningBatchSize]; slotCount must be > 0.
func ComputeProvisionBatchPlan(slotCount, batchSize int) (ProvisionBatchPlan, error) {
	if slotCount <= 0 {
		return ProvisionBatchPlan{}, fmt.Errorf("slotCount must be > 0")
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if batchSize > MaxProvisioningBatchSize {
		return ProvisionBatchPlan{}, fmt.Errorf("batchSize exceeds max %d", MaxProvisioningBatchSize)
	}
	batchCount := (slotCount + batchSize - 1) / batchSize
	return ProvisionBatchPlan{
		SlotCount:  slotCount,
		BatchSize:  batchSize,
		BatchCount: batchCount,
	}, nil
}

// BatchRange returns inclusive slot index bounds for batch index i.
func (p ProvisionBatchPlan) BatchRange(i int) (from, to int) {
	if i < 0 || i >= p.BatchCount {
		return 0, -1
	}
	from = i * p.BatchSize
	to = from + p.BatchSize - 1
	if to >= p.SlotCount {
		to = p.SlotCount - 1
	}
	return from, to
}

// Fingerprint is a stable string for immutable plan comparison (size + ranges).
func (p ProvisionBatchPlan) Fingerprint() string {
	return fmt.Sprintf("slots=%d;batchSize=%d;batches=%d", p.SlotCount, p.BatchSize, p.BatchCount)
}

// MatchAssignedEventID is the deterministic outbox event identity for one slot assignment.
func MatchAssignedEventID(tournamentID TournamentID, roundNumber int, slotID SlotID) string {
	return fmt.Sprintf("%s:r%d:%s:TournamentMatchAssigned", tournamentID, roundNumber, slotID)
}

// RoomIDForSlot exposes deterministic room identity for differential prepare.
func RoomIDForSlot(tournamentID TournamentID, roundNumber int, slotID SlotID) RoomID {
	return roomIDForSlot(tournamentID, roundNumber, slotID)
}

// BatchIDForIndex exposes deterministic batch identity.
func BatchIDForIndex(index int) BatchID {
	return batchIDForIndex(index)
}

// DecideProvisionKickoff evaluates ProvisionRoundMatches for durable differential kickoff.
// Accepted schedule/noop outcomes are always factless; assignments happen in ProcessProvisioningBatch prepare.
func DecideProvisionKickoff(ctx ProvisionKickoffContext, cmd ProvisionRoundMatchesCommand) ProvisionKickoffDecision {
	if !cmd.CommandID.Valid() || cmd.RoundNumber < 1 {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "provision requires commandId and roundNumber>=1",
			}),
		}
	}
	if !ctx.Exists {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "tournament not found",
			}),
		}
	}
	if ctx.Phase.IsTerminal() {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament is terminal",
			}),
		}
	}
	if ctx.RoundStatus == "" {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundNotFound,
				Message: "round not found",
			}),
		}
	}
	if ctx.BatchSize > MaxProvisioningBatchSize || ctx.BatchSize < 0 {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "batchSize exceeds max provisioning batch size",
			}),
		}
	}
	if ctx.RetryBudget < 0 {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "retryBudget invalid",
			}),
		}
	}

	batchSize := ctx.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Already provisioning / in_progress / completed: accepted no-op only if immutable plan matches.
	switch ctx.RoundStatus {
	case RoundProvisioning, RoundInProgress, RoundCompleted:
		if !ctx.ExistingBatchesFilled || ctx.SlotCount <= 0 || !ctx.SlotsContiguous {
			return ProvisionKickoffDecision{
				Kind: ProvisionKickoffReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: "existing provisioning setup drift",
				}),
			}
		}
		plan, err := ComputeProvisionBatchPlan(ctx.SlotCount, batchSize)
		if err != nil {
			return ProvisionKickoffDecision{
				Kind: ProvisionKickoffReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: err.Error(),
				}),
			}
		}
		if ctx.ExistingBatchPlanFingerprint != "" && ctx.ExistingBatchPlanFingerprint != plan.Fingerprint() {
			return ProvisionKickoffDecision{
				Kind: ProvisionKickoffReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: "immutable batch plan mismatch",
				}),
			}
		}
		return ProvisionKickoffDecision{
			Kind:    ProvisionKickoffAlreadyDone,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
			Plan:    plan,
		}
	case RoundSeeded:
		// continue to schedule
	case RoundBlocked:
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectQuarantined,
				Message: "round is blocked",
			}),
		}
	default:
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundNotReady,
				Message: "round not ready to provision",
			}),
		}
	}

	if ctx.SlotCount <= 0 {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "no slots to provision",
			}),
		}
	}
	if !ctx.SlotsContiguous {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "slots must be contiguous 0..slotCount-1",
			}),
		}
	}
	plan, err := ComputeProvisionBatchPlan(ctx.SlotCount, batchSize)
	if err != nil {
		return ProvisionKickoffDecision{
			Kind: ProvisionKickoffReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: err.Error(),
			}),
		}
	}
	if ctx.ExistingBatchesFilled {
		if ctx.ExistingBatchPlanFingerprint != plan.Fingerprint() {
			return ProvisionKickoffDecision{
				Kind: ProvisionKickoffReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: "immutable batch plan mismatch",
				}),
			}
		}
		// Seeded with matching batches already present — treat as schedule no-op that still
		// transitions if needed; store commit is responsible for seeded→provisioning.
		return ProvisionKickoffDecision{
			Kind:    ProvisionKickoffAlreadyDone,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
			Plan:    plan,
		}
	}
	return ProvisionKickoffDecision{
		Kind:    ProvisionKickoffSchedule,
		Outcome: acceptedOutcome(cmd.CommandID, nil), // factless; assignments in prepare
		Plan:    plan,
	}
}
