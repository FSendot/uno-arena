package domain

import "strconv"

// CompleteRoundContext is the bounded durable CompleteRound decision input.
// Store loads O(64) shard sums + round/batch gates without hydrating the tournament.
type CompleteRoundContext struct {
	TournamentID TournamentID
	Exists       bool
	Phase        TournamentPhase
	RoundNumber  int
	RoundFound   bool
	RoundStatus  RoundStatus
	IsFinal      bool
	// O(64) shard sums under lock.
	AssignedCount    int
	ResolvedCount    int
	QuarantinedCount int
	AdvancingCount   int
	// QuarantinedBatches counts provisioning_batches in quarantined status for the round.
	QuarantinedBatches int
	// AdvancementRecordsPlayers is SUM(cardinality(advancing_player_ids)) for the round.
	// T9 one-time drift check against AdvancingCount (not a per-result scan).
	AdvancementRecordsPlayers int
	// NormalizedAdvancingPlayers is COUNT(*) from round_advancing_players for the round.
	// T4 exact source parity with AdvancingCount (and optionally array cardinality).
	NormalizedAdvancingPlayers int
}

// CompleteRoundKind classifies durable CompleteRound apply.
type CompleteRoundKind string

const (
	CompleteRoundReject      CompleteRoundKind = "reject"
	CompleteRoundAlreadyDone CompleteRoundKind = "already_done" // factless no-op
	CompleteRoundSuccess     CompleteRoundKind = "success"
)

// NextRoundSeedingPlan is the atomic CompleteRound → SeedRound(N+1) handoff for non-final rounds.
type NextRoundSeedingPlan struct {
	RoundNumber       int
	IsFinal           bool
	PlayerCount       int
	SlotCount         int
	BaseSize          int
	Remainder         int
	Source            string // always advancement
	SourceRoundNumber int    // completed round N
	JobCommandID      string // seed:{tid}:r{N+1}
}

// CompleteRoundDecision is pure policy before durable CompleteRound apply.
type CompleteRoundDecision struct {
	Kind             CompleteRoundKind
	Outcome          CommandOutcome
	RemainingPlayers int
	IsFinal          bool
	// NextRound is set on non-final success; nil for final rounds / reject / already-done.
	NextRound *NextRoundSeedingPlan
}

// DecideCompleteRound evaluates CompleteRound against a bounded context.
// Mirrors Tournament.CompleteRound aggregate semantics without whole hydrate.
// Non-final success includes NextRound plan so the same TX can schedule SeedRound(N+1).
func DecideCompleteRound(ctx CompleteRoundContext, cmd CompleteRoundCommand) CompleteRoundDecision {
	if !cmd.CommandID.Valid() || cmd.RoundNumber < 1 {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "complete round requires commandId and roundNumber",
			}),
		}
	}
	if !ctx.Exists {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "tournament not found",
			}),
		}
	}
	if ctx.Phase.IsTerminal() {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament is terminal",
			}),
		}
	}
	if !ctx.RoundFound {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundNotFound,
				Message: "round not found",
			}),
		}
	}
	if ctx.RoundStatus == RoundCompleted {
		return CompleteRoundDecision{
			Kind:    CompleteRoundAlreadyDone,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
			IsFinal: ctx.IsFinal,
		}
	}
	if ctx.QuarantinedBatches > 0 {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectQuarantined,
				Message: "cannot complete round with quarantined batch",
			}),
		}
	}
	if ctx.QuarantinedCount > 0 {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectQuarantined,
				Message: "cannot complete round with quarantined slot",
			}),
		}
	}
	if ctx.RoundStatus != RoundInProgress ||
		ctx.AssignedCount <= 0 ||
		ctx.ResolvedCount != ctx.AssignedCount ||
		ctx.AdvancingCount <= 0 {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundIncomplete,
				Message: "not all matches terminal with advancement filled",
			}),
		}
	}
	// One-time completion drift check: shard advancing_count vs advancement_records cardinality.
	if ctx.AdvancingCount != ctx.AdvancementRecordsPlayers {
		return CompleteRoundDecision{
			Kind: CompleteRoundReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundIncomplete,
				Message: "advancing_count drift vs advancement_records",
			}),
		}
	}
	// T4 exact source: normalized rows must match advancing_count (and array cardinality).
	if ctx.NormalizedAdvancingPlayers > 0 || ctx.AdvancingCount > 0 {
		if ctx.NormalizedAdvancingPlayers != ctx.AdvancingCount {
			return CompleteRoundDecision{
				Kind: CompleteRoundReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectRoundIncomplete,
					Message: "advancing_count drift vs round_advancing_players",
				}),
			}
		}
	}

	remaining := ctx.AdvancingCount
	d := CompleteRoundDecision{
		Kind:             CompleteRoundSuccess,
		RemainingPlayers: remaining,
		IsFinal:          ctx.IsFinal,
		Outcome: acceptedOutcome(cmd.CommandID, []Fact{
			newFact(FactTournamentRoundCompleted, map[string]string{
				"tournamentId":     string(ctx.TournamentID),
				"roundNumber":      strconv.Itoa(cmd.RoundNumber),
				"remainingPlayers": strconv.Itoa(remaining),
				"isFinal":          strconv.FormatBool(ctx.IsFinal),
			}),
		}),
	}
	if !ctx.IsFinal {
		plan, err := ComputeRoundSlotPlan(remaining)
		if err != nil {
			return CompleteRoundDecision{
				Kind: CompleteRoundReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: "cannot plan next round seeding",
				}),
			}
		}
		nextRN := cmd.RoundNumber + 1
		d.NextRound = &NextRoundSeedingPlan{
			RoundNumber:       nextRN,
			IsFinal:           plan.IsFinal,
			PlayerCount:       plan.PlayerCount,
			SlotCount:         plan.SlotCount,
			BaseSize:          plan.BaseSize,
			Remainder:         plan.Remainder,
			Source:            SeedingSourceAdvancement,
			SourceRoundNumber: cmd.RoundNumber,
			JobCommandID:      SeedRoundCommandID(ctx.TournamentID, nextRN),
		}
	}
	return d
}

// CompleteRoundCommandID is the deterministic worker/manual command identity.
func CompleteRoundCommandID(tournamentID TournamentID, roundNumber int) string {
	return "complete:" + string(tournamentID) + ":r" + strconv.Itoa(roundNumber)
}
