package domain

// CompleteTournamentContext is the bounded durable CompleteTournament decision input.
// Store loads tournament lifecycle + current final round/slot advancement only —
// never a whole Tournament hydrate.
type CompleteTournamentContext struct {
	TournamentID TournamentID
	Exists       bool
	Phase        TournamentPhase
	CurrentRound int
	RoundFound   bool
	IsFinal      bool
	// RoundCompleted is true when the current round status is completed.
	RoundCompleted bool
	// FinalSlotCount is how many current-round bracket slots were observed (capped at 2).
	// Success requires exactly one final room: FinalSlotCount == 1 && FinalSlotIndex == 0.
	FinalSlotCount int
	// FinalSlotIndex is the slot_index of the single observed slot when FinalSlotCount == 1.
	FinalSlotIndex int
	// FinalStandings is the authoritative ordered advancement for the final slot (max 10),
	// loaded only from advancement_records joined to disposition=recorded match_results.
	FinalStandings []PlayerID
}

// CompleteTournamentKind classifies durable CompleteTournament apply.
type CompleteTournamentKind string

const (
	CompleteTournamentReject      CompleteTournamentKind = "reject"
	CompleteTournamentAlreadyDone CompleteTournamentKind = "already_done" // factless no-op
	CompleteTournamentSuccess     CompleteTournamentKind = "success"
)

// CompleteTournamentDecision is pure policy before durable CompleteTournament apply.
type CompleteTournamentDecision struct {
	Kind           CompleteTournamentKind
	Outcome        CommandOutcome
	FinalStandings []PlayerID
	ChampionID     PlayerID
}

// DecideCompleteTournament evaluates CompleteTournament against a bounded context.
// Mirrors Tournament.CompleteTournament aggregate semantics without whole hydrate.
func DecideCompleteTournament(ctx CompleteTournamentContext, cmd CompleteTournamentCommand) CompleteTournamentDecision {
	if !cmd.CommandID.Valid() {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "complete tournament requires commandId",
			}),
		}
	}
	if !ctx.Exists {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "tournament not found",
			}),
		}
	}
	if ctx.Phase == PhaseCompleted {
		return CompleteTournamentDecision{
			Kind:    CompleteTournamentAlreadyDone,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
		}
	}
	if ctx.Phase == PhaseCancelled {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament cancelled",
			}),
		}
	}
	if !ctx.RoundFound || !ctx.IsFinal {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectNotFinal,
				Message: "current round is not the final",
			}),
		}
	}
	if !ctx.RoundCompleted {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundIncomplete,
				Message: "final round not completed",
			}),
		}
	}
	// Invariant: one final room — exactly one current-round slot at slot_index 0.
	if ctx.FinalSlotCount != 1 || ctx.FinalSlotIndex != 0 {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundIncomplete,
				Message: "final room invariant requires exactly one slot at index 0",
			}),
		}
	}
	if len(ctx.FinalStandings) == 0 {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundIncomplete,
				Message: "final standings not determined",
			}),
		}
	}
	standings := append([]PlayerID(nil), ctx.FinalStandings...)
	if err := ValidateFinalStandings(standings); err != nil {
		return CompleteTournamentDecision{
			Kind: CompleteTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundIncomplete,
				Message: err.Error(),
			}),
		}
	}
	return CompleteTournamentDecision{
		Kind:           CompleteTournamentSuccess,
		FinalStandings: standings,
		ChampionID:     standings[0],
		Outcome: acceptedOutcome(cmd.CommandID, []Fact{
			newFact(FactTournamentCompleted, map[string]string{
				"tournamentId":   string(ctx.TournamentID),
				"finalStandings": joinPlayerIDs(standings),
				"phase":          string(PhaseCompleted),
			}),
		}),
	}
}

// CancelTournamentContext is the bounded durable CancelTournament decision input.
// Store loads only the tournament lifecycle row — O(1), no bracket/player scans.
type CancelTournamentContext struct {
	TournamentID TournamentID
	Exists       bool
	Phase        TournamentPhase
}

// CancelTournamentKind classifies durable CancelTournament apply.
type CancelTournamentKind string

const (
	CancelTournamentReject      CancelTournamentKind = "reject"
	CancelTournamentAlreadyDone CancelTournamentKind = "already_done" // factless no-op
	CancelTournamentSuccess     CancelTournamentKind = "success"
)

// CancelTournamentDecision is pure policy before durable CancelTournament apply.
type CancelTournamentDecision struct {
	Kind    CancelTournamentKind
	Outcome CommandOutcome
}

// DecideCancelTournament evaluates CancelTournament against a bounded context.
// Mirrors Tournament.CancelTournament aggregate semantics without whole hydrate.
func DecideCancelTournament(ctx CancelTournamentContext, cmd CancelTournamentCommand) CancelTournamentDecision {
	if !cmd.CommandID.Valid() {
		return CancelTournamentDecision{
			Kind: CancelTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "cancel requires commandId",
			}),
		}
	}
	if !ctx.Exists {
		return CancelTournamentDecision{
			Kind: CancelTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "tournament not found",
			}),
		}
	}
	if ctx.Phase == PhaseCancelled {
		return CancelTournamentDecision{
			Kind:    CancelTournamentAlreadyDone,
			Outcome: acceptedOutcome(cmd.CommandID, nil),
		}
	}
	if ctx.Phase == PhaseCompleted {
		return CancelTournamentDecision{
			Kind: CancelTournamentReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "completed tournament cannot cancel",
			}),
		}
	}
	return CancelTournamentDecision{
		Kind: CancelTournamentSuccess,
		Outcome: acceptedOutcome(cmd.CommandID, []Fact{
			newFact(FactTournamentCancelled, map[string]string{
				"tournamentId": string(ctx.TournamentID),
				"phase":        string(PhaseCancelled),
			}),
		}),
	}
}
