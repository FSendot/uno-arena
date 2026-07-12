package domain

import "strconv"

// ExplicitQuarantineReasonCode is the closed fixed reason for QuarantineTournamentResult.
// Raw operator text must never reach persistence, outcome facts, or logs.
const ExplicitQuarantineReasonCode = "explicit_quarantine"

// SanitizeExplicitQuarantineReason maps any operator/raw reason to the closed code.
// Empty and non-empty inputs both resolve to ExplicitQuarantineReasonCode.
func SanitizeExplicitQuarantineReason(raw string) string {
	_ = raw
	return ExplicitQuarantineReasonCode
}

// QuarantineTournamentResultContext is the bounded durable QuarantineTournamentResult input.
// Store loads tournament existence + at most one assigned room/slot + exact match_results
// and quarantine ledger row for (roomId, completionVersion) — never whole hydrate.
type QuarantineTournamentResultContext struct {
	TournamentID       TournamentID
	Exists             bool
	RoomID             RoomID
	CompletionVersion  CompletionVersion
	AssignmentResolved bool
	RoundNumber        int
	SlotID             SlotID
	// PriorDisposition is empty when no match_results row exists for the business key.
	PriorDisposition ResultDisposition
	LedgerExists     bool
}

// QuarantineTournamentResultKind classifies durable QuarantineTournamentResult apply.
type QuarantineTournamentResultKind string

const (
	QuarantineResultReject            QuarantineTournamentResultKind = "reject"
	QuarantineResultAlreadyDone       QuarantineTournamentResultKind = "already_done" // factless no-op
	QuarantineResultLedgerOnly        QuarantineTournamentResultKind = "ledger_only"  // recorded preserved or unknown room
	QuarantineResultInsertQuarantined QuarantineTournamentResultKind = "insert_quarantined"
)

// QuarantineTournamentResultDecision is pure policy before durable apply.
type QuarantineTournamentResultDecision struct {
	Kind             QuarantineTournamentResultKind
	Outcome          CommandOutcome
	ReasonCode       string
	AffectsSlot      bool
	WriteMatchResult bool
	PersistRound     int
	PersistSlot      SlotID
}

// DecideQuarantineTournamentResult evaluates QuarantineTournamentResult against a bounded context.
// Mirrors Tournament.QuarantineTournamentResult without whole hydrate.
// Idempotent by (roomId, completionVersion); completionVersion must be > 0.
func DecideQuarantineTournamentResult(ctx QuarantineTournamentResultContext, cmd QuarantineTournamentResultCommand) QuarantineTournamentResultDecision {
	if !cmd.CommandID.Valid() {
		return QuarantineTournamentResultDecision{
			Kind: QuarantineResultReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "quarantine result requires commandId",
			}),
		}
	}
	if !cmd.RoomID.Valid() {
		return QuarantineTournamentResultDecision{
			Kind: QuarantineResultReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "quarantine result requires roomId",
			}),
		}
	}
	if cmd.CompletionVersion == 0 {
		return QuarantineTournamentResultDecision{
			Kind: QuarantineResultReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "completionVersion must be > 0",
			}),
		}
	}
	if !ctx.Exists {
		return QuarantineTournamentResultDecision{
			Kind: QuarantineResultReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "tournament not found",
			}),
		}
	}

	reason := SanitizeExplicitQuarantineReason(cmd.Reason)

	// Already quarantined (ledger or disposition): accepted factless.
	if ctx.LedgerExists || ctx.PriorDisposition == DispositionQuarantined {
		return QuarantineTournamentResultDecision{
			Kind:       QuarantineResultAlreadyDone,
			Outcome:    acceptedOutcome(cmd.CommandID, nil),
			ReasonCode: reason,
		}
	}

	facts := []Fact{
		newFact(FactTournamentResultQuarantined, map[string]string{
			"tournamentId":      string(ctx.TournamentID),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
			"reason":            reason,
		}),
	}
	out := acceptedOutcome(cmd.CommandID, facts)

	if ctx.PriorDisposition == DispositionRecorded {
		// Preserve recorded result/advancement/slot/counters; ledger metadata only.
		d := QuarantineTournamentResultDecision{
			Kind:             QuarantineResultLedgerOnly,
			Outcome:          out,
			ReasonCode:       reason,
			AffectsSlot:      ctx.AssignmentResolved,
			WriteMatchResult: false,
		}
		if ctx.AssignmentResolved {
			d.PersistRound = ctx.RoundNumber
			d.PersistSlot = ctx.SlotID
		}
		return d
	}

	if !ctx.AssignmentResolved {
		// Unknown/unassigned room: ledger only, no fake round/slot claims.
		return QuarantineTournamentResultDecision{
			Kind:             QuarantineResultLedgerOnly,
			Outcome:          out,
			ReasonCode:       reason,
			AffectsSlot:      false,
			WriteMatchResult: false,
		}
	}

	// Assigned room, no exact result: insert disposition=quarantined match_results + ledger.
	return QuarantineTournamentResultDecision{
		Kind:             QuarantineResultInsertQuarantined,
		Outcome:          out,
		ReasonCode:       reason,
		AffectsSlot:      true,
		WriteMatchResult: true,
		PersistRound:     ctx.RoundNumber,
		PersistSlot:      ctx.SlotID,
	}
}
