package domain

import "strconv"

// ProgressShardCount is the fixed shard fan-out for round progress counters.
// shard_id = slot_index % ProgressShardCount; never mutate a single hot round counter per event.
const ProgressShardCount = 64

// ProgressShardID maps a durable slot_index onto a fixed shard.
func ProgressShardID(slotIndex int) int {
	if slotIndex < 0 {
		slotIndex = -slotIndex
	}
	return slotIndex % ProgressShardCount
}

// RoundMatchPriorResult is the durable (roomId, completionVersion) row if present.
type RoundMatchPriorResult struct {
	Disposition   ResultDisposition
	Fingerprint   string
	SourceEventID string
}

// RoundMatchSlotState is the bounded slot view needed for MatchCompleted policy.
type RoundMatchSlotState struct {
	SlotID             SlotID
	RoomID             RoomID
	SlotIndex          int
	Status             SlotStatus
	HasResult          bool
	CompletionVersion  CompletionVersion
	ResultFingerprint  string
	Advancing          []PlayerID
	QuarantineReason   string
	AssignmentResolved bool // true when assigned_matches resolved this room to a slot
}

// RoundMatchContext is the bounded decision input (never a whole tournament hydrate).
type RoundMatchContext struct {
	TournamentID TournamentID
	Phase        TournamentPhase
	RoundNumber  int // resolved round when AssignmentResolved; else 0
	IsFinal      bool
	RoundStatus  RoundStatus
	Slot         RoundMatchSlotState
	PriorResult  *RoundMatchPriorResult // by (roomId, completionVersion)
	PriorEvent   *CommandOutcome        // same EventID already processed
	RoundFound   bool
	SlotFound    bool
}

// RoundMatchDecisionKind classifies the durable/memory apply path.
type RoundMatchDecisionKind string

const (
	RoundMatchReject               RoundMatchDecisionKind = "reject"
	RoundMatchDuplicateEvent       RoundMatchDecisionKind = "duplicate_event"
	RoundMatchExactDuplicate       RoundMatchDecisionKind = "exact_duplicate"
	RoundMatchRecord               RoundMatchDecisionKind = "record"
	RoundMatchQuarantineUnresolved RoundMatchDecisionKind = "quarantine_unresolved"
	RoundMatchQuarantineConflict   RoundMatchDecisionKind = "quarantine_conflict_after_recorded"
	// RoundMatchQuarantineHeld: slot already quarantined; accept without further mutation.
	RoundMatchQuarantineHeld RoundMatchDecisionKind = "quarantine_held_stable"
)

// RoundMatchDecision is pure policy output for one MatchCompleted / RecordMatchResult.
// Durable differential commit applies this without reimplementing ranking/top-three/final rules.
type RoundMatchDecision struct {
	Kind             RoundMatchDecisionKind
	Outcome          CommandOutcome
	Fingerprint      string
	Advancing        []PlayerID
	SlotStatus       SlotStatus
	Disposition      ResultDisposition // durable row disposition for NEW match_results writes only
	QuarantineReason string
	BlockRound       bool // true only when an unresolved affecting slot is quarantined
	// PreserveRecordedRow: conflict/exact-dup paths must not overwrite match_results=recorded.
	PreserveRecordedRow bool
	// IncrementQuarantined: bump shard quarantined_count once for unresolved affecting quarantine.
	IncrementQuarantined bool
	// IncrementResolved: bump shard resolved_count once for first successful record.
	IncrementResolved bool
	Standings         []PlayerMatchStanding
	// AffectsSlot: quarantine binds a trustworthy resolved slot (public projection / slot block).
	AffectsSlot bool
	// WriteMatchResult: insert match_results(disposition=quarantined) when FK ownership is valid.
	WriteMatchResult bool
	// PersistRound/PersistSlot: authoritative identity for differential writes (resolved when known).
	PersistRound int
	PersistSlot  SlotID
}

// DecideRecordMatchResult evaluates MatchCompleted policy against a bounded RoundMatchContext.
// cmd carries claimed identity; ctx carries resolved assignment. Callers must not overwrite
// nonempty claimed round/slot before invoking this function.
func DecideRecordMatchResult(ctx RoundMatchContext, cmd RecordMatchResultCommand) RoundMatchDecision {
	if !cmd.CommandID.Valid() || !cmd.RoomID.Valid() {
		return RoundMatchDecision{
			Kind: RoundMatchReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidIdentity,
				Message: "record result requires identities",
			}),
		}
	}
	if cmd.CompletionVersion == 0 {
		return RoundMatchDecision{
			Kind: RoundMatchReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: "completionVersion must be > 0",
			}),
		}
	}
	if ctx.Phase.IsTerminal() {
		return RoundMatchDecision{
			Kind: RoundMatchReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectAlreadyTerminal,
				Message: "tournament is terminal",
			}),
		}
	}
	if cmd.EventID.Valid() && ctx.PriorEvent != nil {
		dup := duplicateOutcome(*ctx.PriorEvent)
		dup.CommandID = cmd.CommandID
		return RoundMatchDecision{Kind: RoundMatchDuplicateEvent, Outcome: dup}
	}

	ranked, err := RankStandings(cmd.Standings)
	if err != nil {
		return RoundMatchDecision{
			Kind: RoundMatchReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectInvalidCommand,
				Message: err.Error(),
			}),
		}
	}
	fp := fingerprintFromRanked(ranked)

	if ctx.PriorResult != nil {
		prev := *ctx.PriorResult
		if prev.Disposition == DispositionQuarantined {
			out := acceptedOutcome(cmd.CommandID, nil)
			return RoundMatchDecision{Kind: RoundMatchExactDuplicate, Outcome: out, Fingerprint: fp, PreserveRecordedRow: true}
		}
		if prev.Fingerprint == fp {
			out := acceptedOutcome(cmd.CommandID, nil)
			return RoundMatchDecision{
				Kind:                RoundMatchExactDuplicate,
				Outcome:             out,
				Fingerprint:         fp,
				PreserveRecordedRow: true,
			}
		}
		// Conflict on same (room, version): quarantine ledger only; preserve recorded row.
		return quarantineConflictDecision(ctx, cmd, fp, "conflicting result for same room and completionVersion")
	}

	// Unknown room: no trustworthy resolved assignment. Quarantine ledger only —
	// never mutate another player's claimed slot or public projection.
	if !ctx.Slot.AssignmentResolved {
		if cmd.RoundNumber < 1 || !cmd.SlotID.Valid() {
			return RoundMatchDecision{
				Kind: RoundMatchReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidIdentity,
					Message: "record result requires identities",
				}),
			}
		}
		return quarantineUnresolvedDecision(ctx, cmd, fp, "room does not match assigned slot", false, false)
	}

	// Authoritative resolved identity for persistence.
	persistRound := ctx.RoundNumber
	persistSlot := ctx.Slot.SlotID

	// Claimed vs resolved identity comparison (fill-only happens outside; nonempty claims compared).
	if identityMismatch(ctx, cmd) {
		reason := "claimed round/slot does not match assigned room identity"
		if ctx.Slot.Status == SlotQuarantined {
			return quarantineHeldDecision(cmd, fp)
		}
		if ctx.Slot.HasResult {
			return quarantineConflictDecision(ctx, cmd, fp, reason)
		}
		return quarantineUnresolvedDecision(ctx, cmd, fp, reason, true, true)
	}

	if !ctx.RoundFound {
		return RoundMatchDecision{
			Kind: RoundMatchReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectRoundNotFound,
				Message: "round not found",
			}),
		}
	}
	if !ctx.SlotFound {
		return RoundMatchDecision{
			Kind: RoundMatchReject,
			Outcome: rejectedOutcome(cmd.CommandID, Rejection{
				Code:    RejectSlotNotFound,
				Message: "slot not found",
			}),
		}
	}

	slot := ctx.Slot
	if !slot.RoomID.Valid() || slot.RoomID != cmd.RoomID {
		return quarantineUnresolvedDecision(ctx, cmd, fp, "room does not match assigned slot", false, false)
	}

	// Stable held quarantine: later completions for this resolved slot are accepted no-ops.
	if slot.Status == SlotQuarantined {
		return quarantineHeldDecision(cmd, fp)
	}

	if slot.HasResult {
		if slot.CompletionVersion == cmd.CompletionVersion && slot.ResultFingerprint == fp {
			out := acceptedOutcome(cmd.CommandID, nil)
			return RoundMatchDecision{
				Kind:                RoundMatchExactDuplicate,
				Outcome:             out,
				Fingerprint:         fp,
				PreserveRecordedRow: true,
			}
		}
		return quarantineConflictDecision(ctx, cmd, fp, "conflicting completion for slot with existing result")
	}

	if cmd.IsAbandoned {
		return quarantineUnresolvedDecision(ctx, cmd, fp, "abandoned match cannot advance bracket", true, true)
	}

	var advancing []PlayerID
	var status SlotStatus
	if ctx.IsFinal {
		advancing = make([]PlayerID, len(ranked))
		for i := range ranked {
			advancing[i] = ranked[i].PlayerID
		}
		if err := ValidateFinalStandings(advancing); err != nil {
			return RoundMatchDecision{
				Kind: RoundMatchReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: err.Error(),
				}),
			}
		}
		status = SlotResultRecorded
	} else {
		advancing, err = TopThree(ranked)
		if err != nil {
			return RoundMatchDecision{
				Kind: RoundMatchReject,
				Outcome: rejectedOutcome(cmd.CommandID, Rejection{
					Code:    RejectInvalidCommand,
					Message: err.Error(),
				}),
			}
		}
		status = SlotAdvanced
	}

	// Persist Tournament-owned ranked order (not caller/producer input order).
	standings := make([]PlayerMatchStanding, len(ranked))
	copy(standings, ranked)

	facts := []Fact{
		newFact(FactTournamentMatchResultRecorded, map[string]string{
			"tournamentId":      string(ctx.TournamentID),
			"roundNumber":       strconv.Itoa(persistRound),
			"slotId":            string(persistSlot),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
		}),
	}
	if !ctx.IsFinal {
		facts = append(facts, newFact(FactPlayersAdvanced, map[string]string{
			"tournamentId":       string(ctx.TournamentID),
			"roundNumber":        strconv.Itoa(persistRound),
			"sourceSlotId":       string(persistSlot),
			"advancingPlayerIds": joinPlayerIDs(advancing),
			"rule":               "match_wins_card_points_completion_time",
		}))
	}

	return RoundMatchDecision{
		Kind:              RoundMatchRecord,
		Outcome:           acceptedOutcome(cmd.CommandID, facts),
		Fingerprint:       fp,
		Advancing:         advancing,
		SlotStatus:        status,
		Disposition:       DispositionRecorded,
		IncrementResolved: true,
		AffectsSlot:       true,
		WriteMatchResult:  true,
		PersistRound:      persistRound,
		PersistSlot:       persistSlot,
		Standings:         standings,
	}
}

func identityMismatch(ctx RoundMatchContext, cmd RecordMatchResultCommand) bool {
	if !ctx.Slot.AssignmentResolved {
		return false
	}
	if cmd.RoundNumber >= 1 && ctx.RoundNumber >= 1 && cmd.RoundNumber != ctx.RoundNumber {
		return true
	}
	if cmd.SlotID.Valid() && ctx.Slot.SlotID.Valid() && cmd.SlotID != ctx.Slot.SlotID {
		return true
	}
	return false
}

func quarantineHeldDecision(cmd RecordMatchResultCommand, fp string) RoundMatchDecision {
	out := acceptedOutcome(cmd.CommandID, nil)
	return RoundMatchDecision{
		Kind:                RoundMatchQuarantineHeld,
		Outcome:             out,
		Fingerprint:         fp,
		Disposition:         DispositionQuarantined,
		PreserveRecordedRow: true,
		AffectsSlot:         false,
		WriteMatchResult:    false,
	}
}

func quarantineConflictDecision(ctx RoundMatchContext, cmd RecordMatchResultCommand, fp, reason string) RoundMatchDecision {
	if fp == "" {
		if computed, err := standingsFingerprint(cmd.Standings); err == nil {
			fp = computed
		}
	}
	facts := []Fact{
		newFact(FactTournamentResultQuarantined, map[string]string{
			"tournamentId":      string(ctx.TournamentID),
			"roundNumber":       strconv.Itoa(cmd.RoundNumber),
			"slotId":            string(cmd.SlotID),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
			"reason":            reason,
		}),
	}
	persistRound := ctx.RoundNumber
	persistSlot := ctx.Slot.SlotID
	if persistRound < 1 {
		persistRound = cmd.RoundNumber
	}
	if !persistSlot.Valid() {
		persistSlot = cmd.SlotID
	}
	return RoundMatchDecision{
		Kind:                RoundMatchQuarantineConflict,
		Outcome:             acceptedOutcome(cmd.CommandID, facts),
		Fingerprint:         fp,
		QuarantineReason:    reason,
		Disposition:         DispositionQuarantined,
		PreserveRecordedRow: true,
		BlockRound:          false,
		AffectsSlot:         ctx.Slot.AssignmentResolved,
		WriteMatchResult:    false,
		PersistRound:        persistRound,
		PersistSlot:         persistSlot,
	}
}

func quarantineUnresolvedDecision(ctx RoundMatchContext, cmd RecordMatchResultCommand, fp, reason string, affectsSlot, writeMatchResult bool) RoundMatchDecision {
	if fp == "" {
		if computed, err := standingsFingerprint(cmd.Standings); err == nil {
			fp = computed
		}
	}
	facts := []Fact{
		newFact(FactTournamentResultQuarantined, map[string]string{
			"tournamentId":      string(ctx.TournamentID),
			"roundNumber":       strconv.Itoa(cmd.RoundNumber),
			"slotId":            string(cmd.SlotID),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
			"reason":            reason,
		}),
	}
	out := acceptedOutcome(cmd.CommandID, facts)
	persistRound := ctx.RoundNumber
	persistSlot := ctx.Slot.SlotID
	if affectsSlot {
		if persistRound < 1 {
			persistRound = cmd.RoundNumber
		}
		if !persistSlot.Valid() {
			persistSlot = cmd.SlotID
		}
	}
	return RoundMatchDecision{
		Kind:                 RoundMatchQuarantineUnresolved,
		Outcome:              out,
		Fingerprint:          fp,
		QuarantineReason:     reason,
		Disposition:          DispositionQuarantined,
		SlotStatus:           SlotQuarantined,
		BlockRound:           affectsSlot,
		IncrementQuarantined: affectsSlot,
		AffectsSlot:          affectsSlot,
		WriteMatchResult:     writeMatchResult && affectsSlot,
		PersistRound:         persistRound,
		PersistSlot:          persistSlot,
	}
}

// BuildRoundMatchContext projects tournament aggregate state into a bounded decision context.
func (t *Tournament) BuildRoundMatchContext(cmd RecordMatchResultCommand) RoundMatchContext {
	ctx := RoundMatchContext{
		TournamentID: t.id,
		Phase:        t.phase,
	}
	if cmd.EventID.Valid() {
		if prior, seen := t.processedEvents[cmd.EventID]; seen {
			cp := prior
			ctx.PriorEvent = &cp
		}
	}
	key := resultKey(cmd.RoomID, cmd.CompletionVersion)
	if prev, ok := t.resultKeys[key]; ok {
		ctx.PriorResult = &RoundMatchPriorResult{
			Disposition:   prev.Disposition,
			Fingerprint:   prev.Fingerprint,
			SourceEventID: prev.SourceEventID,
		}
	}

	// Resolve assignment by room first (authoritative), then fall back to claimed hints.
	if rn, sid, ok := t.AssignmentByRoomID(cmd.RoomID); ok {
		ctx.RoundNumber = rn
		round, rok := t.rounds[rn]
		if !rok {
			return ctx
		}
		ctx.RoundFound = true
		ctx.IsFinal = round.IsFinal
		ctx.RoundStatus = round.Status
		slot, sok := round.findSlot(sid)
		if !sok {
			return ctx
		}
		ctx.SlotFound = true
		ctx.Slot = projectSlotState(slot, true)
		return ctx
	}

	// Unknown room: keep claimed hints for quarantine metadata only; AssignmentResolved=false.
	ctx.RoundNumber = cmd.RoundNumber
	if cmd.RoundNumber >= 1 {
		if round, ok := t.rounds[cmd.RoundNumber]; ok {
			ctx.RoundFound = true
			ctx.IsFinal = round.IsFinal
			ctx.RoundStatus = round.Status
			if cmd.SlotID.Valid() {
				if slot, ok := round.findSlot(cmd.SlotID); ok {
					ctx.SlotFound = true
					// Claimed slot of another room must NOT look assignment-resolved for cmd.RoomID.
					ctx.Slot = projectSlotState(slot, false)
					ctx.Slot.RoomID = slot.RoomID // actual assigned room (may differ)
				}
			}
		}
	}
	return ctx
}

func projectSlotState(slot *BracketSlot, assignmentResolved bool) RoundMatchSlotState {
	adv := make([]PlayerID, len(slot.Advancing))
	copy(adv, slot.Advancing)
	return RoundMatchSlotState{
		SlotID:             slot.SlotID,
		RoomID:             slot.RoomID,
		SlotIndex:          slot.Index,
		Status:             slot.Status,
		HasResult:          slot.HasResult,
		CompletionVersion:  slot.CompletionVersion,
		ResultFingerprint:  slot.ResultFingerprint,
		Advancing:          adv,
		QuarantineReason:   slot.QuarantineReason,
		AssignmentResolved: assignmentResolved,
	}
}

// applyRoundMatchDecision mutates the aggregate to match DecideRecordMatchResult (memory path).
func (t *Tournament) applyRoundMatchDecision(cmd RecordMatchResultCommand, d RoundMatchDecision) CommandOutcome {
	switch d.Kind {
	case RoundMatchReject:
		return t.store(d.Outcome)
	case RoundMatchDuplicateEvent:
		return t.store(d.Outcome)
	case RoundMatchExactDuplicate, RoundMatchQuarantineHeld:
		key := resultKey(cmd.RoomID, cmd.CompletionVersion)
		src := ""
		if prev, ok := t.resultKeys[key]; ok {
			src = prev.SourceEventID
		}
		if cmd.EventID.Valid() {
			src = string(cmd.EventID)
		}
		fp := d.Fingerprint
		if prev, ok := t.resultKeys[key]; ok && prev.Fingerprint != "" {
			fp = prev.Fingerprint
		}
		disp := DispositionDuplicateIgnored
		if d.Kind == RoundMatchQuarantineHeld {
			disp = DispositionQuarantined
		}
		t.resultKeys[key] = resultRecord{Disposition: disp, Fingerprint: fp, SourceEventID: src}
		return t.rememberEvent(cmd.EventID, d.Outcome)
	case RoundMatchRecord:
		rn := d.PersistRound
		if rn < 1 {
			rn = cmd.RoundNumber
		}
		sid := d.PersistSlot
		if !sid.Valid() {
			sid = cmd.SlotID
		}
		round := t.rounds[rn]
		slot, _ := round.findSlot(sid)
		slot.Standings = d.Standings
		slot.CompletionVersion = cmd.CompletionVersion
		slot.ResultFingerprint = d.Fingerprint
		slot.HasResult = true
		slot.Advancing = append([]PlayerID(nil), d.Advancing...)
		slot.Status = d.SlotStatus
		if round.IsFinal && len(d.Advancing) > 0 {
			t.champion = d.Advancing[0]
		}
		t.resultKeys[resultKey(cmd.RoomID, cmd.CompletionVersion)] = resultRecord{
			Disposition: DispositionRecorded, Fingerprint: d.Fingerprint, SourceEventID: string(cmd.EventID),
		}
		return t.rememberEvent(cmd.EventID, d.Outcome)
	case RoundMatchQuarantineUnresolved, RoundMatchQuarantineConflict:
		key := resultKey(cmd.RoomID, cmd.CompletionVersion)
		t.resultKeys[key] = resultRecord{
			Disposition: DispositionQuarantined, Fingerprint: d.Fingerprint, SourceEventID: string(cmd.EventID),
		}
		if d.AffectsSlot {
			rn := d.PersistRound
			if rn < 1 {
				rn = cmd.RoundNumber
			}
			sid := d.PersistSlot
			if !sid.Valid() {
				sid = cmd.SlotID
			}
			if round, ok := t.rounds[rn]; ok {
				if slot, ok := round.findSlot(sid); ok {
					if d.Kind == RoundMatchQuarantineUnresolved && !slot.HasResult {
						slot.Status = SlotQuarantined
						slot.QuarantineReason = d.QuarantineReason
						if d.BlockRound {
							round.Status = RoundBlocked
						}
					} else if d.Kind == RoundMatchQuarantineConflict {
						slot.QuarantineReason = d.QuarantineReason
					}
				}
			}
		}
		return t.rememberEvent(cmd.EventID, d.Outcome)
	default:
		return t.reject(cmd.CommandID, RejectInvalidCommand, "unknown round match decision")
	}
}
