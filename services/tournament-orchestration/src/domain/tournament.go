package domain

import (
	"sort"
	"strconv"
	"strings"
)

// Tournament is the aggregate consistency boundary for lifecycle, registration,
// seeding, provisioning, result recording, and completion.
// Pure stdlib; not goroutine-safe — callers serialize access.
type Tournament struct {
	id          TournamentID
	phase       TournamentPhase
	capacity    int
	retryBudget int
	batchSize   int

	// registrations maps playerId -> registered. Sparse; never pre-allocates capacity slots.
	registrations map[PlayerID]struct{}
	// registrationOrder preserves deterministic first-seen order for seeding.
	registrationOrder []PlayerID

	rounds       map[int]*Round
	currentRound int
	champion     PlayerID

	outcomes        map[CommandID]CommandOutcome
	processedEvents map[EventID]CommandOutcome
	// resultKeys tracks (roomId, completionVersion) for idempotency/conflict.
	resultKeys map[string]resultRecord
	// roomOwners maps roomId -> "round:slot" for assignment uniqueness.
	roomOwners map[RoomID]string
}

type resultRecord struct {
	Disposition ResultDisposition
	Fingerprint string
}

// CreateTournament starts a tournament in registration phase.
func CreateTournament(cmd CreateTournamentCommand) (*Tournament, CommandOutcome) {
	if !cmd.CommandID.Valid() || !cmd.TournamentID.Valid() {
		return nil, rejectedOutcome(cmd.CommandID, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "create requires commandId and tournamentId",
		})
	}
	if cmd.Capacity <= 0 {
		return nil, rejectedOutcome(cmd.CommandID, Rejection{
			Code:    RejectInvalidCommand,
			Message: "capacity must be positive",
		})
	}
	retry := cmd.RetryBudget
	if retry <= 0 {
		retry = DefaultRetryBudget
	}
	batch := cmd.BatchSize
	if batch <= 0 {
		batch = DefaultBatchSize
	}
	t := &Tournament{
		id:              cmd.TournamentID,
		phase:           PhaseRegistration,
		capacity:        cmd.Capacity,
		retryBudget:     retry,
		batchSize:       batch,
		registrations:   map[PlayerID]struct{}{},
		rounds:          map[int]*Round{},
		outcomes:        map[CommandID]CommandOutcome{},
		processedEvents: map[EventID]CommandOutcome{},
		resultKeys:      map[string]resultRecord{},
		roomOwners:      map[RoomID]string{},
	}
	out := acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentCreated, map[string]string{
			"tournamentId": string(cmd.TournamentID),
			"capacity":     strconv.Itoa(cmd.Capacity),
			"phase":        string(PhaseRegistration),
		}),
	})
	t.outcomes[cmd.CommandID] = out
	return t, out
}

func (t *Tournament) ID() TournamentID       { return t.id }
func (t *Tournament) Phase() TournamentPhase { return t.phase }
func (t *Tournament) Capacity() int          { return t.capacity }
func (t *Tournament) RegisteredCount() int   { return len(t.registrations) }
func (t *Tournament) Champion() PlayerID     { return t.champion }
func (t *Tournament) CurrentRound() int      { return t.currentRound }
func (t *Tournament) RetryBudget() int       { return t.retryBudget }
func (t *Tournament) BatchSize() int         { return t.batchSize }

func (t *Tournament) Round(n int) (*Round, bool) {
	r, ok := t.rounds[n]
	return r, ok
}

func (t *Tournament) IsRegistered(playerID PlayerID) bool {
	_, ok := t.registrations[playerID]
	return ok
}

func (t *Tournament) recall(commandID CommandID) (CommandOutcome, bool) {
	if !commandID.Valid() {
		return CommandOutcome{}, false
	}
	out, ok := t.outcomes[commandID]
	if !ok {
		return CommandOutcome{}, false
	}
	return duplicateOutcome(out), true
}

func (t *Tournament) store(out CommandOutcome) CommandOutcome {
	if out.CommandID.Valid() {
		t.outcomes[out.CommandID] = out
	}
	return out
}

func (t *Tournament) reject(commandID CommandID, code RejectionCode, msg string) CommandOutcome {
	return t.store(rejectedOutcome(commandID, Rejection{Code: code, Message: msg}))
}

func (t *Tournament) rememberEvent(eventID EventID, out CommandOutcome) CommandOutcome {
	if eventID.Valid() {
		t.processedEvents[eventID] = out
	}
	return t.store(out)
}

// RegisterPlayer registers a unique player while in registration phase.
// Idempotent by (tournamentId, playerId): already-registered returns accepted with no facts.
func (t *Tournament) RegisterPlayer(cmd RegisterPlayerCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "register requires commandId and playerId")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	if t.phase != PhaseRegistration {
		return t.reject(cmd.CommandID, RejectWrongPhase, "registration is closed")
	}
	if _, exists := t.registrations[cmd.PlayerID]; exists {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if len(t.registrations) >= t.capacity {
		return t.reject(cmd.CommandID, RejectCapacityExceeded, "tournament at capacity")
	}
	t.registrations[cmd.PlayerID] = struct{}{}
	t.registrationOrder = append(t.registrationOrder, cmd.PlayerID)
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactPlayerRegisteredInTournament, map[string]string{
			"tournamentId": string(t.id),
			"playerId":     string(cmd.PlayerID),
		}),
	}))
}

// CloseRegistration moves registration -> seeding. Duplicate close is idempotent no-op.
func (t *Tournament) CloseRegistration(cmd CloseRegistrationCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "close requires commandId")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	if t.phase != PhaseRegistration {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if len(t.registrations) == 0 {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "cannot close with zero registrations")
	}
	t.phase = PhaseSeeding
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentRegistrationClosed, map[string]string{
			"tournamentId":    string(t.id),
			"registeredCount": strconv.Itoa(len(t.registrations)),
			"phase":           string(PhaseSeeding),
		}),
	}))
}

// SeedRound deterministically seeds round slots from remaining/registered players.
// Idempotent by (tournamentId, roundNumber).
func (t *Tournament) SeedRound(cmd SeedRoundCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "seed requires commandId and roundNumber>=1")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	if existing, ok := t.rounds[cmd.RoundNumber]; ok && existing.Status != RoundPending {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if t.phase != PhaseSeeding && t.phase != PhaseInProgress {
		return t.reject(cmd.CommandID, RejectWrongPhase, "seeding not allowed in current phase")
	}
	if cmd.RoundNumber > 1 {
		prev, ok := t.rounds[cmd.RoundNumber-1]
		if !ok || !prev.Completed {
			return t.reject(cmd.CommandID, RejectRoundNotReady, "previous round not completed")
		}
	} else if t.phase != PhaseSeeding {
		return t.reject(cmd.CommandID, RejectWrongPhase, "round 1 requires seeding phase")
	}

	players := t.playersForRound(cmd.RoundNumber)
	if len(players) == 0 {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "no players to seed")
	}
	sort.Slice(players, func(i, j int) bool {
		return string(players[i]) < string(players[j])
	})

	isFinal := len(players) <= FinalPlayerThreshold
	var slots []BracketSlot
	if isFinal {
		seeded := make([]PlayerID, len(players))
		copy(seeded, players)
		slots = []BracketSlot{{
			SlotID:        slotIDForIndex(0),
			Index:         0,
			Status:        SlotPending,
			SeededPlayers: seeded,
		}}
	} else {
		slots = buildSlots(players)
	}

	round := &Round{
		Number:  cmd.RoundNumber,
		Status:  RoundSeeded,
		IsFinal: isFinal,
		Slots:   slots,
	}
	t.rounds[cmd.RoundNumber] = round
	t.currentRound = cmd.RoundNumber
	if t.phase == PhaseSeeding {
		t.phase = PhaseInProgress
	}
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentRoundSeeded, map[string]string{
			"tournamentId": string(t.id),
			"roundNumber":  strconv.Itoa(cmd.RoundNumber),
			"slotCount":    strconv.Itoa(len(slots)),
			"playerCount":  strconv.Itoa(len(players)),
			"isFinal":      strconv.FormatBool(isFinal),
		}),
	}))
}

func (t *Tournament) playersForRound(roundNumber int) []PlayerID {
	if roundNumber == 1 {
		out := make([]PlayerID, len(t.registrationOrder))
		copy(out, t.registrationOrder)
		return out
	}
	prev, ok := t.rounds[roundNumber-1]
	if !ok {
		return nil
	}
	return prev.advancingPlayers()
}

// ProvisionRoundMatches creates deterministic provisioning batches for a seeded round.
// Emits TournamentMatchAssigned with deterministic room ids per slot (idempotent by slot).
func (t *Tournament) ProvisionRoundMatches(cmd ProvisionRoundMatchesCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "provision requires commandId and roundNumber")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	if round.anyBatchQuarantined() {
		return t.reject(cmd.CommandID, RejectQuarantined, "round has quarantined provisioning batch")
	}
	if round.Status == RoundProvisioning || round.Status == RoundInProgress || round.Status == RoundCompleted {
		if len(round.Batches) > 0 {
			return t.store(acceptedOutcome(cmd.CommandID, nil))
		}
	}
	if round.Status != RoundSeeded && round.Status != RoundPending {
		return t.reject(cmd.CommandID, RejectRoundNotReady, "round not ready to provision")
	}

	round.Batches = buildBatches(len(round.Slots), t.batchSize)
	round.Status = RoundProvisioning

	facts := make([]Fact, 0, len(round.Slots)+1)
	for i := range round.Slots {
		slot := &round.Slots[i]
		if slot.RoomID.Valid() {
			continue
		}
		roomID := roomIDForSlot(t.id, cmd.RoundNumber, slot.SlotID)
		batchID := batchContaining(round.Batches, slot.Index)
		if err := t.assignRoomLocked(round, slot, roomID, batchID); err != "" {
			return t.reject(cmd.CommandID, RejectConflictingAssignment, err)
		}
		facts = append(facts, newFact(FactTournamentMatchAssigned, map[string]string{
			"tournamentId": string(t.id),
			"roundNumber":  strconv.Itoa(cmd.RoundNumber),
			"slotId":       string(slot.SlotID),
			"roomId":       string(roomID),
			"batchId":      string(batchID),
		}))
	}
	// Batches stay pending for sharded workers; Room calls are not part of this command.
	for i := range round.Batches {
		round.Batches[i].Status = BatchPending
	}
	return t.store(acceptedOutcome(cmd.CommandID, facts))
}

// CompleteTournamentProvisioningBatch marks a worker-finished batch complete.
// When every batch is complete, the round becomes in_progress.
func (t *Tournament) CompleteTournamentProvisioningBatch(cmd CompleteTournamentProvisioningBatchCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.BatchID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "complete batch requires identities")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	batch, ok := round.findBatch(cmd.BatchID)
	if !ok {
		return t.reject(cmd.CommandID, RejectBatchNotFound, "batch not found")
	}
	if batch.Status == BatchQuarantined {
		return t.reject(cmd.CommandID, RejectQuarantined, "batch is quarantined")
	}
	if batch.Status == BatchCompleted {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	batch.Status = BatchCompleted
	batch.LastError = ""
	allDone := true
	for i := range round.Batches {
		if round.Batches[i].Status != BatchCompleted {
			allDone = false
			break
		}
	}
	if allDone {
		round.Status = RoundInProgress
	}
	return t.store(acceptedOutcome(cmd.CommandID, nil))
}

func batchContaining(batches []ProvisioningBatch, slotIndex int) BatchID {
	for _, b := range batches {
		if b.coversSlotIndex(slotIndex) {
			return b.BatchID
		}
	}
	return ""
}

func (t *Tournament) assignRoomLocked(round *Round, slot *BracketSlot, roomID RoomID, batchID BatchID) string {
	ownerKey := strconv.Itoa(round.Number) + ":" + string(slot.SlotID)
	if existing, ok := t.roomOwners[roomID]; ok && existing != ownerKey {
		return "room already assigned to another slot"
	}
	if slot.RoomID.Valid() && slot.RoomID != roomID {
		return "slot already has a different room"
	}
	slot.RoomID = roomID
	slot.BatchID = batchID
	slot.Status = SlotAssigned
	t.roomOwners[roomID] = ownerKey
	return ""
}

// AssignRoom records an external worker room assignment idempotent by (tournamentId, roundNumber, slotId).
func (t *Tournament) AssignRoom(cmd AssignRoomCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.SlotID.Valid() || !cmd.RoomID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "assign requires identities")
	}
	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	slot, ok := round.findSlot(cmd.SlotID)
	if !ok {
		return t.reject(cmd.CommandID, RejectSlotNotFound, "slot not found")
	}
	if slot.RoomID.Valid() {
		if slot.RoomID == cmd.RoomID {
			return t.store(acceptedOutcome(cmd.CommandID, nil))
		}
		return t.reject(cmd.CommandID, RejectConflictingAssignment, "slot already assigned to different room")
	}
	ownerKey := strconv.Itoa(cmd.RoundNumber) + ":" + string(cmd.SlotID)
	if existing, ok := t.roomOwners[cmd.RoomID]; ok && existing != ownerKey {
		return t.reject(cmd.CommandID, RejectConflictingAssignment, "room already assigned to another slot")
	}
	batchID := cmd.BatchID
	if !batchID.Valid() {
		batchID = slot.BatchID
	}
	slot.RoomID = cmd.RoomID
	slot.BatchID = batchID
	slot.Status = SlotAssigned
	t.roomOwners[cmd.RoomID] = ownerKey
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentMatchAssigned, map[string]string{
			"tournamentId": string(t.id),
			"roundNumber":  strconv.Itoa(cmd.RoundNumber),
			"slotId":       string(cmd.SlotID),
			"roomId":       string(cmd.RoomID),
			"batchId":      string(batchID),
		}),
	}))
}

// RecordMatchResult consumes async MatchCompleted facts.
// Validated against assigned room; idempotent by eventId and (roomId, completionVersion);
// conflicts quarantine without overwriting advancement.
func (t *Tournament) RecordMatchResult(cmd RecordMatchResultCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.RoomID.Valid() || !cmd.SlotID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "record result requires identities")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	if cmd.EventID.Valid() {
		if prior, seen := t.processedEvents[cmd.EventID]; seen {
			dup := duplicateOutcome(prior)
			dup.CommandID = cmd.CommandID
			return t.store(dup)
		}
	}

	ranked, err := RankStandings(cmd.Standings)
	if err != nil {
		return t.reject(cmd.CommandID, RejectInvalidCommand, err.Error())
	}
	fp := fingerprintFromRanked(ranked)
	key := resultKey(cmd.RoomID, cmd.CompletionVersion)
	if prev, ok := t.resultKeys[key]; ok {
		if prev.Disposition == DispositionQuarantined {
			out := acceptedOutcome(cmd.CommandID, nil)
			return t.rememberEvent(cmd.EventID, out)
		}
		if prev.Fingerprint == fp {
			t.resultKeys[key] = resultRecord{Disposition: DispositionDuplicateIgnored, Fingerprint: fp}
			out := acceptedOutcome(cmd.CommandID, nil)
			return t.rememberEvent(cmd.EventID, out)
		}
		return t.quarantineResult(cmd, fp, "conflicting result for same room and completionVersion")
	}

	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	slot, ok := round.findSlot(cmd.SlotID)
	if !ok {
		return t.reject(cmd.CommandID, RejectSlotNotFound, "slot not found")
	}

	if !slot.RoomID.Valid() || slot.RoomID != cmd.RoomID {
		return t.quarantineResult(cmd, fp, "room does not match assigned slot")
	}

	if slot.HasResult {
		if slot.CompletionVersion == cmd.CompletionVersion && slot.ResultFingerprint == fp {
			t.resultKeys[key] = resultRecord{Disposition: DispositionDuplicateIgnored, Fingerprint: fp}
			out := acceptedOutcome(cmd.CommandID, nil)
			return t.rememberEvent(cmd.EventID, out)
		}
		return t.quarantineResult(cmd, fp, "conflicting completion for slot with existing result")
	}

	// Abandoned matches cannot advance the normal bracket; quarantine for operator handling.
	if cmd.IsAbandoned {
		return t.quarantineResult(cmd, fp, "abandoned match cannot advance bracket")
	}

	var advancing []PlayerID
	var status SlotStatus
	var advanceRule string
	if round.IsFinal {
		advancing = make([]PlayerID, len(ranked))
		for i := range ranked {
			advancing[i] = ranked[i].PlayerID
		}
		status = SlotResultRecorded
		advanceRule = "final_ranked_result"
	} else {
		advancing, err = TopThree(cmd.Standings)
		if err != nil {
			return t.reject(cmd.CommandID, RejectInvalidCommand, err.Error())
		}
		status = SlotAdvanced
		advanceRule = "match_wins_card_points_completion_time"
	}

	standings := make([]PlayerMatchStanding, len(cmd.Standings))
	copy(standings, cmd.Standings)

	slot.Standings = standings
	slot.CompletionVersion = cmd.CompletionVersion
	slot.ResultFingerprint = fp
	slot.HasResult = true
	slot.Advancing = advancing
	slot.Status = status
	if round.IsFinal {
		if champ, ok := ChampionFromStandings(standings); ok {
			t.champion = champ
		}
	}

	facts := []Fact{
		newFact(FactTournamentMatchResultRecorded, map[string]string{
			"tournamentId":      string(t.id),
			"roundNumber":       strconv.Itoa(cmd.RoundNumber),
			"slotId":            string(cmd.SlotID),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
		}),
	}
	advanceData := map[string]string{
		"tournamentId":       string(t.id),
		"roundNumber":        strconv.Itoa(cmd.RoundNumber),
		"sourceSlotId":       string(cmd.SlotID),
		"advancingPlayerIds": joinPlayerIDs(advancing),
		"rule":               advanceRule,
	}
	if round.IsFinal {
		advanceData["isFinal"] = "true"
	}
	facts = append(facts, newFact(FactPlayersAdvanced, advanceData))

	t.resultKeys[key] = resultRecord{Disposition: DispositionRecorded, Fingerprint: fp}
	out := acceptedOutcome(cmd.CommandID, facts)
	return t.rememberEvent(cmd.EventID, out)
}

func fingerprintFromRanked(ranked []PlayerMatchStanding) string {
	parts := make([]string, len(ranked))
	for i, s := range ranked {
		parts[i] = string(s.PlayerID) + ":" + strconv.Itoa(s.MatchWins) + ":" +
			strconv.Itoa(s.CumulativeCardPoints) + ":" + strconv.FormatInt(s.FinalGameCompletedAt.UTC().UnixNano(), 10)
	}
	return strings.Join(parts, "|")
}

func standingsFingerprint(standings []PlayerMatchStanding) (string, error) {
	ranked, err := RankStandings(standings)
	if err != nil {
		return "", err
	}
	return fingerprintFromRanked(ranked), nil
}

func (t *Tournament) quarantineResult(cmd RecordMatchResultCommand, fp, reason string) CommandOutcome {
	key := resultKey(cmd.RoomID, cmd.CompletionVersion)
	if fp == "" {
		if computed, err := standingsFingerprint(cmd.Standings); err == nil {
			fp = computed
		}
	}
	t.resultKeys[key] = resultRecord{Disposition: DispositionQuarantined, Fingerprint: fp}
	if round, ok := t.rounds[cmd.RoundNumber]; ok {
		if slot, ok := round.findSlot(cmd.SlotID); ok {
			// Do not overwrite an already-accepted advancement decision.
			if !slot.HasResult {
				slot.Status = SlotQuarantined
				slot.QuarantineReason = reason
				round.Status = RoundBlocked
			} else {
				slot.QuarantineReason = reason
			}
		}
	}
	out := acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentResultQuarantined, map[string]string{
			"tournamentId":      string(t.id),
			"roundNumber":       strconv.Itoa(cmd.RoundNumber),
			"slotId":            string(cmd.SlotID),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
			"reason":            reason,
		}),
	})
	return t.rememberEvent(cmd.EventID, out)
}

func resultKey(roomID RoomID, ver CompletionVersion) string {
	return string(roomID) + ":" + strconv.FormatUint(uint64(ver), 10)
}

// CompleteRound marks a round complete when all assigned matches are terminal and advancement filled.
// Idempotent by round status. Does not auto-seed; policy issues SeedRound for next tier.
func (t *Tournament) CompleteRound(cmd CompleteRoundCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "complete round requires commandId and roundNumber")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	if round.Completed || round.Status == RoundCompleted {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if round.anyBatchQuarantined() {
		return t.reject(cmd.CommandID, RejectQuarantined, "cannot complete round with quarantined batch")
	}
	for i := range round.Slots {
		if round.Slots[i].Status == SlotQuarantined {
			return t.reject(cmd.CommandID, RejectQuarantined, "cannot complete round with quarantined slot")
		}
	}
	if !round.allSlotsTerminalAndAdvanced() {
		return t.reject(cmd.CommandID, RejectRoundIncomplete, "not all matches terminal with advancement filled")
	}
	round.Status = RoundCompleted
	round.Completed = true
	remaining := len(round.advancingPlayers())
	if round.IsFinal {
		remaining = len(round.Slots[0].Advancing)
	}
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentRoundCompleted, map[string]string{
			"tournamentId":     string(t.id),
			"roundNumber":      strconv.Itoa(cmd.RoundNumber),
			"remainingPlayers": strconv.Itoa(remaining),
			"isFinal":          strconv.FormatBool(round.IsFinal),
		}),
	}))
}

// CompleteTournament completes when the final room has an authoritative ranked result and champion.
func (t *Tournament) CompleteTournament(cmd CompleteTournamentCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "complete tournament requires commandId")
	}
	if t.phase == PhaseCompleted {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if t.phase == PhaseCancelled {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament cancelled")
	}
	round, ok := t.rounds[t.currentRound]
	if !ok || !round.IsFinal {
		return t.reject(cmd.CommandID, RejectNotFinal, "current round is not the final")
	}
	if !round.Completed {
		return t.reject(cmd.CommandID, RejectRoundIncomplete, "final round not completed")
	}
	if !t.champion.Valid() {
		return t.reject(cmd.CommandID, RejectRoundIncomplete, "champion not determined")
	}
	t.phase = PhaseCompleted
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentCompleted, map[string]string{
			"tournamentId": string(t.id),
			"championId":   string(t.champion),
			"phase":        string(PhaseCompleted),
		}),
	}))
}

// CancelTournament moves to cancelled from non-terminal phases.
func (t *Tournament) CancelTournament(cmd CancelTournamentCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return t.reject(cmd.CommandID, RejectInvalidCommand, "cancel requires commandId")
	}
	if t.phase == PhaseCancelled {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if t.phase == PhaseCompleted {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "completed tournament cannot cancel")
	}
	t.phase = PhaseCancelled
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentCancelled, map[string]string{
			"tournamentId": string(t.id),
			"phase":        string(PhaseCancelled),
		}),
	}))
}

// RetryTournamentProvisioningBatch records retry saga state.
// Idempotent by (tournamentId, roundNumber, batchId, retryAttempt).
// Exhausting retry budget quarantines the batch.
func (t *Tournament) RetryTournamentProvisioningBatch(cmd RetryTournamentProvisioningBatchCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.BatchID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "retry requires identities")
	}
	if t.phase.IsTerminal() {
		return t.reject(cmd.CommandID, RejectAlreadyTerminal, "tournament is terminal")
	}
	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	batch, ok := round.findBatch(cmd.BatchID)
	if !ok {
		return t.reject(cmd.CommandID, RejectBatchNotFound, "batch not found")
	}
	if batch.Status == BatchQuarantined {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	if cmd.RetryAttempt > 0 && batch.RetryAttempt == cmd.RetryAttempt && batch.Status == BatchRetried {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	nextAttempt := batch.RetryAttempt + 1
	if cmd.RetryAttempt > 0 {
		nextAttempt = cmd.RetryAttempt
	}
	if nextAttempt > t.retryBudget {
		batch.Status = BatchQuarantined
		batch.QuarantineReason = "retry budget exhausted"
		round.Status = RoundBlocked
		return t.store(acceptedOutcome(cmd.CommandID, []Fact{
			newFact(FactTournamentProvisioningBatchQuarantined, map[string]string{
				"tournamentId": string(t.id),
				"roundNumber":  strconv.Itoa(cmd.RoundNumber),
				"batchId":      string(cmd.BatchID),
				"reason":       "retry budget exhausted",
				"retryAttempt": strconv.Itoa(nextAttempt),
			}),
		}))
	}
	batch.RetryAttempt = nextAttempt
	batch.Status = BatchRetried
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentProvisioningBatchRetried, map[string]string{
			"tournamentId": string(t.id),
			"roundNumber":  strconv.Itoa(cmd.RoundNumber),
			"batchId":      string(cmd.BatchID),
			"retryAttempt": strconv.Itoa(nextAttempt),
		}),
	}))
}

// QuarantineTournamentProvisioningBatch marks a batch for operator review.
// Idempotent by (tournamentId, roundNumber, batchId).
func (t *Tournament) QuarantineTournamentProvisioningBatch(cmd QuarantineTournamentProvisioningBatchCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.BatchID.Valid() || cmd.RoundNumber < 1 {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "quarantine batch requires identities")
	}
	round, ok := t.rounds[cmd.RoundNumber]
	if !ok {
		return t.reject(cmd.CommandID, RejectRoundNotFound, "round not found")
	}
	batch, ok := round.findBatch(cmd.BatchID)
	if !ok {
		return t.reject(cmd.CommandID, RejectBatchNotFound, "batch not found")
	}
	if batch.Status == BatchQuarantined {
		return t.store(acceptedOutcome(cmd.CommandID, nil))
	}
	reason := cmd.Reason
	if reason == "" {
		reason = "operator quarantine"
	}
	batch.Status = BatchQuarantined
	batch.QuarantineReason = reason
	round.Status = RoundBlocked
	return t.store(acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentProvisioningBatchQuarantined, map[string]string{
			"tournamentId": string(t.id),
			"roundNumber":  strconv.Itoa(cmd.RoundNumber),
			"batchId":      string(cmd.BatchID),
			"reason":       reason,
		}),
	}))
}

// QuarantineTournamentResult records an explicit result quarantine.
// Idempotent by (roomId, completionVersion).
func (t *Tournament) QuarantineTournamentResult(cmd QuarantineTournamentResultCommand) CommandOutcome {
	if out, ok := t.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.RoomID.Valid() {
		return t.reject(cmd.CommandID, RejectInvalidIdentity, "quarantine result requires identities")
	}
	key := resultKey(cmd.RoomID, cmd.CompletionVersion)
	fp := ""
	if prev, ok := t.resultKeys[key]; ok {
		if prev.Disposition == DispositionQuarantined {
			return t.store(acceptedOutcome(cmd.CommandID, nil))
		}
		fp = prev.Fingerprint
	}
	reason := cmd.Reason
	if reason == "" {
		reason = "explicit quarantine"
	}
	t.resultKeys[key] = resultRecord{Disposition: DispositionQuarantined, Fingerprint: fp}
	out := acceptedOutcome(cmd.CommandID, []Fact{
		newFact(FactTournamentResultQuarantined, map[string]string{
			"tournamentId":      string(t.id),
			"roomId":            string(cmd.RoomID),
			"completionVersion": strconv.FormatUint(uint64(cmd.CompletionVersion), 10),
			"reason":            reason,
		}),
	})
	return t.store(out)
}
