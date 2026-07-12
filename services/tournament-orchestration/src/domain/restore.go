package domain

// RestoreTournamentInput carries every Tournament field needed for exact durable round-trip.
// Command application rules are unchanged; this only reconstructs persisted aggregate state.
type RestoreTournamentInput struct {
	ID                TournamentID
	Phase             TournamentPhase
	Capacity          int
	RetryBudget       int
	BatchSize         int
	Visibility        TournamentVisibility // empty defaults to public
	Registrations     map[PlayerID]struct{}
	RegistrationOrder []PlayerID
	Rounds            map[int]*Round
	CurrentRound      int
	Champion          PlayerID
	Outcomes          map[CommandID]CommandOutcome
	ProcessedEvents   map[EventID]CommandOutcome
	ResultKeys        map[string]ResultRecord
	RoomOwners        map[RoomID]string
}

// ResultRecord is the durable (roomId, completionVersion) disposition fingerprint.
type ResultRecord struct {
	Disposition   ResultDisposition
	Fingerprint   string
	SourceEventID string
}

// RestoreTournament rebuilds a Tournament from durable storage without applying commands.
func RestoreTournament(in RestoreTournamentInput) *Tournament {
	regs := in.Registrations
	if regs == nil {
		regs = map[PlayerID]struct{}{}
	}
	rounds := in.Rounds
	if rounds == nil {
		rounds = map[int]*Round{}
	}
	outcomes := in.Outcomes
	if outcomes == nil {
		outcomes = map[CommandID]CommandOutcome{}
	}
	processed := in.ProcessedEvents
	if processed == nil {
		processed = map[EventID]CommandOutcome{}
	}
	resultKeys := make(map[string]resultRecord, len(in.ResultKeys))
	for k, v := range in.ResultKeys {
		resultKeys[k] = resultRecord{
			Disposition:   v.Disposition,
			Fingerprint:   v.Fingerprint,
			SourceEventID: v.SourceEventID,
		}
	}
	roomOwners := in.RoomOwners
	if roomOwners == nil {
		roomOwners = map[RoomID]string{}
	}
	retry := in.RetryBudget
	if retry <= 0 {
		retry = DefaultRetryBudget
	}
	batch := in.BatchSize
	if batch <= 0 {
		batch = DefaultBatchSize
	}
	vis := in.Visibility
	if vis == "" {
		vis = TournamentVisibilityPublic
	}
	return &Tournament{
		id:                in.ID,
		phase:             in.Phase,
		capacity:          in.Capacity,
		retryBudget:       retry,
		batchSize:         batch,
		visibility:        vis,
		registrations:     clonePlayerSet(regs),
		registrationOrder: append([]PlayerID(nil), in.RegistrationOrder...),
		rounds:            cloneRounds(rounds),
		currentRound:      in.CurrentRound,
		champion:          in.Champion,
		outcomes:          cloneOutcomes(outcomes),
		processedEvents:   cloneOutcomesByEvent(processed),
		resultKeys:        resultKeys,
		roomOwners:        cloneRoomOwners(roomOwners),
	}
}
