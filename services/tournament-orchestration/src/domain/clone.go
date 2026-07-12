package domain

// Clone returns a deep copy of the tournament aggregate for staging before Commit.
func (t *Tournament) Clone() *Tournament {
	if t == nil {
		return nil
	}
	out := &Tournament{
		id:                t.id,
		phase:             t.phase,
		capacity:          t.capacity,
		retryBudget:       t.retryBudget,
		batchSize:         t.batchSize,
		visibility:        t.visibility,
		registrations:     clonePlayerSet(t.registrations),
		registrationOrder: append([]PlayerID(nil), t.registrationOrder...),
		rounds:            cloneRounds(t.rounds),
		currentRound:      t.currentRound,
		champion:          t.champion,
		outcomes:          cloneOutcomes(t.outcomes),
		processedEvents:   cloneOutcomesByEvent(t.processedEvents),
		resultKeys:        cloneResultKeys(t.resultKeys),
		roomOwners:        cloneRoomOwners(t.roomOwners),
	}
	return out
}

func clonePlayerSet(in map[PlayerID]struct{}) map[PlayerID]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[PlayerID]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func cloneRounds(in map[int]*Round) map[int]*Round {
	if in == nil {
		return nil
	}
	out := make(map[int]*Round, len(in))
	for n, r := range in {
		if r == nil {
			out[n] = nil
			continue
		}
		out[n] = cloneRound(r)
	}
	return out
}

func cloneRound(r *Round) *Round {
	out := &Round{
		Number:    r.Number,
		Status:    r.Status,
		IsFinal:   r.IsFinal,
		Completed: r.Completed,
		Slots:     make([]BracketSlot, len(r.Slots)),
		Batches:   make([]ProvisioningBatch, len(r.Batches)),
	}
	for i := range r.Slots {
		out.Slots[i] = cloneSlot(r.Slots[i])
	}
	for i := range r.Batches {
		out.Batches[i] = cloneBatch(r.Batches[i])
	}
	return out
}

func cloneSlot(s BracketSlot) BracketSlot {
	out := s
	out.SeededPlayers = append([]PlayerID(nil), s.SeededPlayers...)
	out.Advancing = append([]PlayerID(nil), s.Advancing...)
	if s.Standings != nil {
		out.Standings = make([]PlayerMatchStanding, len(s.Standings))
		copy(out.Standings, s.Standings)
	}
	return out
}

func cloneBatch(b ProvisioningBatch) ProvisioningBatch {
	out := b
	out.SlotIndexes = append([]int(nil), b.SlotIndexes...)
	return out
}

func cloneOutcomes(in map[CommandID]CommandOutcome) map[CommandID]CommandOutcome {
	if in == nil {
		return nil
	}
	out := make(map[CommandID]CommandOutcome, len(in))
	for k, v := range in {
		out[k] = cloneOutcome(v)
	}
	return out
}

func cloneOutcomesByEvent(in map[EventID]CommandOutcome) map[EventID]CommandOutcome {
	if in == nil {
		return nil
	}
	out := make(map[EventID]CommandOutcome, len(in))
	for k, v := range in {
		out[k] = cloneOutcome(v)
	}
	return out
}

func cloneOutcome(v CommandOutcome) CommandOutcome {
	cp := v
	if v.Rejection != nil {
		r := *v.Rejection
		cp.Rejection = &r
	}
	if v.Facts != nil {
		cp.Facts = make([]Fact, len(v.Facts))
		for i, f := range v.Facts {
			cp.Facts[i] = Fact{Name: f.Name, Data: copyStringMap(f.Data)}
		}
	}
	return cp
}

func cloneResultKeys(in map[string]resultRecord) map[string]resultRecord {
	if in == nil {
		return nil
	}
	out := make(map[string]resultRecord, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneRoomOwners(in map[RoomID]string) map[RoomID]string {
	if in == nil {
		return nil
	}
	out := make(map[RoomID]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
