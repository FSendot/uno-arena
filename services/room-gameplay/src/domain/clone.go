package domain

// Clone returns a deep copy of the room aggregate for staging before commit.
func (r *Room) Clone() *Room {
	if r == nil {
		return nil
	}
	out := &Room{
		id:                   r.id,
		roomType:             r.roomType,
		tournamentID:         r.tournamentID,
		roundNumber:          r.roundNumber,
		slotID:               r.slotID,
		status:               r.status,
		visibility:           r.visibility,
		hostID:               r.hostID,
		roster:               cloneRoster(r.roster),
		sequence:             r.sequence,
		outcomes:             cloneRoomOutcomes(r.outcomes),
		disconnects:          cloneDisconnects(r.disconnects),
		nextDisconnectVer:    cloneDisconnectVersions(r.nextDisconnectVer),
		unoWindow:            r.unoWindow,
		hasUno:               r.hasUno,
		gameCompletedInMatch: r.gameCompletedInMatch,
	}
	return out
}

// Clone returns a deep copy of the session (room + game + match) for staging.
func (s *Session) Clone() *Session {
	if s == nil {
		return nil
	}
	out := &Session{
		room:        s.room.Clone(),
		game:        nil,
		match:       nil,
		gameID:      s.gameID,
		usedGameIDs: cloneGameIDSet(s.usedGameIDs),
		turnVersion: s.turnVersion,
		skipped:     cloneSkipKeys(s.skipped),
	}
	if s.game != nil {
		out.game = s.game.Clone()
	}
	if s.match != nil {
		out.match = s.match.Clone()
	}
	return out
}

func cloneRoster(in Roster) Roster {
	seats := make([]Seat, len(in.seats))
	copy(seats, in.seats)
	return Roster{seats: seats, capacity: in.capacity}
}

func cloneRoomOutcomes(in map[CommandID]CommandOutcome) map[CommandID]CommandOutcome {
	if in == nil {
		return nil
	}
	out := make(map[CommandID]CommandOutcome, len(in))
	for k, v := range in {
		cp := v
		if v.Rejection != nil {
			r := *v.Rejection
			cp.Rejection = &r
		}
		if v.Facts != nil {
			cp.Facts = make([]Fact, len(v.Facts))
			for i, f := range v.Facts {
				cp.Facts[i] = Fact{Name: f.Name, Data: copyData(f.Data)}
			}
		}
		out[k] = cp
	}
	return out
}

func cloneDisconnects(in map[PlayerID]DisconnectState) map[PlayerID]DisconnectState {
	if in == nil {
		return nil
	}
	out := make(map[PlayerID]DisconnectState, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneDisconnectVersions(in map[PlayerID]DisconnectVersion) map[PlayerID]DisconnectVersion {
	if in == nil {
		return nil
	}
	out := make(map[PlayerID]DisconnectVersion, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneGameIDSet(in map[GameID]struct{}) map[GameID]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[GameID]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func cloneSkipKeys(in map[skipKey]struct{}) map[skipKey]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[skipKey]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}
