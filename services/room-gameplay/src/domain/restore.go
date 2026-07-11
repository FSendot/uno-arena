package domain

import "time"

// RestoreRoomInput carries every Room field needed for exact durable round-trip.
// Gameplay rules are unchanged; this only reconstructs persisted aggregate state.
type RestoreRoomInput struct {
	ID                   RoomID
	RoomType             RoomType
	TournamentID         TournamentID
	RoundNumber          int
	SlotID               string
	Status               RoomStatus
	Visibility           Visibility
	HostID               PlayerID
	Roster               Roster
	Sequence             SequenceNumber
	Outcomes             map[CommandID]CommandOutcome
	Disconnects          map[PlayerID]DisconnectState
	NextDisconnectVer    map[PlayerID]DisconnectVersion
	UnoWindow            UnoWindow
	HasUno               bool
	GameCompletedInMatch bool
}

// RestoreRoom rebuilds a Room from durable storage without applying commands.
func RestoreRoom(in RestoreRoomInput) *Room {
	outcomes := in.Outcomes
	if outcomes == nil {
		outcomes = map[CommandID]CommandOutcome{}
	}
	disconnects := in.Disconnects
	if disconnects == nil {
		disconnects = map[PlayerID]DisconnectState{}
	}
	nextVer := in.NextDisconnectVer
	if nextVer == nil {
		nextVer = map[PlayerID]DisconnectVersion{}
	}
	return &Room{
		id:                   in.ID,
		roomType:             in.RoomType,
		tournamentID:         in.TournamentID,
		roundNumber:          in.RoundNumber,
		slotID:               in.SlotID,
		status:               in.Status,
		visibility:           in.Visibility,
		hostID:               in.HostID,
		roster:               in.Roster,
		sequence:             in.Sequence,
		outcomes:             cloneRoomOutcomes(outcomes),
		disconnects:          cloneDisconnects(disconnects),
		nextDisconnectVer:    cloneDisconnectVersions(nextVer),
		unoWindow:            in.UnoWindow,
		hasUno:               in.HasUno,
		gameCompletedInMatch: in.GameCompletedInMatch,
	}
}

// RestoreRoster rebuilds a Roster from ordered seats and capacity.
func RestoreRoster(capacity int, seats []Seat) Roster {
	if capacity < MinMaxSeats {
		capacity = DefaultMaxSeats
	}
	out := make([]Seat, capacity)
	for i := range out {
		out[i] = Seat{Index: SeatIndex(i)}
	}
	for _, s := range seats {
		idx := int(s.Index)
		if idx < 0 || idx >= capacity {
			continue
		}
		out[idx] = s
		out[idx].Index = SeatIndex(idx)
	}
	return Roster{seats: out, capacity: capacity}
}

// RestoreUnoWindow rebuilds a domain UnoWindow including unexported open flags.
func RestoreUnoWindow(
	playerID PlayerID,
	gameID GameID,
	triggering TriggeringGameEventID,
	expiresAt time.Time,
	openingSequence SequenceNumber,
	open bool,
	closedByNextTurn bool,
) UnoWindow {
	return UnoWindow{
		PlayerID:              playerID,
		GameID:                gameID,
		TriggeringGameEventID: triggering,
		ExpiresAt:             expiresAt.UTC(),
		OpeningSequence:       openingSequence,
		open:                  open,
		closedByNextTurn:      closedByNextTurn,
	}
}

// SkippedTurn is the durable form of Session skip-key idempotency.
type SkippedTurn struct {
	PlayerID    PlayerID
	TurnVersion SequenceNumber
}

// NextDisconnectVersions exposes the durable next-disconnect counters for persistence.
func (r *Room) NextDisconnectVersions() map[PlayerID]DisconnectVersion {
	return cloneDisconnectVersions(r.nextDisconnectVer)
}

// DisconnectsMap exposes active disconnect episodes for persistence.
func (r *Room) DisconnectsMap() map[PlayerID]DisconnectState {
	return cloneDisconnects(r.disconnects)
}

// OutcomesMap exposes room-scoped command outcomes for persistence.
func (r *Room) OutcomesMap() map[CommandID]CommandOutcome {
	return cloneRoomOutcomes(r.outcomes)
}
