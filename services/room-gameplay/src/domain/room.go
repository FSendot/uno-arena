package domain

import (
	"strconv"
	"time"
)

// Room is the aggregate consistency boundary for one match space.
// It is pure and not goroutine-safe; callers serialize access. Concurrent
// conflicting commands are resolved by sequence-number contract: exactly one
// command matching the current sequence may be accepted when applied in order.
type Room struct {
	id           RoomID
	roomType     RoomType
	tournamentID TournamentID
	roundNumber  int
	slotID       string
	status       RoomStatus
	visibility   Visibility
	hostID       PlayerID
	roster       Roster
	sequence     SequenceNumber

	outcomes map[CommandID]CommandOutcome

	disconnects       map[PlayerID]DisconnectState
	nextDisconnectVer map[PlayerID]DisconnectVersion

	unoWindow UnoWindow
	hasUno    bool

	gameCompletedInMatch bool // individual game ended; room/match may still be in_progress
}

// CreateRoom starts an ad-hoc waiting room with the host seated at the lowest seat.
func CreateRoom(cmd CreateRoomCommand) (*Room, CommandOutcome) {
	if !cmd.CommandID.Valid() || !cmd.RoomID.Valid() || !cmd.HostID.Valid() {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "create room requires commandId, roomId, and hostId",
		})
	}
	vis := cmd.Visibility
	if vis == "" {
		vis = VisibilityPublic
	}
	maxSeats, ok := resolveMaxSeats(cmd.MaxSeats)
	if !ok {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectInvalidMaxSeats,
			Message: "maxSeats must be between 2 and 10 inclusive (zero/nonpositive uses default 10)",
		})
	}

	r := &Room{
		id:                cmd.RoomID,
		roomType:          RoomTypeAdHoc,
		status:            RoomStatusWaiting,
		visibility:        vis,
		hostID:            cmd.HostID,
		roster:            newRoster(maxSeats),
		sequence:          0,
		outcomes:          map[CommandID]CommandOutcome{},
		disconnects:       map[PlayerID]DisconnectState{},
		nextDisconnectVer: map[PlayerID]DisconnectVersion{},
	}
	if _, ok := r.roster.assignLowestEmpty(cmd.HostID); !ok {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectRoomFull,
			Message: "cannot seat host",
		})
	}
	r.sequence = 1
	out := acceptedOutcome(cmd.CommandID, r.sequence, []Fact{
		newFact(FactRoomCreated, map[string]string{
			"roomId":       string(cmd.RoomID),
			"hostPlayerId": string(cmd.HostID),
			"status":       string(RoomStatusWaiting),
			"roomType":     string(RoomTypeAdHoc),
			"visibility":   string(vis),
		}),
		newFact(FactPlayerJoinedRoom, map[string]string{
			"roomId":   string(cmd.RoomID),
			"playerId": string(cmd.HostID),
			"seat":     "0",
		}),
	})
	r.outcomes[cmd.CommandID] = out
	return r, out
}

// ProvisionTournamentRoom creates a tournament-provisioned waiting room identity.
func ProvisionTournamentRoom(cmd ProvisionTournamentRoomCommand) (*Room, CommandOutcome) {
	if !cmd.CommandID.Valid() || !cmd.RoomID.Valid() || !cmd.TournamentID.Valid() {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectTournamentIdentity,
			Message: "tournament room requires commandId, roomId, and tournamentId",
		})
	}
	if !cmd.HostID.Valid() {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectInvalidIdentity,
			Message: "tournament room requires an initial seated player/host placeholder",
		})
	}
	vis := cmd.Visibility
	if vis == "" {
		vis = VisibilityPrivate
	}
	maxSeats, ok := resolveMaxSeats(cmd.MaxSeats)
	if !ok {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectInvalidMaxSeats,
			Message: "maxSeats must be between 2 and 10 inclusive (zero/nonpositive uses default 10)",
		})
	}

	r := &Room{
		id:                cmd.RoomID,
		roomType:          RoomTypeTournament,
		tournamentID:      cmd.TournamentID,
		roundNumber:       cmd.RoundNumber,
		slotID:            cmd.SlotID,
		status:            RoomStatusWaiting,
		visibility:        vis,
		hostID:            cmd.HostID,
		roster:            newRoster(maxSeats),
		sequence:          0,
		outcomes:          map[CommandID]CommandOutcome{},
		disconnects:       map[PlayerID]DisconnectState{},
		nextDisconnectVer: map[PlayerID]DisconnectVersion{},
	}
	if _, ok := r.roster.assignLowestEmpty(cmd.HostID); !ok {
		return nil, rejectedOutcome(cmd.CommandID, 0, Rejection{
			Code:    RejectRoomFull,
			Message: "cannot seat initial player",
		})
	}
	r.sequence = 1
	out := acceptedOutcome(cmd.CommandID, r.sequence, []Fact{
		newFact(FactRoomCreated, map[string]string{
			"roomId":       string(cmd.RoomID),
			"hostPlayerId": string(cmd.HostID),
			"status":       string(RoomStatusWaiting),
			"roomType":     string(RoomTypeTournament),
			"tournamentId": string(cmd.TournamentID),
			"visibility":   string(vis),
		}),
		newFact(FactPlayerJoinedRoom, map[string]string{
			"roomId":   string(cmd.RoomID),
			"playerId": string(cmd.HostID),
			"seat":     "0",
		}),
	})
	r.outcomes[cmd.CommandID] = out
	return r, out
}

func (r *Room) ID() RoomID                     { return r.id }
func (r *Room) Type() RoomType                 { return r.roomType }
func (r *Room) TournamentID() TournamentID     { return r.tournamentID }
func (r *Room) RoundNumber() int               { return r.roundNumber }
func (r *Room) SlotID() string                 { return r.slotID }
func (r *Room) Status() RoomStatus             { return r.status }
func (r *Room) Visibility() Visibility         { return r.visibility }
func (r *Room) HostID() PlayerID               { return r.hostID }
func (r *Room) Sequence() SequenceNumber       { return r.sequence }
func (r *Room) Roster() Roster                 { return r.roster }
func (r *Room) GameCompletedInMatch() bool     { return r.gameCompletedInMatch }
func (r *Room) HostHasGameplayAuthority() bool { return r.status.HostHasGameplayAuthority() }

// RememberOutcome caches a stable command outcome on the live aggregate without
// applying other mutations. Used to persist rejected outcomes for idempotent replay.
func (r *Room) RememberOutcome(out CommandOutcome) {
	if r == nil || !out.CommandID.Valid() {
		return
	}
	if r.outcomes == nil {
		r.outcomes = map[CommandID]CommandOutcome{}
	}
	if _, ok := r.outcomes[out.CommandID]; ok {
		return
	}
	r.outcomes[out.CommandID] = out
}

func (r *Room) UnoWindow() (UnoWindow, bool) {
	if !r.hasUno {
		return UnoWindow{}, false
	}
	return r.unoWindow, true
}

func (r *Room) DisconnectState(playerID PlayerID) (DisconnectState, bool) {
	d, ok := r.disconnects[playerID]
	return d, ok && d.Active
}

// SpectatorAdmission evaluates admission against current room/match status.
func (r *Room) SpectatorAdmission(auth SpectatorAuth) SpectatorAdmissionDecision {
	auth.IsPublicRoom = r.visibility == VisibilityPublic
	return EvaluateSpectatorAdmission(r.status, auth)
}

func (r *Room) recall(commandID CommandID) (CommandOutcome, bool) {
	if !commandID.Valid() {
		return CommandOutcome{}, false
	}
	out, ok := r.outcomes[commandID]
	if !ok {
		return CommandOutcome{}, false
	}
	return duplicateOutcome(out), true
}

// PriorOutcome returns a prior command outcome for idempotent retries (adapter use).
func (r *Room) PriorOutcome(commandID CommandID) (CommandOutcome, bool) {
	return r.recall(commandID)
}

func (r *Room) store(out CommandOutcome) CommandOutcome {
	r.outcomes[out.CommandID] = out
	return out
}

func (r *Room) reject(commandID CommandID, submitted SequenceNumber, code RejectionCode, msg string) CommandOutcome {
	return r.store(rejectedOutcome(commandID, r.sequence, Rejection{
		Code:              code,
		Message:           msg,
		SubmittedSequence: submitted,
		CurrentSequence:   r.sequence,
	}))
}

func (r *Room) requireSequence(commandID CommandID, expected SequenceNumber) (CommandOutcome, bool) {
	if expected < r.sequence {
		return r.reject(commandID, expected, RejectStaleSequence, "submitted sequence is stale"), false
	}
	if expected > r.sequence {
		return r.reject(commandID, expected, RejectFutureSequence, "submitted sequence is in the future"), false
	}
	return CommandOutcome{}, true
}

func (r *Room) advance() SequenceNumber {
	r.sequence++
	return r.sequence
}

// JoinRoom seats a player while waiting. Already-seated players are idempotent no-ops (no sequence bump).
func (r *Room) JoinRoom(cmd JoinRoomCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "join requires commandId and playerId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status.IsTerminal() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	if r.status != RoomStatusWaiting {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectJoinAfterLock, "no new players after lock")
	}
	if r.roster.IsSeated(cmd.PlayerID) {
		// Idempotent by (roomId, playerId) while already seated: accept without reapplying.
		return r.store(acceptedOutcome(cmd.CommandID, r.sequence, nil))
	}
	seat, ok := r.roster.assignLowestEmpty(cmd.PlayerID)
	if !ok {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectRoomFull, "roster is full")
	}
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactPlayerJoinedRoom, map[string]string{
			"roomId":   string(r.id),
			"playerId": string(cmd.PlayerID),
			"seat":     strconv.Itoa(int(seat.Index)),
		}),
	}))
}

// LeaveRoom removes a seated player. Pre-lock/start ad-hoc host leave reassigns
// to the lowest occupied seat or cancels immediately when empty. After lock/start,
// host reassignment is not performed and host labels have no gameplay authority.
func (r *Room) LeaveRoom(cmd LeaveRoomCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "leave requires commandId and playerId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status.IsTerminal() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	if !r.roster.IsSeated(cmd.PlayerID) {
		// Idempotent if already absent.
		return r.store(acceptedOutcome(cmd.CommandID, r.sequence, nil))
	}

	wasHost := r.hostID == cmd.PlayerID
	seat, _ := r.roster.clear(cmd.PlayerID)
	delete(r.disconnects, cmd.PlayerID)

	facts := []Fact{
		newFact(FactPlayerLeftRoom, map[string]string{
			"roomId":   string(r.id),
			"playerId": string(cmd.PlayerID),
			"seat":     strconv.Itoa(int(seat.Index)),
		}),
	}

	preLockStart := r.status == RoomStatusWaiting
	if wasHost && preLockStart && r.roomType == RoomTypeAdHoc {
		if next, ok := r.roster.LowestOccupiedSeat(); ok {
			r.hostID = next.PlayerID
			facts = append(facts, newFact(FactHostReassigned, map[string]string{
				"roomId":       string(r.id),
				"hostPlayerId": string(r.hostID),
				"seat":         strconv.Itoa(int(next.Index)),
			}))
		} else {
			r.status = RoomStatusCancelled
			r.hostID = ""
			facts = append(facts,
				newFact(FactRoomCancelled, map[string]string{
					"roomId": string(r.id),
					"status": string(RoomStatusCancelled),
				}),
				newFact(FactSpectatorStreamsClose, map[string]string{
					"roomId": string(r.id),
					"reason": "room_cancelled",
				}),
			)
		}
	}
	// After lock/start: no host reassignment and no gameplay authority transfer.

	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, facts))
}

// LockRoom transitions waiting -> locked. Ad-hoc requires host while host still has authority;
// system/tournament may lock with AsSystem.
func (r *Room) LockRoom(cmd LockRoomCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidCommand, "lock requires commandId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status == RoomStatusLocked || r.status == RoomStatusInProgress || r.status == RoomStatusCompleted {
		// Duplicate lock after success: idempotent no-op.
		return r.store(acceptedOutcome(cmd.CommandID, r.sequence, nil))
	}
	if r.status == RoomStatusCancelled {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room cancelled")
	}
	if r.status != RoomStatusWaiting {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotWaiting, "room must be waiting to lock")
	}
	if !cmd.AsSystem {
		if !r.HostHasGameplayAuthority() {
			return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectHostAuthorityEnded, "host has no gameplay authority")
		}
		if cmd.ActorID != r.hostID {
			return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotHost, "only host or system may lock")
		}
	}
	if r.roster.OccupiedCount() < MinPlayersToLock {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInsufficientRoster, "need at least two players to lock")
	}

	r.status = RoomStatusLocked
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactRoomLocked, map[string]string{
			"roomId": string(r.id),
			"status": string(RoomStatusLocked),
		}),
	}))
}

// StartMatch transitions locked -> in_progress. After lock, host authority for
// further reassignment is gone; Start still requires the recorded host or system
// while status is locked (host label unchanged after lock).
func (r *Room) StartMatch(cmd StartMatchCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.GameID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "start requires commandId and gameId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status == RoomStatusInProgress || r.status == RoomStatusCompleted {
		return r.store(acceptedOutcome(cmd.CommandID, r.sequence, nil))
	}
	if r.status.IsTerminal() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	if r.status != RoomStatusLocked {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotLocked, "room must be locked to start")
	}
	if !cmd.AsSystem {
		// Host reassignment has no gameplay authority after lock; the recorded host
		// may start only while still seated. Otherwise only system may start.
		if cmd.ActorID != r.hostID || !r.roster.IsSeated(cmd.ActorID) {
			return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotHost, "only seated host or system may start")
		}
	}

	r.status = RoomStatusInProgress
	r.gameCompletedInMatch = false
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactMatchStarted, map[string]string{
			"roomId": string(r.id),
			"status": string(RoomStatusInProgress),
		}),
		newFact(FactGameStarted, map[string]string{
			"roomId": string(r.id),
			"gameId": string(cmd.GameID),
		}),
	}))
}

// CancelRoom transitions waiting -> cancelled.
func (r *Room) CancelRoom(cmd CancelRoomCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidCommand, "cancel requires commandId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status == RoomStatusCancelled {
		return r.store(acceptedOutcome(cmd.CommandID, r.sequence, nil))
	}
	if r.status != RoomStatusWaiting {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotWaiting, "only waiting rooms may cancel")
	}
	if !cmd.AsSystem {
		if !r.HostHasGameplayAuthority() {
			return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectHostAuthorityEnded, "host has no gameplay authority")
		}
		if cmd.ActorID != r.hostID {
			return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotHost, "only host or system may cancel")
		}
	}

	r.status = RoomStatusCancelled
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactRoomCancelled, map[string]string{
			"roomId": string(r.id),
			"status": string(RoomStatusCancelled),
		}),
		newFact(FactSpectatorStreamsClose, map[string]string{
			"roomId": string(r.id),
			"reason": "room_cancelled",
		}),
	}))
}

// CompleteGame is guarded: Session/game.Match owns game completion.
func (r *Room) CompleteGame(cmd CompleteGameCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectMatchOwnsCompletion, "game completion owned by Session/Match")
}

// CompleteMatch is guarded: Session/game.Match owns match completion and must
// emit MatchCompleted. Direct room completion is not a second authority.
func (r *Room) CompleteMatch(cmd CompleteMatchCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectMatchOwnsCompletion, "match completion owned by Session/Match")
}

// DisconnectPlayer opens a 60s reconnect window keyed by disconnectVersion.
func (r *Room) DisconnectPlayer(cmd DisconnectPlayerCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "disconnect requires commandId and playerId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status.IsTerminal() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	if !r.roster.IsSeated(cmd.PlayerID) {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotSeated, "player not seated")
	}

	ver := r.nextDisconnectVer[cmd.PlayerID] + 1
	r.nextDisconnectVer[cmd.PlayerID] = ver
	state := BeginDisconnect(cmd.PlayerID, ver, cmd.NowUTC)
	r.disconnects[cmd.PlayerID] = state

	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactPlayerDisconnected, map[string]string{
			"roomId":            string(r.id),
			"playerId":          string(cmd.PlayerID),
			"disconnectVersion": strconv.FormatUint(uint64(ver), 10),
			"deadlineUTC":       state.DeadlineUTC.Format(time.RFC3339Nano),
		}),
	}))
}

// ReconnectPlayer restores participation within the absolute UTC deadline for the version.
func (r *Room) ReconnectPlayer(cmd ReconnectPlayerCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "reconnect requires commandId and playerId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status.IsTerminal() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	state, ok := r.disconnects[cmd.PlayerID]
	if !ok {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDisconnectInactive, "no active disconnect")
	}
	if okReconnect, code := state.CanReconnect(cmd.PlayerID, cmd.DisconnectVersion, cmd.NowUTC); !okReconnect {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, code, "reconnect denied")
	}
	r.disconnects[cmd.PlayerID] = state.Clear()
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactPlayerReconnected, map[string]string{
			"roomId":            string(r.id),
			"playerId":          string(cmd.PlayerID),
			"disconnectVersion": strconv.FormatUint(uint64(cmd.DisconnectVersion), 10),
		}),
	}))
}

// ForfeitPlayer is guarded: Session owns forfeit plus game turn-ring removal.
// Adapters must not call this directly during an active Session.
func (r *Room) ForfeitPlayer(cmd ForfeitPlayerCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectSessionOwnsForfeit, "forfeit owned by Session")
}

// applyForfeitPlayer applies forfeit once when the reconnect deadline is reached for the version.
// Removes the player from occupied roster participation, clears disconnect state, and does
// not reassign host after lock/start. Casual rooms continue in_progress without the player.
// Used by Session and pre-session/lifecycle tests; public ForfeitPlayer is guarded.
func (r *Room) applyForfeitPlayer(cmd ForfeitPlayerCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "forfeit requires commandId and playerId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status.IsTerminal() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	state, ok := r.disconnects[cmd.PlayerID]
	if !ok {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDisconnectInactive, "no active disconnect")
	}
	if okForfeit, code := state.CanForfeit(cmd.PlayerID, cmd.DisconnectVersion, cmd.NowUTC); !okForfeit {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, code, "forfeit denied")
	}
	r.disconnects[cmd.PlayerID] = state.Clear()
	delete(r.disconnects, cmd.PlayerID)
	if r.roster.IsSeated(cmd.PlayerID) {
		r.roster.clear(cmd.PlayerID)
	}
	// After lock/start: no host reassignment (forfeit occurs during match).

	factData := map[string]string{
		"roomId":            string(r.id),
		"playerId":          string(cmd.PlayerID),
		"disconnectVersion": strconv.FormatUint(uint64(cmd.DisconnectVersion), 10),
		"roomType":          string(r.roomType),
	}
	if r.roomType == RoomTypeTournament && r.tournamentID.Valid() {
		factData["tournamentId"] = string(r.tournamentID)
	}
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactPlayerForfeited, factData),
	}))
}

// OpenUnoWindow publishes absolute UTC expiresAt and opening sequence.
func (r *Room) OpenUnoWindow(cmd OpenUnoWindowCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() || !cmd.GameID.Valid() || !cmd.TriggeringGameEventID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "open uno window requires identities")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if r.status != RoomStatusInProgress {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotInProgress, "uno window requires in_progress room")
	}

	// Opening sequence is the sequence after this command commits.
	nextSeq := r.sequence + 1
	window := OpenUnoWindow(cmd.PlayerID, cmd.GameID, cmd.TriggeringGameEventID, cmd.NowUTC, nextSeq)
	r.unoWindow = window
	r.hasUno = true
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactUnoWindowOpened, map[string]string{
			"roomId":                string(r.id),
			"playerId":              string(cmd.PlayerID),
			"gameId":                string(cmd.GameID),
			"triggeringGameEventId": string(cmd.TriggeringGameEventID),
			"expiresAt":             window.ExpiresAt.Format(time.RFC3339Nano),
			"openingSequence":       strconv.FormatUint(uint64(window.OpeningSequence), 10),
		}),
	}))
}

// ExpireUnoWindow closes the window when the absolute UTC deadline is reached.
func (r *Room) ExpireUnoWindow(cmd ExpireUnoWindowCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidCommand, "expire uno requires commandId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if !r.hasUno {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "no open uno window")
	}
	expired, ok, code := r.unoWindow.Expire(cmd.NowUTC, cmd.GameID, cmd.PlayerID, cmd.TriggeringGameEventID, cmd.OpeningSequence)
	if !ok {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, code, "uno expiry denied")
	}
	r.unoWindow = expired
	r.hasUno = false
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactUnoWindowExpired, map[string]string{
			"roomId":                string(r.id),
			"playerId":              string(cmd.PlayerID),
			"gameId":                string(cmd.GameID),
			"triggeringGameEventId": string(cmd.TriggeringGameEventID),
			"openingSequence":       strconv.FormatUint(uint64(expired.OpeningSequence), 10),
		}),
	}))
}

// CloseUnoWindowByNextTurn closes an open Uno window because the next turn began.
func (r *Room) CloseUnoWindowByNextTurn(cmd CloseUnoWindowByNextTurnCommand) CommandOutcome {
	if out, ok := r.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidCommand, "close uno requires commandId")
	}
	if out, ok := r.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if !r.hasUno || !r.unoWindow.IsOpen() {
		return r.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "no open uno window")
	}
	r.unoWindow = r.unoWindow.CloseByNextTurn()
	r.hasUno = false
	seq := r.advance()
	return r.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactUnoWindowClosed, map[string]string{
			"roomId": string(r.id),
			"reason": "next_turn_began",
		}),
	}))
}

// FactNames extracts fact names for assertions.
func FactNames(facts []Fact) []FactName {
	out := make([]FactName, len(facts))
	for i, f := range facts {
		out[i] = f.Name
	}
	return out
}

// HasFact reports whether a fact name is present.
func HasFact(facts []Fact, name FactName) bool {
	for _, f := range facts {
		if f.Name == name {
			return true
		}
	}
	return false
}
