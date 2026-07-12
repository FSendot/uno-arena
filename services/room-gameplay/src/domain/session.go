package domain

import (
	"strconv"
	"time"

	"unoarena/services/room-gameplay/game"
)

// Session is the application/domain seam composing Room lifecycle with the
// authoritative game engine. Card legality stays in game; Session owns room
// sequence mapping, match orchestration, Uno room-sequence publication, and
// disconnect-turn policy.
type Session struct {
	room   *Room
	game   *game.Game
	match  *game.Match
	gameID GameID

	usedGameIDs map[GameID]struct{}

	// turnVersion is the room sequence at which the current acting turn began.
	turnVersion SequenceNumber
	// skipped records SkipDisconnectedTurn idempotency by (player, turnVersion).
	skipped map[skipKey]struct{}
}

type skipKey struct {
	player PlayerID
	turn   SequenceNumber
}

// OpenSession wraps an existing Room aggregate. Call StartMatch with deal
// material to bind the first game engine instance.
func OpenSession(room *Room) *Session {
	return &Session{
		room:        room,
		usedGameIDs: map[GameID]struct{}{},
		skipped:     map[skipKey]struct{}{},
	}
}

func (s *Session) Room() *Room                 { return s.room }
func (s *Session) Game() *game.Game            { return s.game }
func (s *Session) Match() *game.Match          { return s.match }
func (s *Session) GameID() GameID              { return s.gameID }
func (s *Session) TurnVersion() SequenceNumber { return s.turnVersion }

// PrevalidateStartMatch runs non-mutating StartMatch rejection checks without
// consuming deal material or advancing room/GI state.
func (s *Session) PrevalidateStartMatch(cmd StartMatchCommand) *Rejection {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		if out.Rejection != nil {
			return out.Rejection
		}
		return nil
	}
	return s.prevalidateStartMatchReadonly(cmd)
}

func (s *Session) prevalidateStartMatchReadonly(cmd StartMatchCommand) *Rejection {
	if !cmd.CommandID.Valid() || !cmd.GameID.Valid() {
		return &Rejection{Code: RejectInvalidIdentity, Message: "start requires commandId and gameId", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if rej := sequenceRejection(s.room.sequence, cmd.ExpectedSequence); rej != nil {
		return rej
	}
	if s.game != nil && !s.game.Completed() {
		return &Rejection{Code: RejectGameStillActive, Message: "active game already bound", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if s.room.status == RoomStatusInProgress || s.room.status == RoomStatusCompleted {
		return &Rejection{Code: RejectGameStillActive, Message: "match already started", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if _, used := s.usedGameIDs[cmd.GameID]; used {
		return &Rejection{Code: RejectGameIDReused, Message: "game id already used in match", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if s.room.status.IsTerminal() {
		return &Rejection{Code: RejectAlreadyTerminal, Message: "room is terminal", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if s.room.status != RoomStatusLocked {
		return &Rejection{Code: RejectNotLocked, Message: "room must be locked to start", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if !cmd.AsSystem {
		if cmd.ActorID != s.room.hostID || !s.room.roster.IsSeated(cmd.ActorID) {
			return &Rejection{Code: RejectNotHost, Message: "only seated host or system may start", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
		}
	}
	return nil
}

// PrevalidateStartNextGame runs non-mutating next-game checks without deal material.
func (s *Session) PrevalidateStartNextGame(cmd StartNextGameCommand) *Rejection {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		if out.Rejection != nil {
			return out.Rejection
		}
		return nil
	}
	if !cmd.CommandID.Valid() || !cmd.GameID.Valid() {
		return &Rejection{Code: RejectInvalidIdentity, Message: "start next game requires identities", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if rej := sequenceRejection(s.room.sequence, cmd.ExpectedSequence); rej != nil {
		return rej
	}
	if s.room.status.IsTerminal() {
		return &Rejection{Code: RejectAlreadyTerminal, Message: "room is terminal", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if s.room.status != RoomStatusInProgress {
		return &Rejection{Code: RejectNotInProgress, Message: "next game requires in_progress room", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if s.match != nil && s.match.Completed() {
		return &Rejection{Code: RejectAlreadyTerminal, Message: "match already complete", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if s.game != nil && s.gameID == cmd.GameID && !s.game.Completed() {
		return nil // idempotent accept path
	}
	if s.game != nil && !s.game.Completed() {
		return &Rejection{Code: RejectGameStillActive, Message: "current game still active", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if !s.room.gameCompletedInMatch {
		return &Rejection{Code: RejectGameStillActive, Message: "no completed game awaiting next", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	if _, used := s.usedGameIDs[cmd.GameID]; used {
		return &Rejection{Code: RejectGameIDReused, Message: "game id already used in match", SubmittedSequence: cmd.ExpectedSequence, CurrentSequence: s.room.sequence}
	}
	return nil
}

// PrevalidateDrawCard runs gameplay prechecks without draw material.
func (s *Session) PrevalidateDrawCard(cmd DrawCardCommand) *Rejection {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		if out.Rejection != nil {
			return out.Rejection
		}
		return nil
	}
	if out, ok := s.precheckGameplayReadonly(cmd.CommandID, cmd.PlayerID, cmd.ExpectedSequence); !ok {
		return out
	}
	return nil
}

// PrevalidateReportMissingUno runs gameplay prechecks without penalty draw material.
func (s *Session) PrevalidateReportMissingUno(cmd ReportMissingUnoCommand) *Rejection {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		if out.Rejection != nil {
			return out.Rejection
		}
		return nil
	}
	if out, ok := s.precheckGameplayReadonly(cmd.CommandID, cmd.ChallengerID, cmd.ExpectedSequence); !ok {
		return out
	}
	return nil
}

func (s *Session) precheckGameplayReadonly(commandID CommandID, playerID PlayerID, expected SequenceNumber) (*Rejection, bool) {
	if !commandID.Valid() || !playerID.Valid() {
		return &Rejection{Code: RejectInvalidIdentity, Message: "gameplay requires identities", SubmittedSequence: expected, CurrentSequence: s.room.sequence}, false
	}
	if rej := sequenceRejection(s.room.sequence, expected); rej != nil {
		return rej, false
	}
	if s.room.status.IsTerminal() {
		return &Rejection{Code: RejectAlreadyTerminal, Message: "room is terminal", SubmittedSequence: expected, CurrentSequence: s.room.sequence}, false
	}
	if s.room.status != RoomStatusInProgress {
		return &Rejection{Code: RejectNotInProgress, Message: "gameplay requires in_progress", SubmittedSequence: expected, CurrentSequence: s.room.sequence}, false
	}
	if s.game == nil {
		return &Rejection{Code: RejectNoActiveGame, Message: "no active game", SubmittedSequence: expected, CurrentSequence: s.room.sequence}, false
	}
	if !s.room.roster.IsSeated(playerID) {
		return &Rejection{Code: RejectNotSeated, Message: "player not seated", SubmittedSequence: expected, CurrentSequence: s.room.sequence}, false
	}
	if _, ok := s.room.DisconnectState(playerID); ok {
		return &Rejection{Code: RejectPlayerDisconnected, Message: "player disconnected during reconnect window", SubmittedSequence: expected, CurrentSequence: s.room.sequence}, false
	}
	return nil, true
}

func sequenceRejection(current, expected SequenceNumber) *Rejection {
	if expected < current {
		return &Rejection{Code: RejectStaleSequence, Message: "submitted sequence is stale", SubmittedSequence: expected, CurrentSequence: current}
	}
	if expected > current {
		return &Rejection{Code: RejectFutureSequence, Message: "submitted sequence is in the future", SubmittedSequence: expected, CurrentSequence: current}
	}
	return nil
}

// StartMatch locks-to-in_progress via Room, then binds the injected deal.
func (s *Session) StartMatch(cmd StartMatchCommand, deal game.DealMaterial) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	// Never replace an active authoritative game when Room would no-op in_progress.
	if s.game != nil && !s.game.Completed() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectGameStillActive, "active game already bound")
	}
	if s.room.status == RoomStatusInProgress || s.room.status == RoomStatusCompleted {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectGameStillActive, "match already started")
	}
	if _, used := s.usedGameIDs[cmd.GameID]; used {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectGameIDReused, "game id already used in match")
	}
	g, err := s.newGame(cmd.GameID, deal)
	if err != nil {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDealMismatch, err.Error())
	}
	out := s.room.StartMatch(cmd)
	if !out.Accepted() || out.Rejection != nil {
		return out
	}
	s.attachGame(cmd.GameID, g)
	s.turnVersion = s.room.Sequence()
	return out
}

// StartNextGame begins the next game in a best-of-three after GameCompleted.
func (s *Session) StartNextGame(cmd StartNextGameCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.GameID.Valid() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "start next game requires identities")
	}
	if out, ok := s.room.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if s.room.status.IsTerminal() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	if s.room.status != RoomStatusInProgress {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotInProgress, "next game requires in_progress room")
	}
	if s.match != nil && s.match.Completed() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "match already complete")
	}
	if s.game != nil && s.gameID == cmd.GameID && !s.game.Completed() {
		// Idempotent if next game already exists.
		return s.room.store(acceptedOutcome(cmd.CommandID, s.room.sequence, nil))
	}
	if s.game != nil && !s.game.Completed() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectGameStillActive, "current game still active")
	}
	if !s.room.gameCompletedInMatch {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectGameStillActive, "no completed game awaiting next")
	}
	if _, used := s.usedGameIDs[cmd.GameID]; used {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectGameIDReused, "game id already used in match")
	}
	g, err := s.newGame(cmd.GameID, cmd.Deal)
	if err != nil {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDealMismatch, err.Error())
	}
	s.attachGame(cmd.GameID, g)
	s.room.gameCompletedInMatch = false
	seq := s.room.advance()
	s.turnVersion = seq
	return s.room.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactGameStarted, map[string]string{
			"roomId": string(s.room.id),
			"gameId": string(cmd.GameID),
		}),
	}))
}

func (s *Session) PlayCard(cmd PlayCardCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if out, ok := s.precheckGameplay(cmd.CommandID, cmd.PlayerID, cmd.ExpectedSequence); !ok {
		return out
	}
	gout := s.game.PlayCard(game.PlayCardCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		PlayerID:         game.PlayerID(cmd.PlayerID),
		CardID:           cmd.CardID,
		ExpectedSequence: s.game.Sequence(),
		NowUTC:           cmd.NowUTC,
	})
	return s.commitGameplay(cmd.CommandID, cmd.ExpectedSequence, gout, TriggeringGameEventID(cmd.CommandID), cmd.NowUTC)
}

func (s *Session) DrawCard(cmd DrawCardCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if out, ok := s.precheckGameplay(cmd.CommandID, cmd.PlayerID, cmd.ExpectedSequence); !ok {
		return out
	}
	gout := s.game.DrawCard(game.DrawCardCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		PlayerID:         game.PlayerID(cmd.PlayerID),
		Cards:            cmd.Cards,
		ExpectedSequence: s.game.Sequence(),
		DrawPileSize:     cmd.DrawPileSize,
		HasDrawPileSize:  cmd.HasDrawPileSize,
	})
	return s.commitGameplay(cmd.CommandID, cmd.ExpectedSequence, gout, "", time.Time{})
}

func (s *Session) ChooseColor(cmd ChooseColorCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if out, ok := s.precheckGameplay(cmd.CommandID, cmd.PlayerID, cmd.ExpectedSequence); !ok {
		return out
	}
	gout := s.game.ChooseColor(game.ChooseColorCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		PlayerID:         game.PlayerID(cmd.PlayerID),
		Color:            cmd.Color,
		ExpectedSequence: s.game.Sequence(),
	})
	return s.commitGameplay(cmd.CommandID, cmd.ExpectedSequence, gout, "", time.Time{})
}

func (s *Session) CallUno(cmd CallUnoCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if out, ok := s.precheckGameplay(cmd.CommandID, cmd.PlayerID, cmd.ExpectedSequence); !ok {
		return out
	}
	gout := s.game.CallUno(game.CallUnoCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		PlayerID:         game.PlayerID(cmd.PlayerID),
		ExpectedSequence: s.game.Sequence(),
		NowUTC:           cmd.NowUTC,
	})
	out := s.commitGameplay(cmd.CommandID, cmd.ExpectedSequence, gout, "", time.Time{})
	if out.Accepted() && out.Rejection == nil {
		// Room timer authority closes on CallUno; later expiry must reject.
		s.clearRoomUno()
	}
	return out
}

func (s *Session) ReportMissingUno(cmd ReportMissingUnoCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if out, ok := s.precheckGameplay(cmd.CommandID, cmd.ChallengerID, cmd.ExpectedSequence); !ok {
		return out
	}
	gout := s.game.ReportMissingUno(game.ReportMissingUnoCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		ChallengerID:     game.PlayerID(cmd.ChallengerID),
		TargetID:         game.PlayerID(cmd.TargetID),
		Cards:            cmd.Cards,
		ExpectedSequence: s.game.Sequence(),
		NowUTC:           cmd.NowUTC,
		DrawPileSize:     cmd.DrawPileSize,
		HasDrawPileSize:  cmd.HasDrawPileSize,
	})
	out := s.commitGameplay(cmd.CommandID, cmd.ExpectedSequence, gout, "", time.Time{})
	if out.Accepted() && out.Rejection == nil {
		s.clearRoomUno()
	}
	return out
}

// ExpireUnoWindow rechecks absolute UTC expiresAt plus opening room sequence.
// Engine acceptance is required before any room mutation.
func (s *Session) ExpireUnoWindow(cmd ExpireUnoWindowCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidCommand, "expire uno requires commandId")
	}
	if out, ok := s.room.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if s.game == nil {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNoActiveGame, "no active game")
	}
	if !s.room.hasUno {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "no open uno window")
	}
	// Read-only room key/timeliness checks — no mutation yet.
	if !s.room.unoWindow.Matches(cmd.GameID, cmd.PlayerID, cmd.TriggeringGameEventID, cmd.OpeningSequence) {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowMismatch, "uno expiry denied")
	}
	if !s.room.unoWindow.IsOpen() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "uno expiry denied")
	}
	if s.room.unoWindow.IsTimely(cmd.NowUTC) {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowNotExpired, "uno expiry denied")
	}

	uw := s.game.UnoWindow()
	if uw == nil {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "engine window inactive")
	}
	gout := s.game.ExpireUnoWindow(game.ExpireUnoWindowCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		PlayerID:         game.PlayerID(cmd.PlayerID),
		OpeningSequence:  uw.OpeningSequence,
		ExpectedSequence: s.game.Sequence(),
		NowUTC:           cmd.NowUTC,
	})
	if gout.Rejection != nil {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, mapRejection(gout.Rejection.Code), gout.Rejection.Message)
	}

	expired, ok, code := s.room.unoWindow.Expire(cmd.NowUTC, cmd.GameID, cmd.PlayerID, cmd.TriggeringGameEventID, cmd.OpeningSequence)
	if !ok {
		// Engine accepted but room key failed — should not happen after prechecks.
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, code, "uno expiry denied")
	}
	s.room.unoWindow = expired
	s.room.hasUno = false
	seq := s.room.advance()
	return s.room.store(acceptedOutcome(cmd.CommandID, seq, []Fact{
		newFact(FactUnoWindowExpired, map[string]string{
			"roomId":                string(s.room.id),
			"playerId":              string(cmd.PlayerID),
			"gameId":                string(cmd.GameID),
			"triggeringGameEventId": string(cmd.TriggeringGameEventID),
			"openingSequence":       strconv.FormatUint(uint64(expired.OpeningSequence), 10),
		}),
	}))
}

// ForfeitPlayer applies room forfeit then removes the player from the game turn ring.
func (s *Session) ForfeitPlayer(cmd ForfeitPlayerCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "forfeit requires commandId and playerId")
	}
	if out, ok := s.room.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if s.room.status.IsTerminal() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectAlreadyTerminal, "room is terminal")
	}
	state, ok := s.room.disconnects[cmd.PlayerID]
	if !ok {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDisconnectInactive, "no active disconnect")
	}
	if okForfeit, code := state.CanForfeit(cmd.PlayerID, cmd.DisconnectVersion, cmd.NowUTC); !okForfeit {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, code, "forfeit denied")
	}

	var gameFacts []Fact
	gameDone := false
	if s.game != nil && !s.game.Completed() {
		gout := s.game.ForfeitPlayer(game.ForfeitPlayerCommand{
			CommandID:        game.CommandID(cmd.CommandID),
			PlayerID:         game.PlayerID(cmd.PlayerID),
			ExpectedSequence: s.game.Sequence(),
		})
		if gout.Rejection != nil {
			return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, mapRejection(gout.Rejection.Code), gout.Rejection.Message)
		}
		gameFacts = mapGameFacts(gout.Facts, s.room.id, s.gameID, 0)
		gameDone = HasFact(gameFacts, FactGameCompleted)
		if HasFact(gameFacts, FactUnoWindowClosed) {
			s.clearRoomUno()
		}
	}

	// Room roster / disconnect mutation (mirrors applyForfeitPlayer).
	s.room.disconnects[cmd.PlayerID] = state.Clear()
	delete(s.room.disconnects, cmd.PlayerID)
	if s.room.roster.IsSeated(cmd.PlayerID) {
		s.room.roster.clear(cmd.PlayerID)
	}

	factData := map[string]string{
		"roomId":            string(s.room.id),
		"playerId":          string(cmd.PlayerID),
		"disconnectVersion": strconv.FormatUint(uint64(cmd.DisconnectVersion), 10),
		"roomType":          string(s.room.roomType),
	}
	if s.room.roomType == RoomTypeTournament && s.room.tournamentID.Valid() {
		factData["tournamentId"] = string(s.room.tournamentID)
	}
	facts := []Fact{newFact(FactPlayerForfeited, factData)}
	facts = append(facts, gameFacts...)

	if s.match != nil {
		s.match.RecordForfeit(game.PlayerID(cmd.PlayerID))
	}

	if gameDone && s.match != nil {
		_, scoreFacts, okApply, matchDone := s.match.ApplyGameCompletion(s.game, cmd.NowUTC)
		if okApply {
			for _, gf := range scoreFacts {
				facts = append(facts, Fact{Name: FactName(gf.Name), Data: copyData(gf.Data)})
			}
		}
		s.room.gameCompletedInMatch = true
		s.clearRoomUno()
		if matchDone {
			facts = append(facts, s.terminalMatchFacts()...)
		}
	}

	seq := s.room.advance()
	if HasFact(facts, FactTurnAdvanced) || HasFact(facts, FactTurnSkipped) || gameDone {
		s.turnVersion = seq
	}
	return s.room.store(acceptedOutcome(cmd.CommandID, seq, facts))
}

// SkipDisconnectedTurn skips a disconnected player's turn during the reconnect window.
func (s *Session) SkipDisconnectedTurn(cmd SkipDisconnectedTurnCommand) CommandOutcome {
	if out, ok := s.room.recall(cmd.CommandID); ok {
		return out
	}
	if !cmd.CommandID.Valid() || !cmd.PlayerID.Valid() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "skip requires identities")
	}
	if out, ok := s.room.requireSequence(cmd.CommandID, cmd.ExpectedSequence); !ok {
		return out
	}
	if s.room.status != RoomStatusInProgress {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotInProgress, "skip requires in_progress")
	}
	if s.game == nil || s.game.Completed() {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNoActiveGame, "no active game")
	}
	if _, ok := s.room.DisconnectState(cmd.PlayerID); !ok {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotDisconnected, "player not disconnected")
	}
	key := skipKey{player: cmd.PlayerID, turn: cmd.TurnVersion}
	if _, done := s.skipped[key]; done {
		return s.room.store(acceptedOutcome(cmd.CommandID, s.room.sequence, nil))
	}
	if cmd.TurnVersion != s.turnVersion {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, RejectTurnVersionMismatch, "stale turn version")
	}

	gout := s.game.SkipTurn(game.SkipTurnCommand{
		CommandID:        game.CommandID(cmd.CommandID),
		PlayerID:         game.PlayerID(cmd.PlayerID),
		ExpectedSequence: s.game.Sequence(),
	})
	if gout.Rejection != nil {
		return s.room.reject(cmd.CommandID, cmd.ExpectedSequence, mapRejection(gout.Rejection.Code), gout.Rejection.Message)
	}
	facts := mapGameFacts(gout.Facts, s.room.id, s.gameID, 0)
	if HasFact(facts, FactUnoWindowClosed) {
		s.clearRoomUno()
	}
	s.skipped[key] = struct{}{}
	seq := s.room.advance()
	s.turnVersion = seq
	return s.room.store(acceptedOutcome(cmd.CommandID, seq, facts))
}

func (s *Session) newGame(gameID GameID, deal game.DealMaterial) (*game.Game, error) {
	seats := seatedPlayers(s.room.roster)
	gPlayers := make([]game.PlayerID, len(seats))
	for i, p := range seats {
		gPlayers[i] = game.PlayerID(p)
	}
	return game.StartGame(game.GameID(gameID), gPlayers, deal)
}

func (s *Session) attachGame(gameID GameID, g *game.Game) {
	seats := seatedPlayers(s.room.roster)
	gPlayers := make([]game.PlayerID, len(seats))
	for i, p := range seats {
		gPlayers[i] = game.PlayerID(p)
	}
	s.game = g
	s.gameID = gameID
	s.usedGameIDs[gameID] = struct{}{}
	if s.match == nil {
		s.match = game.NewMatch(gPlayers)
	}
	s.clearRoomUno()
}

func (s *Session) precheckGameplay(commandID CommandID, playerID PlayerID, expected SequenceNumber) (CommandOutcome, bool) {
	if !commandID.Valid() || !playerID.Valid() {
		return s.room.reject(commandID, expected, RejectInvalidIdentity, "gameplay requires identities"), false
	}
	if out, ok := s.room.requireSequence(commandID, expected); !ok {
		return out, false
	}
	if s.room.status.IsTerminal() {
		return s.room.reject(commandID, expected, RejectAlreadyTerminal, "room is terminal"), false
	}
	if s.room.status != RoomStatusInProgress {
		return s.room.reject(commandID, expected, RejectNotInProgress, "gameplay requires in_progress"), false
	}
	if s.game == nil {
		return s.room.reject(commandID, expected, RejectNoActiveGame, "no active game"), false
	}
	if !s.room.roster.IsSeated(playerID) {
		return s.room.reject(commandID, expected, RejectNotSeated, "player not seated"), false
	}
	if _, ok := s.room.DisconnectState(playerID); ok {
		return s.room.reject(commandID, expected, RejectPlayerDisconnected, "player disconnected during reconnect window"), false
	}
	// Host label confers no gameplay privilege after lock/start — same path for all seats.
	return CommandOutcome{}, true
}

func (s *Session) commitGameplay(
	commandID CommandID,
	expected SequenceNumber,
	gout game.CommandOutcome,
	unoTrigger TriggeringGameEventID,
	nowUTC time.Time,
) CommandOutcome {
	if gout.Kind == game.OutcomeDuplicate {
		// Engine duplicate should not occur when Session gates by room command id first.
		mapped := mapGameFacts(gout.Facts, s.room.id, s.gameID, 0)
		dup := acceptedOutcome(commandID, s.room.sequence, mapped)
		dup.Kind = OutcomeDuplicate
		if gout.Rejection != nil {
			code := mapRejection(gout.Rejection.Code)
			return duplicateOutcome(rejectedOutcome(commandID, s.room.sequence, Rejection{Code: code, Message: gout.Rejection.Message}))
		}
		return s.room.store(dup)
	}
	if gout.Rejection != nil {
		return s.room.reject(commandID, expected, mapRejection(gout.Rejection.Code), gout.Rejection.Message)
	}

	nextSeq := s.room.sequence + 1
	facts := mapGameFacts(gout.Facts, s.room.id, s.gameID, nextSeq)

	if HasFact(facts, FactUnoWindowOpened) {
		player := PlayerID("")
		expiresAt := time.Time{}
		for _, f := range facts {
			if f.Name != FactUnoWindowOpened {
				continue
			}
			player = PlayerID(f.Data["playerId"])
			if t, err := time.Parse(time.RFC3339Nano, f.Data["expiresAt"]); err == nil {
				expiresAt = t
			}
		}
		trigger := unoTrigger
		if !trigger.Valid() {
			trigger = TriggeringGameEventID(commandID)
		}
		openedAt := nowUTC
		if openedAt.IsZero() && !expiresAt.IsZero() {
			openedAt = expiresAt.Add(-UnoWindowDuration)
		}
		if openedAt.IsZero() {
			openedAt = time.Now().UTC()
		}
		s.room.unoWindow = OpenUnoWindow(player, s.gameID, trigger, openedAt, nextSeq)
		// Preserve absolute expiresAt from engine when provided.
		if !expiresAt.IsZero() {
			s.room.unoWindow.ExpiresAt = expiresAt.UTC()
		}
		s.room.hasUno = true
		for i := range facts {
			if facts[i].Name == FactUnoWindowOpened {
				facts[i].Data["openingSequence"] = strconv.FormatUint(uint64(nextSeq), 10)
				facts[i].Data["roomId"] = string(s.room.id)
				facts[i].Data["gameId"] = string(s.gameID)
				facts[i].Data["triggeringGameEventId"] = string(s.room.unoWindow.TriggeringGameEventID)
				facts[i].Data["expiresAt"] = s.room.unoWindow.ExpiresAt.Format(time.RFC3339Nano)
			}
		}
	}
	if HasFact(facts, FactUnoWindowClosed) {
		s.clearRoomUno()
	}

	turnAdvanced := HasFact(facts, FactTurnAdvanced) || HasFact(facts, FactTurnSkipped)
	gameDone := HasFact(facts, FactGameCompleted)

	if gameDone && s.match != nil {
		_, scoreFacts, ok, matchDone := s.match.ApplyGameCompletion(s.game, nowUTC)
		if ok {
			for _, gf := range scoreFacts {
				facts = append(facts, Fact{Name: FactName(gf.Name), Data: copyData(gf.Data)})
			}
		}
		s.room.gameCompletedInMatch = true
		s.clearRoomUno()
		if matchDone {
			facts = append(facts, s.terminalMatchFacts()...)
		}
	}

	seq := s.room.advance()
	if turnAdvanced || gameDone {
		s.turnVersion = seq
	}
	return s.room.store(acceptedOutcome(commandID, seq, facts))
}

func (s *Session) terminalMatchFacts() []Fact {
	s.room.status = RoomStatusCompleted
	data := map[string]string{
		"roomId": string(s.room.id),
		"status": string(RoomStatusCompleted),
	}
	if s.match != nil {
		for k, v := range s.match.CompletionFacts() {
			data[k] = v
		}
	}
	if s.room.roomType == RoomTypeTournament && s.room.tournamentID.Valid() {
		data["tournamentId"] = string(s.room.tournamentID)
		if s.room.roundNumber > 0 {
			data["roundNumber"] = strconv.Itoa(s.room.roundNumber)
		}
		if s.room.slotID != "" {
			data["slotId"] = s.room.slotID
		}
	}
	return []Fact{
		newFact(FactMatchCompleted, data),
		newFact(FactRoomCompleted, map[string]string{
			"roomId": string(s.room.id),
			"status": string(RoomStatusCompleted),
		}),
		newFact(FactSpectatorStreamsClose, map[string]string{
			"roomId": string(s.room.id),
			"reason": "room_completed",
		}),
	}
}

func (s *Session) clearRoomUno() {
	if s.room.hasUno {
		s.room.unoWindow = s.room.unoWindow.CloseResolved()
		s.room.hasUno = false
	}
}

func seatedPlayers(roster Roster) []PlayerID {
	seats := roster.Seats()
	out := make([]PlayerID, 0, len(seats))
	for _, seat := range seats {
		if seat.Occupied {
			out = append(out, seat.PlayerID)
		}
	}
	return out
}

func mapGameFacts(in []game.Fact, roomID RoomID, gameID GameID, openingSeq SequenceNumber) []Fact {
	out := make([]Fact, 0, len(in))
	for _, f := range in {
		data := copyData(f.Data)
		data["roomId"] = string(roomID)
		if gameID.Valid() {
			data["gameId"] = string(gameID)
		}
		if f.Name == game.FactUnoWindowOpened && openingSeq > 0 {
			data["openingSequence"] = strconv.FormatUint(uint64(openingSeq), 10)
		}
		out = append(out, Fact{Name: FactName(f.Name), Data: data})
	}
	return out
}

func copyData(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mapRejection(code game.RejectionCode) RejectionCode {
	switch RejectionCode(code) {
	case RejectStaleSequence, RejectFutureSequence, RejectSequenceRequired,
		RejectInvalidIdentity, RejectInvalidCommand, RejectNotInHand, RejectIllegalCard,
		RejectOutOfTurn, RejectPendingColor, RejectNotPenaltyTarget, RejectJumpInBlocked,
		RejectJumpInMismatch, RejectColorNotPending, RejectInvalidColor,
		RejectUnoWindowInactive, RejectUnoWindowMismatch, RejectUnoWindowNotExpired,
		RejectUnoAlreadyCalled, RejectDrawBatchMismatch, RejectGameCompleted, RejectDealMismatch:
		return RejectionCode(code)
	case RejectionCode(game.RejectNotCurrentPlayer):
		return RejectOutOfTurn
	case RejectionCode(game.RejectPlayerNotActive):
		return RejectNotSeated
	default:
		return RejectionCode(code)
	}
}
