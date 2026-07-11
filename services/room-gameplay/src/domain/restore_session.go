package domain

import "unoarena/services/room-gameplay/game"

// RestoreSession rebuilds a Session from durable storage without applying commands.
func RestoreSession(
	room *Room,
	g *game.Game,
	m *game.Match,
	gameID GameID,
	used map[GameID]struct{},
	turnVersion SequenceNumber,
	skipped []SkippedTurn,
) *Session {
	usedOut := map[GameID]struct{}{}
	for k := range used {
		usedOut[k] = struct{}{}
	}
	skipOut := map[skipKey]struct{}{}
	for _, s := range skipped {
		skipOut[skipKey{player: s.PlayerID, turn: s.TurnVersion}] = struct{}{}
	}
	return &Session{
		room:        room,
		game:        g,
		match:       m,
		gameID:      gameID,
		usedGameIDs: usedOut,
		turnVersion: turnVersion,
		skipped:     skipOut,
	}
}

// UsedGameIDs exposes used game IDs for persistence.
func (s *Session) UsedGameIDs() map[GameID]struct{} {
	return cloneGameIDSet(s.usedGameIDs)
}

// SkippedTurns exposes skip-idempotency keys for persistence.
func (s *Session) SkippedTurns() []SkippedTurn {
	if len(s.skipped) == 0 {
		return nil
	}
	out := make([]SkippedTurn, 0, len(s.skipped))
	for k := range s.skipped {
		out = append(out, SkippedTurn{PlayerID: k.player, TurnVersion: k.turn})
	}
	return out
}
