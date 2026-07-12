package game

import "time"

// RestoreGameInput carries every Game field needed for exact durable round-trip.
type RestoreGameInput struct {
	ID            GameID
	Seats         []PlayerID
	Hands         map[PlayerID][]Card
	Discard       Card
	Active        Color
	Dir           Direction
	Current       int
	Sequence      SequenceNumber
	PendingColor  bool
	ColorChooser  PlayerID
	PenaltyAmount int
	PenaltyTarget PlayerID
	Uno           *UnoWindow
	Completed     bool
	Abandoned     bool
	Placement     []PlayerID
	CardPoints    map[PlayerID]int
	Outcomes      map[CommandID]CommandOutcome
	DrawPileSize  int
}

// RestoreGame rebuilds a Game from durable storage without applying commands.
func RestoreGame(in RestoreGameInput) *Game {
	hands := cloneHands(in.Hands)
	if hands == nil {
		hands = map[PlayerID][]Card{}
	}
	pts := cloneIntMap(in.CardPoints)
	if pts == nil {
		pts = map[PlayerID]int{}
	}
	outcomes := cloneOutcomes(in.Outcomes)
	if outcomes == nil {
		outcomes = map[CommandID]CommandOutcome{}
	}
	if in.DrawPileSize < 0 {
		in.DrawPileSize = 0
	}
	g := &Game{
		id:            in.ID,
		seats:         append([]PlayerID(nil), in.Seats...),
		hands:         hands,
		discard:       in.Discard,
		active:        in.Active,
		dir:           in.Dir,
		current:       in.Current,
		sequence:      in.Sequence,
		pendingColor:  in.PendingColor,
		colorChooser:  in.ColorChooser,
		penaltyAmount: in.PenaltyAmount,
		penaltyTarget: in.PenaltyTarget,
		completed:     in.Completed,
		abandoned:     in.Abandoned,
		placement:     append([]PlayerID(nil), in.Placement...),
		cardPoints:    pts,
		outcomes:      outcomes,
		drawPileSize:  in.DrawPileSize,
	}
	if in.Uno != nil {
		uw := *in.Uno
		g.uno = &uw
	}
	return g
}

// RestoreUnoWindow rebuilds a game UnoWindow including unexported open flags.
func RestoreUnoWindow(
	playerID PlayerID,
	expiresAt time.Time,
	openingSequence SequenceNumber,
	called bool,
	open bool,
	closedByNextTurn bool,
) UnoWindow {
	return UnoWindow{
		PlayerID:         playerID,
		ExpiresAt:        expiresAt.UTC(),
		OpeningSequence:  openingSequence,
		Called:           called,
		open:             open,
		closedByNextTurn: closedByNextTurn,
	}
}

// RestoreMatchInput carries every Match field for exact durable round-trip.
type RestoreMatchInput struct {
	Players       []PlayerID
	Wins          map[PlayerID]int
	CardPoints    map[PlayerID]int
	Forfeits      []PlayerID
	CompletedAt   time.Time
	Abandoned     bool
	CompletionVer int
}

// RestoreMatch rebuilds a Match from durable storage.
func RestoreMatch(in RestoreMatchInput) *Match {
	wins := cloneIntMap(in.Wins)
	if wins == nil {
		wins = map[PlayerID]int{}
	}
	pts := cloneIntMap(in.CardPoints)
	if pts == nil {
		pts = map[PlayerID]int{}
	}
	return &Match{
		players:       append([]PlayerID(nil), in.Players...),
		score:         MatchScore{wins: wins},
		cardPoints:    pts,
		forfeits:      append([]PlayerID(nil), in.Forfeits...),
		completedAt:   in.CompletedAt.UTC(),
		abandoned:     in.Abandoned,
		completionVer: in.CompletionVer,
	}
}

// Seats returns a copy of the turn-order seat list.
func (g *Game) Seats() []PlayerID {
	return append([]PlayerID(nil), g.seats...)
}

// HandsMap returns a deep copy of private hands for persistence.
func (g *Game) HandsMap() map[PlayerID][]Card {
	return cloneHands(g.hands)
}

// CurrentSeatIndex returns the current acting seat index.
func (g *Game) CurrentSeatIndex() int { return g.current }

// ColorChooser returns the player who must choose a wild color, if any.
func (g *Game) ColorChooser() PlayerID { return g.colorChooser }

// OutcomesMap exposes game-scoped command outcomes for persistence.
func (g *Game) OutcomesMap() map[CommandID]CommandOutcome {
	return cloneOutcomes(g.outcomes)
}

// CompletionVer exposes the match completion version counter.
func (m *Match) CompletionVer() int { return m.completionVer }
