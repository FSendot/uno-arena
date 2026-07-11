package game

import (
	"strconv"
	"time"
)

// DealMaterial is the authoritative initial deal from Game Integrity.
// The engine never shuffles or invents cards.
type DealMaterial struct {
	Hands           map[PlayerID][]Card
	DiscardTop      Card
	ActiveColor     Color // required when DiscardTop is wild; otherwise derived from DiscardTop.Color
	CurrentSeat     int
	Direction       Direction
	ApplyTopEffects bool // when true, apply skip/reverse/draw on the flipped discard
}

// Game is a pure deterministic Uno game state machine.
type Game struct {
	id    GameID
	seats []PlayerID
	hands map[PlayerID][]Card

	discard Card
	active  Color
	dir     Direction
	current int

	sequence SequenceNumber

	pendingColor bool
	colorChooser PlayerID

	penaltyAmount int
	penaltyTarget PlayerID

	uno *UnoWindow

	completed  bool
	abandoned  bool
	placement  []PlayerID
	cardPoints map[PlayerID]int

	outcomes map[CommandID]CommandOutcome
}

// StartGame creates a game from an injected authoritative deal.
func StartGame(id GameID, seats []PlayerID, deal DealMaterial) (*Game, error) {
	if id == "" {
		return nil, Rejection{Code: RejectInvalidCommand, Message: "game id required"}
	}
	if len(seats) < 2 {
		return nil, Rejection{Code: RejectWrongPlayerCount, Message: "need at least 2 players"}
	}
	if deal.Hands == nil {
		return nil, Rejection{Code: RejectDealMismatch, Message: "missing hands"}
	}
	hands := make(map[PlayerID][]Card, len(seats))
	for _, p := range seats {
		h, ok := deal.Hands[p]
		if !ok {
			return nil, Rejection{Code: RejectDealMismatch, Message: "missing hand for " + string(p)}
		}
		hands[p] = cloneHand(h)
	}
	dir := deal.Direction
	if dir == 0 {
		dir = DirectionClockwise
	}
	active := deal.ActiveColor
	if deal.DiscardTop.IsWild() {
		if !validPlayableColor(active) {
			return nil, Rejection{Code: RejectDealMismatch, Message: "wild discard requires active color"}
		}
	} else {
		active = deal.DiscardTop.Color
	}
	cur := deal.CurrentSeat
	if cur < 0 || cur >= len(seats) {
		return nil, Rejection{Code: RejectDealMismatch, Message: "invalid current seat"}
	}
	g := &Game{
		id:         id,
		seats:      append([]PlayerID(nil), seats...),
		hands:      hands,
		discard:    deal.DiscardTop,
		active:     active,
		dir:        dir,
		current:    cur,
		sequence:   1,
		outcomes:   make(map[CommandID]CommandOutcome),
		cardPoints: make(map[PlayerID]int),
	}
	if deal.ApplyTopEffects && !deal.DiscardTop.IsWild() {
		g.applyDealTopEffects(deal.DiscardTop)
	}
	return g, nil
}

func (g *Game) applyDealTopEffects(top Card) {
	switch top.Face {
	case FaceSkip:
		g.current = g.nextIndex(g.current)
	case FaceReverse:
		g.dir = -g.dir
		if len(g.seats) == 2 {
			return // reverse-as-skip: starting seat keeps the turn
		}
		g.current = g.nextIndex(g.current)
	case FaceDrawTwo:
		g.penaltyAmount = 2
		g.penaltyTarget = g.seats[g.current]
	}
}

// ID returns the game identity.
func (g *Game) ID() GameID { return g.id }

// Sequence returns the current game sequence number.
func (g *Game) Sequence() SequenceNumber { return g.sequence }

// CurrentPlayer returns the player who must act.
func (g *Game) CurrentPlayer() PlayerID {
	if g.pendingColor {
		return g.colorChooser
	}
	if g.penaltyAmount > 0 && g.penaltyTarget != "" {
		return g.penaltyTarget
	}
	return g.seats[g.current]
}

// Direction returns turn direction.
func (g *Game) Direction() Direction { return g.dir }

// ActiveColor returns the effective discard color.
func (g *Game) ActiveColor() Color { return g.active }

// DiscardTop returns the top discard card.
func (g *Game) DiscardTop() Card { return g.discard }

// Completed reports whether the game has finished.
func (g *Game) Completed() bool { return g.completed }

// Abandoned reports whether completion was due to forfeit collapse.
func (g *Game) Abandoned() bool { return g.abandoned }

// IsActive reports whether player is still in the turn/hand ring.
func (g *Game) IsActive(player PlayerID) bool {
	_, ok := g.seatIndex(player)
	return ok
}

// ActiveSeats returns the ordered active player ring.
func (g *Game) ActiveSeats() []PlayerID {
	return append([]PlayerID(nil), g.seats...)
}

// PlacementOrder returns first-through-last player IDs after completion.
func (g *Game) PlacementOrder() []PlayerID {
	return append([]PlayerID(nil), g.placement...)
}

// CardPoints returns per-player hand points (final after completion).
func (g *Game) CardPoints() map[PlayerID]int {
	out := make(map[PlayerID]int, len(g.seats))
	for _, p := range g.seats {
		if g.completed {
			out[p] = g.cardPoints[p]
		} else {
			out[p] = HandPoints(g.hands[p])
		}
	}
	return out
}

// Hand returns a copy of a player's private hand.
func (g *Game) Hand(player PlayerID) []Card {
	return cloneHand(g.hands[player])
}

// HandCount returns the public card count for a player (never identities).
func (g *Game) HandCount(player PlayerID) int {
	return len(g.hands[player])
}

// DirectionLabel returns a spectator-safe direction string.
func DirectionLabel(d Direction) string {
	if d == DirectionCounterClockwise {
		return "counterclockwise"
	}
	return "clockwise"
}

// PenaltyAmount returns the accumulated draw penalty (0 if none).
func (g *Game) PenaltyAmount() int { return g.penaltyAmount }

// PenaltyTarget returns the player who must stack or draw.
func (g *Game) PenaltyTarget() PlayerID { return g.penaltyTarget }

// PendingColorChoice reports whether a wild color must be chosen.
func (g *Game) PendingColorChoice() bool { return g.pendingColor }

// UnoWindow returns a copy of the open window, or nil.
func (g *Game) UnoWindow() *UnoWindow {
	if g.uno == nil || !g.uno.IsOpen() {
		return nil
	}
	cp := *g.uno
	return &cp
}

// PublicState is privacy-safe: card counts only, never hand contents.
type PublicState struct {
	GameID             GameID
	Sequence           SequenceNumber
	DiscardTop         Card
	ActiveColor        Color
	CurrentPlayer      PlayerID
	Direction          Direction
	HandCounts         map[PlayerID]int
	PenaltyAmount      int
	PenaltyTarget      PlayerID
	PendingColorChoice bool
	Completed          bool
	Uno                *PublicUnoWindow
}

// PublicUnoWindow exposes absolute expiry and opening sequence only.
type PublicUnoWindow struct {
	PlayerID        PlayerID
	ExpiresAt       time.Time
	OpeningSequence SequenceNumber
	Called          bool
}

// PublicSnapshot returns spectator-safe state (counts only for hands).
func (g *Game) PublicSnapshot() PublicState {
	counts := make(map[PlayerID]int, len(g.seats))
	for _, p := range g.seats {
		counts[p] = len(g.hands[p])
	}
	ps := PublicState{
		GameID:             g.id,
		Sequence:           g.sequence,
		DiscardTop:         g.discard,
		ActiveColor:        g.active,
		CurrentPlayer:      g.CurrentPlayer(),
		Direction:          g.dir,
		HandCounts:         counts,
		PenaltyAmount:      g.penaltyAmount,
		PenaltyTarget:      g.penaltyTarget,
		PendingColorChoice: g.pendingColor,
		Completed:          g.completed,
	}
	if w := g.UnoWindow(); w != nil {
		ps.Uno = &PublicUnoWindow{
			PlayerID:        w.PlayerID,
			ExpiresAt:       w.ExpiresAt,
			OpeningSequence: w.OpeningSequence,
			Called:          w.Called,
		}
	}
	return ps
}

func (g *Game) seatIndex(p PlayerID) (int, bool) {
	for i, s := range g.seats {
		if s == p {
			return i, true
		}
	}
	return -1, false
}

func (g *Game) nextIndex(from int) int {
	n := len(g.seats)
	return (from + int(g.dir) + n) % n
}

func (g *Game) checkSequence(commandID CommandID, expected SequenceNumber) (CommandOutcome, bool) {
	if prior, ok := g.outcomes[commandID]; ok {
		return duplicateOutcome(prior), true
	}
	if g.completed {
		out := rejectedOutcome(commandID, g.sequence, Rejection{Code: RejectGameCompleted, SubmittedSequence: expected})
		g.outcomes[commandID] = out
		return out, true
	}
	if expected == 0 {
		out := rejectedOutcome(commandID, g.sequence, Rejection{Code: RejectSequenceRequired, SubmittedSequence: expected})
		g.outcomes[commandID] = out
		return out, true
	}
	if expected < g.sequence {
		out := rejectedOutcome(commandID, g.sequence, Rejection{Code: RejectStaleSequence, SubmittedSequence: expected})
		g.outcomes[commandID] = out
		return out, true
	}
	if expected > g.sequence {
		out := rejectedOutcome(commandID, g.sequence, Rejection{Code: RejectFutureSequence, SubmittedSequence: expected})
		g.outcomes[commandID] = out
		return out, true
	}
	return CommandOutcome{}, false
}

func (g *Game) commit(commandID CommandID, facts []Fact) CommandOutcome {
	g.sequence++
	out := acceptedOutcome(commandID, g.sequence, facts)
	g.outcomes[commandID] = out
	return out
}

func (g *Game) reject(commandID CommandID, expected SequenceNumber, code RejectionCode, msg string) CommandOutcome {
	out := rejectedOutcome(commandID, g.sequence, Rejection{Code: code, Message: msg, SubmittedSequence: expected})
	g.outcomes[commandID] = out
	return out
}

// maybeCloseUnoOnTurnBegin closes an open Uno window when a different player begins acting.
func (g *Game) maybeCloseUnoOnTurnBegin(acting PlayerID, facts *[]Fact) {
	if g.uno == nil || !g.uno.IsOpen() || g.uno.PlayerID == acting {
		return
	}
	closed := g.uno.CloseByNextTurn()
	g.uno = &closed
	*facts = append(*facts, newFact(FactUnoWindowClosed, map[string]string{
		"reason":   "next_turn",
		"playerId": string(closed.PlayerID),
	}))
}

// PlayCard handles turn play, draw stacking, or exact-match jump-in.
func (g *Game) PlayCard(cmd PlayCardCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if _, ok := g.seatIndex(cmd.PlayerID); !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "not seated")
	}
	card, ok := findCard(g.hands[cmd.PlayerID], cmd.CardID)
	if !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotInHand, "card not in hand")
	}
	if g.pendingColor {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectPendingColor, "color choice required")
	}

	mode := PlayModeTurn
	actingSeat, _ := g.seatIndex(cmd.PlayerID)

	if g.penaltyAmount > 0 {
		if cmd.PlayerID != g.penaltyTarget {
			return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectJumpInBlocked, "no jump-in during pending penalty")
		}
		if !canStack(card, g.hands[cmd.PlayerID], g.active, g.discard) {
			return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectIllegalCard, "cannot stack this card")
		}
		mode = PlayModeStack
	} else if cmd.PlayerID != g.seats[g.current] {
		if !ExactMatch(card, g.discard) {
			return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectJumpInMismatch, "not exact match")
		}
		mode = PlayModeJumpIn
	} else {
		if card.Face == FaceWildDrawFour {
			if !wildDrawFourLegal(g.hands[cmd.PlayerID], g.active, card.ID) {
				return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectIllegalCard, "wild draw four illegal")
			}
		} else if !canPlayOrdinary(card, g.discard, g.active) {
			return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectIllegalCard, "card not legal")
		}
	}

	facts := []Fact{}
	g.maybeCloseUnoOnTurnBegin(cmd.PlayerID, &facts)

	hand, card, _ := removeCard(g.hands[cmd.PlayerID], cmd.CardID)
	g.hands[cmd.PlayerID] = hand
	g.discard = card

	facts = append(facts, newFact(FactCardPlayed, map[string]string{
		"playerId": string(cmd.PlayerID),
		"cardId":   string(card.ID),
		"face":     string(card.Face),
		"color":    string(card.Color),
		"playMode": string(mode),
	}))

	switch mode {
	case PlayModeStack:
		g.applyStack(cmd.PlayerID, actingSeat, card, &facts)
	default:
		g.applyCardEffects(cmd.PlayerID, actingSeat, card, &facts)
	}

	if len(hand) == 1 {
		now := cmd.NowUTC
		if now.IsZero() {
			now = time.Now().UTC()
		}
		w := OpenUnoWindow(cmd.PlayerID, now.UTC(), g.sequence+1)
		g.uno = &w
		facts = append(facts, newFact(FactUnoWindowOpened, map[string]string{
			"playerId":        string(cmd.PlayerID),
			"expiresAt":       w.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"openingSequence": strconv.FormatUint(uint64(w.OpeningSequence), 10),
		}))
	}

	if len(hand) == 0 {
		g.finish(cmd.PlayerID, &facts)
	}
	return g.commit(cmd.CommandID, facts)
}

func (g *Game) applyStack(player PlayerID, seat int, card Card, facts *[]Fact) {
	g.penaltyAmount += card.DrawValue()
	next := g.nextIndex(seat)
	g.penaltyTarget = g.seats[next]
	g.current = next
	*facts = append(*facts, newFact(FactPenaltyStackIncreased, map[string]string{
		"amount": strconv.Itoa(g.penaltyAmount),
		"target": string(g.penaltyTarget),
	}))
	if card.Face == FaceWildDrawFour {
		g.pendingColor = true
		g.colorChooser = player
	} else {
		g.active = card.Color
		*facts = append(*facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.penaltyTarget),
		}))
	}
}

func (g *Game) applyCardEffects(player PlayerID, seat int, card Card, facts *[]Fact) {
	switch card.Face {
	case FaceWild:
		g.pendingColor = true
		g.colorChooser = player
		g.current = seat
	case FaceWildDrawFour:
		g.pendingColor = true
		g.colorChooser = player
		g.penaltyAmount += 4
		next := g.nextIndex(seat)
		g.penaltyTarget = g.seats[next]
		g.current = seat
		*facts = append(*facts, newFact(FactPenaltyStackIncreased, map[string]string{
			"amount": strconv.Itoa(g.penaltyAmount),
			"target": string(g.penaltyTarget),
		}))
	case FaceDrawTwo:
		g.active = card.Color
		g.penaltyAmount += 2
		next := g.nextIndex(seat)
		g.penaltyTarget = g.seats[next]
		g.current = next
		*facts = append(*facts,
			newFact(FactPenaltyStackIncreased, map[string]string{
				"amount": strconv.Itoa(g.penaltyAmount),
				"target": string(g.penaltyTarget),
			}),
			newFact(FactTurnAdvanced, map[string]string{
				"playerId": string(g.penaltyTarget),
			}),
		)
	case FaceSkip:
		g.active = card.Color
		skipped := g.nextIndex(seat)
		g.current = g.nextIndex(skipped)
		*facts = append(*facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.seats[g.current]),
		}))
	case FaceReverse:
		g.active = card.Color
		g.dir = -g.dir
		if len(g.seats) == 2 {
			g.current = seat
		} else {
			g.current = g.nextIndex(seat)
		}
		*facts = append(*facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.seats[g.current]),
		}))
	default:
		g.active = card.Color
		g.current = g.nextIndex(seat)
		*facts = append(*facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.seats[g.current]),
		}))
	}
}

// DrawCard draws an injected authoritative batch (ordinary or full penalty).
func (g *Game) DrawCard(cmd DrawCardCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if _, ok := g.seatIndex(cmd.PlayerID); !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "not seated")
	}
	if g.pendingColor {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectPendingColor, "color choice required")
	}

	facts := []Fact{}

	if g.penaltyAmount > 0 {
		if cmd.PlayerID != g.penaltyTarget {
			return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotPenaltyTarget, "only target may resolve penalty")
		}
		if len(cmd.Cards) != g.penaltyAmount {
			return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDrawBatchMismatch,
				"need "+strconv.Itoa(g.penaltyAmount)+" cards")
		}
		g.maybeCloseUnoOnTurnBegin(cmd.PlayerID, &facts)
		g.hands[cmd.PlayerID] = append(g.hands[cmd.PlayerID], cloneHand(cmd.Cards)...)
		amount := g.penaltyAmount
		g.penaltyAmount = 0
		g.penaltyTarget = ""
		facts = append(facts,
			newFact(FactCardDrawn, map[string]string{
				"playerId": string(cmd.PlayerID),
				"count":    strconv.Itoa(len(cmd.Cards)),
			}),
			newFact(FactPenaltyStackResolved, map[string]string{
				"playerId": string(cmd.PlayerID),
				"amount":   strconv.Itoa(amount),
			}),
		)
		seat, _ := g.seatIndex(cmd.PlayerID)
		g.current = g.nextIndex(seat)
		facts = append(facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.seats[g.current]),
		}))
		return g.commit(cmd.CommandID, facts)
	}

	if cmd.PlayerID != g.seats[g.current] {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectOutOfTurn, "not your turn")
	}
	if len(cmd.Cards) == 0 {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDrawBatchMismatch, "empty draw batch")
	}
	g.maybeCloseUnoOnTurnBegin(cmd.PlayerID, &facts)
	g.hands[cmd.PlayerID] = append(g.hands[cmd.PlayerID], cloneHand(cmd.Cards)...)
	facts = append(facts, newFact(FactCardDrawn, map[string]string{
		"playerId": string(cmd.PlayerID),
		"count":    strconv.Itoa(len(cmd.Cards)),
	}))

	retain := false
	for _, c := range cmd.Cards {
		if c.Face == FaceWildDrawFour {
			if wildDrawFourLegal(g.hands[cmd.PlayerID], g.active, c.ID) {
				retain = true
				break
			}
		} else if canPlayOrdinary(c, g.discard, g.active) {
			retain = true
			break
		}
	}
	if retain {
		facts = append(facts, newFact(FactDrawTurnRetained, map[string]string{
			"playerId": string(cmd.PlayerID),
		}))
	} else {
		seat, _ := g.seatIndex(cmd.PlayerID)
		g.current = g.nextIndex(seat)
		facts = append(facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.seats[g.current]),
		}))
	}
	return g.commit(cmd.CommandID, facts)
}

// ChooseColor resolves a pending wild color.
func (g *Game) ChooseColor(cmd ChooseColorCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if !g.pendingColor {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectColorNotPending, "no pending color")
	}
	if cmd.PlayerID != g.colorChooser {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectOutOfTurn, "not color chooser")
	}
	if !validPlayableColor(cmd.Color) {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidColor, "invalid color")
	}
	g.active = cmd.Color
	g.pendingColor = false
	chooser := g.colorChooser
	g.colorChooser = ""
	facts := []Fact{newFact(FactColorChosen, map[string]string{
		"playerId": string(cmd.PlayerID),
		"color":    string(cmd.Color),
	})}

	seat, _ := g.seatIndex(chooser)
	if g.penaltyAmount > 0 {
		idx, _ := g.seatIndex(g.penaltyTarget)
		g.current = idx
		facts = append(facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.penaltyTarget),
		}))
	} else {
		g.current = g.nextIndex(seat)
		facts = append(facts, newFact(FactTurnAdvanced, map[string]string{
			"playerId": string(g.seats[g.current]),
		}))
	}
	return g.commit(cmd.CommandID, facts)
}

// CallUno records a timely Uno call and resolves the missing-Uno challenge window.
func (g *Game) CallUno(cmd CallUnoCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if g.uno == nil || !g.uno.IsOpen() {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "no open window")
	}
	if g.uno.PlayerID != cmd.PlayerID {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowMismatch, "not window owner")
	}
	if !g.uno.IsTimely(cmd.NowUTC) {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "window not timely")
	}
	if g.uno.HasCalled() {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoAlreadyCalled, "already called")
	}
	called := g.uno.MarkCalled()
	closed := called.CloseResolved()
	g.uno = &closed
	return g.commit(cmd.CommandID, []Fact{newFact(FactUnoCalled, map[string]string{
		"playerId": string(cmd.PlayerID),
	})})
}

// ReportMissingUno challenges a missing Uno call while the window is open; injects 2 cards on the target.
// After CallUno (or other closure), the command is rejected inactive with no facts and no challenger penalty.
func (g *Game) ReportMissingUno(cmd ReportMissingUnoCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if _, ok := g.seatIndex(cmd.ChallengerID); !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "challenger not seated")
	}
	if _, ok := g.seatIndex(cmd.TargetID); !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "target not seated")
	}
	if g.uno == nil || !g.uno.IsOpen() {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "no open window")
	}
	if g.uno.PlayerID != cmd.TargetID {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowMismatch, "wrong target")
	}
	if !g.uno.IsTimely(cmd.NowUTC) {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "window not timely")
	}
	if len(cmd.Cards) != 2 {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectDrawBatchMismatch, "need 2 cards")
	}

	facts := []Fact{newFact(FactUnoChallengeIssued, map[string]string{
		"challengerPlayerId": string(cmd.ChallengerID),
		"targetPlayerId":     string(cmd.TargetID),
	})}
	g.hands[cmd.TargetID] = append(g.hands[cmd.TargetID], cloneHand(cmd.Cards)...)
	closed := g.uno.CloseResolved()
	g.uno = &closed
	facts = append(facts, newFact(FactUnoPenaltyApplied, map[string]string{
		"targetPlayerId":     string(cmd.TargetID),
		"challengerPlayerId": string(cmd.ChallengerID),
		"cardsDrawn":         "2",
	}))
	return g.commit(cmd.CommandID, facts)
}

// SkipTurn advances past a disconnected player's turn with no bot substitution.
// Pending color choice is cleared without changing active color; an unresolved
// penalty targeting the skipped player is forfeited (cleared without drawing).
func (g *Game) SkipTurn(cmd SkipTurnCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if _, ok := g.seatIndex(cmd.PlayerID); !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectInvalidIdentity, "not seated")
	}
	if cmd.PlayerID != g.CurrentPlayer() {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectNotCurrentPlayer, "not current player")
	}

	facts := []Fact{}
	seat, _ := g.seatIndex(cmd.PlayerID)

	if g.pendingColor {
		g.pendingColor = false
		g.colorChooser = ""
	}
	if g.penaltyAmount > 0 && g.penaltyTarget == cmd.PlayerID {
		g.penaltyAmount = 0
		g.penaltyTarget = ""
	}

	g.current = g.nextIndex(seat)
	next := g.seats[g.current]
	g.maybeCloseUnoOnTurnBegin(next, &facts)
	facts = append(facts,
		newFact(FactTurnSkipped, map[string]string{"playerId": string(cmd.PlayerID)}),
		newFact(FactTurnAdvanced, map[string]string{"playerId": string(next)}),
	)
	return g.commit(cmd.CommandID, facts)
}

// ForfeitPlayer removes a player from the turn/hand ring so remaining players continue.
// If fewer than two players remain, the game completes as abandoned with the sole
// remaining player (if any) as winner.
func (g *Game) ForfeitPlayer(cmd ForfeitPlayerCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	idx, ok := g.seatIndex(cmd.PlayerID)
	if !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectPlayerNotActive, "player not in ring")
	}

	facts := []Fact{newFact(FactPlayerRemoved, map[string]string{
		"playerId": string(cmd.PlayerID),
		"reason":   "forfeit",
	})}

	wasActing := g.CurrentPlayer() == cmd.PlayerID
	if g.colorChooser == cmd.PlayerID {
		g.pendingColor = false
		g.colorChooser = ""
	}
	if g.penaltyTarget == cmd.PlayerID {
		g.penaltyAmount = 0
		g.penaltyTarget = ""
	}
	if g.uno != nil && g.uno.IsOpen() && g.uno.PlayerID == cmd.PlayerID {
		closed := g.uno.CloseResolved()
		g.uno = &closed
		facts = append(facts, newFact(FactUnoWindowClosed, map[string]string{
			"playerId": string(cmd.PlayerID),
			"reason":   "forfeit",
		}))
	}

	delete(g.hands, cmd.PlayerID)
	g.seats = append(g.seats[:idx], g.seats[idx+1:]...)
	if len(g.seats) == 0 {
		g.completed = true
		return g.commit(cmd.CommandID, facts)
	}
	if idx < g.current {
		g.current--
	} else if idx == g.current {
		if g.current >= len(g.seats) {
			g.current = 0
		}
	}
	if g.current >= len(g.seats) {
		g.current = 0
	}

	if len(g.seats) < 2 {
		winner := g.seats[0]
		g.abandoned = true
		g.finish(winner, &facts)
		facts[len(facts)-1].Data["isAbandoned"] = "true"
		facts[len(facts)-1].Data["completionReason"] = "forfeit_abandonment"
		return g.commit(cmd.CommandID, facts)
	}

	if wasActing {
		next := g.seats[g.current]
		g.maybeCloseUnoOnTurnBegin(next, &facts)
		facts = append(facts,
			newFact(FactTurnSkipped, map[string]string{"playerId": string(cmd.PlayerID)}),
			newFact(FactTurnAdvanced, map[string]string{"playerId": string(next)}),
		)
	}
	return g.commit(cmd.CommandID, facts)
}

// ExpireUnoWindow closes the window after absolute UTC deadline.
func (g *Game) ExpireUnoWindow(cmd ExpireUnoWindowCommand) CommandOutcome {
	if out, done := g.checkSequence(cmd.CommandID, cmd.ExpectedSequence); done {
		return out
	}
	if g.uno == nil {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, RejectUnoWindowInactive, "no window")
	}
	next, ok, code := g.uno.Expire(cmd.NowUTC, cmd.PlayerID, cmd.OpeningSequence)
	if !ok {
		return g.reject(cmd.CommandID, cmd.ExpectedSequence, code, "")
	}
	g.uno = &next
	return g.commit(cmd.CommandID, []Fact{newFact(FactUnoWindowExpired, map[string]string{
		"playerId": string(cmd.PlayerID),
	})})
}

func (g *Game) finish(winner PlayerID, facts *[]Fact) {
	g.completed = true
	g.penaltyAmount = 0
	g.penaltyTarget = ""
	g.pendingColor = false
	if g.uno != nil {
		closed := g.uno.CloseResolved()
		g.uno = &closed
	}

	type scored struct {
		id     PlayerID
		points int
		seat   int
	}
	others := make([]scored, 0, len(g.seats)-1)
	g.cardPoints = make(map[PlayerID]int, len(g.seats))
	for i, p := range g.seats {
		pts := HandPoints(g.hands[p])
		g.cardPoints[p] = pts
		if p != winner {
			others = append(others, scored{id: p, points: pts, seat: i})
		}
	}
	for i := 0; i < len(others); i++ {
		for j := i + 1; j < len(others); j++ {
			if others[j].points < others[i].points ||
				(others[j].points == others[i].points && others[j].seat < others[i].seat) {
				others[i], others[j] = others[j], others[i]
			}
		}
	}
	g.placement = make([]PlayerID, 0, len(g.seats))
	g.placement = append(g.placement, winner)
	for _, o := range others {
		g.placement = append(g.placement, o.id)
	}
	*facts = append(*facts, newFact(FactGameCompleted, map[string]string{
		"winner": string(winner),
	}))
}
