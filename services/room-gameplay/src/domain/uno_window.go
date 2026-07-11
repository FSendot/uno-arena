package domain

import "time"

const UnoWindowDuration = 5 * time.Second

// UnoWindow is a server-authoritative challenge window value object.
// Client countdown is advisory; openness and expiry are decided solely from
// absolute UTC expiresAt, opening sequence, and whether the next turn began.
type UnoWindow struct {
	PlayerID              PlayerID
	GameID                GameID
	TriggeringGameEventID TriggeringGameEventID
	ExpiresAt             time.Time // absolute UTC
	OpeningSequence       SequenceNumber
	open                  bool
	closedByNextTurn      bool
}

// OpenUnoWindow creates an open window with absolute UTC expiry.
func OpenUnoWindow(
	playerID PlayerID,
	gameID GameID,
	triggering TriggeringGameEventID,
	openedAtUTC time.Time,
	openingSequence SequenceNumber,
) UnoWindow {
	openedAtUTC = openedAtUTC.UTC()
	return UnoWindow{
		PlayerID:              playerID,
		GameID:                gameID,
		TriggeringGameEventID: triggering,
		ExpiresAt:             openedAtUTC.Add(UnoWindowDuration),
		OpeningSequence:       openingSequence,
		open:                  true,
	}
}

func (w UnoWindow) IsOpen() bool { return w.open }

// IsTimely reports whether nowUTC is strictly before expiresAt and the window is still open.
// At the exact expiresAt instant the window is no longer timely (boundary closed).
func (w UnoWindow) IsTimely(nowUTC time.Time) bool {
	if !w.open || w.closedByNextTurn {
		return false
	}
	return nowUTC.UTC().Before(w.ExpiresAt)
}

// Matches identifies the window by its durable key, including opening sequence
// so a stale timer for a superseded window is rejected.
func (w UnoWindow) Matches(gameID GameID, playerID PlayerID, triggering TriggeringGameEventID, openingSequence SequenceNumber) bool {
	return w.GameID == gameID &&
		w.PlayerID == playerID &&
		w.TriggeringGameEventID == triggering &&
		w.OpeningSequence == openingSequence
}

// CloseByNextTurn closes the window because the next player began their turn.
func (w UnoWindow) CloseByNextTurn() UnoWindow {
	w.open = false
	w.closedByNextTurn = true
	return w
}

// CloseResolved closes the window after CallUno or a challenge resolution.
func (w UnoWindow) CloseResolved() UnoWindow {
	w.open = false
	return w
}

// Expire closes the window because the absolute UTC deadline was reached.
// Returns false if the window is not open, mismatched, or not yet due.
func (w UnoWindow) Expire(nowUTC time.Time, gameID GameID, playerID PlayerID, triggering TriggeringGameEventID, openingSequence SequenceNumber) (UnoWindow, bool, RejectionCode) {
	if !w.Matches(gameID, playerID, triggering, openingSequence) {
		return w, false, RejectUnoWindowMismatch
	}
	if !w.open {
		return w, false, RejectUnoWindowInactive
	}
	if w.IsTimely(nowUTC) {
		return w, false, RejectUnoWindowNotExpired
	}
	w.open = false
	return w, true, ""
}
