package game

import "time"

const UnoWindowDuration = 5 * time.Second

// UnoWindow is a server-authoritative challenge window.
// Openness uses absolute UTC expiresAt, opening sequence, and next-turn closure.
type UnoWindow struct {
	PlayerID         PlayerID
	ExpiresAt        time.Time
	OpeningSequence  SequenceNumber
	Called           bool
	open             bool
	closedByNextTurn bool
}

func OpenUnoWindow(playerID PlayerID, openedAtUTC time.Time, openingSequence SequenceNumber) UnoWindow {
	openedAtUTC = openedAtUTC.UTC()
	return UnoWindow{
		PlayerID:        playerID,
		ExpiresAt:       openedAtUTC.Add(UnoWindowDuration),
		OpeningSequence: openingSequence,
		open:            true,
	}
}

func (w UnoWindow) IsOpen() bool    { return w.open }
func (w UnoWindow) HasCalled() bool { return w.Called }

func (w UnoWindow) IsTimely(nowUTC time.Time) bool {
	if !w.open || w.closedByNextTurn {
		return false
	}
	return nowUTC.UTC().Before(w.ExpiresAt)
}

func (w UnoWindow) Matches(playerID PlayerID, openingSequence SequenceNumber) bool {
	return w.PlayerID == playerID && w.OpeningSequence == openingSequence
}

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

func (w UnoWindow) MarkCalled() UnoWindow {
	w.Called = true
	return w
}

func (w UnoWindow) Expire(nowUTC time.Time, playerID PlayerID, openingSequence SequenceNumber) (UnoWindow, bool, RejectionCode) {
	if !w.Matches(playerID, openingSequence) {
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
