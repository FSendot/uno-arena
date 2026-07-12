package game

import "time"

// PlayCardCommand plays one owned card as turn play, stack, or jump-in.
// NowUTC sets absolute Uno expiresAt when the play opens a window; zero uses Unix epoch.
type PlayCardCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	CardID           CardID
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
}

// DrawCardCommand draws an injected authoritative batch.
type DrawCardCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	Cards            []Card
	ExpectedSequence SequenceNumber
	// DrawPileSize is the authoritative remaining count after this draw confirms (from GI).
	DrawPileSize    int
	HasDrawPileSize bool
}

// ChooseColorCommand resolves a pending wild color choice.
type ChooseColorCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	Color            Color
	ExpectedSequence SequenceNumber
}

// CallUnoCommand acknowledges reaching one card inside an open Uno window.
type CallUnoCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
}

// ReportMissingUnoCommand challenges a missing Uno call (injects 2 cards).
type ReportMissingUnoCommand struct {
	CommandID        CommandID
	ChallengerID     PlayerID
	TargetID         PlayerID
	Cards            []Card
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
	// DrawPileSize is the authoritative remaining count after this penalty draw confirms (from GI).
	DrawPileSize    int
	HasDrawPileSize bool
}

// ExpireUnoWindowCommand closes an open Uno window after its absolute deadline.
type ExpireUnoWindowCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	OpeningSequence  SequenceNumber
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
}

// SkipTurnCommand advances past a disconnected player's turn with no bot play.
type SkipTurnCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	ExpectedSequence SequenceNumber
}

// ForfeitPlayerCommand removes a player from the active turn/hand ring.
type ForfeitPlayerCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	ExpectedSequence SequenceNumber
}
