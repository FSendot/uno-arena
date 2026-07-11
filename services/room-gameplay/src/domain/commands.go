package domain

import (
	"time"

	"unoarena/services/room-gameplay/game"
)

// Commands carry CommandID for idempotency. Sequence-bearing commands must
// present ExpectedSequence equal to the current room version.

type CreateRoomCommand struct {
	CommandID  CommandID
	RoomID     RoomID
	HostID     PlayerID
	Visibility Visibility
	MaxSeats   int
}

type ProvisionTournamentRoomCommand struct {
	CommandID    CommandID
	RoomID       RoomID
	TournamentID TournamentID
	RoundNumber  int
	SlotID       string
	HostID       PlayerID // seating authority placeholder; system may lock/start
	Visibility   Visibility
	MaxSeats     int
}

type JoinRoomCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	ExpectedSequence SequenceNumber
}

type LeaveRoomCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	ExpectedSequence SequenceNumber
}

type LockRoomCommand struct {
	CommandID        CommandID
	ActorID          PlayerID
	AsSystem         bool // tournament/system path bypasses host check
	ExpectedSequence SequenceNumber
}

type StartMatchCommand struct {
	CommandID        CommandID
	ActorID          PlayerID
	AsSystem         bool
	GameID           GameID
	ExpectedSequence SequenceNumber
}

type CancelRoomCommand struct {
	CommandID        CommandID
	ActorID          PlayerID
	AsSystem         bool
	ExpectedSequence SequenceNumber
}

type CompleteGameCommand struct {
	CommandID        CommandID
	GameID           GameID
	ExpectedSequence SequenceNumber
}

type CompleteMatchCommand struct {
	CommandID        CommandID
	ExpectedSequence SequenceNumber
}

type DisconnectPlayerCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	NowUTC           time.Time
	ExpectedSequence SequenceNumber
}

type ReconnectPlayerCommand struct {
	CommandID         CommandID
	PlayerID          PlayerID
	DisconnectVersion DisconnectVersion
	NowUTC            time.Time
	ExpectedSequence  SequenceNumber
}

type ForfeitPlayerCommand struct {
	CommandID         CommandID
	PlayerID          PlayerID
	DisconnectVersion DisconnectVersion
	NowUTC            time.Time
	ExpectedSequence  SequenceNumber
}

type OpenUnoWindowCommand struct {
	CommandID             CommandID
	PlayerID              PlayerID
	GameID                GameID
	TriggeringGameEventID TriggeringGameEventID
	NowUTC                time.Time
	ExpectedSequence      SequenceNumber
}

type ExpireUnoWindowCommand struct {
	CommandID             CommandID
	PlayerID              PlayerID
	GameID                GameID
	TriggeringGameEventID TriggeringGameEventID
	OpeningSequence       SequenceNumber
	NowUTC                time.Time
	ExpectedSequence      SequenceNumber
}

type CloseUnoWindowByNextTurnCommand struct {
	CommandID        CommandID
	ExpectedSequence SequenceNumber
}

// Gameplay commands are handled by Session; card legality lives in game.

type PlayCardCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	CardID           game.CardID
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
}

type DrawCardCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	Cards            []game.Card
	ExpectedSequence SequenceNumber
}

type ChooseColorCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	Color            game.Color
	ExpectedSequence SequenceNumber
}

type CallUnoCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
}

type ReportMissingUnoCommand struct {
	CommandID        CommandID
	ChallengerID     PlayerID
	TargetID         PlayerID
	Cards            []game.Card
	ExpectedSequence SequenceNumber
	NowUTC           time.Time
}

type StartNextGameCommand struct {
	CommandID        CommandID
	GameID           GameID
	ExpectedSequence SequenceNumber
	Deal             game.DealMaterial
}

type SkipDisconnectedTurnCommand struct {
	CommandID        CommandID
	PlayerID         PlayerID
	TurnVersion      SequenceNumber
	ExpectedSequence SequenceNumber
}
