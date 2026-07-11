package domain

import (
	"bytes"
	"errors"
)

// GameLogEntry is an immutable append-only log record.
// Identity is its chronological LogOffset.
// Revision is not stored on the entry; it is log length / CommandOutcome.Revision.
// GameID is optional metadata (empty for lifecycle-before-game appends).
type GameLogEntry struct {
	Offset    LogOffset
	EventID   EventID
	EventType string
	GameID    GameID
	Payload   []byte
}

// DeterministicStateInput is a full export suitable for deterministic recovery.
// GameID on the export envelope is optional metadata (often empty for room-scoped logs).
type DeterministicStateInput struct {
	RoomID   RoomID
	GameID   GameID
	Revision Revision
	Entries  []GameLogEntry
}

// GameLog is the append-only event history for one room.
// Revision is authoritative and cumulative across games in the room (best-of-three).
// Pure stdlib; not goroutine-safe — callers serialize access.
type GameLog struct {
	roomID RoomID

	entries []GameLogEntry
	byEvent map[EventID]eventRecord
}

type eventRecord struct {
	eventType string
	gameID    GameID
	payload   []byte
	outcome   CommandOutcome
}

// NewGameLog creates an empty append-only log for roomID.
func NewGameLog(roomID RoomID) (*GameLog, error) {
	if !roomID.Valid() {
		return nil, errors.New("roomId required")
	}
	return &GameLog{
		roomID:  roomID,
		entries: nil,
		byEvent: map[EventID]eventRecord{},
	}, nil
}

// RestoreGameLog rebuilds a GameLog from previously accepted entries.
// Offsets must be gapless from 0, event IDs unique, and roomID valid.
// Idempotency outcomes are reconstructed so duplicate/conflict semantics match a live log.
func RestoreGameLog(roomID RoomID, entries []GameLogEntry) (*GameLog, error) {
	if !roomID.Valid() {
		return nil, errors.New("roomId required")
	}
	g := &GameLog{
		roomID:  roomID,
		entries: make([]GameLogEntry, 0, len(entries)),
		byEvent: map[EventID]eventRecord{},
	}
	for i, e := range entries {
		if e.Offset != LogOffset(i) {
			return nil, errors.New("game log offsets must be gapless from zero")
		}
		if !e.EventID.Valid() {
			return nil, errors.New("game log entry requires eventId")
		}
		if _, exists := g.byEvent[e.EventID]; exists {
			return nil, errors.New("game log eventIds must be unique")
		}
		payload := append([]byte(nil), e.Payload...)
		entry := GameLogEntry{
			Offset:    e.Offset,
			EventID:   e.EventID,
			EventType: e.EventType,
			GameID:    e.GameID,
			Payload:   payload,
		}
		g.entries = append(g.entries, entry)
		fact := logAppendedFact(e.EventID, e.Offset, e.EventType)
		rev := g.Revision()
		g.byEvent[e.EventID] = eventRecord{
			eventType: e.EventType,
			gameID:    e.GameID,
			payload:   append([]byte(nil), payload...),
			outcome:   acceptedAppend([]Fact{fact}, e.Offset, rev),
		}
	}
	return g, nil
}

func (g *GameLog) RoomID() RoomID { return g.roomID }

// Revision is the number of successfully appended entries (next expected revision).
func (g *GameLog) Revision() Revision { return Revision(len(g.entries)) }

// Len returns the number of appended entries.
func (g *GameLog) Len() int { return len(g.entries) }

// Entries returns a defensive copy of all log entries.
func (g *GameLog) Entries() []GameLogEntry {
	return copyEntries(g.entries)
}

// Append appends one entry when ExpectedRevision matches the current revision.
// Same EventID with identical EventType+GameID+Payload returns the prior outcome.
// Same EventID with conflicting payload/type/gameId rejects without mutation.
func (g *GameLog) Append(cmd AppendCommand) CommandOutcome {
	if prior, ok := g.byEvent[cmd.EventID]; ok {
		if prior.eventType != cmd.EventType || prior.gameID != cmd.GameID || !bytes.Equal(prior.payload, cmd.Payload) {
			return rejectedOutcome(g.Revision(), Rejection{
				Code:              RejectConflictingDuplicate,
				Message:           "eventId reused with conflicting payload",
				SubmittedRevision: cmd.ExpectedRevision,
			})
		}
		return duplicateOutcome(prior.outcome)
	}

	if !cmd.EventID.Valid() {
		return rejectedOutcome(g.Revision(), Rejection{
			Code:              RejectInvalidIdentity,
			Message:           "append requires eventId",
			SubmittedRevision: cmd.ExpectedRevision,
		})
	}
	if cmd.EventType == "" {
		return rejectedOutcome(g.Revision(), Rejection{
			Code:              RejectInvalidCommand,
			Message:           "append requires eventType",
			SubmittedRevision: cmd.ExpectedRevision,
		})
	}
	if cmd.ExpectedRevision != g.Revision() {
		return rejectedOutcome(g.Revision(), Rejection{
			Code:              RejectRevisionMismatch,
			Message:           "expectedRevision does not match current revision",
			SubmittedRevision: cmd.ExpectedRevision,
		})
	}

	payload := append([]byte(nil), cmd.Payload...)
	offset := LogOffset(len(g.entries))
	entry := GameLogEntry{
		Offset:    offset,
		EventID:   cmd.EventID,
		EventType: cmd.EventType,
		GameID:    cmd.GameID,
		Payload:   payload,
	}
	g.entries = append(g.entries, entry)

	fact := logAppendedFact(cmd.EventID, offset, cmd.EventType)
	rev := g.Revision()
	g.byEvent[cmd.EventID] = eventRecord{
		eventType: cmd.EventType,
		gameID:    cmd.GameID,
		payload:   append([]byte(nil), payload...),
		outcome:   acceptedAppend([]Fact{fact}, offset, rev),
	}
	return acceptedAppend([]Fact{fact}, offset, rev)
}

// ReplayFrom returns defensive copies of entries with Offset >= from.
// Offsets past the end of the log (from > Len) return RejectInvalidOffset.
func (g *GameLog) ReplayFrom(from LogOffset) ([]GameLogEntry, *Rejection) {
	if int(from) > len(g.entries) {
		rej := Rejection{
			Code:    RejectInvalidOffset,
			Message: "replay offset past end of log",
		}
		return nil, &rej
	}
	if int(from) == len(g.entries) {
		return nil, nil
	}
	return copyEntries(g.entries[from:]), nil
}

// ExportStateInput returns a full deterministic recovery export with defensive copies.
func (g *GameLog) ExportStateInput() DeterministicStateInput {
	return DeterministicStateInput{
		RoomID:   g.roomID,
		Revision: g.Revision(),
		Entries:  copyEntries(g.entries),
	}
}

func copyEntries(in []GameLogEntry) []GameLogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]GameLogEntry, len(in))
	for i, e := range in {
		out[i] = GameLogEntry{
			Offset:    e.Offset,
			EventID:   e.EventID,
			EventType: e.EventType,
			GameID:    e.GameID,
			Payload:   append([]byte(nil), e.Payload...),
		}
	}
	return out
}
