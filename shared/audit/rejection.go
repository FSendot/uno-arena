// Package audit defines structured operational/security rejection records.
// These are not domain events and must never be appended to Game Integrity.
package audit

import (
	"fmt"
	"strings"
	"time"
)

// RejectionRecord is the structured operational/security audit record emitted
// for every rejected command. It is explicitly not a domain event.
type RejectionRecord struct {
	CommandID         string    `json:"commandId"`
	CorrelationID     string    `json:"correlationId"`
	SessionID         string    `json:"sessionId,omitempty"`
	PlayerID          string    `json:"playerId,omitempty"`
	RoomID            string    `json:"roomId,omitempty"`
	TournamentID      string    `json:"tournamentId,omitempty"`
	Reason            string    `json:"reason"`
	SubmittedSequence *int64    `json:"submittedSequenceNumber,omitempty"`
	CurrentSequence   *int64    `json:"currentSequenceNumber,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

// Validate checks required rejection-audit fields.
// sessionId/playerId may both be absent for pre-auth or unresolved-principal
// rejections; room/tournament identity is optional when unknown. Never fabricate
// identity fields — only commandId, correlationId, reason, and timestamp are required.
func (r RejectionRecord) Validate() error {
	if strings.TrimSpace(r.CommandID) == "" {
		return fmt.Errorf("commandId is required")
	}
	if strings.TrimSpace(r.CorrelationID) == "" {
		return fmt.Errorf("correlationId is required")
	}
	if strings.TrimSpace(r.Reason) == "" {
		return fmt.Errorf("reason is required")
	}
	if r.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}

// NewRejection builds a rejection audit record with UTC timestamp.
// sessionID/playerID may be empty for pre-auth or unresolved principals.
// Room/tournament and sequence fields remain optional and can be set on the result.
func NewRejection(commandID, correlationID, sessionID, playerID, reason string, at time.Time) RejectionRecord {
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	return RejectionRecord{
		CommandID:     commandID,
		CorrelationID: correlationID,
		SessionID:     sessionID,
		PlayerID:      playerID,
		Reason:        reason,
		Timestamp:     at,
	}
}

// WithRoom sets optional room identity.
func (r RejectionRecord) WithRoom(roomID string) RejectionRecord {
	r.RoomID = roomID
	return r
}

// WithTournament sets optional tournament identity.
func (r RejectionRecord) WithTournament(tournamentID string) RejectionRecord {
	r.TournamentID = tournamentID
	return r
}

// WithSequences sets optional submitted and current sequence numbers.
func (r RejectionRecord) WithSequences(submitted, current *int64) RejectionRecord {
	r.SubmittedSequence = submitted
	r.CurrentSequence = current
	return r
}
