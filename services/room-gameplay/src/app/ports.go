package app

import (
	"context"
	"encoding/json"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/audit"
)

// Clock provides the current UTC time for command handling.
type Clock interface {
	Now() time.Time
}

// CommitRequest is one atomic offline repository commit of session + outbox + bindings.
type CommitRequest struct {
	Session         *domain.Session
	Outbox          OutboxEntry
	ProvisionKey    *ProvisionKey
	ProvisionRoomID string
	PlayerSessionID string
	PlayerID        string
	// BindPlayerSession updates the room|player → session binding. System/timer
	// commands must leave existing player bindings untouched.
	BindPlayerSession    bool
	IntegrityRevision    int64
	SetIntegrityRevision bool
	StreamSeqHighWater   int64
	SetStreamSeq         bool
	// GlobalOutcome persists a command outcome outside a room aggregate
	// (invalid-session / pre-room rejections) for stable idempotent replay.
	GlobalOutcome *domain.CommandOutcome
	// PendingAudit atomically stores a rejection audit that must still be
	// delivered (keyed by record.CommandID). Cleared via MarkAuditComplete.
	PendingAudit *audit.RejectionRecord
}

// SessionRepository stores Session aggregates and owns the transactional outbox.
type SessionRepository interface {
	Get(roomID domain.RoomID) (*domain.Session, bool)
	// Commit atomically persists session state, pending outbox events, optional
	// provision idempotency, player-session bindings, integrity revision, and
	// player-feed publish sequence high-water (spectator uses room sequence).
	Commit(req CommitRequest) error
	Delete(roomID domain.RoomID) error
	ListPendingOutbox(limit int) ([]OutboxEntry, error)
	MarkOutboxEntryPublished(commandID string, publishedAt time.Time) error
	PlayerSession(roomID, playerID string) (sessionID string, ok bool)
	GetProvision(key ProvisionKey) (roomID string, ok bool)
	IntegrityRevision(roomID string) int64
	// PeekStreamSeq returns the last allocated player-feed sequence for the room.
	PeekStreamSeq(roomID string) int64
	// GetGlobalOutcome returns a prior pre-room / global command outcome.
	GetGlobalOutcome(commandID string) (domain.CommandOutcome, bool)
	// GetPendingAudit returns a rejection audit still awaiting sink delivery.
	GetPendingAudit(commandID string) (audit.RejectionRecord, bool)
	// MarkAuditComplete clears the pending audit after successful sink delivery.
	MarkAuditComplete(commandID string) error
}

// GameIntegrity is the append-before-commit port. Accepted room mutations must
// confirm append before the staged session is committed.
type GameIntegrity interface {
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
}

// AppendRequest is a technical log append request.
type AppendRequest struct {
	RoomID           string
	GameID           string
	EventID          string
	ExpectedRevision int64
	EventType        string
	Payload          []byte
}

// AppendResult is a durable append confirmation.
type AppendResult struct {
	LogOffset int64
	Revision  int64
}

// OutboxEntry is one committed outbox row (may contain multiple publish intents).
type OutboxEntry struct {
	RoomID    string
	CommandID string
	LogOffset int64
	Events    []PublishedEvent
}

// EventPublisher publishes one sanitized event (offline: HTTP to Gateway streams).
type EventPublisher interface {
	Publish(ctx context.Context, event PublishedEvent) error
}

// PublishedEvent is one Gateway stream ingest or AsyncAPI topic event.
type PublishedEvent struct {
	Topic          string          `json:"topic,omitempty"`
	Stream         string          `json:"stream,omitempty"` // player | spectator
	RoomID         string          `json:"roomId"`
	EventID        string          `json:"eventId"`
	EventType      string          `json:"eventType"`
	SequenceNumber int64           `json:"sequenceNumber"`
	SchemaVersion  int             `json:"schemaVersion"`
	CorrelationID  string          `json:"correlationId,omitempty"`
	CausationID    string          `json:"causationId,omitempty"`
	OccurredAt     string          `json:"occurredAt,omitempty"`
	PlayerID       string          `json:"playerId,omitempty"`
	SessionID      string          `json:"sessionId,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

// AuditSink records structured rejection audit records (never domain facts/GI).
type AuditSink interface {
	Record(ctx context.Context, rec audit.RejectionRecord) error
}

// MaterialReservation holds deal/draw material until confirm or cancel.
type MaterialReservation struct {
	ID    string
	Deal  *game.DealMaterial
	Cards []game.Card
}

// Stable operation purposes for DealSource operation IDs.
const (
	PurposeDeal       = "deal"
	PurposeDraw       = "draw"
	PurposeUnoPenalty = "uno-penalty"
)

// StableOperationID derives a deterministic GI operation id from a command id + purpose.
// It must not incorporate process counters.
func StableOperationID(commandID, purpose string) string {
	return commandID + ":" + purpose
}

// DealSource supplies authoritative deal/draw material (normally Game Integrity).
// Reserve does not permanently consume; Confirm commits consumption, Cancel releases.
// operationID must be stable across retries (typically StableOperationID(commandID, purpose)).
type DealSource interface {
	ReserveDeal(ctx context.Context, roomID, gameID, operationID string, seats []string) (MaterialReservation, error)
	ReserveDraw(ctx context.Context, roomID, gameID, operationID string, count int) (MaterialReservation, error)
	Confirm(ctx context.Context, reservationID string) error
	Cancel(ctx context.Context, reservationID string) error
}

// ProvisionKey is the idempotency key for tournament room provisioning.
type ProvisionKey struct {
	TournamentID string
	RoundNumber  int
	SlotID       string
}

// SessionValidator validates forwarded SessionID/PlayerID bindings (Identity port).
type SessionValidator interface {
	Validate(ctx context.Context, sessionID, playerID string) error
}
