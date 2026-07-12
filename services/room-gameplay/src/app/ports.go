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
// Capability mode uses MemorySessionRepository + Service.mu. Durable Postgres
// implements this for reads/rejections that do not need a held FOR UPDATE across GI;
// accepted mutations use RoomCommandStore / SessionUnitOfWork instead.
type SessionRepository interface {
	Get(ctx context.Context, roomID domain.RoomID) (*domain.Session, bool)
	// Commit atomically persists session state, pending outbox events, optional
	// provision idempotency, player-session bindings, integrity revision, and
	// player-feed publish sequence high-water (spectator uses room sequence).
	Commit(ctx context.Context, req CommitRequest) error
	Delete(ctx context.Context, roomID domain.RoomID) error
	ListPendingOutbox(ctx context.Context, limit int) ([]OutboxEntry, error)
	MarkOutboxEntryPublished(ctx context.Context, commandID string, publishedAt time.Time) error
	PlayerSession(ctx context.Context, roomID, playerID string) (sessionID string, ok bool)
	GetProvision(ctx context.Context, key ProvisionKey) (roomID string, ok bool)
	IntegrityRevision(ctx context.Context, roomID string) int64
	// PeekStreamSeq returns the last allocated player-feed sequence for the room.
	PeekStreamSeq(ctx context.Context, roomID string) int64
	// GetGlobalOutcome returns a prior pre-room / global command outcome.
	GetGlobalOutcome(ctx context.Context, commandID string) (domain.CommandOutcome, bool)
	// GetPendingAudit returns a rejection audit still awaiting sink delivery.
	GetPendingAudit(ctx context.Context, commandID string) (audit.RejectionRecord, bool)
	// MarkAuditComplete clears the pending audit after successful sink delivery.
	MarkAuditComplete(ctx context.Context, commandID string) error
}

// RoomCommandStore opens SessionUnitOfWork transactions for durable command handling.
// ADR-0019: existing rooms lock with SELECT ... FOR UPDATE; CreateRoom uses unique insert.
type RoomCommandStore interface {
	BeginExisting(ctx context.Context, roomID domain.RoomID) (SessionUnitOfWork, error)
	BeginCreate(ctx context.Context, roomID domain.RoomID) (SessionUnitOfWork, error)
	// BeginReconciliationIntent persists repair material in its own transaction BEFORE GI Append.
	// Keyed uniquely by commandId; retries reuse/inspect an existing pending intent.
	BeginReconciliationIntent(ctx context.Context, commandID, roomID string, expectedRevision int64, giPayload json.RawMessage, commit DurableAcceptedCommit) error
	// FinalizeReconciliationIntent records logOffset/revision after a confirmed GI append.
	FinalizeReconciliationIntent(ctx context.Context, commandID string, logOffset, revision int64) error
	// CancelReconciliationIntent marks an intent cancelled after a definitive pre-write append failure.
	CancelReconciliationIntent(ctx context.Context, commandID string) error
}

// SessionUnitOfWork holds a READ COMMITTED transaction with rooms FOR UPDATE
// (or create-path uniqueness) across domain apply + Game Integrity append + commit.
type SessionUnitOfWork interface {
	// Loaded returns the locked session snapshot (nil for create path before insert).
	Loaded() *domain.Session
	// Exists reports whether the room row was present when the UoW began.
	Exists() bool
	IntegrityRevision() int64
	PeekStreamSeq() int64
	PlayerSession(playerID string) (sessionID string, ok bool)
	GetGlobalOutcome(commandID string) (domain.CommandOutcome, bool)
	GetProvision(key ProvisionKey) (roomID string, ok bool)
	CommitAccepted(req DurableAcceptedCommit) error
	CommitRejected(req DurableRejectedCommit) error
	Rollback() error
}

// ReconciliationIntent is the pre-Append durable repair record (command_id unique).
type ReconciliationIntent struct {
	CommandID        string
	RoomID           string
	ExpectedRevision int64
	Payload          json.RawMessage
}

// ReconciliationMarker is a pending intent row loaded for autonomous repair.
// Payload is a JSON ReconciliationRepairBlob (session snapshot + outbox + commit fields).
type ReconciliationMarker struct {
	CommandID        string
	RoomID           string
	ExpectedRevision int64
	LogOffset        int64
	Revision         int64
	Payload          json.RawMessage
	HasLogOffset     bool
}

// ReconciliationRepairBlob is the durable marker payload for autonomous repair.
type ReconciliationRepairBlob struct {
	GIPayload          json.RawMessage `json:"giPayload"`
	SessionSnapshot    json.RawMessage `json:"sessionSnapshot"`
	Outbox             OutboxEntry     `json:"outbox"`
	CommandType        string          `json:"commandType"`
	Outcome            json.RawMessage `json:"outcome"`
	PlayerID           string          `json:"playerId,omitempty"`
	PlayerSessionID    string          `json:"playerSessionId,omitempty"`
	BindPlayerSession  bool            `json:"bindPlayerSession"`
	IntegrityRevision  int64           `json:"integrityRevision"`
	StreamSeqHighWater int64           `json:"streamSeqHighWater"`
	SetStreamSeq       bool            `json:"setStreamSeq"`
	CreatePath         bool            `json:"createPath"`
	ProvisionKey       *ProvisionKey   `json:"provisionKey,omitempty"`
	ProvisionRoomID    string          `json:"provisionRoomId,omitempty"`
	// Reservation outcome recovery (persisted before GI append).
	ReservationID     string `json:"reservationId,omitempty"`
	ReservationRoomID string `json:"reservationRoomId,omitempty"`
	ReservationGameID string `json:"reservationGameId,omitempty"`
	ReservationAction string `json:"reservationAction,omitempty"` // confirm | cancel
}

// Topic / event type for reconciliation fallback (integration outbox).
const (
	TopicRoomStateReconciled = "room.state.reconciled"
	EventRoomStateReconciled = "RoomStateReconciled"
)

// DurableAcceptedCommit is the atomic local write after GI append success.
type DurableAcceptedCommit struct {
	Session              *domain.Session
	Outbox               OutboxEntry
	ProvisionKey         *ProvisionKey
	ProvisionRoomID      string
	PlayerSessionID      string
	PlayerID             string
	BindPlayerSession    bool
	IntegrityRevision    int64
	SetIntegrityRevision bool
	StreamSeqHighWater   int64
	SetStreamSeq         bool
	LogOffset            int64
	CommandID            string
	CommandType          string
	Outcome              domain.CommandOutcome
	CreatePath           bool
	// Reservation outcome to perform after GI append (and on reconcile recovery).
	ReservationID     string
	ReservationRoomID string
	ReservationGameID string
	ReservationAction string // confirm | cancel | empty
}

// DurableRejectedCommit persists a stable rejection outcome + pending audit without GI/outbox.
type DurableRejectedCommit struct {
	Session       *domain.Session
	GlobalOutcome *domain.CommandOutcome
	PendingAudit  *audit.RejectionRecord
	CommandID     string
	CommandType   string
	PlayerID      string
}

// GameIntegrity is the append-before-commit port. Accepted room mutations must
// confirm append before the staged session is committed.
type GameIntegrity interface {
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	// Replay returns gapless log entries from fromOffset (inclusive) for reconciliation.
	Replay(ctx context.Context, roomID string, fromOffset int64) (ReplayResult, error)
}

// ReplayResult is a Game Integrity log slice used by Room reconciliation.
type ReplayResult struct {
	RoomID   string
	Revision int64
	Entries  []ReplayEntry
}

// ReplayEntry is one GI log entry.
type ReplayEntry struct {
	Offset    int64
	EventID   string
	EventType string
	GameID    string
	Payload   json.RawMessage
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
	// DrawPileSize is the authoritative remaining draw-pile count after this
	// reservation confirms (from Game Integrity). Never derived from client input.
	DrawPileSize int
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
// ConfirmAt/CancelAt take explicit room+game so reconciliation can act after restart
// without relying on in-memory meta. Confirm/Cancel remain convenience wrappers.
type DealSource interface {
	ReserveDeal(ctx context.Context, roomID, gameID, operationID string, seats []string) (MaterialReservation, error)
	ReserveDraw(ctx context.Context, roomID, gameID, operationID string, count int) (MaterialReservation, error)
	Confirm(ctx context.Context, reservationID string) error
	Cancel(ctx context.Context, reservationID string) error
	ConfirmAt(ctx context.Context, roomID, gameID, reservationID string) error
	CancelAt(ctx context.Context, roomID, gameID, reservationID string) error
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
