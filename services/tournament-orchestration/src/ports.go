package main

import (
	"context"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

// Clock is injectable for deterministic tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator creates opaque identifiers for commands/events when callers omit them.
type IDGenerator interface {
	NewID(prefix string) string
}

// CommitRequest is one atomic persistence unit: aggregate + command outcome + pending outbox facts.
type CommitRequest struct {
	Tournament        *domain.Tournament
	CommandID         string
	Outcome           envelope.Result
	Events            []OutboxEvent
	MatchResultSource *MatchResultSource
}

// MatchResultSource carries the inbound MatchCompleted event id for the matching room/version.
type MatchResultSource struct {
	EventID           string
	RoomID            string
	CompletionVersion uint64
}

// TournamentUnitOfWork holds one READ COMMITTED transaction with the tournament row locked
// (FOR UPDATE) or create-path advisory lock across outcome lookup, domain mutation, and commit.
type TournamentUnitOfWork interface {
	Loaded() *domain.Tournament
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	Commit(req CommitRequest) error
	Rollback() error
}

// TournamentRepository persists Tournament aggregates with transactional outbox + command idempotency.
type TournamentRepository interface {
	// BeginExisting locks the tournament row (FOR UPDATE) then hydrates under that transaction.
	BeginExisting(id domain.TournamentID) (TournamentUnitOfWork, error)
	// BeginCreate takes a transaction-scoped advisory lock before checking/inserting.
	BeginCreate(id domain.TournamentID) (TournamentUnitOfWork, error)
	// Get returns a deep clone for read projections (unlocked snapshot).
	Get(id domain.TournamentID) (*domain.Tournament, bool)
	// Commit persists outcome-only rejects (no aggregate lock) or legacy single-shot writes.
	Commit(req CommitRequest) error
	LookupOutcome(commandID string) (envelope.Result, bool)
	ListPendingOutbox(limit int) ([]OutboxEvent, error)
	MarkOutboxPublished(eventID string, at time.Time) error
}

// RoomProvisionRequest is the idempotent Room Gameplay provision call shape.
type RoomProvisionRequest struct {
	TournamentID domain.TournamentID
	RoundNumber  int
	SlotID       domain.SlotID
	RoomID       domain.RoomID
	PlayerIDs    []domain.PlayerID
	BatchID      domain.BatchID
}

// RoomProvisioner creates tournament rooms in Room Gameplay (port; fake offline).
type RoomProvisioner interface {
	Provision(ctx context.Context, req RoomProvisionRequest) error
}

// OutboxEvent is a durable pending tournament fact for async publication.
type OutboxEvent struct {
	EventID       string
	EventType     string
	TournamentID  string
	Topic         string
	PartitionKey  string
	SchemaVersion int
	Payload       map[string]string
	CreatedAt     time.Time
	PublishedAt   *time.Time
}

// EventPublisher publishes pending outbox facts (offline: no-op or fault-injectable).
type EventPublisher interface {
	Publish(ctx context.Context, events ...OutboxEvent) error
}

// AuditSink records operational/security rejection audits (never domain events).
type AuditSink interface {
	RecordRejection(record audit.RejectionRecord) error
}

// MatchCompletedEvent is the consumed room.match.completed payload.
type MatchCompletedEvent struct {
	EventID           string
	EventType         string
	SchemaVersion     int
	CorrelationID     string
	CausationID       string
	OccurredAt        time.Time
	RoomID            string
	TournamentID      string
	RoundNumber       int
	SlotID            string
	CompletionVersion uint64
	IsAbandoned       bool
	HasIsAbandoned    bool // true when the JSON field was present
	Players           []MatchPlayerStanding
	Forfeits          []string
}

// MatchPlayerStanding is one ranked player fact from MatchCompleted.
type MatchPlayerStanding struct {
	PlayerID             string
	MatchWins            int
	CumulativeCardPoints int
	FinalGameCompletedAt time.Time
	Forfeited            bool
}

// ProvisioningBatchWork is the bounded worker admission for one shard.
// RetryAttempt is part of the lease/attempt identity together with BatchID: the same
// batch may be retried after a transient Room failure using the next attempt identity
// without short-circuiting on the prior attempt's accepted outcome.
type ProvisioningBatchWork struct {
	CommandID     string
	TournamentID  string
	RoundNumber   int
	BatchID       string
	SlotFrom      string
	SlotTo        string
	SlotSize      int
	RetryAttempt  int
	CorrelationID string
}
