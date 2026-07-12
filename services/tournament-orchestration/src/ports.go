package main

import (
	"context"
	"errors"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
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
	// ProjectionChanged is set when the accepted domain outcome changed BracketPage-visible
	// state (see projectionChangedFromOutcome). Hidden batch-status-only facts leave
	// projectionVersion/generatedAt stable.
	ProjectionChanged bool
}

// MatchResultSource carries the inbound MatchCompleted event id for the matching room/version.
type MatchResultSource struct {
	EventID           string
	RoomID            string
	CompletionVersion uint64
}

// RoundMatchCommitRequest is one atomic differential MatchCompleted persistence unit.
// Never carries a full Tournament aggregate or triggers subtree DELETE.
type RoundMatchCommitRequest struct {
	TournamentID      string
	CommandID         string
	CommandType       string
	Outcome           envelope.Result
	Events            []OutboxEvent
	Decision          domain.RoundMatchDecision
	Command           domain.RecordMatchResultCommand
	MatchResultSource *MatchResultSource
	ProjectionChanged bool
}

// RoundMatchUnitOfWork is the bounded Round/slot differential transaction.
type RoundMatchUnitOfWork interface {
	Loaded() domain.RoundMatchContext
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	AttachPriorResult(roomID string, completionVersion uint64) error
	Commit(req RoundMatchCommitRequest) error
	Rollback() error
}

// RoundMatchRepository is the durable hot-path port for Kafka MatchCompleted and
// HTTP RecordMatchResult. Implementations must never call BeginExisting /
// loadTournamentQ / persistTournamentTx.
type RoundMatchRepository interface {
	// BeginRoundMatch locks barrier → command (when commandID nonempty) → assignment → shard/slot.
	// AttachPriorResult then takes business key → exact result/ledger.
	BeginRoundMatch(ctx context.Context, tournamentID, roomID string, hintRound int, hintSlot string, commandID string) (RoundMatchUnitOfWork, error)
	// BeginStandaloneCommand locks only the global command id (invalid/outcome-only rejects).
	BeginStandaloneCommand(ctx context.Context, commandID string) (RoundMatchUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
}

// RegistrationCommitRequest is one atomic T-Reg persistence unit (create/register/close).
type RegistrationCommitRequest struct {
	Op                 string
	TournamentID       string
	CommandID          string
	CommandType        string
	Outcome            envelope.Result
	Events             []OutboxEvent
	Decision           domain.RegistrationDecision
	CreateCmd          domain.CreateTournamentCommand
	RetryBudget        int
	BatchSize          int
	BumpBaseProjection bool
}

// RegistrationUnitOfWork is the bounded registration differential transaction.
type RegistrationUnitOfWork interface {
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	RegisterContext() domain.RegistrationContext
	CloseContext() domain.CloseRegistrationContext
	CreateContext() domain.CreateTournamentContext
	ReserveRegistration() (allocatedShard int, autoClosed bool, err error)
	FinalizeRegister(req RegistrationCommitRequest) error
	Commit(req RegistrationCommitRequest) error
	Rollback() error
}

// RegistrationRepository is the durable hot-path port for Create/Register/Close.
// Implementations must never call BeginExisting / BeginCreate / loadTournamentQ / persistTournamentTx.
// Begin* acquire locks in deadlock-safe order: rewrite barrier (when tid exists) →
// global command lock → per-player lock (register) → rows. Standalone is command lock only.
type RegistrationRepository interface {
	BeginCreateTournament(ctx context.Context, tournamentID, commandID string) (RegistrationUnitOfWork, error)
	BeginRegisterPlayer(ctx context.Context, tournamentID, playerID, commandID string) (RegistrationUnitOfWork, error)
	BeginCloseRegistration(ctx context.Context, tournamentID, commandID string) (RegistrationUnitOfWork, error)
	BeginStandaloneCommand(ctx context.Context, commandID string) (RegistrationUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
}

// SeedingCommitRequest is one atomic SeedRound kickoff persistence unit.
type SeedingCommitRequest struct {
	TournamentID  string
	CommandID     string
	CommandType   string
	CorrelationID string
	Outcome       envelope.Result
	Decision      domain.SeedRoundKickoffDecision
}

// SeedingUnitOfWork is the bounded SeedRound kickoff transaction.
type SeedingUnitOfWork interface {
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	KickoffContext() domain.SeedRoundKickoffContext
	Commit(req SeedingCommitRequest) error
	Rollback() error
}

// SeedingRepository is the durable hot-path port for SeedRound kickoff (any round).
// Implementations must never call BeginExisting / loadTournamentQ / persistTournamentTx.
// Kickoff lock order: exclusive rewrite barrier → global command lock → tournament row → round/job.
type SeedingRepository interface {
	BeginSeedRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (SeedingUnitOfWork, error)
	BeginSeedRound1(ctx context.Context, tournamentID, commandID string) (SeedingUnitOfWork, error)
	BeginStandaloneCommand(ctx context.Context, commandID string) (SeedingUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
}

// CompleteRoundCommitRequest is one atomic differential CompleteRound persistence unit.
type CompleteRoundCommitRequest struct {
	TournamentID  string
	CommandID     string
	CommandType   string
	CorrelationID string
	Outcome       envelope.Result
	Events        []OutboxEvent
	Decision      domain.CompleteRoundDecision
}

// CompleteRoundUnitOfWork is the bounded CompleteRound differential transaction.
type CompleteRoundUnitOfWork interface {
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	Loaded() domain.CompleteRoundContext
	Commit(req CompleteRoundCommitRequest) error
	Rollback() error
}

// CompleteRoundRepository is the durable hot-path port for CompleteRound.
// Implementations must never call BeginExisting / loadTournamentQ / persistTournamentTx /
// Service.mu / whole hydrate / subtree DELETE.
// Lock order: shared rewrite barrier → global command lock → 64 shards → batches → round.
type CompleteRoundRepository interface {
	BeginCompleteRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (CompleteRoundUnitOfWork, error)
	BeginStandaloneCommand(ctx context.Context, commandID string) (CompleteRoundUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
	FindReadyRoundCandidate(ctx context.Context) (*store.ReadyRoundCandidate, error)
}

// LifecycleCommitRequest is one atomic differential CompleteTournament / CancelTournament unit.
type LifecycleCommitRequest struct {
	TournamentID  string
	CommandID     string
	CommandType   string
	CorrelationID string
	Outcome       envelope.Result
	Events        []OutboxEvent
	Complete      domain.CompleteTournamentDecision
	Cancel        domain.CancelTournamentDecision
}

// LifecycleUnitOfWork is the bounded Complete/Cancel differential transaction.
type LifecycleUnitOfWork interface {
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	CompleteContext() domain.CompleteTournamentContext
	CancelContext() domain.CancelTournamentContext
	Commit(req LifecycleCommitRequest) error
	Rollback() error
}

// LifecycleRepository is the durable hot-path port for CompleteTournament and CancelTournament.
// Implementations must never call BeginExisting / loadTournamentQ / persistTournamentTx /
// Service.mu / whole hydrate / subtree DELETE / unbounded round/slot/player scans.
// Lock order: exclusive rewrite barrier → global command lock → tournament FOR UPDATE
// → (Complete only) current final round + at most two bracket slots (LIMIT 2) and the
// joined recorded advancement/result for the unique slot_index=0 final room.
type LifecycleRepository interface {
	BeginCompleteTournament(ctx context.Context, tournamentID, commandID string) (LifecycleUnitOfWork, error)
	BeginCancelTournament(ctx context.Context, tournamentID, commandID string) (LifecycleUnitOfWork, error)
	BeginStandaloneCommand(ctx context.Context, commandID string) (LifecycleUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
}

// QuarantineResultCommitRequest is one atomic differential QuarantineTournamentResult unit.
type QuarantineResultCommitRequest struct {
	TournamentID      string
	CommandID         string
	CommandType       string
	CorrelationID     string
	Outcome           envelope.Result
	Decision          domain.QuarantineTournamentResultDecision
	Command           domain.QuarantineTournamentResultCommand
	ProjectionChanged bool
}

// QuarantineResultUnitOfWork is the bounded QuarantineTournamentResult differential transaction.
type QuarantineResultUnitOfWork interface {
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	Loaded() domain.QuarantineTournamentResultContext
	Commit(req QuarantineResultCommitRequest) error
	Rollback() error
}

// QuarantineResultRepository is the durable hot-path port for QuarantineTournamentResult.
// Implementations must never call BeginExisting / loadTournamentQ / persistTournamentTx /
// Service.mu / whole hydrate / subtree DELETE / slot/counter/advancement mutation / outbox.
// Lock order (known room): shared rewrite barrier → global command lock → exact slot
// → business-key advisory (roomId, completionVersion) → exact match_results / ledger.
// Unknown room: shared → command → business → exact result/ledger.
// Plain tournaments existence SELECT only (never tournaments FOR UPDATE).
type QuarantineResultRepository interface {
	BeginQuarantineTournamentResult(ctx context.Context, tournamentID, roomID string, completionVersion uint64, commandID string) (QuarantineResultUnitOfWork, error)
	BeginStandaloneCommand(ctx context.Context, commandID string) (QuarantineResultUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
}

// ProvisioningCommitRequest is one atomic ProvisionRoundMatches kickoff persistence unit.
type ProvisioningCommitRequest struct {
	TournamentID  string
	CommandID     string
	CommandType   string
	CorrelationID string
	Outcome       envelope.Result
	Decision      domain.ProvisionKickoffDecision
	RoundNumber   int
}

// ProvisioningUnitOfWork is the bounded ProvisionRoundMatches kickoff transaction.
type ProvisioningUnitOfWork interface {
	Exists() bool
	LookupOutcome(commandID string) (envelope.Result, bool)
	KickoffContext() domain.ProvisionKickoffContext
	Commit(req ProvisioningCommitRequest) error
	Rollback() error
}

// ProvisioningRepository is the durable hot-path port for ProvisionRoundMatches + ProcessProvisioningBatch.
// Implementations must never call BeginExisting / loadTournamentQ / persistTournamentTx.
type ProvisioningRepository interface {
	BeginProvisionRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (ProvisioningUnitOfWork, error)
	BeginStandaloneCommand(ctx context.Context, commandID string) (ProvisioningUnitOfWork, error)
	LookupOutcome(commandID string) (envelope.Result, bool)
	PrepareProvisioningBatch(ctx context.Context, in store.PrepareProvisioningBatchInput) (*store.PrepareProvisioningBatchResult, error)
	FinalizeProvisioningBatchSuccess(ctx context.Context, in store.FinalizeProvisioningBatchInput) (envelope.Result, error)
	FinalizeProvisioningBatchFailure(ctx context.Context, in store.FinalizeProvisioningBatchInput) (envelope.Result, error)
	HeartbeatProvisioningLease(ctx context.Context, tid string, roundNumber int, batchID, owner string, leaseVersion int64, retryAttempt int, now time.Time, ttl time.Duration) (bool, error)
	ManualRetryProvisioningBatch(ctx context.Context, commandID, tid string, roundNumber int, batchID string, retryAttempt int, correlationID string) (envelope.Result, error)
	ManualQuarantineProvisioningBatch(ctx context.Context, commandID, tid string, roundNumber int, batchID, reason, correlationID string) (envelope.Result, error)
	LoadRetryBudget(ctx context.Context, tournamentID string) (int, error)
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

// RoomProvisionResult is the authoritative Room response after a successful provision.
type RoomProvisionResult struct {
	RoomID string
}

// ErrRoomIDMismatch is returned when Room accepts but returns a different roomId than requested.
// This is an assignment conflict that must quarantine (not ordinary retry).
var ErrRoomIDMismatch = errors.New("room_assignment_conflict")

// RoomProvisioner creates tournament rooms in Room Gameplay (port; fake offline).
type RoomProvisioner interface {
	Provision(ctx context.Context, req RoomProvisionRequest) (RoomProvisionResult, error)
}

// OutboxEvent is a durable pending tournament fact for async publication.
type OutboxEvent struct {
	EventID       string
	EventType     string
	TournamentID  string
	Topic         string
	PartitionKey  string
	SchemaVersion int
	Payload       map[string]any
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
	CommandID      string
	TournamentID   string
	RoundNumber    int
	BatchID        string
	SlotFrom       string
	SlotTo         string
	SlotSize       int
	RetryAttempt   int
	CorrelationID  string
	LeaseOwner     string
	LeaseExpiresAt time.Time
	LeaseVersion   int64
}
