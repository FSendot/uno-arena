package main

import (
	"context"
	"errors"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/envelope"
)

// ErrDurableWholeAggregateMutationDisabled is returned by production durableRepo
// BeginExisting / BeginCreate / Commit. Missing differential ports must fail
// explicitly rather than silently falling back to whole-aggregate rewrite.
var ErrDurableWholeAggregateMutationDisabled = errors.New("durable_whole_aggregate_mutation_disabled")

// durableRepo adapts store.TournamentStore to TournamentRepository (no ctx on ports).
// Mutation via BeginExisting / BeginCreate / Commit is fail-closed; durable writes
// go through differential ports only. Reads + CDC/outbox compatibility remain.
type durableRepo struct {
	store *store.TournamentStore
}

// durableRoundMatchRepo adapts store differential UoW to RoundMatchRepository.
type durableRoundMatchRepo struct {
	store *store.TournamentStore
}

// durableRegistrationRepo adapts store T-Reg UoW to RegistrationRepository.
type durableRegistrationRepo struct {
	store *store.TournamentStore
}

type durableRoundMatchUoW struct {
	inner *store.RoundMatchUnitOfWork
}

type durableRegistrationUoW struct {
	inner *store.RegistrationUnitOfWork
}

func (r *durableRepo) BeginExisting(domain.TournamentID) (TournamentUnitOfWork, error) {
	return nil, ErrDurableWholeAggregateMutationDisabled
}

func (r *durableRepo) BeginCreate(domain.TournamentID) (TournamentUnitOfWork, error) {
	return nil, ErrDurableWholeAggregateMutationDisabled
}

func (r *durableRepo) Get(id domain.TournamentID) (*domain.Tournament, bool) {
	return r.store.Get(context.Background(), id)
}

func (r *durableRepo) Commit(CommitRequest) error {
	return ErrDurableWholeAggregateMutationDisabled
}

func (r *durableRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupOutcome(context.Background(), commandID)
}

func (r *durableRepo) ListPendingOutbox(limit int) ([]OutboxEvent, error) {
	_, err := r.store.ListPendingOutbox(context.Background(), limit)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (r *durableRepo) MarkOutboxPublished(eventID string, at time.Time) error {
	return r.store.MarkOutboxPublished(context.Background(), eventID, at)
}

func (r *durableRoundMatchRepo) BeginRoundMatch(ctx context.Context, tournamentID, roomID string, hintRound int, hintSlot string, commandID string) (RoundMatchUnitOfWork, error) {
	uow, err := r.store.BeginRoundMatch(ctx, tournamentID, roomID, hintRound, hintSlot, commandID)
	if err != nil {
		return nil, err
	}
	return &durableRoundMatchUoW{inner: uow}, nil
}

func (r *durableRoundMatchRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (RoundMatchUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneRoundMatchCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableRoundMatchUoW{inner: uow}, nil
}

func (r *durableRoundMatchRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupOutcome(context.Background(), commandID)
}

func (u *durableRoundMatchUoW) Loaded() domain.RoundMatchContext { return u.inner.Loaded() }
func (u *durableRoundMatchUoW) Exists() bool                     { return u.inner.Exists() }

func (u *durableRoundMatchUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableRoundMatchUoW) AttachPriorResult(roomID string, completionVersion uint64) error {
	return u.inner.AttachPriorResult(roomID, completionVersion)
}

func (u *durableRoundMatchUoW) Commit(req RoundMatchCommitRequest) error {
	events := make([]store.OutboxEvent, 0, len(req.Events))
	for _, e := range req.Events {
		events = append(events, store.OutboxEvent{
			EventID:       e.EventID,
			EventType:     e.EventType,
			TournamentID:  e.TournamentID,
			Topic:         e.Topic,
			PartitionKey:  e.PartitionKey,
			SchemaVersion: e.SchemaVersion,
			Payload:       e.Payload,
			CreatedAt:     e.CreatedAt,
		})
	}
	storeReq := store.RoundMatchCommitRequest{
		TournamentID:      req.TournamentID,
		CommandID:         req.CommandID,
		CommandType:       req.CommandType,
		Outcome:           req.Outcome,
		Events:            events,
		Decision:          req.Decision,
		Command:           req.Command,
		ProjectionChanged: req.ProjectionChanged,
	}
	if req.MatchResultSource != nil {
		storeReq.MatchResultSource = &store.MatchResultSource{
			EventID:           req.MatchResultSource.EventID,
			RoomID:            req.MatchResultSource.RoomID,
			CompletionVersion: req.MatchResultSource.CompletionVersion,
		}
	}
	return u.inner.Commit(storeReq)
}

func (u *durableRoundMatchUoW) Rollback() error { return u.inner.Rollback() }

func (r *durableRegistrationRepo) BeginCreateTournament(ctx context.Context, tournamentID, commandID string) (RegistrationUnitOfWork, error) {
	uow, err := r.store.BeginCreateTournament(ctx, tournamentID, commandID)
	if err != nil {
		return nil, err
	}
	return &durableRegistrationUoW{inner: uow}, nil
}

func (r *durableRegistrationRepo) BeginRegisterPlayer(ctx context.Context, tournamentID, playerID, commandID string) (RegistrationUnitOfWork, error) {
	uow, err := r.store.BeginRegisterPlayer(ctx, tournamentID, playerID, commandID)
	if err != nil {
		return nil, err
	}
	return &durableRegistrationUoW{inner: uow}, nil
}

func (r *durableRegistrationRepo) BeginCloseRegistration(ctx context.Context, tournamentID, commandID string) (RegistrationUnitOfWork, error) {
	uow, err := r.store.BeginCloseRegistration(ctx, tournamentID, commandID)
	if err != nil {
		return nil, err
	}
	return &durableRegistrationUoW{inner: uow}, nil
}

func (r *durableRegistrationRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (RegistrationUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableRegistrationUoW{inner: uow}, nil
}

func (r *durableRegistrationRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupOutcome(context.Background(), commandID)
}

func (u *durableRegistrationUoW) Exists() bool { return u.inner.Exists() }

func (u *durableRegistrationUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableRegistrationUoW) RegisterContext() domain.RegistrationContext {
	return u.inner.RegisterContext()
}

func (u *durableRegistrationUoW) CloseContext() domain.CloseRegistrationContext {
	return u.inner.CloseContext()
}

func (u *durableRegistrationUoW) CreateContext() domain.CreateTournamentContext {
	return u.inner.CreateContext()
}

func (u *durableRegistrationUoW) ReserveRegistration() (int, bool, error) {
	return u.inner.ReserveRegistration()
}

func (u *durableRegistrationUoW) FinalizeRegister(req RegistrationCommitRequest) error {
	return u.inner.FinalizeRegister(toStoreRegistrationCommit(req))
}

func (u *durableRegistrationUoW) Commit(req RegistrationCommitRequest) error {
	return u.inner.Commit(toStoreRegistrationCommit(req))
}

func (u *durableRegistrationUoW) Rollback() error { return u.inner.Rollback() }

func toStoreRegistrationCommit(req RegistrationCommitRequest) store.RegistrationCommitRequest {
	events := make([]store.OutboxEvent, 0, len(req.Events))
	for _, e := range req.Events {
		events = append(events, store.OutboxEvent{
			EventID:       e.EventID,
			EventType:     e.EventType,
			TournamentID:  e.TournamentID,
			Topic:         e.Topic,
			PartitionKey:  e.PartitionKey,
			SchemaVersion: e.SchemaVersion,
			Payload:       e.Payload,
			CreatedAt:     e.CreatedAt,
		})
	}
	return store.RegistrationCommitRequest{
		Op:                 store.RegistrationOp(req.Op),
		TournamentID:       req.TournamentID,
		CommandID:          req.CommandID,
		CommandType:        req.CommandType,
		Outcome:            req.Outcome,
		Events:             events,
		Decision:           req.Decision,
		CreateCmd:          req.CreateCmd,
		RetryBudget:        req.RetryBudget,
		BatchSize:          req.BatchSize,
		BumpBaseProjection: req.BumpBaseProjection,
	}
}

// durableSeedingRepo adapts store seeding UoW to SeedingRepository.
type durableSeedingRepo struct {
	store *store.TournamentStore
}

type durableSeedingUoW struct {
	inner *store.SeedingUnitOfWork
}

func (r *durableSeedingRepo) BeginSeedRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (SeedingUnitOfWork, error) {
	uow, err := r.store.BeginSeedRound(ctx, tournamentID, roundNumber, commandID)
	if err != nil {
		return nil, err
	}
	return &durableSeedingUoW{inner: uow}, nil
}

func (r *durableSeedingRepo) BeginSeedRound1(ctx context.Context, tournamentID, commandID string) (SeedingUnitOfWork, error) {
	return r.BeginSeedRound(ctx, tournamentID, 1, commandID)
}

func (r *durableSeedingRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (SeedingUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneSeedingCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableSeedingUoW{inner: uow}, nil
}

func (r *durableSeedingRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupSeedingOutcome(context.Background(), commandID)
}

func (u *durableSeedingUoW) Exists() bool { return u.inner.Exists() }

func (u *durableSeedingUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableSeedingUoW) KickoffContext() domain.SeedRoundKickoffContext {
	return u.inner.KickoffContext()
}

func (u *durableSeedingUoW) Commit(req SeedingCommitRequest) error {
	return u.inner.Commit(store.SeedingCommitRequest{
		TournamentID:  req.TournamentID,
		CommandID:     req.CommandID,
		CommandType:   req.CommandType,
		CorrelationID: req.CorrelationID,
		Outcome:       req.Outcome,
		Decision:      req.Decision,
	})
}

func (u *durableSeedingUoW) Rollback() error { return u.inner.Rollback() }

// durableCompleteRoundRepo adapts store CompleteRound UoW to CompleteRoundRepository.
type durableCompleteRoundRepo struct {
	store *store.TournamentStore
}

type durableCompleteRoundUoW struct {
	inner *store.CompleteRoundUnitOfWork
}

func (r *durableCompleteRoundRepo) BeginCompleteRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (CompleteRoundUnitOfWork, error) {
	uow, err := r.store.BeginCompleteRound(ctx, tournamentID, roundNumber, commandID)
	if err != nil {
		return nil, err
	}
	return &durableCompleteRoundUoW{inner: uow}, nil
}

func (r *durableCompleteRoundRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (CompleteRoundUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneCompleteRoundCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableCompleteRoundUoW{inner: uow}, nil
}

func (r *durableCompleteRoundRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupCompleteRoundOutcome(context.Background(), commandID)
}

func (r *durableCompleteRoundRepo) FindReadyRoundCandidate(ctx context.Context) (*store.ReadyRoundCandidate, error) {
	return r.store.FindReadyRoundCandidate(ctx)
}

func (u *durableCompleteRoundUoW) Exists() bool { return u.inner.Exists() }

func (u *durableCompleteRoundUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableCompleteRoundUoW) Loaded() domain.CompleteRoundContext {
	return u.inner.Loaded()
}

func (u *durableCompleteRoundUoW) Commit(req CompleteRoundCommitRequest) error {
	events := make([]store.OutboxEvent, 0, len(req.Events))
	for _, e := range req.Events {
		events = append(events, store.OutboxEvent{
			EventID:       e.EventID,
			EventType:     e.EventType,
			TournamentID:  e.TournamentID,
			Topic:         e.Topic,
			PartitionKey:  e.PartitionKey,
			SchemaVersion: e.SchemaVersion,
			Payload:       e.Payload,
			CreatedAt:     e.CreatedAt,
		})
	}
	return u.inner.Commit(store.CompleteRoundCommitRequest{
		TournamentID:  req.TournamentID,
		CommandID:     req.CommandID,
		CommandType:   req.CommandType,
		CorrelationID: req.CorrelationID,
		Outcome:       req.Outcome,
		Events:        events,
		Decision:      req.Decision,
	})
}

func (u *durableCompleteRoundUoW) Rollback() error { return u.inner.Rollback() }

// durableLifecycleRepo adapts store Complete/Cancel UoW to LifecycleRepository.
type durableLifecycleRepo struct {
	store *store.TournamentStore
}

type durableLifecycleUoW struct {
	inner *store.LifecycleUnitOfWork
}

func (r *durableLifecycleRepo) BeginCompleteTournament(ctx context.Context, tournamentID, commandID string) (LifecycleUnitOfWork, error) {
	uow, err := r.store.BeginCompleteTournament(ctx, tournamentID, commandID)
	if err != nil {
		return nil, err
	}
	return &durableLifecycleUoW{inner: uow}, nil
}

func (r *durableLifecycleRepo) BeginCancelTournament(ctx context.Context, tournamentID, commandID string) (LifecycleUnitOfWork, error) {
	uow, err := r.store.BeginCancelTournament(ctx, tournamentID, commandID)
	if err != nil {
		return nil, err
	}
	return &durableLifecycleUoW{inner: uow}, nil
}

func (r *durableLifecycleRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (LifecycleUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneLifecycleCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableLifecycleUoW{inner: uow}, nil
}

func (r *durableLifecycleRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupLifecycleOutcome(context.Background(), commandID)
}

func (u *durableLifecycleUoW) Exists() bool { return u.inner.Exists() }

func (u *durableLifecycleUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableLifecycleUoW) CompleteContext() domain.CompleteTournamentContext {
	return u.inner.CompleteContext()
}

func (u *durableLifecycleUoW) CancelContext() domain.CancelTournamentContext {
	return u.inner.CancelContext()
}

func (u *durableLifecycleUoW) Commit(req LifecycleCommitRequest) error {
	events := make([]store.OutboxEvent, 0, len(req.Events))
	for _, e := range req.Events {
		events = append(events, store.OutboxEvent{
			EventID:       e.EventID,
			EventType:     e.EventType,
			TournamentID:  e.TournamentID,
			Topic:         e.Topic,
			PartitionKey:  e.PartitionKey,
			SchemaVersion: e.SchemaVersion,
			Payload:       e.Payload,
			CreatedAt:     e.CreatedAt,
		})
	}
	return u.inner.Commit(store.LifecycleCommitRequest{
		TournamentID:  req.TournamentID,
		CommandID:     req.CommandID,
		CommandType:   req.CommandType,
		CorrelationID: req.CorrelationID,
		Outcome:       req.Outcome,
		Events:        events,
		Complete:      req.Complete,
		Cancel:        req.Cancel,
	})
}

func (u *durableLifecycleUoW) Rollback() error { return u.inner.Rollback() }

// durableQuarantineResultRepo adapts store QuarantineTournamentResult UoW.
type durableQuarantineResultRepo struct {
	store *store.TournamentStore
}

type durableQuarantineResultUoW struct {
	inner *store.QuarantineResultUnitOfWork
}

func (r *durableQuarantineResultRepo) BeginQuarantineTournamentResult(ctx context.Context, tournamentID, roomID string, completionVersion uint64, commandID string) (QuarantineResultUnitOfWork, error) {
	uow, err := r.store.BeginQuarantineTournamentResult(ctx, tournamentID, roomID, completionVersion, commandID)
	if err != nil {
		return nil, err
	}
	return &durableQuarantineResultUoW{inner: uow}, nil
}

func (r *durableQuarantineResultRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (QuarantineResultUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneQuarantineResultCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableQuarantineResultUoW{inner: uow}, nil
}

func (r *durableQuarantineResultRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupQuarantineResultOutcome(context.Background(), commandID)
}

func (u *durableQuarantineResultUoW) Exists() bool { return u.inner.Exists() }

func (u *durableQuarantineResultUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableQuarantineResultUoW) Loaded() domain.QuarantineTournamentResultContext {
	return u.inner.Loaded()
}

func (u *durableQuarantineResultUoW) Commit(req QuarantineResultCommitRequest) error {
	return u.inner.Commit(store.QuarantineResultCommitRequest{
		TournamentID:      req.TournamentID,
		CommandID:         req.CommandID,
		CommandType:       req.CommandType,
		CorrelationID:     req.CorrelationID,
		Outcome:           req.Outcome,
		Decision:          req.Decision,
		Command:           req.Command,
		ProjectionChanged: req.ProjectionChanged,
	})
}

func (u *durableQuarantineResultUoW) Rollback() error { return u.inner.Rollback() }

// durableProvisioningRepo adapts store provisioning UoW to ProvisioningRepository.
type durableProvisioningRepo struct {
	store *store.TournamentStore
}

type durableProvisioningUoW struct {
	inner *store.ProvisioningUnitOfWork
}

func (r *durableProvisioningRepo) BeginProvisionRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (ProvisioningUnitOfWork, error) {
	uow, err := r.store.BeginProvisionRound(ctx, tournamentID, roundNumber, commandID)
	if err != nil {
		return nil, err
	}
	return &durableProvisioningUoW{inner: uow}, nil
}

func (r *durableProvisioningRepo) BeginStandaloneCommand(ctx context.Context, commandID string) (ProvisioningUnitOfWork, error) {
	uow, err := r.store.BeginStandaloneProvisionCommand(ctx, commandID)
	if err != nil {
		return nil, err
	}
	return &durableProvisioningUoW{inner: uow}, nil
}

func (r *durableProvisioningRepo) LookupOutcome(commandID string) (envelope.Result, bool) {
	return r.store.LookupProvisionOutcome(context.Background(), commandID)
}

func (r *durableProvisioningRepo) PrepareProvisioningBatch(ctx context.Context, in store.PrepareProvisioningBatchInput) (*store.PrepareProvisioningBatchResult, error) {
	return r.store.PrepareProvisioningBatch(ctx, in)
}

func (r *durableProvisioningRepo) FinalizeProvisioningBatchSuccess(ctx context.Context, in store.FinalizeProvisioningBatchInput) (envelope.Result, error) {
	return r.store.FinalizeProvisioningBatchSuccess(ctx, in)
}

func (r *durableProvisioningRepo) FinalizeProvisioningBatchFailure(ctx context.Context, in store.FinalizeProvisioningBatchInput) (envelope.Result, error) {
	return r.store.FinalizeProvisioningBatchFailure(ctx, in)
}

func (r *durableProvisioningRepo) HeartbeatProvisioningLease(ctx context.Context, tid string, roundNumber int, batchID, owner string, leaseVersion int64, retryAttempt int, now time.Time, ttl time.Duration) (bool, error) {
	return r.store.HeartbeatProvisioningLease(ctx, tid, roundNumber, batchID, owner, leaseVersion, retryAttempt, now, ttl)
}

func (r *durableProvisioningRepo) ManualRetryProvisioningBatch(ctx context.Context, commandID, tid string, roundNumber int, batchID string, retryAttempt int, correlationID string) (envelope.Result, error) {
	return r.store.ManualRetryProvisioningBatch(ctx, commandID, tid, roundNumber, batchID, retryAttempt, correlationID)
}

func (r *durableProvisioningRepo) ManualQuarantineProvisioningBatch(ctx context.Context, commandID, tid string, roundNumber int, batchID, reason, correlationID string) (envelope.Result, error) {
	return r.store.ManualQuarantineProvisioningBatch(ctx, commandID, tid, roundNumber, batchID, reason, correlationID)
}

func (r *durableProvisioningRepo) LoadRetryBudget(ctx context.Context, tournamentID string) (int, error) {
	return r.store.LoadRetryBudget(ctx, tournamentID)
}

func (u *durableProvisioningUoW) Exists() bool { return u.inner.Exists() }

func (u *durableProvisioningUoW) LookupOutcome(commandID string) (envelope.Result, bool) {
	return u.inner.LookupOutcome(commandID)
}

func (u *durableProvisioningUoW) KickoffContext() domain.ProvisionKickoffContext {
	return u.inner.KickoffContext()
}

func (u *durableProvisioningUoW) Commit(req ProvisioningCommitRequest) error {
	return u.inner.Commit(store.ProvisioningCommitRequest{
		TournamentID:  req.TournamentID,
		CommandID:     req.CommandID,
		CommandType:   req.CommandType,
		CorrelationID: req.CorrelationID,
		Outcome:       req.Outcome,
		Decision:      req.Decision,
		RoundNumber:   req.RoundNumber,
	})
}

func (u *durableProvisioningUoW) Rollback() error { return u.inner.Rollback() }
