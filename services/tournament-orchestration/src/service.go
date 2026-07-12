package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

// Command type catalog (docs/04 + OpenAPI BFF surface + policy commands).
const (
	CmdCreateTournament      = "CreateTournament"
	CmdRegisterPlayer        = "RegisterPlayer"
	CmdCloseRegistration     = "CloseRegistration"
	CmdSeedRound             = "SeedRound"
	CmdProvisionRoundMatches = "ProvisionRoundMatches"
	CmdRecordMatchResult     = "RecordMatchResult"
	CmdCompleteRound         = "CompleteRound"
	CmdCompleteTournament    = "CompleteTournament"
	CmdCancelTournament      = "CancelTournament"
	CmdRetryProvisioning     = "RetryTournamentProvisioningBatch"
	CmdQuarantineBatch       = "QuarantineTournamentProvisioningBatch"
	CmdQuarantineResult      = "QuarantineTournamentResult"

	eventTypeMatchCompleted = "MatchCompleted"
)

// ServiceDeps wires the offline Tournament Orchestration application.
type ServiceDeps struct {
	Repo              TournamentRepository
	RoundMatches      RoundMatchRepository       // durable MatchCompleted + HTTP RecordMatchResult; nil → memory path
	Registrations     RegistrationRepository     // durable Create/Register/Close; nil → legacy path
	Seeding           SeedingRepository          // durable SeedRound kickoff (any round); nil → legacy path
	Provisioning      ProvisioningRepository     // durable ProvisionRoundMatches + ProcessProvisioningBatch; nil → legacy
	CompleteRounds    CompleteRoundRepository    // durable CompleteRound; nil → memory/legacy path
	Lifecycle         LifecycleRepository        // durable CompleteTournament/CancelTournament; nil → legacy
	QuarantineResults QuarantineResultRepository // durable QuarantineTournamentResult; nil → legacy
	Rooms             RoomProvisioner
	Publisher         EventPublisher
	Audit             AuditSink
	Clock             Clock
	IDs               IDGenerator
	BracketPages      BracketPageLoader          // optional; durable Redis-first / Postgres keyset reader
	BracketRefresh    BracketProjectionRefresher // optional; post-commit Redis refresh (never blocks command success)
	Standings         StandingsLoader            // optional; durable Postgres bounded standings
	ReadAuth          TournamentReadAuthorizer
	Assignments       PlayerAssignmentLoader
}

// BracketPageLoader loads bounded BracketPage from durable storage.
type BracketPageLoader interface {
	LoadBracketPage(ctx context.Context, q store.BracketPageQuery) (store.BracketPage, error)
}

// StandingsLoader loads bounded StandingsProjection from durable storage.
type StandingsLoader interface {
	LoadStandingsProjection(ctx context.Context, tournamentID string) (store.StandingsProjection, error)
}

// Service applies versioned commands over Tournament aggregates.
type Service struct {
	repo              TournamentRepository
	roundMatches      RoundMatchRepository
	registrations     RegistrationRepository
	seeding           SeedingRepository
	provisioning      ProvisioningRepository
	completeRounds    CompleteRoundRepository
	lifecycle         LifecycleRepository
	quarantineResults QuarantineResultRepository
	rooms             RoomProvisioner
	publisher         EventPublisher
	audit             AuditSink
	clock             Clock
	ids               IDGenerator
	bracketPages      BracketPageLoader
	bracketRefresh    BracketProjectionRefresher
	standings         StandingsLoader
	readAuth          TournamentReadAuthorizer
	assignments       PlayerAssignmentLoader
	analyticsBackfill AnalyticsBackfillReader

	// mu serializes aggregate mutation + Commit for memory/capability and legacy
	// whole-rewrite commands. Durable Kafka MatchCompleted, durable HTTP
	// RecordMatchResult, durable T-Reg Create/Register/Close, durable SeedRound,
	// durable CompleteRound, durable Complete/CancelTournament, durable
	// QuarantineTournamentResult, and durable provisioning must NOT hold this mutex.
	mu sync.Mutex
}

func NewService(d ServiceDeps) *Service {
	clock := d.Clock
	if clock == nil {
		clock = systemClock{}
	}
	rooms := d.Rooms
	if rooms == nil {
		rooms = NoopRoomProvisioner{}
	}
	publisher := d.Publisher
	if publisher == nil {
		publisher = NoopPublisher{}
	}
	auditSink := d.Audit
	if auditSink == nil {
		auditSink = NoopAudit{}
	}
	ids := d.IDs
	if ids == nil {
		ids = randomIDs{}
	}
	return &Service{
		repo:              d.Repo,
		roundMatches:      d.RoundMatches,
		registrations:     d.Registrations,
		seeding:           d.Seeding,
		provisioning:      d.Provisioning,
		completeRounds:    d.CompleteRounds,
		lifecycle:         d.Lifecycle,
		quarantineResults: d.QuarantineResults,
		rooms:             rooms,
		publisher:         publisher,
		audit:             auditSink,
		clock:             clock,
		ids:               ids,
		bracketPages:      d.BracketPages,
		bracketRefresh:    d.BracketRefresh,
		standings:         d.Standings,
		readAuth:          d.ReadAuth,
		assignments:       d.Assignments,
	}
}

// NoopAudit drops rejection audits (tests should use MemoryAudit).
type NoopAudit struct{}

func (NoopAudit) RecordRejection(audit.RejectionRecord) error { return nil }

type randomIDs struct{}

func (randomIDs) NewID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

// CommandRequest is the Gateway-forwarded internal command body.
type CommandRequest struct {
	CommandID              string          `json:"commandId"`
	Type                   string          `json:"type"`
	SchemaVersion          int             `json:"schemaVersion"`
	Payload                json.RawMessage `json:"payload"`
	ExpectedSequenceNumber *int64          `json:"expectedSequenceNumber,omitempty"`
	PlayerID               string          `json:"playerId"`
	SessionID              string          `json:"sessionId"`
	RoomID                 string          `json:"roomId,omitempty"`
}

// SubmitCommand routes a versioned command to the domain aggregate.
func (s *Service) SubmitCommand(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if s.registrations != nil && isRegistrationDifferentialCommand(req.Type) {
		return s.submitCommandRegistrationDifferential(ctx, req, correlationID)
	}
	if s.seeding != nil && req.Type == CmdSeedRound {
		return s.submitCommandSeedingDifferential(ctx, req, correlationID)
	}
	if s.provisioning != nil && isProvisioningDifferentialCommand(req.Type) {
		return s.submitCommandProvisioningDifferential(ctx, req, correlationID)
	}
	if s.roundMatches != nil && req.Type == CmdRecordMatchResult {
		return s.submitCommandRecordMatchDifferential(ctx, req, correlationID)
	}
	if s.completeRounds != nil && req.Type == CmdCompleteRound {
		return s.submitCommandCompleteRoundDifferential(ctx, req, correlationID)
	}
	if s.lifecycle != nil && (req.Type == CmdCompleteTournament || req.Type == CmdCancelTournament) {
		return s.submitCommandLifecycleDifferential(ctx, req, correlationID)
	}
	if s.quarantineResults != nil && req.Type == CmdQuarantineResult {
		return s.submitCommandQuarantineResultDifferential(ctx, req, correlationID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submitCommandLocked(req, correlationID)
}

func isRegistrationDifferentialCommand(typ string) bool {
	switch typ {
	case CmdCreateTournament, CmdRegisterPlayer, CmdCloseRegistration:
		return true
	default:
		return false
	}
}

func (s *Service) submitCommandLocked(req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.reject(req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	cmd := envelope.Command{
		CommandID:     req.CommandID,
		Type:          req.Type,
		SchemaVersion: req.SchemaVersion,
		Payload:       req.Payload,
	}
	if err := cmd.Validate(); err != nil {
		return envelope.Result{}, err
	}

	if prior, ok := s.repo.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return envelope.Result{}, fmt.Errorf("payload must be a JSON object")
	}

	tournamentID := stringField(payload, "tournamentId")
	return s.dispatch(req, payload, tournamentID, correlationID)
}

func (s *Service) dispatch(req CommandRequest, payload map[string]any, tournamentID, correlationID string) (envelope.Result, error) {
	switch req.Type {
	case CmdCreateTournament:
		return s.createTournament(req, payload, correlationID)
	case CmdRegisterPlayer:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			playerID := stringField(payload, "playerId")
			if playerID == "" {
				playerID = req.PlayerID
			}
			return t.RegisterPlayer(domain.RegisterPlayerCommand{
				CommandID: domain.CommandID(req.CommandID),
				PlayerID:  domain.PlayerID(playerID),
			})
		})
	case CmdCloseRegistration:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.CloseRegistration(domain.CloseRegistrationCommand{CommandID: domain.CommandID(req.CommandID)})
		})
	case CmdSeedRound:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.SeedRound(domain.SeedRoundCommand{
				CommandID:   domain.CommandID(req.CommandID),
				RoundNumber: intField(payload, "roundNumber"),
			})
		})
	case CmdProvisionRoundMatches:
		// Assignment + outbox only; Room calls happen in ProcessProvisioningBatch.
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.ProvisionRoundMatches(domain.ProvisionRoundMatchesCommand{
				CommandID:   domain.CommandID(req.CommandID),
				RoundNumber: intField(payload, "roundNumber"),
			})
		})
	case CmdRecordMatchResult:
		// Memory/capability whole rewrite when RoundMatches is not wired.
		return s.recordResult(req, payload, tournamentID, correlationID)
	case CmdCompleteRound:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.CompleteRound(domain.CompleteRoundCommand{
				CommandID:   domain.CommandID(req.CommandID),
				RoundNumber: intField(payload, "roundNumber"),
			})
		})
	case CmdCompleteTournament:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.CompleteTournament(domain.CompleteTournamentCommand{CommandID: domain.CommandID(req.CommandID)})
		})
	case CmdCancelTournament:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.CancelTournament(domain.CancelTournamentCommand{CommandID: domain.CommandID(req.CommandID)})
		})
	case CmdRetryProvisioning:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.RetryTournamentProvisioningBatch(domain.RetryTournamentProvisioningBatchCommand{
				CommandID:    domain.CommandID(req.CommandID),
				RoundNumber:  intField(payload, "roundNumber"),
				BatchID:      domain.BatchID(stringField(payload, "batchId")),
				RetryAttempt: intField(payload, "retryAttempt"),
			})
		})
	case CmdQuarantineBatch:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.QuarantineTournamentProvisioningBatch(domain.QuarantineTournamentProvisioningBatchCommand{
				CommandID:   domain.CommandID(req.CommandID),
				RoundNumber: intField(payload, "roundNumber"),
				BatchID:     domain.BatchID(stringField(payload, "batchId")),
				Reason:      stringField(payload, "reason"),
			})
		})
	case CmdQuarantineResult:
		return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
			return t.QuarantineTournamentResult(domain.QuarantineTournamentResultCommand{
				CommandID:         domain.CommandID(req.CommandID),
				RoomID:            domain.RoomID(stringField(payload, "roomId")),
				CompletionVersion: domain.CompletionVersion(uintField(payload, "completionVersion")),
				Reason:            stringField(payload, "reason"),
			})
		})
	default:
		return s.reject(req, correlationID, tournamentID, "unknown_command_type")
	}
}

func (s *Service) createTournament(req CommandRequest, payload map[string]any, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(stringField(payload, "tournamentId"))
	if !tid.Valid() {
		return s.reject(req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	uow, err := s.repo.BeginCreate(tid)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if uow.Exists() {
		return s.rejectUoW(uow, req, correlationID, string(tid), "tournament_already_exists")
	}
	tr, out := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID:    domain.CommandID(req.CommandID),
		TournamentID: tid,
		Capacity:     intField(payload, "capacity"),
		RetryBudget:  intField(payload, "retryBudget"),
		BatchSize:    intField(payload, "batchSize"),
		Visibility:   domain.TournamentVisibility(stringField(payload, "visibility")),
	})
	return s.commitOutcome(uow, req, tr, out, string(tid), correlationID, nil)
}

func (s *Service) withTournament(req CommandRequest, tournamentID, correlationID string, fn func(*domain.Tournament) domain.CommandOutcome) (envelope.Result, error) {
	return s.withTournamentSource(req, tournamentID, correlationID, nil, fn)
}

func (s *Service) withTournamentSource(req CommandRequest, tournamentID, correlationID string, matchSource *MatchResultSource, fn func(*domain.Tournament) domain.CommandOutcome) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	if !tid.Valid() {
		return s.reject(req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	uow, err := s.repo.BeginExisting(tid)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectUoW(uow, req, correlationID, tournamentID, "tournament_not_found")
	}
	t := uow.Loaded()
	out := fn(t)
	return s.commitOutcome(uow, req, t, out, tournamentID, correlationID, matchSource)
}

func (s *Service) recordResult(req CommandRequest, payload map[string]any, tournamentID, correlationID string) (envelope.Result, error) {
	standings, err := parseStandings(payload["standings"])
	if err != nil {
		return s.reject(req, correlationID, tournamentID, "invalid_standings")
	}
	eventID := stringField(payload, "eventId")
	roomID := stringField(payload, "roomId")
	completionVersion := uintField(payload, "completionVersion")
	var matchSource *MatchResultSource
	if eventID != "" && roomID != "" && completionVersion > 0 {
		matchSource = &MatchResultSource{
			EventID:           eventID,
			RoomID:            roomID,
			CompletionVersion: completionVersion,
		}
	}
	return s.withTournamentSource(req, tournamentID, correlationID, matchSource, func(t *domain.Tournament) domain.CommandOutcome {
		rid := domain.RoomID(roomID)
		roundNumber := intField(payload, "roundNumber")
		slotID := domain.SlotID(stringField(payload, "slotId"))
		if roundNumber < 1 || !slotID.Valid() {
			if rn, sid, ok := t.AssignmentByRoomID(rid); ok {
				roundNumber = rn
				slotID = sid
			}
		}
		return t.RecordMatchResult(domain.RecordMatchResultCommand{
			CommandID:         domain.CommandID(req.CommandID),
			EventID:           domain.EventID(eventID),
			RoomID:            rid,
			RoundNumber:       roundNumber,
			SlotID:            slotID,
			CompletionVersion: domain.CompletionVersion(completionVersion),
			Standings:         standings,
			IsAbandoned:       boolField(payload, "isAbandoned"),
		})
	})
}

// provisioningAttemptCommandID is the stable worker lease identity for one batch attempt.
func provisioningAttemptCommandID(tournamentID string, roundNumber int, batchID string, retryAttempt int) string {
	return fmt.Sprintf("provision:%s:r%d:%s:a%d", tournamentID, roundNumber, batchID, retryAttempt)
}

func resolveProvisioningCommandID(work ProvisioningBatchWork) string {
	if strings.TrimSpace(work.CommandID) != "" {
		return work.CommandID
	}
	return provisioningAttemptCommandID(work.TournamentID, work.RoundNumber, work.BatchID, work.RetryAttempt)
}

// ProcessProvisioningBatch runs the idempotent worker for one bounded shard.
// Room Gameplay calls happen outside held database locks.
// Attempt identity is (batchId, retryAttempt) via commandId; a transient Room failure
// records retry state under the current attempt and returns nextRetryWork for the next attempt.
func (s *Service) ProcessProvisioningBatch(ctx context.Context, work ProvisioningBatchWork) (envelope.Result, error) {
	if s.provisioning != nil {
		return s.processProvisioningBatchDifferential(ctx, work)
	}
	return s.processProvisioningBatchLegacy(ctx, work)
}

func (s *Service) processProvisioningBatchLegacy(ctx context.Context, work ProvisioningBatchWork) (envelope.Result, error) {
	work.CommandID = resolveProvisioningCommandID(work)
	s.mu.Lock()
	if prior, ok := s.repo.LookupOutcome(work.CommandID); ok {
		s.mu.Unlock()
		return prior, nil
	}
	req := CommandRequest{
		CommandID:     work.CommandID,
		Type:          "ProcessProvisioningBatch",
		SchemaVersion: envelope.CurrentSchemaVersion,
	}
	if err := validateProvisioningBatchWork(work); err != nil {
		res, err2 := s.reject(req, work.CorrelationID, work.TournamentID, err.Error())
		s.mu.Unlock()
		return res, err2
	}

	tid := domain.TournamentID(work.TournamentID)
	uow, err := s.repo.BeginExisting(tid)
	if err != nil {
		s.mu.Unlock()
		return envelope.Result{}, err
	}
	if prior, ok := uow.LookupOutcome(work.CommandID); ok {
		_ = uow.Rollback()
		s.mu.Unlock()
		return prior, nil
	}
	if !uow.Exists() {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, "tournament_not_found")
		s.mu.Unlock()
		return res, err2
	}
	t := uow.Loaded()
	round, ok := t.Round(work.RoundNumber)
	if !ok {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectRoundNotFound))
		s.mu.Unlock()
		return res, err2
	}
	batch, ok := round.FindBatch(domain.BatchID(work.BatchID))
	if !ok {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectBatchNotFound))
		s.mu.Unlock()
		return res, err2
	}
	if batch.Status == domain.BatchQuarantined {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectQuarantined))
		s.mu.Unlock()
		return res, err2
	}
	if batch.Status == domain.BatchCancelled {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectBatchCancelled))
		s.mu.Unlock()
		return res, err2
	}
	if err := validateBatchRange(batch, work); err != nil {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, err.Error())
		s.mu.Unlock()
		return res, err2
	}
	if batch.Status == domain.BatchCompleted {
		out := t.CompleteTournamentProvisioningBatch(domain.CompleteTournamentProvisioningBatchCommand{
			CommandID:   domain.CommandID(work.CommandID),
			RoundNumber: work.RoundNumber,
			BatchID:     domain.BatchID(work.BatchID),
		})
		res, err2 := s.commitOutcome(uow, req, t, out, work.TournamentID, work.CorrelationID, nil)
		s.mu.Unlock()
		return res, err2
	}
	// Cancelled/terminal work must never reach Room provisioning.
	if t.Phase().IsTerminal() {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectAlreadyTerminal))
		s.mu.Unlock()
		return res, err2
	}
	if round.Status != domain.RoundProvisioning {
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectRoundNotReady))
		s.mu.Unlock()
		return res, err2
	}
	switch batch.Status {
	case domain.BatchPending, domain.BatchRetried, domain.BatchInProgress:
		// claimable/work statuses only
	default:
		res, err2 := s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectUnexpectedBatchStatus))
		s.mu.Unlock()
		return res, err2
	}

	type slotWork struct {
		SlotID    domain.SlotID
		RoomID    domain.RoomID
		BatchID   domain.BatchID
		PlayerIDs []domain.PlayerID
	}
	slots := make([]slotWork, 0, len(batch.SlotIndexes))
	for _, idx := range batch.SlotIndexes {
		if idx < 0 || idx >= len(round.Slots) {
			continue
		}
		slot := round.Slots[idx]
		players := append([]domain.PlayerID(nil), slot.SeededPlayers...)
		slots = append(slots, slotWork{
			SlotID:    slot.SlotID,
			RoomID:    slot.RoomID,
			BatchID:   slot.BatchID,
			PlayerIDs: players,
		})
	}
	_ = uow.Rollback()
	s.mu.Unlock()

	for _, slot := range slots {
		res, err := s.rooms.Provision(ctx, RoomProvisionRequest{
			TournamentID: tid,
			RoundNumber:  work.RoundNumber,
			SlotID:       slot.SlotID,
			RoomID:       slot.RoomID,
			PlayerIDs:    slot.PlayerIDs,
			BatchID:      slot.BatchID,
		})
		if err != nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.recordProvisionFailure(req, work, err.Error())
		}
		if res.RoomID != string(slot.RoomID) {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.recordProvisionFailure(req, work, ErrRoomIDMismatch.Error())
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	uow2, err := s.repo.BeginExisting(tid)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow2.Rollback() }()
	if prior, ok := uow2.LookupOutcome(work.CommandID); ok {
		return prior, nil
	}
	if !uow2.Exists() {
		return s.rejectUoW(uow2, req, work.CorrelationID, work.TournamentID, "tournament_not_found")
	}
	t = uow2.Loaded()
	out := t.CompleteTournamentProvisioningBatch(domain.CompleteTournamentProvisioningBatchCommand{
		CommandID:   domain.CommandID(work.CommandID),
		RoundNumber: work.RoundNumber,
		BatchID:     domain.BatchID(work.BatchID),
	})
	return s.commitOutcome(uow2, req, t, out, work.TournamentID, work.CorrelationID, nil)
}

func (s *Service) recordProvisionFailure(req CommandRequest, work ProvisioningBatchWork, reason string) (envelope.Result, error) {
	uow, err := s.repo.BeginExisting(domain.TournamentID(work.TournamentID))
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(work.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, "tournament_not_found")
	}
	t := uow.Loaded()
	round, ok := t.Round(work.RoundNumber)
	if !ok {
		return s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectRoundNotFound))
	}
	batch, ok := round.FindBatch(domain.BatchID(work.BatchID))
	if !ok {
		return s.rejectUoW(uow, req, work.CorrelationID, work.TournamentID, string(domain.RejectBatchNotFound))
	}
	nextAttempt := batch.RetryAttempt + 1
	out := t.RetryTournamentProvisioningBatch(domain.RetryTournamentProvisioningBatchCommand{
		CommandID:    domain.CommandID(work.CommandID),
		RoundNumber:  work.RoundNumber,
		BatchID:      domain.BatchID(work.BatchID),
		RetryAttempt: nextAttempt,
	})
	if b, ok := round.FindBatch(domain.BatchID(work.BatchID)); ok {
		b.LastError = reason
	}
	// Quarantine (budget exhausted) has no further retry work.
	if b, ok := round.FindBatch(domain.BatchID(work.BatchID)); ok && b.Status == domain.BatchQuarantined {
		return s.commitOutcome(uow, req, t, out, work.TournamentID, work.CorrelationID, nil)
	}
	nextWork := ProvisioningBatchWork{
		CommandID:     provisioningAttemptCommandID(work.TournamentID, work.RoundNumber, work.BatchID, nextAttempt),
		TournamentID:  work.TournamentID,
		RoundNumber:   work.RoundNumber,
		BatchID:       work.BatchID,
		SlotFrom:      work.SlotFrom,
		SlotTo:        work.SlotTo,
		SlotSize:      work.SlotSize,
		RetryAttempt:  nextAttempt,
		CorrelationID: work.CorrelationID,
	}
	// Attach nextRetryWork before Commit so exact replay returns byte-stable retry identity.
	return s.commitOutcome(uow, req, t, out, work.TournamentID, work.CorrelationID, nil, nextRetryWorkPayload(nextWork))
}

func nextRetryWorkPayload(next ProvisioningBatchWork) map[string]any {
	return map[string]any{
		"nextRetryWork": map[string]any{
			"commandId":     next.CommandID,
			"tournamentId":  next.TournamentID,
			"roundNumber":   next.RoundNumber,
			"batchId":       next.BatchID,
			"slotFrom":      next.SlotFrom,
			"slotTo":        next.SlotTo,
			"slotSize":      next.SlotSize,
			"retryAttempt":  next.RetryAttempt,
			"correlationId": next.CorrelationID,
		},
	}
}

func validateProvisioningBatchWork(work ProvisioningBatchWork) error {
	if strings.TrimSpace(work.CommandID) == "" {
		return fmt.Errorf("commandId is required")
	}
	if strings.TrimSpace(work.TournamentID) == "" {
		return fmt.Errorf("tournamentId is required")
	}
	if work.RoundNumber < 1 {
		return fmt.Errorf("roundNumber is required")
	}
	if strings.TrimSpace(work.BatchID) == "" {
		return fmt.Errorf("batchId is required")
	}
	if strings.TrimSpace(work.SlotFrom) == "" || strings.TrimSpace(work.SlotTo) == "" {
		return fmt.Errorf("explicit slotFrom and slotTo are required")
	}
	if work.SlotSize <= 0 {
		return fmt.Errorf("explicit bounded slotSize is required")
	}
	if work.SlotSize > domain.MaxProvisioningBatchSize {
		return fmt.Errorf("slotSize exceeds max provisioning batch size %d", domain.MaxProvisioningBatchSize)
	}
	return nil
}

func validateBatchRange(batch *domain.ProvisioningBatch, work ProvisioningBatchWork) error {
	if string(batch.SlotFrom) != work.SlotFrom || string(batch.SlotTo) != work.SlotTo {
		return fmt.Errorf("slot range does not match batch sharding")
	}
	if len(batch.SlotIndexes) != work.SlotSize {
		return fmt.Errorf("slotSize does not match batch sharding")
	}
	if len(batch.SlotIndexes) > domain.MaxProvisioningBatchSize {
		return fmt.Errorf("batch exceeds max provisioning batch size")
	}
	return nil
}

// IngestMatchCompleted applies an idempotent room.match.completed fact.
// Durable path (RoundMatches wired): bounded Round/slot differential UoW, no Service.mu.
// Memory/capability path: legacy aggregate + Service.mu.
func (s *Service) IngestMatchCompleted(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
	if err := validateMatchCompletedEvent(evt); err != nil {
		return nil, err
	}
	if s.roundMatches != nil {
		return s.ingestMatchCompletedDifferential(ctx, evt)
	}
	return s.ingestMatchCompletedLegacy(ctx, evt)
}

func (s *Service) ingestMatchCompletedDifferential(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
	tid := domain.TournamentID(evt.TournamentID)
	commandID := "ingest:" + evt.EventID
	if prior, ok := s.roundMatches.LookupOutcome(commandID); ok {
		return ingestResponse(prior, commandID, tid, evt, dispositionFromResult(prior)), nil
	}

	uow, err := s.roundMatches.BeginRoundMatch(ctx, string(tid), evt.RoomID, evt.RoundNumber, evt.SlotID, commandID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(commandID); ok {
		return ingestResponse(prior, commandID, tid, evt, dispositionFromResult(prior)), nil
	}
	if !uow.Exists() {
		return nil, fmt.Errorf("tournament_not_found")
	}

	if err := uow.AttachPriorResult(evt.RoomID, evt.CompletionVersion); err != nil {
		return nil, err
	}

	standings := matchCompletedStandings(evt)
	// Fill only missing hints. Never overwrite nonempty claimed round/slot before policy.
	roundNumber := evt.RoundNumber
	slotID := domain.SlotID(evt.SlotID)
	loaded := uow.Loaded()
	if roundNumber < 1 {
		roundNumber = loaded.RoundNumber
	}
	if !slotID.Valid() {
		slotID = loaded.Slot.SlotID
	}

	cmd := domain.RecordMatchResultCommand{
		CommandID:         domain.CommandID(commandID),
		EventID:           domain.EventID(evt.EventID),
		RoomID:            domain.RoomID(evt.RoomID),
		RoundNumber:       roundNumber,
		SlotID:            slotID,
		CompletionVersion: domain.CompletionVersion(evt.CompletionVersion),
		Standings:         standings,
		IsAbandoned:       evt.IsAbandoned,
	}
	decision := domain.DecideRecordMatchResult(loaded, cmd)

	req := CommandRequest{CommandID: commandID, Type: CmdRecordMatchResult, SchemaVersion: 1}
	matchSource := &MatchResultSource{
		EventID:           evt.EventID,
		RoomID:            evt.RoomID,
		CompletionVersion: evt.CompletionVersion,
	}
	result, err := s.commitRoundMatchOutcome(uow, req, decision, cmd, string(tid), evt.CorrelationID, matchSource)
	if err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return ingestResponse(prior, commandID, tid, evt, dispositionFromResult(prior)), nil
		}
		return nil, err
	}
	disposition := dispositionFromDecision(decision)
	return ingestResponse(result, commandID, tid, evt, disposition), nil
}

func (s *Service) ingestMatchCompletedLegacy(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	tid := domain.TournamentID(evt.TournamentID)
	commandID := "ingest:" + evt.EventID
	if prior, ok := s.repo.LookupOutcome(commandID); ok {
		return ingestResponse(prior, commandID, tid, evt, dispositionFromResult(prior)), nil
	}

	uow, err := s.repo.BeginExisting(tid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(commandID); ok {
		return ingestResponse(prior, commandID, tid, evt, dispositionFromResult(prior)), nil
	}
	if !uow.Exists() {
		return nil, fmt.Errorf("tournament_not_found")
	}
	t := uow.Loaded()

	standings := matchCompletedStandings(evt)
	roundNumber := evt.RoundNumber
	slotID := domain.SlotID(evt.SlotID)
	roomID := domain.RoomID(evt.RoomID)
	if roundNumber < 1 || !slotID.Valid() {
		if rn, sid, ok := t.AssignmentByRoomID(roomID); ok {
			roundNumber = rn
			slotID = sid
		}
	}

	out := t.RecordMatchResult(domain.RecordMatchResultCommand{
		CommandID:         domain.CommandID(commandID),
		EventID:           domain.EventID(evt.EventID),
		RoomID:            roomID,
		RoundNumber:       roundNumber,
		SlotID:            slotID,
		CompletionVersion: domain.CompletionVersion(evt.CompletionVersion),
		Standings:         standings,
		IsAbandoned:       evt.IsAbandoned,
	})

	req := CommandRequest{CommandID: commandID, Type: CmdRecordMatchResult, SchemaVersion: 1}
	matchSource := &MatchResultSource{
		EventID:           evt.EventID,
		RoomID:            evt.RoomID,
		CompletionVersion: evt.CompletionVersion,
	}
	result, err := s.commitOutcome(uow, req, t, out, string(tid), evt.CorrelationID, matchSource)
	if err != nil {
		return nil, err
	}
	return ingestResponse(result, commandID, tid, evt, dispositionFromDomainOutcome(out)), nil
}

func matchCompletedStandings(evt MatchCompletedEvent) []domain.PlayerMatchStanding {
	standings := make([]domain.PlayerMatchStanding, 0, len(evt.Players))
	forfeitSet := map[string]struct{}{}
	for _, f := range evt.Forfeits {
		forfeitSet[f] = struct{}{}
	}
	for _, p := range evt.Players {
		_, forfeited := forfeitSet[p.PlayerID]
		if p.Forfeited {
			forfeited = true
		}
		standings = append(standings, domain.PlayerMatchStanding{
			PlayerID:             domain.PlayerID(p.PlayerID),
			MatchWins:            p.MatchWins,
			CumulativeCardPoints: p.CumulativeCardPoints,
			FinalGameCompletedAt: p.FinalGameCompletedAt,
			Forfeited:            forfeited,
		})
	}
	return standings
}

func ingestResponse(result envelope.Result, commandID string, tid domain.TournamentID, evt MatchCompletedEvent, disposition string) map[string]any {
	return map[string]any{
		"status":            result.Status,
		"commandId":         commandID,
		"disposition":       disposition,
		"schemaVersion":     envelope.CurrentSchemaVersion,
		"tournamentId":      string(tid),
		"roomId":            evt.RoomID,
		"completionVersion": evt.CompletionVersion,
	}
}

func dispositionFromDomainOutcome(out domain.CommandOutcome) string {
	disposition := "recorded"
	for _, f := range out.Facts {
		if f.Name == domain.FactTournamentResultQuarantined {
			return "quarantined"
		}
	}
	if out.Accepted() && len(out.Facts) == 0 {
		return "duplicate_ignored"
	}
	if out.Kind == domain.OutcomeDuplicate {
		for _, f := range out.Facts {
			if f.Name == domain.FactTournamentResultQuarantined {
				return "quarantined"
			}
			if f.Name == domain.FactTournamentMatchResultRecorded {
				return "recorded"
			}
		}
	}
	return disposition
}

func dispositionFromDecision(d domain.RoundMatchDecision) string {
	switch d.Kind {
	case domain.RoundMatchExactDuplicate, domain.RoundMatchDuplicateEvent:
		return "duplicate_ignored"
	case domain.RoundMatchQuarantineUnresolved, domain.RoundMatchQuarantineConflict, domain.RoundMatchQuarantineHeld:
		return "quarantined"
	case domain.RoundMatchRecord:
		return "recorded"
	case domain.RoundMatchReject:
		return "rejected"
	default:
		return dispositionFromDomainOutcome(d.Outcome)
	}
}

func (s *Service) commitRoundMatchOutcome(
	uow RoundMatchUnitOfWork,
	req CommandRequest,
	decision domain.RoundMatchDecision,
	cmd domain.RecordMatchResultCommand,
	tournamentID, correlationID string,
	matchSource *MatchResultSource,
) (envelope.Result, error) {
	out := decision.Outcome
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.Commit(RoundMatchCommitRequest{
			TournamentID:      tournamentID,
			CommandID:         req.CommandID,
			CommandType:       req.Type,
			Outcome:           res,
			Decision:          decision,
			Command:           cmd,
			MatchResultSource: matchSource,
			ProjectionChanged: false,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return s.canonicalRoundMatchOutcome(req.CommandID, prior), nil
			}
			return envelope.Result{}, err
		}
		rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(rec); err != nil {
			return envelope.Result{}, err
		}
		return s.canonicalRoundMatchOutcome(req.CommandID, res), nil
	}

	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	if len(out.Facts) > 0 {
		now := s.clock.Now()
		for i, f := range out.Facts {
			topic := topicForFact(f.Name)
			if topic == "" {
				continue
			}
			eventID := req.CommandID + ":" + string(f.Name) + ":" + strconv.Itoa(i)
			events = append(events, OutboxEvent{
				EventID:       eventID,
				EventType:     string(f.Name),
				TournamentID:  tournamentID,
				Topic:         topic,
				PartitionKey:  tournamentID,
				SchemaVersion: 1,
				Payload:       buildTournamentIntegrationPayload(eventID, string(f.Name), correlationID, req.CommandID, tournamentID, f.Data, now),
				CreatedAt:     now,
			})
		}
	}
	payload, _ := json.Marshal(map[string]any{"facts": factsPayload})
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	if err := uow.Commit(RoundMatchCommitRequest{
		TournamentID:      tournamentID,
		CommandID:         req.CommandID,
		CommandType:       req.Type,
		Outcome:           res,
		Events:            events,
		Decision:          decision,
		Command:           cmd,
		MatchResultSource: matchSource,
		ProjectionChanged: differentialProjectionChanged(decision, out),
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return s.canonicalRoundMatchOutcome(req.CommandID, prior), nil
		}
		return envelope.Result{}, err
	}
	if differentialProjectionChanged(decision, out) {
		loaded := uow.Loaded()
		s.refreshBracketBestEffort(context.Background(), s.scopeForSlot(
			context.Background(), tournamentID, loaded.RoundNumber, string(loaded.Slot.SlotID),
		))
	}
	return s.canonicalRoundMatchOutcome(req.CommandID, res), nil
}

// canonicalRoundMatchOutcome re-reads command_idempotency so winners and losers
// both observe the JSONB-canonical stored body.
func (s *Service) canonicalRoundMatchOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s == nil || commandID == "" {
		return fallback
	}
	if s.roundMatches != nil {
		if stored, ok := s.roundMatches.LookupOutcome(commandID); ok {
			return stored
		}
	}
	if s.repo != nil {
		if stored, ok := s.repo.LookupOutcome(commandID); ok {
			return stored
		}
	}
	return fallback
}

// UsesRoundMatchDifferential reports whether Kafka MatchCompleted / HTTP RecordMatchResult
// use the bounded RoundMatch path.
func (s *Service) UsesRoundMatchDifferential() bool {
	return s != nil && s.roundMatches != nil
}

func (s *Service) submitCommandRecordMatchDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectRoundMatch(ctx, req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	if prior, ok := s.roundMatches.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	trimmed := bytes.TrimSpace(req.Payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' || !json.Valid(trimmed) {
		return s.rejectRoundMatch(ctx, req, correlationID, "", "invalid_payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(trimmed, &payload); err != nil || payload == nil {
		return s.rejectRoundMatch(ctx, req, correlationID, "", "invalid_payload")
	}
	tournamentID := stringField(payload, "tournamentId")
	standings, err := parseStandings(payload["standings"])
	if err != nil {
		return s.rejectRoundMatch(ctx, req, correlationID, tournamentID, "invalid_standings")
	}
	eventID := stringField(payload, "eventId")
	roomID := stringField(payload, "roomId")
	completionVersion := uintField(payload, "completionVersion")
	roundNumber := intField(payload, "roundNumber")
	slotID := domain.SlotID(stringField(payload, "slotId"))
	tid := domain.TournamentID(tournamentID)
	rid := domain.RoomID(roomID)
	if !tid.Valid() || !rid.Valid() {
		return s.rejectRoundMatch(ctx, req, correlationID, tournamentID, string(domain.RejectInvalidIdentity))
	}

	uow, err := s.roundMatches.BeginRoundMatch(ctx, tournamentID, roomID, roundNumber, string(slotID), req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectRoundMatchUoW(uow, req, correlationID, tournamentID, "tournament_not_found")
	}

	if err := uow.AttachPriorResult(roomID, completionVersion); err != nil {
		return envelope.Result{}, err
	}

	// Fill only missing hints. Never overwrite nonempty claimed round/slot before policy.
	loaded := uow.Loaded()
	if roundNumber < 1 {
		roundNumber = loaded.RoundNumber
	}
	if !slotID.Valid() {
		slotID = loaded.Slot.SlotID
	}

	recordCmd := domain.RecordMatchResultCommand{
		CommandID:         domain.CommandID(req.CommandID),
		EventID:           domain.EventID(eventID),
		RoomID:            rid,
		RoundNumber:       roundNumber,
		SlotID:            slotID,
		CompletionVersion: domain.CompletionVersion(completionVersion),
		Standings:         standings,
		IsAbandoned:       boolField(payload, "isAbandoned"),
	}
	decision := domain.DecideRecordMatchResult(loaded, recordCmd)

	var matchSource *MatchResultSource
	if eventID != "" && roomID != "" && completionVersion > 0 {
		matchSource = &MatchResultSource{
			EventID:           eventID,
			RoomID:            roomID,
			CompletionVersion: completionVersion,
		}
	}
	return s.commitRoundMatchOutcome(uow, req, decision, recordCmd, tournamentID, correlationID, matchSource)
}

func (s *Service) rejectRoundMatch(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		return s.reject(req, correlationID, tournamentID, reason)
	}
	uow, err := s.roundMatches.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if correlationID == "" {
		correlationID = req.CommandID
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if err := uow.Commit(RoundMatchCommitRequest{
		TournamentID: tournamentID,
		CommandID:    req.CommandID,
		CommandType:  req.Type,
		Outcome:      res,
		Decision:     domain.RoundMatchDecision{Kind: domain.RoundMatchReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return s.canonicalRoundMatchOutcome(req.CommandID, prior), nil
		}
		return envelope.Result{}, err
	}
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalRoundMatchOutcome(req.CommandID, res), nil
}

func (s *Service) rejectRoundMatchUoW(uow RoundMatchUnitOfWork, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if err := uow.Commit(RoundMatchCommitRequest{
		TournamentID: tournamentID,
		CommandID:    req.CommandID,
		CommandType:  req.Type,
		Outcome:      res,
		Decision:     domain.RoundMatchDecision{Kind: domain.RoundMatchReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return s.canonicalRoundMatchOutcome(req.CommandID, prior), nil
		}
		return envelope.Result{}, err
	}
	rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalRoundMatchOutcome(req.CommandID, res), nil
}

// differentialProjectionChanged gates public T1 metadata bumps for the hot path.
// Exact duplicate / reject / held / non-affecting unknown-room quarantine never bump.
func differentialProjectionChanged(d domain.RoundMatchDecision, out domain.CommandOutcome) bool {
	switch d.Kind {
	case domain.RoundMatchReject, domain.RoundMatchDuplicateEvent, domain.RoundMatchExactDuplicate, domain.RoundMatchQuarantineHeld:
		return false
	case domain.RoundMatchQuarantineUnresolved:
		if !d.AffectsSlot {
			return false
		}
	}
	return projectionChangedFromOutcome(out)
}

// UsesRegistrationDifferential reports whether Create/Register/Close use the bounded T-Reg path.
func (s *Service) UsesRegistrationDifferential() bool {
	return s != nil && s.registrations != nil
}

func (s *Service) submitCommandRegistrationDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectRegistration(ctx, req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	cmd := envelope.Command{
		CommandID:     req.CommandID,
		Type:          req.Type,
		SchemaVersion: req.SchemaVersion,
		Payload:       req.Payload,
	}
	if err := cmd.Validate(); err != nil {
		return envelope.Result{}, err
	}
	if prior, ok := s.registrations.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return envelope.Result{}, fmt.Errorf("payload must be a JSON object")
	}
	tournamentID := stringField(payload, "tournamentId")
	switch req.Type {
	case CmdCreateTournament:
		return s.createTournamentDifferential(ctx, req, payload, correlationID)
	case CmdRegisterPlayer:
		return s.registerPlayerDifferential(ctx, req, payload, tournamentID, correlationID)
	case CmdCloseRegistration:
		return s.closeRegistrationDifferential(ctx, req, tournamentID, correlationID)
	default:
		return s.rejectRegistration(ctx, req, correlationID, tournamentID, "unknown_command_type")
	}
}

func (s *Service) createTournamentDifferential(ctx context.Context, req CommandRequest, payload map[string]any, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(stringField(payload, "tournamentId"))
	if !tid.Valid() {
		return s.rejectRegistration(ctx, req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	uow, err := s.registrations.BeginCreateTournament(ctx, string(tid), req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if uow.Exists() {
		return s.rejectRegistrationUoW(uow, req, correlationID, string(tid), "tournament_already_exists")
	}
	createCmd := domain.CreateTournamentCommand{
		CommandID:    domain.CommandID(req.CommandID),
		TournamentID: tid,
		Capacity:     intField(payload, "capacity"),
		RetryBudget:  intField(payload, "retryBudget"),
		BatchSize:    intField(payload, "batchSize"),
		Visibility:   domain.TournamentVisibility(stringField(payload, "visibility")),
	}
	decision := domain.DecideCreateTournament(createCmd)
	return s.commitRegistrationOutcome(uow, req, decision, createCmd, string(tid), correlationID, true)
}

func (s *Service) registerPlayerDifferential(ctx context.Context, req CommandRequest, payload map[string]any, tournamentID, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	if !tid.Valid() {
		return s.rejectRegistration(ctx, req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	playerID := stringField(payload, "playerId")
	if playerID == "" {
		playerID = req.PlayerID
	}
	regCmd := domain.RegisterPlayerCommand{
		CommandID: domain.CommandID(req.CommandID),
		PlayerID:  domain.PlayerID(playerID),
	}
	if !regCmd.CommandID.Valid() || !regCmd.PlayerID.Valid() {
		decision := domain.DecideRegisterPlayer(domain.RegistrationContext{
			Exists: true, Phase: domain.PhaseRegistration,
		}, regCmd)
		return s.persistRegistrationRejectStandalone(ctx, req, correlationID, tournamentID, decision)
	}

	uow, err := s.registrations.BeginRegisterPlayer(ctx, string(tid), playerID, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectRegistrationUoW(uow, req, correlationID, tournamentID, "tournament_not_found")
	}

	decision := domain.DecideRegisterPlayer(uow.RegisterContext(), regCmd)
	if decision.Kind != domain.RegistrationReserve {
		return s.finalizeRegistrationDecision(uow, req, decision, tournamentID, correlationID)
	}

	_, autoClosed, err := uow.ReserveRegistration()
	if store.IsRegistrationAlreadyPresent(err) {
		decision = domain.DecideRegisterPlayer(domain.RegistrationContext{
			TournamentID: tid, Exists: true, Phase: domain.PhaseRegistration, PlayerRegistered: true,
		}, regCmd)
		return s.finalizeRegistrationDecision(uow, req, decision, tournamentID, correlationID)
	}
	if store.IsRegistrationCapacityExceeded(err) {
		decision = domain.RegistrationDecision{
			Kind:    domain.RegistrationReject,
			Outcome: domain.CapacityExceededOutcome(regCmd.CommandID),
		}
		return s.finalizeRegistrationDecision(uow, req, decision, tournamentID, correlationID)
	}
	if err != nil {
		return envelope.Result{}, err
	}

	facts := []domain.Fact{domain.PlayerRegisteredFact(tid, regCmd.PlayerID)}
	if autoClosed {
		facts = append(facts, domain.RegistrationClosedFact(tid, uow.RegisterContext().Capacity))
	}
	decision = domain.RegistrationDecision{
		Kind:    domain.RegistrationReserve,
		Outcome: domain.AcceptedWithFacts(regCmd.CommandID, facts),
	}
	return s.finalizeRegistrationDecision(uow, req, decision, tournamentID, correlationID)
}

func (s *Service) closeRegistrationDifferential(ctx context.Context, req CommandRequest, tournamentID, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	if !tid.Valid() {
		return s.rejectRegistration(ctx, req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	uow, err := s.registrations.BeginCloseRegistration(ctx, string(tid), req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectRegistrationUoW(uow, req, correlationID, tournamentID, "tournament_not_found")
	}
	closeCmd := domain.CloseRegistrationCommand{CommandID: domain.CommandID(req.CommandID)}
	decision := domain.DecideCloseRegistration(uow.CloseContext(), closeCmd)
	bump := decision.Kind == domain.RegistrationClose
	return s.commitRegistrationOutcome(uow, req, decision, domain.CreateTournamentCommand{}, tournamentID, correlationID, bump)
}

func (s *Service) finalizeRegistrationDecision(uow RegistrationUnitOfWork, req CommandRequest, decision domain.RegistrationDecision, tournamentID, correlationID string) (envelope.Result, error) {
	out := decision.Outcome
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.FinalizeRegister(RegistrationCommitRequest{
			Op:           string(store.RegistrationOpRegister),
			TournamentID: tournamentID,
			CommandID:    req.CommandID,
			CommandType:  req.Type,
			Outcome:      res,
			Decision:     decision,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
		rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(rec); err != nil {
			return envelope.Result{}, err
		}
		return s.canonicalRegistrationOutcome(req.CommandID, res), nil
	}
	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	if len(out.Facts) > 0 {
		now := s.clock.Now()
		for i, f := range out.Facts {
			topic := topicForFact(f.Name)
			if topic == "" {
				continue
			}
			eventID := req.CommandID + ":" + string(f.Name) + ":" + strconv.Itoa(i)
			events = append(events, OutboxEvent{
				EventID:       eventID,
				EventType:     string(f.Name),
				TournamentID:  tournamentID,
				Topic:         topic,
				PartitionKey:  tournamentID,
				SchemaVersion: 1,
				Payload:       buildTournamentIntegrationPayload(eventID, string(f.Name), correlationID, req.CommandID, tournamentID, f.Data, now),
				CreatedAt:     now,
			})
		}
	}
	payload, _ := json.Marshal(map[string]any{"facts": factsPayload})
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	if err := uow.FinalizeRegister(RegistrationCommitRequest{
		Op:           string(store.RegistrationOpRegister),
		TournamentID: tournamentID,
		CommandID:    req.CommandID,
		CommandType:  req.Type,
		Outcome:      res,
		Events:       events,
		Decision:     decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	if projectionChangedFromOutcome(out) {
		s.refreshBracketBestEffort(context.Background(), scopeSummaryOnly(tournamentID))
	}
	return s.canonicalRegistrationOutcome(req.CommandID, res), nil
}

func (s *Service) commitRegistrationOutcome(uow RegistrationUnitOfWork, req CommandRequest, decision domain.RegistrationDecision, createCmd domain.CreateTournamentCommand, tournamentID, correlationID string, bumpBase bool) (envelope.Result, error) {
	out := decision.Outcome
	op := string(store.RegistrationOpCreate)
	if req.Type == CmdCloseRegistration {
		op = string(store.RegistrationOpClose)
	}
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.Commit(RegistrationCommitRequest{
			Op: op, TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			Outcome: res, Decision: decision, CreateCmd: createCmd,
			RetryBudget: createCmd.RetryBudget, BatchSize: createCmd.BatchSize,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
		rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(rec); err != nil {
			return envelope.Result{}, err
		}
		return s.canonicalRegistrationOutcome(req.CommandID, res), nil
	}
	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	if len(out.Facts) > 0 {
		now := s.clock.Now()
		for i, f := range out.Facts {
			topic := topicForFact(f.Name)
			if topic == "" {
				continue
			}
			eventID := req.CommandID + ":" + string(f.Name) + ":" + strconv.Itoa(i)
			events = append(events, OutboxEvent{
				EventID:       eventID,
				EventType:     string(f.Name),
				TournamentID:  tournamentID,
				Topic:         topic,
				PartitionKey:  tournamentID,
				SchemaVersion: 1,
				Payload:       buildTournamentIntegrationPayload(eventID, string(f.Name), correlationID, req.CommandID, tournamentID, f.Data, now),
				CreatedAt:     now,
			})
		}
	}
	payload, _ := json.Marshal(map[string]any{"facts": factsPayload})
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	if err := uow.Commit(RegistrationCommitRequest{
		Op: op, TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		Outcome: res, Events: events, Decision: decision, CreateCmd: createCmd,
		RetryBudget: createCmd.RetryBudget, BatchSize: createCmd.BatchSize,
		BumpBaseProjection: bumpBase && projectionChangedFromOutcome(out),
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	if bumpBase && projectionChangedFromOutcome(out) {
		s.refreshBracketBestEffort(context.Background(), scopeSummaryOnly(tournamentID))
	}
	return s.canonicalRegistrationOutcome(req.CommandID, res), nil
}

// canonicalRegistrationOutcome re-reads command_idempotency so winners and losers
// both observe the JSONB-canonical stored body (not a locally constructed payload).
func (s *Service) canonicalRegistrationOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s == nil || commandID == "" {
		return fallback
	}
	if s.registrations != nil {
		if stored, ok := s.registrations.LookupOutcome(commandID); ok {
			return stored
		}
	}
	if s.repo != nil {
		if stored, ok := s.repo.LookupOutcome(commandID); ok {
			return stored
		}
	}
	return fallback
}

func (s *Service) rejectRegistration(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		return s.reject(req, correlationID, tournamentID, reason)
	}
	uow, err := s.registrations.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if correlationID == "" {
		correlationID = req.CommandID
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if err := uow.Commit(RegistrationCommitRequest{
		Op:           string(store.RegistrationOpStandalone),
		TournamentID: tournamentID,
		CommandID:    req.CommandID,
		CommandType:  req.Type,
		Outcome:      res,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalRegistrationOutcome(req.CommandID, res), nil
}

func (s *Service) rejectRegistrationUoW(uow RegistrationUnitOfWork, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID != "" {
		if prior, ok := uow.LookupOutcome(req.CommandID); ok {
			return prior, nil
		}
	}
	if correlationID == "" {
		correlationID = req.CommandID
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if req.CommandID != "" {
		commitReq := RegistrationCommitRequest{
			TournamentID: tournamentID,
			CommandID:    req.CommandID,
			CommandType:  req.Type,
			Outcome:      res,
		}
		var err error
		switch req.Type {
		case CmdRegisterPlayer:
			commitReq.Op = string(store.RegistrationOpRegister)
			err = uow.FinalizeRegister(commitReq)
		case CmdCloseRegistration:
			commitReq.Op = string(store.RegistrationOpClose)
			err = uow.Commit(commitReq)
		default:
			commitReq.Op = string(store.RegistrationOpCreate)
			err = uow.Commit(commitReq)
		}
		if err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
	} else {
		_ = uow.Rollback()
	}
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalRegistrationOutcome(req.CommandID, res), nil
}

func (s *Service) persistRegistrationRejectStandalone(ctx context.Context, req CommandRequest, correlationID, tournamentID string, decision domain.RegistrationDecision) (envelope.Result, error) {
	reason := string(domain.RejectInvalidIdentity)
	if decision.Outcome.Rejection != nil {
		reason = string(decision.Outcome.Rejection.Code)
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if req.CommandID != "" {
		uow, err := s.registrations.BeginStandaloneCommand(ctx, req.CommandID)
		if err != nil {
			return envelope.Result{}, err
		}
		defer func() { _ = uow.Rollback() }()
		if prior, ok := uow.LookupOutcome(req.CommandID); ok {
			return prior, nil
		}
		if err := uow.Commit(RegistrationCommitRequest{
			Op:           string(store.RegistrationOpStandalone),
			TournamentID: tournamentID,
			CommandID:    req.CommandID,
			CommandType:  req.Type,
			Outcome:      res,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
	}
	rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalRegistrationOutcome(req.CommandID, res), nil
}

func (s *Service) submitCommandSeedingDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectSeeding(ctx, req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	cmd := envelope.Command{
		CommandID:     req.CommandID,
		Type:          req.Type,
		SchemaVersion: req.SchemaVersion,
		Payload:       req.Payload,
	}
	if err := cmd.Validate(); err != nil {
		return envelope.Result{}, err
	}
	if prior, ok := s.seeding.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return envelope.Result{}, fmt.Errorf("payload must be a JSON object")
	}
	tournamentID := stringField(payload, "tournamentId")
	return s.seedRoundDifferential(ctx, req, tournamentID, intField(payload, "roundNumber"), correlationID)
}

func (s *Service) seedRoundDifferential(ctx context.Context, req CommandRequest, tournamentID string, roundNumber int, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	seedCmd := domain.SeedRoundCommand{
		CommandID:   domain.CommandID(req.CommandID),
		RoundNumber: roundNumber,
	}
	if !tid.Valid() {
		return s.rejectSeeding(ctx, req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	if roundNumber < 1 {
		decision := domain.DecideSeedRoundKickoff(domain.SeedRoundKickoffContext{Exists: true, RoundNumber: roundNumber}, seedCmd)
		return s.persistSeedingRejectStandalone(ctx, req, correlationID, tournamentID, decision)
	}

	uow, err := s.seeding.BeginSeedRound(ctx, string(tid), roundNumber, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectSeedingUoW(uow, req, correlationID, tournamentID, "tournament_not_found")
	}
	decision := domain.DecideSeedRoundKickoff(uow.KickoffContext(), seedCmd)
	return s.commitSeedingOutcome(uow, req, decision, tournamentID, correlationID)
}

func (s *Service) commitSeedingOutcome(uow SeedingUnitOfWork, req CommandRequest, decision domain.SeedRoundKickoffDecision, tournamentID, correlationID string) (envelope.Result, error) {
	out := decision.Outcome
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.Commit(SeedingCommitRequest{
			TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			CorrelationID: correlationID, Outcome: res, Decision: decision,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
		rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(rec); err != nil {
			return envelope.Result{}, err
		}
		return s.canonicalSeedingOutcome(req.CommandID, res), nil
	}
	payload, _ := json.Marshal(map[string]any{"facts": []any{}})
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	if err := uow.Commit(SeedingCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Decision: decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	return s.canonicalSeedingOutcome(req.CommandID, res), nil
}

func (s *Service) canonicalSeedingOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s == nil || commandID == "" {
		return fallback
	}
	if s.seeding != nil {
		if stored, ok := s.seeding.LookupOutcome(commandID); ok {
			return stored
		}
	}
	if s.repo != nil {
		if stored, ok := s.repo.LookupOutcome(commandID); ok {
			return stored
		}
	}
	return fallback
}

func (s *Service) rejectSeeding(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		return s.reject(req, correlationID, tournamentID, reason)
	}
	uow, err := s.seeding.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if correlationID == "" {
		correlationID = req.CommandID
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if err := uow.Commit(SeedingCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Decision: domain.SeedRoundKickoffDecision{Kind: domain.SeedKickoffReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalSeedingOutcome(req.CommandID, res), nil
}

func (s *Service) rejectSeedingUoW(uow SeedingUnitOfWork, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if err := uow.Commit(SeedingCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Decision: domain.SeedRoundKickoffDecision{Kind: domain.SeedKickoffReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return s.canonicalSeedingOutcome(req.CommandID, res), nil
}

func (s *Service) persistSeedingRejectStandalone(ctx context.Context, req CommandRequest, correlationID, tournamentID string, decision domain.SeedRoundKickoffDecision) (envelope.Result, error) {
	uow, err := s.seeding.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	return s.commitSeedingOutcome(uow, req, decision, tournamentID, correlationID)
}

// Memory/capability RecordMatchResult still uses BeginExisting + persistTournamentTx
// via recordResult when RoundMatches is nil.

func validateMatchCompletedEvent(evt MatchCompletedEvent) error {
	if evt.SchemaVersion != envelope.CurrentSchemaVersion {
		return fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	if evt.EventType != eventTypeMatchCompleted {
		return fmt.Errorf("eventType must be %s", eventTypeMatchCompleted)
	}
	if strings.TrimSpace(evt.EventID) == "" {
		return fmt.Errorf("eventId is required")
	}
	if !evt.HasIsAbandoned {
		return fmt.Errorf("isAbandoned is required")
	}
	if strings.TrimSpace(evt.RoomID) == "" {
		return fmt.Errorf("roomId is required")
	}
	if strings.TrimSpace(evt.TournamentID) == "" {
		return fmt.Errorf("tournamentId is required")
	}
	if evt.CompletionVersion == 0 {
		return fmt.Errorf("completionVersion is required")
	}
	if len(evt.Players) == 0 {
		return fmt.Errorf("players are required")
	}
	return nil
}

// DrainOutbox publishes pending facts; publish failure leaves them pending for retry.
func (s *Service) DrainOutbox(ctx context.Context, limit int) (int, error) {
	pending, err := s.repo.ListPendingOutbox(limit)
	if err != nil {
		return 0, err
	}
	published := 0
	for _, e := range pending {
		if err := s.publisher.Publish(ctx, e); err != nil {
			return published, err
		}
		if err := s.repo.MarkOutboxPublished(e.EventID, s.clock.Now()); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

func dispositionFromResult(r envelope.Result) string {
	if r.Status == envelope.StatusRejected {
		return "rejected"
	}
	payload := string(r.Payload)
	if strings.Contains(payload, "TournamentResultQuarantined") {
		return "quarantined"
	}
	if strings.Contains(payload, "TournamentMatchResultRecorded") {
		return "recorded"
	}
	// Exact empty-facts accepted outcomes (duplicate ignored) stay stable on replay.
	if strings.Contains(payload, `"facts":[]`) || strings.Contains(payload, `"facts": []`) {
		return "duplicate_ignored"
	}
	return "accepted"
}

// RebuildBracketProjection rebuilds the Redis bracket projection from Postgres (operator repair).
func (s *Service) RebuildBracketProjection(ctx context.Context, tournamentID string) error {
	if s == nil || s.bracketRefresh == nil {
		return store.ErrBracketProjectionUnavailable
	}
	return s.bracketRefresh.Rebuild(ctx, tournamentID)
}

// Bracket returns a bounded public BracketPage (ADR-0038 / OpenAPI).
// When a durable BracketPageLoader is wired, Redis-first / Postgres keyset queries are used
// (never whole-bracket hydrate). Otherwise the in-memory aggregate is paginated.
func (s *Service) Bracket(tournamentID string, roundNumber *int, cursor string, limit int) (map[string]any, error) {
	if limit <= 0 {
		limit = store.DefaultBracketPageLimit
	}
	if limit > store.MaxBracketPageLimit {
		return nil, fmt.Errorf("%w: limit", store.ErrInvalidBracketPageQuery)
	}
	if roundNumber != nil && *roundNumber < 1 {
		return nil, fmt.Errorf("%w: roundNumber", store.ErrInvalidBracketPageQuery)
	}

	if s.bracketPages != nil {
		page, err := s.bracketPages.LoadBracketPage(context.Background(), store.BracketPageQuery{
			TournamentID: tournamentID,
			RoundNumber:  roundNumber,
			Cursor:       cursor,
			Limit:        limit,
		})
		if err != nil {
			return nil, err
		}
		return bracketPageToMap(page), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.repo.Get(domain.TournamentID(tournamentID))
	if !ok {
		return nil, errBracketNotFound
	}
	ver, generatedAt := s.projectionVersion(tournamentID)
	page, err := buildMemoryBracketPage(t, roundNumber, cursor, limit, s.clock.Now(), ver, generatedAt)
	if err != nil {
		return nil, err
	}
	return bracketPageToMap(page), nil
}

var errBracketNotFound = errors.New("tournament not found")

func (s *Service) projectionVersion(tournamentID string) (int64, time.Time) {
	if p, ok := s.repo.(bracketProjectionVersions); ok {
		return p.ProjectionCheckpoint(domain.TournamentID(tournamentID))
	}
	return 0, s.clock.Now()
}

type bracketProjectionVersions interface {
	ProjectionCheckpoint(id domain.TournamentID) (int64, time.Time)
}

func buildMemoryBracketPage(t *domain.Tournament, roundNumber *int, cursor string, limit int, now time.Time, ver int64, generatedAt time.Time) (store.BracketPage, error) {
	var after *store.BracketCursor
	if strings.TrimSpace(cursor) != "" {
		c, err := store.DecodeBracketCursor(cursor)
		if err != nil {
			return store.BracketPage{}, err
		}
		if roundNumber != nil && c.RoundNumber != *roundNumber {
			return store.BracketPage{}, fmt.Errorf("%w: cursor round mismatch", store.ErrInvalidBracketCursor)
		}
		after = &c
	}

	rounds := t.RoundsSnapshot()
	summaryRounds := make([]store.BracketRoundSummary, 0, len(rounds))
	allSlots := make([]store.BracketSlotView, 0)
	for _, r := range rounds {
		summaryRounds = append(summaryRounds, store.BracketRoundSummary{
			RoundNumber: r.Number,
			Status:      string(r.Status),
			IsFinal:     r.IsFinal,
			SlotCount:   len(r.Slots),
			Completed:   r.Completed,
			BatchCount:  len(r.Batches),
		})
		if roundNumber != nil && r.Number != *roundNumber {
			continue
		}
		for _, slot := range r.Slots {
			if after != nil {
				if r.Number < after.RoundNumber || (r.Number == after.RoundNumber && slot.Index <= after.SlotIndex) {
					continue
				}
			}
			v := store.BracketSlotView{
				RoundNumber:        r.Number,
				SlotIndex:          slot.Index,
				SlotID:             string(slot.SlotID),
				Status:             string(slot.Status),
				SeededPlayerIDs:    playerIDs(slot.SeededPlayers),
				RoomID:             string(slot.RoomID),
				BatchID:            string(slot.BatchID),
				AdvancingPlayerIDs: playerIDs(slot.Advancing),
				CompletionVersion:  uint64(slot.CompletionVersion),
				QuarantineReason:   slot.QuarantineReason,
			}
			allSlots = append(allSlots, v)
		}
	}

	var next string
	if len(allSlots) > limit {
		last := allSlots[limit-1]
		enc, err := store.EncodeBracketCursor(store.BracketCursor{RoundNumber: last.RoundNumber, SlotIndex: last.SlotIndex})
		if err != nil {
			return store.BracketPage{}, err
		}
		next = enc
		allSlots = allSlots[:limit]
	}
	if generatedAt.IsZero() {
		generatedAt = now
	}
	return store.BracketPage{
		TournamentID:      string(t.ID()),
		ProjectionVersion: ver,
		GeneratedAt:       generatedAt.UTC(),
		Summary: store.BracketSummary{
			Phase:           string(t.Phase()),
			Capacity:        t.Capacity(),
			RegisteredCount: t.RegisteredCount(),
			CurrentRound:    t.CurrentRound(),
			Rounds:          summaryRounds,
		},
		Slots:      allSlots,
		NextCursor: next,
	}, nil
}

func bracketPageToMap(page store.BracketPage) map[string]any {
	out := map[string]any{
		"tournamentId":      page.TournamentID,
		"projectionVersion": page.ProjectionVersion,
		"generatedAt":       page.GeneratedAt.UTC().Format(time.RFC3339Nano),
		"summary":           page.Summary,
		"slots":             page.Slots,
	}
	if page.NextCursor != "" {
		out["nextCursor"] = page.NextCursor
	}
	return out
}

// Standings returns the bounded public standings projection (ADR-0038 / OpenAPI).
// When a durable StandingsLoader is wired, Postgres one-statement snapshot is used
// (never whole-aggregate hydrate / Service.mu). Otherwise the in-memory aggregate
// is read under mu and finalStandings are derived only from the final round result.
func (s *Service) Standings(tournamentID string) (map[string]any, error) {
	if s.standings != nil {
		page, err := s.standings.LoadStandingsProjection(context.Background(), tournamentID)
		if err != nil {
			return nil, err
		}
		return standingsProjectionToMap(page), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.repo.Get(domain.TournamentID(tournamentID))
	if !ok {
		return nil, errBracketNotFound
	}
	ver, generatedAt := s.projectionVersion(tournamentID)
	if generatedAt.IsZero() {
		generatedAt = s.clock.Now()
	}
	finalStandings, err := memoryFinalStandings(t)
	if err != nil {
		return nil, err
	}
	return standingsProjectionToMap(store.StandingsProjection{
		TournamentID:      string(t.ID()),
		ProjectionVersion: ver,
		GeneratedAt:       generatedAt.UTC(),
		Phase:             string(t.Phase()),
		RegisteredCount:   t.RegisteredCount(),
		CurrentRound:      t.CurrentRound(),
		FinalStandings:    finalStandings,
	}), nil
}

func memoryFinalStandings(t *domain.Tournament) ([]string, error) {
	rounds := t.RoundsSnapshot()
	for _, r := range rounds {
		if !r.IsFinal {
			continue
		}
		if len(r.Slots) == 0 {
			return []string{}, nil
		}
		slot := r.Slots[0]
		if !slot.HasResult {
			return []string{}, nil
		}
		// Prefer Advancing (RankStandings order). Fall back to Standings player IDs.
		ids := append([]domain.PlayerID(nil), slot.Advancing...)
		if len(ids) == 0 && len(slot.Standings) > 0 {
			ids = make([]domain.PlayerID, 0, len(slot.Standings))
			for _, st := range slot.Standings {
				ids = append(ids, st.PlayerID)
			}
		}
		if len(ids) == 0 {
			return []string{}, nil
		}
		if err := domain.ValidateFinalStandings(ids); err != nil {
			return nil, fmt.Errorf("%w: %v", store.ErrMalformedStandings, err)
		}
		out := make([]string, len(ids))
		for i, id := range ids {
			out[i] = string(id)
		}
		return out, nil
	}
	return []string{}, nil
}

func standingsProjectionToMap(page store.StandingsProjection) map[string]any {
	final := page.FinalStandings
	if final == nil {
		final = []string{}
	}
	return map[string]any{
		"tournamentId":      page.TournamentID,
		"projectionVersion": page.ProjectionVersion,
		"generatedAt":       page.GeneratedAt.UTC().Format(time.RFC3339Nano),
		"phase":             page.Phase,
		"registeredCount":   page.RegisteredCount,
		"currentRound":      page.CurrentRound,
		"finalStandings":    final,
	}
}

func (s *Service) commitOutcome(uow TournamentUnitOfWork, req CommandRequest, t *domain.Tournament, out domain.CommandOutcome, tournamentID, correlationID string, matchSource *MatchResultSource, extraPayload ...map[string]any) (envelope.Result, error) {
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.Commit(CommitRequest{
			Tournament:        t,
			CommandID:         req.CommandID,
			Outcome:           res,
			MatchResultSource: matchSource,
			ProjectionChanged: false,
		}); err != nil {
			return envelope.Result{}, err
		}
		rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(rec); err != nil {
			return envelope.Result{}, err
		}
		return res, nil
	}

	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	// Persist facts for accepted and duplicate outcomes so a prior failed publish/commit
	// cannot lose durable outbox rows (idempotent by EventID inside Commit).
	// Only contract-defined Kafka topics get durable integration outbox rows.
	if len(out.Facts) > 0 {
		now := s.clock.Now()
		for i, f := range out.Facts {
			topic := topicForFact(f.Name)
			if topic == "" {
				continue
			}
			eventID := req.CommandID + ":" + string(f.Name) + ":" + strconv.Itoa(i)
			events = append(events, OutboxEvent{
				EventID:       eventID,
				EventType:     string(f.Name),
				TournamentID:  tournamentID,
				Topic:         topic,
				PartitionKey:  tournamentID,
				SchemaVersion: 1,
				Payload:       buildTournamentIntegrationPayload(eventID, string(f.Name), correlationID, req.CommandID, tournamentID, f.Data, now),
				CreatedAt:     now,
			})
		}
	}
	payloadMap := map[string]any{"facts": factsPayload}
	for _, extra := range extraPayload {
		for k, v := range extra {
			payloadMap[k] = v
		}
	}
	payload, _ := json.Marshal(payloadMap)
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	if err := uow.Commit(CommitRequest{
		Tournament:        t,
		CommandID:         req.CommandID,
		Outcome:           res,
		Events:            events,
		MatchResultSource: matchSource,
		ProjectionChanged: projectionChangedFromOutcome(out),
	}); err != nil {
		return envelope.Result{}, err
	}
	return res, nil
}

// projectionChangedFromOutcome reports whether an accepted domain outcome changed
// BracketPage-visible state (phase, registeredCount, rounds/slots/result/advancement,
// or public round status). Hidden provisioning-batch status churn must not bump.
func projectionChangedFromOutcome(out domain.CommandOutcome) bool {
	if !out.Accepted() {
		return false
	}
	for _, f := range out.Facts {
		if factChangesPublicBracketProjection(f) {
			return true
		}
	}
	return false
}

func factChangesPublicBracketProjection(f domain.Fact) bool {
	switch f.Name {
	case domain.FactTournamentProvisioningBatchRetried:
		return false
	case domain.FactTournamentProvisioningBatchCompleted,
		domain.FactTournamentProvisioningBatchQuarantined:
		return f.Data[domain.FactDataPublicBracketVisible] == "true"
	case domain.FactTournamentCreated,
		domain.FactPlayerRegisteredInTournament,
		domain.FactTournamentRegistrationClosed,
		domain.FactTournamentRoundSeeded,
		domain.FactTournamentMatchAssigned,
		domain.FactTournamentMatchResultRecorded,
		domain.FactPlayersAdvanced,
		domain.FactTournamentRoundCompleted,
		domain.FactTournamentCompleted,
		domain.FactTournamentCancelled,
		domain.FactTournamentResultQuarantined:
		return true
	default:
		return false
	}
}

func (s *Service) rejectUoW(uow TournamentUnitOfWork, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID != "" {
		if prior, ok := uow.LookupOutcome(req.CommandID); ok {
			return prior, nil
		}
	}
	if correlationID == "" {
		correlationID = req.CommandID
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if req.CommandID != "" {
		if err := uow.Commit(CommitRequest{
			CommandID: req.CommandID,
			Outcome:   res,
		}); err != nil {
			return envelope.Result{}, err
		}
	} else {
		_ = uow.Rollback()
	}
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return res, nil
}

func (s *Service) reject(req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID != "" {
		if prior, ok := s.repo.LookupOutcome(req.CommandID); ok {
			return prior, nil
		}
	}
	if correlationID == "" {
		correlationID = req.CommandID
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	if req.CommandID != "" {
		if err := s.repo.Commit(CommitRequest{
			CommandID: req.CommandID,
			Outcome:   res,
		}); err != nil {
			return envelope.Result{}, err
		}
	}
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(rec); err != nil {
		return envelope.Result{}, err
	}
	return res, nil
}

func topicForFact(name domain.FactName) string {
	switch name {
	case domain.FactTournamentMatchAssigned:
		return "tournament.match.assigned"
	case domain.FactTournamentMatchResultRecorded:
		return "tournament.match.result_recorded"
	case domain.FactPlayersAdvanced:
		return "tournament.players.advanced"
	case domain.FactTournamentRoundCompleted:
		return "tournament.round.completed"
	case domain.FactTournamentCompleted:
		return "tournament.completed"
	default:
		// Non-contract facts stay internal-only (no durable Kafka/outbox row).
		return ""
	}
}

// buildTournamentIntegrationPayload builds the canonical AsyncAPI envelope for a
// contract tournament topic. Fact.Data string maps are coerced to typed JSON fields.
func buildTournamentIntegrationPayload(
	eventID, eventType, correlationID, causationID, tournamentID string,
	data map[string]string,
	occurredAt time.Time,
) map[string]any {
	at := occurredAt.UTC().Format(time.RFC3339)
	// Callers pass a clock-derived time; zero remains the deterministic zero-UTC stamp
	// (no hidden wall-clock fallback).
	payload := map[string]any{
		"schemaVersion": 1,
		"eventId":       eventID,
		"eventType":     eventType,
		"correlationId": firstNonEmpty(correlationID, causationID),
		"causationId":   causationID,
		"occurredAt":    at,
		"tournamentId":  firstNonEmpty(data["tournamentId"], tournamentID),
	}
	switch domain.FactName(eventType) {
	case domain.FactTournamentMatchAssigned:
		payload["roundNumber"] = atoiDefault(data["roundNumber"], 0)
		payload["slotId"] = data["slotId"]
		payload["roomId"] = data["roomId"]
		if data["batchId"] != "" {
			payload["batchId"] = data["batchId"]
		}
	case domain.FactTournamentMatchResultRecorded:
		payload["roomId"] = data["roomId"]
		payload["completionVersion"] = atoiDefault(data["completionVersion"], 0)
		if data["slotId"] != "" {
			payload["slotId"] = data["slotId"]
		}
		if data["roundNumber"] != "" {
			payload["roundNumber"] = atoiDefault(data["roundNumber"], 0)
		}
	case domain.FactPlayersAdvanced:
		payload["roundNumber"] = atoiDefault(data["roundNumber"], 0)
		payload["sourceSlotId"] = data["sourceSlotId"]
		payload["advancingPlayerIds"] = splitCSV(data["advancingPlayerIds"])
		if data["rule"] != "" {
			payload["rule"] = data["rule"]
		}
	case domain.FactTournamentRoundCompleted:
		payload["roundNumber"] = atoiDefault(data["roundNumber"], 0)
		if data["remainingPlayers"] != "" {
			payload["remainingPlayers"] = atoiDefault(data["remainingPlayers"], 0)
		}
		if data["isFinal"] != "" {
			payload["isFinal"] = data["isFinal"] == "true"
		}
	case domain.FactTournamentCompleted:
		payload["finalStandings"] = splitCSV(data["finalStandings"])
		if data["completionReason"] != "" {
			payload["completionReason"] = data["completionReason"]
		}
	}
	return payload
}

func atoiDefault(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func stringField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case json.Number:
		return t.String()
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func intField(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(t))
		return n
	default:
		return 0
	}
}

func uintField(m map[string]any, key string) uint64 {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return uint64(t)
	case int:
		return uint64(t)
	case int64:
		return uint64(t)
	case uint64:
		return t
	case json.Number:
		n, _ := t.Int64()
		return uint64(n)
	case string:
		n, _ := strconv.ParseUint(strings.TrimSpace(t), 10, 64)
		return n
	default:
		return 0
	}
}

func boolField(m map[string]any, key string) bool {
	if m == nil {
		return false
	}
	v, ok := m[key]
	if !ok || v == nil {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func parseStandings(raw any) ([]domain.PlayerMatchStanding, error) {
	if raw == nil {
		return nil, fmt.Errorf("standings required")
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		PlayerID             string `json:"playerId"`
		MatchWins            int    `json:"matchWins"`
		CumulativeCardPoints int    `json:"cumulativeCardPoints"`
		FinalGameCompletedAt string `json:"finalGameCompletedAt"`
		Forfeited            bool   `json:"forfeited"`
	}
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, err
	}
	out := make([]domain.PlayerMatchStanding, 0, len(rows))
	for _, r := range rows {
		var at time.Time
		if r.FinalGameCompletedAt != "" {
			at, err = time.Parse(time.RFC3339Nano, r.FinalGameCompletedAt)
			if err != nil {
				at, err = time.Parse(time.RFC3339, r.FinalGameCompletedAt)
				if err != nil {
					return nil, fmt.Errorf("invalid finalGameCompletedAt")
				}
			}
		}
		out = append(out, domain.PlayerMatchStanding{
			PlayerID:             domain.PlayerID(r.PlayerID),
			MatchWins:            r.MatchWins,
			CumulativeCardPoints: r.CumulativeCardPoints,
			FinalGameCompletedAt: at,
			Forfeited:            r.Forfeited,
		})
	}
	return out, nil
}

func playerIDs(ids []domain.PlayerID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}
