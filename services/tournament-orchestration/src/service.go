package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"unoarena/services/tournament-orchestration/domain"
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
	Repo      TournamentRepository
	Rooms     RoomProvisioner
	Publisher EventPublisher
	Audit     AuditSink
	Clock     Clock
	IDs       IDGenerator
}

// Service applies versioned commands over Tournament aggregates.
type Service struct {
	repo      TournamentRepository
	rooms     RoomProvisioner
	publisher EventPublisher
	audit     AuditSink
	clock     Clock
	ids       IDGenerator

	// mu serializes aggregate mutation + Commit. Room calls must not run while held.
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
		repo:      d.Repo,
		rooms:     rooms,
		publisher: publisher,
		audit:     auditSink,
		clock:     clock,
		ids:       ids,
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
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submitCommandLocked(req, correlationID)
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
	if existing, ok := s.repo.Get(tid); ok {
		_ = existing
		return s.reject(req, correlationID, string(tid), "tournament_already_exists")
	}
	tr, out := domain.CreateTournament(domain.CreateTournamentCommand{
		CommandID:    domain.CommandID(req.CommandID),
		TournamentID: tid,
		Capacity:     intField(payload, "capacity"),
		RetryBudget:  intField(payload, "retryBudget"),
		BatchSize:    intField(payload, "batchSize"),
	})
	return s.commitOutcome(req, tr, out, string(tid), correlationID)
}

func (s *Service) withTournament(req CommandRequest, tournamentID, correlationID string, fn func(*domain.Tournament) domain.CommandOutcome) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	if !tid.Valid() {
		return s.reject(req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	t, ok := s.repo.Get(tid)
	if !ok {
		return s.reject(req, correlationID, tournamentID, "tournament_not_found")
	}
	out := fn(t)
	return s.commitOutcome(req, t, out, tournamentID, correlationID)
}

func (s *Service) recordResult(req CommandRequest, payload map[string]any, tournamentID, correlationID string) (envelope.Result, error) {
	standings, err := parseStandings(payload["standings"])
	if err != nil {
		return s.reject(req, correlationID, tournamentID, "invalid_standings")
	}
	return s.withTournament(req, tournamentID, correlationID, func(t *domain.Tournament) domain.CommandOutcome {
		roomID := domain.RoomID(stringField(payload, "roomId"))
		roundNumber := intField(payload, "roundNumber")
		slotID := domain.SlotID(stringField(payload, "slotId"))
		if roundNumber < 1 || !slotID.Valid() {
			if rn, sid, ok := t.AssignmentByRoomID(roomID); ok {
				roundNumber = rn
				slotID = sid
			}
		}
		return t.RecordMatchResult(domain.RecordMatchResultCommand{
			CommandID:         domain.CommandID(req.CommandID),
			EventID:           domain.EventID(stringField(payload, "eventId")),
			RoomID:            roomID,
			RoundNumber:       roundNumber,
			SlotID:            slotID,
			CompletionVersion: domain.CompletionVersion(uintField(payload, "completionVersion")),
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
// Room Gameplay calls happen outside the service lock.
// Attempt identity is (batchId, retryAttempt) via commandId; a transient Room failure
// records retry state under the current attempt and returns nextRetryWork for the next attempt.
func (s *Service) ProcessProvisioningBatch(ctx context.Context, work ProvisioningBatchWork) (envelope.Result, error) {
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
	t, ok := s.repo.Get(tid)
	if !ok {
		res, err2 := s.reject(req, work.CorrelationID, work.TournamentID, "tournament_not_found")
		s.mu.Unlock()
		return res, err2
	}
	round, ok := t.Round(work.RoundNumber)
	if !ok {
		res, err2 := s.reject(req, work.CorrelationID, work.TournamentID, string(domain.RejectRoundNotFound))
		s.mu.Unlock()
		return res, err2
	}
	batch, ok := round.FindBatch(domain.BatchID(work.BatchID))
	if !ok {
		res, err2 := s.reject(req, work.CorrelationID, work.TournamentID, string(domain.RejectBatchNotFound))
		s.mu.Unlock()
		return res, err2
	}
	if batch.Status == domain.BatchQuarantined {
		res, err2 := s.reject(req, work.CorrelationID, work.TournamentID, string(domain.RejectQuarantined))
		s.mu.Unlock()
		return res, err2
	}
	if err := validateBatchRange(batch, work); err != nil {
		res, err2 := s.reject(req, work.CorrelationID, work.TournamentID, err.Error())
		s.mu.Unlock()
		return res, err2
	}
	if batch.Status == domain.BatchCompleted {
		out := t.CompleteTournamentProvisioningBatch(domain.CompleteTournamentProvisioningBatchCommand{
			CommandID:   domain.CommandID(work.CommandID),
			RoundNumber: work.RoundNumber,
			BatchID:     domain.BatchID(work.BatchID),
		})
		res, err2 := s.commitOutcome(req, t, out, work.TournamentID, work.CorrelationID)
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
	s.mu.Unlock()

	for _, slot := range slots {
		if err := s.rooms.Provision(ctx, RoomProvisionRequest{
			TournamentID: tid,
			RoundNumber:  work.RoundNumber,
			SlotID:       slot.SlotID,
			RoomID:       slot.RoomID,
			PlayerIDs:    slot.PlayerIDs,
			BatchID:      slot.BatchID,
		}); err != nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.recordProvisionFailure(req, work, err.Error())
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.repo.LookupOutcome(work.CommandID); ok {
		return prior, nil
	}
	t, ok = s.repo.Get(tid)
	if !ok {
		return s.reject(req, work.CorrelationID, work.TournamentID, "tournament_not_found")
	}
	out := t.CompleteTournamentProvisioningBatch(domain.CompleteTournamentProvisioningBatchCommand{
		CommandID:   domain.CommandID(work.CommandID),
		RoundNumber: work.RoundNumber,
		BatchID:     domain.BatchID(work.BatchID),
	})
	return s.commitOutcome(req, t, out, work.TournamentID, work.CorrelationID)
}

func (s *Service) recordProvisionFailure(req CommandRequest, work ProvisioningBatchWork, reason string) (envelope.Result, error) {
	t, ok := s.repo.Get(domain.TournamentID(work.TournamentID))
	if !ok {
		return s.reject(req, work.CorrelationID, work.TournamentID, "tournament_not_found")
	}
	round, ok := t.Round(work.RoundNumber)
	if !ok {
		return s.reject(req, work.CorrelationID, work.TournamentID, string(domain.RejectRoundNotFound))
	}
	batch, ok := round.FindBatch(domain.BatchID(work.BatchID))
	if !ok {
		return s.reject(req, work.CorrelationID, work.TournamentID, string(domain.RejectBatchNotFound))
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
		return s.commitOutcome(req, t, out, work.TournamentID, work.CorrelationID)
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
	return s.commitOutcome(req, t, out, work.TournamentID, work.CorrelationID, nextRetryWorkPayload(nextWork))
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
func (s *Service) IngestMatchCompleted(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateMatchCompletedEvent(evt); err != nil {
		return nil, err
	}
	tid := domain.TournamentID(evt.TournamentID)
	commandID := "ingest:" + evt.EventID
	if prior, ok := s.repo.LookupOutcome(commandID); ok {
		return map[string]any{
			"status":            prior.Status,
			"commandId":         prior.CommandID,
			"disposition":       dispositionFromResult(prior),
			"schemaVersion":     envelope.CurrentSchemaVersion,
			"tournamentId":      string(tid),
			"roomId":            evt.RoomID,
			"completionVersion": evt.CompletionVersion,
		}, nil
	}

	t, ok := s.repo.Get(tid)
	if !ok {
		return nil, fmt.Errorf("tournament_not_found")
	}

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
	result, err := s.commitOutcome(req, t, out, string(tid), evt.CorrelationID)
	if err != nil {
		return nil, err
	}

	disposition := "recorded"
	for _, f := range out.Facts {
		if f.Name == domain.FactTournamentResultQuarantined {
			disposition = "quarantined"
			break
		}
	}
	if out.Accepted() && len(out.Facts) == 0 {
		disposition = "duplicate_ignored"
	}
	if out.Kind == domain.OutcomeDuplicate {
		for _, f := range out.Facts {
			if f.Name == domain.FactTournamentResultQuarantined {
				disposition = "quarantined"
				break
			}
		}
	}
	return map[string]any{
		"status":            result.Status,
		"commandId":         commandID,
		"disposition":       disposition,
		"schemaVersion":     envelope.CurrentSchemaVersion,
		"tournamentId":      string(tid),
		"roomId":            evt.RoomID,
		"completionVersion": evt.CompletionVersion,
	}, nil
}

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

// Bracket returns a public bracket projection for tournamentID.
func (s *Service) Bracket(tournamentID string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.repo.Get(domain.TournamentID(tournamentID))
	if !ok {
		return nil, false
	}
	rounds := t.RoundsSnapshot()
	outRounds := make([]map[string]any, 0, len(rounds))
	for _, r := range rounds {
		slots := make([]map[string]any, 0, len(r.Slots))
		for _, slot := range r.Slots {
			slots = append(slots, map[string]any{
				"slotId":             string(slot.SlotID),
				"status":             string(slot.Status),
				"seededPlayerIds":    playerIDs(slot.SeededPlayers),
				"roomId":             string(slot.RoomID),
				"batchId":            string(slot.BatchID),
				"advancingPlayerIds": playerIDs(slot.Advancing),
				"completionVersion":  uint64(slot.CompletionVersion),
				"quarantineReason":   slot.QuarantineReason,
			})
		}
		batches := make([]map[string]any, 0, len(r.Batches))
		for _, b := range r.Batches {
			batches = append(batches, map[string]any{
				"batchId":  string(b.BatchID),
				"slotFrom": string(b.SlotFrom),
				"slotTo":   string(b.SlotTo),
				"slotSize": len(b.SlotIndexes),
				"status":   string(b.Status),
			})
		}
		outRounds = append(outRounds, map[string]any{
			"roundNumber": r.Number,
			"status":      string(r.Status),
			"isFinal":     r.IsFinal,
			"completed":   r.Completed,
			"slots":       slots,
			"batches":     batches,
		})
	}
	return map[string]any{
		"tournamentId": string(t.ID()),
		"phase":        string(t.Phase()),
		"currentRound": t.CurrentRound(),
		"capacity":     t.Capacity(),
		"rounds":       outRounds,
	}, true
}

// Standings returns a compact standings/read projection.
func (s *Service) Standings(tournamentID string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.repo.Get(domain.TournamentID(tournamentID))
	if !ok {
		return nil, false
	}
	return map[string]any{
		"tournamentId":      string(t.ID()),
		"phase":             string(t.Phase()),
		"championId":        string(t.Champion()),
		"registeredCount":   t.RegisteredCount(),
		"currentRound":      t.CurrentRound(),
		"registeredPlayers": playerIDs(t.RegisteredPlayers()),
	}, true
}

func (s *Service) commitOutcome(req CommandRequest, t *domain.Tournament, out domain.CommandOutcome, tournamentID, correlationID string, extraPayload ...map[string]any) (envelope.Result, error) {
	if prior, ok := s.repo.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := s.repo.Commit(CommitRequest{
			Tournament: t,
			CommandID:  req.CommandID,
			Outcome:    res,
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
	if len(out.Facts) > 0 {
		for i, f := range out.Facts {
			eventID := req.CommandID + ":" + string(f.Name) + ":" + strconv.Itoa(i)
			events = append(events, OutboxEvent{
				EventID:       eventID,
				EventType:     string(f.Name),
				TournamentID:  tournamentID,
				Topic:         topicForFact(f.Name),
				PartitionKey:  tournamentID,
				SchemaVersion: 1,
				Payload:       f.Data,
				CreatedAt:     s.clock.Now(),
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
	if err := s.repo.Commit(CommitRequest{
		Tournament: t,
		CommandID:  req.CommandID,
		Outcome:    res,
		Events:     events,
	}); err != nil {
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
		return "tournament.lifecycle"
	}
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
