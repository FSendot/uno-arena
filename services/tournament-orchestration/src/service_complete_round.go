package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

// ErrCompleteRoundNotReady marks a retryable CompleteRound try failure: revalidation
// rejected (incomplete/quarantined/race) and the UoW was rolled back without
// command_idempotency, outbox, or audit. Manual CmdCompleteRound still persists rejects.
var ErrCompleteRoundNotReady = errors.New("complete_round_not_ready")

// CompleteRoundNotReadyError is the typed retryable not-ready result for the worker try path.
type CompleteRoundNotReadyError struct {
	Reason string
}

func (e *CompleteRoundNotReadyError) Error() string {
	if e == nil {
		return ErrCompleteRoundNotReady.Error()
	}
	if e.Reason == "" {
		return ErrCompleteRoundNotReady.Error()
	}
	return ErrCompleteRoundNotReady.Error() + ":" + e.Reason
}

func (e *CompleteRoundNotReadyError) Unwrap() error { return ErrCompleteRoundNotReady }

func (s *Service) submitCommandCompleteRoundDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectCompleteRound(ctx, req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	if prior, ok := s.completeRounds.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	// RecordMatch parity: nonempty commandId + correct schema → malformed/empty/null/array/string
	// payload persists invalid_payload via standalone UoW (do not call envelope.Validate first).
	trimmed := bytes.TrimSpace(req.Payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' || !json.Valid(trimmed) {
		return s.rejectCompleteRound(ctx, req, correlationID, "", "invalid_payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(trimmed, &payload); err != nil || payload == nil {
		return s.rejectCompleteRound(ctx, req, correlationID, "", "invalid_payload")
	}
	tournamentID := stringField(payload, "tournamentId")
	roundNumber := intField(payload, "roundNumber")
	return s.completeRoundDifferential(ctx, req, tournamentID, roundNumber, correlationID)
}

// TryCompleteReadyRound is the non-poisoning worker path for deterministic CompleteRound.
// Uses the same BeginCompleteRound UoW / DecideCompleteRound / commit success path as
// CmdCompleteRound, but reject/not-ready decisions roll back without persisting outcome.
func (s *Service) TryCompleteReadyRound(ctx context.Context, tournamentID string, roundNumber int) (envelope.Result, error) {
	if s == nil || s.completeRounds == nil {
		return envelope.Result{}, fmt.Errorf("complete round differential not configured")
	}
	tid := domain.TournamentID(tournamentID)
	cmdID := domain.CompleteRoundCommandID(tid, roundNumber)
	if prior, ok := s.completeRounds.LookupOutcome(cmdID); ok {
		return prior, nil
	}
	payload, _ := json.Marshal(map[string]any{
		"tournamentId": tournamentID,
		"roundNumber":  roundNumber,
	})
	req := CommandRequest{
		CommandID:     cmdID,
		Type:          CmdCompleteRound,
		SchemaVersion: envelope.CurrentSchemaVersion,
		Payload:       payload,
	}
	uow, err := s.completeRounds.BeginCompleteRound(ctx, tournamentID, roundNumber, cmdID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(cmdID); ok {
		return prior, nil
	}
	completeCmd := domain.CompleteRoundCommand{
		CommandID:   domain.CommandID(cmdID),
		RoundNumber: roundNumber,
	}
	decision := domain.DecideCompleteRound(uow.Loaded(), completeCmd)
	if decision.Kind == domain.CompleteRoundReject {
		reason := string(decision.Outcome.Rejection.Code)
		if reason == "" {
			reason = string(domain.RejectRoundIncomplete)
		}
		return envelope.Result{}, &CompleteRoundNotReadyError{Reason: reason}
	}
	// Only success / already-done may persist the deterministic worker command outcome.
	return s.commitCompleteRoundOutcome(ctx, uow, req, decision, tournamentID, cmdID)
}

func (s *Service) completeRoundDifferential(ctx context.Context, req CommandRequest, tournamentID string, roundNumber int, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	completeCmd := domain.CompleteRoundCommand{
		CommandID:   domain.CommandID(req.CommandID),
		RoundNumber: roundNumber,
	}
	// Malformed / missing identity: standalone command-only reject (no barrier/tournament).
	if !completeCmd.CommandID.Valid() || roundNumber < 1 {
		decision := domain.DecideCompleteRound(domain.CompleteRoundContext{}, completeCmd)
		return s.persistCompleteRoundRejectStandalone(ctx, req, correlationID, tournamentID, decision)
	}
	if !tid.Valid() {
		decision := domain.DecideCompleteRound(domain.CompleteRoundContext{Exists: false}, completeCmd)
		return s.persistCompleteRoundRejectStandalone(ctx, req, correlationID, "", decision)
	}

	uow, err := s.completeRounds.BeginCompleteRound(ctx, string(tid), roundNumber, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	decision := domain.DecideCompleteRound(uow.Loaded(), completeCmd)
	return s.commitCompleteRoundOutcome(ctx, uow, req, decision, tournamentID, correlationID)
}

func (s *Service) commitCompleteRoundOutcome(
	ctx context.Context,
	uow CompleteRoundUnitOfWork,
	req CommandRequest,
	decision domain.CompleteRoundDecision,
	tournamentID, correlationID string,
) (envelope.Result, error) {
	out := decision.Outcome
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(ctx, rec); err != nil {
			return envelope.Result{}, err
		}
		if err := uow.Commit(CompleteRoundCommitRequest{
			TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			CorrelationID: correlationID, Outcome: res, Decision: decision,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
		return s.canonicalCompleteRoundOutcome(req.CommandID, res), nil
	}

	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	if decision.Kind == domain.CompleteRoundSuccess && len(out.Facts) > 0 {
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
	if err := uow.Commit(CompleteRoundCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Events: events, Decision: decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	recordCommittedTournamentCompletion(context.Background(), out)
	if projectionChangedFromOutcome(out) {
		// CompleteRound bumps public round status; pending next-round slots stay invisible.
		s.refreshBracketBestEffort(context.Background(), scopeSummaryOnly(tournamentID))
	}
	return s.canonicalCompleteRoundOutcome(req.CommandID, res), nil
}

func (s *Service) canonicalCompleteRoundOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s == nil || commandID == "" {
		return fallback
	}
	if s.completeRounds != nil {
		if stored, ok := s.completeRounds.LookupOutcome(commandID); ok {
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

func (s *Service) rejectCompleteRound(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		return s.reject(ctx, req, correlationID, tournamentID, reason)
	}
	uow, err := s.completeRounds.BeginStandaloneCommand(ctx, req.CommandID)
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
	rec := audit.NewRejection(req.CommandID, correlationID, req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(ctx, rec); err != nil {
		return envelope.Result{}, err
	}
	if err := uow.Commit(CompleteRoundCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Decision: domain.CompleteRoundDecision{Kind: domain.CompleteRoundReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	return s.canonicalCompleteRoundOutcome(req.CommandID, res), nil
}

func (s *Service) persistCompleteRoundRejectStandalone(
	ctx context.Context,
	req CommandRequest,
	correlationID, tournamentID string,
	decision domain.CompleteRoundDecision,
) (envelope.Result, error) {
	uow, err := s.completeRounds.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	return s.commitCompleteRoundOutcome(ctx, uow, req, decision, tournamentID, correlationID)
}
