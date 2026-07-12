package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

func (s *Service) submitCommandLifecycleDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectLifecycle(ctx, req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	if prior, ok := s.lifecycle.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	trimmed := bytes.TrimSpace(req.Payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' || !json.Valid(trimmed) {
		return s.rejectLifecycle(ctx, req, correlationID, "", "invalid_payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(trimmed, &payload); err != nil || payload == nil {
		return s.rejectLifecycle(ctx, req, correlationID, "", "invalid_payload")
	}
	tournamentID := stringField(payload, "tournamentId")
	switch req.Type {
	case CmdCompleteTournament:
		return s.completeTournamentDifferential(ctx, req, tournamentID, correlationID)
	case CmdCancelTournament:
		return s.cancelTournamentDifferential(ctx, req, tournamentID, correlationID)
	default:
		return envelope.Result{}, fmt.Errorf("unsupported lifecycle command %s", req.Type)
	}
}

func (s *Service) completeTournamentDifferential(ctx context.Context, req CommandRequest, tournamentID, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	cmd := domain.CompleteTournamentCommand{CommandID: domain.CommandID(req.CommandID)}
	if !cmd.CommandID.Valid() {
		decision := domain.DecideCompleteTournament(domain.CompleteTournamentContext{}, cmd)
		return s.persistLifecycleRejectStandalone(ctx, req, correlationID, tournamentID, decision, domain.CancelTournamentDecision{})
	}
	if !tid.Valid() {
		decision := domain.DecideCompleteTournament(domain.CompleteTournamentContext{Exists: false}, cmd)
		return s.persistLifecycleRejectStandalone(ctx, req, correlationID, "", decision, domain.CancelTournamentDecision{})
	}
	uow, err := s.lifecycle.BeginCompleteTournament(ctx, string(tid), req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	decision := domain.DecideCompleteTournament(uow.CompleteContext(), cmd)
	return s.commitLifecycleComplete(uow, req, decision, tournamentID, correlationID)
}

func (s *Service) cancelTournamentDifferential(ctx context.Context, req CommandRequest, tournamentID, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	cmd := domain.CancelTournamentCommand{CommandID: domain.CommandID(req.CommandID)}
	if !cmd.CommandID.Valid() {
		decision := domain.DecideCancelTournament(domain.CancelTournamentContext{}, cmd)
		return s.persistLifecycleRejectStandalone(ctx, req, correlationID, tournamentID, domain.CompleteTournamentDecision{}, decision)
	}
	if !tid.Valid() {
		decision := domain.DecideCancelTournament(domain.CancelTournamentContext{Exists: false}, cmd)
		return s.persistLifecycleRejectStandalone(ctx, req, correlationID, "", domain.CompleteTournamentDecision{}, decision)
	}
	uow, err := s.lifecycle.BeginCancelTournament(ctx, string(tid), req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	decision := domain.DecideCancelTournament(uow.CancelContext(), cmd)
	return s.commitLifecycleCancel(uow, req, decision, tournamentID, correlationID)
}

func (s *Service) commitLifecycleComplete(
	uow LifecycleUnitOfWork,
	req CommandRequest,
	decision domain.CompleteTournamentDecision,
	tournamentID, correlationID string,
) (envelope.Result, error) {
	out := decision.Outcome
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.Commit(LifecycleCommitRequest{
			TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			CorrelationID: correlationID, Outcome: res, Complete: decision,
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
		return s.canonicalLifecycleOutcome(req.CommandID, res), nil
	}

	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	if decision.Kind == domain.CompleteTournamentSuccess && len(out.Facts) > 0 {
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
	if err := uow.Commit(LifecycleCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Events: events, Complete: decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	if projectionChangedFromOutcome(out) {
		s.refreshBracketBestEffort(context.Background(), scopeSummaryOnly(tournamentID))
	}
	return s.canonicalLifecycleOutcome(req.CommandID, res), nil
}

func (s *Service) commitLifecycleCancel(
	uow LifecycleUnitOfWork,
	req CommandRequest,
	decision domain.CancelTournamentDecision,
	tournamentID, correlationID string,
) (envelope.Result, error) {
	out := decision.Outcome
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		if err := uow.Commit(LifecycleCommitRequest{
			TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			CorrelationID: correlationID, Outcome: res, Cancel: decision,
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
		return s.canonicalLifecycleOutcome(req.CommandID, res), nil
	}

	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": f.Data,
		})
	}
	var events []OutboxEvent
	// TournamentCancelled has no contract Kafka topic today; still persist facts in outcome.
	if decision.Kind == domain.CancelTournamentSuccess && len(out.Facts) > 0 {
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
	if err := uow.Commit(LifecycleCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Events: events, Cancel: decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	if projectionChangedFromOutcome(out) {
		s.refreshBracketBestEffort(context.Background(), scopeSummaryOnly(tournamentID))
	}
	return s.canonicalLifecycleOutcome(req.CommandID, res), nil
}

func (s *Service) canonicalLifecycleOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s == nil || commandID == "" {
		return fallback
	}
	if s.lifecycle != nil {
		if stored, ok := s.lifecycle.LookupOutcome(commandID); ok {
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

func (s *Service) rejectLifecycle(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		return s.reject(req, correlationID, tournamentID, reason)
	}
	uow, err := s.lifecycle.BeginStandaloneCommand(ctx, req.CommandID)
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
	if err := uow.Commit(LifecycleCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Complete: domain.CompleteTournamentDecision{Kind: domain.CompleteTournamentReject},
		Cancel:   domain.CancelTournamentDecision{Kind: domain.CancelTournamentReject},
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
	return s.canonicalLifecycleOutcome(req.CommandID, res), nil
}

func (s *Service) persistLifecycleRejectStandalone(
	ctx context.Context,
	req CommandRequest,
	correlationID, tournamentID string,
	complete domain.CompleteTournamentDecision,
	cancel domain.CancelTournamentDecision,
) (envelope.Result, error) {
	uow, err := s.lifecycle.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	switch req.Type {
	case CmdCompleteTournament:
		return s.commitLifecycleComplete(uow, req, complete, tournamentID, correlationID)
	case CmdCancelTournament:
		return s.commitLifecycleCancel(uow, req, cancel, tournamentID, correlationID)
	default:
		return envelope.Result{}, fmt.Errorf("unsupported lifecycle command %s", req.Type)
	}
}
