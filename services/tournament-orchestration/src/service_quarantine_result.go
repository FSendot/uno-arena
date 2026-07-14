package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

func (s *Service) submitCommandQuarantineResultDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectQuarantineResult(ctx, req, correlationID, "", "invalid_envelope")
	}
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		return envelope.Result{}, fmt.Errorf("schemaVersion must be %d", envelope.CurrentSchemaVersion)
	}
	if prior, ok := s.quarantineResults.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	trimmed := bytes.TrimSpace(req.Payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' || !json.Valid(trimmed) {
		return s.rejectQuarantineResult(ctx, req, correlationID, "", "invalid_payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(trimmed, &payload); err != nil || payload == nil {
		return s.rejectQuarantineResult(ctx, req, correlationID, "", "invalid_payload")
	}
	tournamentID := stringField(payload, "tournamentId")
	roomID := stringField(payload, "roomId")
	completionVersion := uintField(payload, "completionVersion")
	reason := stringField(payload, "reason")

	cmd := domain.QuarantineTournamentResultCommand{
		CommandID:         domain.CommandID(req.CommandID),
		RoomID:            domain.RoomID(roomID),
		CompletionVersion: domain.CompletionVersion(completionVersion),
		Reason:            reason,
	}
	tid := domain.TournamentID(tournamentID)

	if !cmd.CommandID.Valid() || !cmd.RoomID.Valid() || cmd.CompletionVersion == 0 {
		decision := domain.DecideQuarantineTournamentResult(domain.QuarantineTournamentResultContext{
			TournamentID: tid,
			Exists:       true, // identity/version rejects short-circuit before Exists checks
			RoomID:       cmd.RoomID,
		}, cmd)
		return s.persistQuarantineResultRejectStandalone(ctx, req, correlationID, tournamentID, decision)
	}
	if !tid.Valid() {
		decision := domain.DecideQuarantineTournamentResult(domain.QuarantineTournamentResultContext{}, cmd)
		return s.persistQuarantineResultRejectStandalone(ctx, req, correlationID, "", decision)
	}

	uow, err := s.quarantineResults.BeginQuarantineTournamentResult(ctx, string(tid), roomID, completionVersion, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	decision := domain.DecideQuarantineTournamentResult(uow.Loaded(), cmd)
	return s.commitQuarantineResult(ctx, uow, req, decision, cmd, tournamentID, correlationID)
}

func (s *Service) commitQuarantineResult(
	ctx context.Context,
	uow QuarantineResultUnitOfWork,
	req CommandRequest,
	decision domain.QuarantineTournamentResultDecision,
	cmd domain.QuarantineTournamentResultCommand,
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
		if err := uow.Commit(QuarantineResultCommitRequest{
			TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			CorrelationID: correlationID, Outcome: res, Decision: decision, Command: cmd,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
		return s.canonicalQuarantineResultOutcome(req.CommandID, res), nil
	}

	factsPayload := make([]map[string]any, 0, len(out.Facts))
	for _, f := range out.Facts {
		// Closed reason policy: never echo raw operator text into persisted outcome facts.
		data := map[string]any{}
		for k, v := range f.Data {
			if k == "reason" {
				data[k] = domain.ExplicitQuarantineReasonCode
				continue
			}
			data[k] = v
		}
		factsPayload = append(factsPayload, map[string]any{
			"name": string(f.Name),
			"data": data,
		})
	}
	payload, _ := json.Marshal(map[string]any{"facts": factsPayload})
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	proj := decision.Kind == domain.QuarantineResultLedgerOnly || decision.Kind == domain.QuarantineResultInsertQuarantined
	if len(out.Facts) == 0 {
		proj = false
	}
	if err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Decision: decision, Command: cmd,
		ProjectionChanged: proj,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	if proj {
		loaded := uow.Loaded()
		rn := decision.PersistRound
		if rn < 1 {
			rn = loaded.RoundNumber
		}
		slotID := string(decision.PersistSlot)
		if slotID == "" {
			slotID = string(loaded.SlotID)
		}
		s.refreshBracketBestEffort(context.Background(), s.scopeForSlot(
			context.Background(), tournamentID, rn, slotID,
		))
	}
	return s.canonicalQuarantineResultOutcome(req.CommandID, res), nil
}

func (s *Service) canonicalQuarantineResultOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s.quarantineResults != nil {
		if stored, ok := s.quarantineResults.LookupOutcome(commandID); ok {
			return stored
		}
	}
	return fallback
}

func (s *Service) rejectQuarantineResult(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(ctx, rec); err != nil {
			return envelope.Result{}, err
		}
		return res, nil
	}
	uow, err := s.quarantineResults.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(ctx, rec); err != nil {
		return envelope.Result{}, err
	}
	if err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Decision: domain.QuarantineTournamentResultDecision{Kind: domain.QuarantineResultReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	return s.canonicalQuarantineResultOutcome(req.CommandID, res), nil
}

func (s *Service) persistQuarantineResultRejectStandalone(
	ctx context.Context,
	req CommandRequest,
	correlationID, tournamentID string,
	decision domain.QuarantineTournamentResultDecision,
) (envelope.Result, error) {
	out := decision.Outcome
	reason := "invalid_command"
	if out.Rejected() {
		reason = string(out.Rejection.Code)
	}
	uow, err := s.quarantineResults.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(ctx, rec); err != nil {
		return envelope.Result{}, err
	}
	if err := uow.Commit(QuarantineResultCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Decision: decision,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	return s.canonicalQuarantineResultOutcome(req.CommandID, res), nil
}
