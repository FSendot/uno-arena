package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/audit"
	"unoarena/shared/envelope"
)

func (s *Service) submitCommandProvisioningDifferential(ctx context.Context, req CommandRequest, correlationID string) (envelope.Result, error) {
	if req.CommandID == "" || req.Type == "" {
		return s.rejectProvisioning(ctx, req, correlationID, "", "invalid_envelope")
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
	if prior, ok := s.provisioning.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return envelope.Result{}, fmt.Errorf("payload must be a JSON object")
	}
	tournamentID := stringField(payload, "tournamentId")
	switch req.Type {
	case CmdProvisionRoundMatches:
		return s.provisionRoundDifferential(ctx, req, tournamentID, intField(payload, "roundNumber"), correlationID)
	case CmdRetryProvisioning:
		res, err := s.provisioning.ManualRetryProvisioningBatch(ctx, req.CommandID, tournamentID,
			intField(payload, "roundNumber"), stringField(payload, "batchId"), intField(payload, "retryAttempt"), correlationID)
		// Retry is projection-hidden; best-effort refresh is a no-op when version unchanged.
		if err == nil {
			s.refreshBracketBestEffort(ctx, scopeChunks(tournamentID, store.BracketChunkRef{
				RoundNumber: intField(payload, "roundNumber"), BatchID: stringField(payload, "batchId"),
			}))
		}
		return res, err
	case CmdQuarantineBatch:
		res, err := s.provisioning.ManualQuarantineProvisioningBatch(ctx, req.CommandID, tournamentID,
			intField(payload, "roundNumber"), stringField(payload, "batchId"), stringField(payload, "reason"), correlationID)
		if err == nil {
			s.refreshBracketBestEffort(ctx, scopeChunks(tournamentID, store.BracketChunkRef{
				RoundNumber: intField(payload, "roundNumber"), BatchID: stringField(payload, "batchId"),
			}))
		}
		return res, err
	default:
		return s.rejectProvisioning(ctx, req, correlationID, tournamentID, "unknown_command_type")
	}
}

func (s *Service) provisionRoundDifferential(ctx context.Context, req CommandRequest, tournamentID string, roundNumber int, correlationID string) (envelope.Result, error) {
	tid := domain.TournamentID(tournamentID)
	provCmd := domain.ProvisionRoundMatchesCommand{
		CommandID:   domain.CommandID(req.CommandID),
		RoundNumber: roundNumber,
	}
	if !tid.Valid() {
		return s.rejectProvisioning(ctx, req, correlationID, "", string(domain.RejectInvalidIdentity))
	}
	if roundNumber < 1 {
		decision := domain.DecideProvisionKickoff(domain.ProvisionKickoffContext{Exists: true}, provCmd)
		return s.persistProvisioningRejectStandalone(ctx, req, correlationID, tournamentID, decision, roundNumber)
	}

	uow, err := s.provisioning.BeginProvisionRound(ctx, string(tid), roundNumber, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	if !uow.Exists() {
		return s.rejectProvisioningUoW(ctx, uow, req, correlationID, tournamentID, "tournament_not_found")
	}
	decision := domain.DecideProvisionKickoff(uow.KickoffContext(), provCmd)
	return s.commitProvisioningOutcome(ctx, uow, req, decision, tournamentID, correlationID, roundNumber)
}

func (s *Service) commitProvisioningOutcome(ctx context.Context, uow ProvisioningUnitOfWork, req CommandRequest, decision domain.ProvisionKickoffDecision, tournamentID, correlationID string, roundNumber int) (envelope.Result, error) {
	out := decision.Outcome
	if out.Rejected() {
		reason := string(out.Rejection.Code)
		res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
		rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
			WithTournament(tournamentID)
		if err := s.audit.RecordRejection(ctx, rec); err != nil {
			return envelope.Result{}, err
		}
		if err := uow.Commit(ProvisioningCommitRequest{
			TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
			CorrelationID: correlationID, Outcome: res, Decision: decision, RoundNumber: roundNumber,
		}); err != nil {
			if prior, ok := store.AsPriorCommandOutcome(err); ok {
				return prior, nil
			}
			return envelope.Result{}, err
		}
		return s.canonicalProvisioningOutcome(req.CommandID, res), nil
	}
	payload, _ := json.Marshal(map[string]any{"facts": []any{}})
	res := envelope.Accepted(req.CommandID, req.Type, nil, payload)
	if err := uow.Commit(ProvisioningCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res, Decision: decision, RoundNumber: roundNumber,
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	// Provision kickoff bumps public projection when batches become scheduled for this round.
	s.refreshBracketBestEffort(context.Background(), scopeRound(tournamentID, roundNumber))
	return s.canonicalProvisioningOutcome(req.CommandID, res), nil
}

func (s *Service) canonicalProvisioningOutcome(commandID string, fallback envelope.Result) envelope.Result {
	if s == nil || commandID == "" {
		return fallback
	}
	if s.provisioning != nil {
		if stored, ok := s.provisioning.LookupOutcome(commandID); ok {
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

func (s *Service) rejectProvisioning(ctx context.Context, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	if req.CommandID == "" {
		return s.reject(ctx, req, correlationID, tournamentID, reason)
	}
	uow, err := s.provisioning.BeginStandaloneCommand(ctx, req.CommandID)
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
	if err := uow.Commit(ProvisioningCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Decision: domain.ProvisionKickoffDecision{Kind: domain.ProvisionKickoffReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	return s.canonicalProvisioningOutcome(req.CommandID, res), nil
}

func (s *Service) rejectProvisioningUoW(ctx context.Context, uow ProvisioningUnitOfWork, req CommandRequest, correlationID, tournamentID, reason string) (envelope.Result, error) {
	res := envelope.Rejected(req.CommandID, req.Type, reason, nil)
	rec := audit.NewRejection(req.CommandID, firstNonEmpty(correlationID, req.CommandID), req.SessionID, req.PlayerID, reason, s.clock.Now()).
		WithTournament(tournamentID)
	if err := s.audit.RecordRejection(ctx, rec); err != nil {
		return envelope.Result{}, err
	}
	if err := uow.Commit(ProvisioningCommitRequest{
		TournamentID: tournamentID, CommandID: req.CommandID, CommandType: req.Type,
		CorrelationID: correlationID, Outcome: res,
		Decision: domain.ProvisionKickoffDecision{Kind: domain.ProvisionKickoffReject},
	}); err != nil {
		if prior, ok := store.AsPriorCommandOutcome(err); ok {
			return prior, nil
		}
		return envelope.Result{}, err
	}
	return s.canonicalProvisioningOutcome(req.CommandID, res), nil
}

func (s *Service) persistProvisioningRejectStandalone(ctx context.Context, req CommandRequest, correlationID, tournamentID string, decision domain.ProvisionKickoffDecision, roundNumber int) (envelope.Result, error) {
	uow, err := s.provisioning.BeginStandaloneCommand(ctx, req.CommandID)
	if err != nil {
		return envelope.Result{}, err
	}
	defer func() { _ = uow.Rollback() }()
	if prior, ok := uow.LookupOutcome(req.CommandID); ok {
		return prior, nil
	}
	return s.commitProvisioningOutcome(ctx, uow, req, decision, tournamentID, correlationID, roundNumber)
}

// processProvisioningBatchDifferential: prepare TX → concurrent Room calls → success/failure finalize.
// No Tournament transaction is held during Room HTTP calls.
func (s *Service) processProvisioningBatchDifferential(ctx context.Context, work ProvisioningBatchWork) (envelope.Result, error) {
	work.CommandID = resolveProvisioningCommandID(work)
	if prior, ok := s.provisioning.LookupOutcome(work.CommandID); ok {
		return prior, nil
	}
	if err := validateProvisioningBatchWork(work); err != nil {
		return envelope.Rejected(work.CommandID, "ProcessProvisioningBatch", err.Error(), nil), nil
	}
	if work.LeaseOwner == "" || work.LeaseVersion < 1 {
		return envelope.Rejected(work.CommandID, "ProcessProvisioningBatch", "lease fence is required", nil), nil
	}

	prepared, err := s.provisioning.PrepareProvisioningBatch(ctx, store.PrepareProvisioningBatchInput{
		CommandID:     work.CommandID,
		CorrelationID: work.CorrelationID,
		TournamentID:  work.TournamentID,
		RoundNumber:   work.RoundNumber,
		BatchID:       work.BatchID,
		SlotFrom:      work.SlotFrom,
		SlotTo:        work.SlotTo,
		SlotSize:      work.SlotSize,
		RetryAttempt:  work.RetryAttempt,
		LeaseOwner:    work.LeaseOwner,
		LeaseVersion:  work.LeaseVersion,
	})
	if err != nil {
		if errors.Is(err, store.ErrProvisioningFence) {
			return envelope.Result{}, err
		}
		return envelope.Result{}, err
	}
	if prepared.PriorOutcome != nil {
		return *prepared.PriorOutcome, nil
	}
	if prepared.Quarantined {
		s.refreshBracketBestEffort(ctx, scopeChunks(work.TournamentID, store.BracketChunkRef{
			RoundNumber: work.RoundNumber, BatchID: work.BatchID,
		}))
		return envelope.Accepted(work.CommandID, "ProcessProvisioningBatch", nil, json.RawMessage(`{"facts":[]}`)), nil
	}
	if prepared.Rejected {
		return envelope.Rejected(work.CommandID, "ProcessProvisioningBatch", prepared.RejectReason, nil), nil
	}
	if prepared.AlreadyComplete {
		return envelope.Accepted(work.CommandID, "ProcessProvisioningBatch", nil, json.RawMessage(`{"facts":[]}`)), nil
	}

	// Prepare may bump projection when assignments become public; refresh before Room calls.
	s.refreshBracketBestEffort(ctx, scopeChunks(work.TournamentID, store.BracketChunkRef{
		RoundNumber: work.RoundNumber, BatchID: work.BatchID,
	}))

	// ROOM CALL PHASE — no DB transaction held.
	if err := s.provisionRoomsConcurrent(ctx, work, prepared.Slots); err != nil {
		if errors.Is(err, store.ErrProvisioningFence) {
			// Lease lost during heartbeat — cancel calls already done; do NOT failure-finalize.
			return envelope.Result{}, err
		}
		forceQuarantine := roomProvisionFailureForcesQuarantine(err)
		budget, _ := s.provisioning.LoadRetryBudget(ctx, work.TournamentID)
		nextAttempt := work.RetryAttempt + 1
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
		res, ferr := s.provisioning.FinalizeProvisioningBatchFailure(ctx, store.FinalizeProvisioningBatchInput{
			CommandID:       work.CommandID,
			CorrelationID:   work.CorrelationID,
			TournamentID:    work.TournamentID,
			RoundNumber:     work.RoundNumber,
			BatchID:         work.BatchID,
			SlotFrom:        work.SlotFrom,
			SlotTo:          work.SlotTo,
			SlotSize:        work.SlotSize,
			RetryAttempt:    work.RetryAttempt,
			LeaseOwner:      work.LeaseOwner,
			LeaseVersion:    work.LeaseVersion,
			LastError:       err.Error(),
			RetryBudget:     budget,
			ForceQuarantine: forceQuarantine,
			NextRetryWork:   nextRetryWorkPayload(nextWork)["nextRetryWork"].(map[string]any),
		})
		s.refreshBracketBestEffort(ctx, scopeChunks(work.TournamentID, store.BracketChunkRef{
			RoundNumber: work.RoundNumber, BatchID: work.BatchID,
		}))
		return res, ferr
	}

	res, err := s.provisioning.FinalizeProvisioningBatchSuccess(ctx, store.FinalizeProvisioningBatchInput{
		CommandID:     work.CommandID,
		CorrelationID: work.CorrelationID,
		TournamentID:  work.TournamentID,
		RoundNumber:   work.RoundNumber,
		BatchID:       work.BatchID,
		SlotFrom:      work.SlotFrom,
		SlotTo:        work.SlotTo,
		SlotSize:      work.SlotSize,
		RetryAttempt:  work.RetryAttempt,
		LeaseOwner:    work.LeaseOwner,
		LeaseVersion:  work.LeaseVersion,
	})
	if err == nil {
		s.refreshBracketBestEffort(ctx, scopeChunks(work.TournamentID, store.BracketChunkRef{
			RoundNumber: work.RoundNumber, BatchID: work.BatchID,
		}))
	}
	return res, err
}

// provisioningHeartbeatTTL derives renew window from the claimed lease when valid.
// Custom/short test leases must not be silently extended to DefaultProvisioningLease.
// A small positive floor avoids a zero/negative ticker interval.
func provisioningHeartbeatTTL(leaseExpiresAt, now time.Time) time.Duration {
	const floor = 100 * time.Millisecond
	if !leaseExpiresAt.IsZero() {
		if rem := leaseExpiresAt.Sub(now); rem > 0 {
			if rem < floor {
				return floor
			}
			return rem
		}
	}
	return store.DefaultProvisioningLease
}

func (s *Service) provisionRoomsConcurrent(ctx context.Context, work ProvisioningBatchWork, slots []store.PreparedSlotWork) error {
	if len(slots) == 0 {
		return nil
	}
	concurrency := store.DefaultProvisionRoomConcurrency
	if concurrency > len(slots) {
		concurrency = len(slots)
	}
	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	stopHB := make(chan struct{})
	var hbOnce sync.Once
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Exact-fence heartbeat must succeed synchronously before any Room call.
	now := time.Now().UTC()
	if s.clock != nil {
		now = s.clock.Now().UTC()
	}
	ttl := provisioningHeartbeatTTL(work.LeaseExpiresAt, now)
	ok, err := s.provisioning.HeartbeatProvisioningLease(
		ctx, work.TournamentID, work.RoundNumber, work.BatchID,
		work.LeaseOwner, work.LeaseVersion, work.RetryAttempt, now, ttl,
	)
	if err != nil || !ok {
		return store.ErrProvisioningFence
	}

	// Renew at ttl/4 for the duration of Room fanout.
	go func() {
		ticker := time.NewTicker(ttl / 4)
		defer ticker.Stop()
		for {
			select {
			case <-stopHB:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				beatNow := time.Now().UTC()
				if s.clock != nil {
					beatNow = s.clock.Now().UTC()
				}
				ok, err := s.provisioning.HeartbeatProvisioningLease(
					ctx, work.TournamentID, work.RoundNumber, work.BatchID,
					work.LeaseOwner, work.LeaseVersion, work.RetryAttempt, beatNow, ttl,
				)
				if err != nil || !ok {
					select {
					case errCh <- store.ErrProvisioningFence:
					default:
					}
					cancel()
				}
			}
		}
	}()
	defer hbOnce.Do(func() { close(stopHB) })

	for _, slot := range slots {
		select {
		case err := <-errCh:
			cancel()
			wg.Wait()
			return err
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(slot store.PreparedSlotWork) {
			defer wg.Done()
			defer func() { <-sem }()
			players := make([]domain.PlayerID, len(slot.PlayerIDs))
			for i, p := range slot.PlayerIDs {
				players[i] = domain.PlayerID(p)
			}
			res, err := s.rooms.Provision(ctx, RoomProvisionRequest{
				TournamentID: domain.TournamentID(work.TournamentID),
				RoundNumber:  work.RoundNumber,
				SlotID:       domain.SlotID(slot.SlotID),
				RoomID:       domain.RoomID(slot.RoomID),
				PlayerIDs:    players,
				BatchID:      domain.BatchID(slot.BatchID),
			})
			if err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
				return
			}
			if res.RoomID != slot.RoomID {
				select {
				case errCh <- fmt.Errorf("%w: requested %s", ErrRoomIDMismatch, slot.RoomID):
					cancel()
				default:
				}
			}
		}(slot)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func isProvisioningDifferentialCommand(typ string) bool {
	switch typ {
	case CmdProvisionRoundMatches, CmdRetryProvisioning, CmdQuarantineBatch:
		return true
	default:
		return false
	}
}
