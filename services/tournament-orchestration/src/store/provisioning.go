package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

// Durable provisioning lock order:
// Kickoff TX: exclusive rewrite barrier → global command lock → tournament row → round → batches.
// Prepare/Finalize TX: shared rewrite barrier → global attempt command lock → leased batch → round → slots.
// Claim TX: tournament FOR UPDATE SKIP LOCKED → batch FOR UPDATE SKIP LOCKED (compatible with barrier).
//
// Never: Service.mu, legacy whole-rewrite hydrate/persist, full round/tournament hydration.

const (
	provisionCommandType            = "ProvisionRoundMatches"
	provisionAttemptCmdType         = "ProcessProvisioningBatch"
	DefaultProvisionRoomConcurrency = 16
)

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

// ProvisioningUnitOfWork holds one READ COMMITTED kickoff transaction.
type ProvisioningUnitOfWork struct {
	store     *TournamentStore
	ctx       context.Context
	tx        pgx.Tx
	tid       string
	commandID string
	kickoff   domain.ProvisionKickoffContext
	exists    bool
	done      bool
}

func (u *ProvisioningUnitOfWork) Exists() bool { return u != nil && u.exists }

func (u *ProvisioningUnitOfWork) KickoffContext() domain.ProvisionKickoffContext {
	if u == nil {
		return domain.ProvisionKickoffContext{}
	}
	return u.kickoff
}

func (u *ProvisioningUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
	if u == nil || u.done || commandID == "" || commandID != u.commandID {
		return envelope.Result{}, false
	}
	var body []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return envelope.Result{}, false
	}
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if json.Unmarshal(body, &out) != nil {
		return envelope.Result{}, false
	}
	return out, true
}

func (u *ProvisioningUnitOfWork) finishWithPrior(body []byte) error {
	var out envelope.Result
	_ = json.Unmarshal(body, &out)
	_ = u.tx.Rollback(u.ctx)
	u.done = true
	return &PriorCommandOutcome{Outcome: out}
}

func (u *ProvisioningUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

func (u *ProvisioningUnitOfWork) bindCommit(req ProvisioningCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("provisioning uow done")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}
	if req.CommandType == "" {
		return fmt.Errorf("command type required for commit")
	}
	if req.Decision.Kind == domain.ProvisionKickoffSchedule {
		if req.CommandType != provisionCommandType {
			return fmt.Errorf("command type mismatch: want %q got %q", provisionCommandType, req.CommandType)
		}
		if u.tid == "" || req.TournamentID == "" || req.TournamentID != u.tid {
			return fmt.Errorf("schedule decision requires exact nonempty tournament binding")
		}
		rn := req.RoundNumber
		if rn < 1 {
			rn = u.kickoff.RoundNumber
		}
		if rn < 1 {
			return fmt.Errorf("roundNumber required for schedule")
		}
	} else if u.tid != "" {
		if req.TournamentID != "" && req.TournamentID != u.tid {
			return fmt.Errorf("tournamentId mismatch: locked %q got %q", u.tid, req.TournamentID)
		}
	}
	return nil
}

// BeginProvisionRound starts exclusive-barrier + command-lock kickoff UoW.
// Lock order: exclusive rewrite barrier → global command lock → tournament FOR UPDATE → round → batch plan context.
func (s *TournamentStore) BeginProvisionRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (*ProvisioningUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" {
		return nil, fmt.Errorf("tournamentId required")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	if roundNumber < 1 {
		return nil, fmt.Errorf("roundNumber required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &ProvisioningUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, commandID: commandID,
		kickoff: domain.ProvisionKickoffContext{
			TournamentID: domain.TournamentID(tournamentID),
			RoundNumber:  roundNumber,
		},
	}
	if err := acquireRewriteBarrierExclusive(ctx, tx, tournamentID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var phase string
	var rulesRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT phase, rules FROM tournaments WHERE tournament_id = $1 FOR UPDATE
	`, tournamentID).Scan(&phase, &rulesRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.kickoff.Exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	uow.kickoff.Exists = true
	uow.kickoff.Phase = domain.TournamentPhase(phase)

	var rules tournamentRules
	if len(rulesRaw) > 0 {
		_ = json.Unmarshal(rulesRaw, &rules)
	}
	uow.kickoff.BatchSize = rules.BatchSize
	uow.kickoff.RetryBudget = rules.RetryBudget

	var roundStatus string
	err = tx.QueryRow(ctx, `
		SELECT status FROM tournament_rounds
		WHERE tournament_id = $1 AND round_number = $2
		FOR UPDATE
	`, tournamentID, roundNumber).Scan(&roundStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.kickoff.RoundStatus = domain.RoundStatus(roundStatus)

	var slotCount int
	var minIdx, maxIdx *int
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int,
		       min(slot_index),
		       max(slot_index)
		FROM bracket_slots
		WHERE tournament_id = $1 AND round_number = $2
	`, tournamentID, roundNumber).Scan(&slotCount, &minIdx, &maxIdx)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.kickoff.SlotCount = slotCount
	contiguous := slotCount > 0 && minIdx != nil && maxIdx != nil &&
		*minIdx == 0 && *maxIdx == slotCount-1
	if contiguous {
		// Fail closed on gaps: count must equal max-min+1 (already) and no missing indexes.
		var gap int
		err = tx.QueryRow(ctx, `
			SELECT count(*)::int FROM generate_series(0, $3::int - 1) g(i)
			WHERE NOT EXISTS (
				SELECT 1 FROM bracket_slots s
				WHERE s.tournament_id = $1 AND s.round_number = $2 AND s.slot_index = g.i
			)
		`, tournamentID, roundNumber, slotCount).Scan(&gap)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		contiguous = gap == 0
	}
	uow.kickoff.SlotsContiguous = contiguous

	var batchCount int
	err = tx.QueryRow(ctx, `
		SELECT count(*)::int FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2
	`, tournamentID, roundNumber).Scan(&batchCount)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if batchCount > 0 {
		uow.kickoff.ExistingBatchesFilled = true
		batchSize := rules.BatchSize
		if batchSize <= 0 {
			batchSize = domain.DefaultBatchSize
		}
		if slotCount > 0 {
			plan, planErr := domain.ComputeProvisionBatchPlan(slotCount, batchSize)
			if planErr == nil && batchCount == plan.BatchCount {
				if match, mErr := verifyBatchPlanTx(ctx, tx, tournamentID, roundNumber, plan); mErr != nil {
					_ = tx.Rollback(ctx)
					return nil, wrapUnavailable(mErr)
				} else if match {
					uow.kickoff.ExistingBatchPlanFingerprint = plan.Fingerprint()
				} else {
					uow.kickoff.ExistingBatchPlanFingerprint = "drift"
				}
			} else {
				uow.kickoff.ExistingBatchPlanFingerprint = "drift"
			}
		} else {
			uow.kickoff.ExistingBatchPlanFingerprint = "drift"
		}
	}
	return uow, nil
}

func verifyBatchPlanTx(ctx context.Context, tx pgx.Tx, tid string, roundNumber int, plan domain.ProvisionBatchPlan) (bool, error) {
	rows, err := tx.Query(ctx, `
		SELECT batch_id, slot_id_from, slot_id_to, status
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2
		ORDER BY (regexp_replace(slot_id_from, '^slot_', ''))::int ASC
	`, tid, roundNumber)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var batchID, from, to, status string
		if err := rows.Scan(&batchID, &from, &to, &status); err != nil {
			return false, err
		}
		if i >= plan.BatchCount {
			return false, nil
		}
		wantFrom, wantTo := plan.BatchRange(i)
		wantID := string(domain.BatchIDForIndex(i))
		if batchID != wantID || from != string(domain.SlotIDForIndex(wantFrom)) || to != string(domain.SlotIDForIndex(wantTo)) {
			return false, nil
		}
		switch domain.BatchStatus(status) {
		case domain.BatchPending, domain.BatchInProgress, domain.BatchCompleted,
			domain.BatchRetried, domain.BatchQuarantined, domain.BatchCancelled:
			// known statuses ok for plan fingerprint
		default:
			return false, nil
		}
		i++
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return i == plan.BatchCount, nil
}

// BeginStandaloneProvisionCommand locks only the global command id.
func (s *TournamentStore) BeginStandaloneProvisionCommand(ctx context.Context, commandID string) (*ProvisioningUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &ProvisioningUnitOfWork{store: s, ctx: ctx, tx: tx, commandID: commandID}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// LookupProvisionOutcome reads command_idempotency outside a UoW.
func (s *TournamentStore) LookupProvisionOutcome(ctx context.Context, commandID string) (envelope.Result, bool) {
	if s == nil || s.pool == nil || commandID == "" {
		return envelope.Result{}, false
	}
	var body []byte
	err := s.pool.QueryRow(ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if json.Unmarshal(body, &out) != nil {
		return envelope.Result{}, false
	}
	return out, true
}

// Commit applies kickoff decision: create batches + transition round, or outcome-only.
func (u *ProvisioningUnitOfWork) Commit(req ProvisioningCommitRequest) error {
	if err := u.bindCommit(req); err != nil {
		return err
	}
	now := time.Now().UTC()
	if req.CommandType == "" {
		req.CommandType = provisionCommandType
	}
	if req.TournamentID == "" {
		req.TournamentID = u.tid
	}
	if req.RoundNumber < 1 {
		req.RoundNumber = u.kickoff.RoundNumber
	}

	var existingBody []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, req.CommandID).Scan(&existingBody)
	if err == nil {
		return u.finishWithPrior(existingBody)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapUnavailable(err)
	}

	switch req.Decision.Kind {
	case domain.ProvisionKickoffSchedule:
		if err := u.applySchedule(req, now); err != nil {
			return err
		}
	case domain.ProvisionKickoffAlreadyDone, domain.ProvisionKickoffReject:
		// Outcome-only; no mutation (already-done may still need seeded→provisioning if drifted —
		// Decide only returns AlreadyDone when batches+status already match).
	}

	body, err := json.Marshal(req.Outcome)
	if err != nil {
		return err
	}
	status := "accepted"
	if req.Outcome.Status == envelope.StatusRejected {
		status = "rejected"
	}
	tag, err := u.tx.Exec(u.ctx, `
		INSERT INTO command_idempotency (command_id, tournament_id, command_type, outcome_status, outcome_body)
		VALUES ($1, $2, $3, $4, $5::jsonb)
		ON CONFLICT (command_id) DO NOTHING
	`, req.CommandID, nullIfEmpty(req.TournamentID), req.CommandType, status, body)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		var prior []byte
		if err := u.tx.QueryRow(u.ctx, `
			SELECT outcome_body FROM command_idempotency WHERE command_id = $1
		`, req.CommandID).Scan(&prior); err != nil {
			return wrapUnavailable(err)
		}
		return u.finishWithPrior(prior)
	}
	if err := u.tx.Commit(u.ctx); err != nil {
		return wrapUnavailable(err)
	}
	u.done = true
	return nil
}

func (u *ProvisioningUnitOfWork) applySchedule(req ProvisioningCommitRequest, now time.Time) error {
	plan := req.Decision.Plan
	rn := req.RoundNumber

	// Transition seeded → provisioning (exact status guard).
	tag, err := u.tx.Exec(u.ctx, `
		UPDATE tournament_rounds
		SET status = 'provisioning'
		WHERE tournament_id = $1 AND round_number = $2 AND status = 'seeded'
	`, u.tid, rn)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		var st string
		if err := u.tx.QueryRow(u.ctx, `
			SELECT status FROM tournament_rounds WHERE tournament_id = $1 AND round_number = $2
		`, u.tid, rn).Scan(&st); err != nil {
			return wrapUnavailable(err)
		}
		return fmt.Errorf("round status drift: want seeded got %s", st)
	}

	// One bounded bulk insert via generate_series — at most ceil(slotCount/batchSize) rows.
	_, err = u.tx.Exec(u.ctx, `
		INSERT INTO provisioning_batches (
			tournament_id, round_number, batch_id, shard_key, status, retry_attempt,
			slot_id_from, slot_id_to, created_at, updated_at
		)
		SELECT
			$1,
			$2,
			'batch_' || g.i::text,
			'batch_' || g.i::text,
			'pending',
			0,
			'slot_' || (g.i * $3)::text,
			'slot_' || LEAST((g.i * $3) + $3 - 1, $4 - 1)::text,
			$5,
			$5
		FROM generate_series(0, $6::int - 1) AS g(i)
	`, u.tid, rn, plan.BatchSize, plan.SlotCount, now, plan.BatchCount)
	if err != nil {
		return wrapUnavailable(err)
	}

	// Initialize 64 progress shards at zero if absent (no assigned yet at kickoff).
	_, err = u.tx.Exec(u.ctx, `
		INSERT INTO round_progress_shards (tournament_id, round_number, shard_id, assigned_count, resolved_count, quarantined_count)
		SELECT $1, $2, g.i, 0, 0, 0
		FROM generate_series(0, 63) AS g(i)
		ON CONFLICT (tournament_id, round_number, shard_id) DO NOTHING
	`, u.tid, rn)
	if err != nil {
		return wrapUnavailable(err)
	}

	if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, now); err != nil {
		return wrapUnavailable(err)
	}
	return nil
}
