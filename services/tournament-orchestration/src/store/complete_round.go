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

// Differential CompleteRound lock order (all paths must match):
//  1. Shared rewrite barrier (tournament:rewrite:{id})
//  2. Global command lock (AcquireCommandLock)
//  3. All 64 round_progress_shards FOR UPDATE in shard_id order (existing round only)
//  4. All provisioning_batches for round FOR UPDATE in numeric range order
//  5. tournament_rounds FOR UPDATE
//
// Next-round seeding inserts (non-final success) happen in Commit via ON CONFLICT —
// no prior lock of the next round/job is required.
//
// Never: tournaments FOR UPDATE, whole-tournament hydrate, subtree DELETE,
// Service.mu, BeginExisting, or lock-order inversion with RoundMatch (shard→round)
// or provisioning (batch→round). CompleteRound uses shards → batches → round.

const completeRoundCommandType = "CompleteRound"

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

// CompleteRoundUnitOfWork holds one READ COMMITTED tx for bounded CompleteRound.
type CompleteRoundUnitOfWork struct {
	store     *TournamentStore
	ctx       context.Context
	tx        pgx.Tx
	tid       string
	roundNum  int
	commandID string
	loaded    domain.CompleteRoundContext
	exists    bool
	done      bool
}

func (u *CompleteRoundUnitOfWork) Exists() bool { return u != nil && u.exists }

func (u *CompleteRoundUnitOfWork) Loaded() domain.CompleteRoundContext {
	if u == nil {
		return domain.CompleteRoundContext{}
	}
	return u.loaded
}

func (u *CompleteRoundUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
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

func (u *CompleteRoundUnitOfWork) finishWithPrior(body []byte) error {
	var out envelope.Result
	_ = json.Unmarshal(body, &out)
	_ = u.tx.Rollback(u.ctx)
	u.done = true
	return &PriorCommandOutcome{Outcome: out}
}

func (u *CompleteRoundUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

// BeginStandaloneCompleteRoundCommand locks only the global command id for invalid/outcome-only rejects.
func (s *TournamentStore) BeginStandaloneCompleteRoundCommand(ctx context.Context, commandID string) (*CompleteRoundUnitOfWork, error) {
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
	uow := &CompleteRoundUnitOfWork{store: s, ctx: ctx, tx: tx, commandID: commandID}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginCompleteRound starts a differential UoW for CompleteRound.
// Lock order: shared barrier → command → 64 shards → batches → round.
func (s *TournamentStore) BeginCompleteRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (*CompleteRoundUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" {
		return nil, fmt.Errorf("tournamentId required")
	}
	if roundNumber < 1 {
		return nil, fmt.Errorf("roundNumber required")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &CompleteRoundUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, roundNum: roundNumber, commandID: commandID,
	}
	if err := acquireRewriteBarrierShared(ctx, tx, tournamentID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var phase string
	err = tx.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id = $1`, tournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.loaded = domain.CompleteRoundContext{
			TournamentID: domain.TournamentID(tournamentID),
			RoundNumber:  roundNumber,
		}
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	uow.loaded = domain.CompleteRoundContext{
		TournamentID: domain.TournamentID(tournamentID),
		Exists:       true,
		Phase:        domain.TournamentPhase(phase),
		RoundNumber:  roundNumber,
	}

	// Round must exist before locking shards (do not create shard rows for missing rounds).
	var roundExists bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM tournament_rounds
			WHERE tournament_id = $1 AND round_number = $2
		)
	`, tournamentID, roundNumber).Scan(&roundExists)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if !roundExists {
		uow.loaded.RoundFound = false
		return uow, nil
	}
	uow.loaded.RoundFound = true

	// 3. Ensure all 64 progress shards exist (zeros ON CONFLICT DO NOTHING), then lock in shard_id order.
	// Missing shards are an internal init gap (e.g. pre-provision round), not an infra failure —
	// decision must stably reject round_incomplete; no public mutation beyond zero init.
	if _, err := tx.Exec(ctx, `
		INSERT INTO round_progress_shards (
			tournament_id, round_number, shard_id,
			assigned_count, resolved_count, quarantined_count, advancing_count
		)
		SELECT $1, $2, g.i, 0, 0, 0, 0
		FROM generate_series(0, $3::int - 1) AS g(i)
		ON CONFLICT (tournament_id, round_number, shard_id) DO NOTHING
	`, tournamentID, roundNumber, domain.ProgressShardCount); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	rows, err := tx.Query(ctx, `
		SELECT shard_id, assigned_count, resolved_count, quarantined_count, advancing_count
		FROM round_progress_shards
		WHERE tournament_id = $1 AND round_number = $2
		ORDER BY shard_id
		FOR UPDATE
	`, tournamentID, roundNumber)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	var (
		assigned, resolved, quarantined, advancing int
		shardCount                                 int
	)
	for rows.Next() {
		var sid, a, r, q, adv int
		if err := rows.Scan(&sid, &a, &r, &q, &adv); err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		assigned += a
		resolved += r
		quarantined += q
		advancing += adv
		shardCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if shardCount != domain.ProgressShardCount {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("progress shards missing after init: got %d want %d", shardCount, domain.ProgressShardCount)
	}

	// 4. Lock all provisioning_batches for the round in numeric slot range order
	// (not lexical: slot_10 must not precede slot_2).
	brows, err := tx.Query(ctx, `
		SELECT batch_id, status, slot_id_from, slot_id_to
		FROM provisioning_batches
		WHERE tournament_id = $1 AND round_number = $2
		ORDER BY
			(regexp_replace(COALESCE(slot_id_from, 'slot_-1'), '^slot_', ''))::int ASC,
			(regexp_replace(COALESCE(slot_id_to, 'slot_-1'), '^slot_', ''))::int ASC,
			batch_id
		FOR UPDATE
	`, tournamentID, roundNumber)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	var quarantinedBatches int
	for brows.Next() {
		var batchID, status string
		var from, to *string
		if err := brows.Scan(&batchID, &status, &from, &to); err != nil {
			brows.Close()
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		if status == string(domain.BatchQuarantined) {
			quarantinedBatches++
		}
	}
	brows.Close()
	if err := brows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	// 5. Lock tournament_rounds and re-read under lock.
	var roundStatus string
	var isFinal bool
	err = tx.QueryRow(ctx, `
		SELECT status, is_final FROM tournament_rounds
		WHERE tournament_id = $1 AND round_number = $2
		FOR UPDATE
	`, tournamentID, roundNumber).Scan(&roundStatus, &isFinal)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	// One-time drift check source: advancement_records cardinality (not per-result scan).
	var advRecordsPlayers int
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(cardinality(advancing_player_ids)), 0)::int
		FROM advancement_records
		WHERE tournament_id = $1 AND round_number = $2
	`, tournamentID, roundNumber).Scan(&advRecordsPlayers)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var normalizedAdvancing int
	err = tx.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM round_advancing_players
		WHERE tournament_id = $1 AND source_round_number = $2
	`, tournamentID, roundNumber).Scan(&normalizedAdvancing)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	uow.loaded.RoundStatus = domain.RoundStatus(roundStatus)
	uow.loaded.IsFinal = isFinal
	uow.loaded.AssignedCount = assigned
	uow.loaded.ResolvedCount = resolved
	uow.loaded.QuarantinedCount = quarantined
	uow.loaded.AdvancingCount = advancing
	uow.loaded.QuarantinedBatches = quarantinedBatches
	uow.loaded.AdvancementRecordsPlayers = advRecordsPlayers
	uow.loaded.NormalizedAdvancingPlayers = normalizedAdvancing
	if isFinal {
		var standings []string
		err = tx.QueryRow(ctx, `
			SELECT ar.advancing_player_ids
			FROM bracket_slots bs
			INNER JOIN advancement_records ar
				ON ar.tournament_id = bs.tournament_id
				AND ar.round_number = bs.round_number
				AND ar.slot_id = bs.slot_id
			INNER JOIN match_results mr
				ON mr.tournament_id = ar.tournament_id
				AND mr.round_number = ar.round_number
				AND mr.slot_id = ar.slot_id
				AND mr.room_id = ar.source_room_id
				AND mr.completion_version = ar.source_completion_version
			WHERE bs.tournament_id = $1 AND bs.round_number = $2
				AND bs.slot_index = 0
				AND mr.disposition = 'recorded'
				AND (SELECT count(*) FROM bracket_slots
					 WHERE tournament_id=$1 AND round_number=$2) = 1
			FOR UPDATE OF bs, ar, mr
		`, tournamentID, roundNumber).Scan(&standings)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		if err == nil {
			uow.loaded.FinalStandings = make([]domain.PlayerID, len(standings))
			for i, playerID := range standings {
				uow.loaded.FinalStandings[i] = domain.PlayerID(playerID)
			}
		}
	}
	return uow, nil
}

func (u *CompleteRoundUnitOfWork) Commit(req CompleteRoundCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("complete round uow done")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}
	cmdType := req.CommandType
	if cmdType == "" {
		cmdType = completeRoundCommandType
	}
	if cmdType != completeRoundCommandType {
		return fmt.Errorf("command type mismatch: want %q got %q", completeRoundCommandType, cmdType)
	}

	// Canonical prior race: if another commit won the unique command_id, surface prior.
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

	tid := u.tid
	if tid == "" {
		tid = req.TournamentID
	}

	switch req.Decision.Kind {
	case domain.CompleteRoundSuccess:
		if u.tid == "" || u.roundNum < 1 {
			return fmt.Errorf("success decision requires tournament-bound complete round uow")
		}
		now := time.Now().UTC()
		tag, err := u.tx.Exec(u.ctx, `
			UPDATE tournament_rounds
			SET status = $3, completed_at = $4
			WHERE tournament_id = $1 AND round_number = $2 AND status = $5
		`, u.tid, u.roundNum, string(domain.RoundCompleted), now, string(domain.RoundInProgress))
		if err != nil {
			return wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("round status not in_progress at complete commit")
		}
		if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, now); err != nil {
			return wrapUnavailable(err)
		}
		if req.Decision.NextRound != nil {
			if err := u.insertNextRoundSeeding(req.Decision.NextRound, now); err != nil {
				return err
			}
		}
		if req.Decision.TournamentCompletion != nil {
			if !req.Decision.IsFinal || req.Decision.NextRound != nil {
				return fmt.Errorf("tournament completion requires final round without next round")
			}
			championID := string(req.Decision.TournamentCompletion.ChampionID)
			if championID == "" {
				return fmt.Errorf("tournament completion requires champion")
			}
			tag, err := u.tx.Exec(u.ctx, `
				UPDATE tournaments
				SET phase = $2,
				    completed_at = $3,
				    updated_at = $3,
				    rules = jsonb_set(COALESCE(rules, '{}'::jsonb), '{championId}', to_jsonb($4::text), true)
				WHERE tournament_id = $1 AND phase = $5
			`, u.tid, string(domain.PhaseCompleted), now, championID, string(domain.PhaseInProgress))
			if err != nil {
				return wrapUnavailable(err)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("tournament phase not in_progress at final round complete commit")
			}
		}
		if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
			return wrapUnavailable(err)
		}

	case domain.CompleteRoundAlreadyDone:
		// Factless no-op: never bump projection or append outbox.
		if len(req.Events) > 0 {
			return fmt.Errorf("already-done complete round must not carry outbox events")
		}

	case domain.CompleteRoundReject:
		if len(req.Events) > 0 {
			return fmt.Errorf("reject complete round must not carry outbox events")
		}

	default:
		return fmt.Errorf("unknown complete round decision kind %q", req.Decision.Kind)
	}

	if err := insertCommandOutcomeWithTournament(u.ctx, u.tx, req.CommandID, tid, cmdType, req.Outcome); err != nil {
		var existing []byte
		err2 := u.tx.QueryRow(u.ctx, `
			SELECT outcome_body FROM command_idempotency WHERE command_id = $1
		`, req.CommandID).Scan(&existing)
		if err2 == nil {
			return u.finishWithPrior(existing)
		}
		return wrapUnavailable(err)
	}

	if u.store != nil && u.store.FailNextCommits > 0 {
		u.store.FailNextCommits--
		u.done = true
		_ = u.tx.Rollback(u.ctx)
		return fmt.Errorf("injected commit failure")
	}

	u.done = true
	if err := u.tx.Commit(u.ctx); err != nil {
		return wrapUnavailable(err)
	}
	return nil
}

func (u *CompleteRoundUnitOfWork) insertNextRoundSeeding(plan *domain.NextRoundSeedingPlan, now time.Time) error {
	if plan == nil {
		return nil
	}
	tag, err := u.tx.Exec(u.ctx, `
		INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final, seeded_at, completed_at)
		VALUES ($1, $2, 'pending', $3, NULL, NULL)
		ON CONFLICT (tournament_id, round_number) DO NOTHING
	`, u.tid, plan.RoundNumber, plan.IsFinal)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		var status string
		var isFinal bool
		if err := u.tx.QueryRow(u.ctx, `
			SELECT status, is_final FROM tournament_rounds
			WHERE tournament_id = $1 AND round_number = $2
		`, u.tid, plan.RoundNumber).Scan(&status, &isFinal); err != nil {
			return wrapUnavailable(err)
		}
		if status != string(domain.RoundPending) || isFinal != plan.IsFinal {
			return fmt.Errorf("next round immutable conflict: status=%s is_final=%v want pending/%v", status, isFinal, plan.IsFinal)
		}
	}

	tag, err = u.tx.Exec(u.ctx, `
		INSERT INTO round_seeding_jobs (
			tournament_id, round_number, source, source_round_number, status,
			player_count, slot_count, base_size, remainder,
			next_slot_index, processed_player_count, last_player_id,
			command_id, correlation_id, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, 'pending',
			$5, $6, $7, $8,
			0, 0, '',
			$9, NULL, $10, $10
		)
		ON CONFLICT (tournament_id, round_number) DO NOTHING
	`, u.tid, plan.RoundNumber, plan.Source, plan.SourceRoundNumber,
		plan.PlayerCount, plan.SlotCount, plan.BaseSize, plan.Remainder,
		plan.JobCommandID, now)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		var pc, sc, base, rem int
		var status, source, cmdID string
		var srcRound *int
		if err := u.tx.QueryRow(u.ctx, `
			SELECT status, source, source_round_number, player_count, slot_count, base_size, remainder, command_id
			FROM round_seeding_jobs
			WHERE tournament_id = $1 AND round_number = $2
		`, u.tid, plan.RoundNumber).Scan(&status, &source, &srcRound, &pc, &sc, &base, &rem, &cmdID); err != nil {
			return wrapUnavailable(err)
		}
		srcOK := srcRound != nil && *srcRound == plan.SourceRoundNumber
		planOK := source == plan.Source && srcOK &&
			pc == plan.PlayerCount && sc == plan.SlotCount &&
			base == plan.BaseSize && rem == plan.Remainder
		identityOK := status == string(domain.SeedingJobPending) && cmdID == plan.JobCommandID && planOK
		if !identityOK {
			return fmt.Errorf("next seeding job immutable identity conflict")
		}
	}
	return nil
}

// LookupCompleteRoundOutcome reads command_idempotency outside a UoW.
func (s *TournamentStore) LookupCompleteRoundOutcome(ctx context.Context, commandID string) (envelope.Result, bool) {
	if s == nil || s.pool == nil || commandID == "" {
		return envelope.Result{}, false
	}
	var body []byte
	err := s.pool.QueryRow(ctx, `
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
