package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

// Durable seeding lock order (all rounds):
// Kickoff TX: exclusive rewrite barrier → global command lock → tournament row → round/job.
// Chunk TX: shared rewrite barrier → leased job row → source keyset → slots/ledger/checkpoint.
// Claim TX: tournament FOR UPDATE SKIP LOCKED → job FOR UPDATE SKIP LOCKED.
//
// Never: Service.mu, legacy whole-rewrite hydrate/persist, or full registration hydrate.

const (
	DefaultSeedingLease     = 5 * time.Minute
	DefaultSeedingReapLimit = 64
	seedingCommandType      = "SeedRound"
)

// errSeedingStaleFence is an internal signal: claim lost the lease generation fence.
// Callers must roll back and treat it as a harmless no-op (never surface as quarantine).
var errSeedingStaleFence = errors.New("seeding_stale_fence")

// SeedingCommitRequest is one atomic SeedRound kickoff persistence unit.
type SeedingCommitRequest struct {
	TournamentID  string
	CommandID     string
	CommandType   string
	CorrelationID string
	Outcome       envelope.Result
	Decision      domain.SeedRoundKickoffDecision
}

// SeedingUnitOfWork holds one READ COMMITTED kickoff transaction.
type SeedingUnitOfWork struct {
	store     *TournamentStore
	ctx       context.Context
	tx        pgx.Tx
	tid       string // empty for standalone command-only rejects
	roundNum  int
	commandID string
	kickoff   domain.SeedRoundKickoffContext
	exists    bool
	done      bool
}

func (u *SeedingUnitOfWork) Exists() bool { return u != nil && u.exists }

func (u *SeedingUnitOfWork) KickoffContext() domain.SeedRoundKickoffContext {
	if u == nil {
		return domain.SeedRoundKickoffContext{}
	}
	return u.kickoff
}

func (u *SeedingUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
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

func (u *SeedingUnitOfWork) finishWithPrior(body []byte) error {
	var out envelope.Result
	_ = json.Unmarshal(body, &out)
	_ = u.tx.Rollback(u.ctx)
	u.done = true
	return &PriorCommandOutcome{Outcome: out}
}

func (u *SeedingUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

func (u *SeedingUnitOfWork) bindCommit(req SeedingCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("seeding uow done")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}
	if req.CommandType != "" && req.CommandType != seedingCommandType {
		return fmt.Errorf("command type mismatch: want %q got %q", seedingCommandType, req.CommandType)
	}
	if u.tid != "" {
		if req.TournamentID != "" && req.TournamentID != u.tid {
			return fmt.Errorf("tournamentId mismatch: locked %q got %q", u.tid, req.TournamentID)
		}
	} else if req.Decision.Kind == domain.SeedKickoffSchedule {
		return fmt.Errorf("schedule decision requires tournament-bound seeding uow")
	}
	switch req.Decision.Kind {
	case domain.SeedKickoffSchedule:
		if u.tid == "" {
			return fmt.Errorf("schedule decision incompatible with standalone seeding uow")
		}
	case domain.SeedKickoffAlreadyDone, domain.SeedKickoffJobExistsNoop, domain.SeedKickoffReject:
		// Outcome-only paths are valid for tournament-bound and standalone.
	default:
		return fmt.Errorf("unknown seeding decision kind %q", req.Decision.Kind)
	}
	return nil
}

// BeginSeedRound1 starts exclusive-barrier + command-lock kickoff UoW for round 1.
func (s *TournamentStore) BeginSeedRound1(ctx context.Context, tournamentID, commandID string) (*SeedingUnitOfWork, error) {
	return s.BeginSeedRound(ctx, tournamentID, 1, commandID)
}

// BeginSeedRound starts exclusive-barrier + command-lock kickoff UoW for any round.
// Lock order: exclusive rewrite barrier → global command lock → tournament FOR UPDATE → load context.
func (s *TournamentStore) BeginSeedRound(ctx context.Context, tournamentID string, roundNumber int, commandID string) (*SeedingUnitOfWork, error) {
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
	uow := &SeedingUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, roundNum: roundNumber, commandID: commandID,
		kickoff: domain.SeedRoundKickoffContext{
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
	var registeredCount int
	err = tx.QueryRow(ctx, `
		SELECT phase, registered_count FROM tournaments WHERE tournament_id = $1 FOR UPDATE
	`, tournamentID).Scan(&phase, &registeredCount)
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

	if roundNumber == 1 {
		shardSum, err := sumRegistrationShardCountsTx(ctx, tx, tournamentID)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		if registeredCount != shardSum {
			_ = tx.Rollback(ctx)
			return nil, fmt.Errorf("registration count drift: tournaments.registered_count=%d shard_sum=%d", registeredCount, shardSum)
		}
		uow.kickoff.RegisteredCount = registeredCount
	} else {
		srcRound := roundNumber - 1
		var srcCount int
		err = tx.QueryRow(ctx, `
			SELECT COUNT(*)::int FROM round_advancing_players
			WHERE tournament_id = $1 AND source_round_number = $2
		`, tournamentID, srcRound).Scan(&srcCount)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		uow.kickoff.SourcePlayerCount = srcCount

		var prevStatus string
		err = tx.QueryRow(ctx, `
			SELECT status FROM tournament_rounds
			WHERE tournament_id = $1 AND round_number = $2
		`, tournamentID, srcRound).Scan(&prevStatus)
		if err == nil {
			uow.kickoff.PreviousRoundFound = true
			uow.kickoff.PreviousRoundStatus = domain.RoundStatus(prevStatus)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
	}

	var roundStatus string
	err = tx.QueryRow(ctx, `
		SELECT status FROM tournament_rounds
		WHERE tournament_id = $1 AND round_number = $2
	`, tournamentID, roundNumber).Scan(&roundStatus)
	if err == nil {
		uow.kickoff.RoundStatus = domain.RoundStatus(roundStatus)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var jobStatus, jobCmd string
	err = tx.QueryRow(ctx, `
		SELECT status, command_id FROM round_seeding_jobs
		WHERE tournament_id = $1 AND round_number = $2
	`, tournamentID, roundNumber).Scan(&jobStatus, &jobCmd)
	if err == nil {
		uow.kickoff.JobStatus = domain.SeedingJobStatus(jobStatus)
		uow.kickoff.JobCommandID = jobCmd
	} else if !errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginStandaloneSeedingCommand locks only the global command id (invalid envelope rejects).
func (s *TournamentStore) BeginStandaloneSeedingCommand(ctx context.Context, commandID string) (*SeedingUnitOfWork, error) {
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
	uow := &SeedingUnitOfWork{store: s, ctx: ctx, tx: tx, commandID: commandID}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// Commit applies kickoff decision: schedule pending round+job, or persist accepted/rejected outcome only.
func (u *SeedingUnitOfWork) Commit(req SeedingCommitRequest) error {
	if err := u.bindCommit(req); err != nil {
		return err
	}
	now := time.Now().UTC()
	if req.CommandType == "" {
		req.CommandType = seedingCommandType
	}
	if req.TournamentID == "" {
		req.TournamentID = u.tid
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
	case domain.SeedKickoffSchedule:
		if err := u.insertPendingRoundAndJob(req, now); err != nil {
			return err
		}
	case domain.SeedKickoffAlreadyDone, domain.SeedKickoffJobExistsNoop, domain.SeedKickoffReject:
		// Outcome-only; no aggregate mutation.
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

func (u *SeedingUnitOfWork) insertPendingRoundAndJob(req SeedingCommitRequest, now time.Time) error {
	plan := req.Decision.Plan
	rn := u.roundNum
	if rn < 1 {
		rn = 1
	}
	source := req.Decision.Source
	if source == "" {
		source = domain.SeedingSourceRegistrations
	}
	var srcRound any
	if source == domain.SeedingSourceAdvancement {
		srcRound = req.Decision.SourceRoundNumber
	}

	tag, err := u.tx.Exec(u.ctx, `
		INSERT INTO tournament_rounds (tournament_id, round_number, status, is_final, seeded_at, completed_at)
		VALUES ($1, $2, 'pending', $3, NULL, NULL)
		ON CONFLICT (tournament_id, round_number) DO NOTHING
	`, u.tid, rn, plan.IsFinal)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		var status string
		var isFinal bool
		if err := u.tx.QueryRow(u.ctx, `
			SELECT status, is_final FROM tournament_rounds
			WHERE tournament_id = $1 AND round_number = $2
		`, u.tid, rn).Scan(&status, &isFinal); err != nil {
			return wrapUnavailable(err)
		}
		if status != string(domain.RoundPending) || isFinal != plan.IsFinal {
			return fmt.Errorf("round immutable conflict: status=%s is_final=%v want pending/%v", status, isFinal, plan.IsFinal)
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
			$9, $10, $11, $11
		)
		ON CONFLICT (tournament_id, round_number) DO NOTHING
	`, u.tid, rn, source, srcRound,
		plan.PlayerCount, plan.SlotCount, plan.BaseSize, plan.Remainder,
		req.CommandID, nullIfEmpty(req.CorrelationID), now)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		var pc, sc, base, rem int
		var status, cmdID, existingSource string
		var existingSrcRound *int
		if err := u.tx.QueryRow(u.ctx, `
			SELECT status, source, source_round_number, player_count, slot_count, base_size, remainder, command_id
			FROM round_seeding_jobs
			WHERE tournament_id = $1 AND round_number = $2
		`, u.tid, rn).Scan(&status, &existingSource, &existingSrcRound, &pc, &sc, &base, &rem, &cmdID); err != nil {
			return wrapUnavailable(err)
		}
		if pc != plan.PlayerCount || sc != plan.SlotCount || base != plan.BaseSize || rem != plan.Remainder {
			return fmt.Errorf("seeding job immutable plan conflict")
		}
		if existingSource != source {
			return fmt.Errorf("seeding job immutable source conflict")
		}
		if source == domain.SeedingSourceAdvancement {
			if existingSrcRound == nil || *existingSrcRound != req.Decision.SourceRoundNumber {
				return fmt.Errorf("seeding job immutable source_round conflict")
			}
		} else if existingSrcRound != nil {
			return fmt.Errorf("seeding job immutable source_round conflict")
		}
		// Same or other command election under locked context — plan must match; accept.
		_ = status
		_ = cmdID
	}
	return nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// LookupSeedingOutcome reads command_idempotency outside a UoW.
func (s *TournamentStore) LookupSeedingOutcome(ctx context.Context, commandID string) (envelope.Result, bool) {
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

// ClaimedSeedingJob is one exclusively claimed seeding job (any round).
type ClaimedSeedingJob struct {
	TournamentID         string
	RoundNumber          int
	Source               string
	SourceRoundNumber    int // 0 when source=registrations (NULL in DB)
	Status               string
	PlayerCount          int
	SlotCount            int
	BaseSize             int
	Remainder            int
	NextSlotIndex        int
	ProcessedPlayerCount int
	LastPlayerID         string
	CommandID            string
	CorrelationID        string
	LeaseOwner           string
	LeaseExpiresAt       time.Time
	LeaseVersion         int64
	CompletedAt          *time.Time
}

func (j ClaimedSeedingJob) Plan() domain.RoundSlotPlan {
	return domain.RoundSlotPlan{
		PlayerCount: j.PlayerCount,
		SlotCount:   j.SlotCount,
		BaseSize:    j.BaseSize,
		Remainder:   j.Remainder,
		IsFinal:     j.PlayerCount <= domain.FinalPlayerThreshold,
	}
}

// ReapExpiredSeedingLeases resets a bounded set of expired in_progress seeding jobs to pending.
func (s *TournamentStore) ReapExpiredSeedingLeases(ctx context.Context, now time.Time) (int64, error) {
	return s.ReapExpiredSeedingLeasesBounded(ctx, now, DefaultSeedingReapLimit)
}

// ReapExpiredSeedingLeasesBounded is the testable reap entry with an explicit row limit.
// Also cancels active jobs whose tournament is already terminal (terminal drift).
func (s *TournamentStore) ReapExpiredSeedingLeasesBounded(ctx context.Context, now time.Time, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("nil store")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if limit <= 0 {
		limit = DefaultSeedingReapLimit
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := cancelTerminalSeedingJobsTx(ctx, tx, now, limit); err != nil {
		return 0, err
	}

	var tid string
	err = tx.QueryRow(ctx, `
		SELECT t.tournament_id
		FROM tournaments t
		WHERE t.phase NOT IN ('completed', 'cancelled')
		  AND EXISTS (
			SELECT 1 FROM round_seeding_jobs j
			WHERE j.tournament_id = t.tournament_id
			  AND j.status = 'in_progress'
			  AND j.lease_expires_at IS NOT NULL
			  AND j.lease_expires_at < $1
		)
		ORDER BY t.tournament_id ASC
		FOR UPDATE OF t SKIP LOCKED
		LIMIT 1
	`, now).Scan(&tid)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Single UPDATE...FROM with one WHERE (join predicates only). Expiry filter lives in the CTE.
	// Clears owner/expiry only — lease_version is preserved so the next claim still bumps the fence.
	tag, err := tx.Exec(ctx, `
		WITH picked AS (
			SELECT tournament_id, round_number
			FROM round_seeding_jobs
			WHERE tournament_id = $2
			  AND status = 'in_progress'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at < $1
			ORDER BY lease_expires_at ASC, round_number ASC
			FOR UPDATE
			LIMIT $3
		)
		UPDATE round_seeding_jobs AS j
		SET status = 'pending',
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    updated_at = $1
		FROM picked AS p
		WHERE j.tournament_id = p.tournament_id
		  AND j.round_number = p.round_number
	`, now, tid, limit)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func cancelTerminalSeedingJobsTx(ctx context.Context, tx pgx.Tx, now time.Time, limit int) (int64, error) {
	tag, err := tx.Exec(ctx, `
		WITH picked AS (
			SELECT j.tournament_id, j.round_number
			FROM round_seeding_jobs j
			INNER JOIN tournaments t ON t.tournament_id = j.tournament_id
			WHERE t.phase IN ('completed', 'cancelled')
			  AND j.status IN ('pending', 'in_progress')
			ORDER BY j.updated_at ASC, j.tournament_id ASC
			FOR UPDATE OF j
			LIMIT $2
		)
		UPDATE round_seeding_jobs AS j
		SET status = 'cancelled',
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    completed_at = NULL,
		    updated_at = $1
		FROM picked AS p
		WHERE j.tournament_id = p.tournament_id
		  AND j.round_number = p.round_number
	`, now, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ClaimNextSeedingJob claims at most one pending or expired in_progress seeding job (any round).
// Tournament-first, then earliest round_number among claimable jobs.
func (s *TournamentStore) ClaimNextSeedingJob(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration) (*ClaimedSeedingJob, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if leaseTTL <= 0 {
		leaseTTL = DefaultSeedingLease
	}
	expires := now.Add(leaseTTL)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := cancelTerminalSeedingJobsTx(ctx, tx, now, DefaultSeedingReapLimit); err != nil {
		return nil, err
	}

	var lockedTID string
	err = tx.QueryRow(ctx, `
		SELECT t.tournament_id
		FROM tournaments t
		WHERE t.phase NOT IN ('completed', 'cancelled')
		  AND EXISTS (
			SELECT 1 FROM round_seeding_jobs j
			WHERE j.tournament_id = t.tournament_id
			  AND (
				j.status = 'pending'
				OR (
					j.status = 'in_progress'
					AND j.lease_expires_at IS NOT NULL
					AND j.lease_expires_at < $1
				)
			  )
		  )
		ORDER BY t.created_at ASC, t.tournament_id ASC
		FOR UPDATE OF t SKIP LOCKED
		LIMIT 1
	`, now).Scan(&lockedTID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var job ClaimedSeedingJob
	var corr *string
	var srcRound *int
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT tournament_id, round_number
			FROM round_seeding_jobs
			WHERE tournament_id = $1
			  AND (
				status = 'pending'
				OR (
					status = 'in_progress'
					AND lease_expires_at IS NOT NULL
					AND lease_expires_at < $2
				)
			  )
			ORDER BY round_number ASC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE round_seeding_jobs j
		SET status = 'in_progress',
		    lease_owner = $3,
		    lease_expires_at = $4,
		    lease_version = j.lease_version + 1,
		    updated_at = $2
		FROM candidate c
		WHERE j.tournament_id = c.tournament_id
		  AND j.round_number = c.round_number
		RETURNING j.tournament_id, j.round_number, j.source, j.source_round_number, j.status,
			j.player_count, j.slot_count, j.base_size, j.remainder,
			j.next_slot_index, j.processed_player_count, j.last_player_id,
			j.command_id, j.correlation_id, j.lease_owner, j.lease_expires_at,
			j.lease_version
	`, lockedTID, now, owner, expires).Scan(
		&job.TournamentID, &job.RoundNumber, &job.Source, &srcRound, &job.Status,
		&job.PlayerCount, &job.SlotCount, &job.BaseSize, &job.Remainder,
		&job.NextSlotIndex, &job.ProcessedPlayerCount, &job.LastPlayerID,
		&job.CommandID, &corr, &job.LeaseOwner, &job.LeaseExpiresAt,
		&job.LeaseVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if corr != nil {
		job.CorrelationID = *corr
	}
	if srcRound != nil {
		job.SourceRoundNumber = *srcRound
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
}

type seedingSlotIns struct {
	Index           int      `json:"slot_index"`
	SlotID          string   `json:"slot_id"`
	SeededPlayerIDs []string `json:"seeded_player_ids"`
}

// ProcessSeedingChunk advances one leased job by at most MaxSeedingSlotsPerChunk slots.
func (s *TournamentStore) ProcessSeedingChunk(ctx context.Context, job ClaimedSeedingJob, owner string, now time.Time) (completed bool, err error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("nil store")
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return false, fmt.Errorf("lease owner is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	rn := job.RoundNumber
	if rn < 1 {
		rn = 1
	}
	plan := job.Plan()
	slotFrom, slotTo, playerLimit := domain.NextSeedingChunkBounds(plan, job.NextSlotIndex)
	if playerLimit == 0 {
		return s.finalizeSeedingJob(ctx, job, owner, now)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := acquireRewriteBarrierShared(ctx, tx, job.TournamentID); err != nil {
		return false, wrapUnavailable(err)
	}

	var (
		curStatus                             string
		curSource                             string
		curSrcRound                           *int
		curPlayers, curSlots, curBase, curRem int
		curNext, curProcessed                 int
		curLast, curOwner                     string
		curLeaseVersion                       int64
	)
	err = tx.QueryRow(ctx, `
		SELECT status, source, source_round_number, player_count, slot_count, base_size, remainder,
			next_slot_index, processed_player_count, last_player_id, COALESCE(lease_owner, ''), lease_version
		FROM round_seeding_jobs
		WHERE tournament_id = $1 AND round_number = $2
		FOR UPDATE
	`, job.TournamentID, rn).Scan(
		&curStatus, &curSource, &curSrcRound, &curPlayers, &curSlots, &curBase, &curRem,
		&curNext, &curProcessed, &curLast, &curOwner, &curLeaseVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}
	if err != nil {
		return false, wrapUnavailable(err)
	}
	switch domain.SeedingJobStatus(curStatus) {
	case domain.SeedingJobCompleted, domain.SeedingJobCancelled, domain.SeedingJobQuarantined:
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return curStatus == string(domain.SeedingJobCompleted), nil
	case domain.SeedingJobInProgress:
		if curOwner != owner || curLeaseVersion != job.LeaseVersion {
			if err := tx.Commit(ctx); err != nil {
				return false, wrapUnavailable(err)
			}
			return false, nil
		}
	default:
		// Pending/other after reclaim race — stale claim is a no-op.
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}
	if curNext != job.NextSlotIndex || curProcessed != job.ProcessedPlayerCount || curLast != job.LastPlayerID {
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}

	var phase string
	err = tx.QueryRow(ctx, `
		SELECT phase FROM tournaments WHERE tournament_id = $1
	`, job.TournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "tournament_missing")
	}
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if phase == "cancelled" || phase == "completed" {
		return false, cancelSeedingTx(ctx, tx, job, owner, now)
	}
	if phase != "seeding" && phase != "in_progress" {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "phase_drift")
	}
	if curPlayers != plan.PlayerCount || curSlots != plan.SlotCount || curBase != plan.BaseSize || curRem != plan.Remainder {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "immutable_plan_drift")
	}
	source := curSource
	if source == "" {
		source = job.Source
	}
	srcRound := job.SourceRoundNumber
	if curSrcRound != nil {
		srcRound = *curSrcRound
	}

	players, err := loadSeedingSourcePlayers(ctx, tx, job.TournamentID, source, srcRound, curLast, playerLimit)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if len(players) != playerLimit {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "source_count_shortfall")
	}

	offset := 0
	slots := make([]seedingSlotIns, 0, slotTo-slotFrom)
	for i := slotFrom; i < slotTo; i++ {
		size := plan.SizeForSlot(i)
		seeded := make([]string, size)
		copy(seeded, players[offset:offset+size])
		slots = append(slots, seedingSlotIns{
			Index:           i,
			SlotID:          string(domain.SlotIDForIndex(i)),
			SeededPlayerIDs: seeded,
		})
		offset += size
	}

	payload, err := json.Marshal(slots)
	if err != nil {
		return false, err
	}
	// Two bounded statements in this tx: slots first, then mappings.
	// Sibling data-modifying CTEs share one snapshot, so map_ins cannot FK-see
	// bracket_slots rows inserted by a sibling ins CTE in the same statement.
	// Player mappings are populated set-based from the same incoming payload (never scan DB arrays).
	var slotConflicts int
	err = tx.QueryRow(ctx, `
		WITH incoming AS (
			SELECT x.slot_id, x.slot_index, x.seeded_player_ids
			FROM jsonb_to_recordset($4::jsonb) AS x(
				slot_id text,
				slot_index int,
				seeded_player_ids text[]
			)
		),
		ins AS (
			INSERT INTO bracket_slots (
				tournament_id, round_number, slot_id, slot_index, status, seeded_player_ids, created_at, updated_at
			)
			SELECT $1, $2, i.slot_id, i.slot_index, 'pending', i.seeded_player_ids, $3, $3
			FROM incoming i
			ON CONFLICT (tournament_id, round_number, slot_id) DO NOTHING
			RETURNING slot_id
		),
		slot_conflicts AS (
			SELECT 1
			FROM incoming i
			INNER JOIN bracket_slots b
				ON b.tournament_id = $1 AND b.round_number = $2 AND b.slot_id = i.slot_id
			WHERE NOT EXISTS (SELECT 1 FROM ins WHERE ins.slot_id = i.slot_id)
			  AND (
				b.slot_index IS DISTINCT FROM i.slot_index
				OR b.seeded_player_ids IS DISTINCT FROM i.seeded_player_ids
			  )
		)
		SELECT (SELECT COUNT(*)::int FROM slot_conflicts)
	`, job.TournamentID, rn, now, payload).Scan(&slotConflicts)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if slotConflicts > 0 {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "immutable_slot_conflict")
	}

	var mapConflicts int
	err = tx.QueryRow(ctx, `
		WITH incoming AS (
			SELECT x.slot_id, x.slot_index, x.seeded_player_ids
			FROM jsonb_to_recordset($3::jsonb) AS x(
				slot_id text,
				slot_index int,
				seeded_player_ids text[]
			)
		),
		incoming_players AS (
			SELECT i.slot_id, p.player_id, (p.ord - 1)::int AS seat_index
			FROM incoming i
			CROSS JOIN LATERAL unnest(i.seeded_player_ids) WITH ORDINALITY AS p(player_id, ord)
		),
		map_ins AS (
			INSERT INTO tournament_round_slot_players (
				tournament_id, round_number, player_id, slot_id, seat_index
			)
			SELECT $1, $2, ip.player_id, ip.slot_id, ip.seat_index
			FROM incoming_players ip
			ON CONFLICT (tournament_id, round_number, player_id) DO NOTHING
			RETURNING player_id
		),
		map_conflicts AS (
			SELECT 1
			FROM incoming_players ip
			INNER JOIN tournament_round_slot_players m
				ON m.tournament_id = $1 AND m.round_number = $2 AND m.player_id = ip.player_id
			WHERE NOT EXISTS (SELECT 1 FROM map_ins WHERE map_ins.player_id = ip.player_id)
			  AND (
				m.slot_id IS DISTINCT FROM ip.slot_id
				OR m.seat_index IS DISTINCT FROM ip.seat_index
			  )
		)
		SELECT (SELECT COUNT(*)::int FROM map_conflicts)
	`, job.TournamentID, rn, payload).Scan(&mapConflicts)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if mapConflicts > 0 {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "immutable_player_mapping_conflict")
	}

	checksum := seedingChunkChecksum(slots)
	batchIndex := slotFrom
	lastPlayer := players[len(players)-1]
	tag, err := tx.Exec(ctx, `
		INSERT INTO round_seeding_batches (
			tournament_id, round_number, batch_index,
			slot_index_from, slot_index_to, player_count,
			source_cursor_after, source_cursor_to, checksum, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (tournament_id, round_number, batch_index) DO NOTHING
	`, job.TournamentID, rn, batchIndex, slotFrom, slotTo-1, playerLimit, curLast, lastPlayer, checksum, now)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if tag.RowsAffected() == 0 {
		var from, to, pc int
		var after, toCursor, existingChecksum string
		if err := tx.QueryRow(ctx, `
			SELECT slot_index_from, slot_index_to, player_count,
				source_cursor_after, source_cursor_to, checksum
			FROM round_seeding_batches
			WHERE tournament_id = $1 AND round_number = $2 AND batch_index = $3
		`, job.TournamentID, rn, batchIndex).Scan(&from, &to, &pc, &after, &toCursor, &existingChecksum); err != nil {
			return false, wrapUnavailable(err)
		}
		if from != slotFrom || to != slotTo-1 || pc != playerLimit ||
			after != curLast || toCursor != lastPlayer || existingChecksum != checksum {
			return false, quarantineSeedingTx(ctx, tx, job, owner, now, "immutable_batch_conflict")
		}
	}

	newNext := slotTo
	newProcessed := curProcessed + playerLimit
	more := newNext < plan.SlotCount
	if more {
		tag, err = tx.Exec(ctx, `
			UPDATE round_seeding_jobs
			SET next_slot_index = $3,
			    processed_player_count = $4,
			    last_player_id = $5,
			    status = 'pending',
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    updated_at = $6
			WHERE tournament_id = $1 AND round_number = $2
			  AND status = 'in_progress'
			  AND lease_owner = $7
			  AND lease_version = $8
			  AND next_slot_index = $9
			  AND processed_player_count = $10
			  AND last_player_id = $11
		`, job.TournamentID, rn, newNext, newProcessed, lastPlayer, now,
			owner, job.LeaseVersion, curNext, curProcessed, curLast)
		if err != nil {
			return false, wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}

	if newProcessed != plan.PlayerCount || newNext != plan.SlotCount {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "final_counter_mismatch")
	}
	if err := verifySeedingSourceExhausted(ctx, tx, job.TournamentID, source, srcRound, lastPlayer, plan.PlayerCount); err != nil {
		if qerr, ok := err.(*seedingQuarantineSignal); ok {
			return false, quarantineSeedingTx(ctx, tx, job, owner, now, qerr.reason)
		}
		return false, wrapUnavailable(err)
	}

	done, err := finalizeSeedingInTx(ctx, tx, job, owner, plan, newProcessed, lastPlayer, now)
	if errors.Is(err, errSeedingStaleFence) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, wrapUnavailable(err)
	}
	return done, nil
}

type seedingQuarantineSignal struct{ reason string }

func (e *seedingQuarantineSignal) Error() string { return e.reason }

func loadSeedingSourcePlayers(ctx context.Context, tx pgx.Tx, tid, source string, srcRound int, afterPlayerID string, limit int) ([]string, error) {
	var rows pgx.Rows
	var err error
	switch source {
	case domain.SeedingSourceAdvancement:
		rows, err = tx.Query(ctx, `
			SELECT player_id FROM round_advancing_players
			WHERE tournament_id = $1 AND source_round_number = $2 AND player_id > $3
			ORDER BY player_id ASC
			LIMIT $4
		`, tid, srcRound, afterPlayerID, limit)
	default:
		rows, err = tx.Query(ctx, `
			SELECT player_id FROM tournament_registrations
			WHERE tournament_id = $1 AND status = 'registered' AND player_id > $2
			ORDER BY player_id ASC
			LIMIT $3
		`, tid, afterPlayerID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	players := make([]string, 0, limit)
	for rows.Next() {
		var pid string
		if err := rows.Scan(&pid); err != nil {
			return nil, err
		}
		players = append(players, pid)
	}
	return players, rows.Err()
}

func verifySeedingSourceExhausted(ctx context.Context, tx pgx.Tx, tid, source string, srcRound int, lastPlayer string, wantCount int) error {
	var probe string
	var err error
	switch source {
	case domain.SeedingSourceAdvancement:
		err = tx.QueryRow(ctx, `
			SELECT player_id FROM round_advancing_players
			WHERE tournament_id = $1 AND source_round_number = $2 AND player_id > $3
			ORDER BY player_id ASC
			LIMIT 1
		`, tid, srcRound, lastPlayer).Scan(&probe)
		if err == nil {
			return &seedingQuarantineSignal{reason: "extra_source_advancement"}
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var srcCount int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)::int FROM round_advancing_players
			WHERE tournament_id = $1 AND source_round_number = $2
		`, tid, srcRound).Scan(&srcCount); err != nil {
			return err
		}
		if srcCount != wantCount {
			return &seedingQuarantineSignal{reason: "source_count_drift"}
		}
	default:
		err = tx.QueryRow(ctx, `
			SELECT player_id FROM tournament_registrations
			WHERE tournament_id = $1 AND status = 'registered' AND player_id > $2
			ORDER BY player_id ASC
			LIMIT 1
		`, tid, lastPlayer).Scan(&probe)
		if err == nil {
			return &seedingQuarantineSignal{reason: "extra_source_registration"}
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		shardSum, err := sumRegistrationShardCountsTx(ctx, tx, tid)
		if err != nil {
			return err
		}
		if shardSum != wantCount {
			return &seedingQuarantineSignal{reason: "source_count_drift"}
		}
	}
	return nil
}

func (s *TournamentStore) finalizeSeedingJob(ctx context.Context, job ClaimedSeedingJob, owner string, now time.Time) (bool, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return false, fmt.Errorf("lease owner is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := acquireRewriteBarrierShared(ctx, tx, job.TournamentID); err != nil {
		return false, wrapUnavailable(err)
	}
	rn := job.RoundNumber
	if rn < 1 {
		rn = 1
	}
	plan := job.Plan()
	var status string
	var next, processed int
	var last, leaseOwner string
	var leaseVersion int64
	err = tx.QueryRow(ctx, `
		SELECT status, next_slot_index, processed_player_count, last_player_id,
			COALESCE(lease_owner, ''), lease_version
		FROM round_seeding_jobs WHERE tournament_id = $1 AND round_number = $2 FOR UPDATE
	`, job.TournamentID, rn).Scan(&status, &next, &processed, &last, &leaseOwner, &leaseVersion)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if status == string(domain.SeedingJobCompleted) {
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return true, nil
	}
	if status == string(domain.SeedingJobCancelled) || status == string(domain.SeedingJobQuarantined) {
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}
	if status != string(domain.SeedingJobInProgress) || leaseOwner != owner || leaseVersion != job.LeaseVersion {
		if err := tx.Commit(ctx); err != nil {
			return false, wrapUnavailable(err)
		}
		return false, nil
	}
	if next != plan.SlotCount || processed != plan.PlayerCount {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "finalize_counter_mismatch")
	}
	done, err := finalizeSeedingInTx(ctx, tx, job, owner, plan, processed, last, now)
	if errors.Is(err, errSeedingStaleFence) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, wrapUnavailable(err)
	}
	return done, nil
}

// finalizeSeedingInTx applies terminal cancel or pending→seeded (+ later-round provisioning) + projection bump.
// Round 1 stays seeded for manual ProvisionRoundMatches. Round>1 transitions to provisioning with batches+shards.
// Returns done=true only when the successful finalize path ran (caller must commit).
// Stale lease fence returns errSeedingStaleFence so the caller rolls back without committing.
func finalizeSeedingInTx(ctx context.Context, tx pgx.Tx, job ClaimedSeedingJob, owner string, plan domain.RoundSlotPlan, processed int, lastPlayer string, now time.Time) (bool, error) {
	tid := job.TournamentID
	rn := job.RoundNumber
	if rn < 1 {
		rn = 1
	}
	var phase string
	var rulesRaw []byte
	if err := tx.QueryRow(ctx, `
		SELECT phase, rules FROM tournaments WHERE tournament_id = $1 FOR UPDATE
	`, tid).Scan(&phase, &rulesRaw); err != nil {
		return false, wrapUnavailable(err)
	}
	if phase == "cancelled" || phase == "completed" {
		tag, err := tx.Exec(ctx, `
			UPDATE round_seeding_jobs
			SET status = 'cancelled',
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    completed_at = NULL,
			    updated_at = $3
			WHERE tournament_id = $1 AND round_number = $2
			  AND status = 'in_progress'
			  AND lease_owner = $4
			  AND lease_version = $5
		`, tid, rn, now, owner, job.LeaseVersion)
		if err != nil {
			return false, wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return false, errSeedingStaleFence
		}
		// Round stays pending/hidden; no projection bump.
		return false, nil
	}
	if phase != "seeding" && phase != "in_progress" {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "phase_drift")
	}

	var roundStatus string
	err := tx.QueryRow(ctx, `
		SELECT status FROM tournament_rounds
		WHERE tournament_id = $1 AND round_number = $2
		FOR UPDATE
	`, tid, rn).Scan(&roundStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "round_missing")
	}
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if roundStatus != string(domain.RoundPending) {
		return false, quarantineSeedingTx(ctx, tx, job, owner, now, "round_not_pending")
	}

	var rules tournamentRules
	jsonUnmarshalRules(rulesRaw, &rules)
	var laterProv *domain.ProvisionBatchPlan
	if rn > 1 {
		batchSize := rules.BatchSize
		if batchSize <= 0 {
			batchSize = domain.DefaultBatchSize
		}
		if batchSize > domain.MaxProvisioningBatchSize {
			batchSize = domain.MaxProvisioningBatchSize
		}
		provPlan, err := domain.ComputeProvisionBatchPlan(plan.SlotCount, batchSize)
		if err != nil {
			return false, quarantineSeedingTx(ctx, tx, job, owner, now, "provision_plan_invalid")
		}
		laterProv = &provPlan
	}

	// Mutations below must succeed as one unit; RowsAffected failures return so caller rolls back.
	tag, err := tx.Exec(ctx, `
		UPDATE round_seeding_jobs
		SET status = 'completed',
		    next_slot_index = $3,
		    processed_player_count = $4,
		    last_player_id = $5,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    completed_at = $6,
		    updated_at = $6
		WHERE tournament_id = $1 AND round_number = $2
		  AND status = 'in_progress'
		  AND lease_owner = $7
		  AND lease_version = $8
	`, tid, rn, plan.SlotCount, processed, lastPlayer, now, owner, job.LeaseVersion)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return false, errSeedingStaleFence
	}

	tag, err = tx.Exec(ctx, `
		UPDATE tournament_rounds
		SET status = 'seeded', is_final = $3, seeded_at = $4
		WHERE tournament_id = $1 AND round_number = $2 AND status = 'pending'
	`, tid, rn, plan.IsFinal, now)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("seeding finalize: round seed rows_affected=%d", tag.RowsAffected())
	}

	rules.CurrentRound = rn
	newRules, err := json.Marshal(rules)
	if err != nil {
		return false, err
	}
	newPhase := phase
	if phase == "seeding" {
		newPhase = "in_progress"
	}
	tag, err = tx.Exec(ctx, `
		UPDATE tournaments SET phase = $2, rules = $3::jsonb, updated_at = $4
		WHERE tournament_id = $1 AND phase IN ('seeding', 'in_progress')
	`, tid, newPhase, newRules, now)
	if err != nil {
		return false, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("seeding finalize: tournament transition rows_affected=%d", tag.RowsAffected())
	}

	if laterProv != nil {
		if err := finalizeLaterRoundProvisioningInTx(ctx, tx, tid, rn, *laterProv, now); err != nil {
			return false, err
		}
	}

	if err := bumpProjectionVersionTx(ctx, tx, tid, now); err != nil {
		return false, wrapUnavailable(err)
	}
	return true, nil
}

// finalizeLaterRoundProvisioningInTx creates provisioning batches + 64 shards and moves round to provisioning.
// Does not assign rooms or emit MatchAssigned (same generate_series pattern as provisioning.applySchedule).
func finalizeLaterRoundProvisioningInTx(ctx context.Context, tx pgx.Tx, tid string, rn int, provPlan domain.ProvisionBatchPlan, now time.Time) error {
	tag, err := tx.Exec(ctx, `
		UPDATE tournament_rounds
		SET status = 'provisioning'
		WHERE tournament_id = $1 AND round_number = $2 AND status = 'seeded'
	`, tid, rn)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("seeding finalize: later-round provisioning transition rows_affected=%d", tag.RowsAffected())
	}

	_, err = tx.Exec(ctx, `
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
	`, tid, rn, provPlan.BatchSize, provPlan.SlotCount, now, provPlan.BatchCount)
	if err != nil {
		return wrapUnavailable(err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO round_progress_shards (tournament_id, round_number, shard_id, assigned_count, resolved_count, quarantined_count, advancing_count)
		SELECT $1, $2, g.i, 0, 0, 0, 0
		FROM generate_series(0, $3::int - 1) AS g(i)
		ON CONFLICT (tournament_id, round_number, shard_id) DO NOTHING
	`, tid, rn, domain.ProgressShardCount)
	if err != nil {
		return wrapUnavailable(err)
	}
	var shardCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM round_progress_shards
		WHERE tournament_id = $1 AND round_number = $2
	`, tid, rn).Scan(&shardCount); err != nil {
		return wrapUnavailable(err)
	}
	if shardCount != domain.ProgressShardCount {
		return fmt.Errorf("seeding finalize: progress shards=%d want %d", shardCount, domain.ProgressShardCount)
	}
	return nil
}

func quarantineSeedingTx(ctx context.Context, tx pgx.Tx, job ClaimedSeedingJob, owner string, now time.Time, reason string) error {
	reason = sanitizeSeedingReason(reason)
	rn := job.RoundNumber
	if rn < 1 {
		rn = 1
	}
	tag, err := tx.Exec(ctx, `
		UPDATE round_seeding_jobs
		SET status = 'quarantined',
		    quarantine_reason = $3,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    completed_at = NULL,
		    updated_at = $4
		WHERE tournament_id = $1 AND round_number = $2
		  AND status = 'in_progress'
		  AND lease_owner = $5
		  AND lease_version = $6
	`, job.TournamentID, rn, reason, now, owner, job.LeaseVersion)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		// Stale claim — leave TX uncommitted so caller defer rolls back any prior inserts.
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapUnavailable(err)
	}
	return fmt.Errorf("seeding quarantined: %s", reason)
}

func cancelSeedingTx(ctx context.Context, tx pgx.Tx, job ClaimedSeedingJob, owner string, now time.Time) error {
	rn := job.RoundNumber
	if rn < 1 {
		rn = 1
	}
	tag, err := tx.Exec(ctx, `
		UPDATE round_seeding_jobs
		SET status = 'cancelled',
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    completed_at = NULL,
		    updated_at = $3
		WHERE tournament_id = $1 AND round_number = $2
		  AND status = 'in_progress'
		  AND lease_owner = $4
		  AND lease_version = $5
	`, job.TournamentID, rn, now, owner, job.LeaseVersion)
	if err != nil {
		return wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return wrapUnavailable(err)
	}
	return nil
}

// seedingQuarantineReasons is the closed set of operational codes persistable on quarantine_reason.
var seedingQuarantineReasons = map[string]struct{}{
	"tournament_missing":        {},
	"phase_drift":               {},
	"immutable_plan_drift":      {},
	"source_count_shortfall":    {},
	"immutable_slot_conflict":            {},
	"immutable_player_mapping_conflict":  {},
	"immutable_batch_conflict":           {},
	"final_counter_mismatch":    {},
	"extra_source_advancement":  {},
	"extra_source_registration": {},
	"source_count_drift":        {},
	"finalize_counter_mismatch": {},
	"round_missing":             {},
	"round_not_pending":         {},
	"provision_plan_invalid":    {},
	"unknown":                   {},
}

func sanitizeSeedingReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if _, ok := seedingQuarantineReasons[reason]; ok {
		return reason
	}
	return "unknown"
}

func seedingChunkChecksum(slots []seedingSlotIns) string {
	h := sha256.New()
	for _, sl := range slots {
		_, _ = fmt.Fprintf(h, "%d|%s|%s\n", sl.Index, sl.SlotID, strings.Join(sl.SeededPlayerIDs, ","))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// LoadSeedingJob loads one job row (internal/worker tests).
func (s *TournamentStore) LoadSeedingJob(ctx context.Context, tournamentID string, roundNumber int) (*ClaimedSeedingJob, bool, error) {
	if s == nil || s.pool == nil {
		return nil, false, fmt.Errorf("nil store")
	}
	if roundNumber < 1 {
		roundNumber = 1
	}
	var job ClaimedSeedingJob
	var corr *string
	var completedAt *time.Time
	var srcRound *int
	err := s.pool.QueryRow(ctx, `
		SELECT tournament_id, round_number, source, source_round_number, status,
			player_count, slot_count, base_size, remainder,
			next_slot_index, processed_player_count, last_player_id,
			command_id, correlation_id, COALESCE(lease_owner, ''), COALESCE(lease_expires_at, to_timestamp(0)),
			lease_version, completed_at
		FROM round_seeding_jobs
		WHERE tournament_id = $1 AND round_number = $2
	`, tournamentID, roundNumber).Scan(
		&job.TournamentID, &job.RoundNumber, &job.Source, &srcRound, &job.Status,
		&job.PlayerCount, &job.SlotCount, &job.BaseSize, &job.Remainder,
		&job.NextSlotIndex, &job.ProcessedPlayerCount, &job.LastPlayerID,
		&job.CommandID, &corr, &job.LeaseOwner, &job.LeaseExpiresAt,
		&job.LeaseVersion, &completedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if corr != nil {
		job.CorrelationID = *corr
	}
	if srcRound != nil {
		job.SourceRoundNumber = *srcRound
	}
	job.CompletedAt = completedAt
	return &job, true, nil
}
