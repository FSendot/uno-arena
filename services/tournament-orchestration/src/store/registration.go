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

// T-Reg lock order (deadlock-safe):
//  1. Rewrite barrier (shared for register; exclusive for create/close) when tournament id exists
//  2. Global command lock tournament:command:{commandId}
//  3. Per-(tournamentId,playerId) advisory xact lock (register only)
//  4. command_idempotency re-read (return canonical prior if present)
//  5. Phase/capacity read (never tournaments FOR UPDATE on register)
//  6. Registration row existence / shard UPDATE ... WHERE count<quota
//  7. On shard-full (newCount==quota): registration-close election lock, then fresh all-full check
//  8. Insert registration; bump projection shard (and base on auto-close/create/close)
//
// Standalone invalid/outcome-only: command lock only.
//
// Never: whole-tournament hydrate, full-aggregate rewrite, or SUM-admit across shards on every register.

const registrationPlayerLockPrefix = "tournament:regplayer:"

const registrationPlayerLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

func registrationPlayerLockKey(tournamentID, playerID string) string {
	// Unit separator (U+001F): valid UTF-8 for Postgres text; distinct from rewrite barrier keys.
	// (NUL cannot be used — Postgres rejects 0x00 in text parameters.)
	return registrationPlayerLockPrefix + tournamentID + "\x1f" + playerID
}

func acquireRegistrationPlayerLock(ctx context.Context, tx pgx.Tx, tournamentID, playerID string) error {
	if tournamentID == "" || playerID == "" {
		return fmt.Errorf("tournamentId and playerId required for registration lock")
	}
	_, err := tx.Exec(ctx, registrationPlayerLockSQL, registrationPlayerLockKey(tournamentID, playerID))
	return err
}

// RegistrationOp classifies the durable registration UoW commit path.
type RegistrationOp string

const (
	RegistrationOpCreate     RegistrationOp = "create"
	RegistrationOpRegister   RegistrationOp = "register"
	RegistrationOpClose      RegistrationOp = "close"
	RegistrationOpStandalone RegistrationOp = "standalone"
)

// RegistrationCommitRequest is one atomic T-Reg persistence unit.
type RegistrationCommitRequest struct {
	Op                 RegistrationOp
	TournamentID       string
	CommandID          string
	CommandType        string
	Outcome            envelope.Result
	Events             []OutboxEvent
	Decision           domain.RegistrationDecision
	CreateCmd          domain.CreateTournamentCommand
	RetryBudget        int
	BatchSize          int
	BumpBaseProjection bool
}

// RegistrationUnitOfWork holds one READ COMMITTED tx for bounded create/register/close.
type RegistrationUnitOfWork struct {
	store     *TournamentStore
	ctx       context.Context
	tx        pgx.Tx
	tid       string
	playerID  string
	commandID string
	op        RegistrationOp
	regCtx    domain.RegistrationContext
	closeCtx  domain.CloseRegistrationContext
	createCtx domain.CreateTournamentContext
	exists    bool
	reserved  bool // true after ReserveRegistration mutated quota/row/projection/phase
	done      bool
}

func (u *RegistrationUnitOfWork) Exists() bool { return u != nil && u.exists }

func (u *RegistrationUnitOfWork) RegisterContext() domain.RegistrationContext {
	if u == nil {
		return domain.RegistrationContext{}
	}
	return u.regCtx
}

func (u *RegistrationUnitOfWork) CloseContext() domain.CloseRegistrationContext {
	if u == nil {
		return domain.CloseRegistrationContext{}
	}
	return u.closeCtx
}

func (u *RegistrationUnitOfWork) CreateContext() domain.CreateTournamentContext {
	if u == nil {
		return domain.CreateTournamentContext{}
	}
	return u.createCtx
}

// LookupOutcome reads command_idempotency under the held transaction.
// Only the commandID locked at Begin may be queried; any other id returns false.
func (u *RegistrationUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
	if u == nil || u.done || commandID == "" || commandID != u.commandID {
		return envelope.Result{}, false
	}
	var body []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1
	`, commandID).Scan(&body)
	if err != nil {
		return envelope.Result{}, false
	}
	var out envelope.Result
	if err := json.Unmarshal(body, &out); err != nil {
		return envelope.Result{}, false
	}
	return out, true
}

func (u *RegistrationUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

func (u *RegistrationUnitOfWork) finishWithPrior(body []byte) error {
	var prior envelope.Result
	if err := json.Unmarshal(body, &prior); err != nil {
		u.done = true
		_ = u.tx.Rollback(u.ctx)
		return wrapUnavailable(err)
	}
	u.done = true
	_ = u.tx.Rollback(u.ctx)
	return &PriorCommandOutcome{Outcome: prior}
}

// BeginStandaloneCommand starts a command-lock-only UoW for invalid/outcome-only T-Reg rejects.
func (s *TournamentStore) BeginStandaloneCommand(ctx context.Context, commandID string) (*RegistrationUnitOfWork, error) {
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
	uow := &RegistrationUnitOfWork{
		store: s, ctx: ctx, tx: tx, commandID: commandID, op: RegistrationOpStandalone,
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginCreateTournament starts an exclusive-barrier + command-lock UoW for durable CreateTournament.
// Lock order: rewrite barrier → global command lock → rows.
func (s *TournamentStore) BeginCreateTournament(ctx context.Context, tournamentID, commandID string) (*RegistrationUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" {
		return nil, fmt.Errorf("tournamentId required")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &RegistrationUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, commandID: commandID, op: RegistrationOpCreate,
		createCtx: domain.CreateTournamentContext{TournamentID: domain.TournamentID(tournamentID)},
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
	err = tx.QueryRow(ctx, `SELECT phase FROM tournaments WHERE tournament_id = $1`, tournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.createCtx.Exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	uow.createCtx.Exists = true
	return uow, nil
}

// BeginRegisterPlayer starts shared-barrier + command + per-player lock UoW for RegisterPlayer.
// Lock order: rewrite barrier shared → global command lock → per-player lock → rows.
func (s *TournamentStore) BeginRegisterPlayer(ctx context.Context, tournamentID, playerID, commandID string) (*RegistrationUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" || playerID == "" {
		return nil, fmt.Errorf("tournamentId and playerId required")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &RegistrationUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, playerID: playerID, commandID: commandID, op: RegistrationOpRegister,
		regCtx: domain.RegistrationContext{
			TournamentID: domain.TournamentID(tournamentID),
		},
	}
	if err := acquireRewriteBarrierShared(ctx, tx, tournamentID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	if err := acquireRegistrationPlayerLock(ctx, tx, tournamentID, playerID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}

	var phase string
	var capacity int
	err = tx.QueryRow(ctx, `
		SELECT phase, capacity FROM tournaments WHERE tournament_id = $1
	`, tournamentID).Scan(&phase, &capacity)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.regCtx.Exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	uow.regCtx.Exists = true
	uow.regCtx.Phase = domain.TournamentPhase(phase)
	uow.regCtx.Capacity = capacity

	var one int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM tournament_registrations
		WHERE tournament_id = $1 AND player_id = $2 AND status = 'registered'
	`, tournamentID, playerID).Scan(&one)
	if err == nil {
		uow.regCtx.PlayerRegistered = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginCloseRegistration starts exclusive-barrier + command-lock UoW (waits in-flight shared registrations).
// Lock order: rewrite barrier exclusive → global command lock → rows. Never takes registration-close election.
func (s *TournamentStore) BeginCloseRegistration(ctx context.Context, tournamentID, commandID string) (*RegistrationUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if tournamentID == "" {
		return nil, fmt.Errorf("tournamentId required")
	}
	if commandID == "" {
		return nil, fmt.Errorf("commandId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &RegistrationUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, commandID: commandID, op: RegistrationOpClose,
		closeCtx: domain.CloseRegistrationContext{TournamentID: domain.TournamentID(tournamentID)},
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
	var capacity int
	err = tx.QueryRow(ctx, `
		SELECT phase, capacity FROM tournaments WHERE tournament_id = $1
	`, tournamentID).Scan(&phase, &capacity)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		uow.closeCtx.Exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	uow.closeCtx.Exists = true
	uow.closeCtx.Phase = domain.TournamentPhase(phase)
	uow.closeCtx.Capacity = capacity

	sum, err := sumRegistrationShardCountsTx(ctx, tx, tournamentID)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.closeCtx.RegisteredCount = sum
	return uow, nil
}

func sumRegistrationShardCountsTx(ctx context.Context, tx pgx.Tx, tournamentID string) (int, error) {
	var sum int
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(count), 0)::int
		FROM tournament_registration_shards
		WHERE tournament_id = $1
	`, tournamentID).Scan(&sum)
	return sum, err
}

// errRegistrationCapacityExceeded signals all 64 probes failed (no row/reservation leak).
var errRegistrationCapacityExceeded = errors.New("registration_capacity_exceeded")

// errRegistrationAlreadyPresent signals another command registered this player first.
var errRegistrationAlreadyPresent = errors.New("registration_already_present")

// IsRegistrationCapacityExceeded reports whether err is the capacity probe failure.
func IsRegistrationCapacityExceeded(err error) bool {
	return errors.Is(err, errRegistrationCapacityExceeded)
}

// IsRegistrationAlreadyPresent reports whether err is the same-player race.
func IsRegistrationAlreadyPresent(err error) bool {
	return errors.Is(err, errRegistrationAlreadyPresent)
}

// Commit applies create or close mutation + outcome + outbox.
// Register uses ReserveRegistration + FinalizeRegister instead.
// On late prior discovery returns *PriorCommandOutcome after rollback (never nil success).
func (u *RegistrationUnitOfWork) Commit(req RegistrationCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work already finished")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}
	if req.Op != u.op {
		return fmt.Errorf("op mismatch: locked %q got %q", u.op, req.Op)
	}
	if req.Op == RegistrationOpRegister {
		return fmt.Errorf("register must use ReserveRegistration + FinalizeRegister")
	}

	var existingBody []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1 FOR UPDATE
	`, req.CommandID).Scan(&existingBody)
	if err == nil {
		return u.finishWithPrior(existingBody)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapUnavailable(err)
	}

	switch req.Op {
	case RegistrationOpCreate:
		if err := u.applyCreate(req); err != nil {
			return wrapUnavailable(err)
		}
	case RegistrationOpClose:
		if err := u.applyClose(req); err != nil {
			return wrapUnavailable(err)
		}
	case RegistrationOpStandalone:
		// outcome-only
	default:
		return fmt.Errorf("unknown registration op %q", req.Op)
	}

	if req.BumpBaseProjection {
		if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, time.Now().UTC()); err != nil {
			return wrapUnavailable(err)
		}
	}
	if err := insertCommandOutcomeWithTournament(u.ctx, u.tx, req.CommandID, u.tid, req.CommandType, req.Outcome); err != nil {
		return wrapUnavailable(err)
	}
	if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
		return wrapUnavailable(err)
	}
	u.done = true
	if err := u.tx.Commit(u.ctx); err != nil {
		_ = u.tx.Rollback(u.ctx)
		return wrapUnavailable(err)
	}
	return nil
}

func (u *RegistrationUnitOfWork) applyCreate(req RegistrationCommitRequest) error {
	if req.Decision.Kind != domain.RegistrationCreate {
		return nil
	}
	cmd := req.CreateCmd
	retry, batch, normErr := domain.NormalizedCreateDefaults(req.RetryBudget, req.BatchSize)
	if normErr != nil {
		return normErr
	}
	rules, err := json.Marshal(tournamentRules{
		RetryBudget:  retry,
		BatchSize:    batch,
		CurrentRound: 0,
	})
	if err != nil {
		return err
	}
	vis, verr := domain.NormalizeTournamentVisibility(string(cmd.Visibility))
	if verr != nil {
		return verr
	}
	now := time.Now().UTC()
	if _, err = u.tx.Exec(u.ctx, `
		INSERT INTO tournaments (
			tournament_id, phase, capacity, registered_count, visibility, rules, created_at, updated_at
		) VALUES ($1, $2, $3, 0, $4, $5, $6, $6)
	`, u.tid, string(domain.PhaseRegistration), cmd.Capacity, string(vis), rules, now); err != nil {
		return err
	}
	if _, err := u.tx.Exec(u.ctx, `
		INSERT INTO bracket_projection_versions (tournament_id, projection_version, generated_at)
		VALUES ($1, 0, $2)
	`, u.tid, now); err != nil {
		return err
	}
	return insertRegistrationQuotaShardsTx(u.ctx, u.tx, u.tid, cmd.Capacity)
}

func insertRegistrationQuotaShardsTx(ctx context.Context, tx pgx.Tx, tid string, capacity int) error {
	quotas := domain.AllocateRegistrationQuotas(capacity)
	for shardID, quota := range quotas {
		if _, err := tx.Exec(ctx, `
			INSERT INTO tournament_registration_shards (tournament_id, shard_id, quota, count)
			VALUES ($1, $2, $3, 0)
		`, tid, shardID, quota); err != nil {
			return fmt.Errorf("insert registration shard %d: %w", shardID, err)
		}
	}
	return nil
}

func (u *RegistrationUnitOfWork) applyClose(req RegistrationCommitRequest) error {
	if req.Decision.Kind != domain.RegistrationClose {
		return nil
	}
	now := time.Now().UTC()
	count := u.closeCtx.RegisteredCount
	_, err := u.tx.Exec(u.ctx, `
		UPDATE tournaments
		SET phase = $2, registered_count = $3, updated_at = $4
		WHERE tournament_id = $1 AND phase = $5
	`, u.tid, string(domain.PhaseSeeding), count, now, string(domain.PhaseRegistration))
	return err
}

// ReserveRegistration attempts shard reservation without finalizing outcome.
// On success the UoW remains open so the service can build the final envelope
// (including auto-close facts) then call FinalizeRegister.
// On capacity failure, no reservation/row is left and the UoW remains open for reject finalize.
func (u *RegistrationUnitOfWork) ReserveRegistration() (allocatedShard int, autoClosed bool, err error) {
	if u == nil || u.done {
		return 0, false, fmt.Errorf("unit of work already finished")
	}
	if u.op != RegistrationOpRegister {
		return 0, false, fmt.Errorf("ReserveRegistration requires register op, got %q", u.op)
	}
	if u.commandID == "" {
		return 0, false, fmt.Errorf("commandId required for reserve")
	}
	var existingShard int
	err = u.tx.QueryRow(u.ctx, `
		SELECT shard_id FROM tournament_registrations
		WHERE tournament_id = $1 AND player_id = $2 AND status = 'registered'
	`, u.tid, u.playerID).Scan(&existingShard)
	if err == nil {
		return existingShard, false, errRegistrationAlreadyPresent
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, wrapUnavailable(err)
	}

	allocated := -1
	var newCount, shardQuota int
	for _, shardID := range domain.RegistrationProbeOrder(u.tid, u.playerID) {
		err := u.tx.QueryRow(u.ctx, `
			UPDATE tournament_registration_shards
			SET count = count + 1
			WHERE tournament_id = $1 AND shard_id = $2 AND count < quota
			RETURNING count, quota
		`, u.tid, shardID).Scan(&newCount, &shardQuota)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, false, wrapUnavailable(err)
		}
		allocated = shardID
		break
	}
	if allocated < 0 {
		return 0, false, errRegistrationCapacityExceeded
	}

	now := time.Now().UTC()
	if _, err := u.tx.Exec(u.ctx, `
		INSERT INTO tournament_registrations (tournament_id, player_id, shard_id, registered_at, status)
		VALUES ($1, $2, $3, $4, 'registered')
	`, u.tid, u.playerID, allocated, now); err != nil {
		return 0, false, wrapUnavailable(err)
	}
	if err := bumpProjectionShardTx(u.ctx, u.tx, u.tid, allocated, now); err != nil {
		return 0, false, wrapUnavailable(err)
	}
	u.reserved = true

	if newCount == shardQuota {
		// Election lock after local shard fill: at most once per shard reaching full.
		if err := acquireRegistrationCloseElectionLock(u.ctx, u.tx, u.tid); err != nil {
			return 0, false, wrapUnavailable(err)
		}
		// Fresh statement snapshot after waiting — later waiter sees earlier committed fills.
		var hasRoom bool
		if err := u.tx.QueryRow(u.ctx, `
			SELECT EXISTS(
				SELECT 1 FROM tournament_registration_shards
				WHERE tournament_id = $1 AND count < quota
			)
		`, u.tid).Scan(&hasRoom); err != nil {
			return 0, false, wrapUnavailable(err)
		}
		if !hasRoom {
			cap := u.regCtx.Capacity
			tag, err := u.tx.Exec(u.ctx, `
				UPDATE tournaments
				SET phase = $2, registered_count = $3, updated_at = $4
				WHERE tournament_id = $1 AND phase = $5
			`, u.tid, string(domain.PhaseSeeding), cap, now, string(domain.PhaseRegistration))
			if err != nil {
				return 0, false, wrapUnavailable(err)
			}
			if tag.RowsAffected() == 1 {
				autoClosed = true
				if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, now); err != nil {
					return 0, false, wrapUnavailable(err)
				}
			}
		}
	}
	return allocated, autoClosed, nil
}

// FinalizeRegister stores outcome/outbox and commits (after ReserveRegistration or reject path).
// If an unexpected prior outcome is found, rolls back any pre-applied reservation and returns
// *PriorCommandOutcome — never commits local loser mutations.
func (u *RegistrationUnitOfWork) FinalizeRegister(req RegistrationCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work already finished")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
	}
	if u.op != RegistrationOpRegister || req.Op != RegistrationOpRegister {
		return fmt.Errorf("FinalizeRegister requires register op: locked %q got %q", u.op, req.Op)
	}
	var existingBody []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1 FOR UPDATE
	`, req.CommandID).Scan(&existingBody)
	if err == nil {
		// Defense-in-depth: never commit a pre-applied reservation against a prior outcome.
		return u.finishWithPrior(existingBody)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapUnavailable(err)
	}
	if err := insertCommandOutcomeWithTournament(u.ctx, u.tx, req.CommandID, u.tid, req.CommandType, req.Outcome); err != nil {
		return wrapUnavailable(err)
	}
	if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
		return wrapUnavailable(err)
	}
	u.done = true
	if err := u.tx.Commit(u.ctx); err != nil {
		_ = u.tx.Rollback(u.ctx)
		return wrapUnavailable(err)
	}
	return nil
}

// ensureRegistrationShardsTx initializes quota rows if missing (legacy create via persist).
func ensureRegistrationShardsTx(ctx context.Context, tx pgx.Tx, tid string, capacity int) error {
	var n int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int FROM tournament_registration_shards WHERE tournament_id = $1
	`, tid).Scan(&n); err != nil {
		return err
	}
	if n == domain.RegistrationShardCount {
		return nil
	}
	if n != 0 {
		return fmt.Errorf("registration shards invariant: want 0 or 64 rows, got %d", n)
	}
	return insertRegistrationQuotaShardsTx(ctx, tx, tid, capacity)
}

// loadRegistrationShardAllocationsTx captures player→shard before legacy delete/reinsert.
func loadRegistrationShardAllocationsTx(ctx context.Context, tx pgx.Tx, tid string) (map[string]int, error) {
	rows, err := tx.Query(ctx, `
		SELECT player_id, shard_id FROM tournament_registrations WHERE tournament_id = $1
	`, tid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var pid string
		var shard int
		if err := rows.Scan(&pid, &shard); err != nil {
			return nil, err
		}
		out[pid] = shard
	}
	return out, rows.Err()
}

// rebuildRegistrationShardCountsTx resets counts from registration rows; quotas stay immutable.
func rebuildRegistrationShardCountsTx(ctx context.Context, tx pgx.Tx, tid string, capacity int) error {
	if err := ensureRegistrationShardsTx(ctx, tx, tid, capacity); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tournament_registration_shards SET count = 0 WHERE tournament_id = $1
	`, tid); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT shard_id, count(*)::int
		FROM tournament_registrations
		WHERE tournament_id = $1 AND status = 'registered'
		GROUP BY shard_id
	`, tid)
	if err != nil {
		return err
	}
	type shardCount struct{ shard, cnt int }
	var counts []shardCount
	for rows.Next() {
		var sc shardCount
		if err := rows.Scan(&sc.shard, &sc.cnt); err != nil {
			rows.Close()
			return err
		}
		counts = append(counts, sc)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	total := 0
	for _, sc := range counts {
		total += sc.cnt
		if _, err := tx.Exec(ctx, `
			UPDATE tournament_registration_shards
			SET count = $3
			WHERE tournament_id = $1 AND shard_id = $2
		`, tid, sc.shard, sc.cnt); err != nil {
			return err
		}
	}
	var sum int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(count), 0)::int FROM tournament_registration_shards WHERE tournament_id = $1
	`, tid).Scan(&sum); err != nil {
		return err
	}
	if sum != total {
		return fmt.Errorf("registration shard count mismatch: sum=%d rows=%d", sum, total)
	}
	if total > capacity {
		return fmt.Errorf("registration overcapacity: %d > %d", total, capacity)
	}
	var rowCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int FROM tournament_registrations
		WHERE tournament_id = $1 AND status = 'registered'
	`, tid).Scan(&rowCount); err != nil {
		return err
	}
	if rowCount != sum {
		return fmt.Errorf("registration invariant: rows=%d sum(counts)=%d", rowCount, sum)
	}
	return nil
}
