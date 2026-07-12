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

// Differential CompleteTournament / CancelTournament lock order (all paths must match):
//  1. Exclusive rewrite barrier (tournament:rewrite:{id}) — fences in-flight shared
//     differential/worker work and blocks new shared acquires while remaining O(1)
//  2. Global command lock (AcquireCommandLock)
//  3. tournaments row FOR UPDATE
//  4. Complete only: current final tournament_rounds FOR UPDATE, then at most two
//     current-round bracket_slots (LIMIT 2) and the single joined advancement_records
//     + disposition=recorded match_results row for the unique slot_index=0 final room.
//
// Never: whole-tournament hydrate, subtree DELETE, Service.mu, BeginExisting,
// scanning all rounds/slots/registrations/batches/players, or Cancel updating
// bracket/player rows synchronously.
// Never: shared rewrite barrier on Complete/Cancel (that fails to fence RoundMatch).

const (
	completeTournamentCommandType = "CompleteTournament"
	cancelTournamentCommandType   = "CancelTournament"
)

// LifecycleOp identifies which differential lifecycle command a UoW serves.
type LifecycleOp string

const (
	LifecycleOpComplete   LifecycleOp = "complete"
	LifecycleOpCancel     LifecycleOp = "cancel"
	LifecycleOpStandalone LifecycleOp = "standalone"
)

// LifecycleCommitRequest is one atomic differential Complete/Cancel persistence unit.
type LifecycleCommitRequest struct {
	TournamentID  string
	CommandID     string
	CommandType   string
	CorrelationID string
	Outcome       envelope.Result
	Events        []OutboxEvent
	Complete      domain.CompleteTournamentDecision
	Cancel        domain.CancelTournamentDecision
}

// LifecycleUnitOfWork holds one READ COMMITTED tx for bounded Complete/Cancel.
type LifecycleUnitOfWork struct {
	store       *TournamentStore
	ctx         context.Context
	tx          pgx.Tx
	tid         string
	commandID   string
	op          LifecycleOp
	exists      bool
	completeCtx domain.CompleteTournamentContext
	cancelCtx   domain.CancelTournamentContext
	done        bool
}

func (u *LifecycleUnitOfWork) Exists() bool { return u != nil && u.exists }

func (u *LifecycleUnitOfWork) CompleteContext() domain.CompleteTournamentContext {
	if u == nil {
		return domain.CompleteTournamentContext{}
	}
	return u.completeCtx
}

func (u *LifecycleUnitOfWork) CancelContext() domain.CancelTournamentContext {
	if u == nil {
		return domain.CancelTournamentContext{}
	}
	return u.cancelCtx
}

func (u *LifecycleUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
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

func (u *LifecycleUnitOfWork) finishWithPrior(body []byte) error {
	var out envelope.Result
	_ = json.Unmarshal(body, &out)
	_ = u.tx.Rollback(u.ctx)
	u.done = true
	return &PriorCommandOutcome{Outcome: out}
}

func (u *LifecycleUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}

// BeginStandaloneLifecycleCommand locks only the global command id for invalid/outcome-only rejects.
func (s *TournamentStore) BeginStandaloneLifecycleCommand(ctx context.Context, commandID string) (*LifecycleUnitOfWork, error) {
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
	uow := &LifecycleUnitOfWork{store: s, ctx: ctx, tx: tx, commandID: commandID, op: LifecycleOpStandalone}
	if err := AcquireCommandLock(ctx, tx, commandID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	return uow, nil
}

// BeginCompleteTournament starts a differential UoW for CompleteTournament.
// Lock order: exclusive barrier → command → tournament FOR UPDATE → final round → final slot/result.
func (s *TournamentStore) BeginCompleteTournament(ctx context.Context, tournamentID, commandID string) (*LifecycleUnitOfWork, error) {
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
	uow := &LifecycleUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, commandID: commandID, op: LifecycleOpComplete,
		completeCtx: domain.CompleteTournamentContext{TournamentID: domain.TournamentID(tournamentID)},
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
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	var rules tournamentRules
	jsonUnmarshalRules(rulesRaw, &rules)
	uow.completeCtx.Exists = true
	uow.completeCtx.Phase = domain.TournamentPhase(phase)
	uow.completeCtx.CurrentRound = rules.CurrentRound

	rn := rules.CurrentRound
	if rn < 1 {
		uow.completeCtx.RoundFound = false
		return uow, nil
	}

	var roundStatus string
	var isFinal bool
	err = tx.QueryRow(ctx, `
		SELECT status, is_final FROM tournament_rounds
		WHERE tournament_id = $1 AND round_number = $2
		FOR UPDATE
	`, tournamentID, rn).Scan(&roundStatus, &isFinal)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.completeCtx.RoundFound = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.completeCtx.RoundFound = true
	uow.completeCtx.IsFinal = isFinal
	uow.completeCtx.RoundCompleted = roundStatus == string(domain.RoundCompleted)

	// Final room invariant: load/lock at most two slots (LIMIT 2). Success requires
	// exactly one slot with slot_index == 0; missing/multiple/nonzero leave standings empty.
	rows, err := tx.Query(ctx, `
		SELECT slot_id, slot_index FROM bracket_slots
		WHERE tournament_id = $1 AND round_number = $2
		ORDER BY slot_index
		LIMIT 2
		FOR UPDATE
	`, tournamentID, rn)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	type slotRow struct {
		id    string
		index int
	}
	var slots []slotRow
	for rows.Next() {
		var s slotRow
		if err := rows.Scan(&s.id, &s.index); err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return nil, wrapUnavailable(err)
		}
		slots = append(slots, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	rows.Close()

	uow.completeCtx.FinalSlotCount = len(slots)
	if len(slots) != 1 || slots[0].index != 0 {
		if len(slots) == 1 {
			uow.completeCtx.FinalSlotIndex = slots[0].index
		}
		return uow, nil
	}
	uow.completeCtx.FinalSlotIndex = 0
	slotID := slots[0].id

	// Authoritative standings: advancement must join the exact recorded match_results row.
	var advancing []string
	err = tx.QueryRow(ctx, `
		SELECT ar.advancing_player_ids
		FROM advancement_records ar
		INNER JOIN match_results mr
			ON mr.tournament_id = ar.tournament_id
			AND mr.round_number = ar.round_number
			AND mr.slot_id = ar.slot_id
			AND mr.room_id = ar.source_room_id
			AND mr.completion_version = ar.source_completion_version
		WHERE ar.tournament_id = $1
			AND ar.round_number = $2
			AND ar.slot_id = $3
			AND mr.disposition = 'recorded'
		FOR UPDATE OF ar, mr
	`, tournamentID, rn, slotID).Scan(&advancing)
	if errors.Is(err, pgx.ErrNoRows) {
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	standings := make([]domain.PlayerID, len(advancing))
	for i, p := range advancing {
		standings[i] = domain.PlayerID(p)
	}
	uow.completeCtx.FinalStandings = standings
	return uow, nil
}

// BeginCancelTournament starts a differential UoW for CancelTournament.
// Lock order: exclusive barrier → command → tournament FOR UPDATE. O(1) lifecycle only.
func (s *TournamentStore) BeginCancelTournament(ctx context.Context, tournamentID, commandID string) (*LifecycleUnitOfWork, error) {
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
	uow := &LifecycleUnitOfWork{
		store: s, ctx: ctx, tx: tx, tid: tournamentID, commandID: commandID, op: LifecycleOpCancel,
		cancelCtx: domain.CancelTournamentContext{TournamentID: domain.TournamentID(tournamentID)},
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
	err = tx.QueryRow(ctx, `
		SELECT phase FROM tournaments WHERE tournament_id = $1 FOR UPDATE
	`, tournamentID).Scan(&phase)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	uow.cancelCtx.Exists = true
	uow.cancelCtx.Phase = domain.TournamentPhase(phase)
	return uow, nil
}

// Commit applies the bounded Complete/Cancel mutation + outcome + outbox.
func (u *LifecycleUnitOfWork) Commit(req LifecycleCommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("lifecycle uow done")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}
	if req.CommandID != u.commandID {
		return fmt.Errorf("commandId mismatch: locked %q got %q", u.commandID, req.CommandID)
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

	tid := u.tid
	if tid == "" {
		tid = req.TournamentID
	}
	cmdType := req.CommandType
	now := time.Now().UTC()

	switch u.op {
	case LifecycleOpStandalone:
		if cmdType == "" {
			return fmt.Errorf("command type required for standalone lifecycle commit")
		}
		if len(req.Events) > 0 {
			return fmt.Errorf("standalone lifecycle reject must not carry outbox events")
		}
		// Outcome-only: no tournament mutation / projection bump.
	case LifecycleOpComplete:
		if cmdType == "" {
			cmdType = completeTournamentCommandType
		}
		if cmdType != completeTournamentCommandType {
			return fmt.Errorf("command type mismatch: want %q got %q", completeTournamentCommandType, cmdType)
		}
		if err := u.applyComplete(req, now); err != nil {
			return err
		}
	case LifecycleOpCancel:
		if cmdType == "" {
			cmdType = cancelTournamentCommandType
		}
		if cmdType != cancelTournamentCommandType {
			return fmt.Errorf("command type mismatch: want %q got %q", cancelTournamentCommandType, cmdType)
		}
		if err := u.applyCancel(req, now); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown lifecycle op %q", u.op)
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

func (u *LifecycleUnitOfWork) applyComplete(req LifecycleCommitRequest, now time.Time) error {
	switch req.Complete.Kind {
	case domain.CompleteTournamentSuccess:
		if u.tid == "" {
			return fmt.Errorf("success decision requires tournament-bound complete uow")
		}
		champ := string(req.Complete.ChampionID)
		if champ == "" && len(req.Complete.FinalStandings) > 0 {
			champ = string(req.Complete.FinalStandings[0])
		}
		tag, err := u.tx.Exec(u.ctx, `
			UPDATE tournaments
			SET phase = $2,
			    completed_at = $3,
			    updated_at = $3,
			    rules = jsonb_set(COALESCE(rules, '{}'::jsonb), '{championId}', to_jsonb($4::text), true)
			WHERE tournament_id = $1
			  AND phase NOT IN ('completed', 'cancelled')
		`, u.tid, string(domain.PhaseCompleted), now, champ)
		if err != nil {
			return wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("tournament phase not mutable at complete commit")
		}
		if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, now); err != nil {
			return wrapUnavailable(err)
		}
		if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
			return wrapUnavailable(err)
		}
	case domain.CompleteTournamentAlreadyDone:
		if len(req.Events) > 0 {
			return fmt.Errorf("already-done complete tournament must not carry outbox events")
		}
	case domain.CompleteTournamentReject:
		if len(req.Events) > 0 {
			return fmt.Errorf("reject complete tournament must not carry outbox events")
		}
	default:
		return fmt.Errorf("unknown complete tournament decision kind %q", req.Complete.Kind)
	}
	return nil
}

func (u *LifecycleUnitOfWork) applyCancel(req LifecycleCommitRequest, now time.Time) error {
	switch req.Cancel.Kind {
	case domain.CancelTournamentSuccess:
		if u.tid == "" {
			return fmt.Errorf("success decision requires tournament-bound cancel uow")
		}
		tag, err := u.tx.Exec(u.ctx, `
			UPDATE tournaments
			SET phase = $2, updated_at = $3
			WHERE tournament_id = $1
			  AND phase NOT IN ('completed', 'cancelled')
		`, u.tid, string(domain.PhaseCancelled), now)
		if err != nil {
			return wrapUnavailable(err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("tournament phase not mutable at cancel commit")
		}
		if err := bumpProjectionVersionTx(u.ctx, u.tx, u.tid, now); err != nil {
			return wrapUnavailable(err)
		}
		if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
			return wrapUnavailable(err)
		}
	case domain.CancelTournamentAlreadyDone:
		if len(req.Events) > 0 {
			return fmt.Errorf("already-done cancel tournament must not carry outbox events")
		}
	case domain.CancelTournamentReject:
		if len(req.Events) > 0 {
			return fmt.Errorf("reject cancel tournament must not carry outbox events")
		}
	default:
		return fmt.Errorf("unknown cancel tournament decision kind %q", req.Cancel.Kind)
	}
	return nil
}

// LookupLifecycleOutcome reads command_idempotency outside a UoW.
func (s *TournamentStore) LookupLifecycleOutcome(ctx context.Context, commandID string) (envelope.Result, bool) {
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
