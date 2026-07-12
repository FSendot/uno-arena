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

// TournamentUnitOfWork holds FOR UPDATE (or create advisory lock) across hydrate + mutate + commit.
type TournamentUnitOfWork struct {
	store      *TournamentStore
	ctx        context.Context
	tx         pgx.Tx
	tid        string
	loaded     *domain.Tournament
	exists     bool
	createPath bool
	done       bool
}

// BeginExisting takes the exclusive rewrite barrier, locks tournaments FOR UPDATE,
// then hydrates under the same transaction.
//
// Lock order: exclusive rewrite barrier → tournaments FOR UPDATE → hydrate (reads).
func (s *TournamentStore) BeginExisting(ctx context.Context, id domain.TournamentID) (*TournamentUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if !id.Valid() {
		return nil, fmt.Errorf("tournamentId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &TournamentUnitOfWork{store: s, ctx: ctx, tx: tx, tid: string(id)}
	if err := acquireRewriteBarrierExclusive(ctx, tx, string(id)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	var locked string
	err = tx.QueryRow(ctx, `
		SELECT tournament_id FROM tournaments WHERE tournament_id = $1 FOR UPDATE
	`, string(id)).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	t, err := s.loadTournamentQ(ctx, tx, string(id))
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.loaded = t
	return uow, nil
}

// BeginCreate takes the exclusive rewrite barrier (same namespace as differential/legacy)
// before checking/inserting so create cannot race a differential MatchCompleted commit.
//
// Lock order: exclusive rewrite barrier → tournaments FOR UPDATE (or absent) → hydrate.
func (s *TournamentStore) BeginCreate(ctx context.Context, id domain.TournamentID) (*TournamentUnitOfWork, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("nil store")
	}
	if !id.Valid() {
		return nil, fmt.Errorf("tournamentId required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	uow := &TournamentUnitOfWork{store: s, ctx: ctx, tx: tx, tid: string(id), createPath: true}
	if err := acquireRewriteBarrierExclusive(ctx, tx, string(id)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	var locked string
	err = tx.QueryRow(ctx, `
		SELECT tournament_id FROM tournaments WHERE tournament_id = $1 FOR UPDATE
	`, string(id)).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		uow.exists = false
		return uow, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.exists = true
	t, err := s.loadTournamentQ(ctx, tx, string(id))
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, wrapUnavailable(err)
	}
	uow.loaded = t
	return uow, nil
}

// Loaded returns the locked hydrate snapshot (nil when !Exists).
func (u *TournamentUnitOfWork) Loaded() *domain.Tournament { return u.loaded }

// Exists reports whether the tournament row was present when the UoW began.
func (u *TournamentUnitOfWork) Exists() bool { return u.exists }

// LookupOutcome reads command_idempotency under the held transaction.
func (u *TournamentUnitOfWork) LookupOutcome(commandID string) (envelope.Result, bool) {
	if u == nil || u.done || commandID == "" {
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

// Commit persists aggregate + outcome + outbox on the held transaction then commits.
func (u *TournamentUnitOfWork) Commit(req CommitRequest) error {
	if u == nil || u.done {
		return fmt.Errorf("unit of work already finished")
	}
	if req.CommandID == "" {
		return fmt.Errorf("commandId required for commit")
	}

	var existingBody []byte
	err := u.tx.QueryRow(u.ctx, `
		SELECT outcome_body FROM command_idempotency WHERE command_id = $1 FOR UPDATE
	`, req.CommandID).Scan(&existingBody)
	if err == nil {
		u.done = true
		if err := u.tx.Commit(u.ctx); err != nil {
			_ = u.tx.Rollback(u.ctx)
			return wrapUnavailable(err)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapUnavailable(err)
	}

	if req.Tournament != nil {
		if err := persistTournamentTx(u.ctx, u.tx, req.Tournament, req.MatchResultSource); err != nil {
			return wrapUnavailable(err)
		}
		if req.ProjectionChanged {
			if err := bumpProjectionVersionTx(u.ctx, u.tx, string(req.Tournament.ID()), time.Now().UTC()); err != nil {
				return wrapUnavailable(err)
			}
		}
	}
	if err := insertCommandOutcome(u.ctx, u.tx, req); err != nil {
		return wrapUnavailable(err)
	}
	if err := insertOutboxEvents(u.ctx, u.tx, req.Events); err != nil {
		return wrapUnavailable(err)
	}

	if u.store.FailNextCommits > 0 {
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

// Rollback aborts the held transaction.
func (u *TournamentUnitOfWork) Rollback() error {
	if u == nil || u.done {
		return nil
	}
	u.done = true
	return u.tx.Rollback(u.ctx)
}
