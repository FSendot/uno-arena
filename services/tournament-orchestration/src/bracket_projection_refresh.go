package main

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

// BracketProjectionScope describes which Redis keys to refresh after a Postgres bump.
// Refresh is always bounded: summary and/or named chunks — never whole-aggregate hydrate.
type BracketProjectionScope struct {
	TournamentID string
	// Summary refreshes compact BracketSummary.
	Summary bool
	// RoundNumber > 0 refreshes summary + all batches of that one round only.
	RoundNumber int
	// Chunks lists specific round/batch pairs to upsert.
	Chunks []store.BracketChunkRef
}

// BracketProjectionRefresher upserts Redis after authoritative Postgres commits.
// Redis failures are logged; callers must not roll back or misreport the command.
type BracketProjectionRefresher interface {
	Refresh(ctx context.Context, scope BracketProjectionScope) error
	Rebuild(ctx context.Context, tournamentID string) error
}

// postgresBracketSource loads rebuild/refresh inputs from TournamentStore.
type postgresBracketSource struct {
	store *store.TournamentStore
}

func (p postgresBracketSource) LoadBracketSummary(ctx context.Context, tournamentID string) (store.BracketSummary, bool, error) {
	return p.store.LoadBracketSummary(ctx, tournamentID)
}

func (p postgresBracketSource) LoadProjectionCheckpoint(ctx context.Context, tournamentID string) (int64, time.Time, error) {
	return p.store.LoadProjectionCheckpoint(ctx, tournamentID)
}

func (p postgresBracketSource) ListVisibleBracketChunks(ctx context.Context, tournamentID string) ([]store.BracketChunkRef, error) {
	return p.store.ListVisibleBracketChunks(ctx, tournamentID)
}

func (p postgresBracketSource) LoadBatchChunkForProjection(ctx context.Context, tournamentID string, roundNumber int, batchID string) ([]store.BracketSlotView, error) {
	return p.store.LoadBatchChunkForProjection(ctx, tournamentID, roundNumber, batchID)
}

// redisBracketRefresher implements BracketProjectionRefresher.
type redisBracketRefresher struct {
	redis *store.RedisBracketStore
	pg    *store.TournamentStore
}

const bracketProjectionRefreshTimeout = 2 * time.Second

func newRedisBracketRefresher(rdb *store.RedisBracketStore, pg *store.TournamentStore) *redisBracketRefresher {
	return &redisBracketRefresher{redis: rdb, pg: pg}
}

func (r *redisBracketRefresher) Rebuild(ctx context.Context, tournamentID string) error {
	if r == nil || r.redis == nil || r.pg == nil {
		return store.ErrBracketProjectionUnavailable
	}
	ver, _, err := r.pg.LoadProjectionCheckpoint(ctx, tournamentID)
	if err != nil {
		return err
	}
	return r.redis.RebuildFromPostgres(ctx, tournamentID, postgresBracketSource{store: r.pg}, ver)
}

func (r *redisBracketRefresher) Refresh(ctx context.Context, scope BracketProjectionScope) error {
	if r == nil || r.redis == nil || r.pg == nil {
		return nil
	}
	tid := strings.TrimSpace(scope.TournamentID)
	if tid == "" {
		return nil
	}
	ver, generatedAt, err := r.pg.LoadProjectionCheckpoint(ctx, tid)
	if err != nil {
		return err
	}
	if ver <= 0 {
		// No public fence yet — nothing to project.
		return nil
	}
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	// Chunks before summary so markReadyIfConsistent never flips ready while the
	// index is still empty during first populate (would serve incomplete pages).
	chunks := scope.Chunks
	if scope.RoundNumber > 0 {
		listed, err := r.pg.ListRoundBracketChunks(ctx, tid, scope.RoundNumber)
		if err != nil {
			return err
		}
		chunks = append(chunks, listed...)
	}
	seen := make(map[string]struct{}, len(chunks))
	for _, ch := range chunks {
		key := strings.TrimSpace(ch.BatchID) + ":" + strconv.Itoa(ch.RoundNumber)
		if ch.BatchID == "" || ch.RoundNumber < 1 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		slots, err := r.pg.LoadBatchChunkForProjection(ctx, tid, ch.RoundNumber, ch.BatchID)
		if err != nil {
			return err
		}
		if err := r.redis.UpsertChunk(ctx, tid, ch.RoundNumber, ch.BatchID, slots, ver, generatedAt); err != nil {
			return err
		}
	}

	needSummary := scope.Summary || scope.RoundNumber > 0 || len(scope.Chunks) > 0
	if needSummary {
		summary, ok, err := r.pg.LoadBracketSummary(ctx, tid)
		if err != nil {
			return err
		}
		if !ok {
			return store.ErrTournamentNotFound
		}
		if err := r.redis.UpsertSummary(ctx, tid, summary, ver, generatedAt); err != nil {
			return err
		}
	}
	return nil
}

// refreshBracketBestEffort logs Redis projection failures without failing the command.
func (s *Service) refreshBracketBestEffort(ctx context.Context, scope BracketProjectionScope) {
	if s == nil || s.bracketRefresh == nil {
		return
	}
	if strings.TrimSpace(scope.TournamentID) == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bracketProjectionRefreshTimeout)
	defer cancel()
	if err := s.bracketRefresh.Refresh(refreshCtx, scope); err != nil {
		slog.WarnContext(refreshCtx, "bracket projection refresh failed", "event", "bracket_projection_refresh_failed",
			"tournamentId", scope.TournamentID, "error", err.Error())
	}
}

func scopeSummaryOnly(tournamentID string) BracketProjectionScope {
	return BracketProjectionScope{TournamentID: tournamentID, Summary: true}
}

func scopeRound(tournamentID string, roundNumber int) BracketProjectionScope {
	return BracketProjectionScope{TournamentID: tournamentID, Summary: true, RoundNumber: roundNumber}
}

func scopeChunks(tournamentID string, chunks ...store.BracketChunkRef) BracketProjectionScope {
	return BracketProjectionScope{TournamentID: tournamentID, Summary: true, Chunks: chunks}
}

// scopeForSlot resolves one slot's provisioning batch and returns a bounded chunk refresh.
// On lookup failure / empty batch / missing inputs, returns an empty no-op scope so we
// never advance Redis summary/meta without the affected chunk.
func (s *Service) scopeForSlot(ctx context.Context, tournamentID string, roundNumber int, slotID string) BracketProjectionScope {
	noop := BracketProjectionScope{}
	if s == nil || roundNumber < 1 || strings.TrimSpace(slotID) == "" || strings.TrimSpace(tournamentID) == "" {
		return noop
	}
	pg, ok := s.bracketRefreshPG()
	if !ok {
		return noop
	}
	batchID, err := pg.LookupBatchIDForSlot(ctx, tournamentID, roundNumber, slotID)
	if err != nil || batchID == "" {
		return noop
	}
	return scopeChunks(tournamentID, store.BracketChunkRef{RoundNumber: roundNumber, BatchID: batchID})
}

func (s *Service) bracketRefreshPG() (*store.TournamentStore, bool) {
	if s == nil || s.bracketRefresh == nil {
		return nil, false
	}
	r, ok := s.bracketRefresh.(*redisBracketRefresher)
	if !ok || r == nil || r.pg == nil {
		return nil, false
	}
	return r.pg, true
}
