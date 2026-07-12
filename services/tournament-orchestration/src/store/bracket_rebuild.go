package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BracketChunkRef identifies one provisioning-batch chunk for rebuild/refresh.
type BracketChunkRef struct {
	RoundNumber int
	BatchID     string
}

// BracketRebuildSource supplies bounded authoritative summary/chunks for rebuild.
type BracketRebuildSource interface {
	LoadBracketSummary(ctx context.Context, tournamentID string) (BracketSummary, bool, error)
	LoadProjectionCheckpoint(ctx context.Context, tournamentID string) (version int64, generatedAt time.Time, err error)
	ListVisibleBracketChunks(ctx context.Context, tournamentID string) ([]BracketChunkRef, error)
	LoadBatchChunkForProjection(ctx context.Context, tournamentID string, roundNumber int, batchID string) ([]BracketSlotView, error)
}

const DefaultBracketRebuildChunkBatch = 32

var (
	bracketRebuildTokenMu       sync.Mutex
	bracketRebuildTokenOverride string
)

// SetBracketRebuildTokenForTest forces RebuildFromPostgres to use a fixed ownership token.
func SetBracketRebuildTokenForTest(token string) func() {
	bracketRebuildTokenMu.Lock()
	prev := bracketRebuildTokenOverride
	bracketRebuildTokenOverride = token
	bracketRebuildTokenMu.Unlock()
	return func() {
		bracketRebuildTokenMu.Lock()
		bracketRebuildTokenOverride = prev
		bracketRebuildTokenMu.Unlock()
	}
}

func newBracketRebuildToken() (string, error) {
	bracketRebuildTokenMu.Lock()
	override := bracketRebuildTokenOverride
	bracketRebuildTokenMu.Unlock()
	if override != "" {
		return override, nil
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// RebuildFromPostgres rebuilds the Redis bracket projection from authoritative Postgres
// using bounded per-batch chunks + generation cutover (Ranking-style). Concurrent
// higher-version incremental refreshes dual-write staging and are preserved at cutover
// via watermark fences (never lost).
func (s *RedisBracketStore) RebuildFromPostgres(
	ctx context.Context,
	tournamentID string,
	src BracketRebuildSource,
	versionWatermark int64,
) error {
	if strings.TrimSpace(tournamentID) == "" {
		return fmt.Errorf("tournamentId required")
	}
	if src == nil {
		return fmt.Errorf("nil bracket rebuild source")
	}
	_, liveGen, _, err := s.readMeta(ctx, tournamentID)
	if err != nil {
		return err
	}
	newGen := liveGen + 1
	if newGen < 1 {
		newGen = 1
	}
	token, err := newBracketRebuildToken()
	if err != nil {
		return fmt.Errorf("rebuild token: %w", err)
	}
	summaryKey := s.keys.GenSummary(tournamentID, newGen)
	idxKey := s.keys.GenIndex(tournamentID, newGen)
	mapKey := s.keys.GenSlotMap(tournamentID, newGen)
	chunksetKey := s.keys.GenChunkSet(tournamentID, newGen)
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = beginBracketRebuildScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(tournamentID), summaryKey, idxKey, mapKey, chunksetKey},
		strconv.FormatInt(newGen, 10), now, token, s.keys.TournamentRoot(tournamentID),
	).Result()
	if err != nil {
		ok, rerr := s.reconcileBeginRebuild(ctx, tournamentID, newGen, token)
		if rerr != nil {
			return rerr
		}
		if !ok {
			return wrapBracketUnavailable(err)
		}
	}

	summary, ok, err := src.LoadBracketSummary(ctx, tournamentID)
	if err != nil {
		_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
		return err
	}
	if !ok {
		_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
		return ErrTournamentNotFound
	}
	if versionWatermark <= 0 {
		if ver, _, verr := src.LoadProjectionCheckpoint(ctx, tournamentID); verr == nil {
			versionWatermark = ver
		}
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
		return err
	}
	if _, err := rebuildBracketSummaryScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(tournamentID), summaryKey},
		strconv.FormatInt(newGen, 10), token, strconv.FormatInt(versionWatermark, 10), string(summaryJSON),
	).Result(); err != nil {
		_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
		return wrapBracketUnavailable(err)
	}

	chunks, err := src.ListVisibleBracketChunks(ctx, tournamentID)
	if err != nil {
		_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
		return err
	}
	for _, ch := range chunks {
		slots, err := src.LoadBatchChunkForProjection(ctx, tournamentID, ch.RoundNumber, ch.BatchID)
		if err != nil {
			_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
			return err
		}
		if slots == nil {
			slots = []BracketSlotView{}
		}
		raw, err := json.Marshal(slots)
		if err != nil {
			_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
			return err
		}
		args := make([]any, 0, 8+2*len(slots))
		args = append(args,
			s.keys.TournamentRoot(tournamentID),
			strconv.FormatInt(newGen, 10),
			token,
			strconv.FormatInt(versionWatermark, 10),
			strconv.Itoa(ch.RoundNumber),
			ch.BatchID,
			string(raw),
			strconv.Itoa(len(slots)),
		)
		for _, sl := range slots {
			args = append(args, BracketIndexScore(sl.RoundNumber, sl.SlotIndex), BracketIndexMember(sl.RoundNumber, sl.SlotIndex))
		}
		if _, err := rebuildBracketChunkScript.Run(ctx, s.rdb,
			[]string{s.keys.Meta(tournamentID)}, args...,
		).Result(); err != nil {
			_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
			return wrapBracketUnavailable(err)
		}
	}

	oldSummary := s.keys.GenSummary(tournamentID, 0)
	oldIdx := s.keys.GenIndex(tournamentID, 0)
	oldMap := s.keys.GenSlotMap(tournamentID, 0)
	oldChunkset := s.keys.GenChunkSet(tournamentID, 0)
	oldGenArg := "0"
	if liveGen >= 1 {
		oldSummary = s.keys.GenSummary(tournamentID, liveGen)
		oldIdx = s.keys.GenIndex(tournamentID, liveGen)
		oldMap = s.keys.GenSlotMap(tournamentID, liveGen)
		oldChunkset = s.keys.GenChunkSet(tournamentID, liveGen)
		oldGenArg = strconv.FormatInt(liveGen, 10)
	}
	cutoverAt := time.Now().UTC().Format(time.RFC3339)
	_, err = cutoverBracketRebuildScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(tournamentID), oldSummary, oldIdx, oldMap, oldChunkset},
		strconv.FormatInt(newGen, 10),
		cutoverAt,
		strconv.FormatInt(versionWatermark, 10),
		s.keys.TournamentRoot(tournamentID),
		token,
		oldGenArg,
	).Result()
	if err != nil {
		if s.cutoverAlreadyAppliedBestEffort(ctx, tournamentID, newGen) {
			return nil
		}
		_ = s.abortRebuildBestEffort(ctx, tournamentID, newGen, token)
		return wrapBracketUnavailable(err)
	}
	return nil
}

func (s *RedisBracketStore) reconcileBeginRebuild(
	parent context.Context,
	tournamentID string,
	newGen int64,
	token string,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	m, liveGen, rebuilding, err := s.readMeta(ctx, tournamentID)
	if err != nil {
		log.Printf(`{"level":"error","service":"tournament-orchestration","event":"bracket_begin_reconcile_failed","err":%q}`, err.Error())
		return false, nil
	}
	if rebuilding == newGen && m.RebuildToken == token {
		return true, nil
	}
	if rebuilding != 0 {
		return false, fmt.Errorf(
			"%w: rebuild already in progress (rebuilding_gen=%d, wanted=%d, live=%d)",
			ErrBracketProjectionUnavailable, rebuilding, newGen, liveGen,
		)
	}
	return false, nil
}

func (s *RedisBracketStore) abortRebuildBestEffort(parent context.Context, tournamentID string, gen int64, token string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	err := s.abortRebuild(ctx, tournamentID, gen, token)
	if err != nil {
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"bracket_rebuild_abort_failed","tournamentId":%q,"err":%q}`,
			tournamentID, err.Error())
	}
	return err
}

func (s *RedisBracketStore) abortRebuild(ctx context.Context, tournamentID string, gen int64, token string) error {
	_, err := abortBracketRebuildScript.Run(ctx, s.rdb,
		[]string{
			s.keys.Meta(tournamentID),
			s.keys.GenSummary(tournamentID, gen),
			s.keys.GenIndex(tournamentID, gen),
			s.keys.GenSlotMap(tournamentID, gen),
			s.keys.GenChunkSet(tournamentID, gen),
		},
		strconv.FormatInt(gen, 10), token, s.keys.TournamentRoot(tournamentID),
	).Result()
	return wrapBracketUnavailable(err)
}

func (s *RedisBracketStore) cutoverAlreadyAppliedBestEffort(parent context.Context, tournamentID string, newGen int64) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	m, live, rebuilding, err := s.readMeta(ctx, tournamentID)
	if err != nil {
		return false
	}
	_ = m
	return live == newGen && rebuilding == 0
}

// AbortRebuildForTest exposes abort for unit tests.
func (s *RedisBracketStore) AbortRebuildForTest(ctx context.Context, tournamentID string, gen int64, token string) error {
	return s.abortRebuild(ctx, tournamentID, gen, token)
}
