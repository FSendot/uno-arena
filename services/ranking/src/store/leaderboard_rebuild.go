package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"unoarena/services/ranking/domain"
)

// RatingBatchSource supplies bounded authoritative rating pages for rebuild.
type RatingBatchSource interface {
	LeaderboardKeysetPage(ctx context.Context, boardType domain.RatingSourceType, after *LeaderboardCursor, limit int) ([]domain.LeaderboardEntry, error)
}

var (
	rebuildTokenMu       sync.Mutex
	rebuildTokenOverride string
)

// SetRebuildTokenForTest forces RebuildFromPostgres to use a fixed ownership token.
// Returns a restore func.
func SetRebuildTokenForTest(token string) func() {
	rebuildTokenMu.Lock()
	prev := rebuildTokenOverride
	rebuildTokenOverride = token
	rebuildTokenMu.Unlock()
	return func() {
		rebuildTokenMu.Lock()
		rebuildTokenOverride = prev
		rebuildTokenMu.Unlock()
	}
}

func newRebuildToken() (string, error) {
	rebuildTokenMu.Lock()
	override := rebuildTokenOverride
	rebuildTokenMu.Unlock()
	if override != "" {
		return override, nil
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// RebuildFromPostgres rebuilds the Redis projection for one board from authoritative Postgres
// using bounded keyset batches. versionWatermark should be Postgres dirty_version when
// available; <=0 means bump projectionVersion by 1 at cutover (never set from ZCARD).
func (s *RedisLeaderboardStore) RebuildFromPostgres(
	ctx context.Context,
	boardType domain.RatingSourceType,
	src RatingBatchSource,
	batchSize int,
	versionWatermark int64,
) error {
	if err := validateBoardType(boardType); err != nil {
		return err
	}
	if src == nil {
		return fmt.Errorf("nil rating batch source")
	}
	if batchSize < 1 {
		batchSize = DefaultLeaderboardRebuildBatch
	}
	if batchSize > MaxLeaderboardPageLimit*20 {
		batchSize = MaxLeaderboardPageLimit * 20
	}

	meta, liveGen, _, err := s.readMeta(ctx, boardType)
	if err != nil {
		return err
	}
	_ = meta
	newGen := liveGen + 1
	if newGen < 1 {
		newGen = 1
	}
	token, err := newRebuildToken()
	if err != nil {
		return fmt.Errorf("rebuild token: %w", err)
	}
	zkey := s.keys.GenZSet(boardType, newGen)
	akey := s.keys.GenApplied(boardType, newGen)
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = beginRebuildScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(boardType), zkey, akey},
		strconv.FormatInt(newGen, 10), now, token,
	).Result()
	if err != nil {
		ok, rerr := s.reconcileBeginRebuild(ctx, boardType, newGen, token)
		if rerr != nil {
			return rerr
		}
		if !ok {
			return wrapLeaderboardUnavailable(err)
		}
		// Begin applied before the client saw the error — continue rebuild.
	}

	var after *LeaderboardCursor
	for {
		batch, err := src.LeaderboardKeysetPage(ctx, boardType, after, batchSize)
		if err != nil {
			if aerr := s.abortRebuildBestEffort(ctx, boardType, newGen, token); aerr != nil {
				return errors.Join(err, fmt.Errorf("abort rebuild: %w", aerr))
			}
			return err
		}
		if len(batch) == 0 {
			break
		}
		args := make([]any, 0, 4+2*len(batch))
		args = append(args, strconv.FormatInt(newGen, 10), token, strconv.FormatInt(versionWatermark, 10), strconv.Itoa(len(batch)))
		for _, e := range batch {
			args = append(args, string(e.PlayerID), strconv.FormatFloat(RedisScoreForRating(e.Rating), 'f', -1, 64))
		}
		if _, err := rebuildMemberBatchScript.Run(ctx, s.rdb,
			[]string{s.keys.Meta(boardType), zkey, akey}, args...,
		).Result(); err != nil {
			if aerr := s.abortRebuildBestEffort(ctx, boardType, newGen, token); aerr != nil {
				return errors.Join(wrapLeaderboardUnavailable(err), fmt.Errorf("abort rebuild: %w", aerr))
			}
			return wrapLeaderboardUnavailable(err)
		}
		last := batch[len(batch)-1]
		after = &LeaderboardCursor{Rating: last.Rating, PlayerID: string(last.PlayerID)}
		if len(batch) < batchSize {
			break
		}
	}

	// Always pass same-slot old keys (gen:0 placeholder when no prior live) — never "".
	oldZ := s.keys.GenZSet(boardType, 0)
	oldApplied := s.keys.GenApplied(boardType, 0)
	if liveGen >= 1 {
		oldZ = s.keys.GenZSet(boardType, liveGen)
		oldApplied = s.keys.GenApplied(boardType, liveGen)
	}
	cutoverAt := time.Now().UTC().Format(time.RFC3339)
	_, err = cutoverRebuildScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(boardType), oldZ, oldApplied},
		strconv.FormatInt(newGen, 10),
		cutoverAt,
		strconv.FormatInt(versionWatermark, 10),
		s.keys.BoardRoot(boardType),
		token,
	).Result()
	if err != nil {
		if s.cutoverAlreadyAppliedBestEffort(ctx, boardType, newGen) {
			return nil
		}
		if aerr := s.abortRebuildBestEffort(ctx, boardType, newGen, token); aerr != nil {
			return errors.Join(wrapLeaderboardUnavailable(err), fmt.Errorf("abort rebuild: %w", aerr))
		}
		return wrapLeaderboardUnavailable(err)
	}
	return nil
}

// reconcileBeginRebuild re-reads meta after an ambiguous begin error.
// Success only when rebuilding_gen == newGen AND rebuild_token == ourToken.
func (s *RedisLeaderboardStore) reconcileBeginRebuild(
	parent context.Context,
	boardType domain.RatingSourceType,
	newGen int64,
	token string,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	m, liveGen, rebuilding, err := s.readMeta(ctx, boardType)
	if err != nil {
		log.Printf(`{"level":"error","service":"ranking","event":"leaderboard_begin_reconcile_failed","err":%q}`, err.Error())
		return false, nil
	}
	if rebuilding == newGen && m.RebuildToken == token {
		return true, nil
	}
	if rebuilding != 0 {
		return false, fmt.Errorf(
			"%w: rebuild already in progress (rebuilding_gen=%d, wanted=%d, live=%d)",
			ErrLeaderboardProjectionUnavailable, rebuilding, newGen, liveGen,
		)
	}
	return false, nil
}

func (s *RedisLeaderboardStore) abortRebuildBestEffort(parent context.Context, boardType domain.RatingSourceType, gen int64, token string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	err := s.abortRebuild(ctx, boardType, gen, token)
	if err != nil {
		log.Printf(`{"level":"error","service":"ranking","event":"leaderboard_rebuild_abort_failed","err":%q}`, err.Error())
	}
	return err
}

func (s *RedisLeaderboardStore) cutoverAlreadyAppliedBestEffort(parent context.Context, boardType domain.RatingSourceType, newGen int64) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	return s.cutoverAlreadyApplied(ctx, boardType, newGen)
}

func (s *RedisLeaderboardStore) cutoverAlreadyApplied(ctx context.Context, boardType domain.RatingSourceType, newGen int64) bool {
	m, live, rebuilding, err := s.readMeta(ctx, boardType)
	if err != nil {
		return false
	}
	return live == newGen && rebuilding == 0 && m.Ready
}

// AbortRebuildForTest runs the real abort script with the stored rebuild_token (tests only).
func (s *RedisLeaderboardStore) AbortRebuildForTest(ctx context.Context, boardType domain.RatingSourceType, gen int64) error {
	m, err := s.Meta(ctx, boardType)
	if err != nil {
		return err
	}
	return s.abortRebuild(ctx, boardType, gen, m.RebuildToken)
}

// RunRebuildMemberBatchForTest exposes the ownership-fenced batch script for tests.
func (s *RedisLeaderboardStore) RunRebuildMemberBatchForTest(
	ctx context.Context,
	boardType domain.RatingSourceType,
	gen int64,
	token string,
	versionWatermark int64,
	entries []domain.LeaderboardEntry,
) error {
	args := make([]any, 0, 4+2*len(entries))
	args = append(args, strconv.FormatInt(gen, 10), token, strconv.FormatInt(versionWatermark, 10), strconv.Itoa(len(entries)))
	for _, e := range entries {
		args = append(args, string(e.PlayerID), strconv.FormatFloat(RedisScoreForRating(e.Rating), 'f', -1, 64))
	}
	_, err := rebuildMemberBatchScript.Run(ctx, s.rdb,
		[]string{
			s.keys.Meta(boardType),
			s.keys.GenZSet(boardType, gen),
			s.keys.GenApplied(boardType, gen),
		},
		args...,
	).Result()
	return err
}

func (s *RedisLeaderboardStore) abortRebuild(ctx context.Context, boardType domain.RatingSourceType, gen int64, token string) error {
	_, err := abortRebuildScript.Run(ctx, s.rdb,
		[]string{
			s.keys.Meta(boardType),
			s.keys.GenZSet(boardType, gen),
			s.keys.GenApplied(boardType, gen),
		},
		strconv.FormatInt(gen, 10),
		token,
	).Result()
	return err
}
