package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"unoarena/services/ranking/domain"
)

// ErrLeaderboardProjectionUnavailable is returned when Redis projection ops fail
// or the live projection has not been rebuilt yet (callers fall back to Postgres).
var ErrLeaderboardProjectionUnavailable = errors.New("leaderboard projection unavailable")

// ErrLeaderboardProjectionConflict is returned when an equal projectionVersion targets a
// different score, or a higher version's previousRating does not match the live member.
var ErrLeaderboardProjectionConflict = errors.New("leaderboard projection conflict")

var (
	pageMaxScanMu       sync.Mutex
	pageMaxScanOverride int
)

// SetLeaderboardPageMaxScanForTest overrides the same-score fallback scan cap.
// Pass <=0 to clear. Returns a restore func.
func SetLeaderboardPageMaxScanForTest(n int) func() {
	pageMaxScanMu.Lock()
	prev := pageMaxScanOverride
	pageMaxScanOverride = n
	pageMaxScanMu.Unlock()
	return func() {
		pageMaxScanMu.Lock()
		pageMaxScanOverride = prev
		pageMaxScanMu.Unlock()
	}
}

func leaderboardPageMaxScan() int {
	pageMaxScanMu.Lock()
	n := pageMaxScanOverride
	pageMaxScanMu.Unlock()
	if n > 0 {
		return n
	}
	return DefaultLeaderboardPageMaxScan
}

// RedisLeaderboardStore is the non-authoritative complete-board Redis projection.
type RedisLeaderboardStore struct {
	rdb  redis.UniversalClient
	keys LeaderboardKeySpace
}

// NewRedisLeaderboardStore wraps a Redis client for leaderboard projection I/O.
func NewRedisLeaderboardStore(rdb redis.UniversalClient, keyPrefix string) *RedisLeaderboardStore {
	return &RedisLeaderboardStore{
		rdb:  rdb,
		keys: NewLeaderboardKeySpace(keyPrefix),
	}
}

// Client exposes the underlying Redis client (tests/ops).
func (s *RedisLeaderboardStore) Client() redis.UniversalClient { return s.rdb }

// LoadScripts preloads Lua scripts (fail-closed wiring).
func (s *RedisLeaderboardStore) LoadScripts(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: nil store", ErrLeaderboardProjectionUnavailable)
	}
	for _, script := range []*redis.Script{
		upsertLeaderboardMemberScript,
		beginRebuildScript,
		rebuildMemberBatchScript,
		cutoverRebuildScript,
		abortRebuildScript,
		pageScript,
	} {
		if err := script.Load(ctx, s.rdb).Err(); err != nil {
			return wrapLeaderboardUnavailable(err)
		}
	}
	return nil
}

// Ping checks Redis connectivity.
func (s *RedisLeaderboardStore) Ping(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("%w: nil store", ErrLeaderboardProjectionUnavailable)
	}
	return wrapLeaderboardUnavailable(PingRedis(ctx, s.rdb))
}

// UpsertPlayer applies one CDC rating update fenced by board projectionVersion.
// Empty projections are a no-op. Stale live rejects do not dual-write staging.
// Zero-delta (previousRating == newRating) is a no-op without requiring projectionVersion.
func (s *RedisLeaderboardStore) UpsertPlayer(
	ctx context.Context,
	boardType domain.RatingSourceType,
	playerID domain.PlayerID,
	previousRating, newRating int,
	occurredAt time.Time,
	projectionVersion int64,
) error {
	if err := validateBoardType(boardType); err != nil {
		return err
	}
	if !playerID.Valid() {
		return fmt.Errorf("playerId required")
	}
	if previousRating == newRating {
		return nil
	}
	if projectionVersion <= 0 {
		return fmt.Errorf("projectionVersion required for score-changing upsert")
	}
	if occurredAt.IsZero() {
		return fmt.Errorf("occurredAt required for leaderboard generatedAt metadata")
	}
	score := RedisScoreForRating(newRating)
	raw, err := upsertLeaderboardMemberScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(boardType)},
		s.keys.BoardRoot(boardType),
		string(playerID),
		strconv.FormatFloat(score, 'f', -1, 64),
		strconv.FormatInt(projectionVersion, 10),
		strconv.Itoa(previousRating),
		occurredAt.UTC().Format(time.RFC3339),
	).Int64()
	if err != nil {
		return wrapLeaderboardUnavailable(err)
	}
	if raw == 3 {
		return fmt.Errorf("%w: playerId=%s version=%d", ErrLeaderboardProjectionConflict, playerID, projectionVersion)
	}
	return nil
}

// Page returns a bounded live-keyset page from the Redis projection.
// Head and cursor pages are served by one atomic Lua script (generation, version,
// generatedAt, rankBase, entries). Retries once on cutover races.
func (s *RedisLeaderboardStore) Page(ctx context.Context, q LeaderboardPageQuery) (LeaderboardPage, error) {
	if err := validateBoardType(q.BoardType); err != nil {
		return LeaderboardPage{}, err
	}
	limit := ClampLeaderboardLimit(q.Limit)
	var after *LeaderboardCursor
	if strings.TrimSpace(q.Cursor) != "" {
		c, err := DecodeLeaderboardCursor(q.Cursor)
		if err != nil {
			return LeaderboardPage{}, err
		}
		after = &c
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		page, retry, err := s.pageAtomic(ctx, q.BoardType, after, limit)
		if err != nil {
			return LeaderboardPage{}, err
		}
		if retry {
			lastErr = fmt.Errorf("%w: generation cutover race", ErrLeaderboardProjectionUnavailable)
			continue
		}
		return page, nil
	}
	return LeaderboardPage{}, lastErr
}

func (s *RedisLeaderboardStore) pageAtomic(
	ctx context.Context,
	boardType domain.RatingSourceType,
	after *LeaderboardCursor,
	limit int,
) (LeaderboardPage, bool, error) {
	fetch := int64(limit + 1)
	hasCursor := "0"
	afterRating := "0"
	afterPlayer := ""
	if after != nil {
		hasCursor = "1"
		afterRating = strconv.Itoa(after.Rating)
		afterPlayer = after.PlayerID
	}
	raw, err := pageScript.Run(ctx, s.rdb,
		[]string{s.keys.Meta(boardType)},
		s.keys.BoardRoot(boardType),
		strconv.FormatInt(fetch, 10),
		hasCursor,
		afterRating,
		afterPlayer,
		strconv.Itoa(leaderboardPageMaxScan()),
	).Slice()
	if err != nil {
		return LeaderboardPage{}, false, wrapLeaderboardUnavailable(err)
	}
	if len(raw) < 1 {
		return LeaderboardPage{}, false, wrapLeaderboardUnavailable(fmt.Errorf("empty page script result"))
	}
	status := toInt64(raw[0])
	switch status {
	case 0:
		return LeaderboardPage{}, false, ErrLeaderboardProjectionUnavailable
	case 2:
		return LeaderboardPage{}, true, nil
	}
	if len(raw) < 5 {
		return LeaderboardPage{}, false, wrapLeaderboardUnavailable(fmt.Errorf("short page script result"))
	}
	ver := toInt64(raw[2])
	generatedAt, err := parseGeneratedAt(fmt.Sprint(raw[3]))
	if err != nil {
		return LeaderboardPage{}, false, ErrLeaderboardProjectionUnavailable
	}
	rankBase := int(toInt64(raw[4]))
	if rankBase < 1 {
		rankBase = 1
	}
	pairs := decodeScorePairs(raw[5:])
	page, err := s.assemblePage(boardType, ver, generatedAt, pairs, limit, rankBase)
	if err != nil {
		return LeaderboardPage{}, false, err
	}
	return page, false, nil
}

func (s *RedisLeaderboardStore) assemblePage(
	boardType domain.RatingSourceType,
	ver int64,
	generatedAt time.Time,
	pairs []redis.Z,
	limit int,
	rankBase int,
) (LeaderboardPage, error) {
	entries := make([]RankedLeaderboardEntry, 0, min(len(pairs), limit))
	for i, z := range pairs {
		if i >= limit {
			break
		}
		entries = append(entries, RankedLeaderboardEntry{
			PlayerID: domain.PlayerID(fmt.Sprint(z.Member)),
			Rating:   RatingFromRedisScore(z.Score),
			Rank:     rankBase + i,
		})
	}
	page := LeaderboardPage{
		BoardType:         boardType,
		ProjectionVersion: ver,
		GeneratedAt:       generatedAt,
		Entries:           entries,
	}
	if len(pairs) > limit && len(entries) > 0 {
		last := entries[len(entries)-1]
		enc, err := EncodeLeaderboardCursor(LeaderboardCursor{
			Rating: last.Rating, PlayerID: string(last.PlayerID),
		})
		if err != nil {
			return LeaderboardPage{}, err
		}
		page.NextCursor = enc
	}
	return page, nil
}

func decodeScorePairs(raw []any) []redis.Z {
	out := make([]redis.Z, 0, len(raw)/2)
	for i := 0; i+1 < len(raw); i += 2 {
		member := fmt.Sprint(raw[i])
		score, _ := strconv.ParseFloat(fmt.Sprint(raw[i+1]), 64)
		out = append(out, redis.Z{Member: member, Score: score})
	}
	return out
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}

func parseGeneratedAt(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" || v == "<nil>" {
		return time.Time{}, fmt.Errorf("generatedAt missing")
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("generatedAt invalid: %w", err)
	}
	return t.UTC(), nil
}

// LeaderboardMeta is the Redis board metadata projection.
type LeaderboardMeta struct {
	Generation        int64
	RebuildingGen     int64
	RebuildToken      string
	ProjectionVersion int64
	MemberCount       int64
	MemberCountSet    bool
	Ready             bool
	GeneratedAt       time.Time
}

func (s *RedisLeaderboardStore) readMeta(ctx context.Context, boardType domain.RatingSourceType) (LeaderboardMeta, int64, int64, error) {
	vals, err := s.rdb.HGetAll(ctx, s.keys.Meta(boardType)).Result()
	if err != nil {
		return LeaderboardMeta{}, 0, 0, wrapLeaderboardUnavailable(err)
	}
	var m LeaderboardMeta
	if v := vals["generation"]; v != "" {
		m.Generation, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := vals["rebuilding_gen"]; v != "" {
		m.RebuildingGen, _ = strconv.ParseInt(v, 10, 64)
	}
	m.RebuildToken = vals["rebuild_token"]
	if v := vals["projectionVersion"]; v != "" {
		m.ProjectionVersion, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := vals["memberCount"]; ok {
		m.MemberCountSet = true
		m.MemberCount, _ = strconv.ParseInt(v, 10, 64)
	}
	m.Ready = vals["ready"] == "1"
	if v := vals["generatedAt"]; v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			m.GeneratedAt = t.UTC()
		}
	}
	if m.GeneratedAt.IsZero() {
		m.GeneratedAt = time.Now().UTC()
	}
	return m, m.Generation, m.RebuildingGen, nil
}

// Meta returns current board projection metadata.
func (s *RedisLeaderboardStore) Meta(ctx context.Context, boardType domain.RatingSourceType) (LeaderboardMeta, error) {
	m, _, _, err := s.readMeta(ctx, boardType)
	return m, err
}

func validateBoardType(boardType domain.RatingSourceType) error {
	if boardType != domain.SourceCasualElo && boardType != domain.SourceTournamentPlacement {
		return fmt.Errorf("boardType must be casual_elo or tournament_placement")
	}
	return nil
}

func wrapLeaderboardUnavailable(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrLeaderboardProjectionUnavailable) {
		return err
	}
	return errors.Join(ErrLeaderboardProjectionUnavailable, err)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
