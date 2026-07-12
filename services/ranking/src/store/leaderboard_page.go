package store

import (
	"time"

	"unoarena/services/ranking/domain"
)

const (
	// DefaultLeaderboardPageLimit is the OpenAPI default for public leaderboard pages.
	DefaultLeaderboardPageLimit = 100
	// MaxLeaderboardPageLimit is the OpenAPI hard maximum (ADR-0038).
	MaxLeaderboardPageLimit = 500
	// DefaultLeaderboardRebuildBatch is the bounded Postgres keyset batch for rebuilds.
	DefaultLeaderboardRebuildBatch = 1000
	// DefaultLeaderboardPageMaxScan caps same-score cursor fallback scans in Redis Lua
	// so a massive rating tie cannot block the Redis event loop.
	DefaultLeaderboardPageMaxScan = 4096
)

// RankedLeaderboardEntry is one public page row with absolute 1-based rank.
type RankedLeaderboardEntry struct {
	PlayerID domain.PlayerID
	Rating   int
	Rank     int
}

// LeaderboardPage is the OpenAPI LeaderboardPage contract body.
type LeaderboardPage struct {
	BoardType         domain.RatingSourceType
	ProjectionVersion int64
	GeneratedAt       time.Time
	Entries           []RankedLeaderboardEntry
	NextCursor        string // absent/empty on the final page
}

// LeaderboardPageQuery is the bounded public/rebuild read input.
type LeaderboardPageQuery struct {
	BoardType domain.RatingSourceType
	Cursor    string
	Limit     int
}

// ClampLeaderboardLimit returns a page size in [1, MaxLeaderboardPageLimit], defaulting empty/0 to 100.
func ClampLeaderboardLimit(limit int) int {
	if limit <= 0 {
		return DefaultLeaderboardPageLimit
	}
	if limit > MaxLeaderboardPageLimit {
		return MaxLeaderboardPageLimit
	}
	return limit
}

// RedisScoreForRating encodes rating so ZRANGE (ascending) yields rating DESC,
// with equal scores ordered by member (playerId) ASC.
// Redis ZREVRANGE would reverse member lex order on ties — do not use raw rating + ZREVRANGE.
func RedisScoreForRating(rating int) float64 {
	return -float64(rating)
}

// RatingFromRedisScore recovers the integer rating from an encoded score.
func RatingFromRedisScore(score float64) int {
	return int(-score)
}
