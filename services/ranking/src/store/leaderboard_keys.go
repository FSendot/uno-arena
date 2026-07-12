package store

import (
	"fmt"
	"strings"

	"unoarena/services/ranking/domain"
)

const (
	leaderboardKeySchemaVersion = "v1"
	defaultLeaderboardKeyPrefix = "ranking:"
)

// LeaderboardKeySpace builds versioned Redis keys for Ranking leaderboard projections.
// Board-type hash tags keep meta/zset/applied keys in one Redis Cluster slot so Lua
// can resolve generation-scoped keys from a shared root (mirrors Spectator {roomId}):
//
//	ranking:v1:lb:{boardType}:meta
//	ranking:v1:lb:{boardType}:gen:{N}:z
//	ranking:v1:lb:{boardType}:gen:{N}:applied
type LeaderboardKeySpace struct {
	prefix string
}

// NewLeaderboardKeySpace returns a key space. Empty prefix defaults to ranking:.
func NewLeaderboardKeySpace(prefix string) LeaderboardKeySpace {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = defaultLeaderboardKeyPrefix
	}
	if !strings.HasSuffix(p, ":") {
		p += ":"
	}
	return LeaderboardKeySpace{prefix: p}
}

// Prefix returns the configured key prefix.
func (k LeaderboardKeySpace) Prefix() string { return k.prefix }

// BoardRoot is the shared key root including the cluster hash-tag for boardType.
func (k LeaderboardKeySpace) BoardRoot(boardType domain.RatingSourceType) string {
	return fmt.Sprintf("%s%s:lb:{%s}:", k.prefix, leaderboardKeySchemaVersion, string(boardType))
}

func (k LeaderboardKeySpace) boardRoot(boardType domain.RatingSourceType) string {
	return k.BoardRoot(boardType)
}

// Meta is the board metadata hash (generation, projectionVersion, generatedAt, rebuilding_gen).
func (k LeaderboardKeySpace) Meta(boardType domain.RatingSourceType) string {
	return k.boardRoot(boardType) + "meta"
}

// GenZSet is the generation-scoped complete-board sorted set.
func (k LeaderboardKeySpace) GenZSet(boardType domain.RatingSourceType, generation int64) string {
	return fmt.Sprintf("%sgen:%d:z", k.boardRoot(boardType), generation)
}

// GenApplied is the per-player board projectionVersion fence hash for a generation.
func (k LeaderboardKeySpace) GenApplied(boardType domain.RatingSourceType, generation int64) string {
	return fmt.Sprintf("%sgen:%d:applied", k.boardRoot(boardType), generation)
}
