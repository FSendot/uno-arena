package store

import (
	"context"
	"errors"
	"log"
	"time"
)

// BracketProjectionCheckpointLoader exposes the authoritative Postgres public
// projection fence. Comparing it with Redis prevents a missed best-effort refresh
// (for example, a process crash after commit) from serving stale data indefinitely.
type BracketProjectionCheckpointLoader interface {
	LoadProjectionCheckpoint(ctx context.Context, tournamentID string) (int64, time.Time, error)
}

// CompositeBracketPageLoader reads Redis first, then falls back to Postgres LoadBracketPage.
// Auth must already have succeeded before this loader is invoked.
type CompositeBracketPageLoader struct {
	Redis *RedisBracketStore
	PG    interface {
		LoadBracketPage(ctx context.Context, q BracketPageQuery) (BracketPage, error)
	}
}

// LoadBracketPage prefers Redis; on miss/unavailable/error falls back to Postgres.
func (c *CompositeBracketPageLoader) LoadBracketPage(ctx context.Context, q BracketPageQuery) (BracketPage, error) {
	if c == nil || c.PG == nil {
		return BracketPage{}, errors.New("nil composite bracket page loader")
	}
	if c.Redis != nil {
		page, err := c.Redis.Page(ctx, q)
		if err == nil {
			if checkpoints, ok := c.PG.(BracketProjectionCheckpointLoader); ok {
				authoritativeVersion, _, checkpointErr := checkpoints.LoadProjectionCheckpoint(ctx, q.TournamentID)
				if checkpointErr == nil && authoritativeVersion == page.ProjectionVersion {
					return page, nil
				}
				if checkpointErr != nil {
					log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"bracket_checkpoint_fallback","tournamentId":%q,"err":%q}`,
						q.TournamentID, checkpointErr.Error())
				} else {
					log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"bracket_stale_fallback","tournamentId":%q,"redisVersion":%d,"postgresVersion":%d}`,
						q.TournamentID, page.ProjectionVersion, authoritativeVersion)
				}
			} else {
				return page, nil
			}
		} else {
			log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"bracket_redis_fallback","tournamentId":%q,"err":%q}`,
				q.TournamentID, err.Error())
		}
	}
	return c.PG.LoadBracketPage(ctx, q)
}
