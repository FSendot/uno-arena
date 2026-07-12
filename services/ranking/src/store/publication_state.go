package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/ranking/domain"
)

const (
	// LeaderboardSnapshotTopN is the AsyncAPI maxItems bound for LeaderboardSnapshotPublished.entries.
	LeaderboardSnapshotTopN = 100
	// DefaultLeaderboardSnapshotCooldown is the coalesce window (ADR-0038): at most one publish per board.
	DefaultLeaderboardSnapshotCooldown = 15 * time.Second
)

// markBoardDirty increments dirty_version exactly once for boardType in the open transaction
// and returns the resulting dirty_version (board projection fence for Redis CDC).
// Callers must invoke this only when at least one authoritative score changed in the same TX.
// last_dirty_at uses the transaction's authoritative Postgres now().
func markBoardDirty(ctx context.Context, tx pgx.Tx, boardType domain.RatingSourceType) (int64, error) {
	if boardType != domain.SourceCasualElo && boardType != domain.SourceTournamentPlacement {
		return 0, fmt.Errorf("invalid board type %q", boardType)
	}
	var ver int64
	err := tx.QueryRow(ctx, `
		UPDATE leaderboard_publication_state
		SET dirty_version = dirty_version + 1,
		    last_dirty_at = now()
		WHERE board_type = $1
		RETURNING dirty_version
	`, string(boardType)).Scan(&ver)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("leaderboard_publication_state missing seed row for %s", boardType)
		}
		return 0, wrapUnavailable(err)
	}
	return ver, nil
}

// PublicationState is one board's durable dirty/published checkpoint.
type PublicationState struct {
	BoardType        domain.RatingSourceType
	DirtyVersion     int64
	PublishedVersion int64
	LastDirtyAt      *time.Time
	LastPublishedAt  *time.Time
}

// GetPublicationState loads one board row (test/ops helper).
func (s *RankingStore) GetPublicationState(ctx context.Context, boardType domain.RatingSourceType) (PublicationState, error) {
	var st PublicationState
	var board string
	err := s.pool.QueryRow(ctx, `
		SELECT board_type, dirty_version, published_version, last_dirty_at, last_published_at
		FROM leaderboard_publication_state WHERE board_type = $1
	`, string(boardType)).Scan(&board, &st.DirtyVersion, &st.PublishedVersion, &st.LastDirtyAt, &st.LastPublishedAt)
	if err != nil {
		return PublicationState{}, wrapUnavailable(err)
	}
	st.BoardType = domain.RatingSourceType(board)
	return st, nil
}
