package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"unoarena/services/ranking/domain"
)

// ClaimedBoardPublish is the result of one successful snapshot publication TX.
type ClaimedBoardPublish struct {
	BoardType        domain.RatingSourceType
	ClaimedDirty     int64
	PublishedVersion int64
	SnapshotID       string
	EntryCount       int
	Published        bool // false when no dirty/eligible board was claimed
}

// PublishNextDirtyLeaderboardSnapshot claims one dirty board (FOR UPDATE SKIP LOCKED),
// enforces the coalesce cooldown via Postgres clock_timestamp() (wall clock, not TX now()),
// queries top-N, and atomically writes snapshot metadata + outbox + published_version =
// claimed dirty using one late clock_timestamp() for generated_at / outbox / checkpoint.
// Concurrent dirties after the claim bump dirty_version further so the board stays dirty.
func (s *RankingStore) PublishNextDirtyLeaderboardSnapshot(ctx context.Context, cooldown time.Duration) (ClaimedBoardPublish, error) {
	if s == nil || s.pool == nil {
		return ClaimedBoardPublish{}, fmt.Errorf("nil store")
	}
	if cooldown <= 0 {
		cooldown = DefaultLeaderboardSnapshotCooldown
	}
	cooldownSecs := cooldown.Seconds()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ClaimedBoardPublish{}, wrapUnavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var board string
	var dirty, published int64
	err = tx.QueryRow(ctx, `
		SELECT board_type, dirty_version, published_version
		FROM leaderboard_publication_state
		WHERE dirty_version > published_version
		  AND (last_published_at IS NULL OR last_published_at <= clock_timestamp() - make_interval(secs => $1::double precision))
		ORDER BY last_dirty_at ASC NULLS FIRST, board_type ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, cooldownSecs).Scan(&board, &dirty, &published)
	if err == pgx.ErrNoRows {
		if err := tx.Commit(ctx); err != nil {
			return ClaimedBoardPublish{}, wrapUnavailable(err)
		}
		return ClaimedBoardPublish{Published: false}, nil
	}
	if err != nil {
		return ClaimedBoardPublish{}, wrapUnavailable(err)
	}
	boardType := domain.RatingSourceType(board)
	claimedDirty := dirty

	entries, err := queryTopLeaderboard(ctx, tx, boardType, LeaderboardSnapshotTopN)
	if err != nil {
		return ClaimedBoardPublish{}, err
	}

	snapshotID := board + ":v" + strconv.FormatInt(claimedDirty, 10)
	cmdOut := domain.PublishLeaderboardSnapshot(domain.PublishLeaderboardSnapshotCommand{
		CommandID:  domain.CommandID("snapshotter:" + snapshotID),
		SnapshotID: domain.SnapshotID(snapshotID),
		BoardType:  boardType,
		Entries:    entries,
	})
	if cmdOut.Kind != domain.OutcomeAccepted || len(cmdOut.Facts) == 0 {
		return ClaimedBoardPublish{}, fmt.Errorf("domain snapshot rejected: %#v", cmdOut)
	}

	// Wall-clock stamp taken after work so cooldown and publication reflect actual publish time,
	// not transaction start (now() is stable for the whole TX).
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return ClaimedBoardPublish{}, wrapUnavailable(err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO leaderboard_snapshots (
			snapshot_id, board_type, schema_version, generated_at, player_count,
			content_version, published_event_id, metadata
		) VALUES ($1, $2, 1, $3, $4, $5, $6, '{}'::jsonb)
		ON CONFLICT (snapshot_id) DO NOTHING
	`, snapshotID, board, dbNow, len(entries), strconv.FormatInt(claimedDirty, 10), snapshotID); err != nil {
		return ClaimedBoardPublish{}, wrapUnavailable(err)
	}

	fact := cmdOut.Facts[0]
	ev, err := outboxFromFact(fact, outboxMeta{
		UpstreamEventID: snapshotID,
		CorrelationID:   snapshotID,
		CausationID:     "ranking-leaderboard-snapshotter",
		Now:             dbNow,
	})
	if err != nil {
		return ClaimedBoardPublish{}, err
	}
	if err := insertOutboxEvents(ctx, tx, []OutboxEvent{ev}); err != nil {
		return ClaimedBoardPublish{}, err
	}

	// Checkpoint only to the claimed dirty version so dirties during this TX remain dirty.
	tag, err := tx.Exec(ctx, `
		UPDATE leaderboard_publication_state
		SET published_version = $2,
		    last_published_at = $3
		WHERE board_type = $1
		  AND published_version < $2
	`, board, claimedDirty, dbNow)
	if err != nil {
		return ClaimedBoardPublish{}, wrapUnavailable(err)
	}
	if tag.RowsAffected() != 1 {
		return ClaimedBoardPublish{}, fmt.Errorf("failed to checkpoint published_version for %s", board)
	}

	if err := tx.Commit(ctx); err != nil {
		return ClaimedBoardPublish{}, wrapUnavailable(err)
	}
	return ClaimedBoardPublish{
		BoardType:        boardType,
		ClaimedDirty:     claimedDirty,
		PublishedVersion: claimedDirty,
		SnapshotID:       snapshotID,
		EntryCount:       len(entries),
		Published:        true,
	}, nil
}

func queryTopLeaderboard(ctx context.Context, tx pgx.Tx, boardType domain.RatingSourceType, limit int) ([]domain.LeaderboardEntry, error) {
	if limit <= 0 {
		limit = LeaderboardSnapshotTopN
	}
	col := "casual_elo"
	if boardType == domain.SourceTournamentPlacement {
		col = "tournament_placement_rating"
	}
	//nolint:gosec // col is fixed enum branch only
	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT player_id, %s
		FROM player_ratings
		ORDER BY %s DESC, player_id ASC
		LIMIT $1
	`, col, col), limit)
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer rows.Close()
	entries := make([]domain.LeaderboardEntry, 0, limit)
	for rows.Next() {
		var id string
		var rating int
		if err := rows.Scan(&id, &rating); err != nil {
			return nil, wrapUnavailable(err)
		}
		entries = append(entries, domain.LeaderboardEntry{
			PlayerID: domain.PlayerID(id),
			Rating:   rating,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, wrapUnavailable(err)
	}
	return entries, nil
}
