package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultContinuationLeaseTTL = 30 * time.Second

// NextGameContinuation is a Room-owned durable request to advance one
// non-terminal best-of-three match. Its identities are stable across retries.
type NextGameContinuation struct {
	RoomID          string
	CompletedGameID string
	CommandID       string
	NextGameID      string
	Attempts        int
}

// NextGameContinuationQueue claims the Postgres-authoritative continuation
// queue. Rows are inserted/deleted in the same transaction as Room snapshots.
type NextGameContinuationQueue struct {
	pool     *pgxpool.Pool
	leaseTTL time.Duration
}

func NewNextGameContinuationQueue(pool *pgxpool.Pool) *NextGameContinuationQueue {
	return &NextGameContinuationQueue{pool: pool, leaseTTL: defaultContinuationLeaseTTL}
}

func (q *NextGameContinuationQueue) WithLeaseTTL(ttl time.Duration) *NextGameContinuationQueue {
	cp := *q
	if ttl > 0 {
		cp.leaseTTL = ttl
	}
	return &cp
}

// ClaimDue uses one bounded SKIP LOCKED statement, allowing timer-worker
// replicas to scale without duplicate ownership or a process-local coordinator.
func (q *NextGameContinuationQueue) ClaimDue(ctx context.Context, now time.Time, limit int) ([]NextGameContinuation, error) {
	if limit <= 0 {
		limit = 16
	}
	rows, err := q.pool.Query(ctx, `
		WITH due AS (
			SELECT room_id
			FROM next_game_continuations
			WHERE available_at <= $1
			  AND (lease_until IS NULL OR lease_until <= $1)
			ORDER BY available_at, room_id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE next_game_continuations AS c
		SET lease_until = $3, attempts = c.attempts + 1, updated_at = $1
		FROM due
		WHERE c.room_id = due.room_id
		RETURNING c.room_id, c.completed_game_id, c.command_id, c.next_game_id, c.attempts
	`, now.UTC(), limit, now.Add(q.leaseTTL).UTC())
	if err != nil {
		return nil, wrapUnavailable(err)
	}
	defer rows.Close()
	var out []NextGameContinuation
	for rows.Next() {
		var item NextGameContinuation
		if err := rows.Scan(&item.RoomID, &item.CompletedGameID, &item.CommandID, &item.NextGameID, &item.Attempts); err != nil {
			return nil, wrapUnavailable(err)
		}
		out = append(out, item)
	}
	return out, wrapUnavailable(rows.Err())
}

// Release makes a failed attempt eligible after a bounded delay. A worker
// crash needs no call here: lease expiry provides the same recovery path.
func (q *NextGameContinuationQueue) Release(ctx context.Context, item NextGameContinuation, retryAt time.Time) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE next_game_continuations
		SET available_at = $2, lease_until = NULL, updated_at = now()
		WHERE room_id = $1 AND command_id = $3
	`, item.RoomID, retryAt.UTC(), item.CommandID)
	return wrapUnavailable(err)
}

// Ack removes a stale/terminal continuation. Accepted StartNextGame commands
// normally delete the row atomically while committing the new game snapshot.
func (q *NextGameContinuationQueue) Ack(ctx context.Context, item NextGameContinuation) error {
	_, err := q.pool.Exec(ctx, `
		DELETE FROM next_game_continuations WHERE room_id = $1 AND command_id = $2
	`, item.RoomID, item.CommandID)
	return wrapUnavailable(err)
}
