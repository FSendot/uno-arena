package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/services/room-gameplay/app"
)

// ListAnalyticsBackfill reads append-only outbox rows. It never updates or deletes.
func ListAnalyticsBackfill(ctx context.Context, pool *pgxpool.Pool, q app.AnalyticsBackfillQuery) ([]app.AnalyticsBackfillRow, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil pool")
	}
	topic := strings.TrimSpace(q.Topic)
	if topic == "" {
		return nil, fmt.Errorf("topic required")
	}
	if q.Limit < 1 {
		return nil, fmt.Errorf("limit required")
	}

	args := []any{topic, q.AfterOutboxID}
	var b strings.Builder
	b.WriteString(`
SELECT outbox_id, topic, event_type, payload, occurred_at
FROM integration_outbox_events
WHERE topic = $1
  AND outbox_id > $2`)
	argN := 3
	if q.FromOutboxID != nil {
		fmt.Fprintf(&b, "\n  AND outbox_id >= $%d", argN)
		args = append(args, *q.FromOutboxID)
		argN++
	}
	if q.ToOutboxID != nil {
		fmt.Fprintf(&b, "\n  AND outbox_id <= $%d", argN)
		args = append(args, *q.ToOutboxID)
		argN++
	}
	if q.FromOccurredAt != nil {
		fmt.Fprintf(&b, "\n  AND occurred_at >= $%d", argN)
		args = append(args, *q.FromOccurredAt)
		argN++
	}
	if q.ToOccurredAt != nil {
		fmt.Fprintf(&b, "\n  AND occurred_at <= $%d", argN)
		args = append(args, *q.ToOccurredAt)
		argN++
	}
	fmt.Fprintf(&b, "\nORDER BY outbox_id ASC\nLIMIT $%d", argN)
	args = append(args, q.Limit)

	rows, err := pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("analytics backfill query: %w", err)
	}
	defer rows.Close()

	out := make([]app.AnalyticsBackfillRow, 0, q.Limit)
	for rows.Next() {
		var row app.AnalyticsBackfillRow
		var occurredAt *time.Time
		if err := rows.Scan(&row.OutboxID, &row.Topic, &row.EventType, &row.Payload, &occurredAt); err != nil {
			return nil, fmt.Errorf("analytics backfill scan: %w", err)
		}
		row.OccurredAt = occurredAt
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analytics backfill rows: %w", err)
	}
	return out, nil
}

// AnalyticsBackfillStore wraps a pool for SessionStore-style injection.
type AnalyticsBackfillStore struct {
	pool *pgxpool.Pool
}

// NewAnalyticsBackfillStore constructs a durable backfill reader.
func NewAnalyticsBackfillStore(pool *pgxpool.Pool) *AnalyticsBackfillStore {
	return &AnalyticsBackfillStore{pool: pool}
}

// List implements app.AnalyticsBackfillReader.
func (s *AnalyticsBackfillStore) List(ctx context.Context, q app.AnalyticsBackfillQuery) ([]app.AnalyticsBackfillRow, error) {
	return ListAnalyticsBackfill(ctx, s.pool, q)
}

// AssertNoOutboxMutation is a compile-time reminder: backfill paths must stay read-only.
// Integration tests grep this file for UPDATE/DELETE against integration_outbox_events.
var _ = pgx.ErrNoRows
var _ app.AnalyticsBackfillReader = (*AnalyticsBackfillStore)(nil)
