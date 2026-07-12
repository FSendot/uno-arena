package main

import (
	"context"

	"unoarena/services/tournament-orchestration/store"
)

// durableAnalyticsBackfillStore adapts store.AnalyticsBackfillStore to the service reader.
type durableAnalyticsBackfillStore struct {
	inner *store.AnalyticsBackfillStore
}

func newDurableAnalyticsBackfillStore(inner *store.AnalyticsBackfillStore) *durableAnalyticsBackfillStore {
	return &durableAnalyticsBackfillStore{inner: inner}
}

func (d *durableAnalyticsBackfillStore) List(ctx context.Context, q AnalyticsBackfillQuery) ([]AnalyticsBackfillRow, error) {
	rows, err := d.inner.List(ctx, store.AnalyticsBackfillQuery{
		Topic:          q.Topic,
		AfterOutboxID:  q.AfterOutboxID,
		Limit:          q.Limit,
		FromOutboxID:   q.FromOutboxID,
		ToOutboxID:     q.ToOutboxID,
		FromOccurredAt: q.FromOccurredAt,
		ToOccurredAt:   q.ToOccurredAt,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AnalyticsBackfillRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, AnalyticsBackfillRow{
			OutboxID:      row.OutboxID,
			Topic:         row.Topic,
			EventType:     row.EventType,
			SchemaVersion: row.SchemaVersion,
			Payload:       append([]byte(nil), row.Payload...),
			OccurredAt:    row.OccurredAt,
		})
	}
	return out, nil
}
