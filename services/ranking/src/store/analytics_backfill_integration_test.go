//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"unoarena/services/ranking/store"
)

func TestIntegration_AnalyticsBackfill_KeysetReadOnly(t *testing.T) {
	url := postgresURL(t)
	ctx := context.Background()
	pool, err := store.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	resetPublic(t, ctx, pool)
	applyMigration(t, ctx, pool)

	at := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	var ids []int64
	for i := 0; i < 5; i++ {
		ts := at.Add(time.Duration(i) * time.Minute)
		payload, _ := json.Marshal(map[string]any{
			"eventId": fmt.Sprintf("pg-%d", i), "eventType": "PlayerRatingUpdated", "schemaVersion": 1,
			"correlationId": "c", "occurredAt": ts.UTC().Format(time.RFC3339Nano),
			"playerId": "p1", "gameId": "g1", "previousRating": 1000, "newRating": 1001,
			"projectionVersion": i + 1,
		})
		var id int64
		err := pool.QueryRow(ctx, `
			INSERT INTO outbox_events (
				event_id, event_type, player_id, topic, partition_key, schema_version, payload, created_at
			) VALUES ($1,$2,$3,$4,$3,1,$5,$6)
			RETURNING outbox_id
		`, fmt.Sprintf("pg-eid-%d", i), "PlayerRatingUpdated", "p1",
			"ranking.player_rating_updated", payload, ts).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, topic, partition_key, schema_version, payload, created_at
		) VALUES ('noise','LeaderboardSnapshotPublished','ranking.leaderboard_snapshot_published','casual_elo',1,'{"eventId":"noise"}',$1)
	`, at)
	if err != nil {
		t.Fatal(err)
	}

	reader := store.NewAnalyticsBackfillStore(pool.Pool)
	fromOID, toOID := ids[0], ids[len(ids)-1]
	page1, err := reader.List(ctx, store.AnalyticsBackfillQuery{
		Topic: "ranking.player_rating_updated", AfterOutboxID: 0, Limit: 3,
		FromOutboxID: &fromOID, ToOutboxID: &toOID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1=%d", len(page1))
	}
	page2, err := reader.List(ctx, store.AnalyticsBackfillQuery{
		Topic: "ranking.player_rating_updated", AfterOutboxID: page1[len(page1)-1].OutboxID, Limit: 3,
		FromOutboxID: &fromOID, ToOutboxID: &toOID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2=%d", len(page2))
	}
	seen := map[int64]struct{}{}
	for _, row := range append(page1, page2...) {
		if _, ok := seen[row.OutboxID]; ok {
			t.Fatalf("duplicate outbox_id %d", row.OutboxID)
		}
		seen[row.OutboxID] = struct{}{}
		if row.Topic != "ranking.player_rating_updated" {
			t.Fatalf("topic leak: %s", row.Topic)
		}
	}
	if len(seen) != 5 {
		t.Fatalf("seen=%d", len(seen))
	}

	var countBefore, countAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&countBefore)
	_, _ = reader.List(ctx, store.AnalyticsBackfillQuery{
		Topic: "ranking.player_rating_updated", AfterOutboxID: 0, Limit: 100,
		FromOutboxID: &fromOID, ToOutboxID: &toOID,
	})
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&countAfter)
	if countBefore != countAfter {
		t.Fatalf("outbox mutated: %d -> %d", countBefore, countAfter)
	}

	var idx int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE tablename = 'outbox_events'
		  AND indexname = 'outbox_events_topic_outbox_idx'
	`).Scan(&idx); err != nil || idx != 1 {
		t.Fatalf("baseline topic/outbox index missing: idx=%d err=%v", idx, err)
	}
}
