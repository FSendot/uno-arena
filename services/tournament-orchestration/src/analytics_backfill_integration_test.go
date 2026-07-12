//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

func TestIntegration_AnalyticsBackfill_KeysetReadOnly(t *testing.T) {
	restore := SetAnalyticsBackfillCursorMACKeyForTest("pg-tournament-analytics-backfill-cursor")
	t.Cleanup(restore)

	pool, _, svc := serviceDurablePostgres(t)
	svc.SetAnalyticsBackfillReader(newDurableAnalyticsBackfillStore(store.NewAnalyticsBackfillStore(pool.Pool)))
	ctx := context.Background()
	at := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	var ids []int64
	for i := 0; i < 5; i++ {
		ts := at.Add(time.Duration(i) * time.Minute)
		payload, _ := json.Marshal(map[string]any{
			"eventId": fmt.Sprintf("pg-%d", i), "eventType": "TournamentMatchAssigned", "schemaVersion": 1,
			"correlationId": "c", "occurredAt": ts.UTC().Format(time.RFC3339Nano),
			"tournamentId": "t-pg", "roundNumber": 1, "slotId": fmt.Sprintf("slot_%d", i), "roomId": "room_pg",
		})
		var id int64
		err := pool.QueryRow(ctx, `
			INSERT INTO outbox_events (
				event_id, event_type, tournament_id, topic, partition_key, schema_version, payload, created_at
			) VALUES ($1,$2,$3,$4,$3,1,$5,$6)
			RETURNING outbox_id
		`, fmt.Sprintf("pg-eid-%d", i), "TournamentMatchAssigned", "t-pg",
			TopicMatchAssigned, payload, ts).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	// Noise topic must not leak into tournament.match.assigned pages.
	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, event_type, tournament_id, topic, partition_key, schema_version, payload, created_at
		) VALUES ('noise','TournamentCompleted','t-pg','tournament.completed','t-pg',1,'{"eventId":"noise"}',$1)
	`, at)
	if err != nil {
		t.Fatal(err)
	}

	fromCP, toCP := fmt.Sprintf("%d", ids[0]), fmt.Sprintf("%d", ids[len(ids)-1])
	page1, err := svc.AnalyticsBackfill(ctx, AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Records) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}
	page2, err := svc.AnalyticsBackfill(ctx, AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	page3, err := svc.AnalyticsBackfill(ctx, AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: page2.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	total := len(page1.Records) + len(page2.Records) + len(page3.Records)
	if total != 5 || page3.NextCursor != "" {
		t.Fatalf("total=%d next3=%q", total, page3.NextCursor)
	}

	var countBefore, countAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&countBefore)
	_, _ = svc.AnalyticsBackfill(ctx, AnalyticsBackfillRequest{
		RecoveryJobID: "pg-job", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 100,
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
