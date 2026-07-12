package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"unoarena/services/room-gameplay/app"
)

func TestMain(m *testing.M) {
	restore := app.SetAnalyticsBackfillCursorMACKeyForTest("test-room-analytics-backfill-cursor")
	code := m.Run()
	restore()
	os.Exit(code)
}

func metricEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "GameplayMetric", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"roomId": "room_1", "visibility": "anonymized_adhoc", "metricType": "card_played",
	})
	return b
}

func matchEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "MatchCompleted", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"roomId": "room_1", "completionVersion": 1,
		"players": []map[string]any{
			{"playerId": "p1", "matchWins": 1, "cumulativeCardPoints": 10},
		},
		"isAbandoned": false,
	})
	return b
}

func newBackfillSvc(t *testing.T) (*app.Service, *app.MemoryAnalyticsBackfillStore) {
	t.Helper()
	svc := app.NewService(app.ServiceDeps{
		Sessions:  app.NewMemorySessionRepository(),
		Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(),
		Audit:     app.NewFakeAuditSink(),
		Deals:     app.NewFakeDealSource(),
		Clock:     app.NewFixedClock(time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)),
		SessionsV: app.AllowAllSessionValidator{},
	})
	mem := app.NewMemoryAnalyticsBackfillStore()
	svc.SetAnalyticsBackfillReader(mem)
	return svc, mem
}

func TestAnalyticsBackfill_StrictValidation(t *testing.T) {
	svc, _ := newBackfillSvc(t)
	cases := []app.AnalyticsBackfillRequest{
		{SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1, FromCheckpoint: "1", ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 2, FromCheckpoint: "1", ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: "room.spectator-safe.events", SchemaVersion: 1, FromCheckpoint: "1", ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1, FromCheckpoint: "1"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1, ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1, FromOccurredAt: "2026-01-01T00:00:00Z"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1, FromCheckpoint: "10", ToCheckpoint: "1"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
			FromOccurredAt: "2026-02-01T00:00:00Z", ToOccurredAt: "2026-01-01T00:00:00Z"},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
			FromCheckpoint: "1", ToCheckpoint: "10", Limit: 1001},
		{RecoveryJobID: "job-1", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
			FromCheckpoint: "1", ToCheckpoint: "10", Limit: -1},
	}
	for i, req := range cases {
		_, err := svc.AnalyticsBackfill(context.Background(), req)
		if !errors.Is(err, app.ErrAnalyticsBackfillBadRequest) {
			t.Fatalf("case %d: want bad request, got %v", i, err)
		}
	}
}

func TestAnalyticsBackfill_DefaultLimitAndMax1000(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 150; i++ {
		id := fmt.Sprintf("m-%03d", i)
		ts := at.Add(time.Duration(i) * time.Second)
		mem.Append(app.TopicGameplayMetrics, "GameplayMetric", metricEnvelope(id, ts), &ts)
	}
	resp, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-lim", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "1000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != app.AnalyticsBackfillDefaultLimit {
		t.Fatalf("default limit: got %d want %d", len(resp.Records), app.AnalyticsBackfillDefaultLimit)
	}
	if resp.NextCursor == "" {
		t.Fatal("expected nextCursor for full default page")
	}

	_, err = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-lim", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "1000", Limit: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-lim", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "1000", Limit: 1001,
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("limit 1001: %v", err)
	}
}

func TestAnalyticsBackfill_ExactlyFullFinalPageHasNoCursor(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		ts := at.Add(time.Duration(i) * time.Second)
		mem.Append(app.TopicGameplayMetrics, "GameplayMetric",
			metricEnvelope(fmt.Sprintf("exact-%d", i), ts), &ts)
	}
	resp, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-exact", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "2", Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 2 || resp.NextCursor != "" {
		t.Fatalf("records=%d next=%q", len(resp.Records), resp.NextCursor)
	}
}

func TestAnalyticsBackfill_KeysetNoGapsDups_CursorBinding(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	var ids []int64
	for i := 0; i < 5; i++ {
		ts := at.Add(time.Duration(i) * time.Minute)
		id := mem.Append(app.TopicGameplayMetrics, "GameplayMetric",
			metricEnvelope(fmt.Sprintf("e%d", i), ts), &ts)
		ids = append(ids, id)
	}
	job := "job-page"
	fromCP, toCP := fmt.Sprintf("%d", ids[0]), fmt.Sprintf("%d", ids[len(ids)-1])
	first, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.NextCursor == "" {
		t.Fatalf("first page=%d cursor=%q", len(first.Records), first.NextCursor)
	}
	if first.FromCheckpoint != fmt.Sprintf("%d", ids[0]) || first.ToCheckpoint != fmt.Sprintf("%d", ids[1]) {
		t.Fatalf("coverage=%s..%s", first.FromCheckpoint, first.ToCheckpoint)
	}

	tampered := first.NextCursor[:len(first.NextCursor)-2] + "aa"
	_, err = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: tampered,
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("tampered cursor: %v", err)
	}

	_, err = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "other-job", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: first.NextCursor,
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("job binding: %v", err)
	}

	seen := map[string]struct{}{}
	for _, raw := range first.Records {
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		seen[m["eventId"].(string)] = struct{}{}
	}
	second, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: first.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range second.Records {
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		eid := m["eventId"].(string)
		if _, ok := seen[eid]; ok {
			t.Fatalf("duplicate eventId %s across pages", eid)
		}
		seen[eid] = struct{}{}
	}
	third, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: second.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Records) != 1 || third.NextCursor != "" {
		t.Fatalf("final page=%d next=%q", len(third.Records), third.NextCursor)
	}
	for _, raw := range third.Records {
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		seen[m["eventId"].(string)] = struct{}{}
	}
	if len(seen) != 5 {
		t.Fatalf("gaps/dups: seen=%d want 5", len(seen))
	}
	before := mem.Count()
	_, _ = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 100,
	})
	if mem.Count() != before {
		t.Fatal("backfill must not mutate outbox")
	}
}

func TestAnalyticsBackfill_TopicAllowlistAndCorruptPrivate(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	id := mem.Append(app.TopicMatchCompleted, "MatchCompleted", matchEnvelope("mc-1", at), &at)
	_, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-t", SourceTopic: "ranking.player_rating_updated", SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "9",
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("allowlist: %v", err)
	}

	ok, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-t", SourceTopic: app.TopicMatchCompleted, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", id), ToCheckpoint: fmt.Sprintf("%d", id),
	})
	if err != nil || len(ok.Records) != 1 {
		t.Fatalf("match topic: %v %#v", err, ok)
	}

	bad := mem.Append(app.TopicGameplayMetrics, "GameplayMetric", json.RawMessage(`{
		"eventId":"bad","eventType":"GameplayMetric","schemaVersion":1,
		"correlationId":"c","occurredAt":"2026-07-01T00:00:00Z",
		"roomId":"room_1","visibility":"anonymized_adhoc","metricType":"x","hand":["r1"]
	}`), &at)
	_, err = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-priv", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", bad), ToCheckpoint: fmt.Sprintf("%d", bad),
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillCorrupt) {
		t.Fatalf("private field: %v", err)
	}

	corrupt := mem.Append(app.TopicGameplayMetrics, "GameplayMetric", json.RawMessage(`{"eventId":"x"}`), &at)
	_, err = svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "job-bad", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", corrupt), ToCheckpoint: fmt.Sprintf("%d", corrupt),
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillCorrupt) {
		t.Fatalf("corrupt: %v", err)
	}
}

func TestAnalyticsBackfill_UnavailableWithoutReader(t *testing.T) {
	svc := app.NewService(app.ServiceDeps{
		Sessions: app.NewMemorySessionRepository(), Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), Clock: app.SystemClock{}, SessionsV: app.AllowAllSessionValidator{},
	})
	_, err := svc.AnalyticsBackfill(context.Background(), app.AnalyticsBackfillRequest{
		RecoveryJobID: "j", SourceTopic: app.TopicGameplayMetrics, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "2",
	})
	if !errors.Is(err, app.ErrAnalyticsBackfillUnavailable) {
		t.Fatalf("got %v", err)
	}
}
