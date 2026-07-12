package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func assignedEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "TournamentMatchAssigned", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"tournamentId": "t1", "roundNumber": 1, "slotId": "slot_0", "roomId": "room_1",
	})
	return b
}

func resultRecordedEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "TournamentMatchResultRecorded", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"tournamentId": "t1", "roomId": "room_1", "completionVersion": 1,
	})
	return b
}

func playersAdvancedEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "PlayersAdvanced", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"tournamentId": "t1", "roundNumber": 1, "sourceSlotId": "slot_0",
		"advancingPlayerIds": []string{"p1", "p2", "p3"},
	})
	return b
}

func roundCompletedEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "TournamentRoundCompleted", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"tournamentId": "t1", "roundNumber": 1,
	})
	return b
}

func tournamentCompletedEnvelope(id string, at time.Time) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"eventId": id, "eventType": "TournamentCompleted", "schemaVersion": 1,
		"correlationId": "corr-" + id, "occurredAt": at.UTC().Format(time.RFC3339Nano),
		"tournamentId": "t1", "finalStandings": []string{"p1", "p2", "p3"},
	})
	return b
}

func newBackfillSvc(t *testing.T) (*Service, *MemoryAnalyticsBackfillStore) {
	t.Helper()
	restore := SetAnalyticsBackfillCursorMACKeyForTest("test-tournament-analytics-backfill-cursor")
	t.Cleanup(restore)
	svc := NewService(ServiceDeps{
		Repo:      NewMemoryTournamentRepository(),
		Rooms:     NoopRoomProvisioner{},
		Publisher: NoopPublisher{},
		Audit:     NewMemoryAudit(),
		Clock:     fixedClock{now: time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)},
	})
	mem := NewMemoryAnalyticsBackfillStore()
	svc.SetAnalyticsBackfillReader(mem)
	return svc, mem
}

func TestAnalyticsBackfill_StrictValidation(t *testing.T) {
	svc, _ := newBackfillSvc(t)
	cases := []AnalyticsBackfillRequest{
		{SourceTopic: TopicMatchAssigned, SchemaVersion: 1, FromCheckpoint: "1", ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 2, FromCheckpoint: "1", ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: "room.gameplay.metrics", SchemaVersion: 1, FromCheckpoint: "1", ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1, FromCheckpoint: "1"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1, ToCheckpoint: "10"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1, FromOccurredAt: "2026-01-01T00:00:00Z"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1, FromCheckpoint: "10", ToCheckpoint: "1"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
			FromOccurredAt: "2026-02-01T00:00:00Z", ToOccurredAt: "2026-01-01T00:00:00Z"},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
			FromCheckpoint: "1", ToCheckpoint: "10", Limit: 1001},
		{RecoveryJobID: "job-1", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
			FromCheckpoint: "1", ToCheckpoint: "10", Limit: -1},
	}
	for i, req := range cases {
		_, err := svc.AnalyticsBackfill(context.Background(), req)
		if !errors.Is(err, ErrAnalyticsBackfillBadRequest) {
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
		mem.Append(TopicMatchAssigned, "TournamentMatchAssigned", assignedEnvelope(id, ts), &ts)
	}
	resp, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-lim", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "1000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != AnalyticsBackfillDefaultLimit {
		t.Fatalf("default limit: got %d want %d", len(resp.Records), AnalyticsBackfillDefaultLimit)
	}
	if resp.NextCursor == "" {
		t.Fatal("expected nextCursor for full default page")
	}

	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-lim", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "1000", Limit: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-lim", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "1000", Limit: 1001,
	})
	if !errors.Is(err, ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("limit 1001: %v", err)
	}
}

func TestAnalyticsBackfill_ExactlyFullFinalPageHasNoCursor(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		ts := at.Add(time.Duration(i) * time.Second)
		mem.Append(TopicMatchAssigned, "TournamentMatchAssigned",
			assignedEnvelope(fmt.Sprintf("exact-%d", i), ts), &ts)
	}
	resp, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-exact", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
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
		id := mem.Append(TopicMatchAssigned, "TournamentMatchAssigned",
			assignedEnvelope(fmt.Sprintf("e%d", i), ts), &ts)
		ids = append(ids, id)
	}
	job := "job-page"
	fromCP, toCP := fmt.Sprintf("%d", ids[0]), fmt.Sprintf("%d", ids[len(ids)-1])
	first, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.NextCursor == "" {
		t.Fatalf("first=%+v", first)
	}
	if first.FromCheckpoint != fromCP || first.ToCheckpoint == "" {
		t.Fatalf("honest coverage first: from=%s to=%s", first.FromCheckpoint, first.ToCheckpoint)
	}

	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "other-job", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: first.NextCursor,
	})
	if !errors.Is(err, ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("job binding: %v", err)
	}
	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: TopicMatchResultRecorded, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: first.NextCursor,
	})
	if !errors.Is(err, ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("topic binding: %v", err)
	}

	seen := map[string]struct{}{}
	for _, r := range first.Records {
		var m map[string]any
		_ = json.Unmarshal(r, &m)
		seen[m["eventId"].(string)] = struct{}{}
	}
	second, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: first.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range second.Records {
		var m map[string]any
		_ = json.Unmarshal(r, &m)
		eid := m["eventId"].(string)
		if _, ok := seen[eid]; ok {
			t.Fatalf("duplicate %s", eid)
		}
		seen[eid] = struct{}{}
	}
	third, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 2, Cursor: second.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range third.Records {
		var m map[string]any
		_ = json.Unmarshal(r, &m)
		eid := m["eventId"].(string)
		if _, ok := seen[eid]; ok {
			t.Fatalf("duplicate %s", eid)
		}
		seen[eid] = struct{}{}
	}
	if len(seen) != 5 || third.NextCursor != "" {
		t.Fatalf("seen=%d next3=%q", len(seen), third.NextCursor)
	}

	before := mem.Count()
	_, _ = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: job, SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fromCP, ToCheckpoint: toCP, Limit: 100,
	})
	if mem.Count() != before {
		t.Fatal("backfill must not mutate outbox")
	}
}

func TestAnalyticsBackfill_AllFiveTopicMappings(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		topic, eventType string
		payload          json.RawMessage
	}{
		{TopicMatchAssigned, "TournamentMatchAssigned", assignedEnvelope("a1", at)},
		{TopicMatchResultRecorded, "TournamentMatchResultRecorded", resultRecordedEnvelope("r1", at)},
		{TopicPlayersAdvanced, "PlayersAdvanced", playersAdvancedEnvelope("p1", at)},
		{TopicRoundCompleted, "TournamentRoundCompleted", roundCompletedEnvelope("rc1", at)},
		{TopicTournamentCompleted, "TournamentCompleted", tournamentCompletedEnvelope("tc1", at)},
	}
	for _, tc := range cases {
		id := mem.Append(tc.topic, tc.eventType, tc.payload, &at)
		resp, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
			RecoveryJobID: "job-" + tc.topic, SourceTopic: tc.topic, SchemaVersion: 1,
			FromCheckpoint: fmt.Sprintf("%d", id), ToCheckpoint: fmt.Sprintf("%d", id),
		})
		if err != nil || len(resp.Records) != 1 {
			t.Fatalf("%s: err=%v records=%d", tc.topic, err, len(resp.Records))
		}
	}
}

func TestAnalyticsBackfill_TopicAllowlistAndCorruptPrivate(t *testing.T) {
	svc, mem := newBackfillSvc(t)
	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	id := mem.Append(TopicMatchAssigned, "TournamentMatchAssigned", assignedEnvelope("ok-1", at), &at)
	_, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-t", SourceTopic: "ranking.player_rating_updated", SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "9",
	})
	if !errors.Is(err, ErrAnalyticsBackfillBadRequest) {
		t.Fatalf("allowlist: %v", err)
	}

	ok, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-t", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", id), ToCheckpoint: fmt.Sprintf("%d", id),
	})
	if err != nil || len(ok.Records) != 1 {
		t.Fatalf("assigned topic: %v %#v", err, ok)
	}

	bad := mem.Append(TopicMatchAssigned, "TournamentMatchAssigned", json.RawMessage(`{
		"eventId":"bad","eventType":"TournamentMatchAssigned","schemaVersion":1,
		"correlationId":"c","occurredAt":"2026-07-01T00:00:00Z",
		"tournamentId":"t1","roundNumber":1,"slotId":"slot_0","roomId":"room_1","hand":["r1"]
	}`), &at)
	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-priv", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", bad), ToCheckpoint: fmt.Sprintf("%d", bad),
	})
	if !errors.Is(err, ErrAnalyticsBackfillCorrupt) {
		t.Fatalf("private field: %v", err)
	}

	corrupt := mem.Append(TopicMatchAssigned, "TournamentMatchAssigned", json.RawMessage(`{"eventId":"x"}`), &at)
	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-bad", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", corrupt), ToCheckpoint: fmt.Sprintf("%d", corrupt),
	})
	if !errors.Is(err, ErrAnalyticsBackfillCorrupt) {
		t.Fatalf("corrupt: %v", err)
	}

	wrongType := mem.AppendCorrupt(TopicMatchAssigned, "PlayersAdvanced", 1, assignedEnvelope("wt", at), &at)
	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-et", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", wrongType), ToCheckpoint: fmt.Sprintf("%d", wrongType),
	})
	if !errors.Is(err, ErrAnalyticsBackfillCorrupt) {
		t.Fatalf("event_type column: %v", err)
	}

	wrongSchema := mem.AppendCorrupt(TopicMatchAssigned, "TournamentMatchAssigned", 99, assignedEnvelope("ws", at), &at)
	_, err = svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "job-sv", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: fmt.Sprintf("%d", wrongSchema), ToCheckpoint: fmt.Sprintf("%d", wrongSchema),
	})
	if !errors.Is(err, ErrAnalyticsBackfillCorrupt) {
		t.Fatalf("schema_version column: %v", err)
	}
}

func TestAnalyticsBackfill_UnavailableWithoutReader(t *testing.T) {
	svc := NewService(ServiceDeps{
		Repo: NewMemoryTournamentRepository(), Rooms: NoopRoomProvisioner{},
		Publisher: NoopPublisher{}, Audit: NewMemoryAudit(),
	})
	_, err := svc.AnalyticsBackfill(context.Background(), AnalyticsBackfillRequest{
		RecoveryJobID: "j", SourceTopic: TopicMatchAssigned, SchemaVersion: 1,
		FromCheckpoint: "1", ToCheckpoint: "2",
	})
	if !errors.Is(err, ErrAnalyticsBackfillUnavailable) {
		t.Fatalf("got %v", err)
	}
}
