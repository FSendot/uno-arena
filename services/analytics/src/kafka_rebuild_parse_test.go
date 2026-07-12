package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validRebuildEnvelope(mods ...func(map[string]any)) []byte {
	m := map[string]any{
		"eventId":             "evt-rebuild-1",
		"eventType":           EventTypeProjectionRebuildReq,
		"schemaVersion":       1,
		"correlationId":       "corr-1",
		"occurredAt":          "2026-07-12T15:00:00.000Z",
		"recoveryJobId":       "job-1",
		"sourceContext":       "room",
		"expectedSourceTopic": "room.gameplay.metrics",
		"fromCheckpoint":      "1",
		"toCheckpoint":        "100",
	}
	for _, mod := range mods {
		mod(m)
	}
	b, _ := json.Marshal(m)
	return b
}

func TestParseAnalyticsProjectionRebuildRequested_OK(t *testing.T) {
	parsed, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.RecoveryJobID != "job-1" || parsed.SourceContext != "room" {
		t.Fatalf("%+v", parsed)
	}
	if !parsed.HasCheckpointRange || parsed.PageCursor != "" {
		t.Fatalf("%+v", parsed)
	}
	if parsed.IdempotencyKey() != "job-1|room.gameplay.metrics|" {
		t.Fatalf("idemp=%q", parsed.IdempotencyKey())
	}
}

func TestParseAnalyticsProjectionRebuildRequested_RejectsEventArrays(t *testing.T) {
	_, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["records"] = []any{map[string]any{"eventId": "x"}}
	}))
	if err == nil || !IsTerminalKafkaConsumeError(err) {
		t.Fatalf("want terminal, got %v", err)
	}
}

func TestParseAnalyticsProjectionRebuildRequested_ContextTopicMapping(t *testing.T) {
	_, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["sourceContext"] = "ranking"
		m["expectedSourceTopic"] = "room.gameplay.metrics"
	}))
	if err == nil || !strings.Contains(err.Error(), "not valid for sourceContext") {
		t.Fatalf("got %v", err)
	}
}

func TestParseAnalyticsProjectionRebuildRequested_RequiresPairedRange(t *testing.T) {
	_, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		delete(m, "fromCheckpoint")
		delete(m, "toCheckpoint")
	}))
	if err == nil || !strings.Contains(err.Error(), "bounded paired range") {
		t.Fatalf("got %v", err)
	}
	_, err = ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		delete(m, "toCheckpoint")
	}))
	if err == nil || !strings.Contains(err.Error(), "paired") {
		t.Fatalf("got %v", err)
	}
}

func TestParseAnalyticsProjectionRebuildRequested_OccurredAtRange(t *testing.T) {
	parsed, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		delete(m, "fromCheckpoint")
		delete(m, "toCheckpoint")
		m["fromOccurredAt"] = "2026-07-01T00:00:00Z"
		m["toOccurredAt"] = "2026-07-02T00:00:00Z"
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.HasOccurredRange || parsed.ToOccurredAt.Before(parsed.FromOccurredAt) {
		t.Fatalf("%+v", parsed)
	}
}

func TestParseAnalyticsProjectionRebuildRequested_PageCursorAndAttempt(t *testing.T) {
	parsed, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["pageCursor"] = "cur-abc"
		m["attempt"] = 2
	}))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.PageCursor != "cur-abc" || parsed.Attempt != 2 {
		t.Fatalf("%+v", parsed)
	}
	if parsed.IdempotencyKey() != "job-1|room.gameplay.metrics|cur-abc" {
		t.Fatalf("idemp=%q", parsed.IdempotencyKey())
	}
}

func TestParseAnalyticsBackfillRecord_ReusesTopicParse(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"eventId": "m1", "eventType": "GameplayMetric", "schemaVersion": 1,
		"correlationId": "c", "occurredAt": time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "draw",
	})
	evt, err := ParseAnalyticsBackfillRecord("room.gameplay.metrics", raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(evt.EventID) != "m1" || string(evt.Source) != "room.gameplay.metrics" {
		t.Fatalf("%+v", evt)
	}
}

func TestEncodeFollowUpRoundTrip(t *testing.T) {
	req, err := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["pageCursor"] = "next-1"
	}))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeAnalyticsProjectionRebuildRequested(req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseAnalyticsProjectionRebuildRequested(raw)
	if err != nil {
		t.Fatal(err)
	}
	if again.PageCursor != "next-1" || again.RecoveryJobID != req.RecoveryJobID {
		t.Fatalf("%+v", again)
	}
}
