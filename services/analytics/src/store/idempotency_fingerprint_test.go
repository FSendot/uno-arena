package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"unoarena/services/analytics/domain"
)

func TestApply_TopicIdempotencyFingerprint(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	s := &AnalyticsStore{client: ch}
	ctx := context.Background()

	evt := domain.UpstreamEvent{
		EventID: "evt-fp-1", EventType: domain.EventGameplayMetric,
		Source: domain.SourceRoomGameplayMetrics, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c1", OccurredAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: "evt-fp-1",
		Payload: map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	}
	domain.EnsureIngressIdentity(&evt)

	out, err := s.Apply(ctx, evt)
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("first apply: %+v err=%v", out, err)
	}

	dup := evt
	out2, err := s.Apply(ctx, dup)
	if err != nil || out2.Kind != domain.OutcomeDuplicate {
		t.Fatalf("same fingerprint: %+v err=%v", out2, err)
	}

	conflict := evt
	conflict.Payload = map[string]any{
		"visibility": "anonymized_adhoc", "metricType": "different", "roomId": "room_1",
	}
	conflict.PayloadFingerprint = domain.FingerprintPayload(conflict.Payload)
	out3, err := s.Apply(ctx, conflict)
	if err != nil {
		t.Fatal(err)
	}
	if out3.Kind != domain.OutcomeQuarantined || out3.Rejection == nil || out3.Rejection.Code != domain.RejectPayloadConflict {
		t.Fatalf("conflict: %+v", out3)
	}
	// First-wins: processed map still has one marker for the contract key.
	ch.mu.Lock()
	n := 0
	for k := range ch.processed {
		if strings.Contains(k, string(domain.SourceRoomGameplayMetrics)) && strings.Contains(k, "evt-fp-1") {
			n++
		}
	}
	ch.mu.Unlock()
	if n != 1 {
		t.Fatalf("want single first-wins marker, got %d", n)
	}
}

func TestApply_IgnoredDispositionPersisted(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	s := &AnalyticsStore{client: ch}
	ctx := context.Background()

	evt := domain.UpstreamEvent{
		EventID: "evt-ign-1", EventType: domain.EventTournamentStatistic,
		Source: domain.SourceRoomMatchCompleted, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c1", OccurredAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: "room-1|1", DurableIgnore: true,
		Payload: map[string]any{"roomId": "room-1", "reason": "adhoc"},
	}
	domain.EnsureIngressIdentity(&evt)
	out, err := s.Apply(ctx, evt)
	if err != nil || out.Kind != domain.OutcomeIgnored {
		t.Fatalf("ignored: %+v err=%v", out, err)
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	found := false
	for k, raw := range ch.processed {
		if strings.Contains(k, "room-1|1") {
			found = true
			if !strings.Contains(raw, `"kind":"ignored"`) {
				t.Fatalf("marker=%s", raw)
			}
		}
	}
	if !found {
		t.Fatal("ignored marker missing")
	}
}

func TestApply_ConflictRecordsIngestionConflictsBeforeReturn(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	s := &AnalyticsStore{client: ch}
	ctx := context.Background()

	evt := domain.UpstreamEvent{
		EventID: "evt-fp-1", EventType: domain.EventGameplayMetric,
		Source: domain.SourceRoomGameplayMetrics, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c1", OccurredAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: "evt-fp-1",
		Payload: map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	}
	domain.EnsureIngressIdentity(&evt)
	if _, err := s.Apply(ctx, evt); err != nil {
		t.Fatal(err)
	}

	conflict := evt
	conflict.EventID = "evt-fp-2"
	conflict.Payload = map[string]any{
		"visibility": "anonymized_adhoc", "metricType": "different", "roomId": "room_1",
	}
	conflict.PayloadFingerprint = domain.FingerprintPayload(conflict.Payload)
	out, err := s.Apply(ctx, conflict)
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != domain.OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != domain.RejectPayloadConflict {
		t.Fatalf("conflict: %+v", out)
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.conflicts) != 1 {
		t.Fatalf("want 1 conflict row, got %d (%v)", len(ch.conflicts), ch.conflicts)
	}
	for k, oj := range ch.conflicts {
		if !strings.Contains(k, conflict.PayloadFingerprint) {
			t.Fatalf("conflict key missing fingerprint: %s", k)
		}
		if !strings.Contains(oj, `"kind":"quarantined"`) {
			t.Fatalf("outcome=%s", oj)
		}
	}
	if len(ch.processed) != 1 {
		t.Fatalf("first-wins marker count=%d", len(ch.processed))
	}
}

func TestApply_ConflictWriteFailurePropagates(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	ch.failExec = func(query string, params map[string]string) error {
		if strings.Contains(strings.ToLower(query), "ingestion_conflicts") {
			return fmt.Errorf("clickhouse unavailable")
		}
		return nil
	}
	s := &AnalyticsStore{client: ch}
	ctx := context.Background()

	evt := domain.UpstreamEvent{
		EventID: "evt-fail-1", EventType: domain.EventGameplayMetric,
		Source: domain.SourceRoomGameplayMetrics, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c1", OccurredAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: "evt-fail-1",
		Payload: map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	}
	domain.EnsureIngressIdentity(&evt)
	if _, err := s.Apply(ctx, evt); err != nil {
		t.Fatal(err)
	}
	conflict := evt
	conflict.EventID = "evt-fail-2"
	conflict.PayloadFingerprint = domain.FingerprintPayload(map[string]any{
		"visibility": "anonymized_adhoc", "metricType": "other", "roomId": "room_1",
	})
	_, err := s.Apply(ctx, conflict)
	if err == nil || !strings.Contains(err.Error(), "ingestion_conflicts") {
		t.Fatalf("want conflict write error, got %v", err)
	}
	if len(ch.conflicts) != 0 {
		t.Fatal("conflict must not be considered recorded on write failure")
	}
}

func TestInsertProjectionRows_IncludesSourceTopic(t *testing.T) {
	ch := newBlockingCH()
	ch.seedInitial()
	var sawTopic bool
	ch.blockExec = func(query string, params map[string]string) {
		if strings.Contains(strings.ToLower(query), "insert into gameplay_metrics") {
			if params["topic"] != string(domain.SourceRoomGameplayMetrics) {
				t.Fatalf("topic=%q", params["topic"])
			}
			sawTopic = true
		}
	}
	s := &AnalyticsStore{client: ch}
	evt := domain.UpstreamEvent{
		EventID: "evt-st-1", EventType: domain.EventGameplayMetric,
		Source: domain.SourceRoomGameplayMetrics, SchemaVersion: domain.CurrentSchemaVersion,
		CorrelationID: "c1", OccurredAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: "evt-st-1",
		Payload: map[string]any{
			"visibility": "anonymized_adhoc", "metricType": "turn_advanced", "roomId": "room_1",
		},
	}
	domain.EnsureIngressIdentity(&evt)
	if _, err := s.Apply(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	if !sawTopic {
		t.Fatal("gameplay insert must include source_topic")
	}
}
