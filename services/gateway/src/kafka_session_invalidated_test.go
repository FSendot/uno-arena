package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"unoarena/services/gateway/bff/store"
)

func canonicalSessionInvalidatedJSON(mut ...func(map[string]any)) []byte {
	base := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	m := map[string]any{
		"schemaVersion": 1,
		"eventId":       "evt-si-1",
		"eventType":     "SessionInvalidated",
		"correlationId": "corr-si-1",
		"occurredAt":    base.Format(time.RFC3339Nano),
		"playerId":      "player-1",
		"sessionId":     "sess-1",
		"reason":        "superseded",
	}
	for _, fn := range mut {
		fn(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func siRec(offset int64, value []byte) ConsumerRecord {
	return ConsumerRecord{
		Topic: DefaultSessionInvalidatedTopic, Partition: 0, Offset: offset,
		Key: []byte("player-1"), Value: value,
	}
}

func TestParseSessionInvalidatedRecord_OK(t *testing.T) {
	parsed, err := ParseSessionInvalidatedRecord(canonicalSessionInvalidatedJSON())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.EventType != "SessionInvalidated" || parsed.SchemaVersion != 1 {
		t.Fatalf("%+v", parsed)
	}
	if parsed.PlayerID != "player-1" || parsed.SessionID != "sess-1" || parsed.Reason != "superseded" {
		t.Fatalf("%+v", parsed)
	}
}

func TestParseSessionInvalidatedRecord_Required(t *testing.T) {
	cases := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"bad_schema", func(m map[string]any) { m["schemaVersion"] = 2 }},
		{"bad_type", func(m map[string]any) { m["eventType"] = "Other" }},
		{"missing_player", func(m map[string]any) { delete(m, "playerId") }},
		{"missing_session", func(m map[string]any) { delete(m, "sessionId") }},
		{"missing_reason", func(m map[string]any) { delete(m, "reason") }},
		{"missing_corr", func(m map[string]any) { delete(m, "correlationId") }},
		{"bad_occurred", func(m map[string]any) { m["occurredAt"] = "nope" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSessionInvalidatedRecord(canonicalSessionInvalidatedJSON(tc.mut))
			if err == nil || !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal, got %v", err)
			}
		})
	}
}

type fakeSISource struct {
	mu      sync.Mutex
	polls   [][]ConsumerRecord
	commits []ConsumerRecord
}

func (f *fakeSISource) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.polls) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	batch := f.polls[0]
	f.polls = f.polls[1:]
	return batch, nil
}

func (f *fakeSISource) Commit(_ context.Context, rec ConsumerRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, rec)
	return nil
}

func (f *fakeSISource) Close() error { return nil }

type fakeSIDLQ struct {
	mu   sync.Mutex
	recs []ConsumerRecord
	meta []DLQFailureMeta
}

func (f *fakeSIDLQ) PublishDLQ(_ context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs = append(f.recs, original)
	f.meta = append(f.meta, meta)
	return nil
}

type fakeSIHandler struct {
	fn func(ctx context.Context, evt ParsedSessionInvalidated) (store.SessionInvalidationApplyKind, error)
}

func (f *fakeSIHandler) Apply(ctx context.Context, evt ParsedSessionInvalidated) (store.SessionInvalidationApplyKind, error) {
	if f.fn != nil {
		return f.fn(ctx, evt)
	}
	return store.SessionInvalidationAccepted, nil
}

type fakeSIQuarantine struct {
	mu          sync.Mutex
	quarantined map[string]bool
	persisted   []AggregateQuarantineRecord
}

func (f *fakeSIQuarantine) IsQuarantined(_ context.Context, _, _, aggregateKey string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.quarantined[aggregateKey], nil
}

func (f *fakeSIQuarantine) Quarantine(_ context.Context, rec AggregateQuarantineRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.quarantined == nil {
		f.quarantined = map[string]bool{}
	}
	f.quarantined[rec.AggregateKey] = true
	f.persisted = append(f.persisted, rec)
	return nil
}

func TestSessionInvalidatedConsumer_AcceptCommits(t *testing.T) {
	src := &fakeSISource{}
	dlq := &fakeSIDLQ{}
	c := &SessionInvalidatedKafkaConsumer{
		source: src, dlq: dlq, handler: &fakeSIHandler{},
		cfg:   SessionInvalidatedKafkaConfig{Group: DefaultGatewayKafkaGroup, Topic: DefaultSessionInvalidatedTopic, MaxAttempts: 3, MaxPartitionWorkers: 2},
		clock: systemClock{},
	}
	batch := []ConsumerRecord{siRec(1, canonicalSessionInvalidatedJSON())}
	if err := c.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(src.commits) != 1 {
		t.Fatalf("commits=%d", len(src.commits))
	}
	if len(dlq.recs) != 0 {
		t.Fatal("unexpected dlq")
	}
}

func TestSessionInvalidatedConsumer_KeyMustEqualPlayerID(t *testing.T) {
	src := &fakeSISource{}
	dlq := &fakeSIDLQ{}
	q := &fakeSIQuarantine{}
	c := &SessionInvalidatedKafkaConsumer{
		source: src, dlq: dlq, handler: &fakeSIHandler{}, quarantine: q,
		cfg:   SessionInvalidatedKafkaConfig{Group: DefaultGatewayKafkaGroup, Topic: DefaultSessionInvalidatedTopic, MaxAttempts: 1, MaxPartitionWorkers: 1},
		clock: systemClock{},
	}
	rec := siRec(2, canonicalSessionInvalidatedJSON())
	rec.Key = []byte("other-player")
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(dlq.recs) != 1 || dlq.meta[0].Classification != KafkaFailureSchemaInvalid {
		t.Fatalf("dlq=%+v meta=%+v", dlq.recs, dlq.meta)
	}
	if len(q.persisted) != 1 || q.persisted[0].AggregateKey != "other-player" {
		t.Fatalf("quarantine=%+v", q.persisted)
	}
}

func TestSessionInvalidatedConsumer_MissingKeyNoInventedQuarantine(t *testing.T) {
	src := &fakeSISource{}
	dlq := &fakeSIDLQ{}
	q := &fakeSIQuarantine{}
	c := &SessionInvalidatedKafkaConsumer{
		source: src, dlq: dlq, handler: &fakeSIHandler{}, quarantine: q,
		cfg:   SessionInvalidatedKafkaConfig{Group: DefaultGatewayKafkaGroup, Topic: DefaultSessionInvalidatedTopic, MaxAttempts: 1, MaxPartitionWorkers: 1},
		clock: systemClock{},
	}
	rec := siRec(3, canonicalSessionInvalidatedJSON())
	rec.Key = nil
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(dlq.recs) != 1 {
		t.Fatal("expected dlq")
	}
	if len(q.persisted) != 0 {
		t.Fatalf("must not invent quarantine from body playerId, got %+v", q.persisted)
	}
}

func TestSessionInvalidatedConsumer_ConflictToDLQ(t *testing.T) {
	src := &fakeSISource{}
	dlq := &fakeSIDLQ{}
	c := &SessionInvalidatedKafkaConsumer{
		source: src, dlq: dlq,
		handler: &fakeSIHandler{fn: func(context.Context, ParsedSessionInvalidated) (store.SessionInvalidationApplyKind, error) {
			return store.SessionInvalidationConflict, nil
		}},
		quarantine: &fakeSIQuarantine{},
		cfg:        SessionInvalidatedKafkaConfig{Group: DefaultGatewayKafkaGroup, Topic: DefaultSessionInvalidatedTopic, MaxAttempts: 2, MaxPartitionWorkers: 1},
		clock:      systemClock{},
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{siRec(4, canonicalSessionInvalidatedJSON())}); err != nil {
		t.Fatal(err)
	}
	if len(dlq.meta) != 1 || !strings.Contains(dlq.meta[0].ErrorSummary, "conflict") {
		t.Fatalf("meta=%+v", dlq.meta)
	}
}

func TestSessionInvalidatedConsumer_RestoredCommits(t *testing.T) {
	src := &fakeSISource{}
	dlq := &fakeSIDLQ{}
	c := &SessionInvalidatedKafkaConsumer{
		source: src, dlq: dlq,
		handler: &fakeSIHandler{fn: func(context.Context, ParsedSessionInvalidated) (store.SessionInvalidationApplyKind, error) {
			return store.SessionInvalidationRestored, nil
		}},
		quarantine: &fakeSIQuarantine{},
		cfg:        SessionInvalidatedKafkaConfig{Group: DefaultGatewayKafkaGroup, Topic: DefaultSessionInvalidatedTopic, MaxAttempts: 1, MaxPartitionWorkers: 1},
		clock:      systemClock{},
	}
	rec := siRec(6, canonicalSessionInvalidatedJSON())
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(src.commits) != 1 {
		t.Fatalf("restored must commit, got %d", len(src.commits))
	}
	if len(dlq.recs) != 0 {
		t.Fatal("restored must not DLQ")
	}
}

func TestSessionInvalidatedConsumer_RetainsBatchOnHandlerError(t *testing.T) {
	src := &fakeSISource{}
	dlq := &fakeSIDLQ{}
	calls := 0
	c := &SessionInvalidatedKafkaConsumer{
		source: src, dlq: dlq,
		handler: &fakeSIHandler{fn: func(context.Context, ParsedSessionInvalidated) (store.SessionInvalidationApplyKind, error) {
			calls++
			return "", errors.New("redis unavailable")
		}},
		cfg: SessionInvalidatedKafkaConfig{
			Group: DefaultGatewayKafkaGroup, Topic: DefaultSessionInvalidatedTopic,
			MaxAttempts: 2, MaxPartitionWorkers: 1, RetryBackoff: time.Millisecond,
		},
		clock: systemClock{},
		sleep: func(context.Context, time.Duration) error { return nil },
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{siRec(5, canonicalSessionInvalidatedJSON())}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
	if len(dlq.recs) != 1 || dlq.meta[0].Classification != KafkaFailureDependency {
		t.Fatalf("meta=%+v", dlq.meta)
	}
}

func TestLoadSessionInvalidatedKafkaConfig_DisabledWhenEmpty(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	_, enabled, err := LoadSessionInvalidatedKafkaConfigFromEnv()
	if err != nil || enabled {
		t.Fatalf("enabled=%v err=%v", enabled, err)
	}
}

func TestLoadSessionInvalidatedKafkaConfig_Defaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	cfg, enabled, err := LoadSessionInvalidatedKafkaConfigFromEnv()
	if err != nil || !enabled {
		t.Fatal(err)
	}
	if cfg.Group != DefaultGatewayKafkaGroup || cfg.Topic != DefaultSessionInvalidatedTopic || cfg.DLQTopic != DefaultSessionInvalidatedDLQTopic {
		t.Fatalf("%+v", cfg)
	}
}

func TestTrustworthyPlayerAggregateKey(t *testing.T) {
	if trustworthyPlayerAggregateKey("") != "" {
		t.Fatal("empty")
	}
	if trustworthyPlayerAggregateKey("bad:key") != "" {
		t.Fatal("malformed")
	}
	if trustworthyPlayerAggregateKey("player-1") != "player-1" {
		t.Fatal("ok")
	}
}
