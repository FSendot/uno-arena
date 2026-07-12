package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/spectator-view/domain"
)

func canonicalSpectatorSafeJSON(mut ...func(map[string]any)) []byte {
	base := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	m := map[string]any{
		"schemaVersion":  1,
		"eventId":        "evt-ss-1",
		"eventType":      "RoomCreated",
		"correlationId":  "corr-ss-1",
		"causationId":    "cause-ss-1",
		"occurredAt":     base.Format(time.RFC3339Nano),
		"roomId":         "room-42",
		"sequenceNumber": 1,
		"payload": map[string]any{
			"visibility": "public",
			"status":     "waiting",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			},
		},
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

func roomRec(offset int64, value []byte) ConsumerRecord {
	return ConsumerRecord{
		Topic: DefaultSpectatorSafeTopic, Partition: 0, Offset: offset,
		Key: []byte("room-42"), Value: value,
	}
}

func TestParseSpectatorSafeRecord_NestedPayloadMapping(t *testing.T) {
	parsed, evt, err := ParseSpectatorSafeRecord(canonicalSpectatorSafeJSON())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.SchemaVersion != 1 || parsed.EventID != "evt-ss-1" || parsed.EventType != "RoomCreated" {
		t.Fatalf("metadata: %+v", parsed)
	}
	if parsed.CorrelationID != "corr-ss-1" || parsed.RoomID != "room-42" || parsed.Sequence != 1 {
		t.Fatalf("ids: %+v", parsed)
	}
	if evt.Payload == nil || evt.Payload["visibility"] != "public" {
		t.Fatalf("domain payload: %+v", evt.Payload)
	}
	if _, ok := evt.Payload["eventId"]; ok {
		t.Fatal("must not pass envelope fields into domain payload")
	}
	if _, ok := evt.Payload["roomId"]; ok {
		t.Fatal("must not pass envelope roomId into domain payload")
	}
}

func TestParseSpectatorSafeRecord_PrivateFieldRejectedByDomain(t *testing.T) {
	_, evt, err := ParseSpectatorSafeRecord(canonicalSpectatorSafeJSON(func(m map[string]any) {
		m["payload"] = map[string]any{
			"visibility":  "public",
			"privateHand": []any{"r1"},
		}
	}))
	if err != nil {
		t.Fatalf("parse must succeed; domain drops: %v", err)
	}
	p := domain.NewSpectatorRoomProjection(domain.RoomID("room-42"))
	out := p.Apply(evt)
	if out.Kind != domain.OutcomeDropped {
		t.Fatalf("kind=%s want dropped", out.Kind)
	}
}

func TestParseSpectatorSafeRecord_UnoWindowMergeAndConflict(t *testing.T) {
	uno := map[string]any{
		"playerId":        "p1",
		"openingSequence": 3,
		"expiresAt":       time.Date(2026, 7, 11, 16, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	t.Run("merge_when_absent", func(t *testing.T) {
		_, evt, err := ParseSpectatorSafeRecord(canonicalSpectatorSafeJSON(func(m map[string]any) {
			m["eventType"] = "UnoWindowOpened"
			m["sequenceNumber"] = 3
			m["unoWindow"] = uno
			m["payload"] = map[string]any{"playerId": "p1"}
		}))
		if err != nil {
			t.Fatal(err)
		}
		got, ok := evt.Payload["unoWindow"].(map[string]any)
		if !ok || got["playerId"] != "p1" {
			t.Fatalf("merged unoWindow: %+v", evt.Payload)
		}
	})
	t.Run("agree_when_both", func(t *testing.T) {
		_, evt, err := ParseSpectatorSafeRecord(canonicalSpectatorSafeJSON(func(m map[string]any) {
			m["eventType"] = "UnoWindowOpened"
			m["sequenceNumber"] = 3
			m["unoWindow"] = uno
			m["payload"] = map[string]any{"unoWindow": cloneStringAnyMap(uno)}
		}))
		if err != nil {
			t.Fatal(err)
		}
		if evt.Payload["unoWindow"] == nil {
			t.Fatal("expected nested unoWindow preserved")
		}
	})
	t.Run("conflict", func(t *testing.T) {
		conflict := cloneStringAnyMap(uno)
		conflict["playerId"] = "p2"
		_, _, err := ParseSpectatorSafeRecord(canonicalSpectatorSafeJSON(func(m map[string]any) {
			m["unoWindow"] = uno
			m["payload"] = map[string]any{"unoWindow": conflict}
		}))
		if err == nil || !IsTerminalKafkaConsumeError(err) {
			t.Fatalf("want terminal conflict, got %v", err)
		}
	})
	t.Run("top_level_not_object", func(t *testing.T) {
		_, _, err := ParseSpectatorSafeRecord(canonicalSpectatorSafeJSON(func(m map[string]any) {
			m["unoWindow"] = "bad"
		}))
		if err == nil || !IsTerminalKafkaConsumeError(err) {
			t.Fatalf("want terminal, got %v", err)
		}
	})
}

func TestParseSpectatorSafeRecord_RequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"missing_schema", func(m map[string]any) { delete(m, "schemaVersion") }},
		{"bad_schema", func(m map[string]any) { m["schemaVersion"] = 2 }},
		{"schema_string", func(m map[string]any) { m["schemaVersion"] = "1" }},
		{"missing_event_id", func(m map[string]any) { delete(m, "eventId") }},
		{"missing_event_type", func(m map[string]any) { delete(m, "eventType") }},
		{"missing_corr", func(m map[string]any) { delete(m, "correlationId") }},
		{"missing_occurred", func(m map[string]any) { delete(m, "occurredAt") }},
		{"bad_occurred", func(m map[string]any) { m["occurredAt"] = "nope" }},
		{"missing_room", func(m map[string]any) { delete(m, "roomId") }},
		{"missing_seq", func(m map[string]any) { delete(m, "sequenceNumber") }},
		{"seq_zero", func(m map[string]any) { m["sequenceNumber"] = 0 }},
		{"seq_float", func(m map[string]any) { m["sequenceNumber"] = 1.5 }},
		{"missing_payload", func(m map[string]any) { delete(m, "payload") }},
		{"payload_array", func(m map[string]any) { m["payload"] = []any{} }},
		{"invalid_json", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw []byte
			if tc.name == "invalid_json" {
				raw = []byte(`{not-json`)
			} else {
				raw = canonicalSpectatorSafeJSON(tc.mut)
			}
			_, _, err := ParseSpectatorSafeRecord(raw)
			if err == nil || !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal parse error, got %v", err)
			}
		})
	}
}

type fakeHandler struct {
	mu      sync.Mutex
	calls   []domain.SpectatorSafeEvent
	fn      func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error)
	blockCh chan struct{}
	entered chan struct{}
}

func (f *fakeHandler) Apply(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
	_ = roomID
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.blockCh != nil {
		select {
		case <-f.blockCh:
		case <-ctx.Done():
			return domain.ApplyOutcome{}, ctx.Err()
		}
	}
	f.mu.Lock()
	if len(events) > 0 {
		f.calls = append(f.calls, events[0])
	}
	f.mu.Unlock()
	if f.fn != nil {
		return f.fn(ctx, roomID, events)
	}
	return domain.ApplyOutcome{Kind: domain.OutcomeAccepted, EventID: events[0].EventID, Sequence: events[0].Sequence}, nil
}

type fakeSource struct {
	mu       sync.Mutex
	queue    [][]ConsumerRecord
	commits  []ConsumerRecord
	pollErr  error
	commitFn func(rec ConsumerRecord) error
	pollFn   func(ctx context.Context) ([]ConsumerRecord, error)
}

func (f *fakeSource) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	if f.pollFn != nil {
		return f.pollFn(ctx)
	}
	if f.pollErr != nil {
		return nil, f.pollErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
			return nil, nil
		}
	}
	batch := f.queue[0]
	f.queue = f.queue[1:]
	return batch, nil
}

func (f *fakeSource) Commit(ctx context.Context, rec ConsumerRecord) error {
	_ = ctx
	if f.commitFn != nil {
		if err := f.commitFn(rec); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.commits = append(f.commits, rec)
	f.mu.Unlock()
	return nil
}

func (f *fakeSource) Close() error { return nil }

func (f *fakeSource) committedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.commits))
	for i, c := range f.commits {
		out[i] = c.Offset
	}
	return out
}

type fakeDLQ struct {
	mu       sync.Mutex
	pubs     []dlqPublication
	failOnce atomic.Bool
	failN    atomic.Int32
	failErr  error
}

type dlqPublication struct {
	Original ConsumerRecord
	Meta     DLQFailureMeta
}

func (f *fakeDLQ) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	_ = ctx
	if f.failN.Load() > 0 {
		f.failN.Add(-1)
		if f.failErr != nil {
			return f.failErr
		}
		return errors.New("dlq publish failed")
	}
	if f.failOnce.Load() {
		f.failOnce.Store(false)
		if f.failErr != nil {
			return f.failErr
		}
		return errors.New("dlq publish failed")
	}
	f.mu.Lock()
	f.pubs = append(f.pubs, dlqPublication{Original: original, Meta: meta})
	f.mu.Unlock()
	return nil
}

func (f *fakeDLQ) publications() []dlqPublication {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]dlqPublication, len(f.pubs))
	copy(out, f.pubs)
	return out
}

type fakeQuarantine struct {
	mu       sync.Mutex
	active   map[string]bool
	records  []AggregateQuarantineRecord
	failOnce atomic.Bool
}

func (f *fakeQuarantine) key(group, topic, agg string) string {
	return group + "|" + topic + "|" + agg
}

func (f *fakeQuarantine) IsQuarantined(ctx context.Context, consumerGroup, sourceTopic, aggregateKey string) (bool, error) {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active == nil {
		return false, nil
	}
	return f.active[f.key(consumerGroup, sourceTopic, aggregateKey)], nil
}

func (f *fakeQuarantine) Quarantine(ctx context.Context, rec AggregateQuarantineRecord) error {
	_ = ctx
	if f.failOnce.Load() {
		f.failOnce.Store(false)
		return errors.New("quarantine persist failed")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active == nil {
		f.active = map[string]bool{}
	}
	f.active[f.key(rec.ConsumerGroup, rec.SourceTopic, rec.AggregateKey)] = true
	f.records = append(f.records, rec)
	return nil
}

func (f *fakeQuarantine) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func newTestConsumer(src *fakeSource, dlq *fakeDLQ, h *fakeHandler) *SpectatorSafeKafkaConsumer {
	return &SpectatorSafeKafkaConsumer{
		source:  src,
		dlq:     dlq,
		handler: h,
		cfg: SpectatorSafeKafkaConfig{
			Group:               DefaultSpectatorKafkaGroup,
			Topic:               DefaultSpectatorSafeTopic,
			DLQTopic:            DefaultSpectatorSafeDLQTopic,
			MaxAttempts:         3,
			RetryBackoff:        time.Millisecond,
			MaxPartitionWorkers: 4,
		},
		clock: fixedClock{now: time.Date(2026, 7, 11, 16, 0, 0, 0, time.UTC)},
		sleep: func(ctx context.Context, d time.Duration) error {
			_ = d
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		},
	}
}

func TestSpectatorSafeConsumer_AcceptedDuplicateIgnoredCommit(t *testing.T) {
	for _, kind := range []domain.OutcomeKind{domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeIgnored} {
		t.Run(string(kind), func(t *testing.T) {
			src := &fakeSource{}
			dlq := &fakeDLQ{}
			h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
				return domain.ApplyOutcome{Kind: kind, EventID: events[0].EventID, Sequence: events[0].Sequence}, nil
			}}
			c := newTestConsumer(src, dlq, h)
			if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(10, canonicalSpectatorSafeJSON())}); err != nil {
				t.Fatal(err)
			}
			if got := src.committedOffsets(); len(got) != 1 || got[0] != 10 {
				t.Fatalf("commits=%v", got)
			}
			if len(dlq.publications()) != 0 {
				t.Fatalf("unexpected dlq for %s", kind)
			}
		})
	}
}

func TestSpectatorSafeConsumer_DomainDroppedQuarantinesThenDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
		return domain.ApplyOutcome{
			Kind: domain.OutcomeDropped, EventID: events[0].EventID,
			Rejection: &domain.Rejection{Code: domain.RejectForbiddenField, Message: "forbidden private field: privateHand"},
		}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(7, canonicalSpectatorSafeJSON())}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailurePrivacyViolation {
		t.Fatalf("pubs=%+v", pubs)
	}
	// Apply already marked quarantine atomically; consumer must not re-persist.
	if q.count() != 0 {
		t.Fatalf("domain drop must not re-persist quarantine, got %d", q.count())
	}
	if got := src.committedOffsets(); len(got) != 1 {
		t.Fatalf("commits=%v", got)
	}
}

func TestSpectatorSafeConsumer_DomainQuarantinedDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
		return domain.ApplyOutcome{
			Kind: domain.OutcomeQuarantined, EventID: events[0].EventID,
			Rejection: &domain.Rejection{Code: domain.RejectOutOfOrderSequence, Message: "gap"},
		}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(8, canonicalSpectatorSafeJSON())}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureApplication {
		t.Fatalf("pubs=%+v", pubs)
	}
}

func TestSpectatorSafeConsumer_AlreadyQuarantinedSkipsDomain(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{active: map[string]bool{
		DefaultSpectatorKafkaGroup + "|" + DefaultSpectatorSafeTopic + "|room-42": true,
	}}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
		attempts.Add(1)
		return domain.ApplyOutcome{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(1, canonicalSpectatorSafeJSON())}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("already quarantined path must skip domain apply")
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureAggregateQuarantined {
		t.Fatalf("pubs=%+v", pubs)
	}
	if q.count() != 0 {
		t.Fatal("already quarantined path must not re-persist quarantine before DLQ")
	}
}

func TestSpectatorSafeConsumer_KeyMismatch(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	rec := roomRec(2, canonicalSpectatorSafeJSON())
	rec.Key = []byte("other-room")
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(h.calls) != 0 {
		t.Fatal("handler must not run on key mismatch")
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureSchemaInvalid {
		t.Fatalf("pubs=%+v", pubs)
	}
	if q.count() != 1 {
		t.Fatal("key mismatch must quarantine before DLQ")
	}
}

func TestSpectatorSafeConsumer_RetryThenSuccess(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
		n := attempts.Add(1)
		if n < 3 {
			return domain.ApplyOutcome{}, errors.New("redis temporarily unavailable")
		}
		return domain.ApplyOutcome{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(3, canonicalSpectatorSafeJSON())}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}

func TestSpectatorSafeConsumer_RetryExhaustionPublishesDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
		return domain.ApplyOutcome{}, errors.New("connection reset by peer")
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(99, canonicalSpectatorSafeJSON())}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 {
		t.Fatalf("dlq pubs=%d", len(pubs))
	}
	if string(pubs[0].Original.Key) != "room-42" {
		t.Fatal("dlq must preserve original key")
	}
	if q.count() != 1 {
		t.Fatalf("quarantine records=%d", q.count())
	}
}

func TestSpectatorSafeConsumer_DLQFailureDoesNotCommit(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{failErr: errors.New("broker reject")}
	dlq.failOnce.Store(true)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultSpectatorSafeTopic, Partition: 2, Offset: 44,
		Key: []byte("room-42"), Value: []byte(`not-json`),
	}})
	if err == nil {
		t.Fatal("expected dlq failure")
	}
	if got := src.committedOffsets(); len(got) != 0 {
		t.Fatalf("must not commit on dlq failure, commits=%v", got)
	}
}

func TestSpectatorSafeConsumer_RunRetainsBatchOnCommitFailure(t *testing.T) {
	var commitAttempts atomic.Int32
	src := &fakeSource{
		queue: [][]ConsumerRecord{{roomRec(11, canonicalSpectatorSafeJSON())}},
		commitFn: func(rec ConsumerRecord) error {
			if commitAttempts.Add(1) == 1 {
				return errors.New("commit temporarily failed")
			}
			return nil
		},
	}
	dlq := &fakeDLQ{}
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		if len(src.committedOffsets()) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("commit never succeeded")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop")
	}
	if commitAttempts.Load() < 2 {
		t.Fatalf("expected retained retry, attempts=%d", commitAttempts.Load())
	}
}

func TestSpectatorSafeConsumer_PartitionOrderingAndParallelism(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var mu sync.Mutex
	order := map[int32][]string{}
	gate := make(chan struct{})
	enteredFirst := make(chan struct{}, 1)
	var startedP1 atomic.Bool
	h := &fakeHandler{fn: func(ctx context.Context, roomID domain.RoomID, events []domain.SpectatorSafeEvent) (domain.ApplyOutcome, error) {
		id := string(events[0].EventID)
		part := int32(0)
		if strings.HasPrefix(id, "p1-") {
			part = 1
			startedP1.Store(true)
		}
		if id == "p0-first" {
			select {
			case enteredFirst <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return domain.ApplyOutcome{}, ctx.Err()
			}
		}
		mu.Lock()
		order[part] = append(order[part], id)
		mu.Unlock()
		return domain.ApplyOutcome{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	recs := []ConsumerRecord{
		{Topic: DefaultSpectatorSafeTopic, Partition: 0, Offset: 1, Key: []byte("room-42"), Value: canonicalSpectatorSafeJSON(func(m map[string]any) { m["eventId"] = "p0-first" })},
		{Topic: DefaultSpectatorSafeTopic, Partition: 0, Offset: 2, Key: []byte("room-42"), Value: canonicalSpectatorSafeJSON(func(m map[string]any) { m["eventId"] = "p0-second"; m["sequenceNumber"] = 2 })},
		{Topic: DefaultSpectatorSafeTopic, Partition: 1, Offset: 1, Key: []byte("room-99"), Value: canonicalSpectatorSafeJSON(func(m map[string]any) {
			m["eventId"] = "p1-a"
			m["roomId"] = "room-99"
		})},
	}
	done := make(chan error, 1)
	go func() { done <- c.ProcessBatch(context.Background(), recs) }()
	select {
	case <-enteredFirst:
	case <-time.After(2 * time.Second):
		t.Fatal("first record did not start")
	}
	deadline := time.After(2 * time.Second)
	for !startedP1.Load() {
		select {
		case <-deadline:
			t.Fatal("unrelated partition did not progress")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("process hung")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order[0]) != 2 || order[0][0] != "p0-first" || order[0][1] != "p0-second" {
		t.Fatalf("partition 0 order=%v", order[0])
	}
}

func TestLoadSpectatorSafeKafkaConfig_DefaultsAndFailClosed(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "")
		_, enabled, err := LoadSpectatorSafeKafkaConfigFromEnv()
		if err != nil || enabled {
			t.Fatal("empty brokers must disable consumer")
		}
	})
	t.Run("defaults", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		os.Unsetenv("KAFKA_CONSUMER_GROUP")
		os.Unsetenv("KAFKA_SPECTATOR_SAFE_TOPIC")
		os.Unsetenv("KAFKA_SPECTATOR_SAFE_DLQ_TOPIC")
		cfg, enabled, err := LoadSpectatorSafeKafkaConfigFromEnv()
		if err != nil || !enabled {
			t.Fatalf("enabled=%v err=%v", enabled, err)
		}
		if cfg.Group != DefaultSpectatorKafkaGroup || cfg.Topic != DefaultSpectatorSafeTopic || cfg.DLQTopic != DefaultSpectatorSafeDLQTopic {
			t.Fatalf("cfg=%+v", cfg)
		}
	})
	t.Run("blank_group", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", "  ")
		if _, _, err := LoadSpectatorSafeKafkaConfigFromEnv(); err == nil {
			t.Fatal("expected fail closed")
		}
	})
}

func TestWireSpectatorRuntime_CapabilityIgnoresKafka(t *testing.T) {
	t.Setenv("SPECTATOR_CAPABILITY_MODE", "1")
	t.Setenv("DEPLOYMENT_ENV", "development")
	t.Setenv("SPECTATOR_VIEW_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("REDIS_URL", "")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	rt, err := wireSpectatorRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.kafka != nil {
		t.Fatal("capability mode must remain offline (no kafka consumer)")
	}
}

func TestWireSpectatorRuntime_DurableRequiresKafka(t *testing.T) {
	t.Setenv("SPECTATOR_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "development")
	t.Setenv("SPECTATOR_VIEW_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
	t.Setenv("KAFKA_BROKERS", "")
	rt, err := wireSpectatorRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "durable" || rt.ready {
		t.Fatalf("want durable not ready without kafka: mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.reason)
	}
	if !strings.Contains(rt.reason, "kafka") {
		t.Fatalf("reason=%q", rt.reason)
	}
	if !strings.Contains(rt.reason, "durable_dependencies_missing") {
		t.Fatalf("reason=%q", rt.reason)
	}
	if rt.kafka != nil {
		t.Fatal("missing kafka must not start a consumer")
	}
}

func TestWireSpectatorRuntime_CapabilityIgnoresMalformedKafka(t *testing.T) {
	t.Setenv("SPECTATOR_CAPABILITY_MODE", "1")
	t.Setenv("DEPLOYMENT_ENV", "development")
	t.Setenv("SPECTATOR_VIEW_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("REDIS_URL", "")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "  ")
	rt, err := wireSpectatorRuntime()
	if err != nil {
		t.Fatalf("capability must ignore malformed kafka env: %v", err)
	}
	if rt.mode != "capability" || rt.kafka != nil {
		t.Fatalf("mode=%s kafka=%v", rt.mode, rt.kafka != nil)
	}
}

func TestSpectatorSafeKafkaLifecycle_UnhealthyAfterUnexpectedStop(t *testing.T) {
	life := &spectatorSafeKafkaLifecycle{}
	life.healthy.Store(true)
	life.healthy.Store(false)
	if life.Healthy() {
		t.Fatal("expected unhealthy")
	}
}

func TestSpectatorRuntime_DurableReadyFailsWhenKafkaUnhealthy(t *testing.T) {
	life := &spectatorSafeKafkaLifecycle{}
	life.healthy.Store(false)
	rt := spectatorRuntime{
		app:   NewMemoryProjectionStore(),
		mode:  "durable",
		ready: true,
		kafka: life,
	}
	err := rt.durableReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "kafka_consumer_stopped") {
		t.Fatalf("want kafka_consumer_stopped, got %v", err)
	}
}

func TestClassifyKafkaConsumeError(t *testing.T) {
	term := &kafkaConsumeError{class: KafkaFailureSchemaInvalid, terminal: true, err: errors.New("bad")}
	if !IsTerminalKafkaConsumeError(term) {
		t.Fatal("expected terminal")
	}
	if ClassifyKafkaConsumeError(errors.New("timeout dialing redis")) != KafkaFailureDependency {
		t.Fatal("expected dependency")
	}
}
