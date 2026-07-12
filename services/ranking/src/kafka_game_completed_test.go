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

	"unoarena/services/ranking/domain"
)

func canonicalGameCompletedJSON(mut ...func(map[string]any)) []byte {
	base := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	m := map[string]any{
		"schemaVersion":  1,
		"eventId":        "evt-gc-1",
		"eventType":      "GameCompleted",
		"correlationId":  "corr-gc-1",
		"causationId":    "cmd-gc-1",
		"occurredAt":     base.Format(time.RFC3339Nano),
		"roomId":         "room-42",
		"gameId":         "game-1",
		"roomType":       "ad_hoc",
		"isAbandoned":    false,
		"authoritative":  true,
		"completed":      true,
		"commandId":      "cmd-gc-1",
		"placementOrder": []string{"p1", "p2"},
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1, "cardPoints": 10, "outcome": "won"},
			{"playerId": "p2", "placement": 2},
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
		Topic: DefaultGameCompletedTopic, Partition: 0, Offset: offset,
		Key: []byte("room-42"), Value: value,
	}
}

func TestParseGameCompletedRecord_CanonicalMapping(t *testing.T) {
	evt, err := ParseGameCompletedRecord(canonicalGameCompletedJSON())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if evt.SchemaVersion != 1 || evt.EventID != "evt-gc-1" || evt.EventType != "GameCompleted" {
		t.Fatalf("metadata: %+v", evt)
	}
	if evt.CorrelationID != "corr-gc-1" || evt.CausationID != "cmd-gc-1" || evt.CommandID != "cmd-gc-1" {
		t.Fatalf("ids: %+v", evt)
	}
	if evt.RoomID != "room-42" || evt.GameID != "game-1" || evt.RoomType != "ad_hoc" {
		t.Fatalf("body ids: %+v", evt)
	}
	if evt.IsAbandoned || !evt.Authoritative || !evt.Completed {
		t.Fatalf("flags: abandoned=%v auth=%v completed=%v", evt.IsAbandoned, evt.Authoritative, evt.Completed)
	}
	if len(evt.PlacementOrder) != 2 || evt.PlacementOrder[0] != "p1" {
		t.Fatalf("placementOrder: %+v", evt.PlacementOrder)
	}
	if len(evt.Participants) != 2 || evt.Participants[0].PlayerID != "p1" || evt.Participants[0].Placement != 1 {
		t.Fatalf("participants: %+v", evt.Participants)
	}
	if evt.Participants[0].CardPoints == nil || *evt.Participants[0].CardPoints != 10 {
		t.Fatalf("cardPoints: %+v", evt.Participants[0])
	}
}

func TestMapGameCompletedToRequest_CommandAndCausation(t *testing.T) {
	evt, err := ParseGameCompletedRecord(canonicalGameCompletedJSON())
	if err != nil {
		t.Fatal(err)
	}
	req := MapGameCompletedToRequest(evt)
	if string(req.CommandID) != "cmd-gc-1" || req.CausationID != "cmd-gc-1" || req.CorrelationID != "corr-gc-1" {
		t.Fatalf("mapped: %+v", req)
	}

	evt2, err := ParseGameCompletedRecord(canonicalGameCompletedJSON(func(m map[string]any) {
		delete(m, "commandId")
		m["causationId"] = "cause-only"
	}))
	if err != nil {
		t.Fatal(err)
	}
	req2 := MapGameCompletedToRequest(evt2)
	if string(req2.CommandID) != "kafka:evt-gc-1" {
		t.Fatalf("commandId=%q", req2.CommandID)
	}
	if req2.CausationID != "cause-only" {
		t.Fatalf("causationId=%q", req2.CausationID)
	}
}

func TestParseGameCompletedRecord_RequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"missing_schema", func(m map[string]any) { delete(m, "schemaVersion") }},
		{"bad_schema", func(m map[string]any) { m["schemaVersion"] = 2 }},
		{"schema_string", func(m map[string]any) { m["schemaVersion"] = "1" }},
		{"schema_float", func(m map[string]any) { m["schemaVersion"] = 1.5 }},
		{"missing_event_id", func(m map[string]any) { delete(m, "eventId") }},
		{"empty_event_id", func(m map[string]any) { m["eventId"] = "" }},
		{"event_id_number", func(m map[string]any) { m["eventId"] = 1 }},
		{"bad_event_type", func(m map[string]any) { m["eventType"] = "MatchCompleted" }},
		{"missing_room", func(m map[string]any) { delete(m, "roomId") }},
		{"missing_game", func(m map[string]any) { delete(m, "gameId") }},
		{"bad_room_type", func(m map[string]any) { m["roomType"] = "lobby" }},
		{"missing_abandoned", func(m map[string]any) { delete(m, "isAbandoned") }},
		{"abandoned_string", func(m map[string]any) { m["isAbandoned"] = "false" }},
		{"abandoned_number", func(m map[string]any) { m["isAbandoned"] = 0 }},
		{"authoritative_string", func(m map[string]any) { m["authoritative"] = "true" }},
		{"completed_number", func(m map[string]any) { m["completed"] = 1 }},
		{"missing_occurred_at", func(m map[string]any) { delete(m, "occurredAt") }},
		{"bad_occurred_at", func(m map[string]any) { m["occurredAt"] = "not-a-time" }},
		{"missing_placement_order", func(m map[string]any) { delete(m, "placementOrder") }},
		{"empty_placement_order", func(m map[string]any) { m["placementOrder"] = []any{} }},
		{"dup_placement_order", func(m map[string]any) { m["placementOrder"] = []any{"p1", "p1"} }},
		{"empty_placement_entry", func(m map[string]any) { m["placementOrder"] = []any{"p1", ""} }},
		{"missing_participants", func(m map[string]any) { delete(m, "participants") }},
		{"empty_participants", func(m map[string]any) { m["participants"] = []any{} }},
		{"participants_not_array", func(m map[string]any) { m["participants"] = map[string]any{} }},
		{"player_missing_id", func(m map[string]any) {
			m["participants"] = []map[string]any{{"placement": 1}}
			m["placementOrder"] = []string{"x"}
		}},
		{"placement_string", func(m map[string]any) {
			m["participants"] = []map[string]any{{"playerId": "p1", "placement": "1"}, {"playerId": "p2", "placement": 2}}
		}},
		{"placement_zero", func(m map[string]any) {
			m["placementOrder"] = []string{"p1", "p2"}
			m["participants"] = []map[string]any{{"playerId": "p1", "placement": 0}, {"playerId": "p2", "placement": 2}}
		}},
		{"set_mismatch", func(m map[string]any) {
			m["placementOrder"] = []string{"p1", "p3"}
		}},
		{"order_placement_mismatch", func(m map[string]any) {
			m["participants"] = []map[string]any{
				{"playerId": "p1", "placement": 2},
				{"playerId": "p2", "placement": 1},
			}
		}},
		{"card_points_string", func(m map[string]any) {
			m["participants"] = []map[string]any{
				{"playerId": "p1", "placement": 1, "cardPoints": "10"},
				{"playerId": "p2", "placement": 2},
			}
		}},
		{"invalid_json", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw []byte
			if tc.name == "invalid_json" {
				raw = []byte(`{not-json`)
			} else {
				raw = canonicalGameCompletedJSON(tc.mut)
			}
			_, err := ParseGameCompletedRecord(raw)
			if err == nil {
				t.Fatal("expected error")
			}
			if !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal parse error, got %v", err)
			}
		})
	}
}

type fakeHandler struct {
	mu          sync.Mutex
	calls       []GameCompletedRequest
	fn          func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error)
	perfFn      func(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error)
	ratingFn    func(ctx context.Context, evt PlayerRatingUpdatedEvent) error
	ratingCalls int
	blockCh     chan struct{}
	entered     chan struct{}
}

func (f *fakeHandler) ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
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
			return GameCompletedResult{}, ctx.Err()
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, req)
	fn := f.fn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return GameCompletedResult{Kind: domain.OutcomeAccepted, CommandID: req.CommandID, EventID: req.EventID}, nil
}

func (f *fakeHandler) ApplyTournamentPerformance(ctx context.Context, req TournamentPerformanceRequest) (TournamentPerformanceResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.perfFn != nil {
		return f.perfFn(ctx, req)
	}
	return TournamentPerformanceResult{Kind: domain.OutcomeAccepted, UpstreamEventID: req.UpstreamEventID, BusinessKey: req.BusinessKey}, nil
}

func (f *fakeHandler) ApplyPlayerRatingUpdated(ctx context.Context, evt PlayerRatingUpdatedEvent) error {
	f.mu.Lock()
	f.ratingCalls++
	fn := f.ratingFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, evt)
	}
	return nil
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

func newTestConsumer(src *fakeSource, dlq *fakeDLQ, h *fakeHandler) *GameCompletedKafkaConsumer {
	return &GameCompletedKafkaConsumer{
		source:  src,
		dlq:     dlq,
		handler: h,
		cfg: GameCompletedKafkaConfig{
			Group:               DefaultRankingKafkaGroup,
			Topic:               DefaultGameCompletedTopic,
			DLQTopic:            DefaultGameCompletedDLQTopic,
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

func TestGameCompletedConsumer_RunCancelsCleanly(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	time.Sleep(15 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop")
	}
}

func TestGameCompletedConsumer_AcceptedDuplicateIgnoredCommit(t *testing.T) {
	for _, kind := range []domain.OutcomeKind{domain.OutcomeAccepted, domain.OutcomeDuplicate, domain.OutcomeRejected} {
		t.Run(string(kind), func(t *testing.T) {
			src := &fakeSource{}
			dlq := &fakeDLQ{}
			h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
				return GameCompletedResult{Kind: kind, CommandID: req.CommandID, EventID: req.EventID}, nil
			}}
			c := newTestConsumer(src, dlq, h)
			if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(10, canonicalGameCompletedJSON())}); err != nil {
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

func TestGameCompletedConsumer_RetryThenSuccess(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		n := attempts.Add(1)
		if n < 3 {
			return GameCompletedResult{}, errors.New("database temporarily unavailable")
		}
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(3, canonicalGameCompletedJSON())}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	if got := src.committedOffsets(); len(got) != 1 {
		t.Fatalf("commits=%v", got)
	}
}

func TestGameCompletedConsumer_RetryExhaustionPublishesDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		return GameCompletedResult{}, errors.New("connection reset by peer")
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	original := roomRec(99, canonicalGameCompletedJSON())
	original.Partition = 4
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{original}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 {
		t.Fatalf("dlq pubs=%d", len(pubs))
	}
	if string(pubs[0].Original.Key) != "room-42" || string(pubs[0].Original.Value) != string(original.Value) {
		t.Fatal("dlq must preserve original key/value")
	}
	meta := pubs[0].Meta
	if meta.Consumer != DefaultRankingKafkaGroup || meta.SourceTopic != DefaultGameCompletedTopic {
		t.Fatalf("meta=%+v", meta)
	}
	if meta.AttemptCount != 3 || meta.CorrelationID != "corr-gc-1" {
		t.Fatalf("meta=%+v", meta)
	}
	if got := src.committedOffsets(); len(got) != 1 || got[0] != 99 {
		t.Fatalf("commits=%v", got)
	}
	if q.count() != 1 {
		t.Fatalf("quarantine records=%d", q.count())
	}
}

func TestGameCompletedConsumer_SchemaFailureDLQNoRetry(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		attempts.Add(1)
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	bad := ConsumerRecord{
		Topic: DefaultGameCompletedTopic, Partition: 1, Offset: 5,
		Key: []byte("room-x"), Value: canonicalGameCompletedJSON(func(m map[string]any) {
			m["eventType"] = "Nope"
			m["roomId"] = "room-x"
		}),
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{bad}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("handler must not run for schema failure")
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureSchemaInvalid {
		t.Fatalf("pubs=%+v", pubs)
	}
	if q.count() != 1 {
		t.Fatal("schema failure must quarantine before DLQ")
	}
}

func TestGameCompletedConsumer_NoCommitBeforeApply(t *testing.T) {
	src := &fakeSource{}
	var commitBeforeApply atomic.Bool
	applied := make(chan struct{})
	src.commitFn = func(rec ConsumerRecord) error {
		select {
		case <-applied:
		default:
			commitBeforeApply.Store(true)
		}
		return nil
	}
	dlq := &fakeDLQ{}
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		close(applied)
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(1, canonicalGameCompletedJSON())}); err != nil {
		t.Fatal(err)
	}
	if commitBeforeApply.Load() {
		t.Fatal("committed before apply completed")
	}
}

func TestGameCompletedConsumer_DLQFailureDoesNotCommit(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{failErr: errors.New("broker reject")}
	dlq.failOnce.Store(true)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultGameCompletedTopic, Partition: 2, Offset: 44,
		Key: []byte("room-42"), Value: []byte(`not-json`),
	}})
	if err == nil {
		t.Fatal("expected dlq failure")
	}
	if got := src.committedOffsets(); len(got) != 0 {
		t.Fatalf("must not commit on dlq failure, commits=%v", got)
	}
}

func TestGameCompletedConsumer_RunRetainsBatchOnCommitFailure(t *testing.T) {
	var commitAttempts atomic.Int32
	src := &fakeSource{
		queue: [][]ConsumerRecord{{roomRec(11, canonicalGameCompletedJSON())}},
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
		t.Fatalf("want retry after commit failure, attempts=%d", commitAttempts.Load())
	}
}

func TestGameCompletedConsumer_RunRetainsBatchOnDLQFailure(t *testing.T) {
	src := &fakeSource{
		queue: [][]ConsumerRecord{{
			{Topic: DefaultGameCompletedTopic, Partition: 0, Offset: 12, Key: []byte("room-42"), Value: []byte(`not-json`)},
		}},
	}
	dlq := &fakeDLQ{failErr: errors.New("dlq down")}
	dlq.failN.Store(1)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		if len(dlq.publications()) == 1 && len(src.committedOffsets()) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("dlq/commit never succeeded pubs=%d commits=%v", len(dlq.publications()), src.committedOffsets())
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
}

func TestGameCompletedConsumer_RunDoesNotTerminateOnProcessError(t *testing.T) {
	var polls atomic.Int32
	src := &fakeSource{
		pollFn: func(ctx context.Context) ([]ConsumerRecord, error) {
			n := polls.Add(1)
			if n == 1 {
				return []ConsumerRecord{{
					Topic: DefaultGameCompletedTopic, Partition: 0, Offset: 1,
					Key: []byte("room-42"), Value: []byte(`not-json`),
				}}, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
				return nil, nil
			}
		},
	}
	dlq := &fakeDLQ{failErr: errors.New("dlq down")}
	dlq.failN.Store(100)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Run must not exit on process error, got %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop on cancel")
	}
}

func TestGameCompletedConsumer_PartitionOrdering(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var order []string
	var mu sync.Mutex
	gate := make(chan struct{})
	enteredFirst := make(chan struct{}, 1)
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		mu.Lock()
		order = append(order, string(req.EventID))
		n := len(order)
		mu.Unlock()
		if n == 1 {
			enteredFirst <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
				return GameCompletedResult{}, ctx.Err()
			}
		}
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	recs := []ConsumerRecord{
		{Topic: DefaultGameCompletedTopic, Partition: 3, Offset: 1, Key: []byte("room-42"), Value: canonicalGameCompletedJSON(func(m map[string]any) { m["eventId"] = "first" })},
		{Topic: DefaultGameCompletedTopic, Partition: 3, Offset: 2, Key: []byte("room-42"), Value: canonicalGameCompletedJSON(func(m map[string]any) { m["eventId"] = "second"; m["gameId"] = "game-2" })},
	}
	done := make(chan error, 1)
	go func() { done <- c.ProcessBatch(context.Background(), recs) }()
	select {
	case <-enteredFirst:
	case <-time.After(2 * time.Second):
		t.Fatal("first record did not start")
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if len(order) != 1 || order[0] != "first" {
		mu.Unlock()
		t.Fatalf("ordering violated early: %v", order)
	}
	mu.Unlock()
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
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("order=%v", order)
	}
}

func TestGameCompletedConsumer_UnrelatedPartitionProgressesDuringBlock(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	gate := make(chan struct{})
	enteredBlocked := make(chan struct{}, 1)
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		if string(req.EventID) == "blocked" {
			enteredBlocked <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
				return GameCompletedResult{}, ctx.Err()
			}
		}
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.cfg.MaxPartitionWorkers = 2
	recs := []ConsumerRecord{
		{Topic: DefaultGameCompletedTopic, Partition: 1, Offset: 1, Key: []byte("room-42"),
			Value: canonicalGameCompletedJSON(func(m map[string]any) { m["eventId"] = "blocked"; m["roomId"] = "room-42" })},
		{Topic: DefaultGameCompletedTopic, Partition: 2, Offset: 1, Key: []byte("room-99"),
			Value: canonicalGameCompletedJSON(func(m map[string]any) {
				m["eventId"] = "free"
				m["roomId"] = "room-99"
				m["gameId"] = "game-free"
			})},
	}
	done := make(chan error, 1)
	go func() { done <- c.ProcessBatch(context.Background(), recs) }()
	select {
	case <-enteredBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked partition did not start")
	}
	deadline := time.After(2 * time.Second)
	for {
		offs := src.committedOffsets()
		if len(offs) == 1 && offs[0] == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("unrelated partition did not progress, commits=%v", src.committedOffsets())
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
}

func TestGameCompletedConsumer_KeyMismatchIsTerminal(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		attempts.Add(1)
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	rec := ConsumerRecord{
		Topic: DefaultGameCompletedTopic, Partition: 0, Offset: 3,
		Key: []byte("other-room"), Value: canonicalGameCompletedJSON(),
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("must not apply on key mismatch")
	}
	if q.count() != 1 || q.records[0].AggregateKey != "other-room" {
		t.Fatalf("quarantine=%+v", q.records)
	}
}

func TestGameCompletedConsumer_MissingKeyWithoutRoomID_DLQWithoutQuarantine(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	rec := ConsumerRecord{
		Topic: DefaultGameCompletedTopic, Partition: 0, Offset: 8,
		Key: nil, Value: []byte(`not-json`),
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if q.count() != 0 {
		t.Fatalf("must not quarantine without identifiable aggregate, got %+v", q.records)
	}
	if len(dlq.publications()) != 1 {
		t.Fatal("expected DLQ")
	}
}

func TestGameCompletedConsumer_AlreadyQuarantinedSkipsDomain(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{active: map[string]bool{
		DefaultRankingKafkaGroup + "|" + DefaultGameCompletedTopic + "|room-42": true,
	}}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
		attempts.Add(1)
		return GameCompletedResult{Kind: domain.OutcomeAccepted}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(4, canonicalGameCompletedJSON())}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("must not apply when aggregate quarantined")
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureAggregateQuarantined {
		t.Fatalf("pubs=%+v", pubs)
	}
	if q.count() != 0 {
		t.Fatal("already quarantined path must not re-persist quarantine before DLQ")
	}
}

func TestGameCompletedConsumer_QuarantineFailureBlocksDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	q.failOnce.Store(true)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultGameCompletedTopic, Partition: 0, Offset: 1,
		Key: []byte("room-42"), Value: []byte(`not-json`),
	}})
	if err == nil {
		t.Fatal("expected quarantine failure")
	}
	if len(dlq.publications()) != 0 {
		t.Fatal("dlq must not publish when quarantine fails")
	}
	if got := src.committedOffsets(); len(got) != 0 {
		t.Fatalf("commits=%v", got)
	}
}

func TestLoadGameCompletedKafkaConfig_DefaultsAndFailClosed(t *testing.T) {
	t.Run("empty_brokers_disabled", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "")
		_, enabled, err := LoadGameCompletedKafkaConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			t.Fatal("empty brokers must disable consumer")
		}
	})
	t.Run("defaults_when_unset", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		for _, k := range []string{
			"KAFKA_CONSUMER_GROUP", "KAFKA_GAME_COMPLETED_TOPIC", "KAFKA_GAME_COMPLETED_DLQ_TOPIC",
			"KAFKA_PLAYERS_ADVANCED_TOPIC", "KAFKA_PLAYERS_ADVANCED_DLQ_TOPIC",
			"KAFKA_TOURNAMENT_COMPLETED_TOPIC", "KAFKA_TOURNAMENT_COMPLETED_DLQ_TOPIC",
			"KAFKA_PLAYER_RATING_UPDATED_TOPIC", "KAFKA_PLAYER_RATING_UPDATED_DLQ_TOPIC",
			"KAFKA_GAME_COMPLETED_MAX_ATTEMPTS", "KAFKA_GAME_COMPLETED_MAX_PARTITION_WORKERS",
			"KAFKA_MAX_ATTEMPTS", "KAFKA_MAX_PARTITION_WORKERS",
		} {
			_ = os.Unsetenv(k)
		}
		cfg, enabled, err := LoadGameCompletedKafkaConfigFromEnv()
		if err != nil || !enabled {
			t.Fatalf("enabled=%v err=%v", enabled, err)
		}
		if cfg.Group != DefaultRankingKafkaGroup {
			t.Fatalf("group=%q", cfg.Group)
		}
		wantTopics := []string{
			DefaultGameCompletedTopic, DefaultPlayersAdvancedTopic,
			DefaultTournamentCompletedTopic, DefaultPlayerRatingUpdatedTopic,
		}
		if len(cfg.Topics) != len(wantTopics) {
			t.Fatalf("topics=%v", cfg.Topics)
		}
		for i, want := range wantTopics {
			if cfg.Topics[i] != want {
				t.Fatalf("topics=%v", cfg.Topics)
			}
		}
		if cfg.DLQTopicFor(DefaultGameCompletedTopic) != DefaultGameCompletedDLQTopic {
			t.Fatalf("game dlq=%q", cfg.DLQTopicFor(DefaultGameCompletedTopic))
		}
		if cfg.DLQTopicFor(DefaultPlayersAdvancedTopic) != DefaultPlayersAdvancedDLQTopic {
			t.Fatalf("adv dlq=%q", cfg.DLQTopicFor(DefaultPlayersAdvancedTopic))
		}
		if cfg.DLQTopicFor(DefaultTournamentCompletedTopic) != DefaultTournamentCompletedDLQTopic {
			t.Fatalf("completed dlq=%q", cfg.DLQTopicFor(DefaultTournamentCompletedTopic))
		}
		if cfg.DLQTopicFor(DefaultPlayerRatingUpdatedTopic) != DefaultPlayerRatingUpdatedDLQTopic {
			t.Fatalf("rating dlq=%q", cfg.DLQTopicFor(DefaultPlayerRatingUpdatedTopic))
		}
		if cfg.MaxPartitionWorkers != defaultRankingPartitionWorkers {
			t.Fatalf("workers=%d", cfg.MaxPartitionWorkers)
		}
		if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "kafka.uno-arena.svc.cluster.local:9092" {
			t.Fatalf("brokers=%v", cfg.Brokers)
		}
	})
	t.Run("blank_group_fails", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", "   ")
		t.Setenv("KAFKA_GAME_COMPLETED_TOPIC", DefaultGameCompletedTopic)
		t.Setenv("KAFKA_GAME_COMPLETED_DLQ_TOPIC", DefaultGameCompletedDLQTopic)
		if _, _, err := LoadGameCompletedKafkaConfigFromEnv(); err == nil {
			t.Fatal("blank group must fail")
		}
	})
	t.Run("blank_topic_fails", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", DefaultRankingKafkaGroup)
		t.Setenv("KAFKA_GAME_COMPLETED_TOPIC", "  ")
		t.Setenv("KAFKA_GAME_COMPLETED_DLQ_TOPIC", DefaultGameCompletedDLQTopic)
		if _, _, err := LoadGameCompletedKafkaConfigFromEnv(); err == nil {
			t.Fatal("blank topic must fail")
		}
	})
	t.Run("blank_dlq_fails", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", DefaultRankingKafkaGroup)
		t.Setenv("KAFKA_GAME_COMPLETED_TOPIC", DefaultGameCompletedTopic)
		t.Setenv("KAFKA_GAME_COMPLETED_DLQ_TOPIC", " ")
		if _, _, err := LoadGameCompletedKafkaConfigFromEnv(); err == nil {
			t.Fatal("blank dlq must fail")
		}
	})
}

func TestWireRankingRuntime_CapabilityIgnoresKafka(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.kafka != nil {
		t.Fatal("capability mode must remain offline (no kafka consumer)")
	}
}

func TestWireRankingRuntime_DurableRequiresKafka(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://ranking@localhost/ranking")
	t.Setenv("RANKING_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("KAFKA_BROKERS", "")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "durable" || rt.ready {
		t.Fatalf("want durable not ready without kafka: %+v", rt)
	}
	if !strings.Contains(rt.readyReason, "kafka") {
		t.Fatalf("reason=%q", rt.readyReason)
	}
}

func TestWireRankingRuntime_CapabilityIgnoresMalformedKafka(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RANKING_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("RANKING_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "   ")
	rt, err := wireRankingRuntime()
	if err != nil {
		t.Fatalf("capability must ignore malformed kafka env: %v", err)
	}
	if rt.mode != "capability" || rt.kafka != nil {
		t.Fatalf("mode=%s kafka=%v", rt.mode, rt.kafka != nil)
	}
}

func TestGameCompletedKafkaLifecycle_UnhealthyAfterUnexpectedStop(t *testing.T) {
	life := &gameCompletedKafkaLifecycle{}
	life.healthy.Store(true)
	life.healthy.Store(false)
	life.stoppedErr.Store(errors.New("boom"))
	if life.Healthy() {
		t.Fatal("expected unhealthy")
	}
}

func TestClassifyKafkaConsumeError(t *testing.T) {
	term := &kafkaConsumeError{class: KafkaFailureSchemaInvalid, terminal: true, err: errors.New("bad")}
	if !IsTerminalKafkaConsumeError(term) {
		t.Fatal("expected terminal")
	}
	if ClassifyKafkaConsumeError(errors.New("timeout dialing postgres")) != KafkaFailureDependency {
		t.Fatal("want dependency")
	}
	if len(sanitizeDLQErrorSummary(strings.Repeat("y", 500))) > maxDLQErrorSummaryLen {
		t.Fatal("summary not bounded")
	}
}
