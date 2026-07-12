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
)

func canonicalMatchCompletedJSON(mut ...func(map[string]any)) []byte {
	base := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	m := map[string]any{
		"schemaVersion":     1,
		"eventId":           "evt-mc-1",
		"eventType":         "MatchCompleted",
		"correlationId":     "corr-mc-1",
		"causationId":       "cmd-mc-1",
		"occurredAt":        base.Format(time.RFC3339Nano),
		"roomId":            "room-42",
		"tournamentId":      "tour-1",
		"roundNumber":       1,
		"slotId":            "1",
		"completionVersion": 3,
		"isAbandoned":       false,
		"players": []map[string]any{
			{"playerId": "p1", "matchWins": 2, "cumulativeCardPoints": 10, "finalGameCompletedAt": base.Format(time.RFC3339Nano), "forfeited": false},
			{"playerId": "p2", "matchWins": 1, "cumulativeCardPoints": 8, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
		},
		"forfeits": []string{"p3"},
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
		Topic: DefaultMatchCompletedTopic, Partition: 0, Offset: offset,
		Key: []byte("room-42"), Value: value,
	}
}

func TestParseMatchCompletedRecord_CanonicalMapping(t *testing.T) {
	evt, err := ParseMatchCompletedRecord(canonicalMatchCompletedJSON())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if evt.SchemaVersion != 1 || evt.EventID != "evt-mc-1" || evt.EventType != "MatchCompleted" {
		t.Fatalf("metadata: %+v", evt)
	}
	if evt.CorrelationID != "corr-mc-1" || evt.CausationID != "cmd-mc-1" {
		t.Fatalf("correlation/causation: %+v", evt)
	}
	if evt.RoomID != "room-42" || evt.TournamentID != "tour-1" || evt.CompletionVersion != 3 {
		t.Fatalf("ids/version: %+v", evt)
	}
	if !evt.HasIsAbandoned || evt.IsAbandoned {
		t.Fatalf("isAbandoned: has=%v val=%v", evt.HasIsAbandoned, evt.IsAbandoned)
	}
	if evt.OccurredAt.IsZero() {
		t.Fatal("occurredAt required")
	}
	if len(evt.Players) != 2 || evt.Players[0].PlayerID != "p1" || evt.Players[0].MatchWins != 2 {
		t.Fatalf("players: %+v", evt.Players)
	}
	if len(evt.Forfeits) != 1 || evt.Forfeits[0] != "p3" {
		t.Fatalf("forfeits: %+v", evt.Forfeits)
	}
}

func TestParseMatchCompletedRecord_RequiredFields(t *testing.T) {
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
		{"bad_event_type", func(m map[string]any) { m["eventType"] = "GameCompleted" }},
		{"missing_room", func(m map[string]any) { delete(m, "roomId") }},
		{"missing_tournament", func(m map[string]any) { delete(m, "tournamentId") }},
		{"zero_completion", func(m map[string]any) { m["completionVersion"] = 0 }},
		{"completion_string", func(m map[string]any) { m["completionVersion"] = "3" }},
		{"missing_abandoned", func(m map[string]any) { delete(m, "isAbandoned") }},
		{"abandoned_string", func(m map[string]any) { m["isAbandoned"] = "false" }},
		{"abandoned_number", func(m map[string]any) { m["isAbandoned"] = 0 }},
		{"missing_occurred_at", func(m map[string]any) { delete(m, "occurredAt") }},
		{"bad_occurred_at", func(m map[string]any) { m["occurredAt"] = "not-a-time" }},
		{"occurred_at_number", func(m map[string]any) { m["occurredAt"] = 1 }},
		{"missing_players", func(m map[string]any) { delete(m, "players") }},
		{"empty_players", func(m map[string]any) { m["players"] = []any{} }},
		{"players_not_array", func(m map[string]any) { m["players"] = map[string]any{} }},
		{"player_missing_id", func(m map[string]any) {
			m["players"] = []map[string]any{{"matchWins": 1, "cumulativeCardPoints": 1}}
		}},
		{"player_missing_wins", func(m map[string]any) {
			m["players"] = []map[string]any{{"playerId": "p1", "cumulativeCardPoints": 1}}
		}},
		{"player_wins_string", func(m map[string]any) {
			m["players"] = []map[string]any{{"playerId": "p1", "matchWins": "2", "cumulativeCardPoints": 1}}
		}},
		{"player_forfeited_string", func(m map[string]any) {
			m["players"] = []map[string]any{{"playerId": "p1", "matchWins": 1, "cumulativeCardPoints": 1, "forfeited": "true"}}
		}},
		{"round_number_string", func(m map[string]any) { m["roundNumber"] = "1" }},
		{"bad_forfeits", func(m map[string]any) { m["forfeits"] = []any{1, 2} }},
		{"empty_forfeit_entry", func(m map[string]any) { m["forfeits"] = []any{"p3", ""} }},
		{"invalid_json", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw []byte
			if tc.name == "invalid_json" {
				raw = []byte(`{not-json`)
			} else {
				raw = canonicalMatchCompletedJSON(tc.mut)
			}
			_, err := ParseMatchCompletedRecord(raw)
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
	mu      sync.Mutex
	calls   []MatchCompletedEvent
	fn      func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error)
	blockCh chan struct{}
	entered chan struct{}
}

func (f *fakeHandler) IngestMatchCompleted(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
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
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, evt)
	f.mu.Unlock()
	if f.fn != nil {
		return f.fn(ctx, evt)
	}
	return map[string]any{"disposition": "recorded"}, nil
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

func newTestConsumer(src *fakeSource, dlq *fakeDLQ, h *fakeHandler) *MatchCompletedKafkaConsumer {
	return &MatchCompletedKafkaConsumer{
		source:  src,
		dlq:     dlq,
		handler: h,
		cfg: MatchCompletedKafkaConfig{
			Group:               DefaultTournamentKafkaGroup,
			Topic:               DefaultMatchCompletedTopic,
			DLQTopic:            DefaultMatchCompletedDLQTopic,
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

func TestMatchCompletedConsumer_RunCancelsCleanly(t *testing.T) {
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

func TestMatchCompletedConsumer_DomainSuccessCommits(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recs := []ConsumerRecord{roomRec(10, canonicalMatchCompletedJSON())}
	if err := c.ProcessBatch(ctx, recs); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := src.committedOffsets(); len(got) != 1 || got[0] != 10 {
		t.Fatalf("commits=%v", got)
	}
	if len(dlq.publications()) != 0 {
		t.Fatalf("unexpected dlq: %+v", dlq.publications())
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.calls) != 1 || h.calls[0].EventID != "evt-mc-1" {
		t.Fatalf("handler calls=%+v", h.calls)
	}
}

func TestMatchCompletedConsumer_DuplicateAndQuarantineCommit(t *testing.T) {
	for _, disposition := range []string{"duplicate_ignored", "quarantined"} {
		t.Run(disposition, func(t *testing.T) {
			src := &fakeSource{}
			dlq := &fakeDLQ{}
			h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
				return map[string]any{"disposition": disposition}, nil
			}}
			c := newTestConsumer(src, dlq, h)
			recs := []ConsumerRecord{roomRec(7, canonicalMatchCompletedJSON())}
			if err := c.ProcessBatch(context.Background(), recs); err != nil {
				t.Fatal(err)
			}
			if got := src.committedOffsets(); len(got) != 1 || got[0] != 7 {
				t.Fatalf("commits=%v", got)
			}
		})
	}
}

func TestMatchCompletedConsumer_RetryThenSuccess(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		n := attempts.Add(1)
		if n < 3 {
			return nil, errors.New("database temporarily unavailable")
		}
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	recs := []ConsumerRecord{roomRec(3, canonicalMatchCompletedJSON())}
	if err := c.ProcessBatch(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	if got := src.committedOffsets(); len(got) != 1 {
		t.Fatalf("commits=%v", got)
	}
	if len(dlq.publications()) != 0 {
		t.Fatal("dlq should be empty after success")
	}
}

func TestMatchCompletedConsumer_RetryExhaustionPublishesDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		return nil, errors.New("connection reset by peer")
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	original := roomRec(99, canonicalMatchCompletedJSON())
	original.Partition = 4
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{original}); err != nil {
		t.Fatal(err)
	}
	pubs := dlq.publications()
	if len(pubs) != 1 {
		t.Fatalf("dlq pubs=%d", len(pubs))
	}
	if string(pubs[0].Original.Key) != "room-42" || string(pubs[0].Original.Value) != string(original.Value) {
		t.Fatalf("dlq must preserve original key/value")
	}
	meta := pubs[0].Meta
	if meta.Consumer != DefaultTournamentKafkaGroup {
		t.Fatalf("consumer=%q", meta.Consumer)
	}
	if meta.SourceTopic != DefaultMatchCompletedTopic || meta.SourcePartition != 4 || meta.SourceOffset != 99 {
		t.Fatalf("source meta=%+v", meta)
	}
	if meta.AttemptCount != 3 {
		t.Fatalf("attempts=%d", meta.AttemptCount)
	}
	if meta.Classification == "" || meta.CorrelationID != "corr-mc-1" {
		t.Fatalf("classification/corr=%+v", meta)
	}
	if meta.ErrorSummary == "" || len(meta.ErrorSummary) > maxDLQErrorSummaryLen {
		t.Fatalf("error summary=%q", meta.ErrorSummary)
	}
	if got := src.committedOffsets(); len(got) != 1 || got[0] != 99 {
		t.Fatalf("commits=%v", got)
	}
	if q.count() != 1 {
		t.Fatalf("quarantine records=%d", q.count())
	}
}

func TestMatchCompletedConsumer_SchemaFailureDLQNoRetry(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		attempts.Add(1)
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	bad := ConsumerRecord{
		Topic: DefaultMatchCompletedTopic, Partition: 1, Offset: 5,
		Key: []byte("room-x"), Value: canonicalMatchCompletedJSON(func(m map[string]any) {
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
	if len(pubs) != 1 {
		t.Fatalf("want 1 dlq, got %d", len(pubs))
	}
	if pubs[0].Meta.Classification != KafkaFailureSchemaInvalid {
		t.Fatalf("classification=%s", pubs[0].Meta.Classification)
	}
	if string(pubs[0].Original.Value) != string(bad.Value) {
		t.Fatal("original value mutated")
	}
	if got := src.committedOffsets(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("commits=%v", got)
	}
	if q.count() != 1 {
		t.Fatal("schema failure must quarantine aggregate before DLQ")
	}
}

func TestMatchCompletedConsumer_NoCommitBeforeApply(t *testing.T) {
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
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		close(applied)
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(1, canonicalMatchCompletedJSON())}); err != nil {
		t.Fatal(err)
	}
	if commitBeforeApply.Load() {
		t.Fatal("committed before ingest completed")
	}
}

func TestMatchCompletedConsumer_DLQFailureDoesNotCommit(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{failErr: errors.New("broker reject")}
	dlq.failOnce.Store(true)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultMatchCompletedTopic, Partition: 2, Offset: 44,
		Key: []byte("room-42"), Value: []byte(`not-json`),
	}})
	if err == nil {
		t.Fatal("expected dlq failure")
	}
	if got := src.committedOffsets(); len(got) != 0 {
		t.Fatalf("must not commit on dlq failure, commits=%v", got)
	}
}

func TestMatchCompletedConsumer_RunRetainsBatchOnCommitFailure(t *testing.T) {
	var commitAttempts atomic.Int32
	src := &fakeSource{
		queue: [][]ConsumerRecord{{roomRec(11, canonicalMatchCompletedJSON())}},
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
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.calls) < 2 {
		t.Fatalf("handler should re-apply retained batch, calls=%d", len(h.calls))
	}
}

func TestMatchCompletedConsumer_RunRetainsBatchOnDLQFailure(t *testing.T) {
	src := &fakeSource{
		queue: [][]ConsumerRecord{{
			{Topic: DefaultMatchCompletedTopic, Partition: 0, Offset: 12, Key: []byte("room-42"), Value: []byte(`not-json`)},
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

func TestMatchCompletedConsumer_RunDoesNotTerminateOnProcessError(t *testing.T) {
	var polls atomic.Int32
	src := &fakeSource{
		pollFn: func(ctx context.Context) ([]ConsumerRecord, error) {
			n := polls.Add(1)
			if n == 1 {
				return []ConsumerRecord{{
					Topic: DefaultMatchCompletedTopic, Partition: 0, Offset: 1,
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
	// Keep failing DLQ so ProcessBatch keeps erroring; prove Run does not return.
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

func TestMatchCompletedConsumer_PartitionOrdering(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var order []string
	var mu sync.Mutex
	gate := make(chan struct{})
	enteredFirst := make(chan struct{}, 1)
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		mu.Lock()
		order = append(order, evt.EventID)
		n := len(order)
		mu.Unlock()
		if n == 1 {
			enteredFirst <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	recs := []ConsumerRecord{
		{Topic: DefaultMatchCompletedTopic, Partition: 3, Offset: 1, Key: []byte("room-42"), Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "first" })},
		{Topic: DefaultMatchCompletedTopic, Partition: 3, Offset: 2, Key: []byte("room-42"), Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "second" })},
	}
	done := make(chan error, 1)
	go func() {
		done <- c.ProcessBatch(context.Background(), recs)
	}()
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
	if got := src.committedOffsets(); len(got) != 0 {
		t.Fatalf("no commit before first finishes, got %v", got)
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
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("order=%v", order)
	}
	if got := src.committedOffsets(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("commits=%v", got)
	}
}

func TestMatchCompletedConsumer_UnrelatedPartitionProgressesDuringBlock(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	gate := make(chan struct{})
	enteredBlocked := make(chan struct{}, 1)
	var mu sync.Mutex
	seen := make([]string, 0, 2)
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		mu.Lock()
		seen = append(seen, evt.EventID)
		mu.Unlock()
		if evt.EventID == "blocked" {
			enteredBlocked <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.cfg.MaxPartitionWorkers = 2
	recs := []ConsumerRecord{
		{Topic: DefaultMatchCompletedTopic, Partition: 1, Offset: 1, Key: []byte("room-42"),
			Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "blocked"; m["roomId"] = "room-42" })},
		{Topic: DefaultMatchCompletedTopic, Partition: 2, Offset: 1, Key: []byte("room-99"),
			Value: canonicalMatchCompletedJSON(func(m map[string]any) {
				m["eventId"] = "free"
				m["roomId"] = "room-99"
				m["completionVersion"] = 9
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
			// free partition committed while blocked still held
			break
		}
		select {
		case <-deadline:
			t.Fatalf("unrelated partition did not progress, commits=%v seen=%v", src.committedOffsets(), func() []string {
				mu.Lock()
				defer mu.Unlock()
				return append([]string{}, seen...)
			}())
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
	if got := src.committedOffsets(); len(got) != 2 {
		t.Fatalf("commits=%v", got)
	}
}

func TestMatchCompletedConsumer_KeyMismatchIsTerminal(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		attempts.Add(1)
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	rec := ConsumerRecord{
		Topic: DefaultMatchCompletedTopic, Partition: 0, Offset: 3,
		Key: []byte("other-room"), Value: canonicalMatchCompletedJSON(),
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("must not apply on key mismatch")
	}
	pubs := dlq.publications()
	if len(pubs) != 1 || pubs[0].Meta.Classification != KafkaFailureSchemaInvalid {
		t.Fatalf("pubs=%+v", pubs)
	}
	if q.count() != 1 || q.records[0].AggregateKey != "other-room" {
		t.Fatalf("quarantine=%+v", q.records)
	}
}

func TestMatchCompletedConsumer_MissingKeyWithoutRoomID_DLQWithoutQuarantine(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	rec := ConsumerRecord{
		Topic: DefaultMatchCompletedTopic, Partition: 0, Offset: 8,
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
	if got := src.committedOffsets(); len(got) != 1 {
		t.Fatalf("commits=%v", got)
	}
}

func TestMatchCompletedConsumer_AlreadyQuarantinedSkipsDomain(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{active: map[string]bool{
		DefaultTournamentKafkaGroup + "|" + DefaultMatchCompletedTopic + "|room-42": true,
	}}
	var attempts atomic.Int32
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		attempts.Add(1)
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{roomRec(4, canonicalMatchCompletedJSON())}); err != nil {
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
	if got := src.committedOffsets(); len(got) != 1 {
		t.Fatalf("commits=%v", got)
	}
}

func TestMatchCompletedConsumer_QuarantineFailureBlocksDLQ(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	q := &fakeQuarantine{}
	q.failOnce.Store(true)
	h := &fakeHandler{}
	c := newTestConsumer(src, dlq, h)
	c.quarantine = q
	err := c.ProcessBatch(context.Background(), []ConsumerRecord{{
		Topic: DefaultMatchCompletedTopic, Partition: 0, Offset: 1,
		Key: []byte("room-42"), Value: []byte(`not-json`),
	}})
	if err == nil {
		t.Fatal("expected quarantine failure")
	}
	if len(dlq.publications()) != 0 {
		t.Fatal("DLQ must wait for quarantine persist")
	}
	if got := src.committedOffsets(); len(got) != 0 {
		t.Fatal("must not commit")
	}
}

func TestMatchCompletedConsumer_CrossPartitionSerialStillOrderedPerPartition(t *testing.T) {
	src := &fakeSource{}
	dlq := &fakeDLQ{}
	var mu sync.Mutex
	seen := make([]string, 0, 4)
	h := &fakeHandler{fn: func(ctx context.Context, evt MatchCompletedEvent) (map[string]any, error) {
		mu.Lock()
		seen = append(seen, evt.EventID)
		mu.Unlock()
		return map[string]any{"disposition": "recorded"}, nil
	}}
	c := newTestConsumer(src, dlq, h)
	recs := []ConsumerRecord{
		{Topic: DefaultMatchCompletedTopic, Partition: 1, Offset: 1, Key: []byte("room-a"),
			Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "p1-a"; m["roomId"] = "room-a" })},
		{Topic: DefaultMatchCompletedTopic, Partition: 2, Offset: 1, Key: []byte("room-b"),
			Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "p2-a"; m["roomId"] = "room-b"; m["completionVersion"] = 2 })},
		{Topic: DefaultMatchCompletedTopic, Partition: 1, Offset: 2, Key: []byte("room-a"),
			Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "p1-b"; m["roomId"] = "room-a"; m["completionVersion"] = 3 })},
		{Topic: DefaultMatchCompletedTopic, Partition: 2, Offset: 2, Key: []byte("room-b"),
			Value: canonicalMatchCompletedJSON(func(m map[string]any) { m["eventId"] = "p2-b"; m["roomId"] = "room-b"; m["completionVersion"] = 4 })},
	}
	if err := c.ProcessBatch(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	idx := map[string]int{}
	for i, id := range seen {
		idx[id] = i
	}
	if idx["p1-a"] > idx["p1-b"] || idx["p2-a"] > idx["p2-b"] {
		t.Fatalf("per-partition order broken: %v", seen)
	}
}

func TestLoadMatchCompletedKafkaConfig_DefaultsAndFailClosed(t *testing.T) {
	t.Run("disabled_without_brokers", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "")
		cfg, enabled, err := LoadMatchCompletedKafkaConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			t.Fatal("empty brokers must disable consumer")
		}
		_ = cfg
	})

	t.Run("defaults_when_overrides_unset", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		for _, k := range []string{
			"KAFKA_CONSUMER_GROUP",
			"KAFKA_MATCH_COMPLETED_TOPIC",
			"KAFKA_MATCH_COMPLETED_DLQ_TOPIC",
			"KAFKA_MATCH_COMPLETED_MAX_ATTEMPTS",
			"KAFKA_MATCH_COMPLETED_MAX_PARTITION_WORKERS",
		} {
			if err := os.Unsetenv(k); err != nil {
				t.Fatal(err)
			}
		}
		cfg, enabled, err := LoadMatchCompletedKafkaConfigFromEnv()
		if err != nil || !enabled {
			t.Fatalf("enabled=%v err=%v", enabled, err)
		}
		if cfg.Group != DefaultTournamentKafkaGroup || cfg.Topic != DefaultMatchCompletedTopic || cfg.DLQTopic != DefaultMatchCompletedDLQTopic {
			t.Fatalf("defaults: %+v", cfg)
		}
		if cfg.MaxPartitionWorkers != defaultMatchCompletedPartitionWorkers {
			t.Fatalf("workers=%d", cfg.MaxPartitionWorkers)
		}
		if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "kafka.uno-arena.svc.cluster.local:9092" {
			t.Fatalf("brokers=%v", cfg.Brokers)
		}
	})

	t.Run("blank_group_fail_closed", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", "   ")
		t.Setenv("KAFKA_MATCH_COMPLETED_TOPIC", DefaultMatchCompletedTopic)
		t.Setenv("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", DefaultMatchCompletedDLQTopic)
		_, _, err := LoadMatchCompletedKafkaConfigFromEnv()
		if err == nil {
			t.Fatal("blank group must fail closed")
		}
	})

	t.Run("blank_topic_fail_closed", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", DefaultTournamentKafkaGroup)
		t.Setenv("KAFKA_MATCH_COMPLETED_TOPIC", "   ")
		t.Setenv("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", DefaultMatchCompletedDLQTopic)
		_, _, err := LoadMatchCompletedKafkaConfigFromEnv()
		if err == nil {
			t.Fatal("blank topic must fail closed")
		}
	})

	t.Run("blank_dlq_fail_closed", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
		t.Setenv("KAFKA_CONSUMER_GROUP", DefaultTournamentKafkaGroup)
		t.Setenv("KAFKA_MATCH_COMPLETED_TOPIC", DefaultMatchCompletedTopic)
		t.Setenv("KAFKA_MATCH_COMPLETED_DLQ_TOPIC", "   ")
		_, _, err := LoadMatchCompletedKafkaConfigFromEnv()
		if err == nil {
			t.Fatal("blank dlq must fail closed")
		}
	})
}

func TestWireTournamentRuntime_KafkaOnlyInDurable(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "capability" {
		t.Fatalf("mode=%s", rt.mode)
	}
	if rt.kafka != nil {
		t.Fatal("capability mode must remain offline (no kafka consumer)")
	}
}

func TestWireTournamentRuntime_DurableRequiresKafka(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("DATABASE_URL", "postgres://tournament@localhost/tournament")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("ROOM_GAMEPLAY_URL", "http://room-gameplay")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/7")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("TOURNAMENT_BRACKET_CURSOR_SECRET", "test-cursor-secret")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_CURSOR_SECRET", "analytics-cursor")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if rt.mode != "durable" || rt.ready {
		t.Fatalf("want durable not ready without kafka: mode=%s ready=%v reason=%s", rt.mode, rt.ready, rt.readyReason)
	}
	if !strings.Contains(rt.readyReason, "kafka") {
		t.Fatalf("reason=%q", rt.readyReason)
	}
	if !strings.Contains(rt.readyReason, "durable_dependencies_missing") {
		t.Fatalf("reason=%q", rt.readyReason)
	}
	if rt.kafka != nil {
		t.Fatal("missing kafka must not start a consumer")
	}
}

func TestWireTournamentRuntime_CapabilityIgnoresMalformedKafka(t *testing.T) {
	t.Setenv("WORKER_ROLE", "")
	t.Setenv("TOURNAMENT_CAPABILITY_MODE", "true")
	t.Setenv("DEPLOYMENT_ENV", "local")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOURNAMENT_INTERNAL_CREDENTIAL", "cred")
	t.Setenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL", "analytics-cred")
	t.Setenv("KAFKA_BROKERS", "kafka.uno-arena.svc.cluster.local:9092")
	t.Setenv("KAFKA_CONSUMER_GROUP", "   ")
	rt, err := wireTournamentRuntime()
	if err != nil {
		t.Fatalf("capability must ignore malformed kafka env: %v", err)
	}
	if rt.mode != "capability" || rt.kafka != nil {
		t.Fatalf("mode=%s kafka=%v", rt.mode, rt.kafka != nil)
	}
}

func TestMatchCompletedKafkaLifecycle_UnhealthyAfterUnexpectedStop(t *testing.T) {
	life := &matchCompletedKafkaLifecycle{}
	life.healthy.Store(true)
	life.healthy.Store(false)
	life.stoppedErr.Store(errors.New("boom"))
	if life.Healthy() {
		t.Fatal("expected unhealthy")
	}
}

func TestMatchCompletedKafkaLifecycle_DurableReadyFailsWhenUnhealthy(t *testing.T) {
	life := &matchCompletedKafkaLifecycle{}
	life.healthy.Store(false)
	rt := tournamentRuntime{
		mode:  "durable",
		ready: true,
		kafka: life,
		durableReady: func(ctx context.Context) error {
			if !life.Healthy() {
				return errors.New("kafka_consumer_stopped")
			}
			return nil
		},
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
	if ClassifyKafkaConsumeError(errors.New("timeout dialing postgres")) != KafkaFailureDependency {
		t.Fatal("want dependency")
	}
	if !strings.Contains(sanitizeDLQErrorSummary(strings.Repeat("x", 500)), "x") {
		t.Fatal("summary")
	}
	if len(sanitizeDLQErrorSummary(strings.Repeat("y", 500))) > maxDLQErrorSummaryLen {
		t.Fatal("summary not bounded")
	}
}
