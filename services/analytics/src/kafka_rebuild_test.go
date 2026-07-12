package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"unoarena/services/analytics/domain"
	"unoarena/services/analytics/store"
)

type memRebuildSource struct {
	mu      sync.Mutex
	recs    []ConsumerRecord
	commits []ConsumerRecord
}

func (m *memRebuildSource) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.recs) == 0 {
		return nil, nil
	}
	out := m.recs
	m.recs = nil
	return out, nil
}

func (m *memRebuildSource) Commit(ctx context.Context, rec ConsumerRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commits = append(m.commits, rec)
	return nil
}

func (m *memRebuildSource) Close() error { return nil }

type memRebuildDLQ struct {
	mu   sync.Mutex
	recs []ConsumerRecord
}

func (m *memRebuildDLQ) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, original)
	return nil
}

type memFollowPub struct {
	mu    sync.Mutex
	keys  []string
	vals  [][]byte
	fail  bool
	order *[]string
}

func (m *memFollowPub) PublishRebuildRequest(ctx context.Context, key string, value []byte) error {
	if m.fail {
		return errors.New("follow publish failed")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys = append(m.keys, key)
	m.vals = append(m.vals, append([]byte(nil), value...))
	if m.order != nil {
		*m.order = append(*m.order, "follow")
	}
	return nil
}

type stubExec struct {
	res RebuildPageResult
	err error
}

func (s stubExec) ExecutePage(ctx context.Context, req ParsedAnalyticsProjectionRebuildRequest) (RebuildPageResult, error) {
	return s.res, s.err
}

func TestRebuildConsumer_DLQBeforeCommit(t *testing.T) {
	src := &memRebuildSource{}
	dlq := &memRebuildDLQ{}
	order := []string{}
	c := &AnalyticsProjectionRebuildKafkaConsumer{
		source: &rebuildOrderedSource{inner: src, order: &order},
		dlq:    &rebuildOrderedDLQ{inner: dlq, order: &order},
		exec:   stubExec{},
		cfg: ProjectionRebuildKafkaConfig{
			Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic,
			DLQTopic: DefaultProjectionRebuildDLQTopic, MaxAttempts: 1,
		},
		clock: systemClock{},
	}
	rec := ConsumerRecord{
		Topic: DefaultProjectionRebuildTopic, Partition: 0, Offset: 7,
		Key: []byte("job-1"), Value: []byte(`{"not":"valid"}`),
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(order) < 2 || order[0] != "dlq" || order[1] != "commit" {
		t.Fatalf("order=%v want dlq then commit", order)
	}
}

type rebuildOrderedDLQ struct {
	inner DLQPublisher
	order *[]string
}

func (o *rebuildOrderedDLQ) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	*o.order = append(*o.order, "dlq")
	return o.inner.PublishDLQ(ctx, original, meta)
}

type rebuildOrderedSource struct {
	inner KafkaRecordSource
	order *[]string
}

func (o *rebuildOrderedSource) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	return o.inner.Poll(ctx)
}
func (o *rebuildOrderedSource) Commit(ctx context.Context, rec ConsumerRecord) error {
	*o.order = append(*o.order, "commit")
	return o.inner.Commit(ctx, rec)
}
func (o *rebuildOrderedSource) Close() error { return o.inner.Close() }

func TestRebuildConsumer_FollowUpBeforeCommit(t *testing.T) {
	req := validRebuildEnvelope()
	rec := ConsumerRecord{
		Topic: DefaultProjectionRebuildTopic, Key: []byte("job-1"), Value: req, Offset: 3,
	}
	src := &memRebuildSource{}
	follow := &memFollowPub{}
	order := []string{}
	follow.order = &order
	trackingSrc := &rebuildOrderedSource{inner: src, order: &order}
	followReq, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope(func(m map[string]any) {
		m["pageCursor"] = "next"
		m["eventId"] = "evt_follow"
	}))
	c := &AnalyticsProjectionRebuildKafkaConsumer{
		source: trackingSrc,
		dlq:    &memRebuildDLQ{},
		follow: follow,
		exec: stubExec{res: RebuildPageResult{
			FollowUp: &followReq, FollowUpEventID: "evt_follow",
		}},
		cfg: ProjectionRebuildKafkaConfig{
			Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic, MaxAttempts: 1,
		},
		clock: systemClock{},
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(order) < 2 || order[0] != "follow" || order[1] != "commit" {
		t.Fatalf("order=%v want follow then commit", order)
	}
	if len(follow.keys) != 1 || follow.keys[0] != "job-1" {
		t.Fatalf("follow keys=%v", follow.keys)
	}
}

func TestRebuildConsumer_KeyMustEqualRecoveryJobID(t *testing.T) {
	src := &memRebuildSource{}
	dlq := &memRebuildDLQ{}
	c := &AnalyticsProjectionRebuildKafkaConsumer{
		source: src, dlq: dlq, exec: stubExec{},
		cfg:   ProjectionRebuildKafkaConfig{Group: "g", Topic: DefaultProjectionRebuildTopic, MaxAttempts: 1},
		clock: systemClock{},
	}
	rec := ConsumerRecord{Topic: DefaultProjectionRebuildTopic, Key: []byte("other"), Value: validRebuildEnvelope()}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(dlq.recs) != 1 || len(src.commits) != 1 {
		t.Fatalf("dlq=%d commits=%d", len(dlq.recs), len(src.commits))
	}
}

func TestBackfillClient_AuthAndValidation(t *testing.T) {
	var gotCred string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCred = r.Header.Get("X-Service-Credential")
		if r.URL.Path != "/internal/v1/rooms/analytics-backfill" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		var body AnalyticsBackfillHTTPRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Limit != 1000 || body.SourceTopic != "room.gameplay.metrics" {
			t.Fatalf("%+v", body)
		}
		rec, _ := json.Marshal(map[string]any{
			"eventId": "m1", "eventType": "GameplayMetric", "schemaVersion": 1,
			"correlationId": "c", "occurredAt": "2026-07-01T00:00:00Z",
			"roomId": "room-1", "visibility": "anonymized_adhoc", "metricType": "draw",
		})
		_ = json.NewEncoder(w).Encode(AnalyticsBackfillHTTPResponse{
			Records: []json.RawMessage{rec}, RecoveryJobID: body.RecoveryJobID,
			SourceTopic: body.SourceTopic, SchemaVersion: 1, NextCursor: "n1",
		})
	}))
	defer srv.Close()

	client := &HTTPAnalyticsBackfillClients{
		HTTP: srv.Client(), RoomURL: srv.URL, RoomCred: "pair-room",
		TournURL: "http://t", TournCred: "t", RankURL: "http://r", RankCred: "r",
	}
	req, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope())
	page, evts, err := client.FetchPage(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if gotCred != "pair-room" {
		t.Fatalf("cred=%q", gotCred)
	}
	if page.NextCursor != "n1" || len(evts) != 1 || string(evts[0].EventID) != "m1" {
		t.Fatalf("page=%+v evts=%d", page, len(evts))
	}
}

func TestBackfillClient_RejectsEmptyJobTopicAndBadSchema(t *testing.T) {
	req, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope())
	cases := []struct {
		name string
		page AnalyticsBackfillHTTPResponse
		want string
	}{
		{"empty job", AnalyticsBackfillHTTPResponse{SourceTopic: req.ExpectedSourceTopic, SchemaVersion: 1}, "recoveryJobId"},
		{"empty topic", AnalyticsBackfillHTTPResponse{RecoveryJobID: req.RecoveryJobID, SchemaVersion: 1}, "sourceTopic"},
		{"bad schema", AnalyticsBackfillHTTPResponse{RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic, SchemaVersion: 2}, "schemaVersion"},
		{"cursor loop", AnalyticsBackfillHTTPResponse{
			RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic, SchemaVersion: 1, NextCursor: "cur-1",
		}, "nextCursor"},
	}
	reqLoop := req
	reqLoop.PageCursor = "cur-1"

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page := tc.page
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(page)
			}))
			defer srv.Close()
			client := &HTTPAnalyticsBackfillClients{
				HTTP: srv.Client(), RoomURL: srv.URL, RoomCred: "c",
				TournURL: "http://t", TournCred: "t", RankURL: "http://r", RankCred: "r",
			}
			in := req
			if tc.name == "cursor loop" {
				in = reqLoop
			}
			_, _, err := client.FetchPage(context.Background(), in)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("got %v want containing %q", err, tc.want)
			}
		})
	}
}

func TestBackfillClient_EmptyTerminalPageAllowed(t *testing.T) {
	req, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(AnalyticsBackfillHTTPResponse{
			Records: nil, RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic, SchemaVersion: 1,
		})
	}))
	defer srv.Close()
	client := &HTTPAnalyticsBackfillClients{
		HTTP: srv.Client(), RoomURL: srv.URL, RoomCred: "c",
		TournURL: "http://t", TournCred: "t", RankURL: "http://r", RankCred: "r",
	}
	page, evts, err := client.FetchPage(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != "" || len(evts) != 0 || page.FromCheckpoint != "" {
		t.Fatalf("page=%+v evts=%d", page, len(evts))
	}
}

func TestBackfillClient_RejectsOverMaxRecords(t *testing.T) {
	req, _ := ParseAnalyticsProjectionRebuildRequested(validRebuildEnvelope())
	recs := make([]json.RawMessage, analyticsBackfillMaxLimit+1)
	for i := range recs {
		recs[i] = json.RawMessage(`{}`)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(AnalyticsBackfillHTTPResponse{
			Records: recs, SchemaVersion: 1, RecoveryJobID: req.RecoveryJobID, SourceTopic: req.ExpectedSourceTopic,
		})
	}))
	defer srv.Close()
	client := &HTTPAnalyticsBackfillClients{
		HTTP: srv.Client(), RoomURL: srv.URL, RoomCred: "c",
		TournURL: "http://t", TournCred: "t", RankURL: "http://r", RankCred: "r",
	}
	_, _, err := client.FetchPage(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "hard max") {
		t.Fatalf("got %v", err)
	}
}

func TestVerifyPageContinuityUnit(t *testing.T) {
	pages := []store.RecoveryPageCheckpoint{
		{PageIndex: 0, PageCursor: "", NextPageCursor: "a", Status: store.PageStatusApplied},
		{PageIndex: 1, PageCursor: "a", NextPageCursor: "", Status: store.PageStatusApplied},
	}
	if err := store.VerifyPageContinuityForTest(pages); err != nil {
		t.Fatal(err)
	}
	pages[1].PageCursor = "b"
	if err := store.VerifyPageContinuityForTest(pages); err == nil {
		t.Fatal("expected gap")
	}
}

func TestAdHocRebuildDisabledByDefault(t *testing.T) {
	s := &store.AnalyticsStore{}
	_, err := s.Rebuild(context.Background(), nil)
	if !errors.Is(err, store.ErrAdHocRebuildDisabled) {
		t.Fatalf("got %v", err)
	}
}

func TestDeterministicFollowUpEventIDStable(t *testing.T) {
	a := store.DeterministicFollowUpEventID("job", "room.gameplay.metrics", "cur")
	b := store.DeterministicFollowUpEventID("job", "room.gameplay.metrics", "cur")
	if a != b || a == "" {
		t.Fatalf("%q %q", a, b)
	}
}

// Ensure unused domain import in stub stays quiet for compile when editing.
var _ = domain.OutcomeAccepted
