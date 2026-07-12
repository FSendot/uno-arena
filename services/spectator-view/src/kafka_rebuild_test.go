package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"unoarena/services/spectator-view/domain"

	"unoarena/services/spectator-view/store"
)

func canonicalRebuildJSON(mut ...func(map[string]any)) []byte {
	m := map[string]any{
		"schemaVersion":       1,
		"eventId":             "rebuild-evt-1",
		"eventType":           EventTypeProjectionRebuildReq,
		"correlationId":       "corr-rebuild-1",
		"occurredAt":          time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"recoveryJobId":       "job-42",
		"roomId":              "room-42",
		"failedCheckpoint":    float64(10),
		"expectedSourceTopic": ExpectedSpectatorSafeSourceTopic,
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

func TestParseSpectatorProjectionRebuildRequested_Strict(t *testing.T) {
	t.Parallel()
	ok, err := ParseSpectatorProjectionRebuildRequested(canonicalRebuildJSON())
	if err != nil {
		t.Fatal(err)
	}
	if ok.RoomID != "room-42" || ok.RecoveryJobID != "job-42" || ok.FailedCheckpoint != 10 {
		t.Fatalf("%+v", ok)
	}
	if ok.IdempotencyKey() != "job-42|room-42|10" {
		t.Fatalf("idemp=%s", ok.IdempotencyKey())
	}

	cases := []struct {
		name string
		mut  func(map[string]any)
	}{
		{"events_forbidden", func(m map[string]any) { m["events"] = []any{} }},
		{"heldEvents_forbidden", func(m map[string]any) { m["heldEvents"] = []any{} }},
		{"wrong_event_type", func(m map[string]any) { m["eventType"] = "RoomCreated" }},
		{"wrong_source", func(m map[string]any) { m["expectedSourceTopic"] = "other" }},
		{"schema_2", func(m map[string]any) { m["schemaVersion"] = float64(2) }},
		{"failed_zero", func(m map[string]any) { m["failedCheckpoint"] = float64(0) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSpectatorProjectionRebuildRequested(canonicalRebuildJSON(tc.mut))
			if err == nil || !IsTerminalKafkaConsumeError(err) {
				t.Fatalf("want terminal, got %v", err)
			}
		})
	}
}

func TestWireProjectionRebuildWorker_FailClosed(t *testing.T) {
	clearWorkerEnv := func() {
		for _, k := range []string{
			"REDIS_URL", "ROOM_GAMEPLAY_URL", "ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL",
			"KAFKA_BROKERS", "KAFKA_PROJECTION_REBUILD_GROUP", "KAFKA_PROJECTION_REBUILD_TOPIC",
			"KAFKA_PROJECTION_REBUILD_DLQ_TOPIC", "KAFKA_SPECTATOR_SAFE_DLQ_TOPIC",
		} {
			t.Setenv(k, "")
		}
	}
	t.Run("missing_redis", func(t *testing.T) {
		clearWorkerEnv()
		t.Setenv("ROOM_GAMEPLAY_URL", "http://room")
		t.Setenv("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", "cred")
		t.Setenv("KAFKA_BROKERS", "localhost:9092")
		_, err := wireProjectionRebuildWorker()
		if err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("missing_room_url", func(t *testing.T) {
		clearWorkerEnv()
		t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/5")
		t.Setenv("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", "cred")
		t.Setenv("KAFKA_BROKERS", "localhost:9092")
		_, err := wireProjectionRebuildWorker()
		if err == nil || !strings.Contains(err.Error(), "ROOM_GAMEPLAY_URL") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("missing_cred", func(t *testing.T) {
		clearWorkerEnv()
		t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/5")
		t.Setenv("ROOM_GAMEPLAY_URL", "http://room")
		t.Setenv("KAFKA_BROKERS", "localhost:9092")
		_, err := wireProjectionRebuildWorker()
		if err == nil || !strings.Contains(err.Error(), "ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("missing_brokers", func(t *testing.T) {
		clearWorkerEnv()
		t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/5")
		t.Setenv("ROOM_GAMEPLAY_URL", "http://room")
		t.Setenv("ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL", "cred")
		_, err := wireProjectionRebuildWorker()
		if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestWorkerRoleDispatch_Structure(t *testing.T) {
	t.Parallel()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	mainSrc, err := os.ReadFile(filepath.Join(filepath.Dir(file), "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	workerSrc, err := os.ReadFile(filepath.Join(filepath.Dir(file), "projection_rebuild_worker.go"))
	if err != nil {
		t.Fatal(err)
	}
	main := string(mainSrc)
	worker := string(workerSrc)
	for _, needle := range []string{
		"workerRoleSpectatorProjectionRebuilder",
		"wireProjectionRebuildWorker",
		"runProjectionRebuildWorker",
	} {
		if !strings.Contains(main, needle) {
			t.Fatalf("main.go missing %q", needle)
		}
	}
	for _, needle := range []string{
		`WORKER_ROLE=%s requires REDIS_URL`,
		`ROOM_GAMEPLAY_URL`,
		`ROOM_SPECTATOR_RECOVERY_SERVICE_CREDENTIAL`,
		"NewRedisRebuildIdempotency",
		"KafkaHeldSpectatorDLQSource",
		"StoreBackedProjectionRebuildExecutor",
		"signal.NotifyContext",
	} {
		if !strings.Contains(worker, needle) {
			t.Fatalf("projection_rebuild_worker.go missing %q", needle)
		}
	}
	// Worker path must not start HTTP ListenAndServe.
	if strings.Contains(worker, "ListenAndServe") {
		t.Fatal("worker must remain HTTP-free")
	}
}

type fakeRebuildSource struct {
	mu      sync.Mutex
	recs    []ConsumerRecord
	commits []ConsumerRecord
}

func (f *fakeRebuildSource) Poll(ctx context.Context) ([]ConsumerRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.recs) == 0 {
		return nil, nil
	}
	out := f.recs
	f.recs = nil
	return out, nil
}

func (f *fakeRebuildSource) Commit(ctx context.Context, rec ConsumerRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, rec)
	return nil
}

func (f *fakeRebuildSource) Close() error { return nil }

type fakeRebuildDLQ struct {
	mu   sync.Mutex
	pubs []ConsumerRecord
	meta []DLQFailureMeta
	err  error
}

func (f *fakeRebuildDLQ) PublishDLQ(ctx context.Context, original ConsumerRecord, meta DLQFailureMeta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.pubs = append(f.pubs, original)
	f.meta = append(f.meta, meta)
	return nil
}

type fakeRebuildExec struct {
	err   error
	calls atomic.Int32
	delay time.Duration
}

func (f *fakeRebuildExec) ExecuteRebuild(ctx context.Context, req ParsedProjectionRebuildRequest) error {
	f.calls.Add(1)
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.delay):
		}
	}
	return f.err
}

func TestProjectionRebuildKafkaConsumer_IdempotencyRetriesAndDLQBeforeCommit(t *testing.T) {
	t.Parallel()
	rec := ConsumerRecord{
		Topic: DefaultProjectionRebuildTopic, Partition: 0, Offset: 7,
		Key: []byte("room-42"), Value: canonicalRebuildJSON(),
	}

	t.Run("success_marks_idempotent_and_commits", func(t *testing.T) {
		src := &fakeRebuildSource{recs: []ConsumerRecord{rec}}
		dlq := &fakeRebuildDLQ{}
		idemp := NewMemoryRebuildIdempotency()
		c := &ProjectionRebuildKafkaConsumer{
			source: src, dlq: dlq, exec: &fakeRebuildExec{}, idemp: idemp,
			cfg: ProjectionRebuildKafkaConfig{
				Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic,
				MaxAttempts: 3, RetryBackoff: time.Millisecond,
			},
			clock: systemClock{},
			sleep: func(ctx context.Context, d time.Duration) error { return nil },
		}
		if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
			t.Fatal(err)
		}
		done, err := idemp.AlreadyDone(context.Background(), "job-42|room-42|10")
		if err != nil || !done {
			t.Fatalf("idempotent done=%v err=%v", done, err)
		}
		if len(src.commits) != 1 || len(dlq.pubs) != 0 {
			t.Fatalf("commits=%d dlq=%d", len(src.commits), len(dlq.pubs))
		}
		// Second delivery skips exec.
		exec2 := &fakeRebuildExec{}
		c.exec = exec2
		if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
			t.Fatal(err)
		}
		if exec2.calls.Load() != 0 {
			t.Fatal("idempotent hit must skip rebuild")
		}
		if len(src.commits) != 2 {
			t.Fatalf("commits=%d", len(src.commits))
		}
	})

	t.Run("retries_then_dlq_before_commit", func(t *testing.T) {
		src := &fakeRebuildSource{}
		dlq := &fakeRebuildDLQ{}
		exec := &fakeRebuildExec{err: errors.New("transient redis blip")}
		c := &ProjectionRebuildKafkaConsumer{
			source: src, dlq: dlq, exec: exec, idemp: NewMemoryRebuildIdempotency(),
			cfg: ProjectionRebuildKafkaConfig{
				Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic,
				MaxAttempts: 3, RetryBackoff: time.Millisecond,
			},
			clock: systemClock{},
			sleep: func(ctx context.Context, d time.Duration) error { return nil },
		}
		if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
			t.Fatal(err)
		}
		if exec.calls.Load() != 3 {
			t.Fatalf("calls=%d", exec.calls.Load())
		}
		if len(dlq.pubs) != 1 {
			t.Fatalf("dlq pubs=%d", len(dlq.pubs))
		}
		if len(src.commits) != 1 {
			t.Fatalf("commit after dlq want 1 got %d", len(src.commits))
		}
	})

	t.Run("dlq_failure_blocks_commit", func(t *testing.T) {
		src := &fakeRebuildSource{}
		dlq := &fakeRebuildDLQ{err: errors.New("broker down")}
		c := &ProjectionRebuildKafkaConsumer{
			source: src, dlq: dlq,
			exec:  &fakeRebuildExec{err: newTerminalKafkaError(KafkaFailureSchemaInvalid, errors.New("bad"))},
			idemp: NewMemoryRebuildIdempotency(),
			cfg: ProjectionRebuildKafkaConfig{
				Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic,
				MaxAttempts: 1, RetryBackoff: time.Millisecond,
			},
			clock: systemClock{},
		}
		bad := ConsumerRecord{Topic: DefaultProjectionRebuildTopic, Partition: 0, Offset: 1, Key: []byte("room-42"), Value: []byte("{")}
		err := c.ProcessBatch(context.Background(), []ConsumerRecord{bad})
		if err == nil || !strings.Contains(err.Error(), "dlq publish") {
			t.Fatalf("err=%v", err)
		}
		if len(src.commits) != 0 {
			t.Fatal("must not commit without DLQ ack")
		}
	})
}

func TestHTTPRoomSpectatorRecoveryClient_MappingAndPrivacy(t *testing.T) {
	t.Parallel()
	var sawCred string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawCred = r.Header.Get(internalCredentialHeader)
		if r.URL.Query().Get("failedCheckpoint") != "10" || r.URL.Query().Get("recoveryJobId") != "job-42" {
			t.Fatalf("query=%s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schemaVersion":    1,
			"roomId":           "room-42",
			"recoveryJobId":    "job-42",
			"failedCheckpoint": 10,
			"sequenceNumber":   10,
			"resumeCheckpoint": 10,
			"status":           "waiting",
			"visibility":       "public",
			"privateHand":      []any{"secret"}, // must be stripped by Room; Spectator still applies policy
			"roster": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
			},
		})
	}))
	defer srv.Close()

	client := NewHTTPRoomSpectatorRecoveryClient(srv.URL, "scoped-recovery-cred")
	snap, err := client.FetchSpectatorRecoverySnapshot(context.Background(), "room-42", 10, "job-42")
	if err != nil {
		t.Fatal(err)
	}
	if sawCred != "scoped-recovery-cred" {
		t.Fatalf("cred=%q", sawCred)
	}
	evt := snap.ToSnapshotSanitizedEvent()
	if evt.EventType != domain.EventSnapshotSanitized || evt.Sequence != 10 {
		t.Fatalf("%+v", evt)
	}
	// Domain privacy still drops private fields when applied.
	proj, outcomes, err := domain.RebuildFromRecoverySnapshot(domain.RoomID("room-42"), evt, nil)
	if err == nil {
		t.Fatal("privateHand in payload must fail privacy validation")
	}
	_ = proj
	_ = outcomes

	// Clean public payload maps successfully.
	clean := snap
	delete(clean.Payload, "privateHand")
	evt2 := clean.ToSnapshotSanitizedEvent()
	if _, _, err := domain.RebuildFromRecoverySnapshot(domain.RoomID("room-42"), evt2, nil); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryRebuildIdempotency_DurableKeyShape(t *testing.T) {
	t.Parallel()
	id := NewMemoryRebuildIdempotency()
	key := ParsedProjectionRebuildRequest{RecoveryJobID: "j", RoomID: "r", FailedCheckpoint: 3}.IdempotencyKey()
	done, err := id.AlreadyDone(context.Background(), key)
	if err != nil || done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if err := id.MarkDone(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	done, err = id.AlreadyDone(context.Background(), key)
	if err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
}

func TestRedisRebuildIdempotency_DurableRoundTrip(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	id := NewRedisRebuildIdempotency(rdb, "spectator:")
	key := "job|room|9"
	done, err := id.AlreadyDone(context.Background(), key)
	if err != nil || done {
		t.Fatalf("before: done=%v err=%v", done, err)
	}
	if err := id.MarkDone(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	done, err = id.AlreadyDone(context.Background(), key)
	if err != nil || !done {
		t.Fatalf("after: done=%v err=%v", done, err)
	}
	want := store.NewKeySpace("spectator:").RebuildDone(domain.RoomID("room"), key)
	if !mr.Exists(want) {
		t.Fatalf("expected redis key %s", want)
	}
}

func TestRedisRebuildIdempotency_KeyPrefix(t *testing.T) {
	t.Parallel()
	r := NewRedisRebuildIdempotency(nil, "spectator:")
	got, err := r.key("a|b|1")
	if err != nil {
		t.Fatal(err)
	}
	want := store.NewKeySpace("spectator:").RebuildDone(domain.RoomID("b"), "a|b|1")
	if got != want {
		t.Fatalf("key=%s want=%s", got, want)
	}
	_, err = r.AlreadyDone(context.Background(), "x")
	if err == nil {
		t.Fatal("nil redis must fail closed")
	}
}

func TestStoreBackedExecutor_StaleIsSuccess(t *testing.T) {
	// Structure: executor treats Stale as nil error (newer live Apply won).
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "kafka_rebuild_consumer.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	if !strings.Contains(src, "if res.Stale") || !strings.Contains(src, "return nil") {
		t.Fatal("stale fencing must succeed without DLQ")
	}
	if !strings.Contains(src, "failureToDLQAndCommit") {
		t.Fatal("DLQ-before-commit path required")
	}
	if !strings.Contains(src, "MarksRebuildDoneAtomically") {
		t.Fatal("production executor must skip external MarkDone when atomic")
	}
	_ = store.ErrRecoveryConflict
}

func TestStoreBackedProjectionRebuildExecutor_AtomicIdempotencyCrashWindow(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	prefix := "speccrash:"
	rs := store.NewRedisProjectionStore(rdb, prefix)
	rs = rs.WithKafkaIdentity(DefaultSpectatorKafkaGroup, DefaultSpectatorSafeTopic)
	ctx := context.Background()
	if err := rs.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	room := domain.RoomID("room-crash")
	out, err := rs.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "seed", EventType: domain.EventRoomCreated, SchemaVersion: 1,
		RoomID: room, Sequence: 1,
		Payload: map[string]any{
			"visibility": "public",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			},
		},
	}})
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("seed: %+v err=%v", out, err)
	}
	if err := rs.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup: DefaultSpectatorKafkaGroup, SourceTopic: DefaultSpectatorSafeTopic,
		AggregateKey: string(room), Classification: store.QuarantineClassApplication,
		Reason: "gap", QuarantinedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	req := ParsedProjectionRebuildRequest{
		RoomID: string(room), RecoveryJobID: "job-crash", FailedCheckpoint: 1,
		CorrelationID: "corr-crash",
	}
	exec := &StoreBackedProjectionRebuildExecutor{
		Room: stubRoomRecovery{snap: RoomSpectatorRecoverySnapshot{
			SchemaVersion: 1, RoomID: string(room), RecoveryJobID: req.RecoveryJobID,
			FailedCheckpoint: req.FailedCheckpoint, SequenceNumber: 1, ResumeCheckpoint: 1,
			Payload: map[string]any{
				"status": "waiting", "visibility": "public",
				"roster": []any{
					map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
				},
			},
		}},
		Held:  stubHeldSource{},
		Store: rs,
		Release: store.RecoveryRelease{
			Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven,
		},
	}
	if !exec.MarksRebuildDoneAtomically() {
		t.Fatal("production executor must mark done atomically in Redis Lua")
	}
	if err := exec.ExecuteRebuild(ctx, req); err != nil {
		t.Fatal(err)
	}

	// Crash window: executor succeeded; consumer never called MarkDone.
	idemp := NewRedisRebuildIdempotency(rdb, prefix)
	done, err := idemp.AlreadyDone(ctx, req.IdempotencyKey())
	if err != nil || !done {
		t.Fatalf("atomic marker must exist without external MarkDone: done=%v err=%v", done, err)
	}
	storeDone, err := rs.IsRebuildIdempotencyDone(ctx, room, req.IdempotencyKey())
	if err != nil || !storeDone {
		t.Fatalf("store marker missing: done=%v err=%v", storeDone, err)
	}

	fields, err := rs.KafkaQuarantineFields(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if fields["active"] != "0" {
		t.Fatalf("quarantine should be released: %v", fields)
	}
	releasedAt := fields["released_at"]

	// Duplicate worker / rebalance: same identity succeeds, no conflict/DLQ, audit intact.
	if err := exec.ExecuteRebuild(ctx, req); err != nil {
		t.Fatalf("duplicate must be idempotent success, got %v", err)
	}
	gen, seq, err := rs.CurrentFence(ctx, room)
	if err != nil || gen != "2" || seq != 1 {
		t.Fatalf("duplicate must not regress fence gen=%s seq=%d err=%v", gen, seq, err)
	}
	after, err := rs.KafkaQuarantineFields(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if after["released_at"] != releasedAt || after["release_note"] != store.ReleaseNoteRecoveryContinuityProven {
		t.Fatalf("quarantine audit corrupted: before released_at=%s after=%v", releasedAt, after)
	}

	rec := ConsumerRecord{
		Topic: DefaultProjectionRebuildTopic, Partition: 0, Offset: 9,
		Key: []byte(room), Value: canonicalRebuildJSON(func(m map[string]any) {
			m["roomId"] = string(room)
			m["recoveryJobId"] = req.RecoveryJobID
			m["failedCheckpoint"] = float64(req.FailedCheckpoint)
			m["correlationId"] = req.CorrelationID
		}),
	}
	src := &fakeRebuildSource{}
	dlq := &fakeRebuildDLQ{}
	c := &ProjectionRebuildKafkaConsumer{
		source: src, dlq: dlq, exec: exec, idemp: idemp,
		cfg: ProjectionRebuildKafkaConfig{
			Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic,
			MaxAttempts: 3, RetryBackoff: time.Millisecond,
		},
		clock: systemClock{},
		sleep: func(ctx context.Context, d time.Duration) error { return nil },
	}
	if err := c.ProcessBatch(ctx, []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if len(dlq.pubs) != 0 {
		t.Fatalf("duplicate must not DLQ: %d", len(dlq.pubs))
	}
	if len(src.commits) != 1 {
		t.Fatalf("want commit without MarkDone, commits=%d", len(src.commits))
	}
}

func TestStore_RecoveryAtomicIdempotency_TwoWorkersMiniredis(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	rs := store.NewRedisProjectionStore(rdb, "specrace:")
	rs = rs.WithKafkaIdentity(DefaultSpectatorKafkaGroup, DefaultSpectatorSafeTopic)
	ctx := context.Background()
	if err := rs.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	room := domain.RoomID("room-race2")
	out, err := rs.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "seed", EventType: domain.EventRoomCreated, SchemaVersion: 1,
		RoomID: room, Sequence: 1,
		Payload: map[string]any{
			"visibility": "public",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			},
		},
	}})
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("seed: %+v err=%v", out, err)
	}
	if err := rs.QuarantineKafkaAggregate(ctx, store.KafkaAggregateQuarantine{
		ConsumerGroup: DefaultSpectatorKafkaGroup, SourceTopic: DefaultSpectatorSafeTopic,
		AggregateKey: string(room), Classification: store.QuarantineClassApplication,
		Reason: "gap", QuarantinedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	identity := "job-race|" + string(room) + "|1"
	idemp := store.RecoveryIdempotency{Identity: identity}
	release := store.RecoveryRelease{Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven}
	snap := domain.SpectatorSafeEvent{
		EventID: "snap_race", EventType: domain.EventSnapshotSanitized, SchemaVersion: 1,
		RoomID: room, Sequence: 1,
		Payload: map[string]any{
			"status": "waiting", "visibility": "public",
			"roster": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
			},
		},
	}

	var wg sync.WaitGroup
	var mutated, already atomic.Int64
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := rs.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room, snap, "1", 1, nil, release, idemp)
			if err != nil {
				errs <- err
				return
			}
			if res.AlreadyDone {
				already.Add(1)
				return
			}
			if res.Stale {
				errs <- fmt.Errorf("unexpected stale")
				return
			}
			mutated.Add(1)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("worker: %v", err)
	}
	if mutated.Load() != 1 || already.Load() != 1 {
		t.Fatalf("want 1 mutation + 1 already_done, mutated=%d already=%d", mutated.Load(), already.Load())
	}
	done, err := rs.IsRebuildIdempotencyDone(ctx, room, identity)
	if err != nil || !done {
		t.Fatalf("marker: done=%v err=%v", done, err)
	}
	ok, err := rs.IsKafkaAggregateQuarantined(ctx, DefaultSpectatorKafkaGroup, DefaultSpectatorSafeTopic, string(room))
	if err != nil || ok {
		t.Fatalf("quarantine released: ok=%v err=%v", ok, err)
	}
	fields, err := rs.KafkaQuarantineFields(ctx, room)
	if err != nil {
		t.Fatal(err)
	}
	if fields["active"] != "0" || fields["release_note"] != store.ReleaseNoteRecoveryContinuityProven {
		t.Fatalf("audit=%v", fields)
	}
}

func TestStore_RecoveryAtomicIdempotency_MarkerOnlyOnSuccessMiniredis(t *testing.T) {
	t.Parallel()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	rs := store.NewRedisProjectionStore(rdb, "specmark:")
	ctx := context.Background()
	if err := rs.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	room := domain.RoomID("room-mark")
	_, err = rs.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "seed", EventType: domain.EventRoomCreated, SchemaVersion: 1,
		RoomID: room, Sequence: 1,
		Payload: map[string]any{
			"visibility": "public",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	identity := "job-fail|" + string(room) + "|1"
	snap := domain.SpectatorSafeEvent{
		EventID: "snap1", EventType: domain.EventSnapshotSanitized, SchemaVersion: 1,
		RoomID: room, Sequence: 1,
		Payload: map[string]any{
			"status": "waiting", "visibility": "public",
			"roster": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
			},
		},
	}
	release := store.RecoveryRelease{Enabled: true, Note: store.ReleaseNoteRecoveryContinuityProven}

	_, err = rs.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room, snap, "1", 1,
		[]domain.SpectatorSafeEvent{{
			EventID: "gap", EventType: domain.EventRoomLocked, SchemaVersion: 1,
			RoomID: room, Sequence: 3, Payload: map[string]any{"status": "locked"},
		}}, release, store.RecoveryIdempotency{Identity: identity})
	if !errors.Is(err, domain.ErrHeldContinuityGap) {
		t.Fatalf("want gap, got %v", err)
	}
	done, err := rs.IsRebuildIdempotencyDone(ctx, room, identity)
	if err != nil || done {
		t.Fatalf("gap must not mark: done=%v err=%v", done, err)
	}

	_, _ = rs.Apply(ctx, room, []domain.SpectatorSafeEvent{{
		EventID: "live2", EventType: domain.EventRoomLocked, SchemaVersion: 1,
		RoomID: room, Sequence: 2, Payload: map[string]any{"status": "locked"},
	}})
	res, err := rs.RecoveryRebuildFromSnapshotWithIdempotency(ctx, room, snap, "1", 1, nil, release,
		store.RecoveryIdempotency{Identity: identity})
	if err != nil || !res.Stale {
		t.Fatalf("want stale: %+v err=%v", res, err)
	}
	done, err = rs.IsRebuildIdempotencyDone(ctx, room, identity)
	if err != nil || done {
		t.Fatalf("stale must not mark: done=%v err=%v", done, err)
	}
}

func TestProjectionRebuildKafkaConsumer_SkipsMarkDoneForAtomicExecutor(t *testing.T) {
	t.Parallel()
	rec := ConsumerRecord{
		Topic: DefaultProjectionRebuildTopic, Partition: 0, Offset: 7,
		Key: []byte("room-42"), Value: canonicalRebuildJSON(),
	}
	src := &fakeRebuildSource{}
	dlq := &fakeRebuildDLQ{}
	marking := &countingIdempotency{inner: NewMemoryRebuildIdempotency()}
	c := &ProjectionRebuildKafkaConsumer{
		source: src, dlq: dlq,
		exec:  atomicFakeRebuildExec{},
		idemp: marking,
		cfg: ProjectionRebuildKafkaConfig{
			Group: DefaultProjectionRebuildGroup, Topic: DefaultProjectionRebuildTopic,
			MaxAttempts: 1, RetryBackoff: time.Millisecond,
		},
		clock: systemClock{},
	}
	if err := c.ProcessBatch(context.Background(), []ConsumerRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if marking.markCalls.Load() != 0 {
		t.Fatalf("atomic executor must not call external MarkDone, calls=%d", marking.markCalls.Load())
	}
	if len(src.commits) != 1 || len(dlq.pubs) != 0 {
		t.Fatalf("commits=%d dlq=%d", len(src.commits), len(dlq.pubs))
	}
}

type countingIdempotency struct {
	inner     RebuildIdempotencyStore
	markCalls atomic.Int32
}

func (c *countingIdempotency) AlreadyDone(ctx context.Context, key string) (bool, error) {
	return c.inner.AlreadyDone(ctx, key)
}

func (c *countingIdempotency) MarkDone(ctx context.Context, key string) error {
	c.markCalls.Add(1)
	return c.inner.MarkDone(ctx, key)
}

type atomicFakeRebuildExec struct{}

func (atomicFakeRebuildExec) ExecuteRebuild(ctx context.Context, req ParsedProjectionRebuildRequest) error {
	return nil
}

func (atomicFakeRebuildExec) MarksRebuildDoneAtomically() bool { return true }
