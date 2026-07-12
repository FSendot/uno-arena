package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

type stubSnapshotPublisher struct {
	calls   atomic.Int32
	res     store.ClaimedBoardPublish
	err     error
	block   chan struct{}
	sawCtx  atomic.Pointer[context.Context]
	started chan struct{}
}

func (s *stubSnapshotPublisher) PublishNextDirtyLeaderboardSnapshot(ctx context.Context, _ time.Duration) (store.ClaimedBoardPublish, error) {
	s.calls.Add(1)
	cp := ctx
	s.sawCtx.Store(&cp)
	if s.started != nil {
		select {
		case <-s.started:
		default:
			close(s.started)
		}
	}
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return store.ClaimedBoardPublish{}, ctx.Err()
		}
	}
	return s.res, s.err
}

func TestLeaderboardSnapshotterWorker_StopWaitsInflight(t *testing.T) {
	pub := &stubSnapshotPublisher{res: store.ClaimedBoardPublish{Published: false}}
	w := NewLeaderboardSnapshotterWorker(pub)
	w.pollInterval = 20 * time.Millisecond
	w.Start()
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}
	if pub.calls.Load() < 1 {
		t.Fatal("expected at least one tick")
	}
}

func TestLeaderboardSnapshotterWorker_PublishUsesBoundedContext(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	pub := &stubSnapshotPublisher{
		res:     store.ClaimedBoardPublish{Published: false},
		block:   block,
		started: started,
	}
	w := NewLeaderboardSnapshotterWorker(pub)
	w.pollInterval = 5 * time.Millisecond
	w.publishTimeout = 40 * time.Millisecond
	w.stopGrace = 200 * time.Millisecond
	w.Start()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("publish never started")
	}
	ctxPtr := pub.sawCtx.Load()
	if ctxPtr == nil {
		t.Fatal("missing publish context")
	}
	deadline, ok := (*ctxPtr).Deadline()
	if !ok {
		t.Fatal("publish context must be bounded with a deadline")
	}
	if remaining := time.Until(deadline); remaining > w.publishTimeout+20*time.Millisecond || remaining < 0 {
		t.Fatalf("publish deadline remaining=%v timeout=%v", remaining, w.publishTimeout)
	}

	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop must return within stop grace even if publish blocks on cancel")
	}
}

func TestLeaderboardSnapshotterWorker_StopGraceIsFinite(t *testing.T) {
	started := make(chan struct{})
	// Never closes: publish ignores cancel to prove Stop still bounds waiting.
	block := make(chan struct{})
	pub := &stubSnapshotPublisher{
		block:   block,
		started: started,
		res:     store.ClaimedBoardPublish{Published: false},
	}
	w := NewLeaderboardSnapshotterWorker(pub)
	w.pollInterval = 5 * time.Millisecond
	w.publishTimeout = time.Hour // cancel won't free a ctx-ignoring stub
	w.stopGrace = 50 * time.Millisecond
	w.Start()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("publish never started")
	}

	start := time.Now()
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung indefinitely")
	}
	elapsed := time.Since(start)
	if elapsed < w.stopGrace {
		t.Fatalf("Stop returned too early elapsed=%v grace=%v", elapsed, w.stopGrace)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Stop grace not bounded elapsed=%v", elapsed)
	}
	close(block) // release leaked publish goroutine after bounded Stop
}

func TestSnapshotterCooldownFromEnv(t *testing.T) {
	t.Setenv("LEADERBOARD_SNAPSHOT_COALESCE_SECONDS", "30")
	if got := snapshotterCooldownFromEnv(); got != 30*time.Second {
		t.Fatalf("got %v", got)
	}
	t.Setenv("LEADERBOARD_SNAPSHOT_COALESCE_SECONDS", "")
	if got := snapshotterCooldownFromEnv(); got != defaultSnapshotterCooldown {
		t.Fatalf("default got %v", got)
	}
}

func TestLeaderboardSnapshotterWorker_PublishesWhenDirty(t *testing.T) {
	pub := &stubSnapshotPublisher{res: store.ClaimedBoardPublish{
		Published: true, BoardType: domain.SourceCasualElo, SnapshotID: "casual_elo:v1", ClaimedDirty: 1, EntryCount: 2,
	}}
	w := NewLeaderboardSnapshotterWorker(pub)
	w.pollInterval = 10 * time.Millisecond
	w.Start()
	deadline := time.Now().Add(time.Second)
	for pub.calls.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	w.Stop()
	if pub.calls.Load() < 1 {
		t.Fatal("expected publish attempt")
	}
}

func TestSnapshotterSource_BoundedPublishAndNoProcessClock(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "snapshotter.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, needle := range []string{
		"context.WithTimeout",
		"publishTimeout",
		"stopGrace",
		"PublishNextDirtyLeaderboardSnapshot(ctx, w.cooldown)",
	} {
		if !strings.Contains(src, needle) {
			t.Fatalf("snapshotter.go missing %q", needle)
		}
	}
	if strings.Contains(src, "context.Background(), w.clock()") {
		t.Fatal("publish must not use Background+process clock")
	}
	if !strings.Contains(src, "time.After(w.stopGrace)") {
		t.Fatal("Stop must bound inflight wait with stopGrace")
	}
	if strings.Contains(src, "clock        func() time.Time") {
		t.Fatal("snapshotter must not carry a process wall clock for publication cadence")
	}
}

func TestShutdownLeaderboardSnapshotter_StopsBeforePoolClose(t *testing.T) {
	var mu sync.Mutex
	var events []string
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})

	done := make(chan struct{})
	go func() {
		shutdownLeaderboardSnapshotter(
			func() {
				mu.Lock()
				events = append(events, "stop-enter")
				mu.Unlock()
				close(stopEntered)
				<-releaseStop
				mu.Lock()
				events = append(events, "stop-exit")
				mu.Unlock()
			},
			func() {
				mu.Lock()
				events = append(events, "close")
				mu.Unlock()
			},
		)
		close(done)
	}()

	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("stop never entered")
	}
	mu.Lock()
	if len(events) != 1 || events[0] != "stop-enter" {
		mu.Unlock()
		t.Fatalf("pool close must wait for stop: events=%v", events)
	}
	mu.Unlock()

	close(releaseStop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown hung")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"stop-enter", "stop-exit", "close"}
	if len(events) != len(want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events=%v want=%v", events, want)
		}
	}
}

func TestShutdownLeaderboardSnapshotter_NilCloseIsSafe(t *testing.T) {
	called := false
	shutdownLeaderboardSnapshotter(func() { called = true }, nil)
	if !called {
		t.Fatal("stop must still run")
	}
}
