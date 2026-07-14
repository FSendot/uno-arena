package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"unoarena/services/ranking/store"
)

const (
	workerRoleLeaderboardSnapshotter = "ranking-leaderboard-snapshotter"
	defaultSnapshotterPollInterval   = 500 * time.Millisecond
	defaultSnapshotterCooldown       = store.DefaultLeaderboardSnapshotCooldown
	defaultSnapshotterPublishTimeout = 30 * time.Second
	defaultSnapshotterStopGrace      = 35 * time.Second
)

// LeaderboardSnapshotPublisher is the store surface used by the snapshotter worker.
type LeaderboardSnapshotPublisher interface {
	PublishNextDirtyLeaderboardSnapshot(ctx context.Context, cooldown time.Duration) (store.ClaimedBoardPublish, error)
}

// LeaderboardSnapshotterWorker claims dirty boards and publishes coalesced top-100 snapshots.
type LeaderboardSnapshotterWorker struct {
	pub            LeaderboardSnapshotPublisher
	cooldown       time.Duration
	pollInterval   time.Duration
	publishTimeout time.Duration
	stopGrace      time.Duration

	stopCh   chan struct{}
	doneCh   chan struct{}
	inflight sync.WaitGroup
	stopped  atomic.Bool
}

// NewLeaderboardSnapshotterWorker constructs a DB-only snapshot publication loop.
func NewLeaderboardSnapshotterWorker(pub LeaderboardSnapshotPublisher) *LeaderboardSnapshotterWorker {
	return &LeaderboardSnapshotterWorker{
		pub:            pub,
		cooldown:       defaultSnapshotterCooldown,
		pollInterval:   defaultSnapshotterPollInterval,
		publishTimeout: defaultSnapshotterPublishTimeout,
		stopGrace:      defaultSnapshotterStopGrace,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
}

// Start begins the claim/publish loop in a background goroutine.
func (w *LeaderboardSnapshotterWorker) Start() {
	go w.loop()
}

// Stop stops new claims and waits up to stopGrace for an in-flight publication.
func (w *LeaderboardSnapshotterWorker) Stop() {
	if w == nil || w.stopped.Swap(true) {
		return
	}
	close(w.stopCh)
	done := make(chan struct{})
	go func() {
		<-w.doneCh
		w.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(w.stopGrace):
		processLogger().WarnContext(context.Background(), "snapshotter stop grace elapsed",
			"event", "leaderboard_snapshotter_stop_grace_elapsed", "graceMs", w.stopGrace.Milliseconds())
	}
}

func (w *LeaderboardSnapshotterWorker) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			if w.stopped.Load() {
				return
			}
			w.tick()
		}
	}
}

func (w *LeaderboardSnapshotterWorker) tick() {
	// Count before the stopped check so Stop's WaitGroup covers this publication.
	w.inflight.Add(1)
	defer w.inflight.Done()
	if w.stopped.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.publishTimeout)
	defer cancel()
	res, err := w.pub.PublishNextDirtyLeaderboardSnapshot(ctx, w.cooldown)
	if err != nil {
		processLogger().WarnContext(ctx, "leaderboard snapshot publication failed",
			"event", "leaderboard_snapshot_publish_failed", "error", err.Error())
		return
	}
	if !res.Published {
		return
	}
	processLogger().InfoContext(ctx, "leaderboard snapshot published",
		"event", "leaderboard_snapshot_published", "boardType", string(res.BoardType),
		"snapshotId", res.SnapshotID, "dirtyVersion", res.ClaimedDirty, "entries", res.EntryCount)
}

func snapshotterCooldownFromEnv() time.Duration {
	v := os.Getenv("LEADERBOARD_SNAPSHOT_COALESCE_SECONDS")
	if v == "" {
		return defaultSnapshotterCooldown
	}
	secs, err := time.ParseDuration(v + "s")
	if err != nil || secs <= 0 {
		return defaultSnapshotterCooldown
	}
	return secs
}

// shutdownLeaderboardSnapshotter stops/waits the worker before closing the pool.
// Order is intentional: never close DB under an in-flight publish.
func shutdownLeaderboardSnapshotter(stop func(), closePool func()) {
	if stop != nil {
		stop()
	}
	if closePool != nil {
		closePool()
	}
}

func runLeaderboardSnapshotterWorker(rt rankingRuntime) error {
	if rt.store == nil {
		return fmt.Errorf("leaderboard snapshotter requires durable store")
	}
	if rt.durableReady != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := rt.durableReady(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("leaderboard snapshotter schema readiness: %w", err)
		}
	}
	worker := NewLeaderboardSnapshotterWorker(rt.store)
	worker.cooldown = snapshotterCooldownFromEnv()
	worker.Start()

	var poolOnce sync.Once
	closePool := func() {
		poolOnce.Do(func() {
			if rt.pool != nil {
				rt.pool.Close()
			}
		})
	}
	// Single cleanup: Stop (≤35s grace) then pool close. Avoids LIFO defer closing DB first.
	defer shutdownLeaderboardSnapshotter(worker.Stop, closePool)

	processLogger().InfoContext(context.Background(), "leaderboard snapshotter started",
		"event", "leaderboard_snapshotter_started", "mode", rt.mode, "cooldownMs", worker.cooldown.Milliseconds())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	processLogger().InfoContext(context.Background(), "leaderboard snapshotter stopping", "event", "leaderboard_snapshotter_stopping")
	// Signal path: stop/wait before pool close; defer is a no-op safety net (Stop + Once).
	shutdownLeaderboardSnapshotter(worker.Stop, closePool)
	return nil
}
