package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"unoarena/services/room-gameplay/app"
)

var errReconciliationLeaseLost = errors.New("reconciliation claim lost")

// ReconciliationWorker drains pending GI-success/DB-commit-failure markers.
// It runs only in the bounded room-integrity-reconciler process role.
type ReconciliationWorker struct {
	store     reconciliationQueue
	integrity app.GameIntegrity
	deals     app.DealSource
	owner     string
	batch     int
	lease     time.Duration
	interval  time.Duration
	jitter    func(time.Duration) time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
}

type reconciliationQueue interface {
	ClaimPendingReconciliationMarkers(context.Context, string, int, time.Duration) ([]app.ReconciliationMarker, error)
	RenewReconciliationClaim(context.Context, string, string, time.Duration) (bool, error)
	ReconcileClaimedMarker(context.Context, app.GameIntegrity, app.DealSource, app.ReconciliationMarker, string) error
	ReleaseReconciliationClaim(context.Context, string, string, time.Duration) error
}

// NewReconciliationWorker constructs a background reconciler.
func NewReconciliationWorker(sessions reconciliationQueue, integrity app.GameIntegrity, deals app.DealSource) *ReconciliationWorker {
	return &ReconciliationWorker{
		store:     sessions,
		integrity: integrity,
		deals:     deals,
		owner:     "room-integrity-reconciler",
		batch:     32,
		lease:     time.Minute,
		interval:  2 * time.Second,
		jitter:    randomJitterDuration,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Configure sets bounded per-replica claim controls.
func (w *ReconciliationWorker) Configure(owner string, batch int, lease, interval time.Duration) {
	if strings.TrimSpace(owner) != "" {
		w.owner = strings.TrimSpace(owner)
	}
	if batch > 0 {
		w.batch = batch
	}
	if w.batch > 128 {
		w.batch = 128
	}
	if lease > 0 {
		w.lease = lease
	}
	if interval > 0 {
		w.interval = interval
	}
}

// Start begins the reconcile loop.
func (w *ReconciliationWorker) Start() {
	go w.loop()
}

// Stop stops the loop and waits for exit.
func (w *ReconciliationWorker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

func (w *ReconciliationWorker) loop() {
	defer close(w.doneCh)
	timer := time.NewTimer(w.jitter(w.interval))
	defer timer.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-timer.C:
			w.tick(context.Background())
			timer.Reset(w.jitter(w.interval))
		}
	}
}

func (w *ReconciliationWorker) tick(ctx context.Context) {
	markers, err := w.store.ClaimPendingReconciliationMarkers(ctx, w.owner, w.batch, w.lease)
	if err != nil {
		slog.WarnContext(ctx, "reconciliation claim failed", "event", "reconciliation_claim_failed", "error", err.Error())
		return
	}
	for _, marker := range markers {
		if err := w.reconcileWithLease(ctx, marker); err != nil {
			slog.WarnContext(ctx, "reconciliation failed", "event", "reconciliation_failed", "commandId", marker.CommandID, "error", err.Error())
			if errors.Is(err, errReconciliationLeaseLost) {
				continue
			}
			if releaseErr := w.store.ReleaseReconciliationClaim(ctx, marker.CommandID, w.owner, w.jitter(reconciliationRetryDelay(marker.Attempts))); releaseErr != nil {
				slog.WarnContext(ctx, "reconciliation release failed", "event", "reconciliation_release_failed", "commandId", marker.CommandID, "error", releaseErr.Error())
			}
		}
	}
	if len(markers) > 0 {
		slog.InfoContext(ctx, "reconciliation batch processed", "event", "reconciliation_tick", "markers", len(markers))
	}
}

// reconcileWithLease keeps one marker's lease live for slow GI replay, append,
// and reservation work. A failed renewal cancels the repair context; the store
// also fences every completion write so cancellation timing cannot make a stale
// worker complete a marker.
func (w *ReconciliationWorker) reconcileWithLease(ctx context.Context, marker app.ReconciliationMarker) error {
	repairCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	renewResult := make(chan error, 1)
	interval := w.lease / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-repairCtx.Done():
				renewResult <- nil
				return
			case <-ticker.C:
				renewed, err := w.store.RenewReconciliationClaim(repairCtx, marker.CommandID, w.owner, w.lease)
				if err != nil {
					renewResult <- err
					cancel()
					return
				}
				if !renewed {
					renewResult <- fmt.Errorf("%w command=%s owner=%s", errReconciliationLeaseLost, marker.CommandID, w.owner)
					cancel()
					return
				}
			}
		}
	}()

	repairErr := w.store.ReconcileClaimedMarker(repairCtx, w.integrity, w.deals, marker, w.owner)
	cancel()
	renewErr := <-renewResult
	if renewErr != nil {
		return renewErr
	}
	return repairErr
}

func reconciliationRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := time.Second
	for i := 1; i < attempts && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}
