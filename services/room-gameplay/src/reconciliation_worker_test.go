package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"unoarena/services/room-gameplay/app"
)

type fakeReconciliationQueue struct {
	mu          sync.Mutex
	markers     []app.ReconciliationMarker
	claimOwner  string
	claimLimit  int
	reconciled  []string
	released    []string
	retryDelay  time.Duration
	failCommand string
	renewals    int
	renew       func(context.Context) (bool, error)
	reconcile   func(context.Context) error
}

func (q *fakeReconciliationQueue) ClaimPendingReconciliationMarkers(_ context.Context, owner string, limit int, _ time.Duration) ([]app.ReconciliationMarker, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.claimOwner, q.claimLimit = owner, limit
	if len(q.markers) > limit {
		return append([]app.ReconciliationMarker(nil), q.markers[:limit]...), nil
	}
	return append([]app.ReconciliationMarker(nil), q.markers...), nil
}

func (q *fakeReconciliationQueue) ReconcileClaimedMarker(ctx context.Context, _ app.GameIntegrity, _ app.DealSource, marker app.ReconciliationMarker, owner string) error {
	q.mu.Lock()
	if owner != q.claimOwner {
		q.mu.Unlock()
		return errors.New("wrong owner")
	}
	q.reconciled = append(q.reconciled, marker.CommandID)
	reconcile := q.reconcile
	failCommand := q.failCommand
	q.mu.Unlock()
	if reconcile != nil {
		return reconcile(ctx)
	}
	if marker.CommandID == failCommand {
		return errors.New("retry")
	}
	return nil
}

func (q *fakeReconciliationQueue) RenewReconciliationClaim(ctx context.Context, _ string, _ string, _ time.Duration) (bool, error) {
	q.mu.Lock()
	q.renewals++
	renew := q.renew
	q.mu.Unlock()
	if renew != nil {
		return renew(ctx)
	}
	return true, nil
}

func (q *fakeReconciliationQueue) ReleaseReconciliationClaim(_ context.Context, commandID, owner string, retryDelay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if owner != q.claimOwner {
		return errors.New("wrong owner")
	}
	q.released = append(q.released, commandID)
	q.retryDelay = retryDelay
	return nil
}

func TestIntegrityReconcilerClaimsBoundedBatchAndReleasesFailures(t *testing.T) {
	queue := &fakeReconciliationQueue{
		markers:     []app.ReconciliationMarker{{CommandID: "repair-1"}, {CommandID: "repair-2", Attempts: 3}, {CommandID: "repair-3"}},
		failCommand: "repair-2",
	}
	worker := NewReconciliationWorker(queue, app.NewFakeGameIntegrity(), app.NewFakeDealSource())
	worker.Configure("reconciler-a", 2, time.Minute, time.Second)
	worker.tick(context.Background())
	if queue.claimOwner != "reconciler-a" || queue.claimLimit != 2 {
		t.Fatalf("claim owner=%q limit=%d", queue.claimOwner, queue.claimLimit)
	}
	if len(queue.reconciled) != 2 || queue.reconciled[0] != "repair-1" || queue.reconciled[1] != "repair-2" {
		t.Fatalf("reconciled=%v", queue.reconciled)
	}
	if len(queue.released) != 1 || queue.released[0] != "repair-2" {
		t.Fatalf("released=%v", queue.released)
	}
	if queue.retryDelay != 4*time.Second {
		t.Fatalf("retry delay=%s want=4s", queue.retryDelay)
	}
}

func TestIntegrityReconcilerRenewsMarkerLeasePastOriginalTTL(t *testing.T) {
	renewed := make(chan struct{})
	var once sync.Once
	queue := &fakeReconciliationQueue{
		markers: []app.ReconciliationMarker{{CommandID: "slow-repair"}},
		renew: func(context.Context) (bool, error) {
			once.Do(func() { close(renewed) })
			return true, nil
		},
		reconcile: func(ctx context.Context) error {
			select {
			case <-renewed:
			case <-ctx.Done():
				return ctx.Err()
			}
			// Hold repair longer than the initial lease; its heartbeat must keep it live.
			time.Sleep(35 * time.Millisecond)
			return nil
		},
	}
	worker := NewReconciliationWorker(queue, app.NewFakeGameIntegrity(), app.NewFakeDealSource())
	worker.Configure("reconciler-a", 1, 30*time.Millisecond, time.Second)
	worker.tick(context.Background())
	if queue.renewals == 0 || len(queue.reconciled) != 1 || len(queue.released) != 0 {
		t.Fatalf("renewals=%d reconciled=%v released=%v", queue.renewals, queue.reconciled, queue.released)
	}
}

func TestIntegrityReconcilerCancelsWhenMarkerLeaseOwnershipIsLost(t *testing.T) {
	cancelled := make(chan struct{})
	queue := &fakeReconciliationQueue{
		markers: []app.ReconciliationMarker{{CommandID: "lost-repair"}},
		renew:   func(context.Context) (bool, error) { return false, nil },
		reconcile: func(ctx context.Context) error {
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		},
	}
	worker := NewReconciliationWorker(queue, app.NewFakeGameIntegrity(), app.NewFakeDealSource())
	worker.Configure("reconciler-a", 1, 30*time.Millisecond, time.Second)
	worker.tick(context.Background())
	select {
	case <-cancelled:
	default:
		t.Fatal("repair was not cancelled after lease ownership loss")
	}
	if len(queue.released) != 0 {
		t.Fatalf("stale worker must not release a lost claim: %v", queue.released)
	}
}
