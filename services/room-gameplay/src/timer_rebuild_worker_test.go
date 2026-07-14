package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeTimerRebuildLease struct {
	claimed   bool
	completed bool
	claimCall int
	doneCall  int
	renewed   chan struct{}
	renewOnce sync.Once
}

func (f *fakeTimerRebuildLease) TimerIndexRebuildGeneration(context.Context) (int64, error) {
	return 0, nil
}
func (f *fakeTimerRebuildLease) ClaimTimerIndexRebuild(context.Context, string, int64, time.Duration) (bool, error) {
	f.claimCall++
	return f.claimed, nil
}
func (f *fakeTimerRebuildLease) TimerIndexRebuildCompletedAfter(context.Context, int64) (bool, error) {
	return f.completed, nil
}
func (f *fakeTimerRebuildLease) RenewTimerIndexRebuild(context.Context, string, time.Duration) (bool, error) {
	if f.renewed != nil {
		f.renewOnce.Do(func() { close(f.renewed) })
	}
	return true, nil
}
func (f *fakeTimerRebuildLease) CompleteTimerIndexRebuild(context.Context, string) error {
	f.doneCall++
	return nil
}

func TestTimerIndexRebuildWorkerRenewsLeaseUntilRebuildCompletes(t *testing.T) {
	renewed := make(chan struct{})
	lease := &fakeTimerRebuildLease{claimed: true, renewed: renewed}
	worker := NewTimerIndexRebuildWorker(lease, "timer-a", func(ctx context.Context) error {
		select {
		case <-renewed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	worker.leaseTTL = 30 * time.Millisecond
	if !worker.tick(context.Background()) || lease.doneCall != 1 {
		t.Fatalf("lease heartbeat did not preserve completion ownership: done=%d", lease.doneCall)
	}
}

func TestTimerIndexRebuildWorkerLetsOneLeaseHolderRebuild(t *testing.T) {
	lease := &fakeTimerRebuildLease{claimed: true}
	rebuilds := 0
	worker := NewTimerIndexRebuildWorker(lease, "timer-a", func(context.Context) error {
		rebuilds++
		return nil
	})
	completed := worker.tick(context.Background())
	if !completed || rebuilds != 1 || lease.doneCall != 1 {
		t.Fatalf("completed=%v rebuilds=%d done=%d", completed, rebuilds, lease.doneCall)
	}
}

func TestTimerIndexRebuildWorkerStopsWhenSiblingCompletedThisRollout(t *testing.T) {
	lease := &fakeTimerRebuildLease{completed: true}
	rebuilds := 0
	worker := NewTimerIndexRebuildWorker(lease, "timer-b", func(context.Context) error {
		rebuilds++
		return nil
	})
	if !worker.tick(context.Background()) || rebuilds != 0 {
		t.Fatalf("sibling completion not observed: rebuilds=%d", rebuilds)
	}
}
