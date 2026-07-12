package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

type fakeClaimStore struct {
	mu       sync.Mutex
	reapN    int
	claims   []*store.ClaimedProvisioningBatch
	claimErr error
	claimLog []string
}

func (f *fakeClaimStore) ReapExpiredProvisioningLeases(context.Context, time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reapN++
	return 0, nil
}

func (f *fakeClaimStore) ClaimNextProvisioningBatch(_ context.Context, owner string, _ time.Time, _ time.Duration) (*store.ClaimedProvisioningBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimLog = append(f.claimLog, owner)
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.claims) == 0 {
		return nil, nil
	}
	c := f.claims[0]
	f.claims = f.claims[1:]
	return c, nil
}

func TestProvisioningWorker_ProcessesClaimOutsideLoop(t *testing.T) {
	claims := &fakeClaimStore{
		claims: []*store.ClaimedProvisioningBatch{{
			TournamentID: "t1",
			RoundNumber:  1,
			BatchID:      "batch_0",
			SlotFrom:     "slot_0",
			SlotTo:       "slot_0",
			SlotSize:     1,
			RetryAttempt: 0,
			LeaseOwner:   "worker-a",
			LeaseVersion: 3,
		}},
	}
	var processed atomic.Int32
	gate := make(chan struct{})
	w := NewProvisioningWorker(claims, func(_ context.Context, work ProvisioningBatchWork) error {
		if work.TournamentID != "t1" || work.BatchID != "batch_0" || work.SlotSize != 1 {
			t.Errorf("unexpected work %+v", work)
		}
		if work.LeaseOwner != "worker-a" || work.LeaseVersion != 3 {
			t.Errorf("worker must pass fence fields: owner=%q ver=%d", work.LeaseOwner, work.LeaseVersion)
		}
		processed.Add(1)
		<-gate
		return nil
	}, "worker-a")
	w.pollInterval = 20 * time.Millisecond
	w.Start()

	deadline := time.Now().Add(2 * time.Second)
	for processed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if processed.Load() != 1 {
		t.Fatalf("processed=%d", processed.Load())
	}
	close(gate)
	w.Stop()
	if processed.Load() != 1 {
		t.Fatalf("after stop processed=%d", processed.Load())
	}
}

func TestProvisioningWorker_StopWaitsForInflight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	claims := &fakeClaimStore{
		claims: []*store.ClaimedProvisioningBatch{{
			TournamentID: "t1", RoundNumber: 1, BatchID: "batch_0",
			SlotFrom: "slot_0", SlotTo: "slot_0", SlotSize: 1,
		}},
	}
	w := NewProvisioningWorker(claims, func(context.Context, ProvisioningBatchWork) error {
		close(started)
		<-release
		return nil
	}, "worker-b")
	w.pollInterval = 10 * time.Millisecond
	w.Start()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("process did not start")
	}
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Stop returned before inflight finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not wait for inflight")
	}
}

func TestProvisioningWorker_StopPreventsFurtherClaims(t *testing.T) {
	claims := &fakeClaimStore{}
	w := NewProvisioningWorker(claims, func(context.Context, ProvisioningBatchWork) error {
		return nil
	}, "worker-c")
	w.pollInterval = 10 * time.Millisecond
	w.Start()
	time.Sleep(40 * time.Millisecond)
	w.Stop()
	claims.mu.Lock()
	before := len(claims.claimLog)
	claims.mu.Unlock()
	time.Sleep(40 * time.Millisecond)
	claims.mu.Lock()
	after := len(claims.claimLog)
	claims.mu.Unlock()
	if after != before {
		t.Fatalf("claims continued after stop: before=%d after=%d", before, after)
	}
}
