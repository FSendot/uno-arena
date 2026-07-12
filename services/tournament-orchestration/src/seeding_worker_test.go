package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

type fakeSeedingClaims struct {
	reapCalls   atomic.Int64
	claimCalls  atomic.Int64
	chunkCalls  atomic.Int64
	claimSeq    []*store.ClaimedSeedingJob
	claimIdx    int
	chunkDoneAt int
}

func (f *fakeSeedingClaims) ReapExpiredSeedingLeases(ctx context.Context, now time.Time) (int64, error) {
	f.reapCalls.Add(1)
	return 0, nil
}

func (f *fakeSeedingClaims) ClaimNextSeedingJob(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration) (*store.ClaimedSeedingJob, error) {
	f.claimCalls.Add(1)
	if f.claimIdx >= len(f.claimSeq) {
		return nil, nil
	}
	j := f.claimSeq[f.claimIdx]
	f.claimIdx++
	return j, nil
}

func (f *fakeSeedingClaims) ProcessSeedingChunk(ctx context.Context, job store.ClaimedSeedingJob, owner string, now time.Time) (bool, error) {
	n := int(f.chunkCalls.Add(1))
	return n >= f.chunkDoneAt, nil
}

func TestSeedingWorker_OneChunkPerTick_NoStrandedClaim(t *testing.T) {
	claims := &fakeSeedingClaims{
		claimSeq: []*store.ClaimedSeedingJob{
			{TournamentID: "t-a", RoundNumber: 1, PlayerCount: 3, SlotCount: 1, BaseSize: 3},
			{TournamentID: "t-b", RoundNumber: 1, PlayerCount: 3, SlotCount: 1, BaseSize: 3},
		},
		chunkDoneAt: 100, // never complete in one call
	}
	w := NewSeedingWorker(claims, "owner-1")
	w.tick()
	if claims.claimCalls.Load() != 1 {
		t.Fatalf("tick must claim exactly once, got %d", claims.claimCalls.Load())
	}
	if claims.chunkCalls.Load() != 1 {
		t.Fatalf("tick must process exactly one chunk, got %d", claims.chunkCalls.Load())
	}
	// Second tick progresses the next tournament (fairness); no stranded lease from reclaim discard.
	w.tick()
	if claims.claimCalls.Load() != 2 || claims.chunkCalls.Load() != 2 {
		t.Fatalf("second tick claim=%d chunk=%d", claims.claimCalls.Load(), claims.chunkCalls.Load())
	}
}

func TestSeedingWorker_StopWaitsInflight(t *testing.T) {
	claims := &fakeSeedingClaims{
		claimSeq: []*store.ClaimedSeedingJob{{
			TournamentID: "t1", RoundNumber: 1, PlayerCount: 1, SlotCount: 1, BaseSize: 1,
		}},
		chunkDoneAt: 1,
	}
	w := NewSeedingWorker(claims, "owner-1")
	w.pollInterval = 20 * time.Millisecond
	w.Start()
	deadline := time.Now().Add(2 * time.Second)
	for claims.chunkCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if claims.chunkCalls.Load() == 0 {
		t.Fatal("expected chunk processing")
	}
	w.Stop()
	// Stop is idempotent.
	w.Stop()
}

func TestWireSeedingWorkerRequiresDatabaseURL(t *testing.T) {
	t.Setenv("WORKER_ROLE", workerRoleTournamentSeeding)
	t.Setenv("DATABASE_URL", "")
	_, err := wireTournamentRuntime()
	if err == nil {
		t.Fatal("expected error without DATABASE_URL")
	}
}
