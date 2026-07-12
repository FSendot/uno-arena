package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

const (
	defaultSeedingPollInterval  = 500 * time.Millisecond
	workerRoleTournamentSeeding = "tournament-seeding"
)

// SeedingClaimStore is the Postgres claim/process surface used by the seeding worker.
type SeedingClaimStore interface {
	ReapExpiredSeedingLeases(ctx context.Context, now time.Time) (int64, error)
	ClaimNextSeedingJob(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration) (*store.ClaimedSeedingJob, error)
	ProcessSeedingChunk(ctx context.Context, job store.ClaimedSeedingJob, owner string, now time.Time) (bool, error)
}

// SeedingWorker claims lease-safe seeding jobs (any round) and processes chunks outside the claim TX.
type SeedingWorker struct {
	claims       SeedingClaimStore
	owner        string
	leaseTTL     time.Duration
	pollInterval time.Duration
	clock        func() time.Time
	// onFinalize is invoked after a successful seeding finalize (projection bump). Optional.
	onFinalize func(tournamentID string, roundNumber int)

	stopCh   chan struct{}
	doneCh   chan struct{}
	inflight sync.WaitGroup
	stopped  atomic.Bool
}

// NewSeedingWorker constructs a lease-safe tournament seeding worker.
func NewSeedingWorker(claims SeedingClaimStore, owner string) *SeedingWorker {
	if owner == "" {
		owner = defaultSeedingOwner()
	}
	return &SeedingWorker{
		claims:       claims,
		owner:        owner,
		leaseTTL:     store.DefaultSeedingLease,
		pollInterval: defaultSeedingPollInterval,
		clock:        func() time.Time { return time.Now().UTC() },
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// WithFinalizeHook registers a post-finalize callback (Redis bracket refresh for that round).
func (w *SeedingWorker) WithFinalizeHook(fn func(tournamentID string, roundNumber int)) *SeedingWorker {
	if w != nil {
		w.onFinalize = fn
	}
	return w
}

func defaultSeedingOwner() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "tournament-seeding"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// Start begins the claim/process loop in a background goroutine.
func (w *SeedingWorker) Start() {
	go w.loop()
}

// Stop stops new claims and waits for in-flight chunk work.
func (w *SeedingWorker) Stop() {
	if w == nil || w.stopped.Swap(true) {
		return
	}
	close(w.stopCh)
	<-w.doneCh
	w.inflight.Wait()
}

func (w *SeedingWorker) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.tick()
		}
	}
}

func (w *SeedingWorker) tick() {
	if w.stopped.Load() {
		return
	}
	now := w.clock()
	if _, err := w.claims.ReapExpiredSeedingLeases(context.Background(), now); err != nil {
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"seeding_reap_failed","err":%q}`, err.Error())
	}
	if w.stopped.Load() {
		return
	}
	claimed, err := w.claims.ClaimNextSeedingJob(context.Background(), w.owner, now, w.leaseTTL)
	if err != nil {
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"seeding_claim_failed","err":%q}`, err.Error())
		return
	}
	if claimed == nil {
		return
	}
	if w.stopped.Load() {
		return
	}
	w.inflight.Add(1)
	defer w.inflight.Done()
	// Exactly one claimed chunk per tick — never reclaim (avoids stranded leases on other tournaments).
	completed, err := w.claims.ProcessSeedingChunk(context.Background(), *claimed, w.owner, w.clock())
	if err != nil {
		log.Printf(`{"level":"warn","service":"tournament-orchestration","event":"seeding_chunk_failed","tournamentId":%q,"err":%q}`,
			claimed.TournamentID, err.Error())
		return
	}
	if completed && w.onFinalize != nil {
		w.onFinalize(claimed.TournamentID, claimed.RoundNumber)
	}
}
