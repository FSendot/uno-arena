package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"unoarena/services/tournament-orchestration/store"
)

const (
	defaultProvisioningPollInterval  = 500 * time.Millisecond
	workerRoleTournamentProvisioning = "tournament-provisioning"
)

// ProvisioningClaimStore is the Postgres claim/reap surface used by the worker.
type ProvisioningClaimStore interface {
	ReapExpiredProvisioningLeases(ctx context.Context, now time.Time) (int64, error)
	ClaimNextProvisioningBatch(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration) (*store.ClaimedProvisioningBatch, error)
}

// ProvisioningWorker claims lease-safe batches and processes them outside the claim TX.
type ProvisioningWorker struct {
	claims       ProvisioningClaimStore
	process      func(ctx context.Context, work ProvisioningBatchWork) error
	owner        string
	leaseTTL     time.Duration
	pollInterval time.Duration
	clock        func() time.Time

	stopCh   chan struct{}
	doneCh   chan struct{}
	inflight sync.WaitGroup
	stopped  atomic.Bool
}

// NewProvisioningWorker constructs a lease-safe tournament provisioning worker.
func NewProvisioningWorker(claims ProvisioningClaimStore, process func(ctx context.Context, work ProvisioningBatchWork) error, owner string) *ProvisioningWorker {
	if owner == "" {
		owner = defaultProvisioningOwner()
	}
	return &ProvisioningWorker{
		claims:       claims,
		process:      process,
		owner:        owner,
		leaseTTL:     store.DefaultProvisioningLease,
		pollInterval: defaultProvisioningPollInterval,
		clock:        func() time.Time { return time.Now().UTC() },
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

func defaultProvisioningOwner() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "tournament-provisioning"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// Start begins the claim/process loop in a background goroutine.
func (w *ProvisioningWorker) Start() {
	go w.loop()
}

// Stop stops new claims and waits for in-flight ProcessProvisioningBatch work.
func (w *ProvisioningWorker) Stop() {
	if w == nil || w.stopped.Swap(true) {
		return
	}
	close(w.stopCh)
	<-w.doneCh
	w.inflight.Wait()
}

func (w *ProvisioningWorker) loop() {
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

func (w *ProvisioningWorker) tick() {
	if w.stopped.Load() {
		return
	}
	now := w.clock()
	if _, err := w.claims.ReapExpiredProvisioningLeases(context.Background(), now); err != nil {
		slog.Warn("provisioning lease reap failed", "event", "provisioning_reap_failed", "error", err.Error())
	}
	if w.stopped.Load() {
		return
	}
	claimed, err := w.claims.ClaimNextProvisioningBatch(context.Background(), w.owner, now, w.leaseTTL)
	if err != nil {
		if errors.Is(err, store.ErrProvisioningBatchTooLarge) {
			slog.Error("provisioning batch too large", "event", "provisioning_batch_too_large", "error", err.Error())
			return
		}
		slog.Warn("provisioning claim failed", "event", "provisioning_claim_failed", "error", err.Error())
		return
	}
	if claimed == nil {
		return
	}
	if w.stopped.Load() {
		// Claim already committed; leave lease for reaper/idempotent retry after restart.
		return
	}
	work := ProvisioningBatchWork{
		TournamentID:   claimed.TournamentID,
		RoundNumber:    claimed.RoundNumber,
		BatchID:        claimed.BatchID,
		SlotFrom:       claimed.SlotFrom,
		SlotTo:         claimed.SlotTo,
		SlotSize:       claimed.SlotSize,
		RetryAttempt:   claimed.RetryAttempt,
		LeaseOwner:     claimed.LeaseOwner,
		LeaseExpiresAt: claimed.LeaseExpiresAt,
		LeaseVersion:   claimed.LeaseVersion,
	}
	w.inflight.Add(1)
	defer w.inflight.Done()
	if err := w.process(context.Background(), work); err != nil {
		slog.Warn("provisioning process failed", "event", "provisioning_process_failed",
			"tournamentId", work.TournamentID, "batchId", work.BatchID, "error", err.Error())
	}
}
