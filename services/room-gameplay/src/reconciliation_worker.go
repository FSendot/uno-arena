package main

import (
	"context"
	"log"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
)

// ReconciliationWorker drains pending GI-success/DB-commit-failure markers.
// Runs in the durable API process (not OutboxRetryWorker, not room-timer).
type ReconciliationWorker struct {
	store     *store.SessionStore
	integrity app.GameIntegrity
	deals     app.DealSource
	interval  time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewReconciliationWorker constructs a background reconciler.
func NewReconciliationWorker(sessions *store.SessionStore, integrity app.GameIntegrity, deals app.DealSource) *ReconciliationWorker {
	return &ReconciliationWorker{
		store:     sessions,
		integrity: integrity,
		deals:     deals,
		interval:  2 * time.Second,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
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
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			n, err := w.store.ReconcilePending(context.Background(), w.integrity, w.deals, 32)
			if err != nil {
				log.Printf(`{"level":"warn","event":"reconciliation_failed","error":%q}`, err.Error())
				continue
			}
			if n > 0 {
				log.Printf(`{"level":"info","event":"reconciliation_tick","markers":%d}`, n)
			}
		}
	}
}
