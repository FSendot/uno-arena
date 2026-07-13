package main

import (
	"context"
	"log"
	"strings"
	"time"
)

type timerRebuildLease interface {
	ClaimTimerIndexRebuild(context.Context, string, time.Time, time.Duration) (bool, error)
	TimerIndexRebuildCompletedSince(context.Context, time.Time) (bool, error)
	RenewTimerIndexRebuild(context.Context, string, time.Duration) (bool, error)
	CompleteTimerIndexRebuild(context.Context, string) error
}

// TimerIndexRebuildWorker retries until one timer replica completes the
// context-wide Postgres-to-Redis rebuild for this process rollout.
type TimerIndexRebuildWorker struct {
	leases   timerRebuildLease
	rebuild  func(context.Context) error
	owner    string
	started  time.Time
	leaseTTL time.Duration
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewTimerIndexRebuildWorker(leases timerRebuildLease, owner string, rebuild func(context.Context) error) *TimerIndexRebuildWorker {
	if strings.TrimSpace(owner) == "" {
		owner = "room-timer"
	}
	return &TimerIndexRebuildWorker{
		leases: leases, rebuild: rebuild, owner: strings.TrimSpace(owner),
		started: time.Now().UTC(), leaseTTL: time.Minute, interval: time.Second,
		stopCh: make(chan struct{}), doneCh: make(chan struct{}),
	}
}

func (w *TimerIndexRebuildWorker) Start() { go w.loop() }

func (w *TimerIndexRebuildWorker) Stop() {
	select {
	case <-w.doneCh:
		return
	default:
	}
	close(w.stopCh)
	<-w.doneCh
}

func (w *TimerIndexRebuildWorker) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		completed := w.tick(context.Background())
		if completed {
			return
		}
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (w *TimerIndexRebuildWorker) tick(ctx context.Context) bool {
	claimed, err := w.leases.ClaimTimerIndexRebuild(ctx, w.owner, w.started, w.leaseTTL)
	if err != nil {
		log.Printf(`{"level":"warn","event":"timer_index_rebuild_claim_failed","error":%q}`, err.Error())
		return false
	}
	if !claimed {
		completed, err := w.leases.TimerIndexRebuildCompletedSince(ctx, w.started)
		if err != nil {
			log.Printf(`{"level":"warn","event":"timer_index_rebuild_completion_check_failed","error":%q}`, err.Error())
			return false
		}
		return completed
	}
	if err := w.rebuildWithHeartbeat(ctx); err != nil {
		log.Printf(`{"level":"warn","event":"timer_index_rebuild_failed","error":%q}`, err.Error())
		return false
	}
	if err := w.leases.CompleteTimerIndexRebuild(ctx, w.owner); err != nil {
		log.Printf(`{"level":"warn","event":"timer_index_rebuild_complete_failed","error":%q}`, err.Error())
		return false
	}
	log.Printf(`{"level":"info","event":"timer_index_rebuild_completed"}`)
	return true
}

func (w *TimerIndexRebuildWorker) rebuildWithHeartbeat(ctx context.Context) error {
	rebuildCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewResult := make(chan error, 1)
	interval := w.leaseTTL / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-rebuildCtx.Done():
				renewResult <- nil
				return
			case <-ticker.C:
				renewed, err := w.leases.RenewTimerIndexRebuild(rebuildCtx, w.owner, w.leaseTTL)
				if err != nil {
					renewResult <- err
					cancel()
					return
				}
				if !renewed {
					renewResult <- context.Canceled
					cancel()
					return
				}
			}
		}
	}()
	rebuildErr := w.rebuild(rebuildCtx)
	cancel()
	renewErr := <-renewResult
	if rebuildErr != nil {
		return rebuildErr
	}
	return renewErr
}
