package main

import (
	"sync"
	"time"

	"unoarena/services/identity/domain"
)

// OutboxRetryConfig tunes the autonomous outbox drain worker.
type OutboxRetryConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	DrainLimit      int
	// OnDrain is optional; invoked after each DrainOutbox attempt (tests).
	OnDrain func(published int, err error)
}

// OutboxRetryWorker periodically drains pending SessionInvalidated outbox entries
// with bounded exponential backoff so quiet-period failures still retry.
type OutboxRetryWorker struct {
	svc    *domain.Service
	cfg    OutboxRetryConfig
	stopCh chan struct{}
	doneCh chan struct{}

	mu      sync.Mutex
	started bool
	stopped bool
}

func NewOutboxRetryWorker(svc *domain.Service, cfg OutboxRetryConfig) *OutboxRetryWorker {
	if cfg.InitialInterval <= 0 {
		cfg.InitialInterval = time.Second
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 30 * time.Second
	}
	if cfg.MaxInterval < cfg.InitialInterval {
		cfg.MaxInterval = cfg.InitialInterval
	}
	if cfg.DrainLimit <= 0 {
		cfg.DrainLimit = 32
	}
	return &OutboxRetryWorker{
		svc:    svc,
		cfg:    cfg,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the retry loop. Idempotent if already started.
func (w *OutboxRetryWorker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return
	}
	w.started = true
	go w.loop()
}

// Stop signals the worker and waits for exit. Idempotent and safe after Start.
func (w *OutboxRetryWorker) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	if !w.started {
		close(w.doneCh)
		w.mu.Unlock()
		return
	}
	close(w.stopCh)
	w.mu.Unlock()
	<-w.doneCh
}

func (w *OutboxRetryWorker) loop() {
	defer close(w.doneCh)
	interval := w.cfg.InitialInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-timer.C:
			n, err := w.svc.DrainOutbox(w.cfg.DrainLimit)
			if w.cfg.OnDrain != nil {
				w.cfg.OnDrain(n, err)
			}
			if err != nil {
				interval = nextBackoff(interval, w.cfg.MaxInterval)
			} else if n == 0 {
				interval = nextBackoff(interval, w.cfg.MaxInterval)
			} else {
				interval = w.cfg.InitialInterval
			}
			timer.Reset(interval)
		}
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max || next <= 0 {
		return max
	}
	return next
}
