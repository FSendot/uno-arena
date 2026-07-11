package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

func TestOutboxRetryWorkerStopsCleanly(t *testing.T) {
	ids := domain.RandomIDGenerator{}
	sessions := domain.NewMemorySessionRepository()
	transport := domain.NewFakeInvalidationTransport()
	svc := domain.NewService(domain.ServiceDeps{
		Players:    domain.NewMemoryPlayerRepository(),
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      domain.SystemClock{},
		SessionTTL: time.Hour,
		Transport:  transport,
	})

	w := NewOutboxRetryWorker(svc, OutboxRetryConfig{
		InitialInterval: 20 * time.Millisecond,
		MaxInterval:     50 * time.Millisecond,
	})
	w.Start()
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}
	// Second Stop is idempotent.
	w.Stop()
}

func TestOutboxRetryWorkerRetriesQuietPeriodFailures(t *testing.T) {
	ids := &testIDs{}
	sessions := domain.NewMemorySessionRepository()
	transport := domain.NewFakeInvalidationTransport()
	clock := &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    domain.NewMemoryPlayerRepository(),
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      clock,
		SessionTTL: time.Hour,
		Transport:  transport,
	})
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := svc.Login("alice", "secret"); err != nil {
		t.Fatalf("first login: %v", err)
	}

	transport.SetError(errors.New("gateway down"))
	if _, err := svc.Login("alice", "secret"); err != nil {
		t.Fatalf("takeover login: %v", err)
	}
	pending, err := sessions.ListPendingOutbox(10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d err=%v", len(pending), err)
	}

	var drains atomic.Int64
	w := NewOutboxRetryWorker(svc, OutboxRetryConfig{
		InitialInterval: 15 * time.Millisecond,
		MaxInterval:     30 * time.Millisecond,
		OnDrain: func(n int, err error) {
			drains.Add(1)
		},
	})
	w.Start()
	defer w.Stop()

	// Quiet period: no further service calls that would DrainOutbox.
	time.Sleep(80 * time.Millisecond)
	if drains.Load() == 0 {
		t.Fatal("worker should attempt drain during quiet period")
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 1 {
		t.Fatalf("pending should remain while transport fails, got %d", len(pending))
	}

	transport.SetError(nil)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, _ := sessions.ListPendingOutbox(10)
		if len(pending) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pending, _ := sessions.ListPendingOutbox(10); len(pending) != 0 {
		t.Fatalf("worker should publish pending after transport recovers, got %d", len(pending))
	}
	if got := transport.Delivered(); len(got) != 1 {
		t.Fatalf("expected 1 delivered, got %d", len(got))
	}
}
