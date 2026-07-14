package domain_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

type countingPlayerCreations struct {
	count atomic.Int64
}

func (r *countingPlayerCreations) RecordPlayerCreated(context.Context) {
	r.count.Add(1)
}

func newMetricTestService(recorder domain.PlayerCreationRecorder) *domain.Service {
	ids := &seqIDs{}
	return domain.NewService(domain.ServiceDeps{
		Players:       domain.NewMemoryPlayerRepository(),
		Sessions:      domain.NewMemorySessionRepository(),
		Hasher:        domain.NewCheckpointPasswordHasher(ids),
		IDs:           ids,
		Tokens:        ids,
		Clock:         &fixedClock{now: time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)},
		SessionTTL:    time.Hour,
		Transport:     domain.NewFakeInvalidationTransport(),
		PlayerCreated: recorder,
	})
}

func TestPlayerCreationRecorderRunsOnlyAfterNewAuthoritativeCreation(t *testing.T) {
	recorder := &countingPlayerCreations{}
	svc := newMetricTestService(recorder)

	if _, err := svc.Register(t.Context(), "alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := recorder.count.Load(); got != 1 {
		t.Fatalf("password registration count=%d, want 1", got)
	}
	if _, err := svc.Register(t.Context(), "alice", "duplicate"); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	if got := recorder.count.Load(); got != 1 {
		t.Fatalf("rejected registration count=%d, want 1", got)
	}

	if _, err := svc.EstablishOIDCSession(t.Context(), "https://issuer.example", "subject-1", "bob", []string{"player"}); err != nil {
		t.Fatalf("first OIDC session: %v", err)
	}
	if got := recorder.count.Load(); got != 2 {
		t.Fatalf("new OIDC player count=%d, want 2", got)
	}
	if _, err := svc.EstablishOIDCSession(t.Context(), "https://issuer.example", "subject-1", "ignored", []string{"player"}); err != nil {
		t.Fatalf("idempotent OIDC session: %v", err)
	}
	if got := recorder.count.Load(); got != 2 {
		t.Fatalf("resolved OIDC player count=%d, want 2", got)
	}
}
