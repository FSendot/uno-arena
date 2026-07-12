package main

import (
	"context"
	"testing"
)

func TestScopeForSlot_NoopWhenLookupUnavailable(t *testing.T) {
	ctx := context.Background()

	// Nil service / missing refresher must not fall back to summary-only.
	var nilSvc *Service
	got := nilSvc.scopeForSlot(ctx, "tid", 1, "slot_0")
	if got.TournamentID != "" || got.Summary || len(got.Chunks) != 0 {
		t.Fatalf("nil service want noop, got %+v", got)
	}

	svc := &Service{}
	got = svc.scopeForSlot(ctx, "tid", 1, "slot_0")
	if got.TournamentID != "" || got.Summary || len(got.Chunks) != 0 {
		t.Fatalf("nil refresher want noop, got %+v", got)
	}

	got = svc.scopeForSlot(ctx, "tid", 0, "slot_0")
	if got.TournamentID != "" || got.Summary {
		t.Fatalf("bad round want noop, got %+v", got)
	}
	got = svc.scopeForSlot(ctx, "tid", 1, "")
	if got.TournamentID != "" || got.Summary {
		t.Fatalf("empty slot want noop, got %+v", got)
	}
}
