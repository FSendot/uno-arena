//go:build integration

package store_test

import (
	"context"
	"testing"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestIntegration_TournamentPerformance_EventWide(t *testing.T) {
	ctx := context.Background()
	pool, ts := openStore(t)
	_ = pool

	biz, err := store.PlayersAdvancedBusinessKey("t-perf", 1, "slot-1")
	if err != nil {
		t.Fatal(err)
	}
	req := store.TournamentPerformanceRequest{
		SourceTopic: "tournament.players.advanced", UpstreamEventID: "evt-perf-1",
		BusinessKey: biz, PayloadFingerprint: "fp-perf-1", TournamentID: "t-perf",
		CorrelationID: "corr-perf", CausationID: "cause-perf",
		Players: []store.TournamentPlayerPerformance{
			{PlayerID: "a", RoundNumber: 1, Reason: domain.ReasonTournamentAdvancement},
			{PlayerID: "b", RoundNumber: 1, Reason: domain.ReasonTournamentAdvancement},
		},
	}
	first, err := ts.ApplyTournamentPerformance(ctx, req)
	if err != nil || first.Kind != domain.OutcomeAccepted || !first.ScoreChanged {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	nHist, _ := ts.CountHistory(ctx)
	nOut, _ := ts.CountOutbox(ctx)
	if nHist != 2 || nOut < 2 {
		t.Fatalf("hist=%d outbox=%d", nHist, nOut)
	}

	dup, err := ts.ApplyTournamentPerformance(ctx, req)
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("dup=%+v err=%v", dup, err)
	}
	nHist2, _ := ts.CountHistory(ctx)
	if nHist2 != nHist {
		t.Fatal("duplicate mutated history")
	}

	conflict := req
	conflict.PayloadFingerprint = "fp-other"
	_, err = ts.ApplyTournamentPerformance(ctx, conflict)
	if !store.IsTournamentPerformanceConflict(err) {
		t.Fatalf("want conflict, got %v", err)
	}
	_, tourA, ok, _ := ts.GetPlayerRating(ctx, "a")
	if !ok || tourA != 10 {
		t.Fatalf("conflict must not mutate: %d", tourA)
	}

	// Zero final standing: history + outbox, no score change => no extra snapshot requirement beyond ScoreChanged=false.
	zeroReq := store.TournamentPerformanceRequest{
		SourceTopic: "tournament.completed", UpstreamEventID: "evt-zero",
		BusinessKey: "evt-zero", PayloadFingerprint: "fp-zero", TournamentID: "t-zero",
		Players: []store.TournamentPlayerPerformance{
			{PlayerID: "z1", Placement: 10, Reason: domain.ReasonTournamentFinalStanding},
		},
	}
	zero, err := ts.ApplyTournamentPerformance(ctx, zeroReq)
	if err != nil || zero.Kind != domain.OutcomeAccepted {
		t.Fatalf("zero=%+v err=%v", zero, err)
	}
	if zero.ScoreChanged {
		t.Fatal("tenth place must not mark score changed")
	}
	_, tourZ, ok, _ := ts.GetPlayerRating(ctx, "z1")
	if !ok || tourZ != 0 {
		t.Fatalf("z1 rating=%d", tourZ)
	}
}
