package main

import (
	"context"
	"testing"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestMemoryTournamentPerformance_AtomicDuplicateConflictZero(t *testing.T) {
	s := NewMemoryRatingStore()
	ctx := context.Background()
	req := TournamentPerformanceRequest{
		SourceTopic: DefaultTournamentCompletedTopic, UpstreamEventID: "evt-z",
		BusinessKey: "evt-z", PayloadFingerprint: "fp1", TournamentID: "t1",
		Players: []TournamentPlayerPerformance{
			{PlayerID: "p1", Placement: 1, Reason: domain.ReasonTournamentFinalStanding},
			{PlayerID: "p2", Placement: 10, Reason: domain.ReasonTournamentFinalStanding},
		},
	}
	first, err := s.ApplyTournamentPerformance(ctx, req)
	if err != nil || first.Kind != domain.OutcomeAccepted {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if !first.ScoreChanged {
		t.Fatal("first place should change score")
	}
	hist, ok, err := s.History(ctx, "p2")
	if err != nil || !ok || len(hist) != 1 || hist[0].Delta != 0 {
		t.Fatalf("zero history: ok=%v hist=%+v err=%v", ok, hist, err)
	}
	dup, err := s.ApplyTournamentPerformance(ctx, req)
	if err != nil || dup.Kind != domain.OutcomeDuplicate {
		t.Fatalf("dup=%+v err=%v", dup, err)
	}
	conflict := req
	conflict.PayloadFingerprint = "fp-other"
	_, err = s.ApplyTournamentPerformance(ctx, conflict)
	if !store.IsTournamentPerformanceConflict(err) {
		t.Fatalf("want conflict, got %v", err)
	}
	// Unknown player lazy-create via advancement.
	adv := TournamentPerformanceRequest{
		SourceTopic: DefaultPlayersAdvancedTopic, UpstreamEventID: "evt-a",
		BusinessKey: `["t1",1,"s1"]`, PayloadFingerprint: "fp-a", TournamentID: "t1",
		Players: []TournamentPlayerPerformance{
			{PlayerID: "newbie", RoundNumber: 1, Reason: domain.ReasonTournamentAdvancement},
		},
	}
	out, err := s.ApplyTournamentPerformance(ctx, adv)
	if err != nil || out.Kind != domain.OutcomeAccepted {
		t.Fatalf("adv=%+v err=%v", out, err)
	}
	rating, ok, err := s.History(ctx, "newbie")
	if err != nil || !ok || len(rating) != 1 || rating[0].Delta != 10 {
		t.Fatalf("newbie hist=%+v", rating)
	}
}
