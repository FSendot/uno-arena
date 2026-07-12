package domain

import "testing"

func TestComputeTournamentAward_AdvancementFixedTen(t *testing.T) {
	for _, round := range []int{1, 2, 5, 99} {
		got, err := ComputeTournamentAward(ReasonTournamentAdvancement, 0, round)
		if err != nil {
			t.Fatalf("round=%d: %v", round, err)
		}
		if got != TournamentAdvancementAward {
			t.Fatalf("round=%d award=%d want %d", round, got, TournamentAdvancementAward)
		}
	}
}

func TestComputeTournamentAward_AdvancementRejectsBadInput(t *testing.T) {
	if _, err := ComputeTournamentAward(ReasonTournamentAdvancement, 0, 0); err == nil {
		t.Fatal("round 0 must fail")
	}
	if _, err := ComputeTournamentAward(ReasonTournamentAdvancement, 1, 1); err == nil {
		t.Fatal("placement set must fail")
	}
}

func TestComputeTournamentAward_FinalStandingTable(t *testing.T) {
	want := []int{100, 70, 50, 35, 25, 20, 15, 10, 5, 0}
	for i, w := range want {
		got, err := ComputeTournamentAward(ReasonTournamentFinalStanding, i+1, 0)
		if err != nil {
			t.Fatalf("place=%d: %v", i+1, err)
		}
		if got != w {
			t.Fatalf("place=%d got=%d want=%d", i+1, got, w)
		}
	}
}

func TestComputeTournamentAward_FinalStandingRejectsBadInput(t *testing.T) {
	if _, err := ComputeTournamentAward(ReasonTournamentFinalStanding, 0, 0); err == nil {
		t.Fatal("placement 0 must fail")
	}
	if _, err := ComputeTournamentAward(ReasonTournamentFinalStanding, 11, 0); err == nil {
		t.Fatal("placement 11 must fail")
	}
	if _, err := ComputeTournamentAward(ReasonTournamentFinalStanding, 1, 2); err == nil {
		t.Fatal("roundNumber set must fail")
	}
}

func TestComputeTournamentAward_UnknownReason(t *testing.T) {
	if _, err := ComputeTournamentAward(ReasonCasualGameCompleted, 1, 0); err == nil {
		t.Fatal("casual reason must fail")
	}
}

func TestTournamentAwardsNeverNegative(t *testing.T) {
	for place := 1; place <= 10; place++ {
		got, err := ComputeTournamentAward(ReasonTournamentFinalStanding, place, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got < 0 {
			t.Fatalf("negative award at place %d", place)
		}
	}
	adv, err := ComputeTournamentAward(ReasonTournamentAdvancement, 0, 1)
	if err != nil || adv < 0 {
		t.Fatalf("advancement award=%d err=%v", adv, err)
	}
}
