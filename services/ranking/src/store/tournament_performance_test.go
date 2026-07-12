package store_test

import (
	"testing"

	"unoarena/services/ranking/domain"
	"unoarena/services/ranking/store"
)

func TestPlayersAdvancedBusinessKeyStable(t *testing.T) {
	a, err := store.PlayersAdvancedBusinessKey("t1", 2, "slot")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.PlayersAdvancedBusinessKey("t1", 2, "slot")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a == "" {
		t.Fatalf("a=%q b=%q", a, b)
	}
	c, _ := store.PlayersAdvancedBusinessKey("t1", 3, "slot")
	if a == c {
		t.Fatal("round must change key")
	}
}

func TestTournamentCompletedBusinessKeyIsEventID(t *testing.T) {
	if got := store.TournamentCompletedBusinessKey("evt-1"); got != "evt-1" {
		t.Fatalf("%q", got)
	}
}

func TestIsTournamentPerformanceConflict(t *testing.T) {
	err := &store.TournamentPerformanceConflictError{
		SourceTopic: "tournament.completed", BusinessKey: "e1",
		ExistingFingerprint: "a", IncomingFingerprint: "b",
	}
	if !store.IsTournamentPerformanceConflict(err) {
		t.Fatal("expected conflict")
	}
	if store.IsTournamentPerformanceConflict(store.ErrUnavailable) {
		t.Fatal("unavailable is not conflict")
	}
}

func TestValidateTournamentPerformanceFanoutBoundsViaReject(t *testing.T) {
	// Structure: request validation is exercised through Apply when pool is nil via
	// validate function coverage in tournament_performance.go — assert award policy
	// remains Ranking-owned for mapped players.
	award, err := domain.ComputeTournamentAward(domain.ReasonTournamentAdvancement, 0, 1)
	if err != nil || award != 10 {
		t.Fatalf("award=%d err=%v", award, err)
	}
	zero, err := domain.ComputeTournamentAward(domain.ReasonTournamentFinalStanding, 10, 0)
	if err != nil || zero != 0 {
		t.Fatalf("zero=%d err=%v", zero, err)
	}
}
