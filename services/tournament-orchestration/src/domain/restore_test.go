package domain

import "testing"

func TestRestoreTournamentRoundTripFields(t *testing.T) {
	tr, out := CreateTournament(CreateTournamentCommand{
		CommandID: "c", TournamentID: "t-restore", Capacity: 4, RetryBudget: 7, BatchSize: 2,
	})
	if !out.Accepted() {
		t.Fatal(out)
	}
	_ = tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "r1", PlayerID: "p1"})
	_ = tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "r2", PlayerID: "p2"})
	restored := RestoreTournament(RestoreTournamentInput{
		ID:                tr.ID(),
		Phase:             tr.Phase(),
		Capacity:          tr.Capacity(),
		RetryBudget:       tr.RetryBudget(),
		BatchSize:         tr.BatchSize(),
		Registrations:     map[PlayerID]struct{}{"p1": {}, "p2": {}},
		RegistrationOrder: []PlayerID{"p1", "p2"},
		Rounds:            map[int]*Round{},
		CurrentRound:      0,
		ResultKeys:        tr.ResultKeysSnapshot(),
		RoomOwners:        tr.RoomOwnersSnapshot(),
	})
	if restored.RegisteredCount() != 2 || restored.RetryBudget() != 7 || restored.BatchSize() != 2 {
		t.Fatalf("restore mismatch: %+v count=%d", restored.Phase(), restored.RegisteredCount())
	}
	if !restored.IsRegistered("p1") || restored.RegisteredPlayers()[0] != "p1" {
		t.Fatal("registration order lost")
	}
}
