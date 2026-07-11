package domain

import (
	"testing"
	"time"
)

func TestAssignRoom_ExactIdempotencyAndConflict(t *testing.T) {
	tr := mustCreate(t, "t-assign", 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	if slot.RoomID.Valid() {
		t.Fatal("seeded slot must not have room until AssignRoom/Provision")
	}

	out := tr.AssignRoom(AssignRoomCommand{
		CommandID: "assign-1", RoundNumber: 1, SlotID: slot.SlotID, RoomID: "room-a",
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentMatchAssigned) {
		t.Fatalf("assign: %+v", out)
	}

	// Exact same room, different commandId → business-key idempotent no-op.
	out = tr.AssignRoom(AssignRoomCommand{
		CommandID: "assign-1b", RoundNumber: 1, SlotID: slot.SlotID, RoomID: "room-a",
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("exact idempotent: %+v", out)
	}
	round, _ = tr.Round(1)
	if round.Slots[0].RoomID != "room-a" {
		t.Fatalf("room=%s", round.Slots[0].RoomID)
	}

	// Conflicting room on same slot.
	out = tr.AssignRoom(AssignRoomCommand{
		CommandID: "assign-conflict", RoundNumber: 1, SlotID: slot.SlotID, RoomID: "room-b",
	})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectConflictingAssignment {
		t.Fatalf("conflict: %+v", out)
	}
	if len(out.Facts) != 0 {
		t.Fatalf("conflict facts=%+v", out.Facts)
	}
	round, _ = tr.Round(1)
	if round.Slots[0].RoomID != "room-a" {
		t.Fatal("conflict must not overwrite assignment")
	}
}

func TestBusinessKeyIdempotency_CloseSeedCompleteDifferentCommandIDs(t *testing.T) {
	tr := mustCreate(t, "t-bizkey", 8)
	for i := 0; i < 8; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}

	out := tr.CloseRegistration(CloseRegistrationCommand{CommandID: "close-a"})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentRegistrationClosed) {
		t.Fatalf("close: %+v", out)
	}
	out = tr.CloseRegistration(CloseRegistrationCommand{CommandID: "close-b"})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("close biz-key: %+v", out)
	}

	out = tr.SeedRound(SeedRoundCommand{CommandID: "seed-a", RoundNumber: 1})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentRoundSeeded) {
		t.Fatalf("seed: %+v", out)
	}
	out = tr.SeedRound(SeedRoundCommand{CommandID: "seed-b", RoundNumber: 1})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("seed biz-key: %+v", out)
	}

	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	base := time.Date(2026, 7, 11, 1, 0, 0, 0, time.UTC)
	slot := round.Slots[0]
	standings := rankedStandings(slot.SeededPlayers, base)
	standings[0].MatchWins = 2
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "res", EventID: "evt", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("record: %+v", out)
	}

	out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr-a", RoundNumber: 1})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentRoundCompleted) {
		t.Fatalf("complete round: %+v", out)
	}
	out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr-b", RoundNumber: 1})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("complete round biz-key: %+v", out)
	}

	out = tr.CompleteTournament(CompleteTournamentCommand{CommandID: "ct-a"})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentCompleted) {
		t.Fatalf("complete tournament: %+v", out)
	}
	out = tr.CompleteTournament(CompleteTournamentCommand{CommandID: "ct-b"})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("complete tournament biz-key: %+v", out)
	}
}

func TestQuarantineTournamentResult_AndDifferentCompletionVersionConflict(t *testing.T) {
	tr := mustCreate(t, "t-qres", 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	base := time.Date(2026, 7, 11, 2, 0, 0, 0, time.UTC)
	standings := rankedStandings(slot.SeededPlayers, base)

	out := tr.QuarantineTournamentResult(QuarantineTournamentResultCommand{
		CommandID: "q1", RoomID: slot.RoomID, CompletionVersion: 5, Reason: "operator hold",
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentResultQuarantined) {
		t.Fatalf("explicit quarantine: %+v", out)
	}
	// Same (roomId, completionVersion) different commandId → empty facts.
	out = tr.QuarantineTournamentResult(QuarantineTournamentResultCommand{
		CommandID: "q1b", RoomID: slot.RoomID, CompletionVersion: 5, Reason: "replay",
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("quarantine biz-key: %+v", out)
	}
	// Record against quarantined key is no-op (no overwrite).
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "res-q", EventID: "evt-q", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 5, Standings: standings,
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("record after quarantine: %+v", out)
	}
	round, _ = tr.Round(1)
	if round.Slots[0].HasResult || len(round.Slots[0].Advancing) != 0 {
		t.Fatal("quarantined key must not advance")
	}

	// Accept a result, then different completionVersion on same slot → quarantine conflict.
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "res-ok", EventID: "evt-ok", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatalf("first record: %+v", out)
	}
	round, _ = tr.Round(1)
	priorAdv := append([]PlayerID(nil), round.Slots[0].Advancing...)
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "res-v2", EventID: "evt-v2", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 2, Standings: rankedStandingsConflict(slot.SeededPlayers, base),
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentResultQuarantined) {
		t.Fatalf("different completionVersion conflict: %+v", out)
	}
	if hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatal("conflict must not re-advance")
	}
	round, _ = tr.Round(1)
	if len(round.Slots[0].Advancing) != len(priorAdv) {
		t.Fatal("conflict must not overwrite prior advancement")
	}
	for i := range priorAdv {
		if round.Slots[0].Advancing[i] != priorAdv[i] {
			t.Fatalf("advancement mutated: %v vs %v", round.Slots[0].Advancing, priorAdv)
		}
	}
}

func TestRecordMatchResult_ForfeitedStanding_StillAdvancesTopThree(t *testing.T) {
	tr := mustCreate(t, "t-ff-adv", 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	if round.IsFinal {
		t.Fatal("12 players must seed non-final rooms")
	}
	slot := round.Slots[0]
	if len(slot.SeededPlayers) < 4 {
		t.Fatalf("need >=4 seeded for forfeit+top3, got %d", len(slot.SeededPlayers))
	}
	base := time.Date(2026, 7, 11, 3, 0, 0, 0, time.UTC)
	standings := rankedStandings(slot.SeededPlayers, base)
	// Strongest stats but forfeited — excluded from advancement; others still fill top 3.
	standings[0].Forfeited = true
	standings[0].MatchWins = 2

	out := tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "ff-res", EventID: "ff-evt", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("record: %+v", out)
	}
	if !hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatalf("forfeited peer must not block top-3 advancement: %+v", out.Facts)
	}
	want := SelectAdvancers(standings)
	if len(want) != AdvancersPerMatch {
		t.Fatalf("want %d advancers, got %v", AdvancersPerMatch, want)
	}
	got := factData(out.Facts, FactPlayersAdvanced, "advancingPlayerIds")
	if got != joinPlayerIDs(want) {
		t.Fatalf("advancers=%s want %s", got, joinPlayerIDs(want))
	}
	for _, id := range want {
		if id == standings[0].PlayerID {
			t.Fatal("forfeited player must not advance")
		}
	}
}
