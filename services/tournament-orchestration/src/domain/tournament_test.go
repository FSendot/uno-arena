package domain

import (
	"runtime"
	"strconv"
	"testing"
	"time"
)

func mustCreate(t *testing.T, id TournamentID, capacity int) *Tournament {
	t.Helper()
	tr, out := CreateTournament(CreateTournamentCommand{
		CommandID:    CommandID("create-" + string(id)),
		TournamentID: id,
		Capacity:     capacity,
		RetryBudget:  2,
		BatchSize:    2,
	})
	if tr == nil || out.Kind != OutcomeAccepted {
		t.Fatalf("CreateTournament: %+v", out)
	}
	return tr
}

func mustRegister(t *testing.T, tr *Tournament, player PlayerID) {
	t.Helper()
	out := tr.RegisterPlayer(RegisterPlayerCommand{
		CommandID: CommandID("reg-" + string(player)),
		PlayerID:  player,
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 1 {
		t.Fatalf("RegisterPlayer(%s): %+v", player, out)
	}
}

func mustCloseAndSeed(t *testing.T, tr *Tournament, round int) {
	t.Helper()
	out := tr.CloseRegistration(CloseRegistrationCommand{CommandID: "close-1"})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("CloseRegistration: %+v", out)
	}
	out = tr.SeedRound(SeedRoundCommand{CommandID: CommandID("seed-" + strconv.Itoa(round)), RoundNumber: round})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("SeedRound: %+v", out)
	}
}

func mustProvision(t *testing.T, tr *Tournament, round int) {
	t.Helper()
	out := tr.ProvisionRoundMatches(ProvisionRoundMatchesCommand{
		CommandID:   CommandID("prov-" + strconv.Itoa(round)),
		RoundNumber: round,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("ProvisionRoundMatches: %+v", out)
	}
}

func standing(id PlayerID, wins, points int, at time.Time) PlayerMatchStanding {
	return PlayerMatchStanding{
		PlayerID:             id,
		MatchWins:            wins,
		CumulativeCardPoints: points,
		FinalGameCompletedAt: at,
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func hasFact(facts []Fact, name FactName) bool {
	for _, f := range facts {
		if f.Name == name {
			return true
		}
	}
	return false
}

func factData(facts []Fact, name FactName, key string) string {
	for _, f := range facts {
		if f.Name == name {
			return f.Data[key]
		}
	}
	return ""
}

func TestRegistration_CapacityUniqueAndIdempotent(t *testing.T) {
	tr := mustCreate(t, "t-reg", 2)
	mustRegister(t, tr, "p1")
	out := tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "reg-p1-again", PlayerID: "p1"})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("duplicate register: %+v", out)
	}
	mustRegister(t, tr, "p2")
	out = tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "reg-p3", PlayerID: "p3"})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectCapacityExceeded {
		t.Fatalf("over capacity: %+v", out)
	}
	if tr.RegisteredCount() != 2 {
		t.Fatalf("count=%d", tr.RegisteredCount())
	}
}

func TestMillionCapacity_NoHugePreallocation(t *testing.T) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	tr, out := CreateTournament(CreateTournamentCommand{
		CommandID:    "create-million",
		TournamentID: "t-million",
		Capacity:     1_000_000,
	})
	if tr == nil || out.Kind != OutcomeAccepted {
		t.Fatalf("create: %+v", out)
	}
	mustRegister(t, tr, "only-one")
	runtime.ReadMemStats(&after)
	grew := after.HeapAlloc - before.HeapAlloc
	if after.HeapAlloc < before.HeapAlloc {
		grew = 0
	}
	const maxBytes = 8 << 20
	if grew > maxBytes {
		t.Fatalf("heap grew by %d bytes creating million-capacity tournament; likely preallocated", grew)
	}
	if tr.Capacity() != 1_000_000 || tr.RegisteredCount() != 1 {
		t.Fatalf("capacity=%d registered=%d", tr.Capacity(), tr.RegisteredCount())
	}
}

func TestPhases_RegistrationToCompleted(t *testing.T) {
	base := time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
	tr := mustCreate(t, "t-flow", 12)
	for i := 1; i <= 11; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	if tr.Phase() != PhaseInProgress {
		t.Fatalf("phase=%s", tr.Phase())
	}
	r1, ok := tr.Round(1)
	if !ok || r1.IsFinal {
		t.Fatalf("round1 final=%v ok=%v", r1.IsFinal, ok)
	}
	mustProvision(t, tr, 1)

	for i := range r1.Slots {
		slot := r1.Slots[i]
		standings := make([]PlayerMatchStanding, len(slot.SeededPlayers))
		for j, pid := range slot.SeededPlayers {
			wins := 0
			if j < 3 {
				wins = 3 - j
			}
			standings[j] = standing(pid, wins, j*10, base)
		}
		out := tr.RecordMatchResult(RecordMatchResultCommand{
			CommandID:         CommandID("res-" + string(slot.SlotID)),
			EventID:           EventID("evt-" + string(slot.SlotID)),
			RoomID:            slot.RoomID,
			RoundNumber:       1,
			SlotID:            slot.SlotID,
			CompletionVersion: 1,
			Standings:         standings,
		})
		if out.Kind != OutcomeAccepted {
			t.Fatalf("record %s: %+v", slot.SlotID, out)
		}
	}
	out := tr.CompleteRound(CompleteRoundCommand{CommandID: "cr-1", RoundNumber: 1})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("complete round1: %+v", out)
	}

	out = tr.SeedRound(SeedRoundCommand{CommandID: "seed-2", RoundNumber: 2})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("seed2: %+v", out)
	}
	r2, _ := tr.Round(2)
	if !r2.IsFinal {
		t.Fatalf("expected final round with remaining<=10")
	}
	mustProvision(t, tr, 2)
	slot := r2.Slots[0]
	standings := make([]PlayerMatchStanding, len(slot.SeededPlayers))
	for i, pid := range slot.SeededPlayers {
		standings[i] = standing(pid, len(slot.SeededPlayers)-i, i, base)
	}
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID:         "res-final",
		EventID:           "evt-final",
		RoomID:            slot.RoomID,
		RoundNumber:       2,
		SlotID:            slot.SlotID,
		CompletionVersion: 1,
		Standings:         standings,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("final result: %+v", out)
	}
	out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr-2", RoundNumber: 2})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("complete final: %+v", out)
	}
	out = tr.CompleteTournament(CompleteTournamentCommand{CommandID: "done"})
	if out.Kind != OutcomeAccepted || tr.Phase() != PhaseCompleted {
		t.Fatalf("complete tournament: %+v phase=%s", out, tr.Phase())
	}
	if !tr.Champion().Valid() {
		t.Fatal("expected champion")
	}
}

func TestFinalThreshold_ExactlyTenIsFinal(t *testing.T) {
	tr := mustCreate(t, "t-10", 10)
	for i := 1; i <= 10; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	r, _ := tr.Round(1)
	if !r.IsFinal || len(r.Slots) != 1 {
		t.Fatalf("isFinal=%v slots=%d", r.IsFinal, len(r.Slots))
	}
}

func TestFinalThreshold_ElevenSeedsNonFinal(t *testing.T) {
	tr := mustCreate(t, "t-11", 11)
	for i := 1; i <= 11; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	r, _ := tr.Round(1)
	if r.IsFinal {
		t.Fatal("11 players must not be final")
	}
	if len(r.Slots) != 2 {
		t.Fatalf("slots=%d want 2", len(r.Slots))
	}
	if len(r.Slots[0].SeededPlayers) != 6 || len(r.Slots[1].SeededPlayers) != 5 {
		t.Fatalf("sizes %d %d want 6 5 (not 10+1)",
			len(r.Slots[0].SeededPlayers), len(r.Slots[1].SeededPlayers))
	}
}

func TestBuildSlots_BalancesEvenly(t *testing.T) {
	cases := []struct {
		n     int
		sizes []int
	}{
		{11, []int{6, 5}},
		{12, []int{6, 6}},
		{20, []int{10, 10}},
		{21, []int{7, 7, 7}},
		{23, []int{8, 8, 7}},
		{25, []int{9, 8, 8}},
	}
	for _, tc := range cases {
		players := make([]PlayerID, tc.n)
		for i := 0; i < tc.n; i++ {
			players[i] = PlayerID("p" + strconv.Itoa(i+1))
		}
		slots := buildSlots(players)
		if len(slots) != len(tc.sizes) {
			t.Fatalf("n=%d slots=%d want %d", tc.n, len(slots), len(tc.sizes))
		}
		offset := 0
		for i, want := range tc.sizes {
			got := len(slots[i].SeededPlayers)
			if got != want {
				t.Fatalf("n=%d slot %d size=%d want %d", tc.n, i, got, want)
			}
			if got > PlayersPerRoom || got < AdvancersPerMatch {
				t.Fatalf("n=%d slot %d size=%d out of [%d,%d]", tc.n, i, got, AdvancersPerMatch, PlayersPerRoom)
			}
			if slots[i].SeededPlayers[0] != players[offset] {
				t.Fatalf("n=%d slot %d broke input order", tc.n, i)
			}
			offset += got
		}
	}
}

func TestPartialRound_CannotComplete(t *testing.T) {
	base := time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
	tr := mustCreate(t, "t-partial", 20)
	for i := 1; i <= 20; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	r, _ := tr.Round(1)
	slot := r.Slots[0]
	standings := make([]PlayerMatchStanding, len(slot.SeededPlayers))
	for i, pid := range slot.SeededPlayers {
		wins := 0
		if i < AdvancersPerMatch {
			wins = AdvancersPerMatch - i
		}
		standings[i] = standing(pid, wins, i, base)
	}
	out := tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "res-one", EventID: "evt-one",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("record: %+v", out)
	}
	out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr", RoundNumber: 1})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectRoundIncomplete {
		t.Fatalf("expected incomplete reject: %+v", out)
	}
}

func TestCommandIdempotency_Recall(t *testing.T) {
	tr := mustCreate(t, "t-idem", 5)
	out1 := tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "same", PlayerID: "p1"})
	out2 := tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "same", PlayerID: "p1"})
	if out2.Kind != OutcomeDuplicate {
		t.Fatalf("want duplicate, got %+v", out2)
	}
	if len(out1.Facts) != len(out2.Facts) {
		t.Fatalf("facts mismatch")
	}
}

func TestRejectedCommands_NoFacts(t *testing.T) {
	tr := mustCreate(t, "t-rej", 1)
	mustRegister(t, tr, "p1")
	out := tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "x", PlayerID: "p2"})
	if out.Kind != OutcomeRejected || out.Facts != nil {
		t.Fatalf("rejected must have nil facts: %+v", out)
	}
}

func TestCancelTournament(t *testing.T) {
	tr := mustCreate(t, "t-cancel", 3)
	mustRegister(t, tr, "p1")
	out := tr.CancelTournament(CancelTournamentCommand{CommandID: "cancel"})
	if out.Kind != OutcomeAccepted || tr.Phase() != PhaseCancelled {
		t.Fatalf("%+v phase=%s", out, tr.Phase())
	}
	out = tr.RegisterPlayer(RegisterPlayerCommand{CommandID: "late", PlayerID: "p2"})
	if out.Kind != OutcomeRejected {
		t.Fatalf("register after cancel: %+v", out)
	}
}
