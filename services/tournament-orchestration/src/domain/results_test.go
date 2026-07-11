package domain

import (
	"testing"
	"time"
)

func TestRecordMatchResultDuplicateConflictOutOfOrder(t *testing.T) {
	tr := mustCreate(t, "t-results", 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	base := time.Date(2026, 7, 10, 16, 0, 0, 0, time.UTC)

	slot := round.Slots[0]
	standings := rankedStandings(slot.SeededPlayers, base)

	out := tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID:         "res-a",
		EventID:           "evt-a",
		RoomID:            slot.RoomID,
		RoundNumber:       1,
		SlotID:            slot.SlotID,
		CompletionVersion: 7,
		Standings:         standings,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("record: %+v", out)
	}
	if !hasFact(out.Facts, FactTournamentMatchResultRecorded) || !hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatalf("missing facts: %+v", out.Facts)
	}
	adv := SelectAdvancers(standings)
	got := factData(out.Facts, FactPlayersAdvanced, "advancingPlayerIds")
	if got != joinPlayerIDs(adv) {
		t.Fatalf("advancers=%s want %s", got, joinPlayerIDs(adv))
	}

	tests := []struct {
		name      string
		cmd       RecordMatchResultCommand
		wantKind  OutcomeKind
		wantFact  FactName
		wantEmpty bool
	}{
		{
			name: "duplicate_same_event_id",
			cmd: RecordMatchResultCommand{
				CommandID:         "res-a-dup-event",
				EventID:           "evt-a",
				RoomID:            slot.RoomID,
				RoundNumber:       1,
				SlotID:            slot.SlotID,
				CompletionVersion: 7,
				Standings:         standings,
			},
			wantKind: OutcomeDuplicate,
		},
		{
			name: "duplicate_same_room_completion_version",
			cmd: RecordMatchResultCommand{
				CommandID:         "res-a-dup-key",
				EventID:           "evt-a2",
				RoomID:            slot.RoomID,
				RoundNumber:       1,
				SlotID:            slot.SlotID,
				CompletionVersion: 7,
				Standings:         standings,
			},
			wantKind:  OutcomeAccepted,
			wantEmpty: true,
		},
		{
			name: "conflict_different_standings_same_version",
			cmd: RecordMatchResultCommand{
				CommandID:         "res-conflict",
				EventID:           "evt-conflict",
				RoomID:            slot.RoomID,
				RoundNumber:       1,
				SlotID:            slot.SlotID,
				CompletionVersion: 7,
				Standings:         rankedStandingsConflict(slot.SeededPlayers, base),
			},
			wantKind: OutcomeAccepted,
			wantFact: FactTournamentResultQuarantined,
		},
		{
			name: "room_mismatch_quarantine",
			cmd: RecordMatchResultCommand{
				CommandID:         "res-mismatch",
				EventID:           "evt-mismatch",
				RoomID:            "wrong-room",
				RoundNumber:       1,
				SlotID:            slot.SlotID,
				CompletionVersion: 9,
				Standings:         standings,
			},
			wantKind: OutcomeAccepted,
			wantFact: FactTournamentResultQuarantined,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := tr.RecordMatchResult(tt.cmd)
			if out.Kind != tt.wantKind {
				t.Fatalf("kind=%s want %s (%+v)", out.Kind, tt.wantKind, out)
			}
			if tt.wantEmpty && len(out.Facts) != 0 {
				t.Fatalf("want no facts, got %+v", out.Facts)
			}
			if tt.wantFact != "" && !hasFact(out.Facts, tt.wantFact) {
				t.Fatalf("want fact %s, got %+v", tt.wantFact, out.Facts)
			}
		})
	}
}

func TestAbandonedMatchQuarantinesWithoutAdvancement(t *testing.T) {
	tr := mustCreate(t, "t-abandon", 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	base := time.Date(2026, 7, 10, 16, 30, 0, 0, time.UTC)
	out := tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID:         "abandon-1",
		EventID:           "evt-abandon",
		RoomID:            slot.RoomID,
		RoundNumber:       1,
		SlotID:            slot.SlotID,
		CompletionVersion: 3,
		Standings:         rankedStandings(slot.SeededPlayers, base),
		IsAbandoned:       true,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentResultQuarantined) {
		t.Fatalf("abandoned: %+v", out)
	}
	if hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatal("abandoned must not advance")
	}
	round, _ = tr.Round(1)
	slot = round.Slots[0]
	if slot.HasResult || len(slot.Advancing) != 0 || slot.Status != SlotQuarantined {
		t.Fatalf("slot=%+v", slot)
	}
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID:         "abandon-replay",
		EventID:           "evt-abandon",
		RoomID:            slot.RoomID,
		RoundNumber:       1,
		SlotID:            slot.SlotID,
		CompletionVersion: 3,
		Standings:         rankedStandings(slot.SeededPlayers, base),
		IsAbandoned:       true,
	})
	if out.Kind != OutcomeDuplicate {
		t.Fatalf("exact replay: %+v", out)
	}
}

func TestOutOfOrderSlotResultsThenComplete(t *testing.T) {
	tr := mustCreate(t, "t-ooo-full", 20)
	for i := 0; i < 20; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	base := time.Date(2026, 7, 10, 17, 0, 0, 0, time.UTC)

	for i := len(round.Slots) - 1; i >= 0; i-- {
		slot := round.Slots[i]
		out := tr.RecordMatchResult(RecordMatchResultCommand{
			CommandID:         CommandID("res-" + itoa(i)),
			EventID:           EventID("evt-" + itoa(i)),
			RoomID:            slot.RoomID,
			RoundNumber:       1,
			SlotID:            slot.SlotID,
			CompletionVersion: CompletionVersion(i + 1),
			Standings:         rankedStandings(slot.SeededPlayers, base),
		})
		if out.Kind != OutcomeAccepted {
			t.Fatalf("slot %d: %+v", i, out)
		}
	}

	out := tr.CompleteRound(CompleteRoundCommand{CommandID: "cr1", RoundNumber: 1})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentRoundCompleted) {
		t.Fatalf("complete: %+v", out)
	}

	advancers := round.advancingPlayers()
	out = tr.SeedRound(SeedRoundCommand{CommandID: "seed-2", RoundNumber: 2})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("seed2: %+v", out)
	}
	r2, _ := tr.Round(2)
	if !r2.IsFinal {
		t.Fatalf("expected final with %d advancers", len(advancers))
	}
	if len(r2.Slots) != 1 {
		t.Fatalf("final must be one room, slots=%d", len(r2.Slots))
	}
}

func TestFinalCompletesTournamentWithChampion(t *testing.T) {
	tr := mustCreate(t, "t-final", 8)
	for i := 0; i < 8; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	if !round.IsFinal {
		t.Fatal("expected final")
	}
	base := time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC)
	slot := round.Slots[0]
	standings := rankedStandings(slot.SeededPlayers, base)
	standings[0].MatchWins = 2

	out := tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID:         "final-res",
		EventID:           "final-evt",
		RoomID:            slot.RoomID,
		RoundNumber:       1,
		SlotID:            slot.SlotID,
		CompletionVersion: 1,
		Standings:         standings,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("record: %+v", out)
	}
	out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr-final", RoundNumber: 1})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("complete round: %+v", out)
	}
	out = tr.CompleteTournament(CompleteTournamentCommand{CommandID: "ct"})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentCompleted) {
		t.Fatalf("complete tournament: %+v", out)
	}
	champ := factData(out.Facts, FactTournamentCompleted, "championId")
	want, _ := ChampionFromStandings(standings)
	if champ != string(want) || tr.Champion() != want {
		t.Fatalf("champion=%s want %s", champ, want)
	}
	if tr.Phase() != PhaseCompleted {
		t.Fatalf("phase=%s", tr.Phase())
	}
}

func TestNextRoundWhenMoreThanTenRemain(t *testing.T) {
	tr := mustCreate(t, "t-next", 40)
	for i := 0; i < 40; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	base := time.Date(2026, 7, 10, 19, 0, 0, 0, time.UTC)
	for i := range round.Slots {
		slot := round.Slots[i]
		out := tr.RecordMatchResult(RecordMatchResultCommand{
			CommandID:         CommandID("nr-" + itoa(i)),
			EventID:           EventID("ne-" + itoa(i)),
			RoomID:            slot.RoomID,
			RoundNumber:       1,
			SlotID:            slot.SlotID,
			CompletionVersion: 1,
			Standings:         rankedStandings(slot.SeededPlayers, base),
		})
		if out.Kind != OutcomeAccepted {
			t.Fatalf("record %d: %+v", i, out)
		}
	}
	out := tr.CompleteRound(CompleteRoundCommand{CommandID: "nr-cr", RoundNumber: 1})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("complete: %+v", out)
	}
	if len(round.advancingPlayers()) <= FinalPlayerThreshold {
		t.Fatalf("expected >10 advancers, got %d", len(round.advancingPlayers()))
	}
	out = tr.SeedRound(SeedRoundCommand{CommandID: "nr-seed2", RoundNumber: 2})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("seed2: %+v", out)
	}
	r2, _ := tr.Round(2)
	if r2.IsFinal {
		t.Fatal("12 advancers must seed a non-final round")
	}
}

func rankedStandings(players []PlayerID, base time.Time) []PlayerMatchStanding {
	out := make([]PlayerMatchStanding, len(players))
	for i, pid := range players {
		out[i] = standing(pid, 0, 50+i, base.Add(time.Duration(i)*time.Second))
	}
	if len(out) > 0 {
		out[0].MatchWins = 2
	}
	if len(out) > 1 {
		out[1].MatchWins = 1
	}
	if len(out) > 2 {
		out[2].MatchWins = 1
		out[2].CumulativeCardPoints = 1
	}
	return out
}

func rankedStandingsConflict(players []PlayerID, base time.Time) []PlayerMatchStanding {
	out := rankedStandings(players, base)
	if len(out) > 1 {
		out[0].MatchWins = 0
		out[1].MatchWins = 2
	}
	return out
}
