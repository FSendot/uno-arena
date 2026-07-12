package domain

import (
	"strings"
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

func TestFinalMatchDoesNotEmitPlayersAdvanced(t *testing.T) {
	tr := mustCreate(t, "t-final-no-pa", 8)
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
	if hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatal("final match must not emit PlayersAdvanced")
	}
	if !hasFact(out.Facts, FactTournamentMatchResultRecorded) {
		t.Fatal("final must still record match result")
	}
	round, _ = tr.Round(1)
	if len(round.Slots[0].Advancing) != len(standings) {
		t.Fatalf("final slot must store full ranked order, got %d want %d",
			len(round.Slots[0].Advancing), len(standings))
	}
}

func TestFinalCompletesTournamentWithFinalStandings(t *testing.T) {
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
	if factData(out.Facts, FactTournamentCompleted, "championId") != "" {
		t.Fatal("TournamentCompleted must not carry championId")
	}
	ranked, err := RankStandings(standings)
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := make([]PlayerID, len(ranked))
	for i := range ranked {
		wantIDs[i] = ranked[i].PlayerID
	}
	got := factData(out.Facts, FactTournamentCompleted, "finalStandings")
	if got != joinPlayerIDs(wantIDs) {
		t.Fatalf("finalStandings=%s want %s", got, joinPlayerIDs(wantIDs))
	}
	if tr.Champion() != wantIDs[0] {
		t.Fatalf("champion derived from finalStandings[0]=%s want %s", tr.Champion(), wantIDs[0])
	}
	if tr.Phase() != PhaseCompleted {
		t.Fatalf("phase=%s", tr.Phase())
	}
}

func TestCompleteTournament_RejectsInvalidFinalStandingsAtomically(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(slot *BracketSlot)
		wantPhase TournamentPhase
	}{
		{
			name: "empty",
			mutate: func(slot *BracketSlot) {
				slot.Advancing = nil
			},
		},
		{
			name: "duplicates",
			mutate: func(slot *BracketSlot) {
				slot.Advancing = []PlayerID{"p0", "p0", "p1"}
			},
		},
		{
			name: "more_than_ten",
			mutate: func(slot *BracketSlot) {
				ids := make([]PlayerID, 11)
				for i := range ids {
					ids[i] = PlayerID("x" + itoa(i))
				}
				slot.Advancing = ids
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tr := mustCreate(t, TournamentID("t-bad-"+tt.name), 4)
			for i := 0; i < 4; i++ {
				mustRegister(t, tr, PlayerID("p"+itoa(i)))
			}
			mustCloseAndSeed(t, tr, 1)
			mustProvision(t, tr, 1)
			round, _ := tr.Round(1)
			base := time.Date(2026, 7, 10, 18, 30, 0, 0, time.UTC)
			slot := round.Slots[0]
			standings := rankedStandings(slot.SeededPlayers, base)
			out := tr.RecordMatchResult(RecordMatchResultCommand{
				CommandID: "fr", EventID: "fe", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
				CompletionVersion: 1, Standings: standings,
			})
			if out.Kind != OutcomeAccepted {
				t.Fatalf("record: %+v", out)
			}
			out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr", RoundNumber: 1})
			if out.Kind != OutcomeAccepted {
				t.Fatalf("complete round: %+v", out)
			}
			round, _ = tr.Round(1)
			tt.mutate(&round.Slots[0])
			out = tr.CompleteTournament(CompleteTournamentCommand{CommandID: "ct"})
			if out.Kind != OutcomeRejected {
				t.Fatalf("want reject, got %+v", out)
			}
			if tr.Phase() == PhaseCompleted {
				t.Fatal("invalid final order must not complete tournament")
			}
		})
	}
}

func TestRecordMatchResult_NonFinalAdvancersCardinality(t *testing.T) {
	base := time.Date(2026, 7, 10, 20, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		standingsFn func(seeded []PlayerID) []PlayerMatchStanding
		wantLen     int
		wantReject  bool
	}{
		{
			name: "three_eligible",
			standingsFn: func(seeded []PlayerID) []PlayerMatchStanding {
				return rankedStandings(seeded, base)
			},
			wantLen: 3,
		},
		{
			name: "two_eligible_after_forfeits",
			standingsFn: func(seeded []PlayerID) []PlayerMatchStanding {
				s := rankedStandings(seeded, base)
				for i := 2; i < len(s); i++ {
					s[i].Forfeited = true
				}
				return s
			},
			wantLen: 2,
		},
		{
			name: "one_eligible_after_forfeits",
			standingsFn: func(seeded []PlayerID) []PlayerMatchStanding {
				s := rankedStandings(seeded, base)
				for i := 1; i < len(s); i++ {
					s[i].Forfeited = true
				}
				return s
			},
			wantLen: 1,
		},
		{
			name: "zero_eligible_rejected",
			standingsFn: func(seeded []PlayerID) []PlayerMatchStanding {
				s := rankedStandings(seeded, base)
				for i := range s {
					s[i].Forfeited = true
				}
				return s
			},
			wantReject: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tr := mustCreate(t, TournamentID("t-card-"+tt.name), 12)
			for i := 0; i < 12; i++ {
				mustRegister(t, tr, PlayerID("p"+itoa(i)))
			}
			mustCloseAndSeed(t, tr, 1)
			mustProvision(t, tr, 1)
			round, _ := tr.Round(1)
			if round.IsFinal {
				t.Fatal("expected non-final")
			}
			slot := round.Slots[0]
			standings := tt.standingsFn(slot.SeededPlayers)
			out := tr.RecordMatchResult(RecordMatchResultCommand{
				CommandID: "res", EventID: "evt", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
				CompletionVersion: 1, Standings: standings,
			})
			if tt.wantReject {
				if out.Kind != OutcomeRejected {
					t.Fatalf("want reject, got %+v", out)
				}
				if hasFact(out.Facts, FactPlayersAdvanced) {
					t.Fatal("zero eligible must not emit PlayersAdvanced")
				}
				return
			}
			if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactPlayersAdvanced) {
				t.Fatalf("record: %+v", out)
			}
			got := splitCSVTest(factData(out.Facts, FactPlayersAdvanced, "advancingPlayerIds"))
			if len(got) != tt.wantLen {
				t.Fatalf("advancers=%v want len %d", got, tt.wantLen)
			}
			seen := map[string]struct{}{}
			for _, id := range got {
				if id == "" {
					t.Fatal("empty advancer id")
				}
				if _, dup := seen[id]; dup {
					t.Fatalf("duplicate advancer %s", id)
				}
				seen[id] = struct{}{}
			}
		})
	}
}

func splitCSVTest(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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
