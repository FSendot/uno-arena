package domain

import (
	"testing"
)

func readyCompleteTournamentCtx(tid TournamentID) CompleteTournamentContext {
	return CompleteTournamentContext{
		TournamentID:   tid,
		Exists:         true,
		Phase:          PhaseInProgress,
		CurrentRound:   1,
		RoundFound:     true,
		IsFinal:        true,
		RoundCompleted: true,
		FinalSlotCount: 1,
		FinalSlotIndex: 0,
		FinalStandings: []PlayerID{"p0", "p1", "p2", "p3"},
	}
}

func TestDecideCompleteTournament_InvalidCommand(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: ""})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("want invalid_command, got %+v", d)
	}
}

func TestDecideCompleteTournament_NotFound(t *testing.T) {
	d := DecideCompleteTournament(CompleteTournamentContext{Exists: false}, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectInvalidIdentity {
		t.Fatalf("want invalid_identity, got %+v", d)
	}
}

func TestDecideCompleteTournament_AlreadyCompletedFactless(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	ctx.Phase = PhaseCompleted
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentAlreadyDone || d.Outcome.Kind != OutcomeAccepted {
		t.Fatalf("want already_done, got %+v", d)
	}
	if len(d.Outcome.Facts) != 0 {
		t.Fatalf("already-complete must be factless, facts=%v", d.Outcome.Facts)
	}
}

func TestDecideCompleteTournament_CancelledRejects(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	ctx.Phase = PhaseCancelled
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectAlreadyTerminal {
		t.Fatalf("want already_terminal, got %+v", d)
	}
}

func TestDecideCompleteTournament_NotFinal(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	ctx.IsFinal = false
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectNotFinal {
		t.Fatalf("want not_final, got %+v", d)
	}
	ctx = readyCompleteTournamentCtx("t1")
	ctx.RoundFound = false
	d = DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectNotFinal {
		t.Fatalf("want not_final for missing round, got %+v", d)
	}
}

func TestDecideCompleteTournament_RoundIncomplete(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	ctx.RoundCompleted = false
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
		t.Fatalf("want round_incomplete, got %+v", d)
	}
}

func TestDecideCompleteTournament_FinalSlotInvariant(t *testing.T) {
	cases := []struct {
		name  string
		count int
		index int
	}{
		{"missing", 0, 0},
		{"multiple", 2, 0},
		{"nonzero_only", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := readyCompleteTournamentCtx("t1")
			ctx.FinalSlotCount = tc.count
			ctx.FinalSlotIndex = tc.index
			// Corrupt slot shape must reject even if standings payload looks valid.
			d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
			if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
				t.Fatalf("want round_incomplete, got %+v", d)
			}
		})
	}
}

func TestDecideCompleteTournament_MissingAuthoritativeStandings(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	ctx.FinalStandings = nil
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
		t.Fatalf("want round_incomplete, got %+v", d)
	}
}

func TestDecideCompleteTournament_InvalidStandings(t *testing.T) {
	cases := []struct {
		name      string
		standings []PlayerID
	}{
		{"empty", nil},
		{"duplicates", []PlayerID{"p0", "p0"}},
		{"too_many", func() []PlayerID {
			ids := make([]PlayerID, 11)
			for i := range ids {
				ids[i] = PlayerID("x" + itoa(i))
			}
			return ids
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := readyCompleteTournamentCtx("t1")
			ctx.FinalStandings = tc.standings
			d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
			if d.Kind != CompleteTournamentReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
				t.Fatalf("want round_incomplete, got %+v", d)
			}
		})
	}
}

func TestDecideCompleteTournament_SuccessOrderedStandingsNoChampionId(t *testing.T) {
	ctx := readyCompleteTournamentCtx("t1")
	ctx.FinalStandings = []PlayerID{"champ", "p1", "p2"}
	d := DecideCompleteTournament(ctx, CompleteTournamentCommand{CommandID: "c"})
	if d.Kind != CompleteTournamentSuccess {
		t.Fatalf("want success, got %+v", d)
	}
	if d.ChampionID != "champ" {
		t.Fatalf("champion=%s", d.ChampionID)
	}
	if len(d.Outcome.Facts) != 1 || d.Outcome.Facts[0].Name != FactTournamentCompleted {
		t.Fatalf("facts=%+v", d.Outcome.Facts)
	}
	if d.Outcome.Facts[0].Data["championId"] != "" {
		t.Fatal("TournamentCompleted must not carry championId")
	}
	if d.Outcome.Facts[0].Data["finalStandings"] != "champ,p1,p2" {
		t.Fatalf("finalStandings=%s", d.Outcome.Facts[0].Data["finalStandings"])
	}
}

func TestDecideCancelTournament_InvalidCommand(t *testing.T) {
	d := DecideCancelTournament(CancelTournamentContext{Exists: true, Phase: PhaseRegistration}, CancelTournamentCommand{CommandID: ""})
	if d.Kind != CancelTournamentReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("want invalid_command, got %+v", d)
	}
}

func TestDecideCancelTournament_NotFound(t *testing.T) {
	d := DecideCancelTournament(CancelTournamentContext{Exists: false}, CancelTournamentCommand{CommandID: "c"})
	if d.Kind != CancelTournamentReject || d.Outcome.Rejection.Code != RejectInvalidIdentity {
		t.Fatalf("want invalid_identity, got %+v", d)
	}
}

func TestDecideCancelTournament_AlreadyCancelledFactless(t *testing.T) {
	d := DecideCancelTournament(CancelTournamentContext{
		TournamentID: "t1", Exists: true, Phase: PhaseCancelled,
	}, CancelTournamentCommand{CommandID: "c"})
	if d.Kind != CancelTournamentAlreadyDone || d.Outcome.Kind != OutcomeAccepted {
		t.Fatalf("want already_done, got %+v", d)
	}
	if len(d.Outcome.Facts) != 0 {
		t.Fatalf("already-cancelled must be factless")
	}
}

func TestDecideCancelTournament_CompletedRejects(t *testing.T) {
	d := DecideCancelTournament(CancelTournamentContext{
		TournamentID: "t1", Exists: true, Phase: PhaseCompleted,
	}, CancelTournamentCommand{CommandID: "c"})
	if d.Kind != CancelTournamentReject || d.Outcome.Rejection.Code != RejectAlreadyTerminal {
		t.Fatalf("want already_terminal, got %+v", d)
	}
}

func TestDecideCancelTournament_Success(t *testing.T) {
	for _, phase := range []TournamentPhase{PhaseRegistration, PhaseSeeding, PhaseInProgress} {
		d := DecideCancelTournament(CancelTournamentContext{
			TournamentID: "t1", Exists: true, Phase: phase,
		}, CancelTournamentCommand{CommandID: "c"})
		if d.Kind != CancelTournamentSuccess {
			t.Fatalf("phase=%s want success, got %+v", phase, d)
		}
		if len(d.Outcome.Facts) != 1 || d.Outcome.Facts[0].Name != FactTournamentCancelled {
			t.Fatalf("facts=%+v", d.Outcome.Facts)
		}
	}
}
