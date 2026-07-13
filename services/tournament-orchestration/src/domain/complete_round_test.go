package domain

import (
	"testing"
)

func readyCompleteCtx(tid TournamentID, rn int) CompleteRoundContext {
	return CompleteRoundContext{
		TournamentID:               tid,
		Exists:                     true,
		Phase:                      PhaseInProgress,
		RoundNumber:                rn,
		RoundFound:                 true,
		RoundStatus:                RoundInProgress,
		IsFinal:                    false,
		AssignedCount:              4,
		ResolvedCount:              4,
		QuarantinedCount:           0,
		AdvancingCount:             12,
		QuarantinedBatches:         0,
		AdvancementRecordsPlayers:  12,
		NormalizedAdvancingPlayers: 12,
	}
}

func TestDecideCompleteRound_InvalidCommand(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("want invalid_command, got %+v", d)
	}
	d = DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 0})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("want invalid_command for round 0, got %+v", d)
	}
}

func TestDecideCompleteRound_TournamentNotFound(t *testing.T) {
	d := DecideCompleteRound(CompleteRoundContext{Exists: false}, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectInvalidIdentity {
		t.Fatalf("want invalid_identity, got %+v", d)
	}
}

func TestDecideCompleteRound_Terminal(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.Phase = PhaseCompleted
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectAlreadyTerminal {
		t.Fatalf("want already_terminal, got %+v", d)
	}
}

func TestDecideCompleteRound_RoundNotFound(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.RoundFound = false
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectRoundNotFound {
		t.Fatalf("want round_not_found, got %+v", d)
	}
}

func TestDecideCompleteRound_AlreadyCompletedFactless(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.RoundStatus = RoundCompleted
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundAlreadyDone || d.Outcome.Kind != OutcomeAccepted {
		t.Fatalf("want already_done accepted, got %+v", d)
	}
	if len(d.Outcome.Facts) != 0 {
		t.Fatalf("already-complete must be factless, facts=%v", d.Outcome.Facts)
	}
}

func TestDecideCompleteRound_QuarantinedBatch(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.QuarantinedBatches = 1
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectQuarantined {
		t.Fatalf("want quarantined, got %+v", d)
	}
}

func TestDecideCompleteRound_QuarantinedSlot(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.QuarantinedCount = 1
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectQuarantined {
		t.Fatalf("want quarantined, got %+v", d)
	}
}

func TestDecideCompleteRound_Incomplete(t *testing.T) {
	cases := []CompleteRoundContext{
		func() CompleteRoundContext { c := readyCompleteCtx("t1", 1); c.ResolvedCount = 3; return c }(),
		func() CompleteRoundContext { c := readyCompleteCtx("t1", 1); c.AssignedCount = 0; return c }(),
		func() CompleteRoundContext {
			c := readyCompleteCtx("t1", 1)
			c.AdvancingCount = 0
			c.AdvancementRecordsPlayers = 0
			c.NormalizedAdvancingPlayers = 0
			return c
		}(),
		func() CompleteRoundContext { c := readyCompleteCtx("t1", 1); c.RoundStatus = RoundSeeded; return c }(),
	}
	for i, ctx := range cases {
		d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
		if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
			t.Fatalf("case %d: want round_incomplete, got %+v", i, d)
		}
	}
}

func TestDecideCompleteRound_AdvancingDrift(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.AdvancementRecordsPlayers = 11
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
		t.Fatalf("want drift reject, got %+v", d)
	}
}

func TestDecideCompleteRound_NormalizedAdvancingDrift(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.NormalizedAdvancingPlayers = 11
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundReject || d.Outcome.Rejection.Code != RejectRoundIncomplete {
		t.Fatalf("want normalized drift reject, got %+v", d)
	}
}

func TestDecideCompleteRound_Success(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundSuccess || d.Outcome.Kind != OutcomeAccepted {
		t.Fatalf("want success, got %+v", d)
	}
	if d.RemainingPlayers != 12 || !hasFact(d.Outcome.Facts, FactTournamentRoundCompleted) {
		t.Fatalf("want remaining=12 + RoundCompleted fact, got %+v", d)
	}
	f := d.Outcome.Facts[0]
	if f.Data["remainingPlayers"] != "12" || f.Data["isFinal"] != "false" || f.Data["roundNumber"] != "1" {
		t.Fatalf("fact data=%v", f.Data)
	}
	if d.NextRound == nil || d.NextRound.RoundNumber != 2 || d.NextRound.Source != SeedingSourceAdvancement {
		t.Fatalf("want next-round plan, got %+v", d.NextRound)
	}
	if d.NextRound.JobCommandID != "seed:t1:r2" || d.NextRound.SourceRoundNumber != 1 {
		t.Fatalf("job identity=%+v", d.NextRound)
	}
	if d.NextRound.PlayerCount != 12 || d.NextRound.SlotCount != 2 || d.NextRound.IsFinal {
		t.Fatalf("plan sizes=%+v", d.NextRound)
	}
}

func TestDecideCompleteRound_SuccessFinal(t *testing.T) {
	ctx := readyCompleteCtx("t1", 1)
	ctx.IsFinal = true
	ctx.AssignedCount = 1
	ctx.ResolvedCount = 1
	ctx.AdvancingCount = 8
	ctx.AdvancementRecordsPlayers = 8
	ctx.NormalizedAdvancingPlayers = 8
	ctx.FinalStandings = []PlayerID{"champion", "runner-up", "third"}
	d := DecideCompleteRound(ctx, CompleteRoundCommand{CommandID: "c", RoundNumber: 1})
	if d.Kind != CompleteRoundSuccess || d.RemainingPlayers != 8 || d.IsFinal != true {
		t.Fatalf("want final success remaining=8, got %+v", d)
	}
	if d.Outcome.Facts[0].Data["isFinal"] != "true" {
		t.Fatalf("isFinal fact=%v", d.Outcome.Facts[0].Data)
	}
	if d.NextRound != nil {
		t.Fatalf("final must not schedule next seeding, got %+v", d.NextRound)
	}
	if d.TournamentCompletion == nil || d.TournamentCompletion.ChampionID != "champion" {
		t.Fatalf("final must carry tournament completion, got %+v", d.TournamentCompletion)
	}
	if !hasFact(d.Outcome.Facts, FactTournamentCompleted) {
		t.Fatalf("final must publish TournamentCompleted, got %+v", d.Outcome.Facts)
	}
}

func TestCompleteRoundCommandID(t *testing.T) {
	got := CompleteRoundCommandID("tid-1", 3)
	if got != "complete:tid-1:r3" {
		t.Fatalf("got %q", got)
	}
}
