package domain

import (
	"strings"
	"testing"
	"time"
)

func TestSanitizeExplicitQuarantineReason_ClosedCode(t *testing.T) {
	for _, raw := range []string{
		"",
		"operator hold",
		"secret token=abc DROP TABLE students",
		"explicit_quarantine",
		"conflicting result for same room and completionVersion",
	} {
		got := SanitizeExplicitQuarantineReason(raw)
		if got != ExplicitQuarantineReasonCode {
			t.Fatalf("raw %q → %q want %q", raw, got, ExplicitQuarantineReasonCode)
		}
		if strings.Contains(got, "operator") || strings.Contains(got, "secret") || strings.Contains(got, "DROP") {
			t.Fatalf("sanitized reason must not leak raw text: %q", got)
		}
	}
}

func TestDecideQuarantineTournamentResult_Rejects(t *testing.T) {
	base := QuarantineTournamentResultContext{
		TournamentID: "t-q",
		Exists:       true,
		RoomID:       "room-1",
	}
	t.Run("missing commandId", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(base, QuarantineTournamentResultCommand{
			RoomID: "room-1", CompletionVersion: 1,
		})
		if d.Kind != QuarantineResultReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("missing roomId", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(base, QuarantineTournamentResultCommand{
			CommandID: "c1", CompletionVersion: 1,
		})
		if d.Kind != QuarantineResultReject || d.Outcome.Rejection.Code != RejectInvalidIdentity {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("zero completionVersion", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(base, QuarantineTournamentResultCommand{
			CommandID: "c1", RoomID: "room-1", CompletionVersion: 0, Reason: "ops",
		})
		if d.Kind != QuarantineResultReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
			t.Fatalf("%+v", d)
		}
		if hasFact(d.Outcome.Facts, FactTournamentResultQuarantined) {
			t.Fatal("reject must not emit fact")
		}
	})
	t.Run("tournament missing", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(QuarantineTournamentResultContext{}, QuarantineTournamentResultCommand{
			CommandID: "c1", RoomID: "room-1", CompletionVersion: 1,
		})
		if d.Kind != QuarantineResultReject || d.Outcome.Rejection.Code != RejectInvalidIdentity {
			t.Fatalf("%+v", d)
		}
	})
}

func TestDecideQuarantineTournamentResult_AlreadyQuarantinedFactless(t *testing.T) {
	cmd := QuarantineTournamentResultCommand{
		CommandID: "c1", RoomID: "room-1", CompletionVersion: 2, Reason: "operator hold",
	}
	for _, ctx := range []QuarantineTournamentResultContext{
		{TournamentID: "t", Exists: true, RoomID: "room-1", LedgerExists: true},
		{TournamentID: "t", Exists: true, RoomID: "room-1", PriorDisposition: DispositionQuarantined},
	} {
		d := DecideQuarantineTournamentResult(ctx, cmd)
		if d.Kind != QuarantineResultAlreadyDone || len(d.Outcome.Facts) != 0 {
			t.Fatalf("want factless already-done: %+v", d)
		}
		if d.WriteMatchResult {
			t.Fatal("already-done must not write match_results")
		}
	}
}

func TestDecideQuarantineTournamentResult_FirstPaths(t *testing.T) {
	cmd := QuarantineTournamentResultCommand{
		CommandID: "c1", RoomID: "room-1", CompletionVersion: 3, Reason: "raw operator text NEVER",
	}

	t.Run("unknown room ledger only", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(QuarantineTournamentResultContext{
			TournamentID: "t", Exists: true, RoomID: "room-1", CompletionVersion: 3,
		}, cmd)
		if d.Kind != QuarantineResultLedgerOnly || d.AffectsSlot || d.WriteMatchResult {
			t.Fatalf("%+v", d)
		}
		assertExplicitQuarantineFact(t, d, "t", "room-1", "3")
		if d.PersistRound != 0 || d.PersistSlot.Valid() {
			t.Fatalf("unknown room must not claim fake round/slot: %+v", d)
		}
	})

	t.Run("recorded preserved ledger only", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(QuarantineTournamentResultContext{
			TournamentID: "t", Exists: true, RoomID: "room-1", CompletionVersion: 3,
			AssignmentResolved: true, RoundNumber: 2, SlotID: "slot_0",
			PriorDisposition: DispositionRecorded,
		}, cmd)
		if d.Kind != QuarantineResultLedgerOnly || d.WriteMatchResult {
			t.Fatalf("%+v", d)
		}
		if !d.AffectsSlot || d.PersistRound != 2 || d.PersistSlot != "slot_0" {
			t.Fatalf("recorded path must resolve assigned slot: %+v", d)
		}
		assertExplicitQuarantineFact(t, d, "t", "room-1", "3")
	})

	t.Run("assigned no result inserts quarantined", func(t *testing.T) {
		d := DecideQuarantineTournamentResult(QuarantineTournamentResultContext{
			TournamentID: "t", Exists: true, RoomID: "room-1", CompletionVersion: 3,
			AssignmentResolved: true, RoundNumber: 1, SlotID: "slot_1",
		}, cmd)
		if d.Kind != QuarantineResultInsertQuarantined || !d.WriteMatchResult || !d.AffectsSlot {
			t.Fatalf("%+v", d)
		}
		if d.PersistRound != 1 || d.PersistSlot != "slot_1" {
			t.Fatalf("must persist resolved assignment: %+v", d)
		}
		assertExplicitQuarantineFact(t, d, "t", "room-1", "3")
	})
}

func assertExplicitQuarantineFact(t *testing.T, d QuarantineTournamentResultDecision, tid, room, ver string) {
	t.Helper()
	if d.ReasonCode != ExplicitQuarantineReasonCode {
		t.Fatalf("reason code=%q", d.ReasonCode)
	}
	if !hasFact(d.Outcome.Facts, FactTournamentResultQuarantined) {
		t.Fatalf("missing fact: %+v", d.Outcome.Facts)
	}
	f := d.Outcome.Facts[0]
	if f.Data["reason"] != ExplicitQuarantineReasonCode {
		t.Fatalf("fact reason leaked raw: %q", f.Data["reason"])
	}
	if f.Data["tournamentId"] != tid || f.Data["roomId"] != room || f.Data["completionVersion"] != ver {
		t.Fatalf("fact data=%v", f.Data)
	}
	if strings.Contains(f.Data["reason"], "operator") || strings.Contains(f.Data["reason"], "NEVER") {
		t.Fatal("raw operator text must not reach outcome facts")
	}
}

func TestTournamentQuarantineTournamentResult_UsesClosedReasonAndZeroReject(t *testing.T) {
	tr := mustCreate(t, "t-q-agg", 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]

	out := tr.QuarantineTournamentResult(QuarantineTournamentResultCommand{
		CommandID: "qz", RoomID: slot.RoomID, CompletionVersion: 0, Reason: "ops",
	})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("zero version: %+v", out)
	}

	out = tr.QuarantineTournamentResult(QuarantineTournamentResultCommand{
		CommandID: "q1", RoomID: slot.RoomID, CompletionVersion: 5, Reason: "operator hold secret",
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentResultQuarantined) {
		t.Fatalf("first: %+v", out)
	}
	if out.Facts[0].Data["reason"] != ExplicitQuarantineReasonCode {
		t.Fatalf("aggregate must emit closed reason, got %q", out.Facts[0].Data["reason"])
	}

	out = tr.QuarantineTournamentResult(QuarantineTournamentResultCommand{
		CommandID: "q2", RoomID: slot.RoomID, CompletionVersion: 5, Reason: "other",
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("biz-key factless: %+v", out)
	}

	// Recorded then quarantine: preserve advancement.
	base := time.Date(2026, 7, 11, 3, 0, 0, 0, time.UTC)
	standings := rankedStandings(slot.SeededPlayers, base)
	out = tr.RecordMatchResult(RecordMatchResultCommand{
		CommandID: "rec", EventID: "evt", RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: standings,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactPlayersAdvanced) {
		t.Fatalf("record: %+v", out)
	}
	round, _ = tr.Round(1)
	priorAdv := append([]PlayerID(nil), round.Slots[0].Advancing...)
	out = tr.QuarantineTournamentResult(QuarantineTournamentResultCommand{
		CommandID: "q-rec", RoomID: slot.RoomID, CompletionVersion: 1, Reason: "ops",
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentResultQuarantined) {
		t.Fatalf("quarantine recorded: %+v", out)
	}
	round, _ = tr.Round(1)
	if len(round.Slots[0].Advancing) != len(priorAdv) {
		t.Fatal("must not overwrite advancement")
	}
}
