package domain

import (
	"testing"
	"time"
)

func TestDecideRecordMatchResult_ParityWithTournamentRecordMatchResult(t *testing.T) {
	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		run  func(t *testing.T, tr *Tournament, slot BracketSlot)
	}{
		{
			name: "first_recorded",
			run: func(t *testing.T, tr *Tournament, slot BracketSlot) {
				cmd := RecordMatchResultCommand{
					CommandID: "parity-rec", EventID: "evt-rec",
					RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 1, Standings: rankedStandings(slot.SeededPlayers, base),
				}
				assertDecisionParity(t, tr, cmd)
			},
		},
		{
			name: "exact_duplicate_new_event_id",
			run: func(t *testing.T, tr *Tournament, slot BracketSlot) {
				standings := rankedStandings(slot.SeededPlayers, base)
				first := RecordMatchResultCommand{
					CommandID: "parity-1", EventID: "evt-1",
					RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 2, Standings: standings,
				}
				if out := tr.RecordMatchResult(first); out.Kind != OutcomeAccepted {
					t.Fatalf("seed: %+v", out)
				}
				dup := RecordMatchResultCommand{
					CommandID: "parity-2", EventID: "evt-2",
					RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 2, Standings: standings,
				}
				assertDecisionParity(t, tr, dup)
			},
		},
		{
			name: "conflicting_fingerprint",
			run: func(t *testing.T, tr *Tournament, slot BracketSlot) {
				standings := rankedStandings(slot.SeededPlayers, base)
				first := RecordMatchResultCommand{
					CommandID: "parity-c1", EventID: "evt-c1",
					RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 3, Standings: standings,
				}
				if out := tr.RecordMatchResult(first); out.Kind != OutcomeAccepted {
					t.Fatalf("seed: %+v", out)
				}
				conflict := RecordMatchResultCommand{
					CommandID: "parity-c2", EventID: "evt-c2",
					RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 3, Standings: rankedStandingsConflict(slot.SeededPlayers, base),
				}
				assertDecisionParity(t, tr, conflict)
			},
		},
		{
			name: "abandoned",
			run: func(t *testing.T, tr *Tournament, slot BracketSlot) {
				cmd := RecordMatchResultCommand{
					CommandID: "parity-ab", EventID: "evt-ab",
					RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 4, Standings: rankedStandings(slot.SeededPlayers, base),
					IsAbandoned: true,
				}
				assertDecisionParity(t, tr, cmd)
			},
		},
		{
			name: "room_slot_mismatch",
			run: func(t *testing.T, tr *Tournament, slot BracketSlot) {
				cmd := RecordMatchResultCommand{
					CommandID: "parity-mm", EventID: "evt-mm",
					RoomID: "wrong-room", RoundNumber: 1, SlotID: slot.SlotID,
					CompletionVersion: 5, Standings: rankedStandings(slot.SeededPlayers, base),
				}
				assertDecisionParity(t, tr, cmd)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := mustCreate(t, TournamentID("t-parity-"+tc.name), 12)
			for i := 0; i < 12; i++ {
				mustRegister(t, tr, PlayerID("p"+itoa(i)))
			}
			mustCloseAndSeed(t, tr, 1)
			mustProvision(t, tr, 1)
			round, _ := tr.Round(1)
			tc.run(t, tr, round.Slots[0])
		})
	}
}

func TestDecideRecordMatchResult_NonFinalAndFinal(t *testing.T) {
	base := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	tr := mustCreate(t, TournamentID("t-parity-final"), 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	cmd := RecordMatchResultCommand{
		CommandID: "nf", EventID: "evt-nf",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: rankedStandings(slot.SeededPlayers, base),
	}
	d := DecideRecordMatchResult(tr.BuildRoundMatchContext(cmd), cmd)
	if d.Kind != RoundMatchRecord || d.SlotStatus != SlotAdvanced {
		t.Fatalf("non-final: %+v", d)
	}
	if !hasFact(d.Outcome.Facts, FactPlayersAdvanced) {
		t.Fatal("want PlayersAdvanced")
	}

	// Drive to final round for standings parity.
	for i := range round.Slots {
		s := round.Slots[i]
		out := tr.RecordMatchResult(RecordMatchResultCommand{
			CommandID: CommandID("r" + itoa(i)), EventID: EventID("e" + itoa(i)),
			RoomID: s.RoomID, RoundNumber: 1, SlotID: s.SlotID,
			CompletionVersion: CompletionVersion(i + 1),
			Standings:         rankedStandings(s.SeededPlayers, base),
		})
		if out.Kind != OutcomeAccepted {
			t.Fatalf("slot %d: %+v", i, out)
		}
	}
	if out := tr.CompleteRound(CompleteRoundCommand{CommandID: "cr", RoundNumber: 1}); out.Kind != OutcomeAccepted {
		t.Fatalf("complete: %+v", out)
	}
	if out := tr.SeedRound(SeedRoundCommand{CommandID: "seed2", RoundNumber: 2}); out.Kind != OutcomeAccepted {
		t.Fatalf("seed2: %+v", out)
	}
	if out := tr.ProvisionRoundMatches(ProvisionRoundMatchesCommand{CommandID: "prov2", RoundNumber: 2}); out.Kind != OutcomeAccepted {
		t.Fatalf("prov2: %+v", out)
	}
	final, ok := tr.Round(2)
	if !ok || !final.IsFinal {
		t.Fatalf("want final round, got %+v", final)
	}
	fslot := final.Slots[0]
	fcmd := RecordMatchResultCommand{
		CommandID: "fin", EventID: "evt-fin",
		RoomID: fslot.RoomID, RoundNumber: 2, SlotID: fslot.SlotID,
		CompletionVersion: 9, Standings: rankedStandings(fslot.SeededPlayers, base),
	}
	assertDecisionParity(t, tr, fcmd)
}

func assertDecisionParity(t *testing.T, tr *Tournament, cmd RecordMatchResultCommand) {
	t.Helper()
	ctx := tr.BuildRoundMatchContext(cmd)
	d := DecideRecordMatchResult(ctx, cmd)
	// Apply via RecordMatchResult on a clone path: call RecordMatchResult and compare kinds/facts.
	out := tr.RecordMatchResult(cmd)
	if d.Outcome.Kind != out.Kind {
		t.Fatalf("kind decide=%s record=%s", d.Outcome.Kind, out.Kind)
	}
	if len(d.Outcome.Facts) != len(out.Facts) {
		t.Fatalf("facts decide=%d record=%d decide=%+v record=%+v", len(d.Outcome.Facts), len(out.Facts), d.Outcome.Facts, out.Facts)
	}
	for i := range out.Facts {
		if d.Outcome.Facts[i].Name != out.Facts[i].Name {
			t.Fatalf("fact[%d] decide=%s record=%s", i, d.Outcome.Facts[i].Name, out.Facts[i].Name)
		}
	}
}

func TestProgressShardID(t *testing.T) {
	if ProgressShardCount != 64 {
		t.Fatalf("shard count=%d", ProgressShardCount)
	}
	if ProgressShardID(0) != 0 || ProgressShardID(64) != 0 || ProgressShardID(65) != 1 {
		t.Fatalf("shard mapping")
	}
}

func TestDecideRecordMatchResult_UnknownRoomDoesNotAffectClaimedSlot(t *testing.T) {
	tr := mustCreate(t, TournamentID("t-unk-room"), 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	base := time.Date(2026, 7, 11, 14, 0, 0, 0, time.UTC)
	cmd := RecordMatchResultCommand{
		CommandID: "unk", EventID: "evt-unk",
		RoomID: "unknown-room", RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: rankedStandings(slot.SeededPlayers, base),
	}
	d := DecideRecordMatchResult(tr.BuildRoundMatchContext(cmd), cmd)
	if d.Kind != RoundMatchQuarantineUnresolved || d.AffectsSlot || d.WriteMatchResult || d.BlockRound {
		t.Fatalf("unknown room must quarantine without affecting slot: %+v", d)
	}
	_ = tr.RecordMatchResult(cmd)
	round, _ = tr.Round(1)
	got := round.Slots[0]
	if got.Status == SlotQuarantined {
		t.Fatal("claimed slot of another player must not be quarantined")
	}
}

func TestDecideRecordMatchResult_IdentityMismatchAgainstResolved(t *testing.T) {
	tr := mustCreate(t, TournamentID("t-id-mm"), 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	other := round.Slots[1]
	base := time.Date(2026, 7, 11, 14, 30, 0, 0, time.UTC)
	cmd := RecordMatchResultCommand{
		CommandID: "mm", EventID: "evt-mm",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: other.SlotID, // wrong claimed slot
		CompletionVersion: 2, Standings: rankedStandings(slot.SeededPlayers, base),
	}
	d := DecideRecordMatchResult(tr.BuildRoundMatchContext(cmd), cmd)
	if d.Kind != RoundMatchQuarantineUnresolved || !d.AffectsSlot || !d.WriteMatchResult {
		t.Fatalf("known room wrong slot must affect resolved slot: %+v", d)
	}
	if d.PersistSlot != slot.SlotID || d.PersistRound != 1 {
		t.Fatalf("persist identity=%s/%d want %s/1", d.PersistSlot, d.PersistRound, slot.SlotID)
	}
	_ = tr.applyRoundMatchDecision(cmd, d)
	round, _ = tr.Round(1)
	if round.Slots[0].Status != SlotQuarantined {
		t.Fatalf("resolved slot status=%s", round.Slots[0].Status)
	}
	if round.Slots[1].Status == SlotQuarantined {
		t.Fatal("wrong claimed slot must not be mutated")
	}
	if round.Status != RoundBlocked {
		t.Fatalf("round=%s", round.Status)
	}
}

func TestDecideRecordMatchResult_QuarantineHeldStable(t *testing.T) {
	tr := mustCreate(t, TournamentID("t-held"), 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	base := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	first := RecordMatchResultCommand{
		CommandID: "ab1", EventID: "evt-ab1",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: rankedStandings(slot.SeededPlayers, base),
		IsAbandoned: true,
	}
	if out := tr.RecordMatchResult(first); out.Kind != OutcomeAccepted {
		t.Fatalf("abandon: %+v", out)
	}
	later := RecordMatchResultCommand{
		CommandID: "later", EventID: "evt-later",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 2, Standings: rankedStandings(slot.SeededPlayers, base),
	}
	d := DecideRecordMatchResult(tr.BuildRoundMatchContext(later), later)
	if d.Kind != RoundMatchQuarantineHeld {
		t.Fatalf("kind=%s", d.Kind)
	}
	beforeRound, _ := tr.Round(1)
	before := beforeRound.Slots[0].QuarantineReason
	_ = tr.RecordMatchResult(later)
	afterRound, _ := tr.Round(1)
	after := afterRound.Slots[0]
	if after.Status != SlotQuarantined || after.QuarantineReason != before {
		t.Fatalf("held must not mutate slot: %+v", after)
	}
	// Same event id remains idempotent via recall.
	again := later
	again.CommandID = "later-replay"
	again.EventID = "evt-later"
	if out := tr.RecordMatchResult(again); out.Kind != OutcomeDuplicate {
		t.Fatalf("same event replay: %+v", out)
	}
}

func TestDecideRecordMatchResult_StandingsAreAuthoritativelyRanked(t *testing.T) {
	base := time.Date(2026, 7, 11, 14, 0, 0, 0, time.UTC)
	tr := mustCreate(t, TournamentID("t-rank-order"), 4)
	for i := 0; i < 4; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	if !round.IsFinal {
		t.Fatal("capacity 4 should seed a final round")
	}
	slot := round.Slots[0]
	// Deliberately reverse of Tournament ranking: worst first by matchWins.
	unsorted := []PlayerMatchStanding{
		{PlayerID: slot.SeededPlayers[3], MatchWins: 0, CumulativeCardPoints: 40, FinalGameCompletedAt: base.Add(3 * time.Second)},
		{PlayerID: slot.SeededPlayers[2], MatchWins: 0, CumulativeCardPoints: 30, FinalGameCompletedAt: base.Add(2 * time.Second)},
		{PlayerID: slot.SeededPlayers[1], MatchWins: 1, CumulativeCardPoints: 20, FinalGameCompletedAt: base.Add(1 * time.Second)},
		{PlayerID: slot.SeededPlayers[0], MatchWins: 2, CumulativeCardPoints: 10, FinalGameCompletedAt: base},
	}
	wantRanked, err := RankStandings(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if wantRanked[0].PlayerID == unsorted[0].PlayerID {
		t.Fatal("test setup must use unsorted input")
	}
	cmd := RecordMatchResultCommand{
		CommandID: "rank-order", EventID: "evt-rank-order",
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 1, Standings: unsorted,
	}
	d := DecideRecordMatchResult(tr.BuildRoundMatchContext(cmd), cmd)
	if d.Kind != RoundMatchRecord {
		t.Fatalf("kind=%s", d.Kind)
	}
	if len(d.Standings) != len(wantRanked) {
		t.Fatalf("standings len=%d want %d", len(d.Standings), len(wantRanked))
	}
	for i := range wantRanked {
		if d.Standings[i].PlayerID != wantRanked[i].PlayerID {
			t.Fatalf("Decision.Standings[%d]=%s want %s (must be ranked, not caller order)",
				i, d.Standings[i].PlayerID, wantRanked[i].PlayerID)
		}
	}
	if len(d.Advancing) != len(wantRanked) {
		t.Fatalf("advancing len=%d", len(d.Advancing))
	}
	for i := range wantRanked {
		if d.Advancing[i] != wantRanked[i].PlayerID {
			t.Fatalf("Advancing[%d]=%s want %s", i, d.Advancing[i], wantRanked[i].PlayerID)
		}
	}
	fpWant := fingerprintFromRanked(wantRanked)
	if d.Fingerprint != fpWant {
		t.Fatalf("fingerprint=%q want %q", d.Fingerprint, fpWant)
	}

	// Non-final: Decision.Standings still ranked; Advancing is TopThree of ranked.
	trNF := mustCreate(t, TournamentID("t-rank-nf"), 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, trNF, PlayerID("n"+itoa(i)))
	}
	mustCloseAndSeed(t, trNF, 1)
	mustProvision(t, trNF, 1)
	nfRound, _ := trNF.Round(1)
	nfSlot := nfRound.Slots[0]
	nfUnsorted := []PlayerMatchStanding{
		{PlayerID: nfSlot.SeededPlayers[3], MatchWins: 0, CumulativeCardPoints: 40, FinalGameCompletedAt: base.Add(3 * time.Second)},
		{PlayerID: nfSlot.SeededPlayers[2], MatchWins: 0, CumulativeCardPoints: 30, FinalGameCompletedAt: base.Add(2 * time.Second)},
		{PlayerID: nfSlot.SeededPlayers[1], MatchWins: 1, CumulativeCardPoints: 5, FinalGameCompletedAt: base.Add(1 * time.Second)},
		{PlayerID: nfSlot.SeededPlayers[0], MatchWins: 2, CumulativeCardPoints: 10, FinalGameCompletedAt: base},
	}
	nfRanked, err := RankStandings(nfUnsorted)
	if err != nil {
		t.Fatal(err)
	}
	wantAdv, err := TopThree(nfUnsorted)
	if err != nil {
		t.Fatal(err)
	}
	nfCmd := RecordMatchResultCommand{
		CommandID: "rank-nf", EventID: "evt-rank-nf",
		RoomID: nfSlot.RoomID, RoundNumber: 1, SlotID: nfSlot.SlotID,
		CompletionVersion: 1, Standings: nfUnsorted,
	}
	nd := DecideRecordMatchResult(trNF.BuildRoundMatchContext(nfCmd), nfCmd)
	if nd.Kind != RoundMatchRecord || nd.SlotStatus != SlotAdvanced {
		t.Fatalf("non-final: %+v", nd)
	}
	for i := range nfRanked {
		if nd.Standings[i].PlayerID != nfRanked[i].PlayerID {
			t.Fatalf("non-final Standings[%d]=%s want %s", i, nd.Standings[i].PlayerID, nfRanked[i].PlayerID)
		}
	}
	if len(nd.Advancing) != len(wantAdv) {
		t.Fatalf("non-final Advancing=%v want %v", nd.Advancing, wantAdv)
	}
	for i := range wantAdv {
		if nd.Advancing[i] != wantAdv[i] {
			t.Fatalf("non-final Advancing[%d]=%s want %s", i, nd.Advancing[i], wantAdv[i])
		}
	}
}

func TestDecideRecordMatchResult_ZeroCompletionVersionRejected(t *testing.T) {
	tr := mustCreate(t, TournamentID("t-zero-cv"), 12)
	for i := 0; i < 12; i++ {
		mustRegister(t, tr, PlayerID("p"+itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	round, _ := tr.Round(1)
	slot := round.Slots[0]
	base := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	cmd := RecordMatchResultCommand{
		CommandID: "zero-cv", EventID: "", // eventId remains optional
		RoomID: slot.RoomID, RoundNumber: 1, SlotID: slot.SlotID,
		CompletionVersion: 0, Standings: rankedStandings(slot.SeededPlayers, base),
	}
	d := DecideRecordMatchResult(tr.BuildRoundMatchContext(cmd), cmd)
	if d.Kind != RoundMatchReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("want invalid_command reject, got %+v", d)
	}
	out := tr.RecordMatchResult(cmd)
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("memory RecordMatchResult: %+v", out)
	}
	if _, ok := tr.ResultKeysSnapshot()[resultKey(slot.RoomID, 0)]; ok {
		t.Fatal("must not persist version-0 result key")
	}
}
