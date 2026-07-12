package domain

import (
	"fmt"
	"math"
	"testing"
)

func TestComputeRound1SlotPlan_Table(t *testing.T) {
	cases := []struct {
		n     int
		slots int
		sizes []int
	}{
		{1, 1, []int{1}},
		{10, 1, []int{10}},
		{11, 2, []int{6, 5}},
		{12, 2, []int{6, 6}},
		{20, 2, []int{10, 10}},
		{21, 3, []int{7, 7, 7}},
		{23, 3, []int{8, 8, 7}},
		{25, 3, []int{9, 8, 8}},
	}
	for _, tc := range cases {
		plan, err := ComputeRound1SlotPlan(tc.n)
		if err != nil {
			t.Fatalf("n=%d: %v", tc.n, err)
		}
		if plan.PlayerCount != tc.n || plan.SlotCount != tc.slots {
			t.Fatalf("n=%d got N=%d S=%d want N=%d S=%d", tc.n, plan.PlayerCount, plan.SlotCount, tc.n, tc.slots)
		}
		if len(plan.SlotSizes) != len(tc.sizes) {
			t.Fatalf("n=%d sizes len=%d want %d", tc.n, len(plan.SlotSizes), len(tc.sizes))
		}
		sum := 0
		for i, want := range tc.sizes {
			if plan.SlotSizes[i] != want {
				t.Fatalf("n=%d slot %d size=%d want %d", tc.n, i, plan.SlotSizes[i], want)
			}
			sum += plan.SlotSizes[i]
			if SlotIDForIndex(i) != SlotID(fmt.Sprintf("slot_%d", i)) {
				t.Fatalf("slot id mismatch at %d", i)
			}
		}
		if sum != tc.n {
			t.Fatalf("n=%d size sum=%d", tc.n, sum)
		}
		if tc.n <= FinalPlayerThreshold && !plan.IsFinal {
			t.Fatalf("n=%d must be final", tc.n)
		}
		if tc.n > FinalPlayerThreshold && plan.IsFinal {
			t.Fatalf("n=%d must not be final", tc.n)
		}
	}
}

func TestComputeRound1SlotPlan_MillionProperty(t *testing.T) {
	const n = 1_000_000
	plan, err := ComputeRound1SlotPlan(n)
	if err != nil {
		t.Fatal(err)
	}
	wantS := int(math.Ceil(float64(n) / float64(PlayersPerRoom)))
	if plan.SlotCount != wantS {
		t.Fatalf("S=%d want %d", plan.SlotCount, wantS)
	}
	base := n / wantS
	rem := n % wantS
	if plan.BaseSize != base || plan.Remainder != rem {
		t.Fatalf("base/rem=%d/%d want %d/%d", plan.BaseSize, plan.Remainder, base, rem)
	}
	sum := 0
	for i := 0; i < plan.SlotCount; i++ {
		size := plan.SizeForSlot(i)
		want := base
		if i < rem {
			want++
		}
		if size != want {
			t.Fatalf("slot %d size=%d want %d", i, size, want)
		}
		if size < AdvancersPerMatch || size > PlayersPerRoom {
			t.Fatalf("slot %d size %d out of bounds", i, size)
		}
		sum += size
	}
	if sum != n {
		t.Fatalf("sum=%d want %d", sum, n)
	}
}

func TestComputeRound1SlotPlan_RejectsEmpty(t *testing.T) {
	if _, err := ComputeRound1SlotPlan(0); err == nil {
		t.Fatal("expected error for n=0")
	}
}

func TestPlayerIDLexOrder_P1P10P2Trap(t *testing.T) {
	// Lexicographic ASC (not registration order, not natural numeric).
	got := SortPlayerIDsAsc([]PlayerID{"p2", "p10", "p1"})
	want := []PlayerID{"p1", "p10", "p2"}
	if len(got) != len(want) {
		t.Fatalf("len=%d", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestDecideSeedRound1Kickoff(t *testing.T) {
	cmd := SeedRoundCommand{CommandID: "seed-1", RoundNumber: 1}

	t.Run("reject wrong phase", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{Exists: true, Phase: PhaseRegistration, RegisteredCount: 1}, cmd)
		if d.Kind != SeedKickoffReject || d.Outcome.Rejection.Code != RejectWrongPhase {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("reject no players", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{Exists: true, Phase: PhaseSeeding, RegisteredCount: 0}, cmd)
		if d.Kind != SeedKickoffReject || d.Outcome.Rejection.Code != RejectInvalidCommand {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("reject terminal", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{Exists: true, Phase: PhaseCancelled, RegisteredCount: 1}, cmd)
		if d.Kind != SeedKickoffReject || d.Outcome.Rejection.Code != RejectAlreadyTerminal {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("schedule", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{Exists: true, Phase: PhaseSeeding, RegisteredCount: 12}, cmd)
		if d.Kind != SeedKickoffSchedule || d.Plan.SlotCount != 2 || len(d.Outcome.Facts) != 0 {
			t.Fatalf("%+v", d)
		}
		if d.Source != SeedingSourceRegistrations || d.SourceRoundNumber != 0 {
			t.Fatalf("source=%s srcRound=%d", d.Source, d.SourceRoundNumber)
		}
	})
	t.Run("already seeded noop", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{
			Exists: true, Phase: PhaseInProgress, RegisteredCount: 12,
			Round1Status: RoundSeeded,
		}, cmd)
		if d.Kind != SeedKickoffAlreadyDone || len(d.Outcome.Facts) != 0 {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("job active different command noop", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{
			Exists: true, Phase: PhaseSeeding, RegisteredCount: 12,
			JobStatus: SeedingJobPending, JobCommandID: "other",
		}, cmd)
		if d.Kind != SeedKickoffJobExistsNoop || len(d.Outcome.Facts) != 0 {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("job quarantined reject", func(t *testing.T) {
		d := DecideSeedRound1Kickoff(SeedRound1KickoffContext{
			Exists: true, Phase: PhaseSeeding, RegisteredCount: 12,
			JobStatus: SeedingJobQuarantined, JobCommandID: "other",
		}, cmd)
		if d.Kind != SeedKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
}

func TestDecideSeedRoundKickoff_Round2(t *testing.T) {
	cmd := SeedRoundCommand{CommandID: "seed:t1:r2", RoundNumber: 2}
	ready := SeedRoundKickoffContext{
		TournamentID: "t1", Exists: true, Phase: PhaseInProgress, RoundNumber: 2,
		SourcePlayerCount: 12, PreviousRoundFound: true, PreviousRoundStatus: RoundCompleted,
	}

	t.Run("schedule from advancement", func(t *testing.T) {
		d := DecideSeedRoundKickoff(ready, cmd)
		if d.Kind != SeedKickoffSchedule || d.Plan.SlotCount != 2 || d.Source != SeedingSourceAdvancement {
			t.Fatalf("%+v", d)
		}
		if d.SourceRoundNumber != 1 || d.Plan.PlayerCount != 12 {
			t.Fatalf("plan=%+v srcRound=%d", d.Plan, d.SourceRoundNumber)
		}
		if d.Plan.IsFinal {
			t.Fatal("12 players is not final")
		}
	})
	t.Run("final when <=10 advancers", func(t *testing.T) {
		ctx := ready
		ctx.SourcePlayerCount = 8
		d := DecideSeedRoundKickoff(ctx, cmd)
		if d.Kind != SeedKickoffSchedule || !d.Plan.IsFinal || d.Plan.SlotCount != 1 {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("reject wrong phase", func(t *testing.T) {
		ctx := ready
		ctx.Phase = PhaseSeeding
		d := DecideSeedRoundKickoff(ctx, cmd)
		if d.Kind != SeedKickoffReject || d.Outcome.Rejection.Code != RejectWrongPhase {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("reject previous not completed", func(t *testing.T) {
		ctx := ready
		ctx.PreviousRoundStatus = RoundInProgress
		d := DecideSeedRoundKickoff(ctx, cmd)
		if d.Kind != SeedKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("reject empty source", func(t *testing.T) {
		ctx := ready
		ctx.SourcePlayerCount = 0
		d := DecideSeedRoundKickoff(ctx, cmd)
		if d.Kind != SeedKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("job terminal reject", func(t *testing.T) {
		ctx := ready
		ctx.JobStatus = SeedingJobCancelled
		d := DecideSeedRoundKickoff(ctx, cmd)
		if d.Kind != SeedKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
}

func TestSeedRoundCommandID(t *testing.T) {
	if got := SeedRoundCommandID("tid", 2); got != "seed:tid:r2" {
		t.Fatalf("got %q", got)
	}
}

func TestComputeRoundSlotPlan_Alias(t *testing.T) {
	a, err1 := ComputeRoundSlotPlan(23)
	b, err2 := ComputeRound1SlotPlan(23)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if a.SlotCount != b.SlotCount || a.BaseSize != b.BaseSize || a.Remainder != b.Remainder {
		t.Fatalf("alias mismatch %+v vs %+v", a, b)
	}
}
