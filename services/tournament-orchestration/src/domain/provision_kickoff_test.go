package domain

import "testing"

func TestComputeProvisionBatchPlan_Formula(t *testing.T) {
	cases := []struct {
		slots, batchSize, wantBatches int
		wantFromTo                    [][2]int // first and last batch ranges when relevant
	}{
		{1, 100, 1, [][2]int{{0, 0}}},
		{100, 100, 1, [][2]int{{0, 99}}},
		{1000, 1000, 1, [][2]int{{0, 999}}},
		{1001, 1000, 2, [][2]int{{0, 999}, {1000, 1000}}},
		{100000, 1000, 100, [][2]int{{0, 999}, {99000, 99999}}},
		{25, 10, 3, [][2]int{{0, 9}, {20, 24}}},
		{3, DefaultBatchSize, 1, [][2]int{{0, 2}}},
	}
	for _, tc := range cases {
		plan, err := ComputeProvisionBatchPlan(tc.slots, tc.batchSize)
		if err != nil {
			t.Fatalf("slots=%d batch=%d: %v", tc.slots, tc.batchSize, err)
		}
		if plan.BatchCount != tc.wantBatches {
			t.Fatalf("slots=%d batch=%d: batches=%d want %d", tc.slots, tc.batchSize, plan.BatchCount, tc.wantBatches)
		}
		if plan.BatchCount > 0 && len(tc.wantFromTo) > 0 {
			f0, t0 := plan.BatchRange(0)
			if f0 != tc.wantFromTo[0][0] || t0 != tc.wantFromTo[0][1] {
				t.Fatalf("first range=%d..%d want %v", f0, t0, tc.wantFromTo[0])
			}
			last := tc.wantFromTo[len(tc.wantFromTo)-1]
			fl, tl := plan.BatchRange(plan.BatchCount - 1)
			if fl != last[0] || tl != last[1] {
				t.Fatalf("last range=%d..%d want %v", fl, tl, last)
			}
		}
		// Identities for each batch.
		for i := 0; i < plan.BatchCount; i++ {
			from, to := plan.BatchRange(i)
			if BatchIDForIndex(i) != batchIDForIndex(i) {
				t.Fatal("batch id helper mismatch")
			}
			if SlotIDForIndex(from) != slotIDForIndex(from) || SlotIDForIndex(to) != slotIDForIndex(to) {
				t.Fatal("slot id helper mismatch")
			}
			if from > to || from < 0 || to >= plan.SlotCount {
				t.Fatalf("bad range batch %d: %d..%d", i, from, to)
			}
		}
	}
}

func TestComputeProvisionBatchPlan_Rejects(t *testing.T) {
	if _, err := ComputeProvisionBatchPlan(0, 100); err == nil {
		t.Fatal("slotCount 0 must fail")
	}
	if _, err := ComputeProvisionBatchPlan(1001, MaxProvisioningBatchSize+1); err == nil {
		t.Fatal("batchSize > max must fail")
	}
}

func TestDecideProvisionKickoff(t *testing.T) {
	cmd := ProvisionRoundMatchesCommand{CommandID: "prov-1", RoundNumber: 1}
	base := ProvisionKickoffContext{
		Exists: true, Phase: PhaseInProgress, RoundNumber: 1,
		RoundStatus: RoundSeeded, SlotCount: 25, SlotsContiguous: true,
		BatchSize: 10, RetryBudget: 3,
	}

	t.Run("schedule", func(t *testing.T) {
		d := DecideProvisionKickoff(base, cmd)
		if d.Kind != ProvisionKickoffSchedule || d.Plan.BatchCount != 3 || len(d.Outcome.Facts) != 0 {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("pending rejects", func(t *testing.T) {
		ctx := base
		ctx.RoundStatus = RoundPending
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject || d.Outcome.Rejection.Code != RejectRoundNotReady {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("noncontiguous rejects", func(t *testing.T) {
		ctx := base
		ctx.SlotsContiguous = false
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("zero slots rejects", func(t *testing.T) {
		ctx := base
		ctx.SlotCount = 0
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("terminal rejects", func(t *testing.T) {
		ctx := base
		ctx.Phase = PhaseCancelled
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject || d.Outcome.Rejection.Code != RejectAlreadyTerminal {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("already provisioning matching plan", func(t *testing.T) {
		plan, _ := ComputeProvisionBatchPlan(25, 10)
		ctx := base
		ctx.RoundStatus = RoundProvisioning
		ctx.ExistingBatchesFilled = true
		ctx.ExistingBatchPlanFingerprint = plan.Fingerprint()
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffAlreadyDone || len(d.Outcome.Facts) != 0 {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("already provisioning plan drift", func(t *testing.T) {
		ctx := base
		ctx.RoundStatus = RoundProvisioning
		ctx.ExistingBatchesFilled = true
		ctx.ExistingBatchPlanFingerprint = "slots=25;batchSize=5;batches=5"
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("batch size over max", func(t *testing.T) {
		ctx := base
		ctx.BatchSize = MaxProvisioningBatchSize + 1
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("missing round", func(t *testing.T) {
		ctx := base
		ctx.RoundStatus = ""
		d := DecideProvisionKickoff(ctx, cmd)
		if d.Kind != ProvisionKickoffReject || d.Outcome.Rejection.Code != RejectRoundNotFound {
			t.Fatalf("%+v", d)
		}
	})
}

func TestMatchAssignedEventID_Deterministic(t *testing.T) {
	a := MatchAssignedEventID("tid", 1, "slot_0")
	b := MatchAssignedEventID("tid", 1, "slot_0")
	if a != b || a == "" {
		t.Fatalf("%q vs %q", a, b)
	}
	if RoomIDForSlot("tid", 1, "slot_0") != roomIDForSlot("tid", 1, "slot_0") {
		t.Fatal("room id mismatch")
	}
}
