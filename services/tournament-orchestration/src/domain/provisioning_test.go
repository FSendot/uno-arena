package domain

import (
	"strconv"
	"testing"
)

func TestCompleteBatch_PublicBracketVisibleOnlyOnRoundTransition(t *testing.T) {
	tr := mustCreate(t, "t-vis", 25)
	for i := 1; i <= 25; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	r, _ := tr.Round(1)
	if len(r.Batches) < 2 {
		t.Fatalf("need multi-batch round, got %d", len(r.Batches))
	}

	out := tr.CompleteTournamentProvisioningBatch(CompleteTournamentProvisioningBatchCommand{
		CommandID: "vis-b0", RoundNumber: 1, BatchID: r.Batches[0].BatchID,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchCompleted) {
		t.Fatalf("non-last complete: %+v", out)
	}
	if factData(out.Facts, FactTournamentProvisioningBatchCompleted, FactDataPublicBracketVisible) == "true" {
		t.Fatal("non-last batch completion must not mark publicBracketVisible")
	}
	r, _ = tr.Round(1)
	if r.Status != RoundProvisioning {
		t.Fatalf("round status=%s want provisioning after non-last complete", r.Status)
	}

	out = tr.CompleteTournamentProvisioningBatch(CompleteTournamentProvisioningBatchCommand{
		CommandID: "vis-b1", RoundNumber: 1, BatchID: r.Batches[1].BatchID,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchCompleted) {
		t.Fatalf("last complete: %+v", out)
	}
	if factData(out.Facts, FactTournamentProvisioningBatchCompleted, FactDataPublicBracketVisible) != "true" {
		t.Fatal("last batch completion must mark publicBracketVisible")
	}
	r, _ = tr.Round(1)
	if r.Status != RoundInProgress {
		t.Fatalf("round status=%s want in_progress", r.Status)
	}
}

func TestProvisioningBatches_Deterministic(t *testing.T) {
	tr := mustCreate(t, "t-prov", 25)
	for i := 1; i <= 25; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	r, _ := tr.Round(1)
	if len(r.Slots) != 3 {
		t.Fatalf("slots=%d", len(r.Slots))
	}
	if len(r.Batches) != 2 {
		t.Fatalf("batches=%d want 2", len(r.Batches))
	}
	if r.Batches[0].BatchID != "batch_0" || r.Batches[1].BatchID != "batch_1" {
		t.Fatalf("batch ids: %v %v", r.Batches[0].BatchID, r.Batches[1].BatchID)
	}
	for i, slot := range r.Slots {
		wantRoom := roomIDForSlot(tr.ID(), 1, slot.SlotID)
		if slot.RoomID != wantRoom {
			t.Fatalf("slot %d room=%s want %s", i, slot.RoomID, wantRoom)
		}
		if slot.Status != SlotAssigned {
			t.Fatalf("slot %d status=%s", i, slot.Status)
		}
	}
	for _, b := range r.Batches {
		if b.Status != BatchPending {
			t.Fatalf("batch %s status=%s want pending", b.BatchID, b.Status)
		}
	}
	if r.Status != RoundProvisioning {
		t.Fatalf("round status=%s want provisioning", r.Status)
	}
	out := tr.CompleteTournamentProvisioningBatch(CompleteTournamentProvisioningBatchCommand{
		CommandID: "complete-b0", RoundNumber: 1, BatchID: r.Batches[0].BatchID,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchCompleted) {
		t.Fatalf("complete batch0: %+v", out)
	}
	out = tr.CompleteTournamentProvisioningBatch(CompleteTournamentProvisioningBatchCommand{
		CommandID: "complete-b1", RoundNumber: 1, BatchID: r.Batches[1].BatchID,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchCompleted) {
		t.Fatalf("complete batch1: %+v", out)
	}
	r, _ = tr.Round(1)
	if r.Status != RoundInProgress {
		t.Fatalf("round status=%s want in_progress", r.Status)
	}
	// Duplicate complete of an already-completed batch is factless (version-stable).
	out = tr.CompleteTournamentProvisioningBatch(CompleteTournamentProvisioningBatchCommand{
		CommandID: "complete-b0-again", RoundNumber: 1, BatchID: r.Batches[0].BatchID,
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("duplicate complete must be factless: %+v", out)
	}
	out = tr.ProvisionRoundMatches(ProvisionRoundMatchesCommand{CommandID: "prov-again", RoundNumber: 1})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("reprovision: %+v", out)
	}
}

func TestProvisionRoundMatches_RejectsPendingRound(t *testing.T) {
	tr := mustCreate(t, "t-pending-prov", 10)
	for i := 1; i <= 3; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	if out := tr.CloseRegistration(CloseRegistrationCommand{CommandID: "close-1"}); out.Kind != OutcomeAccepted {
		t.Fatalf("close: %+v", out)
	}
	// Simulate durable partial seed: pending round with no slots provisioned yet.
	tr.rounds[1] = &Round{
		Number: 1,
		Status: RoundPending,
		Slots:  nil,
	}
	out := tr.ProvisionRoundMatches(ProvisionRoundMatchesCommand{CommandID: "prov-pending", RoundNumber: 1})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectRoundNotReady {
		t.Fatalf("pending round must reject provision: %+v", out)
	}
}

func TestProvisioningRetry_ThenQuarantineOnBudget(t *testing.T) {
	tr := mustCreate(t, "t-retry", 20)
	for i := 1; i <= 20; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	r, _ := tr.Round(1)
	batchID := r.Batches[0].BatchID

	out := tr.RetryTournamentProvisioningBatch(RetryTournamentProvisioningBatchCommand{
		CommandID: "retry-1", RoundNumber: 1, BatchID: batchID, RetryAttempt: 1,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchRetried) {
		t.Fatalf("retry1: %+v", out)
	}
	out = tr.RetryTournamentProvisioningBatch(RetryTournamentProvisioningBatchCommand{
		CommandID: "retry-1b", RoundNumber: 1, BatchID: batchID, RetryAttempt: 1,
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("retry idempotent: %+v", out)
	}
	out = tr.RetryTournamentProvisioningBatch(RetryTournamentProvisioningBatchCommand{
		CommandID: "retry-skip", RoundNumber: 1, BatchID: batchID, RetryAttempt: 3,
	})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectInvalidCommand {
		t.Fatalf("skip attempt must reject: %+v", out)
	}
	out = tr.RetryTournamentProvisioningBatch(RetryTournamentProvisioningBatchCommand{
		CommandID: "retry-2", RoundNumber: 1, BatchID: batchID, RetryAttempt: 2,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchRetried) {
		t.Fatalf("retry2: %+v", out)
	}
	out = tr.RetryTournamentProvisioningBatch(RetryTournamentProvisioningBatchCommand{
		CommandID: "retry-3", RoundNumber: 1, BatchID: batchID, RetryAttempt: 3,
	})
	if out.Kind != OutcomeAccepted || !hasFact(out.Facts, FactTournamentProvisioningBatchQuarantined) {
		t.Fatalf("quarantine: %+v", out)
	}
	r, _ = tr.Round(1)
	if r.Status != RoundBlocked || r.Batches[0].Status != BatchQuarantined {
		t.Fatalf("blocked=%s batch=%s", r.Status, r.Batches[0].Status)
	}
	out = tr.CompleteRound(CompleteRoundCommand{CommandID: "cr", RoundNumber: 1})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectQuarantined {
		t.Fatalf("complete blocked: %+v", out)
	}
}

func TestQuarantineBatch_ConflictingAssignmentPath(t *testing.T) {
	tr := mustCreate(t, "t-qbatch", 10)
	for i := 1; i <= 10; i++ {
		mustRegister(t, tr, PlayerID("p"+strconv.Itoa(i)))
	}
	mustCloseAndSeed(t, tr, 1)
	mustProvision(t, tr, 1)
	r, _ := tr.Round(1)
	out := tr.QuarantineTournamentProvisioningBatch(QuarantineTournamentProvisioningBatchCommand{
		CommandID: "qb", RoundNumber: 1, BatchID: r.Batches[0].BatchID, Reason: "conflicting assignment",
	})
	if !hasFact(out.Facts, FactTournamentProvisioningBatchQuarantined) {
		t.Fatalf("%+v", out)
	}
	out = tr.QuarantineTournamentProvisioningBatch(QuarantineTournamentProvisioningBatchCommand{
		CommandID: "qb2", RoundNumber: 1, BatchID: r.Batches[0].BatchID, Reason: "again",
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 {
		t.Fatalf("idempotent quarantine: %+v", out)
	}
}

func TestBuildSlotsAndBatches_DeterministicHelpers(t *testing.T) {
	players := make([]PlayerID, 0, 23)
	for i := 1; i <= 23; i++ {
		players = append(players, PlayerID("p"+strconv.Itoa(i)))
	}
	slots := buildSlots(players)
	if len(slots) != 3 {
		t.Fatalf("slots=%d", len(slots))
	}
	if len(slots[0].SeededPlayers) != 8 || len(slots[1].SeededPlayers) != 8 || len(slots[2].SeededPlayers) != 7 {
		t.Fatalf("sizes %d %d %d", len(slots[0].SeededPlayers), len(slots[1].SeededPlayers), len(slots[2].SeededPlayers))
	}
	batches := buildBatches(len(slots), 2)
	if len(batches) != 2 || batches[0].SlotFrom != "slot_0" || batches[0].SlotTo != "slot_1" {
		t.Fatalf("%+v", batches)
	}
}
