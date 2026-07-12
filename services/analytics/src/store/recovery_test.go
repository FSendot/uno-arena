package store

import (
	"errors"
	"testing"
	"time"
)

func TestRecoveryGenerationIDDeterministic(t *testing.T) {
	a := RecoveryGenerationID("job-xyz")
	b := RecoveryGenerationID("job-xyz")
	if a != b || !stringsHasPrefix(a, "gen_rcv_") {
		t.Fatalf("%q %q", a, b)
	}
	if RecoveryGenerationID("other") == a {
		t.Fatal("expected distinct ids")
	}
}

func TestSelectLeaseWinner_HighestEpochThenOwnerToken(t *testing.T) {
	job := "job-lease"
	a := RecoveryLease{RecoveryJobID: job, OwnerToken: "own_bbb", LeaseEpoch: 3}
	b := RecoveryLease{RecoveryJobID: job, OwnerToken: "own_aaa", LeaseEpoch: 3}
	c := RecoveryLease{RecoveryJobID: job, OwnerToken: "own_zzz", LeaseEpoch: 2}
	got, ok := SelectLeaseWinner([]RecoveryLease{a, b, c})
	if !ok {
		t.Fatal("expected winner")
	}
	// Equal epoch → lexicographically smallest owner_token wins.
	if got.OwnerToken != "own_aaa" || got.LeaseEpoch != 3 {
		t.Fatalf("got=%+v", got)
	}
	// Higher epoch always beats lower regardless of token.
	hi := RecoveryLease{RecoveryJobID: job, OwnerToken: "own_zzz", LeaseEpoch: 4}
	got, ok = SelectLeaseWinner([]RecoveryLease{a, b, hi})
	if !ok || got.OwnerToken != "own_zzz" || got.LeaseEpoch != 4 {
		t.Fatalf("got=%+v", got)
	}
	if _, ok := SelectLeaseWinner(nil); ok {
		t.Fatal("empty must miss")
	}
}

func TestNextLeaseEpoch_AlwaysAdvancesOnRenewAndTakeover(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	ep, err := NextLeaseEpoch(RecoveryLease{}, false, "own_a", now)
	if err != nil || ep != 1 {
		t.Fatalf("fresh ep=%d err=%v", ep, err)
	}
	cur := RecoveryLease{OwnerToken: "own_a", LeaseEpoch: 5, ExpiresAt: now.Add(time.Minute)}
	ep, err = NextLeaseEpoch(cur, true, "own_a", now)
	if err != nil || ep != 6 {
		t.Fatalf("renew must advance ep=%d err=%v", ep, err)
	}
	ep, err = NextLeaseEpoch(RecoveryLease{OwnerToken: "own_b", LeaseEpoch: 5, ExpiresAt: now.Add(-time.Second)}, true, "own_a", now)
	if err != nil || ep != 6 {
		t.Fatalf("takeover ep=%d err=%v", ep, err)
	}
	_, err = NextLeaseEpoch(RecoveryLease{OwnerToken: "own_b", LeaseEpoch: 5, ExpiresAt: now.Add(time.Minute)}, true, "own_a", now)
	if !errors.Is(err, ErrRecoveryLeaseLost) {
		t.Fatalf("held by other: %v", err)
	}
}

func TestReconcileJobProgressFromCheckpoint_IncrementsOnce(t *testing.T) {
	job := RecoveryJobState{RecoveryJobID: "j", PagesCompleted: 0, AcceptedCount: 0, Status: RecoveryStatusBuilding}
	cp := RecoveryPageCheckpoint{PageIndex: 0, PageCursor: "", NextPageCursor: "c1", RecordsApplied: 3}
	out, wrote := ReconcileJobProgressFromCheckpoint(job, cp)
	if !wrote || out.PagesCompleted != 1 || out.AcceptedCount != 3 || out.NextPageCursor != "c1" {
		t.Fatalf("first reconcile: %+v wrote=%v", out, wrote)
	}
	out2, wrote2 := ReconcileJobProgressFromCheckpoint(out, cp)
	if wrote2 || out2.PagesCompleted != 1 || out2.AcceptedCount != 3 {
		t.Fatalf("idempotent: %+v wrote=%v", out2, wrote2)
	}
	cp1 := RecoveryPageCheckpoint{PageIndex: 1, PageCursor: "c1", NextPageCursor: "", RecordsApplied: 2}
	out3, wrote3 := ReconcileJobProgressFromCheckpoint(out2, cp1)
	if !wrote3 || out3.PagesCompleted != 2 || out3.AcceptedCount != 5 {
		t.Fatalf("second page: %+v wrote=%v", out3, wrote3)
	}
}

func TestValidateImmutableRecoveryJobSpec(t *testing.T) {
	existing := RecoveryJobState{
		RecoveryJobID: "j", SourceContext: "room", SourceTopic: "room.gameplay.metrics",
		FromCheckpoint: "1", ToCheckpoint: "9", HasCheckpointRng: true,
		Status: RecoveryStatusBuilding,
	}
	spec := RecoveryJobSpec{
		RecoveryJobID: "j", SourceContext: "room", SourceTopic: "room.gameplay.metrics",
		FromCheckpoint: "1", ToCheckpoint: "9", HasCheckpointRange: true,
	}
	if err := ValidateImmutableRecoveryJobSpec(existing, spec); err != nil {
		t.Fatal(err)
	}
	badTopic := spec
	badTopic.SourceTopic = "room.match.completed"
	if err := ValidateImmutableRecoveryJobSpec(existing, badTopic); !errors.Is(err, ErrRecoveryJobSpecMismatch) {
		t.Fatalf("topic poison: %v", err)
	}
	badRange := spec
	badRange.ToCheckpoint = "99"
	if err := ValidateImmutableRecoveryJobSpec(existing, badRange); !errors.Is(err, ErrRecoveryJobSpecMismatch) {
		t.Fatalf("range poison: %v", err)
	}
	if err := AssertRecoveryJobAcceptsPage(RecoveryStatusComplete, false); !errors.Is(err, ErrRecoveryJobClosed) {
		t.Fatalf("complete unseen page: %v", err)
	}
	if err := AssertRecoveryJobAcceptsPage(RecoveryStatusComplete, true); err != nil {
		t.Fatalf("complete with checkpoint should allow follow-through: %v", err)
	}
	if err := AssertRecoveryJobAcceptsPage(RecoveryStatusFailed, true); !errors.Is(err, ErrRecoveryJobClosed) {
		t.Fatalf("failed must stay closed: %v", err)
	}
}

func TestVerifyPageContinuity_GapsAndFinal(t *testing.T) {
	ok := []RecoveryPageCheckpoint{
		{PageIndex: 0, PageCursor: "", NextPageCursor: "c1", Status: PageStatusApplied},
		{PageIndex: 1, PageCursor: "c1", NextPageCursor: "c2", Status: PageStatusApplied},
		{PageIndex: 2, PageCursor: "c2", NextPageCursor: "", Status: PageStatusFinalized},
	}
	if err := verifyPageContinuity(ok); err != nil {
		t.Fatal(err)
	}
	gap := []RecoveryPageCheckpoint{
		{PageIndex: 0, PageCursor: "", NextPageCursor: "c1", Status: PageStatusApplied},
		{PageIndex: 2, PageCursor: "c1", NextPageCursor: "", Status: PageStatusApplied},
	}
	if err := verifyPageContinuity(gap); err == nil {
		t.Fatal("expected gap")
	}
	open := []RecoveryPageCheckpoint{
		{PageIndex: 0, PageCursor: "", NextPageCursor: "still", Status: PageStatusApplied},
	}
	if err := verifyPageContinuity(open); err == nil {
		t.Fatal("expected open final page error")
	}
}

func TestVerifyPageCheckpointsMatchJob(t *testing.T) {
	job := RecoveryJobState{RecoveryJobID: "j", SourceTopic: "room.gameplay.metrics", GenerationID: "gen_a", PagesCompleted: 1}
	pages := []RecoveryPageCheckpoint{{
		RecoveryJobID: "j", SourceTopic: "room.gameplay.metrics", GenerationID: "gen_a",
		PageIndex: 0, PageCursor: "", NextPageCursor: "", Status: PageStatusApplied,
	}}
	if err := verifyPageCheckpointsMatchJob(job, pages); err != nil {
		t.Fatal(err)
	}
	pages[0].GenerationID = "gen_other"
	if err := verifyPageCheckpointsMatchJob(job, pages); err == nil {
		t.Fatal("expected generation mismatch")
	}
}

func stringsHasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
