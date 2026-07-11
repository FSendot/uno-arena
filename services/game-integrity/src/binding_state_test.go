package main

import (
	"testing"

	"unoarena/services/game-integrity/domain"
)

func TestFoldBindingEvents_EmptyIsClaimable(t *testing.T) {
	st, err := foldBindingRecords(domain.GameID("g1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !st.IsEmptyOrReleased() {
		t.Fatalf("empty must be claimable, got %+v", st)
	}
	if room, ok := st.BoundRoom(); ok || room != "" {
		t.Fatalf("empty must not bind a room: room=%q ok=%v", room, ok)
	}
}

func TestFoldBindingEvents_ReleasedLatestIsClaimable(t *testing.T) {
	recs := []gameBindingRecord{
		{RoomID: "room-a", GameID: "g1", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a"},
		{RoomID: "room-a", GameID: "g1", Phase: gameBindPhaseReleased, OwnerToken: "tok-a"},
	}
	st, err := foldBindingRecords(domain.GameID("g1"), recs)
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != gameBindPhaseReleased || !st.IsEmptyOrReleased() {
		t.Fatalf("released latest must be claimable, got %+v", st)
	}
	if _, ok := st.BoundRoom(); ok {
		t.Fatal("released must not expose a bound room")
	}
}

func TestEvaluateClaim_EmptyAllowsClaimAppend(t *testing.T) {
	st := bindingState{}
	got := evaluateClaim(st, "room-a", "tok-1")
	if got != claimAppend {
		t.Fatalf("want claimAppend, got %v", got)
	}
}

func TestEvaluateClaim_ReleasedAllowsClaimAppend(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseReleased, OwnerToken: "old", EventCount: 2}
	got := evaluateClaim(st, "room-b", "tok-b")
	if got != claimAppend {
		t.Fatalf("released may be claimed by any room, got %v", got)
	}
}

func TestEvaluateClaim_ForeignCannotStealInFlightClaim(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a", EventCount: 1}
	got := evaluateClaim(st, "room-b", "tok-b")
	if got != claimConflict {
		t.Fatalf("foreign must not steal in-flight claim, got %v", got)
	}
}

func TestEvaluateClaim_SameRoomWithoutTokenCannotSteal(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a", EventCount: 1}
	got := evaluateClaim(st, "room-a", "tok-other")
	if got != claimConflict {
		t.Fatalf("same-room without owning token must not steal, got %v", got)
	}
}

func TestEvaluateClaim_OwnerTokenRetainsClaim(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a", EventCount: 1}
	got := evaluateClaim(st, "room-a", "tok-a")
	if got != claimOwned {
		t.Fatalf("exact owner must retain claim, got %v", got)
	}
}

func TestEvaluateClaim_ActiveMatchingOK_ActiveForeignConflict(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseActive, EventCount: 2}
	if evaluateClaim(st, "room-a", "tok") != claimActiveOK {
		t.Fatal("active matching room must be ok")
	}
	if evaluateClaim(st, "room-b", "tok") != claimConflict {
		t.Fatal("active foreign room must conflict")
	}
}

func TestEvaluateClaim_ActiveIsImmutable(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseActive, EventCount: 2}
	if evaluateRelease(st, "any") != releaseNoop {
		t.Fatal("active must not release")
	}
	if evaluateFinalize(st, "room-b", "tok") != finalizeConflict {
		t.Fatal("active foreign finalize must conflict")
	}
}

func TestEvaluateFinalize_RequiresExactOwnerToken(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a", EventCount: 1}
	if evaluateFinalize(st, "room-a", "tok-a") != finalizeAppend {
		t.Fatal("owner must finalize")
	}
	if evaluateFinalize(st, "room-a", "stale") != finalizeConflict {
		t.Fatal("stale owner must not finalize")
	}
	if evaluateFinalize(st, "room-b", "tok-a") != finalizeConflict {
		t.Fatal("foreign room must not finalize")
	}
}

func TestEvaluateFinalizeRepair_ClaimedMatchingRoomWithoutToken(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a", EventCount: 1}
	if evaluateFinalizeRepair(st, "room-a") != finalizeAppend {
		t.Fatal("existing deck repair may finalize claimed matching room")
	}
	if evaluateFinalizeRepair(st, "room-b") != finalizeConflict {
		t.Fatal("conflicting deck/binding must fail closed")
	}
}

func TestEvaluateRelease_OnlyExactOwnerToken(t *testing.T) {
	st := bindingState{RoomID: "room-a", Phase: gameBindPhaseClaimed, OwnerToken: "tok-a", EventCount: 1}
	if evaluateRelease(st, "tok-a") != releaseAppend {
		t.Fatal("owner must release")
	}
	if evaluateRelease(st, "stale") != releaseNoop {
		t.Fatal("stale owner must not release")
	}
}

func TestEvaluateAppendRecovery_KnownFailureReleases(t *testing.T) {
	if evaluateAppendRecovery(true, false) != recoveryRelease {
		t.Fatal("known failure with no deck must release")
	}
	if evaluateAppendRecovery(true, true) != recoveryRelease {
		t.Fatal("known failure must release without treating deck absence as steal auth")
	}
}

func TestEvaluateAppendRecovery_UncertainUsesDeckExistence(t *testing.T) {
	if evaluateAppendRecovery(false, true) != recoveryFinalize {
		t.Fatal("uncertain + deck exists must finalize/repair")
	}
	if evaluateAppendRecovery(false, false) != recoveryRelease {
		t.Fatal("uncertain + no deck must release")
	}
}

func TestFoldBindingEvents_LegacyEmptyPhaseIsActive(t *testing.T) {
	recs := []gameBindingRecord{{RoomID: "room-a", GameID: "g1"}}
	st, err := foldBindingRecords(domain.GameID("g1"), recs)
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != gameBindPhaseActive {
		t.Fatalf("legacy empty phase must be active, got %+v", st)
	}
}
