package main

import (
	"fmt"

	"unoarena/services/game-integrity/domain"
)

const gameBindPhaseReleased = "released"

// bindingState is the folded latest gameId→room binding view.
type bindingState struct {
	RoomID     string
	Phase      string
	OwnerToken string
	EventCount int
}

func (st bindingState) IsEmptyOrReleased() bool {
	return st.EventCount == 0 || st.Phase == gameBindPhaseReleased || st.Phase == ""
}

// BoundRoom reports the room for claimed/active bindings (not released/empty).
func (st bindingState) BoundRoom() (string, bool) {
	if st.Phase == gameBindPhaseClaimed || st.Phase == gameBindPhaseActive {
		if st.RoomID == "" {
			return "", false
		}
		return st.RoomID, true
	}
	return "", false
}

type claimOutcome int

const (
	claimAppend claimOutcome = iota
	claimOwned
	claimActiveOK
	claimConflict
)

type finalizeOutcome int

const (
	finalizeAppend finalizeOutcome = iota
	finalizeAlreadyActive
	finalizeConflict
	finalizeNotClaimed
)

type releaseOutcome int

const (
	releaseAppend releaseOutcome = iota
	releaseNoop
)

type appendRecoveryAction int

const (
	recoveryRelease appendRecoveryAction = iota
	recoveryFinalize
)

func foldBindingRecords(gameID domain.GameID, records []gameBindingRecord) (bindingState, error) {
	var st bindingState
	activeLocked := false
	lockedRoom := ""
	for i, body := range records {
		if body.GameID != string(gameID) {
			return bindingState{}, fmt.Errorf("corrupt game binding")
		}
		if body.RoomID == "" {
			return bindingState{}, fmt.Errorf("corrupt game binding")
		}
		p := bindingPhase(body)
		if activeLocked {
			if body.RoomID != lockedRoom {
				return bindingState{}, fmt.Errorf("conflicting gameId binding")
			}
			st.EventCount = i + 1
			continue
		}
		st = bindingState{
			RoomID:     body.RoomID,
			Phase:      p,
			OwnerToken: body.OwnerToken,
			EventCount: i + 1,
		}
		if p == gameBindPhaseActive {
			activeLocked = true
			lockedRoom = body.RoomID
		}
		if p == gameBindPhaseReleased {
			// Released clears ownership; token retained only for audit of who released.
			st.OwnerToken = body.OwnerToken
		}
	}
	return st, nil
}

func evaluateClaim(st bindingState, roomID, ownerToken string) claimOutcome {
	if st.IsEmptyOrReleased() {
		return claimAppend
	}
	switch st.Phase {
	case gameBindPhaseActive:
		if st.RoomID == roomID {
			return claimActiveOK
		}
		return claimConflict
	case gameBindPhaseClaimed:
		if st.RoomID == roomID && st.OwnerToken != "" && st.OwnerToken == ownerToken {
			return claimOwned
		}
		return claimConflict
	default:
		return claimConflict
	}
}

func evaluateFinalize(st bindingState, roomID, ownerToken string) finalizeOutcome {
	switch st.Phase {
	case gameBindPhaseActive:
		if st.RoomID == roomID {
			return finalizeAlreadyActive
		}
		return finalizeConflict
	case gameBindPhaseClaimed:
		if st.RoomID == roomID && st.OwnerToken != "" && st.OwnerToken == ownerToken {
			return finalizeAppend
		}
		return finalizeConflict
	default:
		if st.IsEmptyOrReleased() {
			return finalizeNotClaimed
		}
		return finalizeConflict
	}
}

// evaluateFinalizeRepair allows an existing matching-room deck to finalize a claimed binding
// without the original in-memory owner token.
func evaluateFinalizeRepair(st bindingState, roomID string) finalizeOutcome {
	switch st.Phase {
	case gameBindPhaseActive:
		if st.RoomID == roomID {
			return finalizeAlreadyActive
		}
		return finalizeConflict
	case gameBindPhaseClaimed:
		if st.RoomID == roomID {
			return finalizeAppend
		}
		return finalizeConflict
	default:
		if st.IsEmptyOrReleased() {
			return finalizeNotClaimed
		}
		return finalizeConflict
	}
}

func evaluateRelease(st bindingState, ownerToken string) releaseOutcome {
	if st.Phase == gameBindPhaseClaimed && st.OwnerToken != "" && st.OwnerToken == ownerToken {
		return releaseAppend
	}
	return releaseNoop
}

// evaluateAppendRecovery chooses release vs finalize after a first-deck append error.
// knownFailure always releases (caller still checks owner token). Uncertain uses deck existence.
func evaluateAppendRecovery(knownFailure, deckExists bool) appendRecoveryAction {
	if !knownFailure && deckExists {
		return recoveryFinalize
	}
	return recoveryRelease
}
