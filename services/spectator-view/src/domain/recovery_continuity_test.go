package domain

import (
	"errors"
	"testing"
)

func publicSnapshotPayload() map[string]any {
	return map[string]any{
		"status":     "waiting",
		"visibility": "public",
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0, "occupied": true},
		},
	}
}

func TestValidateHeldContinuity_EmptyOK(t *testing.T) {
	t.Parallel()
	if err := ValidateHeldContinuity(5, nil); err != nil {
		t.Fatal(err)
	}
}

func TestValidateHeldContinuity_Contiguous(t *testing.T) {
	t.Parallel()
	held := []SpectatorSafeEvent{
		{Sequence: 6},
		{Sequence: 7},
		{Sequence: 8},
	}
	if err := ValidateHeldContinuity(5, held); err != nil {
		t.Fatal(err)
	}
}

func TestValidateHeldContinuity_Gap(t *testing.T) {
	t.Parallel()
	held := []SpectatorSafeEvent{
		{Sequence: 6},
		{Sequence: 8},
	}
	err := ValidateHeldContinuity(5, held)
	if !errors.Is(err, ErrHeldContinuityGap) {
		t.Fatalf("got %v", err)
	}
}

func TestValidateHeldContinuity_WrongStart(t *testing.T) {
	t.Parallel()
	held := []SpectatorSafeEvent{{Sequence: 7}}
	err := ValidateHeldContinuity(5, held)
	if !errors.Is(err, ErrHeldContinuityGap) {
		t.Fatalf("got %v", err)
	}
}

func TestValidateHeldContinuity_HardBound(t *testing.T) {
	t.Parallel()
	held := make([]SpectatorSafeEvent, MaxHeldRecoveryEvents+1)
	for i := range held {
		held[i].Sequence = SequenceNumber(i + 2) // resume=1 → expect 2..
	}
	err := ValidateHeldContinuity(1, held)
	if !errors.Is(err, ErrHeldContinuityBound) {
		t.Fatalf("got %v", err)
	}
}

func TestRebuildFromRecoverySnapshot_MidStreamBootstrap(t *testing.T) {
	t.Parallel()
	room := RoomID("room_rec")
	snap := SpectatorSafeEvent{
		EventID: "snap5", EventType: EventSnapshotSanitized, SchemaVersion: CurrentSchemaVersion,
		RoomID: room, Sequence: 5, Payload: publicSnapshotPayload(),
	}
	held := []SpectatorSafeEvent{{
		EventID: "h6", EventType: EventRoomLocked, SchemaVersion: CurrentSchemaVersion,
		RoomID: room, Sequence: 6, Payload: map[string]any{"status": "locked"},
	}}
	p, outs, err := RebuildFromRecoverySnapshot(room, snap, held)
	if err != nil {
		t.Fatal(err)
	}
	if p.Sequence() != 6 || p.Status() != RoomStatusLocked {
		t.Fatalf("seq=%d status=%s", p.Sequence(), p.Status())
	}
	if len(outs) != 2 || outs[0].Kind != OutcomeAccepted || outs[1].Kind != OutcomeAccepted {
		t.Fatalf("outs=%+v", outs)
	}
}

func TestRebuildFromRecoverySnapshot_GapDoesNotMutateUsableProjection(t *testing.T) {
	t.Parallel()
	room := RoomID("room_gap")
	snap := SpectatorSafeEvent{
		EventID: "snap5", EventType: EventSnapshotSanitized, SchemaVersion: CurrentSchemaVersion,
		RoomID: room, Sequence: 5, Payload: publicSnapshotPayload(),
	}
	held := []SpectatorSafeEvent{{
		EventID: "h7", EventType: EventRoomLocked, SchemaVersion: CurrentSchemaVersion,
		RoomID: room, Sequence: 7, Payload: map[string]any{"status": "locked"},
	}}
	p, _, err := RebuildFromRecoverySnapshot(room, snap, held)
	if !errors.Is(err, ErrHeldContinuityGap) {
		t.Fatalf("got %v", err)
	}
	if p != nil {
		t.Fatal("projection must be nil on continuity failure")
	}
}

func TestRebuildFromRecoverySnapshot_PrivateHeldFailsClosed(t *testing.T) {
	t.Parallel()
	room := RoomID("room_priv")
	snap := SpectatorSafeEvent{
		EventID: "snap1", EventType: EventSnapshotSanitized, SchemaVersion: CurrentSchemaVersion,
		RoomID: room, Sequence: 1, Payload: publicSnapshotPayload(),
	}
	held := []SpectatorSafeEvent{{
		EventID: "bad", EventType: EventCardPlayed, SchemaVersion: CurrentSchemaVersion,
		RoomID: room, Sequence: 2,
		Payload: map[string]any{"hand": []any{"red-1"}},
	}}
	_, outs, err := RebuildFromRecoverySnapshot(room, snap, held)
	if !errors.Is(err, ErrRecoveryApplyFailed) {
		t.Fatalf("got %v", err)
	}
	if len(outs) < 2 || outs[1].Kind != OutcomeDropped {
		t.Fatalf("outs=%+v", outs)
	}
}

func TestRebuildFromRecoverySnapshot_RejectsNonSnapshot(t *testing.T) {
	t.Parallel()
	_, _, err := RebuildFromRecoverySnapshot("room_x", SpectatorSafeEvent{
		EventID: "e1", EventType: EventRoomCreated, SchemaVersion: CurrentSchemaVersion,
		RoomID: "room_x", Sequence: 1, Payload: publicSnapshotPayload(),
	}, nil)
	if !errors.Is(err, ErrRecoverySnapshotInvalid) {
		t.Fatalf("got %v", err)
	}
}
