package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExportRestoreExactRoundTrip(t *testing.T) {
	p := NewSpectatorRoomProjection(RoomID("room_export"))
	out := p.Apply(SpectatorSafeEvent{
		EventID: "e1", EventType: EventRoomCreated, SchemaVersion: 1,
		RoomID: "room_export", Sequence: 1,
		Payload: map[string]any{
			"visibility": "private",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 7},
			},
		},
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("seed: %+v", out)
	}
	expAt := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	out2 := p.Apply(SpectatorSafeEvent{
		EventID: "e2", EventType: EventUnoWindowOpened, SchemaVersion: 1,
		RoomID: "room_export", Sequence: 2,
		Payload: map[string]any{
			"playerId": "p1", "expiresAt": expAt.Format(time.RFC3339), "openingSequence": 2,
		},
	})
	if out2.Kind != OutcomeAccepted {
		t.Fatalf("uno: %+v", out2)
	}
	_ = p.Apply(SpectatorSafeEvent{
		EventID: "e_drop", EventType: EventCardPlayed, SchemaVersion: 1,
		RoomID: "room_export", Sequence: 3,
		Payload: map[string]any{"hand": []any{"red-1"}},
	})

	exp := p.ExportState()
	raw, err := MarshalExport(exp)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalExport(raw)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreProjection(decoded)
	if err != nil {
		t.Fatal(err)
	}

	if restored.RoomID() != p.RoomID() || restored.Status() != p.Status() ||
		restored.Sequence() != p.Sequence() || restored.StreamClosed() != p.StreamClosed() {
		t.Fatalf("meta mismatch restored=%+v want seq=%d status=%s", restored.Snapshot(), p.Sequence(), p.Status())
	}
	snapA, _ := json.Marshal(p.Snapshot())
	snapB, _ := json.Marshal(restored.Snapshot())
	if string(snapA) != string(snapB) {
		t.Fatalf("snapshot mismatch\nA=%s\nB=%s", snapA, snapB)
	}

	// Exact eventId replay returns same disposition without mutating.
	dup := restored.Apply(SpectatorSafeEvent{
		EventID: "e1", EventType: EventRoomCreated, SchemaVersion: 1,
		RoomID: "room_export", Sequence: 1,
		Payload: map[string]any{"visibility": "private"},
	})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("dup kind=%s", dup.Kind)
	}
	dropReplay := restored.Apply(SpectatorSafeEvent{
		EventID: "e_drop", EventType: EventCardPlayed, SchemaVersion: 1,
		RoomID: "room_export", Sequence: 3,
		Payload: map[string]any{"hand": []any{"red-1"}},
	})
	if dropReplay.Kind != OutcomeDuplicate {
		t.Fatalf("drop replay kind=%s", dropReplay.Kind)
	}
	if restored.Sequence() != p.Sequence() {
		t.Fatalf("replay mutated sequence")
	}
}

func TestRestoreWithoutFakeEvents(t *testing.T) {
	exp := ProjectionExport{
		RoomID:       "r1",
		Status:       RoomStatusCompleted,
		Visibility:   VisibilityPublic,
		Sequence:     9,
		StreamClosed: true,
		MatchCompleted: true,
		GameScore:    map[PlayerID]int{"p1": 2},
		MatchWinner:  "p1",
		Outcomes: map[EventID]OutcomeExport{
			"term": {Kind: OutcomeAccepted, EventID: "term", Sequence: 9},
		},
	}
	p, err := RestoreProjection(exp)
	if err != nil {
		t.Fatal(err)
	}
	if !p.StreamClosed() || p.Status() != RoomStatusCompleted || p.Sequence() != 9 {
		t.Fatalf("restored=%+v", p.Snapshot())
	}
	dec := p.Admission(SpectatorAuth{IsOperator: true})
	if dec.Allowed {
		t.Fatal("terminal restored projection must deny admission")
	}
}
