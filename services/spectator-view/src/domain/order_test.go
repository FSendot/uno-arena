package domain

import (
	"testing"
	"time"
)

func TestApply_DuplicateEventIDStable(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	ev := safeEvent(2, EventRoomLocked, map[string]any{"status": "locked"})
	first := mustApply(t, p, ev)
	if p.Status() != RoomStatusLocked {
		t.Fatalf("status=%s", p.Status())
	}
	second := p.Apply(ev)
	if second.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s want duplicate", second.Kind)
	}
	if second.Rejection != nil {
		t.Fatalf("duplicate must be stable success, got %+v", second.Rejection)
	}
	if !hasFact(second.Facts, FactSpectatorRoomProjectionUpdated) {
		t.Fatalf("duplicate should return prior facts, got %v", factNames(second.Facts))
	}
	if len(second.Facts) != len(first.Facts) {
		t.Fatalf("fact count changed on duplicate")
	}
	// Mutating returned facts must not affect stored prior outcome.
	second.Facts[0].Data["roomId"] = "tampered"
	third := p.Apply(ev)
	if third.Facts[0].Data["roomId"] == "tampered" {
		t.Fatal("duplicate facts not defensively copied")
	}
}

func TestApply_StaleSequenceIgnored(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	mustApply(t, p, safeEvent(2, EventRoomLocked, map[string]any{}))
	stale := safeEvent(1, EventPlayerJoinedRoom, map[string]any{
		"playerId":    "p3",
		"displayName": "Carol",
		"seatIndex":   2,
		"cardCount":   0,
	})
	stale.EventID = "evt_stale"
	out := p.Apply(stale)
	if out.Kind != OutcomeIgnored {
		t.Fatalf("kind=%s want ignored", out.Kind)
	}
	if out.Rejection == nil || out.Rejection.Code != RejectStaleSequence {
		t.Fatalf("rejection=%+v", out.Rejection)
	}
	if p.Sequence() != 2 {
		t.Fatalf("sequence=%d", p.Sequence())
	}
	for _, s := range p.Snapshot().Seats {
		if s.PlayerID == "p3" {
			t.Fatal("stale event must not mutate roster")
		}
	}
}

func TestApply_OutOfOrderSequenceQuarantined(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	future := safeEvent(5, EventCardPlayed, map[string]any{
		"discardTop":      "red-1",
		"activeColor":     "red",
		"currentPlayerId": "p1",
	})
	out := p.Apply(future)
	if out.Kind != OutcomeQuarantined {
		t.Fatalf("kind=%s want quarantined", out.Kind)
	}
	if out.Rejection == nil || out.Rejection.Code != RejectOutOfOrderSequence {
		t.Fatalf("rejection=%+v", out.Rejection)
	}
	if !hasFact(out.Facts, FactProjectionEventQuarantined) {
		t.Fatal("expected ProjectionEventQuarantined")
	}
	if hasFact(out.Facts, FactSpectatorRoomProjectionUpdated) {
		t.Fatal("out-of-order must not update projection")
	}
	if p.Sequence() != 1 {
		t.Fatalf("sequence=%d", p.Sequence())
	}
}

func TestApply_MonotonicSequenceAdvances(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_mono")
	for i := SequenceNumber(1); i <= 4; i++ {
		var et EventType
		var pl map[string]any
		switch i {
		case 1:
			et, pl = EventRoomCreated, map[string]any{"visibility": "public"}
		case 2:
			et, pl = EventRoomLocked, map[string]any{}
		case 3:
			et, pl = EventMatchStarted, map[string]any{
				"discardTop": "green-5", "activeColor": "green",
				"direction": "clockwise", "currentPlayerId": "p1",
				"seats": []any{
					map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "A", "cardCount": 7},
				},
			}
		case 4:
			et, pl = EventTurnAdvanced, map[string]any{"currentPlayerId": "p1", "direction": "clockwise"}
		}
		out := mustApply(t, p, safeEvent(i, et, pl))
		if out.Sequence != i {
			t.Fatalf("outcome sequence=%d want %d", out.Sequence, i)
		}
		if p.Sequence() != i {
			t.Fatalf("projection sequence=%d want %d", p.Sequence(), i)
		}
	}
}

func TestApply_RoomMismatchQuarantined(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_a")
	ev := safeEvent(1, EventRoomCreated, map[string]any{"visibility": "public"})
	ev.RoomID = "room_b"
	out := p.Apply(ev)
	if out.Kind != OutcomeQuarantined {
		t.Fatalf("kind=%s", out.Kind)
	}
	if out.Rejection == nil || out.Rejection.Code != RejectRoomMismatch {
		t.Fatalf("rejection=%+v", out.Rejection)
	}
}

func TestApply_InvalidSchemaVersionQuarantined(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_sv")
	ev := safeEvent(1, EventRoomCreated, map[string]any{"visibility": "public"})
	ev.SchemaVersion = 99
	out := p.Apply(ev)
	if out.Kind != OutcomeQuarantined || out.Rejection == nil || out.Rejection.Code != RejectInvalidSchema {
		t.Fatalf("out=%+v", out)
	}
}

func TestApply_PublicUnoExpiresAtAndOpeningSequence(t *testing.T) {
	t.Parallel()
	p := seededInProgress(t)
	exp := time.Date(2026, 5, 17, 12, 0, 9, 0, time.UTC)
	out := mustApply(t, p, SpectatorSafeEvent{
		EventID:       "evt_uno",
		EventType:     EventUnoWindowOpened,
		SchemaVersion: CurrentSchemaVersion,
		RoomID:        p.RoomID(),
		Sequence:      3,
		Payload: map[string]any{
			"playerId":        "p1",
			"expiresAt":       exp.Format(time.RFC3339),
			"openingSequence": 3,
		},
	})
	if !hasFact(out.Facts, FactSpectatorRoomProjectionUpdated) {
		t.Fatal("expected projection updated")
	}
	snap := p.Snapshot()
	if snap.Uno == nil {
		t.Fatal("expected public Uno window")
	}
	if snap.Uno.PlayerID != "p1" || !snap.Uno.ExpiresAt.Equal(exp) || snap.Uno.OpeningSequence != 3 {
		t.Fatalf("uno=%+v", snap.Uno)
	}
}
