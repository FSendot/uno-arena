package domain

import "testing"

// safeEvent builds a versioned spectator-safe event for room_1.
func safeEvent(seq SequenceNumber, typ EventType, payload map[string]any) SpectatorSafeEvent {
	return SpectatorSafeEvent{
		EventID:       EventID("evt_" + u64toa(uint64(seq)) + "_" + string(typ)),
		EventType:     typ,
		SchemaVersion: CurrentSchemaVersion,
		RoomID:        "room_1",
		Sequence:      seq,
		Payload:       payload,
	}
}

// evt builds an event with an explicit id (for duplicate/privacy cases).
func evt(seq SequenceNumber, id EventID, typ EventType, payload map[string]any) SpectatorSafeEvent {
	return SpectatorSafeEvent{
		EventID:       id,
		EventType:     typ,
		SchemaVersion: CurrentSchemaVersion,
		RoomID:        "room_1",
		Sequence:      seq,
		Payload:       payload,
	}
}

func mustApply(t *testing.T, p *SpectatorRoomProjection, e SpectatorSafeEvent) ApplyOutcome {
	t.Helper()
	e.RoomID = p.RoomID()
	out := p.Apply(e)
	if out.Kind != OutcomeAccepted {
		t.Fatalf("apply %s seq=%d: %+v", e.EventID, e.Sequence, out)
	}
	return out
}

func assertHasFact(t *testing.T, facts []Fact, name FactName) {
	t.Helper()
	if !hasFact(facts, name) {
		t.Fatalf("missing fact %s in %+v", name, facts)
	}
}

func hasFact(facts []Fact, name FactName) bool {
	for _, f := range facts {
		if f.Name == name {
			return true
		}
	}
	return false
}

func factNames(facts []Fact) []FactName {
	out := make([]FactName, len(facts))
	for i, f := range facts {
		out[i] = f.Name
	}
	return out
}

func seededWaiting(t *testing.T) *SpectatorRoomProjection {
	t.Helper()
	p := NewSpectatorRoomProjection("room_1")
	mustApply(t, p, safeEvent(1, EventRoomCreated, map[string]any{
		"visibility": "public",
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 0},
		},
	}))
	return p
}

func seededInProgress(t *testing.T) *SpectatorRoomProjection {
	t.Helper()
	p := seededWaiting(t)
	mustApply(t, p, safeEvent(2, EventMatchStarted, map[string]any{
		"discardTop":      "red-7",
		"activeColor":     "red",
		"direction":       "clockwise",
		"currentPlayerId": "p1",
		"gameScore":       map[string]any{"p1": 0, "p2": 0},
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 7},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
		},
	}))
	return p
}
