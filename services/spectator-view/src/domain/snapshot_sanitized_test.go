package domain

import "testing"

func TestSnapshotSanitized_AppliesFullPublicState_GameCompletedStaysInProgress(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_snap")

	out := mustApply(t, p, evt(1, "s1", EventSnapshotSanitized, map[string]any{
		"status":     "waiting",
		"visibility": "public",
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "host", "displayName": "host", "cardCount": 0, "occupied": true},
			map[string]any{"seatIndex": 1, "playerId": "guest", "displayName": "guest", "cardCount": 0, "occupied": true},
		},
	}))
	if hasFact(out.Facts, FactStreamClose) {
		t.Fatal("nonterminal snapshot must not close")
	}
	if p.Sequence() != 1 || p.Status() != RoomStatusWaiting {
		t.Fatalf("seq=%d status=%s", p.Sequence(), p.Status())
	}

	mustApply(t, p, evt(2, "s2", EventSnapshotSanitized, map[string]any{
		"status":          "in_progress",
		"visibility":      "public",
		"gameCompleted":   false,
		"discardTop":      "red-5",
		"activeColor":     "red",
		"direction":       "clockwise",
		"currentPlayerId": "host",
		"penaltyAmount":   0,
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "host", "displayName": "host", "cardCount": 1, "occupied": true},
			map[string]any{"seatIndex": 1, "playerId": "guest", "displayName": "guest", "cardCount": 1, "occupied": true},
		},
	}))

	mustApply(t, p, evt(3, "s3", EventSnapshotSanitized, map[string]any{
		"status":          "in_progress",
		"visibility":      "public",
		"gameCompleted":   true,
		"winnerPlayerId":  "host",
		"gameScore":       map[string]any{"host": 1, "guest": 0},
		"discardTop":      "red-3",
		"activeColor":     "red",
		"direction":       "clockwise",
		"currentPlayerId": "host",
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "host", "displayName": "host", "cardCount": 0, "occupied": true},
			map[string]any{"seatIndex": 1, "playerId": "guest", "displayName": "guest", "cardCount": 1, "occupied": true},
		},
	}))
	if p.Status() != RoomStatusInProgress {
		t.Fatalf("after gameCompleted snapshot status=%s", p.Status())
	}
	snap := p.Snapshot()
	if !snap.GameCompleted || snap.StreamClosed {
		t.Fatalf("snap=%+v", snap)
	}
	if snap.GameScore["host"] != 1 {
		t.Fatalf("score=%v", snap.GameScore)
	}
	dec := p.Admission(SpectatorAuth{IsPublicRoom: true})
	if !dec.Allowed {
		t.Fatalf("admission after gameCompleted: %+v", dec)
	}

	out = mustApply(t, p, evt(4, "s4", EventRoomCompleted, map[string]any{
		"status":        "completed",
		"matchWinner":   "host",
		"gameScore":     map[string]any{"host": 2, "guest": 0},
		"gameCompleted": true,
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "host", "displayName": "host", "cardCount": 0, "occupied": true},
			map[string]any{"seatIndex": 1, "playerId": "guest", "displayName": "guest", "cardCount": 0, "occupied": true},
		},
	}))
	if !hasFact(out.Facts, FactStreamClose) {
		t.Fatal("RoomCompleted must close")
	}
	if p.Status() != RoomStatusCompleted || !p.StreamClosed() {
		t.Fatalf("status=%s closed=%v", p.Status(), p.StreamClosed())
	}
}
