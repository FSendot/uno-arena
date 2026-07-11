package domain

import (
	"testing"
	"time"
)

// End-to-end public projection: monotonic sequence, nested unoWindow with
// absolute expiry + openingRoomSequence, and visible roster/discard/turn.
func TestApply_MonotonicPublicState(t *testing.T) {
	t.Parallel()
	p := NewSpectatorRoomProjection("room_1")
	mustApply(t, p, evt(1, "evt_1", EventRoomCreated, map[string]any{
		"visibility": "public",
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 0},
		},
	}))
	mustApply(t, p, evt(2, "evt_2", EventMatchStarted, map[string]any{
		"discardTop": "yellow-3", "activeColor": "yellow", "direction": "clockwise",
		"currentPlayerId": "p1",
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 7},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
		},
	}))
	mustApply(t, p, evt(3, "evt_3", EventCardPlayed, map[string]any{
		"discardTop": "yellow-5", "activeColor": "yellow", "direction": "clockwise",
		"currentPlayerId": "p2", "penaltyAmount": 0,
		"roster": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 6},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
		},
		"unoWindow": map[string]any{
			"playerId":              "p1",
			"expiresAt":             "2026-05-17T12:00:09Z",
			"openingRoomSequence":   3,
			"triggeringGameEventId": "evt_3",
		},
	}))

	snap := p.Snapshot()
	if snap.Sequence != 3 || snap.Status != RoomStatusInProgress {
		t.Fatalf("snap=%+v", snap)
	}
	if snap.Discard.DiscardTop != "yellow-5" || snap.Discard.ActiveColor != "yellow" {
		t.Fatalf("discard=%+v", snap.Discard)
	}
	if snap.CurrentPlayerID != "p2" || snap.Direction != "clockwise" {
		t.Fatalf("turn=%s dir=%s", snap.CurrentPlayerID, snap.Direction)
	}
	if len(snap.Seats) != 2 || snap.Seats[0].CardCount != 6 {
		t.Fatalf("seats=%+v", snap.Seats)
	}
	if snap.Uno == nil || snap.Uno.PlayerID != "p1" || snap.Uno.OpeningSequence != 3 {
		t.Fatalf("uno=%+v", snap.Uno)
	}
	if !snap.Uno.ExpiresAt.Equal(time.Date(2026, 5, 17, 12, 0, 9, 0, time.UTC)) {
		t.Fatalf("expiresAt=%v", snap.Uno.ExpiresAt)
	}
}
