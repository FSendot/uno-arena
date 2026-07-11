package domain

import (
	"testing"
	"time"
)

func TestRebuildFrom_SanitizedStream(t *testing.T) {
	t.Parallel()
	exp := time.Date(2026, 7, 10, 15, 0, 5, 0, time.UTC)
	room := RoomID("room_rebuild")
	mk := func(seq SequenceNumber, typ EventType, payload map[string]any) SpectatorSafeEvent {
		ev := safeEvent(seq, typ, payload)
		ev.RoomID = room
		ev.EventID = EventID("evt_" + string(room) + "_" + u64toa(uint64(seq)))
		return ev
	}
	events := []SpectatorSafeEvent{
		mk(1, EventRoomCreated, map[string]any{
			"visibility": "public",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
				map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 0},
			},
		}),
		mk(2, EventMatchStarted, map[string]any{
			"discardTop": "red-7", "activeColor": "red", "direction": "clockwise",
			"currentPlayerId": "p1", "penaltyAmount": 0,
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 7},
				map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
			},
		}),
		func() SpectatorSafeEvent {
			ev := mk(3, EventCardPlayed, map[string]any{
				"discardTop": "yellow-1",
				"hand":       []any{"red-1"},
			})
			ev.EventID = "evt_bad_hand"
			return ev
		}(),
		mk(3, EventCardPlayed, map[string]any{
			"discardTop": "yellow-1", "activeColor": "yellow",
			"currentPlayerId": "p2", "direction": "clockwise",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 6},
				map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
			},
		}),
		mk(4, EventUnoWindowOpened, map[string]any{
			"playerId": "p1", "expiresAt": exp.Format(time.RFC3339), "openingSequence": 4,
		}),
		mk(5, EventGameCompleted, map[string]any{
			"gameScore": map[string]any{"p1": 1, "p2": 0}, "winnerPlayerId": "p1",
		}),
		mk(6, EventRoomCompleted, map[string]any{
			"matchWinner": "p1", "gameScore": map[string]any{"p1": 2, "p2": 0},
		}),
	}

	p, outcomes := RebuildFrom(room, events)
	if len(outcomes) != len(events) {
		t.Fatalf("outcomes=%d events=%d", len(outcomes), len(events))
	}
	if outcomes[2].Kind != OutcomeDropped {
		t.Fatalf("privacy event kind=%s rej=%v", outcomes[2].Kind, outcomes[2].Rejection)
	}
	if p.Sequence() != 6 {
		t.Fatalf("sequence=%d want 6", p.Sequence())
	}
	snap := p.Snapshot()
	if snap.Status != RoomStatusCompleted {
		t.Fatalf("status=%s", snap.Status)
	}
	if snap.MatchWinner != "p1" {
		t.Fatalf("matchWinner=%s", snap.MatchWinner)
	}
	if snap.Discard.DiscardTop != "yellow-1" {
		t.Fatalf("discard=%s", snap.Discard.DiscardTop)
	}
	if snap.Seats[0].CardCount != 6 {
		t.Fatalf("cardCount=%d", snap.Seats[0].CardCount)
	}
	if !snap.StreamClosed {
		t.Fatal("expected stream closed")
	}
	if p.Admission(SpectatorAuth{IsPublicRoom: true}).Allowed {
		t.Fatal("terminal rebuild must deny admission")
	}
	if !hasFact(outcomes[len(outcomes)-1].Facts, FactStreamClose) {
		t.Fatal("final RoomCompleted must emit StreamClose")
	}
}

func TestRebuildFrom_EmptyStream(t *testing.T) {
	t.Parallel()
	p, outcomes := RebuildFrom("room_empty", nil)
	if len(outcomes) != 0 {
		t.Fatalf("outcomes=%d", len(outcomes))
	}
	if p.Sequence() != 0 || p.Status() != RoomStatusWaiting {
		t.Fatalf("seq=%d status=%s", p.Sequence(), p.Status())
	}
}

func TestApply_PublicStateFieldsProjected(t *testing.T) {
	t.Parallel()
	p := seededInProgress(t)
	mustApply(t, p, safeEvent(3, EventPenaltyUpdated, map[string]any{
		"penaltyAmount":   4,
		"penaltyTarget":   "p2",
		"direction":       "counterclockwise",
		"activeColor":     "blue",
		"discardTop":      "draw-2-blue",
		"currentPlayerId": "p2",
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 5},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 9},
		},
	}))
	snap := p.Snapshot()
	if snap.PenaltyAmount != 4 || snap.PenaltyTarget != "p2" {
		t.Fatalf("penalty=%d target=%s", snap.PenaltyAmount, snap.PenaltyTarget)
	}
	if snap.Direction != "counterclockwise" || snap.Discard.ActiveColor != "blue" {
		t.Fatalf("direction/color=%s/%s", snap.Direction, snap.Discard.ActiveColor)
	}
	if snap.CurrentPlayerID != "p2" || snap.Seats[1].CardCount != 9 {
		t.Fatalf("turn/seats=%+v", snap)
	}
}
