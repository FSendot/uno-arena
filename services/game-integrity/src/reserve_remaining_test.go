package main

import (
	"context"
	"net/http"
	"testing"
)

func TestReserveDraw_RemainingAfterReservationAndDuplicate(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-rem-draw/deck-operations"
	initDeck(t, mux, path, "g1")

	first := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-1:draw", "count": 3,
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}
	fb := decodeJSON(t, first)
	rem, ok := fb["remaining"].(float64)
	if !ok {
		t.Fatalf("missing remaining: %+v", fb)
	}
	if int(rem) != 105 { // 108 - 3
		t.Fatalf("remaining=%v want 105", rem)
	}

	retry := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-1:draw", "count": 3,
	}, withRoomCred(nil))
	if retry.Code != http.StatusOK {
		t.Fatalf("retry: %d %s", retry.Code, retry.Body.String())
	}
	rb := decodeJSON(t, retry)
	if rb["remaining"] != rem {
		t.Fatalf("duplicate remaining=%v want %v", rb["remaining"], rem)
	}

	id := fb["reservationId"].(string)
	confirm := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "confirm", "gameId": "g1", "reservationId": id,
	}, withRoomCred(nil))
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", confirm.Code, confirm.Body.String())
	}

	// Confirmed duplicate must still return the remaining that was true after that reservation.
	afterConfirm := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-1:draw", "count": 3,
	}, withRoomCred(nil))
	if afterConfirm.Code != http.StatusOK {
		t.Fatalf("after confirm: %d %s", afterConfirm.Code, afterConfirm.Body.String())
	}
	ab := decodeJSON(t, afterConfirm)
	if ab["remaining"] != rem {
		t.Fatalf("confirmed duplicate remaining=%v want %v", ab["remaining"], rem)
	}

	// A later reservation reflects consumption.
	next := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-2:draw", "count": 1,
	}, withRoomCred(nil))
	if next.Code != http.StatusOK {
		t.Fatalf("next: %d %s", next.Code, next.Body.String())
	}
	nb := decodeJSON(t, next)
	if int(nb["remaining"].(float64)) != 104 {
		t.Fatalf("next remaining=%v want 104", nb["remaining"])
	}
}

func TestReserveDeal_RemainingAfterReservation(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryStreamRepository()
	svc := NewService(repo)
	_, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: "r-deal-rem", GameID: "g1"})
	if err != nil || rej != nil {
		t.Fatalf("init: err=%v rej=%v", err, rej)
	}
	res, rej, err := svc.ReserveDeal(ctx, ReserveDealRequest{
		RoomID: "r-deal-rem", GameID: "g1", OperationID: "start:deal",
		Seats: []string{"a", "b"},
	})
	if err != nil || rej != nil {
		t.Fatalf("reserve: err=%v rej=%v", err, rej)
	}
	// 2 seats * 7 + 1 discard = 15; 108 - 15 = 93
	if res.Remaining != 93 {
		t.Fatalf("remaining=%d want 93", res.Remaining)
	}
	dup, rej, err := svc.ReserveDeal(ctx, ReserveDealRequest{
		RoomID: "r-deal-rem", GameID: "g1", OperationID: "start:deal",
		Seats: []string{"a", "b"},
	})
	if err != nil || rej != nil {
		t.Fatalf("dup: err=%v rej=%v", err, rej)
	}
	if dup.Remaining != 93 {
		t.Fatalf("dup remaining=%d want 93", dup.Remaining)
	}
}

func TestReserve_RemainingNeverDerivedFromClientInput(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-rem-client/deck-operations"
	initDeck(t, mux, path, "g1")

	// Client-supplied remaining must be ignored; GI computes authoritatively.
	w := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd:draw",
		"count": 1, "remaining": 999,
	}, withRoomCred(nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	body := decodeJSON(t, w)
	if int(body["remaining"].(float64)) != 107 {
		t.Fatalf("remaining=%v want 107 (not client 999)", body["remaining"])
	}
}
