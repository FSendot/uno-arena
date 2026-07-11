package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
)

func TestReservationRecovery_LostReserveResponseSameMaterial(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-lost-res/deck-operations"
	initDeck(t, mux, path, "g1")

	first := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-1:draw", "count": 2,
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}
	fb := decodeJSON(t, first)
	id1 := fb["reservationId"].(string)
	cards1 := mustJSON(t, fb["cards"])

	// Lost response: client retries same operation+shape.
	retry := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-1:draw", "count": 2,
	}, withRoomCred(nil))
	if retry.Code != http.StatusOK {
		t.Fatalf("retry: %d %s", retry.Code, retry.Body.String())
	}
	rb := decodeJSON(t, retry)
	if rb["reservationId"] != id1 {
		t.Fatalf("reservationId changed after lost response: %v vs %v", id1, rb["reservationId"])
	}
	if mustJSON(t, rb["cards"]) != cards1 {
		t.Fatalf("material changed after lost response")
	}
	if rb["kind"] != "duplicate" && rb["kind"] != "accepted" {
		t.Fatalf("kind=%v", rb["kind"])
	}
}

func TestReservationRecovery_SameOpDifferentShapeConflicts(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-shape/deck-operations"
	initDeck(t, mux, path, "g1")

	ok := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-shape:draw", "count": 1,
	}, withRoomCred(nil))
	if ok.Code != http.StatusOK {
		t.Fatalf("reserve: %d %s", ok.Code, ok.Body.String())
	}
	conflict := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-shape:draw", "count": 3,
	}, withRoomCred(nil))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("want 409 shape conflict, got %d %s", conflict.Code, conflict.Body.String())
	}
}

func TestReservationRecovery_ConfirmOriginalIDIdempotentAfterLostConfirm(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-lost-confirm/deck-operations"
	initDeck(t, mux, path, "g1")

	res := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "cmd-c:draw", "count": 1,
	}, withRoomCred(nil))
	id := decodeJSON(t, res)["reservationId"].(string)

	first := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "confirm", "gameId": "g1", "reservationId": id,
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", first.Code, first.Body.String())
	}

	// Lost confirm response: retry Confirm with the original reservation id.
	retry := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "confirm", "gameId": "g1", "reservationId": id,
	}, withRoomCred(nil))
	if retry.Code != http.StatusOK {
		t.Fatalf("confirm retry: %d %s", retry.Code, retry.Body.String())
	}
	if kind := decodeJSON(t, retry)["kind"]; kind != "duplicate" && kind != "accepted" {
		t.Fatalf("kind=%v", kind)
	}
}

func TestReservationRecovery_BusyOtherOp_SameOpReplayAllowed(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-busy/deck-operations"
	initDeck(t, mux, path, "g1")

	first := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "op-a:draw", "count": 1,
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}

	busy := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "op-b:draw", "count": 1,
	}, withRoomCred(nil))
	if busy.Code != http.StatusConflict {
		t.Fatalf("want busy conflict, got %d %s", busy.Code, busy.Body.String())
	}

	replay := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "op-a:draw", "count": 1,
	}, withRoomCred(nil))
	if replay.Code != http.StatusOK {
		t.Fatalf("same-op replay: %d %s", replay.Code, replay.Body.String())
	}
}

func TestReservationRecovery_CancelThenNewReservationCorrectCards(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-cancel/deck-operations"
	initDeck(t, mux, path, "g1")

	r1 := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "op-1:draw", "count": 2,
	}, withRoomCred(nil))
	b1 := decodeJSON(t, r1)
	id1 := b1["reservationId"].(string)
	cards1 := mustJSON(t, b1["cards"])

	cancel := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "cancel", "gameId": "g1", "reservationId": id1,
	}, withRoomCred(nil))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancel.Code, cancel.Body.String())
	}

	r2 := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_draw", "gameId": "g1", "operationId": "op-2:draw", "count": 2,
	}, withRoomCred(nil))
	if r2.Code != http.StatusOK {
		t.Fatalf("re-reserve: %d %s", r2.Code, r2.Body.String())
	}
	b2 := decodeJSON(t, r2)
	if b2["reservationId"] == id1 {
		t.Fatal("new reservation must use a distinct id for a new operation")
	}
	// Cancel must not shift/reinterpret: next reservation peeks the same front cards.
	if mustJSON(t, b2["cards"]) != cards1 {
		t.Fatalf("after cancel, new reservation must return same front cards; got %s want %s",
			mustJSON(t, b2["cards"]), cards1)
	}
}

func TestReservationRecovery_ConcurrentRoomsNoIDCollision(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()

	const n = 16
	ids := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			room := "room-coll-" + itoa(i)
			path := "/internal/v1/game-logs/" + room + "/deck-operations"
			init := doJSON(t, mux, http.MethodPost, path, map[string]any{
				"operation": "initialize", "gameId": "g-shared",
			}, withRoomCred(nil))
			if init.Code != http.StatusOK {
				t.Errorf("init %s: %d", room, init.Code)
				return
			}
			res := doJSON(t, mux, http.MethodPost, path, map[string]any{
				"operation": "reserve_draw", "gameId": "g-shared",
				"operationId": "same-op:draw", "count": 1,
			}, withRoomCred(nil))
			if res.Code != http.StatusOK {
				t.Errorf("reserve %s: %d %s", room, res.Code, res.Body.String())
				return
			}
			ids[i] = decodeJSON(t, res)["reservationId"].(string)
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for i, id := range ids {
		if id == "" {
			t.Fatalf("empty id at %d", i)
		}
		if prev, ok := seen[id]; ok {
			t.Fatalf("collision: rooms %d and %d share reservationId %q", prev, i, id)
		}
		seen[id] = i
	}
}

func TestReservationRecovery_DeterministicIDIncludesRoomGameOpShape(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-det/deck-operations"
	initDeck(t, mux, path, "g1")

	a := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_deal", "gameId": "g1", "operationId": "cmd-d:deal",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	idA := decodeJSON(t, a)["reservationId"].(string)
	_ = doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "cancel", "gameId": "g1", "reservationId": idA,
	}, withRoomCred(nil))

	b := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_deal", "gameId": "g1", "operationId": "cmd-d:deal",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	idB := decodeJSON(t, b)["reservationId"].(string)
	if idA != idB {
		t.Fatalf("same room+game+op+shape must yield same reservationId: %q vs %q", idA, idB)
	}
	if idA == "" || idA == "res-1" || idA == "res-2" {
		t.Fatalf("reservationId must not be a process counter, got %q", idA)
	}
}

func initDeck(t *testing.T, mux http.Handler, path, gameID string) {
	t.Helper()
	init := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "initialize", "gameId": gameID,
	}, withRoomCred(nil))
	if init.Code != http.StatusOK {
		t.Fatalf("init: %d %s", init.Code, init.Body.String())
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
