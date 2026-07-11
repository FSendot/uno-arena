package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"unoarena/services/game-integrity/domain"
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
	if a.Code != http.StatusOK {
		t.Fatalf("reserve: %d %s", a.Code, a.Body.String())
	}
	idA := decodeJSON(t, a)["reservationId"].(string)
	if idA == "" || idA == "res-1" || idA == "res-2" {
		t.Fatalf("reservationId must not be a process counter, got %q", idA)
	}
	// Same room+game+op+shape is deterministic before cancel.
	again := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_deal", "gameId": "g1", "operationId": "cmd-d:deal",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if again.Code != http.StatusOK {
		t.Fatalf("duplicate reserve: %d %s", again.Code, again.Body.String())
	}
	if idB := decodeJSON(t, again)["reservationId"].(string); idA != idB {
		t.Fatalf("same room+game+op+shape must yield same reservationId: %q vs %q", idA, idB)
	}

	cancel := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "cancel", "gameId": "g1", "reservationId": idA,
	}, withRoomCred(nil))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancel.Code, cancel.Body.String())
	}
	// Repeated cancel is idempotent.
	cancel2 := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "cancel", "gameId": "g1", "reservationId": idA,
	}, withRoomCred(nil))
	if cancel2.Code != http.StatusOK {
		t.Fatalf("cancel retry: %d %s", cancel2.Code, cancel2.Body.String())
	}

	// Cancelled deterministic ID must never be reused.
	reuse := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_deal", "gameId": "g1", "operationId": "cmd-d:deal",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if reuse.Code != http.StatusConflict {
		t.Fatalf("reserve after cancel want 409, got %d %s", reuse.Code, reuse.Body.String())
	}
	body := decodeJSON(t, reuse)
	if body["code"] != string(domain.RejectConflictingDuplicate) {
		t.Fatalf("reuse code: %+v", body)
	}

	// Later reservation with a different operation must succeed under a distinct ID.
	later := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_deal", "gameId": "g1", "operationId": "cmd-d2:deal",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if later.Code != http.StatusOK {
		t.Fatalf("later reserve: %d %s", later.Code, later.Body.String())
	}
	idLater := decodeJSON(t, later)["reservationId"].(string)
	if idLater == idA {
		t.Fatal("later reservation must not reuse cancelled ID")
	}

	// Delayed confirm of the cancelled ID must not affect the later reservation.
	staleConfirm := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "confirm", "gameId": "g1", "reservationId": idA,
	}, withRoomCred(nil))
	if staleConfirm.Code != http.StatusConflict {
		t.Fatalf("stale confirm want 409, got %d %s", staleConfirm.Code, staleConfirm.Body.String())
	}
	stillPending := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "reserve_deal", "gameId": "g1", "operationId": "cmd-d2:deal",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if stillPending.Code != http.StatusOK {
		t.Fatalf("later still pending: %d %s", stillPending.Code, stillPending.Body.String())
	}
	if decodeJSON(t, stillPending)["reservationId"] != idLater {
		t.Fatal("stale confirm mutated later reservation")
	}
}

func TestReservationRecovery_CancelTombstoneSurvivesSnapshotRoundTrip(t *testing.T) {
	seedBytes := make([]byte, 32)
	for i := range seedBytes {
		seedBytes[i] = byte(i + 3)
	}
	ds, err := domain.NewDeckSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	deck, err := domain.NewAuthoritativeDeck("game-t", ds, StandardDeckCards())
	if err != nil {
		t.Fatal(err)
	}
	st := &DeckState{
		Deck:          deck,
		SeedHex:       hex.EncodeToString(seedBytes),
		SeedCommit:    sha256Hex(seedBytes),
		OrderCommit:   orderCommitment(deck.ShuffledOrder()),
		Pending:       map[string]*pendingReservation{},
		ByOp:          map[domain.DrawOperationID]*pendingReservation{},
		Confirmed:     map[domain.DrawOperationID]confirmedOp{},
		ConfirmedByID: map[string]confirmedOp{},
		Cancelled:     map[string]struct{}{"res-tomb": {}},
		CancelledOps:  map[domain.DrawOperationID]struct{}{"op-tomb": {}},
	}
	snap, err := snapshotFromDeckState("room-t", "game-t", st)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := deckStateFromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := restored.Cancelled["res-tomb"]; !ok {
		t.Fatal("cancelled reservation ID not restored")
	}
	if _, ok := restored.CancelledOps["op-tomb"]; !ok {
		t.Fatal("cancelled operation ID not restored")
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

func TestReserveDeal_RejectsInvalidSeatsAtRequest(t *testing.T) {
	cases := []struct {
		name  string
		seats []string
	}{
		{"too_few", []string{"only"}},
		{"blank", []string{"a", ""}},
		{"whitespace", []string{"a", " \t"}},
		{"duplicate", []string{"a", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t)
			mux := srv.routes()
			path := "/internal/v1/game-logs/room-deal-seats-" + tc.name + "/deck-operations"
			game := "g-seats-" + tc.name
			initDeck(t, mux, path, game)
			res := doJSON(t, mux, http.MethodPost, path, map[string]any{
				"operation": "reserve_deal", "gameId": game, "operationId": "op-" + tc.name,
				"seats": tc.seats, "cardsPerHand": 7,
			}, withRoomCred(nil))
			if res.Code == http.StatusOK {
				t.Fatalf("invalid seats %v must be rejected, got %s", tc.seats, res.Body.String())
			}
			body := decodeJSON(t, res)
			if body["code"] == nil {
				t.Fatalf("rejection must include code, body=%s", res.Body.String())
			}
		})
	}
}

func TestReserveDeal_CommaCollidingSeatVectorsGetDistinctShapes(t *testing.T) {
	if dealShape([]string{"a,b", "c"}, 7) == dealShape([]string{"a", "b,c"}, 7) {
		t.Fatal("dealShape must distinguish comma-colliding seat vectors")
	}
	svc := NewService(NewMemoryStreamRepository())
	ctx := context.Background()
	room := "room-inj"
	game := "g-inj"
	_, _, err := svc.Append(ctx, AppendRequest{
		RoomID: room, EventID: "e1", ExpectedRevision: 0, EventType: "CreateRoom",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, rej, err := svc.InitializeDeck(ctx, InitializeDeckRequest{RoomID: room, GameID: game}); err != nil || rej != nil {
		t.Fatalf("init: rej=%v err=%v", rej, err)
	}
	a, rej, err := svc.ReserveDeal(ctx, ReserveDealRequest{
		RoomID: room, GameID: game, OperationID: "op-a",
		Seats: []string{"a,b", "c"}, CardsPerHand: 7,
	})
	if err != nil || rej != nil {
		t.Fatalf("reserve a: rej=%v err=%v", rej, err)
	}
	if _, err := svc.CancelReservation(ctx, room, game, a.ReservationID); err != nil {
		t.Fatal(err)
	}
	b, rej, err := svc.ReserveDeal(ctx, ReserveDealRequest{
		RoomID: room, GameID: game, OperationID: "op-b",
		Seats: []string{"a", "b,c"}, CardsPerHand: 7,
	})
	if err != nil || rej != nil {
		t.Fatalf("reserve b: rej=%v err=%v", rej, err)
	}
	if a.ReservationID == b.ReservationID {
		t.Fatal("comma-colliding seat vectors must not share reservation identity")
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
