package app_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"unoarena/services/room-gameplay/app"
)

// Contract-compatible GI double for Room HTTP adapter tests.
// Mirrors Game Integrity append + reservation HTTP shapes (stdlib only).

type giDouble struct {
	mu       sync.Mutex
	cred     string
	revision int64
	events   []map[string]any
	decks    map[string]*giDeck
}

type giDeck struct {
	inited      bool
	pointer     int
	pending     map[string]giPending
	byOp        map[string]string
	confirmedID map[string]giPending
}

type giPending struct {
	opID           string
	kind           string
	count          int
	seats          []string
	remainingAfter int
}

func newGIDouble(cred string) *giDouble {
	return &giDouble{cred: cred, decks: map[string]*giDeck{}}
}

func detResID(roomID, gameID, opID, shape string) string {
	sum := sha256.Sum256([]byte(roomID + "\x00" + gameID + "\x00" + opID + "\x00" + shape))
	return "res-" + hex.EncodeToString(sum[:])
}

func (g *giDouble) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/append", g.append)
	mux.HandleFunc("/internal/v1/game-logs/{roomId}/deck-operations", g.deckOps)
	return mux
}

func (g *giDouble) auth(r *http.Request) bool {
	return r.Header.Get("X-Service-Credential") == g.cred
}

func (g *giDouble) append(w http.ResponseWriter, r *http.Request) {
	if !g.auth(r) {
		writeErr(w, 401, "unauthorized", "invalid service credential")
		return
	}
	var body struct {
		GameID           string          `json:"gameId"`
		EventID          string          `json:"eventId"`
		ExpectedRevision int64           `json:"expectedRevision"`
		EventType        string          `json:"eventType"`
		Payload          json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad_request", "invalid json")
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if body.ExpectedRevision != g.revision {
		writeErr(w, 409, "revision_mismatch", "expectedRevision does not match")
		return
	}
	g.events = append(g.events, map[string]any{
		"eventId": body.EventID, "eventType": body.EventType, "gameId": body.GameID,
	})
	off := g.revision
	g.revision++
	writeJSON(w, 200, map[string]any{"kind": "accepted", "logOffset": off, "revision": g.revision})
}

func (g *giDouble) deckOps(w http.ResponseWriter, r *http.Request) {
	if !g.auth(r) {
		writeErr(w, 401, "unauthorized", "invalid service credential")
		return
	}
	roomID := r.PathValue("roomId")
	var body struct {
		Operation     string   `json:"operation"`
		GameID        string   `json:"gameId"`
		Seed          string   `json:"seed"`
		Cards         []any    `json:"cards"`
		OperationID   string   `json:"operationId"`
		ReservationID string   `json:"reservationId"`
		Seats         []string `json:"seats"`
		Count         int      `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad_request", "invalid json")
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	key := roomID + "/" + body.GameID
	dk := g.decks[key]
	if dk == nil {
		dk = &giDeck{pending: map[string]giPending{}, byOp: map[string]string{}, confirmedID: map[string]giPending{}}
		g.decks[key] = dk
	}
	switch body.Operation {
	case "initialize":
		if body.Seed != "" || len(body.Cards) > 0 {
			writeErr(w, 400, "invalid_command", "caller must not supply seed or cards")
			return
		}
		if dk.inited {
			writeErr(w, 409, "conflicting_duplicate", "deck already initialized")
			return
		}
		dk.inited = true
		writeJSON(w, 200, map[string]any{"kind": "accepted", "remaining": 108, "seedCommitment": "abc"})
	case "reserve_deal":
		if !dk.inited {
			writeErr(w, 400, "invalid_command", "deck not initialized")
			return
		}
		shape := "deal|" + strings.Join(body.Seats, ",") + "|7"
		id := detResID(roomID, body.GameID, body.OperationID, shape)
		if prior, ok := dk.byOp[body.OperationID]; ok {
			rem := 0
			if p, ok := dk.pending[prior]; ok {
				rem = p.remainingAfter
			} else if p, ok := dk.confirmedID[prior]; ok {
				rem = p.remainingAfter
			}
			writeJSON(w, 200, map[string]any{"kind": "duplicate", "reservationId": prior,
				"hands": map[string]any{}, "discardTop": card("d", "red", "1"),
				"activeColor": "red", "currentSeat": 0, "direction": "clockwise",
				"remaining": rem})
			return
		}
		if len(dk.pending) > 0 {
			writeErr(w, 409, "conflicting_duplicate", "deck has outstanding unconfirmed reservation")
			return
		}
		need := len(body.Seats)*7 + 1
		remaining := 108 - dk.pointer - need
		dk.pending[id] = giPending{opID: body.OperationID, kind: "deal", count: need, seats: body.Seats, remainingAfter: remaining}
		dk.byOp[body.OperationID] = id
		hands := map[string]any{}
		for _, s := range body.Seats {
			hand := make([]any, 7)
			for i := 0; i < 7; i++ {
				hand[i] = card(s+"-"+itoa(i), "red", "1")
			}
			hands[s] = hand
		}
		writeJSON(w, 200, map[string]any{
			"kind": "accepted", "reservationId": id, "hands": hands,
			"discardTop": card("top", "blue", "3"), "activeColor": "blue",
			"currentSeat": 0, "direction": "clockwise", "applyTopEffects": false,
			"remaining": remaining,
		})
	case "reserve_draw":
		if !dk.inited {
			writeErr(w, 400, "invalid_command", "deck not initialized")
			return
		}
		shape := "draw|" + itoa(body.Count)
		id := detResID(roomID, body.GameID, body.OperationID, shape)
		if prior, ok := dk.byOp[body.OperationID]; ok {
			rem := 0
			if p, ok := dk.pending[prior]; ok {
				rem = p.remainingAfter
			} else if p, ok := dk.confirmedID[prior]; ok {
				rem = p.remainingAfter
			}
			writeJSON(w, 200, map[string]any{"kind": "duplicate", "reservationId": prior, "cards": []any{}, "remaining": rem})
			return
		}
		if len(dk.pending) > 0 {
			writeErr(w, 409, "conflicting_duplicate", "deck has outstanding unconfirmed reservation")
			return
		}
		remaining := 108 - dk.pointer - body.Count
		dk.pending[id] = giPending{opID: body.OperationID, kind: "draw", count: body.Count, remainingAfter: remaining}
		dk.byOp[body.OperationID] = id
		cards := make([]any, body.Count)
		for i := 0; i < body.Count; i++ {
			cards[i] = card("draw-"+itoa(i), "yellow", "2")
		}
		writeJSON(w, 200, map[string]any{"kind": "accepted", "reservationId": id, "cards": cards, "remaining": remaining})
	case "confirm":
		if _, ok := dk.confirmedID[body.ReservationID]; ok {
			writeJSON(w, 200, map[string]any{"kind": "duplicate"})
			return
		}
		p, ok := dk.pending[body.ReservationID]
		if !ok {
			writeErr(w, 400, "invalid_command", "unknown reservation")
			return
		}
		dk.pointer += p.count
		dk.confirmedID[body.ReservationID] = p
		delete(dk.pending, body.ReservationID)
		// Keep byOp so exact-duplicate after confirm can resolve remainingAfter.
		writeJSON(w, 200, map[string]any{"kind": "accepted"})
	case "cancel":
		if p, ok := dk.pending[body.ReservationID]; ok {
			delete(dk.pending, body.ReservationID)
			delete(dk.byOp, p.opID)
		}
		writeJSON(w, 200, map[string]any{"kind": "accepted"})
	default:
		writeErr(w, 400, "bad_request", "unknown operation")
	}
}

func card(id, color, face string) map[string]string {
	return map[string]string{"id": id, "color": color, "face": face}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"code": code, "message": msg})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestHTTPGameIntegrityAppendCompatibility(t *testing.T) {
	const cred = "room-cred"
	dbl := newGIDouble(cred)
	srv := httptest.NewServer(dbl.handler())
	defer srv.Close()

	gi := app.NewHTTPGameIntegrity(srv.URL, cred, srv.Client())
	res, err := gi.Append(context.Background(), app.AppendRequest{
		RoomID: "room-1", EventID: "evt-1", ExpectedRevision: 0,
		EventType: "CreateRoom", Payload: []byte(`{"host":"h"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.LogOffset != 0 || res.Revision != 1 {
		t.Fatalf("%+v", res)
	}

	// Empty gameId lifecycle append.
	res, err = gi.Append(context.Background(), app.AppendRequest{
		RoomID: "room-1", EventID: "evt-2", ExpectedRevision: 1,
		EventType: "JoinRoom", Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Revision != 2 {
		t.Fatalf("%+v", res)
	}

	// Stale revision surfaces as error.
	_, err = gi.Append(context.Background(), app.AppendRequest{
		RoomID: "room-1", EventID: "evt-3", ExpectedRevision: 0,
		EventType: "PlayCard", Payload: []byte(`{}`),
	})
	if err == nil || !strings.Contains(err.Error(), "revision_mismatch") {
		t.Fatalf("want revision_mismatch, got %v", err)
	}

	// Wrong credential.
	bad := app.NewHTTPGameIntegrity(srv.URL, "wrong", srv.Client())
	_, err = bad.Append(context.Background(), app.AppendRequest{
		RoomID: "room-1", EventID: "x", ExpectedRevision: 2, EventType: "PlayCard",
	})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want 401, got %v", err)
	}
}

func TestHTTPDealSourceReservationCompatibility(t *testing.T) {
	const cred = "room-cred"
	dbl := newGIDouble(cred)
	srv := httptest.NewServer(dbl.handler())
	defer srv.Close()

	deals := app.NewHTTPDealSource(srv.URL, cred, srv.Client())
	ctx := context.Background()

	res, err := deals.ReserveDeal(ctx, "room-a", "game-1", "cmd-deal:deal", []string{"p1", "p2"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deal == nil || res.Deal.DiscardTop.ID == "" {
		t.Fatalf("%+v", res)
	}
	if len(res.Deal.Hands["p1"]) != 7 || res.Deal.DiscardTop.ID == "" {
		t.Fatalf("deal material: %+v", res.Deal)
	}
	if res.DrawPileSize != 93 || !res.Deal.HasDrawPileSize || res.Deal.DrawPileSize != 93 {
		t.Fatalf("deal remaining=%d deal=%+v", res.DrawPileSize, res.Deal)
	}
	if res.Deal.Direction == 0 {
		t.Fatal("direction not mapped")
	}

	if err := deals.Confirm(ctx, res.ID); err != nil {
		t.Fatal(err)
	}

	draw, err := deals.ReserveDraw(ctx, "room-a", "game-1", "cmd-draw:draw", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(draw.Cards) != 2 {
		t.Fatalf("cards: %+v", draw.Cards)
	}
	if draw.DrawPileSize != 91 {
		t.Fatalf("draw remaining=%d want 91", draw.DrawPileSize)
	}
	if err := deals.Cancel(ctx, draw.ID); err != nil {
		t.Fatal(err)
	}

	// Re-init conflict path is tolerated by ensureDeck.
	res2, err := deals.ReserveDeal(ctx, "room-a", "game-1", "cmd-deal2:deal", []string{"p1", "p2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := deals.Cancel(ctx, res2.ID); err != nil {
		t.Fatal(err)
	}
}
