package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"unoarena/services/game-integrity/domain"
)

// mutatingFailRepo fails store mutations while preserving identity validation paths.
type mutatingFailRepo struct {
	*MemoryStreamRepository
	fail error
}

func (r *mutatingFailRepo) WithRoom(ctx context.Context, roomID domain.RoomID, fn func(*RoomState) error) error {
	if r.fail != nil {
		return r.fail
	}
	return r.MemoryStreamRepository.WithRoom(ctx, roomID, fn)
}

func (r *mutatingFailRepo) WithDeck(ctx context.Context, roomID domain.RoomID, gameID domain.GameID, create bool, fn func(*DeckState) error) error {
	if r.fail != nil {
		return r.fail
	}
	return r.MemoryStreamRepository.WithDeck(ctx, roomID, gameID, create, fn)
}

func TestMutationRoutes_StoreErrorsAreServerFailures(t *testing.T) {
	base := NewMemoryStreamRepository()
	repo := &mutatingFailRepo{MemoryStreamRepository: base, fail: errors.New("kurrent unavailable")}
	srv := NewServerWithAudit(repo, &MemoryAuditRecorder{}, testRoomCredential, testAuditCredential, "offline", "")
	mux := srv.routes()
	room := "room-store-err"
	headers := withRoomCred(nil)

	cases := []struct {
		name string
		path string
		body map[string]any
	}{
		{"append", "/internal/v1/game-logs/" + room + "/append", map[string]any{
			"eventId": "e1", "expectedRevision": 0, "eventType": "PlayCard",
		}},
		{"initialize", "/internal/v1/game-logs/" + room + "/deck-operations", map[string]any{
			"operation": "initialize", "gameId": "g1",
		}},
		{"reserve_deal", "/internal/v1/game-logs/" + room + "/deck-operations", map[string]any{
			"operation": "reserve_deal", "gameId": "g1", "operationId": "op-d",
			"seats": []string{"a", "b"}, "cardsPerHand": 1,
		}},
		{"reserve_draw", "/internal/v1/game-logs/" + room + "/deck-operations", map[string]any{
			"operation": "reserve_draw", "gameId": "g1", "operationId": "op-r", "count": 1,
		}},
		{"confirm", "/internal/v1/game-logs/" + room + "/deck-operations", map[string]any{
			"operation": "confirm", "gameId": "g1", "reservationId": "res-x",
		}},
		{"cancel", "/internal/v1/game-logs/" + room + "/deck-operations", map[string]any{
			"operation": "cancel", "gameId": "g1", "reservationId": "res-x",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := doJSON(t, mux, http.MethodPost, tc.path, tc.body, headers)
			if w.Code == http.StatusBadRequest {
				t.Fatalf("store error must not be 400: %d %s", w.Code, w.Body.String())
			}
			if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusInternalServerError {
				t.Fatalf("want 503 or 500, got %d %s", w.Code, w.Body.String())
			}
			body := decodeJSON(t, w)
			if body["code"] != "store_unavailable" && body["code"] != "store_error" {
				t.Fatalf("unexpected code %+v", body)
			}
		})
	}
}

func TestMutationRoutes_DomainRejectionRemainsClientError(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/room-x/append", map[string]any{
		"eventId": "e1", "expectedRevision": 0, "eventType": "CommandRejected",
	}, withRoomCred(nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("allowlist rejection must stay 400: %d %s", w.Code, w.Body.String())
	}
}
