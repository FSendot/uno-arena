package bff

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

func TestHTTPRoomClientPreservesRuntimeRetryStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/commands", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, `{"code":"room_starting"}`, http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := NewHTTPRoomClient(HTTPClientConfig{BaseURL: srv.URL})
	_, err := client.SubmitCommand(context.Background(), CommandDispatch{
		Command:   envelope.Command{CommandID: "c1", Type: "PlayCard", SchemaVersion: 1, Payload: json.RawMessage(`{"roomId":"r1"}`)},
		Principal: Principal{PlayerID: "p1"}, RoomID: "r1",
	})
	he, ok := err.(*httpStatusError)
	if !ok || he.status != http.StatusServiceUnavailable || he.retryAfter != "1" {
		t.Fatalf("runtime error = %#v", err)
	}
}

func TestHTTPRoomClientPlayerSnapshotPreservesRoomStartingRetry(t *testing.T) {
	type observedRequest struct {
		playerHeader      string
		playerQuery       string
		serviceCredential string
	}
	observed := make(chan observedRequest, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- observedRequest{
			playerHeader:      r.Header.Get("X-Player-Id"),
			playerQuery:       r.URL.Query().Get("playerId"),
			serviceCredential: r.Header.Get(headerServiceCredential),
		}
		w.Header().Set("Retry-After", "3")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"room_starting","message":"runtime pending"}`))
	}))
	defer srv.Close()

	client := NewHTTPRoomClient(HTTPClientConfig{
		BaseURL: srv.URL, ServiceCredential: "gateway-room-secret",
	})
	_, err := client.PlayerSnapshot(context.Background(), "room-1", "player-1", correlation.Headers{})
	he, ok := err.(*httpStatusError)
	if !ok || he.status != http.StatusServiceUnavailable || he.retryAfter != "3" {
		t.Fatalf("snapshot runtime error = %#v", err)
	}
	request := <-observed
	if request.playerHeader != "player-1" || request.playerQuery != "" || request.serviceCredential != "gateway-room-secret" {
		t.Fatalf("snapshot identity header=%q query=%q serviceCredential=%q",
			request.playerHeader, request.playerQuery, request.serviceCredential)
	}
}

func TestGatewayPlayerSnapshotPreservesRoomStartingResponse(t *testing.T) {
	identity := NewFakeIdentity()
	identity.SeedSession("token-1", Principal{PlayerID: "player-1", SessionID: "session-1"})
	room := NewFakeRoom()
	room.SnapshotErr = &httpStatusError{
		status:     http.StatusServiceUnavailable,
		body:       `{"code":"room_starting","message":"runtime pending"}`,
		retryAfter: "3",
	}
	server := NewServer(Dependencies{
		Identity: identity, Room: room, Tournament: NewFakeTournament(),
		Reads: &FakeReads{}, Audit: NewMemoryAudit(), Spectator: NewFakeSpectatorGate(),
		Hub: NewHub(), Ready: true,
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/rooms/room-1/snapshot", nil)
	req.Header.Set("Authorization", "Bearer token-1")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("Retry-After=%q want 3", got)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["code"] != "room_starting" {
		t.Fatalf("body=%v", body)
	}
}
