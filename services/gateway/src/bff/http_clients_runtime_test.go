package bff

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
