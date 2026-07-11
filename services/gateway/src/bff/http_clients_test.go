package bff_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"unoarena/services/gateway/bff"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

func TestHTTPIdentityClient_ValidateSession(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") != "cred" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		if r.Header.Get(correlation.HeaderCorrelationID) != "c1" {
			http.Error(w, "corr", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"playerId":  "p1",
			"sessionId": "s1",
			"username":  "alice",
			"roles":     []string{"player"},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := bff.NewHTTPIdentityClient(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "cred",
		HTTPClient:        srv.Client(),
	})
	p, err := client.ValidateSession(context.Background(), "tok", correlation.Headers{CorrelationID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if p.PlayerID != "p1" || p.SessionID != "s1" {
		t.Fatalf("%+v", p)
	}
}

func TestHTTPRoomClient_SubmitCommand(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/commands", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(envelope.Accepted("cmd_1", "CreateRoom", nil, nil))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := bff.NewHTTPRoomClient(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "cred",
		HTTPClient:        srv.Client(),
	})
	res, err := client.SubmitCommand(context.Background(), bff.CommandDispatch{
		Command: envelope.Command{
			CommandID:     "cmd_1",
			Type:          "CreateRoom",
			SchemaVersion: 1,
			Payload:       json.RawMessage(`{}`),
		},
		Principal:   bff.Principal{PlayerID: "p1", SessionID: "s1"},
		Correlation: correlation.Headers{CorrelationID: "c1", CommandID: "cmd_1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", res)
	}
}
