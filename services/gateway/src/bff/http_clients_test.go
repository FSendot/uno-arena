package bff_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"unoarena/services/gateway/bff"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

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

func TestHTTPIdentityClient_MissingPrincipalIsUpstreamFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	client := bff.NewHTTPIdentityClient(bff.HTTPClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client()})
	_, err := client.ValidateSession(context.Background(), "tok", correlation.Headers{})
	if err == nil || errors.Is(err, bff.ErrUnauthorized) {
		t.Fatalf("missing principal must be an upstream failure, got %v", err)
	}
}

func TestHTTPIdentityClient_LogoutForwardsBearerCredentialAndCorrelation(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/sessions/logout" {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer session-token" {
			t.Fatalf("bearer=%q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Service-Credential") != "identity-credential" {
			t.Fatalf("credential=%q", r.Header.Get("X-Service-Credential"))
		}
		if r.Header.Get(correlation.HeaderCorrelationID) != "corr-logout" {
			t.Fatalf("correlation=%q", r.Header.Get(correlation.HeaderCorrelationID))
		}
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
	})

	client := bff.NewHTTPIdentityClient(bff.HTTPClientConfig{
		BaseURL:           "http://identity.test",
		ServiceCredential: "identity-credential",
		HTTPClient:        &http.Client{Transport: transport},
	})
	if err := client.Logout(context.Background(), "session-token", correlation.Headers{CorrelationID: "corr-logout"}); err != nil {
		t.Fatalf("logout: %v", err)
	}
}

func TestHTTPRoomClient_SubmitCommand(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/commands", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") != "room-cred" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(envelope.Accepted("cmd_1", "CreateRoom", nil, nil))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := bff.NewHTTPRoomClient(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "room-cred",
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

func TestHTTPTournamentClient_SubmitCommand_SendsServiceCredential(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/commands", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") != "tour-cred" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(envelope.Accepted("cmd_2", "CreateTournament", nil, nil))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := bff.NewHTTPTournamentClient(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "tour-cred",
		HTTPClient:        srv.Client(),
	})
	res, err := client.SubmitCommand(context.Background(), bff.CommandDispatch{
		Command: envelope.Command{
			CommandID:     "cmd_2",
			Type:          "CreateTournament",
			SchemaVersion: 1,
			Payload:       json.RawMessage(`{}`),
		},
		Principal:   bff.Principal{PlayerID: "p1", SessionID: "s1"},
		Correlation: correlation.Headers{CorrelationID: "c1", CommandID: "cmd_2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", res)
	}
}

func TestHTTPSpectatorGate_Admit_SendsServiceCredential(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/rooms/r1/spectator-admission", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") != "spec-cred" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"allowed": true})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	gate := bff.NewHTTPSpectatorGate(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "spec-cred",
		HTTPClient:        srv.Client(),
	})
	ok, reason, err := gate.Admit(context.Background(), bff.SpectatorAdmitRequest{
		RoomID:      "r1",
		Correlation: correlation.Headers{CorrelationID: "c1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || reason != "" {
		t.Fatalf("ok=%v reason=%q", ok, reason)
	}
}
