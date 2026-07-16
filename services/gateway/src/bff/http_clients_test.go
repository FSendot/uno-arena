package bff_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unoarena/services/gateway/bff"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
	"unoarena/shared/internalprincipal"
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
	if p.PlayerID != "p1" || p.SessionID != "s1" || p.InternalProof != "" {
		t.Fatalf("%+v", p)
	}
}

func TestHTTPIdentityClient_AuthorizeRoomCommandSendsExactDecodedBinding(t *testing.T) {
	seq := int64(0)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/sessions/authorize-command" {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer session-token" || r.Header.Get("X-Service-Credential") != "identity-credential" {
			t.Fatalf("auth headers=%v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["boundedContext"] != internalprincipal.DefaultRoomAudience || body["commandId"] != "cmd-1" || body["type"] != "PlayCard" || body["roomId"] != "room-route" || body["expectedSequenceNumber"] != float64(0) {
			t.Fatalf("binding body=%#v", body)
		}
		payload := body["payload"].(map[string]any)
		if payload["roomId"] != "room-route" || payload["cardId"] != "c1" {
			t.Fatalf("payload=%#v", payload)
		}
		response := `{"playerId":"p1","sessionId":"s1","roles":["player"],"internalPrincipalProof":"proof-1"}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
	})
	client := bff.NewHTTPIdentityClient(bff.HTTPClientConfig{BaseURL: "http://identity.test", ServiceCredential: "identity-credential", HTTPClient: &http.Client{Transport: transport}})
	p, err := client.AuthorizeCommand(context.Background(), "session-token", bff.CommandAuthorization{
		BoundedContext: internalprincipal.DefaultRoomAudience,
		RoomID:         "room-route",
		Command:        envelope.Command{CommandID: "cmd-1", Type: "PlayCard", SchemaVersion: 1, ExpectedSequenceNumber: &seq, Payload: json.RawMessage(`{"roomId":"room-route","cardId":"c1"}`)},
	}, correlation.Headers{CorrelationID: "corr-1"})
	if err != nil {
		t.Fatal(err)
	}
	if p.InternalProof != "proof-1" {
		t.Fatalf("principal=%+v", p)
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
		if got := r.Header.Get(internalprincipal.HeaderName); got != "proof-1" {
			t.Fatalf("internal proof=%q", got)
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Fatalf("bearer must not be forwarded: %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if _, leaked := body["internalPrincipalProof"]; leaked {
			t.Fatal("internal proof must be header-only")
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
		Principal:   bff.Principal{PlayerID: "p1", SessionID: "s1", InternalProof: "proof-1"},
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
		if got := r.Header.Get(internalprincipal.HeaderName); got != "" {
			t.Fatalf("Room-audience principal leaked to Tournament: %q", got)
		}
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
		Principal:   bff.Principal{PlayerID: "p1", SessionID: "s1", InternalProof: "room-only-proof"},
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
