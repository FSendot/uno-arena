package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

func TestHTTPInvalidationTransportPostsToControlEndpoint(t *testing.T) {
	var gotMethod, gotPath, gotCred, gotCT string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotCred = r.Header.Get(internalCredentialHeader)
		gotCT = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tr := NewHTTPInvalidationTransport(srv.URL, "svc-secret", srv.Client())
	evt := domain.SessionInvalidatedEvent{
		EventID:    "evt-1",
		PlayerID:   domain.PlayerID("player-1"),
		SessionID:  domain.SessionID("session-1"),
		Reason:     domain.ReasonLoginTakeover,
		OccurredAt: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}
	if err := tr.Deliver(evt); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method: %s", gotMethod)
	}
	wantPath := "/internal/v1/control/sessions/" + url.PathEscape("session-1") + "/invalidated"
	if gotPath != wantPath {
		t.Fatalf("path: got %q want %q", gotPath, wantPath)
	}
	if gotCred != "svc-secret" || gotCT != "application/json" {
		t.Fatalf("headers cred=%q ct=%q", gotCred, gotCT)
	}
	if gotBody["schemaVersion"] != float64(1) {
		t.Fatalf("schemaVersion: %+v", gotBody["schemaVersion"])
	}
	if gotBody["eventId"] != "evt-1" {
		t.Fatalf("eventId must preserve outbox id: %+v", gotBody["eventId"])
	}
	if gotBody["eventType"] != "SessionInvalidated" {
		t.Fatalf("eventType: %+v", gotBody["eventType"])
	}
	if gotBody["sessionId"] != "session-1" || gotBody["playerId"] != "player-1" || gotBody["reason"] != domain.ReasonLoginTakeover {
		t.Fatalf("body: %+v", gotBody)
	}
}

func TestHTTPInvalidationTransportOmitsEmptyReason(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tr := NewHTTPInvalidationTransport(srv.URL, "svc-secret", srv.Client())
	if err := tr.Deliver(domain.SessionInvalidatedEvent{
		EventID:   "evt-stable",
		SessionID: domain.SessionID("session-1"),
		PlayerID:  domain.PlayerID("player-1"),
	}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if _, ok := gotBody["reason"]; ok {
		t.Fatalf("empty reason must be omitted: %+v", gotBody)
	}
	if gotBody["eventId"] != "evt-stable" || gotBody["schemaVersion"] != float64(1) || gotBody["eventType"] != "SessionInvalidated" {
		t.Fatalf("body: %+v", gotBody)
	}
}

func TestHTTPInvalidationTransportPathEscapesSessionID(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sessionID := "sess/with space+weird"
	tr := NewHTTPInvalidationTransport(srv.URL, "svc-secret", srv.Client())
	if err := tr.Deliver(domain.SessionInvalidatedEvent{
		EventID:   "evt-1",
		SessionID: domain.SessionID(sessionID),
		PlayerID:  domain.PlayerID("p1"),
	}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	want := "/internal/v1/control/sessions/" + url.PathEscape(sessionID) + "/invalidated"
	if gotPath != want {
		t.Fatalf("path: got %q want %q", gotPath, want)
	}
}

func TestHTTPInvalidationTransportAcceptsBaseOrControlURL(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	evt := domain.SessionInvalidatedEvent{
		EventID:   "evt-1",
		SessionID: domain.SessionID("s1"),
		PlayerID:  domain.PlayerID("p1"),
	}
	for _, base := range []string{srv.URL, srv.URL + "/", srv.URL + "/internal/v1/control", srv.URL + "/internal/v1/control/"} {
		paths = nil
		tr := NewHTTPInvalidationTransport(base, "cred", srv.Client())
		if err := tr.Deliver(evt); err != nil {
			t.Fatalf("deliver base=%q: %v", base, err)
		}
		if len(paths) != 1 || !strings.HasSuffix(paths[0], "/internal/v1/control/sessions/s1/invalidated") {
			t.Fatalf("base=%q path=%v", base, paths)
		}
		if strings.Count(paths[0], "/internal/v1/control") != 1 {
			t.Fatalf("doubled control path for base=%q: %s", base, paths[0])
		}
	}
}

func TestHTTPInvalidationTransportFailsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	tr := NewHTTPInvalidationTransport(srv.URL, "svc-secret", srv.Client())
	err := tr.Deliver(domain.SessionInvalidatedEvent{
		EventID:   "evt-1",
		SessionID: domain.SessionID("s1"),
		PlayerID:  domain.PlayerID("p1"),
	})
	if err == nil {
		t.Fatal("expected error on non-2xx")
	}
}

func TestReadyFailsClosedWithoutInvalidationURL(t *testing.T) {
	clock := &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)}
	ids := &testIDs{}
	sessions := domain.NewMemorySessionRepository()
	svc := domain.NewService(domain.ServiceDeps{
		Players:    domain.NewMemoryPlayerRepository(),
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      clock,
		SessionTTL: time.Hour,
		Transport:  domain.NewMemoryInvalidationTransport(),
	})
	srv := NewServer(svc, "cred", false)
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when invalidation URL unset, got %d", w.Code)
	}
}

func TestReadyOKWhenInvalidationConfigured(t *testing.T) {
	clock := &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)}
	ids := &testIDs{}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    domain.NewMemoryPlayerRepository(),
		Sessions:   domain.NewMemorySessionRepository(),
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      clock,
		SessionTTL: time.Hour,
		Transport:  domain.NewMemoryInvalidationTransport(),
	})
	srv := NewServer(svc, "cred", true)
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNewDefaultServiceNeverUsesDroppingNoop(t *testing.T) {
	// Offline path: durable memory transport + outbox; not a dropping Noop.
	svc, sessions, ready := newDefaultService("http://127.0.0.1:9", "cred")
	if !ready {
		t.Fatal("expected ready when URL+credential configured")
	}
	if _, err := svc.Register("alice", "secret"); err != nil {
		t.Fatalf("register: %v", err)
	}
	first, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("first login: %v", err)
	}
	// Transport will fail (nothing listening); login must still commit outbox.
	second, err := svc.Login("alice", "secret")
	if err != nil {
		t.Fatalf("second login: %v", err)
	}
	if second.SessionID == first.SessionID {
		t.Fatal("expected new session")
	}
	pending, err := sessions.ListPendingOutbox(10)
	if err != nil || len(pending) == 0 {
		t.Fatalf("expected durable pending outbox after publish failure, pending=%d err=%v", len(pending), err)
	}
}

func TestNewDefaultServiceFailClosedReadinessWithoutURL(t *testing.T) {
	_, _, ready := newDefaultService("", "cred")
	if ready {
		t.Fatal("expected not ready when IDENTITY_INVALIDATION_URL unset")
	}
	_, _, ready = newDefaultService("http://gateway:8080", "")
	if ready {
		t.Fatal("expected not ready when credential unset")
	}
}
