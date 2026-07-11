package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"unoarena/services/identity/domain"
	"unoarena/services/identity/oidc"
)

type unavailablePlayers struct{}

func (unavailablePlayers) FindByUsername(context.Context, string) (domain.Player, bool, error) {
	return domain.Player{}, false, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailablePlayers) FindByID(context.Context, domain.PlayerID) (domain.Player, bool, error) {
	return domain.Player{}, false, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailablePlayers) RegisterWithDefaultACL(context.Context, domain.Player, string) error {
	return domain.WrapUnavailable(errors.New("boom"))
}
func (unavailablePlayers) ListRoles(context.Context, domain.PlayerID) ([]string, error) {
	return nil, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailablePlayers) LinkOrResolveExternal(context.Context, string, string, string, domain.Player, []string) (domain.Player, []string, error) {
	return domain.Player{}, nil, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailablePlayers) Save(context.Context, domain.Player) error {
	return domain.WrapUnavailable(errors.New("boom"))
}

type unavailableSessions struct{}

func (unavailableSessions) FindByTokenHash(context.Context, string) (domain.Session, bool, error) {
	return domain.Session{}, false, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) FindByID(context.Context, domain.SessionID) (domain.Session, bool, error) {
	return domain.Session{}, false, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) FindActiveByPlayer(context.Context, domain.PlayerID) (domain.Session, bool, error) {
	return domain.Session{}, false, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) ReplaceActiveSession(context.Context, *domain.Session, domain.Session, func(domain.Session) domain.SessionInvalidatedEvent) error {
	return domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) InvalidateWithOutbox(context.Context, domain.Session, domain.SessionInvalidatedEvent) error {
	return domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) Save(context.Context, domain.Session) error {
	return domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) ListPendingOutbox(context.Context, int) ([]domain.OutboxEntry, error) {
	return nil, domain.WrapUnavailable(errors.New("boom"))
}
func (unavailableSessions) MarkOutboxPublished(context.Context, string, time.Time) error {
	return domain.WrapUnavailable(errors.New("boom"))
}

func newUnavailableServer(t *testing.T) *Server {
	t.Helper()
	clock := &fixedClock{now: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)}
	ids := &testIDs{}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    unavailablePlayers{},
		Sessions:   unavailableSessions{},
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      clock,
		SessionTTL: time.Hour,
		Transport:  domain.NewFakeInvalidationTransport(),
	})
	return NewServer(svc, "test-internal-credential", true)
}

func TestRegisterLoginWhoamiValidateStay503OnDurableErrors(t *testing.T) {
	srv := newUnavailableServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "a", "password": "p"})
	regW := httptest.NewRecorder()
	mux.ServeHTTP(regW, httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody)))
	if regW.Code != http.StatusServiceUnavailable {
		t.Fatalf("register: %d body=%s", regW.Code, regW.Body.String())
	}

	loginBody, _ := json.Marshal(map[string]string{"username": "a", "password": "p"})
	loginW := httptest.NewRecorder()
	mux.ServeHTTP(loginW, httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody)))
	if loginW.Code != http.StatusServiceUnavailable {
		t.Fatalf("login: %d body=%s", loginW.Code, loginW.Body.String())
	}

	whoW := httptest.NewRecorder()
	whoReq := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	whoReq.Header.Set("Authorization", "Bearer tok")
	mux.ServeHTTP(whoW, whoReq)
	if whoW.Code != http.StatusServiceUnavailable {
		t.Fatalf("whoami: %d body=%s", whoW.Code, whoW.Body.String())
	}

	valW := httptest.NewRecorder()
	valReq := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
	valReq.Header.Set("Authorization", "Bearer tok")
	valReq.Header.Set(internalCredentialHeader, "test-internal-credential")
	mux.ServeHTTP(valW, valReq)
	if valW.Code != http.StatusServiceUnavailable {
		t.Fatalf("validate: %d body=%s", valW.Code, valW.Body.String())
	}
}

func TestInternalOIDCExchangeRequiresServiceCredential(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	body, _ := json.Marshal(map[string]string{"idToken": "x"})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/auth/oidc/exchange", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing credential: expected 401, got %d body=%s", w.Code, w.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/auth/oidc/exchange", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(internalCredentialHeader, "wrong")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("wrong credential: expected 401, got %d", w2.Code)
	}

	req3 := httptest.NewRequest(http.MethodPost, "/internal/v1/auth/oidc/exchange", bytes.NewReader(body))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set(internalCredentialHeader, "test-internal-credential")
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)
	if w3.Code != http.StatusNotImplemented {
		t.Fatalf("valid credential with oidc nil: expected 501, got %d body=%s", w3.Code, w3.Body.String())
	}
}

func TestPublicOIDCTokenPathDoesNotRequireServiceCredential(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()
	body, _ := json.Marshal(map[string]string{"idToken": "x"})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/oidc/token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("public path without credential should reach oidc gate (501), got %d", w.Code)
	}
}

func TestAuthorizeInternalConstantTimeRejectsWrongCredential(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
	req.Header.Set(internalCredentialHeader, "test-internal-credentialx")
	if srv.authorizeInternal(req) {
		t.Fatal("expected reject")
	}
	req.Header.Set(internalCredentialHeader, "test-internal-credential")
	if !srv.authorizeInternal(req) {
		t.Fatal("expected accept")
	}
	req.Header.Set(internalCredentialHeader, "x")
	if srv.authorizeInternal(req) {
		t.Fatal("expected reject on length mismatch")
	}
}

func TestLoginOIDCDirectGrantMapsUnavailableVsInvalid(t *testing.T) {
	status, _, _ := mapDomainHTTP(oidc.ErrUnavailable)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("unavailable → 503, got %d", status)
	}
	status, _, _ = mapDomainHTTP(oidc.ErrInvalidToken)
	if status != http.StatusUnauthorized {
		t.Fatalf("invalid → 401, got %d", status)
	}
}
