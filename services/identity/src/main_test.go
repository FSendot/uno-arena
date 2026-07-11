package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"unoarena/services/identity/domain"
)

type testIDs struct{ n int }

func (s *testIDs) NewID(prefix string) string {
	s.n++
	return prefix + "-" + itoa(s.n)
}

func (s *testIDs) NewToken() (string, error) {
	s.n++
	return "tok-" + itoa(s.n), nil
}

func (s *testIDs) NewSalt() ([]byte, error) {
	s.n++
	salt := make([]byte, 16)
	for i := range salt {
		salt[i] = byte(s.n + i)
	}
	return salt, nil
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

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

func newTestServer(t *testing.T) (*Server, *fixedClock) {
	t.Helper()
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
		Transport:  domain.NewFakeInvalidationTransport(),
	})
	return NewServer(svc, "test-internal-credential", true), clock
}

func TestHealthHandler(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp["status"] != "ok" || resp["service"] != "identity" {
		t.Fatalf("unexpected health: %+v", resp)
	}
}

func TestRegisterLoginWhoamiFlow(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "alice", "password": "pass"})
	regReq := httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody))
	regW := httptest.NewRecorder()
	mux.ServeHTTP(regW, regReq)
	if regW.Code != http.StatusOK {
		t.Fatalf("register: expected 200, got %d body=%s", regW.Code, regW.Body.String())
	}
	var regResp map[string]string
	_ = json.NewDecoder(regW.Body).Decode(&regResp)
	if regResp["status"] != "ok" || regResp["username"] != "alice" {
		t.Fatalf("register response: %+v", regResp)
	}

	loginBody, _ := json.Marshal(map[string]string{"username": "alice", "password": "pass"})
	loginReq := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody))
	loginW := httptest.NewRecorder()
	mux.ServeHTTP(loginW, loginReq)
	if loginW.Code != http.StatusOK {
		t.Fatalf("login: expected 200, got %d body=%s", loginW.Code, loginW.Body.String())
	}
	var loginResp map[string]string
	_ = json.NewDecoder(loginW.Body).Decode(&loginResp)
	if loginResp["token"] == "" || loginResp["sessionId"] == "" || loginResp["playerId"] == "" {
		t.Fatalf("login response: %+v", loginResp)
	}

	whoReq := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	whoReq.Header.Set("Authorization", "Bearer "+loginResp["token"])
	whoW := httptest.NewRecorder()
	mux.ServeHTTP(whoW, whoReq)
	if whoW.Code != http.StatusOK {
		t.Fatalf("whoami: expected 200, got %d body=%s", whoW.Code, whoW.Body.String())
	}
	var whoResp map[string]string
	_ = json.NewDecoder(whoW.Body).Decode(&whoResp)
	if whoResp["username"] != "alice" || whoResp["sessionId"] != loginResp["sessionId"] || whoResp["playerId"] != loginResp["playerId"] {
		t.Fatalf("whoami response: %+v", whoResp)
	}
}

func TestLoginTakeoverInvalidatesPriorSessionHTTP(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "bob", "password": "pass"})
	regReq := httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody))
	regW := httptest.NewRecorder()
	mux.ServeHTTP(regW, regReq)
	if regW.Code != http.StatusOK {
		t.Fatalf("register: %d", regW.Code)
	}

	login := func() map[string]string {
		body, _ := json.Marshal(map[string]string{"username": "bob", "password": "pass"})
		req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("login: %d %s", w.Code, w.Body.String())
		}
		var resp map[string]string
		_ = json.NewDecoder(w.Body).Decode(&resp)
		return resp
	}

	first := login()
	second := login()

	oldWho := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	oldWho.Header.Set("Authorization", "Bearer "+first["token"])
	oldW := httptest.NewRecorder()
	mux.ServeHTTP(oldW, oldWho)
	if oldW.Code != http.StatusUnauthorized {
		t.Fatalf("old session should be 401, got %d", oldW.Code)
	}

	newWho := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	newWho.Header.Set("Authorization", "Bearer "+second["token"])
	newW := httptest.NewRecorder()
	mux.ServeHTTP(newW, newWho)
	if newW.Code != http.StatusOK {
		t.Fatalf("new session should be 200, got %d", newW.Code)
	}
}

func TestValidateSessionInternal(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "carol", "password": "pass"})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody)))

	loginBody, _ := json.Marshal(map[string]string{"username": "carol", "password": "pass"})
	loginW := httptest.NewRecorder()
	mux.ServeHTTP(loginW, httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody)))
	var loginResp map[string]string
	_ = json.NewDecoder(loginW.Body).Decode(&loginResp)

	valReq := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
	valReq.Header.Set("Authorization", "Bearer "+loginResp["token"])
	valReq.Header.Set(internalCredentialHeader, "test-internal-credential")
	valW := httptest.NewRecorder()
	mux.ServeHTTP(valW, valReq)
	if valW.Code != http.StatusOK {
		t.Fatalf("validate: expected 200, got %d body=%s", valW.Code, valW.Body.String())
	}
	var valResp map[string]any
	_ = json.NewDecoder(valW.Body).Decode(&valResp)
	if valResp["playerId"] != loginResp["playerId"] || valResp["sessionId"] != loginResp["sessionId"] {
		t.Fatalf("validate response: %+v", valResp)
	}
}

func TestValidateSessionBindingInternal_SessionIDNotBearer(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "bind-carol", "password": "pass"})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody)))
	loginBody, _ := json.Marshal(map[string]string{"username": "bind-carol", "password": "pass"})
	loginW := httptest.NewRecorder()
	mux.ServeHTTP(loginW, httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody)))
	var loginResp map[string]string
	_ = json.NewDecoder(loginW.Body).Decode(&loginResp)

	body, _ := json.Marshal(map[string]string{
		"sessionId": loginResp["sessionId"],
		"playerId":  loginResp["playerId"],
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalCredentialHeader, "test-internal-credential")
	// Intentionally no Authorization — sessionId must not be required as Bearer.
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("binding validate: expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var valResp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&valResp)
	if valResp["playerId"] != loginResp["playerId"] || valResp["sessionId"] != loginResp["sessionId"] {
		t.Fatalf("binding response: %+v", valResp)
	}

	// Presenting sessionId as Bearer token must not authorize (token ≠ sessionId).
	bad := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
	bad.Header.Set("Authorization", "Bearer "+loginResp["sessionId"])
	bad.Header.Set(internalCredentialHeader, "test-internal-credential")
	badW := httptest.NewRecorder()
	mux.ServeHTTP(badW, bad)
	if badW.Code != http.StatusUnauthorized {
		t.Fatalf("sessionId-as-bearer must 401, got %d", badW.Code)
	}
}

func TestInternalValidateRequiresCredential(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "carol", "password": "pass"})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody)))
	loginBody, _ := json.Marshal(map[string]string{"username": "carol", "password": "pass"})
	loginW := httptest.NewRecorder()
	mux.ServeHTTP(loginW, httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody)))
	var loginResp map[string]string
	_ = json.NewDecoder(loginW.Body).Decode(&loginResp)

	t.Run("missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
		req.Header.Set("Authorization", "Bearer "+loginResp["token"])
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
	t.Run("wrong", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
		req.Header.Set("Authorization", "Bearer "+loginResp["token"])
		req.Header.Set(internalCredentialHeader, "wrong-secret")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
}

func TestInternalInvalidateRequiresCredential(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()

	regBody, _ := json.Marshal(map[string]string{"username": "frank", "password": "pass"})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody)))
	loginBody, _ := json.Marshal(map[string]string{"username": "frank", "password": "pass"})
	loginW := httptest.NewRecorder()
	mux.ServeHTTP(loginW, httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody)))
	var loginResp map[string]string
	_ = json.NewDecoder(loginW.Body).Decode(&loginResp)
	sessionID := loginResp["sessionId"]

	t.Run("missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/"+sessionID+"/invalidate", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
	t.Run("wrong", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/"+sessionID+"/invalidate", nil)
		req.Header.Set(internalCredentialHeader, "nope")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
	t.Run("ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/"+sessionID+"/invalidate", nil)
		req.Header.Set(internalCredentialHeader, "test-internal-credential")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
		}
	})
}

func TestInternalEndpointsFailClosedWithoutConfiguredCredential(t *testing.T) {
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
	srv := NewServer(svc, "", false) // production-like default: empty credential rejects
	mux := srv.routes()

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/sessions/validate", nil)
	req.Header.Set(internalCredentialHeader, "anything")
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("fail-closed expected 401, got %d", w.Code)
	}
}

func TestWhoamiUnauthorizedWithoutToken(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestRegisterAliasPath(t *testing.T) {
	srv, _ := newTestServer(t)
	body, _ := json.Marshal(map[string]string{"username": "dave", "password": "pass"})
	req := httptest.NewRequest(http.MethodPost, "/register", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestLoginBadPassword(t *testing.T) {
	srv, _ := newTestServer(t)
	mux := srv.routes()
	regBody, _ := json.Marshal(map[string]string{"username": "eve", "password": "pass"})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/auth/register", bytes.NewReader(regBody)))

	loginBody, _ := json.Marshal(map[string]string{"username": "eve", "password": "nope"})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/auth/login", bytes.NewReader(loginBody)))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}
