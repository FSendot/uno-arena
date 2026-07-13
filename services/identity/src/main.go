package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"unoarena/services/identity/domain"
	"unoarena/services/identity/oidc"
	"unoarena/services/identity/store"
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

// Server wires HTTP handlers to the Identity domain service.
type Server struct {
	svc                *domain.Service
	internalCredential string
	invalidationReady  bool
	durableReady       func(context.Context) error
	allowPasswordLogin bool
	allowRegister      bool
	oidc               *oidc.Validator
	mode               string
	readyReason        string
}

// NewServer builds a capability-mode HTTP server (tests / checkpoint defaults).
func NewServer(svc *domain.Service, internalCredential string, invalidationReady bool) *Server {
	return serverFromRuntime(identityRuntime{
		svc:                svc,
		mode:               "capability",
		ready:              invalidationReady,
		allowPasswordLogin: true,
		allowRegister:      true,
	}, internalCredential)
}

func serverFromRuntime(rt identityRuntime, cred string) *Server {
	s := &Server{
		svc:                rt.svc,
		internalCredential: cred,
		invalidationReady:  rt.ready,
		allowPasswordLogin: rt.allowPasswordLogin,
		allowRegister:      rt.allowRegister,
		oidc:               rt.oidc,
		mode:               rt.mode,
		readyReason:        rt.readyReason,
	}
	if rt.mode == "durable" && rt.pool != nil {
		exp := rt.schemaExp
		pool := rt.pool
		s.durableReady = func(ctx context.Context) error {
			return store.VerifySchema(ctx, pool.Pool, exp)
		}
	}
	return s
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)

	mux.HandleFunc("/v1/auth/register", s.registerHandler)
	mux.HandleFunc("/v1/auth/login", s.loginHandler)
	mux.HandleFunc("/v1/auth/whoami", s.whoamiHandler)
	mux.HandleFunc("/v1/auth/oidc/token", s.oidcPublicTokenHandler)
	mux.HandleFunc("/internal/v1/auth/oidc/exchange", s.oidcInternalExchangeHandler)

	mux.HandleFunc("/register", s.registerHandler)
	mux.HandleFunc("/login", s.loginHandler)
	mux.HandleFunc("/whoami", s.whoamiHandler)

	mux.HandleFunc("/internal/v1/sessions/validate", s.validateSessionHandler)
	mux.HandleFunc("/internal/v1/sessions/logout", s.logoutSessionHandler)
	mux.HandleFunc("/internal/v1/sessions/", s.invalidateSessionHandler)
	return mux
}

func (s *Server) requireService(w http.ResponseWriter, r *http.Request) bool {
	if s.svc != nil {
		return true
	}
	writeError(w, r, http.StatusServiceUnavailable, "unavailable", "identity unavailable")
	return false
}

func (s *Server) authorizeInternal(r *http.Request) bool {
	if s.internalCredential == "" {
		return false
	}
	got := r.Header.Get(internalCredentialHeader)
	if got == "" {
		return false
	}
	if len(got) != len(s.internalCredential) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.internalCredential)) == 1
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "identity"})
	logRequest(r, "/health")
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.durableReady != nil {
		if err := s.durableReady(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status":  "not_ready",
				"service": "identity",
				"reason":  "schema_or_db",
			})
			logRequest(r, "/ready")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "identity"})
		logRequest(r, "/ready")
		return
	}
	if !s.invalidationReady {
		reason := s.readyReason
		if reason == "" {
			reason = "invalidation_transport_unconfigured"
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "identity",
			"reason":  reason,
		})
		logRequest(r, "/ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "identity"})
	logRequest(r, "/ready")
}

func (s *Server) registerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	if !s.allowRegister {
		writeError(w, r, http.StatusForbidden, "register_disabled", "registration disabled outside capability/test provisioning")
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	player, err := s.svc.Register(r.Context(), body.Username, body.Password)
	if err != nil {
		status, code, msg := mapDomainHTTP(err)
		if status == 500 {
			msg = "register failed"
		}
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "ok",
		"username": player.Username,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	if !s.allowPasswordLogin {
		writeError(w, r, http.StatusForbidden, "password_login_disabled", "password login disabled; use OIDC token exchange")
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}

	if s.oidc != nil && envTruthy("IDENTITY_OIDC_DIRECT_GRANT") {
		idToken, err := s.oidc.DirectGrant(r.Context(), body.Username, body.Password)
		if err != nil {
			status, code, msg := mapDomainHTTP(err)
			if status == http.StatusUnauthorized {
				msg = "invalid credentials"
			}
			writeError(w, r, status, code, msg)
			return
		}
		s.completeOIDCLogin(w, r, idToken, "")
		return
	}

	res, err := s.svc.Login(r.Context(), body.Username, body.Password)
	if err != nil {
		status, code, msg := mapDomainHTTP(err)
		if status == 500 {
			msg = "login failed"
		}
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"sessionId": res.SessionID.String(),
		"playerId":  res.PlayerID.String(),
		"token":     res.Token,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) oidcPublicTokenHandler(w http.ResponseWriter, r *http.Request) {
	s.oidcTokenHandler(w, r, false)
}

func (s *Server) oidcInternalExchangeHandler(w http.ResponseWriter, r *http.Request) {
	s.oidcTokenHandler(w, r, true)
}

func (s *Server) oidcTokenHandler(w http.ResponseWriter, r *http.Request, requireCredential bool) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if requireCredential && !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	if s.oidc == nil {
		writeError(w, r, http.StatusNotImplemented, "oidc_unconfigured", "OIDC validator not configured")
		return
	}
	var body struct {
		IDToken     string `json:"idToken"`
		AccessToken string `json:"accessToken"`
		Nonce       string `json:"nonce"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	tok := strings.TrimSpace(body.IDToken)
	if tok == "" {
		tok = strings.TrimSpace(body.AccessToken)
	}
	if tok == "" {
		writeError(w, r, http.StatusBadRequest, "bad_request", "idToken required")
		return
	}
	s.completeOIDCLogin(w, r, tok, body.Nonce)
}

func (s *Server) completeOIDCLogin(w http.ResponseWriter, r *http.Request, idToken, nonce string) {
	claims, err := s.oidc.ValidateIDToken(r.Context(), idToken, nonce)
	if err != nil {
		status, code, msg := mapDomainHTTP(err)
		writeError(w, r, status, code, msg)
		return
	}
	res, err := s.svc.EstablishOIDCSession(r.Context(), claims.Issuer, claims.Subject, claims.Username, claims.Roles)
	if err != nil {
		status, code, msg := mapDomainHTTP(err)
		if status == 500 {
			msg = "oidc session failed"
		}
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"sessionId": res.SessionID.String(),
		"playerId":  res.PlayerID.String(),
		"token":     res.Token,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) whoamiHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	token, ok := bearerToken(r)
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}
	who, err := s.svc.Whoami(r.Context(), token)
	if err != nil {
		status, code, msg := mapDomainHTTP(err)
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"playerId":  who.PlayerID.String(),
		"sessionId": who.SessionID.String(),
		"username":  who.Username,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) validateSessionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	var body struct {
		Token     string `json:"token"`
		SessionID string `json:"sessionId"`
		PlayerID  string `json:"playerId"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	body.Token = strings.TrimSpace(body.Token)
	body.SessionID = strings.TrimSpace(body.SessionID)
	body.PlayerID = strings.TrimSpace(body.PlayerID)

	var principal domain.SessionPrincipal
	var err error
	switch {
	case body.SessionID != "" && body.PlayerID != "":
		principal, err = s.svc.ValidateSessionBinding(r.Context(), domain.SessionID(body.SessionID), domain.PlayerID(body.PlayerID))
	default:
		token, ok := bearerToken(r)
		if !ok {
			token = body.Token
		}
		if token == "" {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing session token")
			return
		}
		principal, err = s.svc.ValidateToken(r.Context(), token)
	}
	if err != nil {
		status, code, msg := mapDomainHTTP(err)
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"playerId":  principal.PlayerID.String(),
		"sessionId": principal.SessionID.String(),
		"roles":     principal.Roles,
		"expiresAt": principal.ExpiresAt.UTC().Format(time.RFC3339),
	})
	logRequest(r, r.URL.Path)
}

// logoutSessionHandler is the Gateway-only session logout seam. It accepts the
// opaque UnoArena bearer solely to identify its hashed session; provider tokens
// and principal claims are never returned.
func (s *Server) logoutSessionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	token, ok := bearerToken(r)
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing session token")
		return
	}
	if err := s.svc.LogoutByToken(r.Context(), token); err != nil {
		status, code, msg := mapDomainHTTP(err)
		if status == http.StatusInternalServerError {
			msg = "logout failed"
		}
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	logRequest(r, r.URL.Path)
}

func (s *Server) invalidateSessionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if !s.requireService(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/internal/v1/sessions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "invalidate" || parts[0] == "" {
		writeError(w, r, http.StatusNotFound, "not_found", "not found")
		return
	}
	if err := s.svc.RevokeSession(r.Context(), domain.SessionID(parts[0]), domain.ReasonAdmin); err != nil {
		status, code, msg := mapDomainHTTP(err)
		if errors.Is(err, domain.ErrSessionNotFound) {
			status, code, msg = http.StatusNotFound, "not_found", "session not found"
		} else if status == 500 {
			msg = "invalidate failed"
		}
		writeError(w, r, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "sessionId": parts[0]})
	logRequest(r, r.URL.Path)
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	tok := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	return tok, tok != ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	body := map[string]string{
		"code":    code,
		"message": message,
	}
	if cid := r.Header.Get("X-Correlation-Id"); cid != "" {
		body["correlationId"] = cid
	}
	writeJSON(w, status, body)
}

func logRequest(r *http.Request, path string) {
	log.Printf(`{"level":"info","service":"identity","event":"request","path":%q,"correlationId":%q}`,
		path, r.Header.Get("X-Correlation-Id"))
}

// newDefaultService preserves checkpoint helper used by existing tests.
func newDefaultService(invalidationURL, credential string) (*domain.Service, *domain.MemorySessionRepository, bool) {
	return newCapabilityService(invalidationURL, credential)
}

func defaultOutboxRetryConfig() OutboxRetryConfig {
	return OutboxRetryConfig{
		InitialInterval: time.Second,
		MaxInterval:     30 * time.Second,
		DrainLimit:      32,
	}
}

func main() {
	cred := os.Getenv("IDENTITY_INTERNAL_CREDENTIAL")
	rt, err := wireIdentityRuntime()
	if err != nil {
		log.Fatalf("identity runtime: %v", err)
	}
	// Never silently convert misconfigured → memory. Memory only via
	// IDENTITY_CAPABILITY_MODE=true + non-prod inside wireIdentityRuntime.
	// Misconfigured serves /health and /ready=503 without auth panic.

	srv := serverFromRuntime(rt, cred)

	var worker *OutboxRetryWorker
	if rt.mode == "capability" && rt.ready {
		worker = NewOutboxRetryWorker(rt.svc, defaultOutboxRetryConfig())
		worker.Start()
	}

	httpSrv := &http.Server{Addr: ":8080", Handler: srv.routes()}
	errCh := make(chan error, 1)
	go func() {
		log.Printf(`{"level":"info","service":"identity","event":"startup","mode":%q,"ready":%t}`, rt.mode, rt.ready)
		errCh <- httpSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-sigCh:
	}

	if worker != nil {
		worker.Stop()
	}
	if rt.pool != nil {
		rt.pool.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}
