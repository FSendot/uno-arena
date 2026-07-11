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
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

// Server wires HTTP handlers to the Identity domain service.
type Server struct {
	svc                *domain.Service
	internalCredential string
	invalidationReady  bool
}

func NewServer(svc *domain.Service, internalCredential string, invalidationReady bool) *Server {
	return &Server{
		svc:                svc,
		internalCredential: internalCredential,
		invalidationReady:  invalidationReady,
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)

	// BFF-aligned auth surface (offline Identity exposes the proxied shapes).
	mux.HandleFunc("/v1/auth/register", s.registerHandler)
	mux.HandleFunc("/v1/auth/login", s.loginHandler)
	mux.HandleFunc("/v1/auth/whoami", s.whoamiHandler)

	// Checkpoint-era aliases (same handlers / response shapes where applicable).
	mux.HandleFunc("/register", s.registerHandler)
	mux.HandleFunc("/login", s.loginHandler)
	mux.HandleFunc("/whoami", s.whoamiHandler)

	mux.HandleFunc("/internal/v1/sessions/validate", s.validateSessionHandler)
	mux.HandleFunc("/internal/v1/sessions/", s.invalidateSessionHandler)
	return mux
}

func (s *Server) authorizeInternal(r *http.Request) bool {
	// Fail-closed: empty configured credential rejects all internal callers.
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
	// Fail-closed: require configured invalidation transport (IDENTITY_INVALIDATION_URL + credential).
	if !s.invalidationReady {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "identity",
			"reason":  "invalidation_transport_unconfigured",
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
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	player, err := s.svc.Register(body.Username, body.Password)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrInvalidInput):
			writeError(w, r, http.StatusBadRequest, "bad_request", "username and password required")
		case errors.Is(err, domain.ErrUsernameTaken):
			writeError(w, r, http.StatusBadRequest, "username_taken", "username already registered")
		default:
			writeError(w, r, http.StatusInternalServerError, "internal_error", "register failed")
		}
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
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	res, err := s.svc.Login(body.Username, body.Password)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidCredentials) {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid credentials")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "internal_error", "login failed")
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
	token, ok := bearerToken(r)
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}
	who, err := s.svc.Whoami(token)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid session")
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
		// Room forwards durable sessionId+playerId under the service credential.
		// Do not treat sessionId as an opaque bearer token.
		principal, err = s.svc.ValidateSessionBinding(domain.SessionID(body.SessionID), domain.PlayerID(body.PlayerID))
	default:
		token, ok := bearerToken(r)
		if !ok {
			token = body.Token
		}
		if token == "" {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing session token")
			return
		}
		principal, err = s.svc.ValidateToken(token)
	}
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid session")
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

func (s *Server) invalidateSessionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	// /internal/v1/sessions/{sessionId}/invalidate
	path := strings.TrimPrefix(r.URL.Path, "/internal/v1/sessions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "invalidate" || parts[0] == "" {
		writeError(w, r, http.StatusNotFound, "not_found", "not found")
		return
	}
	if err := s.svc.RevokeSession(domain.SessionID(parts[0]), domain.ReasonAdmin); err != nil {
		if errors.Is(err, domain.ErrSessionNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "session not found")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "internal_error", "invalidate failed")
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

// newDefaultService wires durable outbox sessions plus a real invalidation transport.
// When IDENTITY_INVALIDATION_URL and credential are set, uses stdlib HTTP to the Gateway
// control endpoint. Otherwise uses a durable in-memory dispatcher (never a dropping Noop)
// and reports not-ready so production cannot silently lose invalidation fan-out.
func newDefaultService(invalidationURL, credential string) (*domain.Service, *domain.MemorySessionRepository, bool) {
	ids := domain.RandomIDGenerator{}
	sessions := domain.NewMemorySessionRepository()
	var transport domain.InvalidationTransport
	ready := false
	if invalidationURL != "" && credential != "" {
		transport = NewHTTPInvalidationTransport(invalidationURL, credential, nil)
		ready = true
	} else {
		transport = domain.NewMemoryInvalidationTransport()
	}
	svc := domain.NewService(domain.ServiceDeps{
		Players:    domain.NewMemoryPlayerRepository(),
		Sessions:   sessions,
		Hasher:     domain.NewCheckpointPasswordHasher(ids),
		IDs:        ids,
		Tokens:     ids,
		Clock:      domain.SystemClock{},
		SessionTTL: 24 * time.Hour,
		Transport:  transport,
	})
	return svc, sessions, ready
}

// defaultOutboxRetryConfig is used when the HTTP publisher is configured.
func defaultOutboxRetryConfig() OutboxRetryConfig {
	return OutboxRetryConfig{
		InitialInterval: time.Second,
		MaxInterval:     30 * time.Second,
		DrainLimit:      32,
	}
}

func main() {
	// Fail-closed when unset: internal routes reject all callers.
	cred := os.Getenv("IDENTITY_INTERNAL_CREDENTIAL")
	invalidationURL := os.Getenv("IDENTITY_INVALIDATION_URL")
	svc, _, ready := newDefaultService(invalidationURL, cred)
	srv := NewServer(svc, cred, ready)

	var worker *OutboxRetryWorker
	if ready {
		worker = NewOutboxRetryWorker(svc, defaultOutboxRetryConfig())
		worker.Start()
	}

	httpSrv := &http.Server{Addr: ":8080", Handler: srv.routes()}
	errCh := make(chan error, 1)
	go func() {
		log.Printf(`{"level":"info","service":"identity","event":"startup","invalidationReady":%t}`, ready)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}
