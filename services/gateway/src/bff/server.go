package bff

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"unoarena/shared/audit"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
	"unoarena/shared/httpx"
)

type corrCtxKey struct{}

// UpstreamProbe actively checks a configured backend health endpoint.
type UpstreamProbe struct {
	Name string
	URL  string // full /health URL
	Do   func(ctx context.Context, method, url string) (int, error)
}

// Dependencies are injectable backends for the offline BFF.
type Dependencies struct {
	Identity                    IdentityClient
	Room                        RoomClient
	Tournament                  TournamentClient
	Reads                       ReadModelClient
	Spectator                   SpectatorGate
	Audit                       AuditSink
	Hub                         *Hub
	EdgeLimiter                 RateLimiter
	PrincipalLimiter            RateLimiter
	LiveFeed                    LiveFeed
	ServiceCredential           string // legacy/compat; outbound uses per-backend HTTP client credentials
	IdentityProducerCredential  string // inbound: Identity session-invalidation producer
	RoomProducerCredential      string // inbound: Room player-private / spectator-safe / room-terminal
	SpectatorProducerCredential string // inbound: Spectator projection / spectator-stream only
	Ready                       bool
	NotReadyReason              string
	RedisAdapterBlocked         bool
	RedisReady                  func(context.Context) error // pings every configured Redis client
	WorkerReady                 func(context.Context) error // kafka/pubsub lifecycle readiness
	SessionInvalidation         SessionInvalidationApplier  // durable Redis SI; nil in capability/fakes
	UpstreamProbes              []UpstreamProbe
	Clock                       func() time.Time
	NewID                       func(prefix string) string
}

// SessionInvalidationApplier persists durable invalidation before local Hub closure.
type SessionInvalidationApplier interface {
	Apply(ctx context.Context, eventID, sessionID, playerID, reason, correlationID string, occurredAt time.Time) (duplicate bool, err error)
}

// Server is the sole public HTTP/SSE edge (Realtime Gateway / BFF).
type Server struct {
	identity                    IdentityClient
	room                        RoomClient
	tournament                  TournamentClient
	reads                       ReadModelClient
	spectator                   SpectatorGate
	audit                       AuditSink
	hub                         *Hub
	edgeLimiter                 RateLimiter
	principalLimiter            RateLimiter
	liveFeed                    LiveFeed
	serviceCredential           string
	identityProducerCredential  string
	roomProducerCredential      string
	spectatorProducerCredential string
	ready                       bool
	notReadyReason              string
	redisAdapterBlocked         bool
	redisReady                  func(context.Context) error
	workerReady                 func(context.Context) error
	sessionInvalidation         SessionInvalidationApplier
	upstreamProbes              []UpstreamProbe
	clock                       func() time.Time
	newID                       func(prefix string) string
}

// NewServer wires a BFF server from injectable dependencies.
// Fakes are never auto-wired; callers (tests/demo) must pass them explicitly.
func NewServer(deps Dependencies) *Server {
	if deps.Hub == nil {
		deps.Hub = NewHub()
	}
	if deps.Audit == nil {
		deps.Audit = ClosedAudit{}
	}
	if deps.Clock == nil {
		deps.Clock = func() time.Time { return time.Now().UTC() }
	}
	if deps.NewID == nil {
		deps.NewID = defaultNewID
	}
	if deps.EdgeLimiter == nil {
		deps.EdgeLimiter = AllowAll{}
	}
	if deps.PrincipalLimiter == nil {
		deps.PrincipalLimiter = AllowAll{}
	}
	if deps.Identity == nil {
		deps.Identity = ClosedIdentity{}
	}
	if deps.Room == nil {
		deps.Room = ClosedRoom{}
	}
	if deps.Tournament == nil {
		deps.Tournament = ClosedTournament{}
	}
	if deps.Reads == nil {
		deps.Reads = ClosedReads{}
	}
	if deps.Spectator == nil {
		deps.Spectator = ClosedSpectator{}
	}
	if deps.LiveFeed == nil {
		deps.LiveFeed = NewHubLiveFeed(deps.Hub)
	}
	if deps.NotReadyReason == "" && !deps.Ready {
		deps.NotReadyReason = "required backends not ready"
	}
	return &Server{
		identity:                    deps.Identity,
		room:                        deps.Room,
		tournament:                  deps.Tournament,
		reads:                       deps.Reads,
		spectator:                   deps.Spectator,
		audit:                       deps.Audit,
		hub:                         deps.Hub,
		edgeLimiter:                 deps.EdgeLimiter,
		principalLimiter:            deps.PrincipalLimiter,
		liveFeed:                    deps.LiveFeed,
		serviceCredential:           deps.ServiceCredential,
		identityProducerCredential:  deps.IdentityProducerCredential,
		roomProducerCredential:      deps.RoomProducerCredential,
		spectatorProducerCredential: deps.SpectatorProducerCredential,
		ready:                       deps.Ready,
		notReadyReason:              deps.NotReadyReason,
		redisAdapterBlocked:         deps.RedisAdapterBlocked,
		redisReady:                  deps.RedisReady,
		workerReady:                 deps.WorkerReady,
		sessionInvalidation:         deps.SessionInvalidation,
		upstreamProbes:              append([]UpstreamProbe(nil), deps.UpstreamProbes...),
		clock:                       deps.Clock,
		newID:                       deps.NewID,
	}
}

// Hub exposes the stream hub for control-plane test triggers.
func (s *Server) Hub() *Hub { return s.hub }

// Handler returns the BFF HTTP handler (sole public edge routes).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)

	mux.HandleFunc("/v1/auth/register", s.handleRegister)
	mux.HandleFunc("/v1/auth/login", s.handleLogin)
	mux.HandleFunc("/v1/auth/logout", s.handleLogout)
	mux.HandleFunc("/v1/auth/whoami", s.handleWhoami)

	mux.HandleFunc("/v1/commands", s.handleCommands)
	mux.HandleFunc("/v1/rooms", s.handlePublicRoomList)
	mux.HandleFunc("/v1/rooms/", s.handleRoomScoped)
	mux.HandleFunc("/v1/spectator/rooms/", s.handleSpectatorSnapshot)

	mux.HandleFunc("/v1/streams/player", s.handlePlayerStream)
	mux.HandleFunc("/v1/streams/spectator", s.handleSpectatorStream)
	mux.HandleFunc("/v1/streams/control", s.handleControlStream)

	mux.HandleFunc("/v1/rankings/leaderboards", s.handleLeaderboard)
	mux.HandleFunc("/v1/analytics/public", s.handlePublicAnalytics)
	mux.HandleFunc("/v1/tournaments/", s.handleTournamentReads)

	mux.HandleFunc("/internal/v1/control/sessions/", s.handleInternalSessionInvalidated)
	mux.HandleFunc("/internal/v1/control/rooms/", s.handleInternalRoomTerminal)
	mux.HandleFunc("/internal/v1/streams/events", s.handleInternalStreamEvents)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corr := correlation.FromHTTP(r.Header).WithDefaults()
		if corr.CorrelationID == "" {
			corr.CorrelationID = s.newID("corr_")
		}
		ctx := context.WithValue(r.Context(), corrCtxKey{}, corr)
		r = r.WithContext(ctx)
		// Static Ready=false (or redis adapter blocked) gates all public /v1 traffic,
		// not only the /ready probe.
		if strings.HasPrefix(r.URL.Path, "/v1/") && !s.requireReady(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// requireReady rejects public traffic when the server is not ready.
func (s *Server) requireReady(w http.ResponseWriter, r *http.Request) bool {
	if s.ready && !s.redisAdapterBlocked {
		return true
	}
	reason := s.notReadyReason
	if reason == "" {
		reason = "required backends not ready"
	}
	code := "not_ready"
	if s.redisAdapterBlocked {
		if strings.Contains(reason, "REDIS_URL") {
			code = "redis_adapter_blocked"
		} else {
			code = "rate_limiter_not_configured"
		}
	}
	s.writeErr(w, r, http.StatusServiceUnavailable, code, reason, "")
	return false
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "gateway"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if s.redisAdapterBlocked || !s.ready {
		reason := s.notReadyReason
		if reason == "" {
			reason = "required backends not ready"
		}
		code := "not_ready"
		if s.redisAdapterBlocked {
			if strings.Contains(reason, "REDIS_URL") {
				code = "redis_adapter_blocked"
			} else {
				code = "rate_limiter_not_configured"
			}
		}
		s.writeErr(w, r, http.StatusServiceUnavailable, code, reason, "")
		return
	}
	if s.redisReady != nil {
		if err := s.redisReady(r.Context()); err != nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "redis_unavailable",
				"redis readiness ping failed: "+err.Error(), "")
			return
		}
	}
	if s.workerReady != nil {
		if err := s.workerReady(r.Context()); err != nil {
			code := "not_ready"
			msg := err.Error()
			if strings.Contains(msg, "kafka_consumer_not_configured") {
				code = "kafka_consumer_not_configured"
			} else if strings.Contains(msg, "kafka_consumer_stopped") {
				code = "kafka_consumer_stopped"
			} else if strings.Contains(msg, "si_subscriber_stopped") {
				code = "si_subscriber_stopped"
			}
			s.writeErr(w, r, http.StatusServiceUnavailable, code, msg, "")
			return
		}
	}
	for _, probe := range s.upstreamProbes {
		do := probe.Do
		if do == nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "not_ready",
				"upstream probe "+probe.Name+" missing transport", "")
			return
		}
		status, err := do(r.Context(), http.MethodGet, probe.URL)
		if err != nil || status < 200 || status >= 300 {
			msg := "upstream " + probe.Name + " /ready probe failed"
			if err != nil {
				msg = msg + ": " + err.Error()
			}
			s.writeErr(w, r, http.StatusServiceUnavailable, "upstream_unhealthy", msg, "")
			return
		}
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "gateway"})
}

func (s *Server) correlation(r *http.Request) correlation.Headers {
	if v, ok := r.Context().Value(corrCtxKey{}).(correlation.Headers); ok {
		return v
	}
	h := correlation.FromHTTP(r.Header).WithDefaults()
	if h.CorrelationID == "" {
		h.CorrelationID = s.newID("corr_")
	}
	return h
}

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, status int, code, message, commandID string) {
	corr := s.correlation(r)
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	_ = httpx.WriteError(w, status, code, message, corr.CorrelationID, commandID)
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	corr := s.correlation(r)
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	_ = httpx.WriteJSON(w, status, v)
}

func (s *Server) recordRejection(ctx context.Context, cmd envelope.Command, corr correlation.Headers, principal Principal, roomID, tournamentID, reason string, currentSeq *int64) error {
	rec := audit.NewRejection(cmd.CommandID, corr.CorrelationID, principal.SessionID, principal.PlayerID, reason, s.clock())
	if roomID != "" {
		rec = rec.WithRoom(roomID)
	}
	if tournamentID != "" {
		rec = rec.WithTournament(tournamentID)
	}
	rec = rec.WithSequences(cmd.ExpectedSequenceNumber, currentSeq)
	return s.audit.RecordRejection(ctx, rec)
}

func (s *Server) authorizeProducer(r *http.Request, expected string) bool {
	if expected == "" {
		return false
	}
	got := r.Header.Get(headerServiceCredential)
	if got == "" {
		return false
	}
	if len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

func (s *Server) allowEdge(w http.ResponseWriter, r *http.Request) bool {
	ok, retry, err := s.edgeLimiter.Allow(r.Context(), "edge:"+clientIP(r))
	if err != nil {
		s.writeErr(w, r, http.StatusServiceUnavailable, "rate_limiter_unavailable",
			"distributed rate limiter unavailable", "")
		return false
	}
	if ok {
		return true
	}
	if retry > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retrySeconds(retry)))
	}
	s.writeErr(w, r, http.StatusTooManyRequests, "rate_limited", "edge rate limit exceeded", "")
	return false
}

func retrySeconds(d time.Duration) int {
	sec := int(d.Seconds())
	if sec < 1 {
		return 1
	}
	return sec
}

func bearerToken(r *http.Request) (string, bool) {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	tok := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	if tok == "" {
		return "", false
	}
	return tok, true
}

func defaultNewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func extractRoomID(payload json.RawMessage) string {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return ""
	}
	if v, ok := m["roomId"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func extractTournamentID(payload json.RawMessage) string {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return ""
	}
	if v, ok := m["tournamentId"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func readLimitedBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, MaxRequestBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(body) > MaxRequestBodyBytes {
		return nil, errBodyTooLarge
	}
	return body, nil
}

var errBodyTooLarge = fmt.Errorf("request body too large")
