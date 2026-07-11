package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
	"unoarena/shared/httpx"
)

const (
	internalCredentialHeader = "X-Service-Credential"
	maxBodyBytes             = 1 << 20
)

var timerCommandAllowlist = map[string]struct{}{
	app.CmdExpireUnoWindow:      {},
	app.CmdForfeitPlayer:        {},
	app.CmdSkipDisconnectedTurn: {},
}

// Server wires HTTP handlers to the Room Gameplay application service.
type Server struct {
	svc                *app.Service
	internalCredential string
	timerCredential    string
	serviceName        string
	ready              bool
	notReadyReason     string
}

// NewServer constructs an injectable HTTP server.
func NewServer(svc *app.Service, internalCredential, serviceName string) *Server {
	return NewServerWithTimerCred(svc, internalCredential, "", serviceName)
}

// NewServerWithTimerCred constructs a server with a dedicated timer-worker credential.
func NewServerWithTimerCred(svc *app.Service, internalCredential, timerCredential, serviceName string) *Server {
	if serviceName == "" {
		serviceName = "room-gameplay"
	}
	return &Server{
		svc:                svc,
		internalCredential: internalCredential,
		timerCredential:    timerCredential,
		serviceName:        serviceName,
		ready:              true,
	}
}

// SetReady marks static readiness (Identity session validator / timer credential wiring).
func (s *Server) SetReady(ready bool, reason string) {
	s.ready = ready
	s.notReadyReason = reason
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)
	mux.HandleFunc("/internal/v1/commands", s.internalCommandsHandler)
	mux.HandleFunc("/internal/v1/rooms/provision", s.provisionHandler)
	mux.HandleFunc("/internal/v1/rooms/", s.internalRoomScopedHandler)
	mux.HandleFunc("/v1/rooms/", s.roomScopedHandler)
	mux.HandleFunc("/v1/rooms", s.createRoomHandler)
	return mux
}

func (s *Server) authorizeInternal(r *http.Request) bool {
	return credentialMatch(r.Header.Get(internalCredentialHeader), s.internalCredential)
}

func (s *Server) authorizeTimer(r *http.Request) bool {
	// Dedicated timer credential only — never fall back to the generic service credential.
	return credentialMatch(r.Header.Get(internalCredentialHeader), s.timerCredential)
}

func credentialMatch(got, want string) bool {
	if want == "" {
		return false
	}
	if got == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": s.serviceName})
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.ready {
		reason := s.notReadyReason
		if reason == "" {
			reason = "required dependencies not configured"
		}
		_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", reason, "", "")
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": s.serviceName})
}

func (s *Server) requireReady(w http.ResponseWriter) bool {
	if s.ready {
		return true
	}
	reason := s.notReadyReason
	if reason == "" {
		reason = "required dependencies not configured"
	}
	_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", reason, "", "")
	return false
}

func (s *Server) createRoomHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.requireReady(w) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
		return
	}
	s.dispatchCommand(w, r, "", false)
}

func (s *Server) roomScopedHandler(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/rooms/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] == "" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
		return
	}
	roomID := parts[0]
	action := parts[1]
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
		return
	}
	switch {
	case action == "commands" && r.Method == http.MethodPost:
		s.dispatchCommand(w, r, roomID, false)
	case action == "snapshot" && r.Method == http.MethodGet:
		s.snapshotHandler(w, r, roomID)
	default:
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
	}
}

func (s *Server) internalCommandsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.requireReady(w) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
		return
	}
	s.dispatchCommand(w, r, "", false)
}

func (s *Server) internalRoomScopedHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/internal/v1/rooms/")
	if path == "provision" {
		s.provisionHandler(w, r)
		return
	}
	if !s.requireReady(w) {
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "timer-commands" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
		return
	}
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.authorizeTimer(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid timer service credential", "", "")
		return
	}
	s.dispatchCommand(w, r, parts[0], true)
}

func (s *Server) provisionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.requireReady(w) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
		return
	}
	body, err := readBody(r)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", "", "")
		return
	}
	var req struct {
		CommandID    string   `json:"commandId"`
		TournamentID string   `json:"tournamentId"`
		RoundNumber  int      `json:"roundNumber"`
		SlotID       string   `json:"slotId"`
		RoomID       string   `json:"roomId"`
		HostID       string   `json:"hostId"`
		PlayerIDs    []string `json:"playerIds"`
		Visibility   string   `json:"visibility"`
		MaxSeats     int      `json:"maxSeats"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body", "", "")
		return
	}
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID == "" {
		corr.CorrelationID = req.CommandID
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	res := s.svc.Provision(r.Context(), app.ProvisionInput{
		CommandID:     req.CommandID,
		TournamentID:  req.TournamentID,
		RoundNumber:   req.RoundNumber,
		SlotID:        req.SlotID,
		RoomID:        req.RoomID,
		HostID:        req.HostID,
		PlayerIDs:     req.PlayerIDs,
		Visibility:    req.Visibility,
		MaxSeats:      req.MaxSeats,
		CorrelationID: corr.CorrelationID,
	})
	if res.Err != nil {
		_ = httpx.WriteError(w, http.StatusBadGateway, "upstream_error", res.Err.Error(), corr.CorrelationID, req.CommandID)
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, res.Result)
}

func (s *Server) snapshotHandler(w http.ResponseWriter, r *http.Request, roomID string) {
	playerID := strings.TrimSpace(r.URL.Query().Get("playerId"))
	if playerID == "" {
		playerID = strings.TrimSpace(r.Header.Get("X-Player-Id"))
	}
	if playerID == "" {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "playerId required", "", "")
		return
	}
	snap, err := s.svc.PlayerSnapshot(roomID, playerID)
	if err != nil {
		switch {
		case errors.Is(err, app.ErrNotFound):
			_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "room not found", "", "")
		case errors.Is(err, app.ErrForbidden):
			_ = httpx.WriteError(w, http.StatusForbidden, "forbidden", "not a room member", "", "")
		default:
			_ = httpx.WriteError(w, http.StatusInternalServerError, "internal_error", "snapshot failed", "", "")
		}
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, snap)
}

func (s *Server) dispatchCommand(w http.ResponseWriter, r *http.Request, pathRoomID string, timerPath bool) {
	body, err := readBody(r)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", "", "")
		return
	}
	if err := requireExplicitSchemaVersion1(body); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", err.Error(), "", "")
		return
	}

	var raw struct {
		CommandID              string          `json:"commandId"`
		Type                   string          `json:"type"`
		SchemaVersion          int             `json:"schemaVersion"`
		Payload                json.RawMessage `json:"payload"`
		PlayerID               string          `json:"playerId"`
		SessionID              string          `json:"sessionId"`
		RoomID                 string          `json:"roomId"`
		ExpectedSequenceNumber *int64          `json:"expectedSequenceNumber"`
		AsSystem               bool            `json:"asSystem"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", "invalid json body", "", "")
		return
	}
	if raw.SchemaVersion != envelope.CurrentSchemaVersion {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", "schemaVersion must be 1", "", raw.CommandID)
		return
	}
	cmd := envelope.Command{
		CommandID:              raw.CommandID,
		Type:                   raw.Type,
		SchemaVersion:          raw.SchemaVersion,
		Payload:                raw.Payload,
		ExpectedSequenceNumber: raw.ExpectedSequenceNumber,
	}
	if err := cmd.Validate(); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", err.Error(), "", raw.CommandID)
		return
	}

	asSystem := false
	if timerPath {
		if _, ok := timerCommandAllowlist[raw.Type]; !ok {
			_ = httpx.WriteError(w, http.StatusBadRequest, "command_not_allowed", "command type not allowed on timer endpoint", "", raw.CommandID)
			return
		}
		// System issuer is derived server-side; clients cannot set asSystem.
		asSystem = true
	}

	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID == "" {
		corr.CorrelationID = raw.CommandID
	}
	corr.CommandID = raw.CommandID
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set(correlation.HeaderCommandID, raw.CommandID)

	roomID := pathRoomID
	if roomID == "" {
		roomID = raw.RoomID
	}
	if roomID == "" {
		var p struct {
			RoomID string `json:"roomId"`
		}
		_ = json.Unmarshal(raw.Payload, &p)
		roomID = p.RoomID
	}

	res := s.svc.HandleCommand(r.Context(), app.CommandInput{
		CommandID:              raw.CommandID,
		Type:                   raw.Type,
		SchemaVersion:          raw.SchemaVersion,
		Payload:                raw.Payload,
		PlayerID:               raw.PlayerID,
		SessionID:              raw.SessionID,
		RoomID:                 roomID,
		ExpectedSequenceNumber: raw.ExpectedSequenceNumber,
		CorrelationID:          corr.CorrelationID,
		AsSystem:               asSystem,
	})
	if res.Err != nil {
		_ = httpx.WriteError(w, http.StatusBadGateway, "integrity_append_failed", res.Err.Error(), corr.CorrelationID, raw.CommandID)
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, res.Result)
}

func requireExplicitSchemaVersion1(body []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return err
	}
	sv, ok := raw["schemaVersion"]
	if !ok {
		return errors.New("schemaVersion is required and must be 1")
	}
	var n int
	if err := json.Unmarshal(sv, &n); err != nil || n != 1 {
		return errors.New("schemaVersion must be 1")
	}
	return nil
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
}

// OutboxRetryWorker drains pending outbox entries with bounded backoff.
type OutboxRetryWorker struct {
	svc     *app.Service
	stopCh  chan struct{}
	doneCh  chan struct{}
	mu      sync.Mutex
	started bool
	stopped bool
	initial time.Duration
	max     time.Duration
	limit   int
}

func NewOutboxRetryWorker(svc *app.Service) *OutboxRetryWorker {
	return &OutboxRetryWorker{
		svc:     svc,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		initial: time.Second,
		max:     30 * time.Second,
		limit:   32,
	}
}

func (w *OutboxRetryWorker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return
	}
	w.started = true
	go w.loop()
}

func (w *OutboxRetryWorker) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	if !w.started {
		close(w.doneCh)
		w.mu.Unlock()
		return
	}
	close(w.stopCh)
	w.mu.Unlock()
	<-w.doneCh
}

func (w *OutboxRetryWorker) loop() {
	defer close(w.doneCh)
	interval := w.initial
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-timer.C:
			n, err := w.svc.DrainOutbox(nil, w.limit)
			if err != nil || n == 0 {
				next := interval * 2
				if next > w.max || next <= 0 {
					next = w.max
				}
				interval = next
			} else {
				interval = w.initial
			}
			timer.Reset(interval)
		}
	}
}

func main() {
	cfg := loadRoomRuntimeConfig()
	wired, err := wireRoomRuntime(cfg)
	if err != nil {
		log.Fatal(err)
	}
	worker := NewOutboxRetryWorker(wired.Service)
	worker.Start()
	defer worker.Stop()

	srv := NewServerWithTimerCred(wired.Service, cfg.ServiceCredential, cfg.TimerCredential, cfg.ServiceName)
	srv.SetReady(wired.Ready, wired.NotReadyReason)
	log.Printf(`{"level":"info","service":"%s","event":"startup","mode":%q,"ready":%t}`, cfg.ServiceName, wired.Mode, wired.Ready)
	log.Fatal(http.ListenAndServe(":8080", srv.routes()))
}

// roomRuntimeConfig is the env-derived Room process wiring input.
type roomRuntimeConfig struct {
	ServiceName       string
	ServiceCredential string
	TimerCredential   string
	GatewayURL        string
	GatewayCred       string
	IdentityURL       string
	IdentityCred      string
	GameIntegrityURL  string
	GameIntegrityCred string
	DatabaseURL       string
	AuditLogPath      string
	AllowFakes        bool
	CapabilityMode    bool
	SpectatorURL      string
	SpectatorCred     string
	RankingURL        string
	RankingCred       string
	AnalyticsURL      string
	AnalyticsCred     string
	TournamentURL     string
	TournamentCred    string
}

type roomRuntime struct {
	Service        *app.Service
	Deps           app.ServiceDeps
	Ready          bool
	NotReadyReason string
	Mode           string
}

func loadRoomRuntimeConfig() roomRuntimeConfig {
	cred := strings.TrimSpace(os.Getenv("SERVICE_CREDENTIAL"))
	if cred == "" {
		cred = strings.TrimSpace(os.Getenv("ROOM_SERVICE_CREDENTIAL"))
	}
	gatewayCred := strings.TrimSpace(os.Getenv("GATEWAY_SERVICE_CREDENTIAL"))
	if gatewayCred == "" {
		gatewayCred = cred
	}
	svcName := os.Getenv("SERVICE_NAME")
	if svcName == "" {
		svcName = "room-gameplay"
	}
	return roomRuntimeConfig{
		ServiceName:       svcName,
		ServiceCredential: cred,
		TimerCredential:   strings.TrimSpace(os.Getenv("ROOM_TIMER_SERVICE_CREDENTIAL")),
		GatewayURL:        strings.TrimSpace(os.Getenv("GATEWAY_URL")),
		GatewayCred:       gatewayCred,
		IdentityURL:       strings.TrimSpace(os.Getenv("IDENTITY_URL")),
		IdentityCred: firstNonEmptyEnv(
			os.Getenv("IDENTITY_SERVICE_CREDENTIAL"),
			os.Getenv("ROOM_IDENTITY_CREDENTIAL"),
			cred,
		),
		GameIntegrityURL: strings.TrimSpace(os.Getenv("GAME_INTEGRITY_URL")),
		GameIntegrityCred: firstNonEmptyEnv(
			os.Getenv("GAME_INTEGRITY_INTERNAL_CREDENTIAL"),
			os.Getenv("ROOM_GAME_INTEGRITY_CREDENTIAL"),
			cred,
		),
		DatabaseURL:    strings.TrimSpace(os.Getenv("DATABASE_URL")),
		AuditLogPath:   strings.TrimSpace(os.Getenv("ROOM_AUDIT_LOG_PATH")),
		AllowFakes:     envTruthy("ROOM_ALLOW_FAKES") || envTruthy("ALLOW_FAKES"),
		CapabilityMode: envTruthy("ROOM_CAPABILITY_MODE"),
		SpectatorURL:   strings.TrimSpace(os.Getenv("SPECTATOR_VIEW_URL")),
		SpectatorCred:  firstEnv("SPECTATOR_VIEW_SERVICE_CREDENTIAL", gatewayCred),
		RankingURL:     strings.TrimSpace(os.Getenv("RANKING_URL")),
		RankingCred:    firstEnv("RANKING_SERVICE_CREDENTIAL", gatewayCred),
		AnalyticsURL:   strings.TrimSpace(os.Getenv("ANALYTICS_URL")),
		AnalyticsCred:  firstEnv("ANALYTICS_SERVICE_CREDENTIAL", gatewayCred),
		TournamentURL:  strings.TrimSpace(os.Getenv("TOURNAMENT_URL")),
		TournamentCred: firstEnv("TOURNAMENT_SERVICE_CREDENTIAL", gatewayCred),
	}
}

// wireRoomRuntime selects isolated fakes, real-service capability, or configured mode.
// Configured mode never silently falls back to memory; readiness reports
// postgres_adapter_blocked until a durable Postgres session adapter exists.
// Capability mode uses MemorySessionRepository only as a missing-Postgres stand-in
// with real HTTP Identity/GI/publisher/JSONL audit — not a production adapter masquerade.
func wireRoomRuntime(cfg roomRuntimeConfig) (roomRuntime, error) {
	clock := app.SystemClock{}

	multiPublisher := app.NewMultiDestinationPublisher(app.PublisherDestinations{
		GatewayURL:     cfg.GatewayURL,
		GatewayCred:    cfg.GatewayCred,
		SpectatorURL:   cfg.SpectatorURL,
		SpectatorCred:  cfg.SpectatorCred,
		RankingURL:     cfg.RankingURL,
		RankingCred:    cfg.RankingCred,
		AnalyticsURL:   cfg.AnalyticsURL,
		AnalyticsCred:  cfg.AnalyticsCred,
		TournamentURL:  cfg.TournamentURL,
		TournamentCred: cfg.TournamentCred,
	}, nil)

	var publisher app.EventPublisher = app.NewFakeEventPublisher()
	if cfg.GatewayURL != "" || cfg.SpectatorURL != "" || cfg.RankingURL != "" {
		publisher = multiPublisher
	}

	if cfg.AllowFakes {
		deps := app.ServiceDeps{
			Sessions:  app.NewMemorySessionRepository(),
			Integrity: app.NewFakeGameIntegrity(),
			Publisher: publisher,
			Audit:     app.NewFakeAuditSink(),
			Deals:     app.NewFakeDealSource(),
			Clock:     clock,
			SessionsV: app.AllowAllSessionValidator{},
		}
		if cfg.IdentityURL != "" {
			deps.SessionsV = app.NewHTTPSessionValidator(cfg.IdentityURL, cfg.IdentityCred, nil)
		}
		return roomRuntime{
			Service: app.NewService(deps),
			Deps:    deps,
			Ready:   true,
			Mode:    "capability-fakes",
		}, nil
	}

	if cfg.CapabilityMode {
		return wireRoomCapabilityRuntime(cfg, clock, multiPublisher)
	}

	// Configured / production mode: real HTTP adapters; never memory fallback.
	ready := false
	notReadyReason := "postgres_adapter_blocked"
	_ = cfg.DatabaseURL // presence or absence: Postgres adapter is not implemented.

	var integrity app.GameIntegrity = app.ClosedGameIntegrity{}
	var deals app.DealSource = app.ClosedDealSource{}
	if cfg.GameIntegrityURL != "" {
		integrity = app.NewHTTPGameIntegrity(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
		deals = app.NewHTTPDealSource(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
	}

	var sessionsV app.SessionValidator = app.ClosedSessionValidator{}
	if cfg.IdentityURL != "" {
		sessionsV = app.NewHTTPSessionValidator(cfg.IdentityURL, cfg.IdentityCred, nil)
	}

	var auditSink app.AuditSink = app.NewStderrJSONLAuditSink()
	if cfg.AuditLogPath != "" {
		sink, err := app.OpenJSONLAuditSink(cfg.AuditLogPath)
		if err != nil {
			return roomRuntime{}, err
		}
		auditSink = sink
	}

	deps := app.ServiceDeps{
		Sessions:  app.BlockedSessionRepository{},
		Integrity: integrity,
		Publisher: publisher,
		Audit:     auditSink,
		Deals:     deals,
		Clock:     clock,
		SessionsV: sessionsV,
	}
	if cfg.TimerCredential == "" && notReadyReason == "postgres_adapter_blocked" {
		// Keep postgres reason primary; timer still required for full readiness later.
	}
	return roomRuntime{
		Service:        app.NewService(deps),
		Deps:           deps,
		Ready:          ready,
		NotReadyReason: notReadyReason,
		Mode:           "configured",
	}, nil
}

func wireRoomCapabilityRuntime(cfg roomRuntimeConfig, clock app.Clock, publisher *app.MultiDestinationPublisher) (roomRuntime, error) {
	var integrity app.GameIntegrity = app.ClosedGameIntegrity{}
	var deals app.DealSource = app.ClosedDealSource{}
	if cfg.GameIntegrityURL != "" {
		integrity = app.NewHTTPGameIntegrity(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
		deals = app.NewHTTPDealSource(cfg.GameIntegrityURL, cfg.GameIntegrityCred, nil)
	}

	var sessionsV app.SessionValidator = app.ClosedSessionValidator{}
	if cfg.IdentityURL != "" {
		sessionsV = app.NewHTTPSessionValidator(cfg.IdentityURL, cfg.IdentityCred, nil)
	}

	var auditSink app.AuditSink = app.NewStderrJSONLAuditSink()
	if cfg.AuditLogPath != "" {
		sink, err := app.OpenJSONLAuditSink(cfg.AuditLogPath)
		if err != nil {
			return roomRuntime{}, err
		}
		auditSink = sink
	}

	deps := app.ServiceDeps{
		// Memory only as missing-Postgres stand-in — not a production adapter claim.
		Sessions:  app.NewMemorySessionRepository(),
		Integrity: integrity,
		Publisher: publisher,
		Audit:     auditSink,
		Deals:     deals,
		Clock:     clock,
		SessionsV: sessionsV,
	}

	ready := true
	notReadyReason := ""
	if missing := roomCapabilityMissing(cfg); len(missing) > 0 {
		ready = false
		notReadyReason = "capability_dependencies_missing: " + strings.Join(missing, ",")
	}

	return roomRuntime{
		Service:        app.NewService(deps),
		Deps:           deps,
		Ready:          ready,
		NotReadyReason: notReadyReason,
		Mode:           "capability",
	}, nil
}

func roomCapabilityMissing(cfg roomRuntimeConfig) []string {
	var missing []string
	require := func(name, v string) {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, name)
		}
	}
	require("SERVICE_CREDENTIAL", cfg.ServiceCredential)
	require("ROOM_TIMER_SERVICE_CREDENTIAL", cfg.TimerCredential)
	require("IDENTITY_URL", cfg.IdentityURL)
	require("IDENTITY_CREDENTIAL", cfg.IdentityCred)
	require("GAME_INTEGRITY_URL", cfg.GameIntegrityURL)
	require("GAME_INTEGRITY_CREDENTIAL", cfg.GameIntegrityCred)
	require("GATEWAY_URL", cfg.GatewayURL)
	require("GATEWAY_CREDENTIAL", cfg.GatewayCred)
	require("SPECTATOR_VIEW_URL", cfg.SpectatorURL)
	require("SPECTATOR_CREDENTIAL", cfg.SpectatorCred)
	require("RANKING_URL", cfg.RankingURL)
	require("RANKING_CREDENTIAL", cfg.RankingCred)
	require("ANALYTICS_URL", cfg.AnalyticsURL)
	require("ANALYTICS_CREDENTIAL", cfg.AnalyticsCred)
	require("TOURNAMENT_URL", cfg.TournamentURL)
	require("TOURNAMENT_CREDENTIAL", cfg.TournamentCred)
	return missing
}

func firstEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func firstNonEmptyEnv(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func envTruthy(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
