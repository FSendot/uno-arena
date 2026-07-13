package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
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
	app.CmdStartNextGame:        {},
}

// Server wires HTTP handlers to the Room Gameplay application service.
type Server struct {
	svc                         *app.Service
	internalCredential          string
	timerCredential             string
	spectatorRecoveryCredential string
	analyticsBackfillCredential string
	serviceName                 string
	ready                       bool
	notReadyReason              string
	durableReady                func(context.Context) error
	runtimeRoomID               string
	runtimeGeneration           int64
	runtimeCredential           string
	mutationQueue               *RoomMutationQueue
}

// ConfigureDedicatedRuntime pins this process to exactly one Room generation.
// It is intentionally unavailable to the stable router/capability process.
func (s *Server) ConfigureDedicatedRuntime(roomID string, generation int64, credential string, queueCapacity int) {
	s.runtimeRoomID = roomID
	s.runtimeGeneration = generation
	s.runtimeCredential = credential
	s.mutationQueue = NewRoomMutationQueue(queueCapacity)
}

func (s *Server) authorizeRuntime(r *http.Request, roomID string) bool {
	if s.runtimeRoomID == "" {
		return true
	}
	if roomID != s.runtimeRoomID || !credentialMatch(r.Header.Get(runtimeCredentialHeader), s.runtimeCredential) {
		return false
	}
	generation, err := strconv.ParseInt(r.Header.Get(runtimeGenerationHeader), 10, 64)
	return err == nil && generation == s.runtimeGeneration
}

// NewServer constructs an injectable HTTP server.
func NewServer(svc *app.Service, internalCredential, serviceName string) *Server {
	return NewServerWithScopedCreds(svc, internalCredential, "", "", "", serviceName)
}

// NewServerWithTimerCred constructs a server with a dedicated timer-worker credential.
func NewServerWithTimerCred(svc *app.Service, internalCredential, timerCredential, serviceName string) *Server {
	return NewServerWithScopedCreds(svc, internalCredential, timerCredential, "", "", serviceName)
}

// NewServerWithScopedCreds constructs a server with dedicated timer, spectator-recovery,
// and Analytics backfill credentials.
func NewServerWithScopedCreds(svc *app.Service, internalCredential, timerCredential, spectatorRecoveryCredential, analyticsBackfillCredential, serviceName string) *Server {
	if serviceName == "" {
		serviceName = "room-gameplay"
	}
	return &Server{
		svc:                         svc,
		internalCredential:          internalCredential,
		timerCredential:             timerCredential,
		spectatorRecoveryCredential: spectatorRecoveryCredential,
		analyticsBackfillCredential: analyticsBackfillCredential,
		serviceName:                 serviceName,
		ready:                       true,
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
	mux.HandleFunc("/internal/v1/rooms/analytics-backfill", s.analyticsBackfillHandler)
	mux.HandleFunc("/internal/v1/rooms/public-list", s.publicListHandler)
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

func (s *Server) authorizeSpectatorRecovery(r *http.Request) bool {
	// Dedicated spectator projection-rebuilder credential only — never fall back to
	// the generic service credential or the timer credential.
	return credentialMatch(r.Header.Get(internalCredentialHeader), s.spectatorRecoveryCredential)
}

func (s *Server) authorizeAnalyticsBackfill(r *http.Request) bool {
	// Scoped Analytics pair credential only — never fall back to Gateway/generic/timer/spectator.
	return credentialMatch(r.Header.Get(internalCredentialHeader), s.analyticsBackfillCredential)
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
	if s.durableReady != nil {
		if err := s.durableReady(r.Context()); err != nil {
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", "schema_or_db", "", "")
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": s.serviceName})
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
	if s.runtimeRoomID != "" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
		return
	}
	if s.runtimeRoomID == "" && !s.authorizeInternal(r) {
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
	if !s.authorizeRuntime(r, roomID) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid room runtime credential or generation", "", "")
		return
	}
	if s.runtimeRoomID == "" && !s.authorizeInternal(r) {
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
	if s.runtimeRoomID != "" {
		body, err := readBody(r)
		if err != nil {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", "", "")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var command struct {
			RoomID string `json:"roomId"`
		}
		if json.Unmarshal(body, &command) != nil || !s.authorizeRuntime(r, command.RoomID) {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid room runtime credential or generation", "", "")
			return
		}
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
	if len(parts) != 2 || parts[0] == "" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
		return
	}
	roomID := parts[0]
	action := parts[1]
	if !s.authorizeRuntime(r, roomID) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid room runtime credential or generation", "", "")
		return
	}
	switch action {
	case "timer-commands":
		if r.Method != http.MethodPost {
			_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		if s.runtimeRoomID == "" && !s.authorizeTimer(r) {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid timer service credential", "", "")
			return
		}
		s.dispatchCommand(w, r, roomID, true)
	case "spectator-recovery-snapshot":
		if r.Method != http.MethodGet {
			_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		if !s.authorizeSpectatorRecovery(r) {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid spectator recovery service credential", "", "")
			return
		}
		s.spectatorRecoverySnapshotHandler(w, r, roomID)
	default:
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
	}
}

func (s *Server) spectatorRecoverySnapshotHandler(w http.ResponseWriter, r *http.Request, roomID string) {
	q := r.URL.Query()
	failedCheckpointRaw := strings.TrimSpace(q.Get("failedCheckpoint"))
	recoveryJobID := strings.TrimSpace(q.Get("recoveryJobId"))
	schemaVersionRaw := strings.TrimSpace(q.Get("schemaVersion"))

	if failedCheckpointRaw == "" || recoveryJobID == "" || schemaVersionRaw == "" {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "failedCheckpoint, recoveryJobId, and schemaVersion are required", "", "")
		return
	}
	failedCheckpoint, err := strconv.ParseInt(failedCheckpointRaw, 10, 64)
	if err != nil || failedCheckpoint < 1 {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "failedCheckpoint must be an integer >= 1", "", "")
		return
	}
	schemaVersion, err := strconv.Atoi(schemaVersionRaw)
	if err != nil || schemaVersion != 1 {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "schemaVersion must be 1", "", "")
		return
	}

	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID != "" {
		w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	}

	snap, err := s.svc.SpectatorRecoverySnapshot(r.Context(), roomID)
	if err != nil {
		switch {
		case errors.Is(err, app.ErrNotFound):
			_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "room not found", corr.CorrelationID, "")
		default:
			_ = httpx.WriteError(w, http.StatusInternalServerError, "internal_error", "spectator recovery snapshot failed", corr.CorrelationID, "")
		}
		return
	}
	snap["schemaVersion"] = schemaVersion
	snap["roomId"] = roomID
	snap["recoveryJobId"] = recoveryJobID
	snap["failedCheckpoint"] = failedCheckpoint
	_ = httpx.WriteJSON(w, http.StatusOK, snap)
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

func (s *Server) analyticsBackfillHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.requireReady(w) {
		return
	}
	if !s.authorizeAnalyticsBackfill(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid analytics service credential", "", "")
		return
	}
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID != "" {
		w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	}
	var req app.AnalyticsBackfillRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json", corr.CorrelationID, "")
		return
	}
	resp, err := s.svc.AnalyticsBackfill(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, app.ErrAnalyticsBackfillBadRequest):
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", err.Error(), corr.CorrelationID, "")
		case errors.Is(err, app.ErrAnalyticsBackfillUnavailable):
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", "analytics backfill unavailable", corr.CorrelationID, "")
		case errors.Is(err, app.ErrAnalyticsBackfillCorrupt):
			_ = httpx.WriteError(w, http.StatusInternalServerError, "corrupt_outbox", "stored envelope failed validation", corr.CorrelationID, "")
		default:
			_ = httpx.WriteError(w, http.StatusInternalServerError, "internal_error", "analytics backfill failed", corr.CorrelationID, "")
		}
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) publicListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.requireReady(w) {
		return
	}
	// Gateway↔Room scoped credential only (constant-time). Public list needs no player bearer.
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", "", "")
		return
	}
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID != "" {
		w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	}
	q := r.URL.Query()
	parsed, err := app.ParsePublicListQuery(q.Get("status"), q.Get("cursor"), q.Get("limit"))
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", err.Error(), corr.CorrelationID, "")
		return
	}
	page, err := s.svc.PublicList(r.Context(), parsed)
	if err != nil {
		switch {
		case errors.Is(err, app.ErrPublicListBadRequest), errors.Is(err, app.ErrInvalidPublicListCursor):
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", err.Error(), corr.CorrelationID, "")
		case errors.Is(err, app.ErrPublicListUnavailable), errors.Is(err, app.ErrPublicListCursorSecretRequired):
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", "public list unavailable", corr.CorrelationID, "")
		default:
			_ = httpx.WriteError(w, http.StatusInternalServerError, "internal_error", "public list failed", corr.CorrelationID, "")
		}
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, page)
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
	snap, err := s.svc.PlayerSnapshot(r.Context(), roomID, playerID)
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

	input := app.CommandInput{
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
		RuntimeGeneration:      s.runtimeGeneration,
	}
	var res app.CommandResult
	if s.mutationQueue != nil {
		if err := s.mutationQueue.Do(func() { res = s.svc.HandleCommand(r.Context(), input) }); err != nil {
			_ = httpx.WriteError(w, http.StatusTooManyRequests, "room_busy", "room mutation queue is full", corr.CorrelationID, raw.CommandID)
			return
		}
	} else {
		res = s.svc.HandleCommand(r.Context(), input)
	}
	if res.Err != nil {
		if errors.Is(res.Err, app.ErrRuntimeGenerationStale) {
			_ = httpx.WriteError(w, http.StatusConflict, "stale_room_generation", "room runtime generation is stale", corr.CorrelationID, raw.CommandID)
			return
		}
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
	if cfg.WorkerRole == "room-runtime-controller" {
		pool, err := store.NewPool(context.Background(), cfg.DatabaseURL)
		if err != nil {
			log.Fatal(err)
		}
		defer pool.Close()
		kube, err := kubeClientFromEnvironment(cfg)
		if err != nil {
			log.Fatal(err)
		}
		controller := NewRuntimeController(store.NewSessionStore(pool.Main), kube, cfg.RuntimeControllerOwner, cfg.RuntimeImage, cfg.RuntimeRouterCredential)
		controller.ConfigureLimits(cfg.RuntimeControllerClaimBatch, cfg.RuntimeControllerConcurrency, cfg.RuntimeReadinessTimeout)
		controller.ConfigureRuntimePod(cfg.RuntimeSecretName, cfg.RuntimeSecretEnv, map[string]string{
			"SERVICE_NAME": cfg.ServiceName, "DEPLOYMENT_ENV": cfg.DeploymentEnv,
			"IDENTITY_URL": cfg.IdentityURL, "GAME_INTEGRITY_URL": cfg.GameIntegrityURL,
			"REDIS_URL": cfg.RedisURL, "GATEWAY_URL": cfg.GatewayURL,
			"SPECTATOR_VIEW_URL": cfg.SpectatorURL, "RANKING_URL": cfg.RankingURL,
			"ANALYTICS_URL": cfg.AnalyticsURL, "TOURNAMENT_URL": cfg.TournamentURL,
			"ROOM_RUNTIME_MUTATION_QUEUE_CAPACITY": strconv.Itoa(cfg.RuntimeQueueCapacity),
		})
		log.Printf(`{"level":"info","service":"%s","event":"room_runtime_controller_startup"}`, cfg.ServiceName)
		cadence := boundedRuntimeControllerCadence(cfg.RuntimeControllerCadence)
		for {
			if err := controller.ReconcileOnce(context.Background()); err != nil {
				log.Printf(`{"level":"error","service":"%s","event":"room_runtime_controller_reconcile","error":%q}`, cfg.ServiceName, err.Error())
			}
			time.Sleep(cadence)
		}
	}
	wired, err := wireRoomRuntime(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.WorkerRole == "room-timer" {
		if wired.Mode != "durable" || wired.Timers == nil {
			log.Fatal("WORKER_ROLE=room-timer requires durable mode with Redis timers")
		}
		continuations := store.NewNextGameContinuationQueue(wired.Pool.Main)
		tw := NewTimerWorkerWithContinuations(wired.Timers, continuations, cfg.RoomGameplayURL, cfg.TimerCredential)
		tw.Start()
		defer tw.Stop()
		log.Printf(`{"level":"info","service":"%s","event":"timer_worker_startup","mode":%q}`, cfg.ServiceName, wired.Mode)
		select {}
	}

	// Capability-only: OutboxRetryWorker + MultiDestinationPublisher drain.
	// Durable mode relies on Debezium CDC — never start polling outbox workers.
	if wired.Mode == "capability" || wired.Mode == "capability-fakes" {
		worker := NewOutboxRetryWorker(wired.Service)
		worker.Start()
		defer worker.Stop()
	}

	// Durable API: autonomous GI reconciliation (markers survive commit failure).
	if wired.Mode == "durable" && wired.Sessions != nil && wired.Deps.Integrity != nil {
		rw := NewReconciliationWorker(wired.Sessions, wired.Deps.Integrity, wired.Deps.Deals)
		rw.Start()
		defer rw.Stop()
	}

	srv := NewServerWithScopedCreds(wired.Service, cfg.ServiceCredential, cfg.TimerCredential, cfg.SpectatorRecoveryCredential, cfg.AnalyticsBackfillCredential, cfg.ServiceName)
	if cfg.WorkerRole == "room-runtime" {
		if cfg.RuntimeRoomID == "" || cfg.RuntimeGeneration < 1 || cfg.RuntimeRouterCredential == "" {
			log.Fatal("room-runtime requires ROOM_RUNTIME_ROOM_ID, ROOM_RUNTIME_GENERATION, and ROOM_RUNTIME_ROUTER_CREDENTIAL")
		}
		srv.ConfigureDedicatedRuntime(cfg.RuntimeRoomID, cfg.RuntimeGeneration, cfg.RuntimeRouterCredential, cfg.RuntimeQueueCapacity)
	}
	srv.SetReady(wired.Ready, wired.NotReadyReason)
	srv.durableReady = wired.DurableReady
	if cfg.WorkerRole == "room-router" {
		kube, err := kubeClientFromEnvironment(cfg)
		if err != nil {
			log.Fatal(err)
		}
		directory := NewPodDirectory()
		refreshPodDirectory(context.Background(), directory, kube)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for range ticker.C {
				refreshPodDirectory(context.Background(), directory, kube)
			}
		}()
		router := NewRoomRouter(directory, cfg.RuntimeRouterCredential, nil)
		base := srv.routes()
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if shouldRouteToRuntime(r) {
				router.ServeHTTP(w, r)
				return
			}
			base.ServeHTTP(w, r)
		})
		log.Printf(`{"level":"info","service":"%s","event":"room_router_startup","mode":%q,"ready":%t}`, cfg.ServiceName, wired.Mode, wired.Ready)
		log.Fatal(http.ListenAndServe(":8080", handler))
	}
	log.Printf(`{"level":"info","service":"%s","event":"startup","mode":%q,"ready":%t}`, cfg.ServiceName, wired.Mode, wired.Ready)
	log.Fatal(http.ListenAndServe(":8080", srv.routes()))
}

func shouldRouteToRuntime(r *http.Request) bool {
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1/rooms/") {
		return true
	}
	if path == "/internal/v1/commands" && r.Method == http.MethodPost {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		if err != nil {
			return true
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var command struct {
			Type string `json:"type"`
		}
		return json.Unmarshal(body, &command) == nil && command.Type != app.CmdCreateRoom
	}
	parts := strings.Split(strings.TrimPrefix(path, "/internal/v1/rooms/"), "/")
	return strings.HasPrefix(path, "/internal/v1/rooms/") && len(parts) == 2 && parts[0] != "" && parts[0] != "provision"
}

func refreshPodDirectory(ctx context.Context, directory *PodDirectory, kube KubePodClient) {
	pods, err := kube.List(ctx)
	if err != nil {
		log.Printf(`{"level":"warn","event":"room_router_directory_refresh","error":%q}`, err.Error())
		return
	}
	directory.Replace(pods)
}

func kubeClientFromEnvironment(cfg roomRuntimeConfig) (KubePodClient, error) {
	baseURL := cfg.KubernetesAPIURL
	if baseURL == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
		if host == "" {
			return nil, fmt.Errorf("KUBERNETES_API_URL or in-cluster Kubernetes service is required")
		}
		if port == "" {
			port = "443"
		}
		baseURL = "https://" + host + ":" + port
	}
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil && cfg.KubernetesAPIURL == "" {
		return nil, fmt.Errorf("read Kubernetes service-account token: %w", err)
	}
	caPEM, caErr := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if caErr != nil && cfg.KubernetesAPIURL == "" {
		return nil, fmt.Errorf("read Kubernetes service-account CA: %w", caErr)
	}
	return newKubeHTTPClient(baseURL, cfg.KubernetesNamespace, strings.TrimSpace(string(token)), caPEM), nil
}
