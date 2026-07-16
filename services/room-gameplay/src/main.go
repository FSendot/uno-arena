package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/store"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
	"unoarena/shared/httpx"
	"unoarena/shared/internalprincipal"
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
	principalVerifier           *internalprincipal.Verifier
}

// ConfigureInternalPrincipalVerifier enables fail-closed verification of the
// Identity-issued principal on every non-system mutation.
func (s *Server) ConfigureInternalPrincipalVerifier(verifier *internalprincipal.Verifier) {
	s.principalVerifier = verifier
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
	if s.runtimeRoomID != "" {
		if !s.authorizeRuntime(r, roomID) {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid room runtime credential or generation", "", "")
			return
		}
	} else if !s.authorizeInternal(r) {
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
	} else if !s.authorizeInternal(r) {
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
	if len(parts) != 2 || parts[0] == "" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
		return
	}
	roomID := parts[0]
	action := parts[1]
	switch action {
	case "timer-commands":
		if r.Method != http.MethodPost {
			_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		if s.runtimeRoomID != "" {
			if !s.authorizeRuntime(r, roomID) {
				_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid room runtime credential or generation", "", "")
				return
			}
		} else if !s.authorizeTimer(r) {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid timer service credential", "", "")
			return
		}
		s.dispatchCommand(w, r, roomID, true)
	case "spectator-recovery-snapshot":
		if s.runtimeRoomID != "" {
			_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "route not found", "", "")
			return
		}
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
	// X-Player-Id is accepted only behind the authenticated stable Room hop.
	// Query input is never an identity source.
	playerID := strings.TrimSpace(r.Header.Get("X-Player-Id"))
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
	principalVerified := false
	if !asSystem && s.principalVerifier != nil {
		binding, err := internalprincipal.NewRoomCommandBinding(raw.CommandID, raw.Type, roomID, raw.ExpectedSequenceNumber, raw.Payload)
		if err != nil {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid internal principal", "", raw.CommandID)
			return
		}
		if _, err := s.principalVerifier.Verify(r.Header.Get(internalprincipal.HeaderName), raw.PlayerID, raw.SessionID, binding); err != nil {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid internal principal", "", raw.CommandID)
			return
		}
		principalVerified = true
	}

	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID == "" {
		corr.CorrelationID = raw.CommandID
	}
	corr.CommandID = raw.CommandID
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set(correlation.HeaderCommandID, raw.CommandID)

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
		PrincipalVerified:      principalVerified,
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

func runtimePodEnvironment(cfg roomRuntimeConfig) map[string]string {
	return map[string]string{
		"SERVICE_NAME": cfg.ServiceName, "DEPLOYMENT_ENV": cfg.DeploymentEnv,
		"TELEMETRY_MODE": os.Getenv("TELEMETRY_MODE"), "SERVICE_VERSION": os.Getenv("SERVICE_VERSION"),
		"UNOARENA_COMPONENT": "room-runtime", "OTEL_EXPORTER_OTLP_ENDPOINT": os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTEL_EXPORTER_OTLP_PROTOCOL": os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"), "OTEL_TRACES_SAMPLER": os.Getenv("OTEL_TRACES_SAMPLER"),
		"OTEL_TRACES_SAMPLER_ARG": os.Getenv("OTEL_TRACES_SAMPLER_ARG"), "OTEL_GO_X_OBSERVABILITY": os.Getenv("OTEL_GO_X_OBSERVABILITY"),
		"METRICS_ADDR":                                os.Getenv("METRICS_ADDR"),
		"GAME_INTEGRITY_URL":                          cfg.GameIntegrityURL,
		"ROOM_INTERNAL_PRINCIPAL_ISSUER":              cfg.InternalPrincipalIssuer,
		"ROOM_INTERNAL_PRINCIPAL_AUDIENCE":            cfg.InternalPrincipalAudience,
		"ROOM_INTERNAL_PRINCIPAL_MAX_TTL_SECONDS":     strconv.Itoa(cfg.InternalPrincipalMaxTTLSeconds),
		"ROOM_INTERNAL_PRINCIPAL_FUTURE_SKEW_SECONDS": strconv.Itoa(cfg.InternalPrincipalFutureSkewSeconds),
		"ROOM_INTERNAL_PRINCIPAL_CURRENT_KEY_ID":      cfg.InternalPrincipalCurrentKeyID,
		"ROOM_INTERNAL_PRINCIPAL_PREVIOUS_KEY_ID":     cfg.InternalPrincipalPreviousKeyID,
		"ROOM_INTERNAL_PRINCIPAL_KEY_VERSION":         cfg.InternalPrincipalKeyVersion,
		"GAME_INTEGRITY_HTTP_TIMEOUT_MILLIS":          strconv.FormatInt(cfg.GameIntegrityHTTPTimeout.Milliseconds(), 10),
		"REDIS_URL":                                   cfg.RedisURL,
		"GATEWAY_URL":                                 cfg.GatewayURL,
		"SPECTATOR_VIEW_URL":                          cfg.SpectatorURL,
		"RANKING_URL":                                 cfg.RankingURL,
		"ANALYTICS_URL":                               cfg.AnalyticsURL,
		"TOURNAMENT_URL":                              cfg.TournamentURL,
		"ROOM_RUNTIME_MUTATION_QUEUE_CAPACITY":        strconv.Itoa(cfg.RuntimeQueueCapacity),
	}
}

func main() {
	if err := runRoomProcess(); err != nil {
		if roomProcessTelemetry != nil {
			roomProcessTelemetry.Logger.Error("room process stopped", "event", "process_stopped", "error", err.Error())
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func runRoomProcess() error {
	telemetryRuntime, err := startRoomTelemetry(context.Background())
	if err != nil {
		return fmt.Errorf("room telemetry: %w", err)
	}
	defer shutdownRoomTelemetry(telemetryRuntime)
	store.ConfigureTelemetry(telemetryRuntime.TracerProvider, telemetryRuntime.MeterProvider)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg := loadRoomRuntimeConfig()
	if cfg.WorkerRole == "room-runtime-controller" {
		pool, err := store.NewPool(rootCtx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		kube, err := kubeClientFromEnvironment(cfg)
		if err != nil {
			return err
		}
		controller := NewRuntimeController(store.NewSessionStore(pool.Main), kube, cfg.RuntimeControllerOwner, cfg.RuntimeImage, cfg.RuntimeRouterCredential)
		controller.ConfigureLimits(cfg.RuntimeControllerClaimBatch, cfg.RuntimeControllerConcurrency, cfg.RuntimeReadinessTimeout)
		controller.ConfigureRuntimePodProfile(runtimePodProfile{
			CPURequest: cfg.RuntimeCPURequest, MemoryRequest: cfg.RuntimeMemoryRequest,
			CPULimit: cfg.RuntimeCPULimit, MemoryLimit: cfg.RuntimeMemoryLimit,
			ProbeTimeoutSeconds: cfg.RuntimeProbeTimeoutSeconds, ProbeFailureThreshold: cfg.RuntimeProbeFailureThreshold,
			NodeSelector: cfg.RuntimeNodeSelector, TopologySpreadEnabled: cfg.RuntimeTopologySpreadEnabled,
			TopologyKey: cfg.RuntimeTopologyKey, TopologyMaxSkew: cfg.RuntimeTopologyMaxSkew,
			TopologyWhenUnsatisfiable: cfg.RuntimeTopologyUnsatisfiable,
		})
		controller.ConfigureRuntimePod(cfg.RuntimeSecretName, cfg.RuntimeSecretEnv, runtimePodEnvironment(cfg), cfg.InternalPrincipalKeyVersion)
		slog.InfoContext(rootCtx, "room runtime controller started", "event", "room_runtime_controller_startup")
		cadence := boundedRuntimeControllerCadence(cfg.RuntimeControllerCadence)
		timer := time.NewTimer(controller.nextCadence(cadence))
		defer timer.Stop()
		for {
			if err := controller.ReconcileOnce(rootCtx); err != nil && rootCtx.Err() == nil {
				slog.ErrorContext(rootCtx, "room runtime reconciliation failed", "event", "room_runtime_controller_reconcile_failed", "error", err.Error())
			}
			select {
			case <-rootCtx.Done():
				return nil
			case <-timer.C:
				timer.Reset(controller.nextCadence(cadence))
			}
		}
	}
	if cfg.WorkerRole == "room-player-stream-compactor" {
		if strings.TrimSpace(cfg.DatabaseURL) == "" || strings.TrimSpace(cfg.RedisURL) == "" {
			return errors.New("room-player-stream-compactor requires Room Postgres and player-feed Redis")
		}
		pool, err := store.NewPool(rootCtx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		rdb, err := store.NewRedisFromURL(cfg.RedisURL)
		if err != nil {
			return fmt.Errorf("player stream Redis: %w", err)
		}
		defer rdb.Close()
		worker := NewPlayerStreamCompactor(store.NewSessionStore(pool.Main), redisPlayerStreamTrimmer{client: rdb})
		worker.Configure(cfg.PlayerStreamCompactorPage, cfg.PlayerStreamMaxLen, cfg.PlayerStreamTrimLimit, cfg.PlayerStreamCompactorCadence, cfg.PlayerStreamOperationTimeout)
		slog.InfoContext(rootCtx, "player stream compactor started", "event", "room_player_stream_compactor_startup", "pageSize", cfg.PlayerStreamCompactorPage, "maxLen", cfg.PlayerStreamMaxLen)
		worker.Run(rootCtx)
		return nil
	}
	wired, err := wireRoomRuntime(cfg)
	if err != nil {
		return err
	}

	if cfg.WorkerRole == "room-timer" {
		if wired.Mode != "durable" || wired.Timers == nil {
			return errors.New("WORKER_ROLE=room-timer requires durable mode with Redis timers")
		}
		continuations := store.NewNextGameContinuationQueue(wired.Pool.Main)
		rebuilder := NewTimerIndexRebuildWorker(wired.Sessions, cfg.RuntimeControllerOwner, func(ctx context.Context) error {
			return wired.Timers.RebuildFromPostgres(ctx, wired.Pool.Main)
		})
		rebuilder.Start()
		defer rebuilder.Stop()
		tw := NewTimerWorkerWithContinuations(wired.Timers, continuations, cfg.RoomGameplayURL, cfg.TimerCredential)
		tw.Start()
		defer tw.Stop()
		slog.InfoContext(rootCtx, "timer worker started", "event", "timer_worker_startup", "mode", wired.Mode)
		<-rootCtx.Done()
		return nil
	}

	if cfg.WorkerRole == "room-integrity-reconciler" {
		if wired.Mode != "durable" || wired.Sessions == nil || wired.Deps.Integrity == nil {
			return errors.New("WORKER_ROLE=room-integrity-reconciler requires durable Room Postgres and Game Integrity")
		}
		rw := NewReconciliationWorker(wired.Sessions, wired.Deps.Integrity, wired.Deps.Deals)
		rw.Configure(cfg.RuntimeControllerOwner, cfg.IntegrityReconcilerBatch, cfg.IntegrityReconcilerLease, cfg.IntegrityReconcilerInterval)
		rw.Start()
		defer rw.Stop()
		slog.InfoContext(rootCtx, "room integrity reconciler started", "event", "room_integrity_reconciler_startup", "mode", wired.Mode)
		<-rootCtx.Done()
		return nil
	}

	// Capability-only: OutboxRetryWorker + MultiDestinationPublisher drain.
	// Durable mode relies on Debezium CDC — never start polling outbox workers.
	if wired.Mode == "capability" || wired.Mode == "capability-fakes" {
		worker := NewOutboxRetryWorker(wired.Service)
		worker.Start()
		defer worker.Stop()
	}

	srv := NewServerWithScopedCreds(wired.Service, cfg.ServiceCredential, cfg.TimerCredential, cfg.SpectatorRecoveryCredential, cfg.AnalyticsBackfillCredential, cfg.ServiceName)
	if roomServesPlayerCommands(cfg.WorkerRole) || cfg.InternalPrincipalCurrentHMACKey != "" {
		var previous *internalprincipal.Key
		if cfg.InternalPrincipalPreviousKeyID != "" || cfg.InternalPrincipalPreviousHMACKey != "" {
			previous = &internalprincipal.Key{ID: cfg.InternalPrincipalPreviousKeyID, Material: []byte(cfg.InternalPrincipalPreviousHMACKey)}
		}
		verifier, err := internalprincipal.NewVerifier(internalprincipal.VerifierConfig{
			CurrentKey:  internalprincipal.Key{ID: cfg.InternalPrincipalCurrentKeyID, Material: []byte(cfg.InternalPrincipalCurrentHMACKey)},
			PreviousKey: previous, Issuer: cfg.InternalPrincipalIssuer,
			Audience:   cfg.InternalPrincipalAudience,
			MaxTTL:     time.Duration(cfg.InternalPrincipalMaxTTLSeconds) * time.Second,
			FutureSkew: time.Duration(cfg.InternalPrincipalFutureSkewSeconds) * time.Second,
		})
		if err != nil {
			return fmt.Errorf("internal principal verifier: %w", err)
		}
		srv.ConfigureInternalPrincipalVerifier(verifier)
	}
	if cfg.WorkerRole == "room-runtime" {
		if cfg.RuntimeRoomID == "" || cfg.RuntimeGeneration < 1 || cfg.RuntimeRouterCredential == "" {
			return errors.New("room-runtime requires ROOM_RUNTIME_ROOM_ID, ROOM_RUNTIME_GENERATION, and ROOM_RUNTIME_ROUTER_CREDENTIAL")
		}
		srv.ConfigureDedicatedRuntime(cfg.RuntimeRoomID, cfg.RuntimeGeneration, cfg.RuntimeRouterCredential, cfg.RuntimeQueueCapacity)
	}
	srv.SetReady(wired.Ready, wired.NotReadyReason)
	srv.durableReady = wired.DurableReady
	if cfg.WorkerRole == "room-router" {
		kube, err := kubeClientFromEnvironment(cfg)
		if err != nil {
			return err
		}
		directory := NewPodDirectory()
		refreshPodDirectory(rootCtx, directory, kube)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-rootCtx.Done():
					return
				case <-ticker.C:
					refreshPodDirectory(rootCtx, directory, kube)
				}
			}
		}()
		router := NewRoomRouter(directory, cfg.RuntimeRouterCredential, nil)
		handler := roomHTTPHandler(telemetryRuntime, NewStableRoomHandler(srv, router))
		slog.InfoContext(rootCtx, "room router started", "event", "room_router_startup", "mode", wired.Mode, "ready", wired.Ready)
		return serveRoomHTTP(rootCtx, handler)
	}
	slog.InfoContext(rootCtx, "room process started", "event", "startup", "mode", wired.Mode, "ready", wired.Ready)
	return serveRoomHTTP(rootCtx, roomHTTPHandler(telemetryRuntime, srv.routes()))
}

func serveRoomHTTP(ctx context.Context, handler http.Handler) error {
	server := newRoomHTTPServer(handler)
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), roomShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

const roomShutdownTimeout = 5 * time.Second

func newRoomHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
		// WriteTimeout intentionally remains zero: Room can proxy streamed
		// responses, and a global deadline would terminate healthy long-lived
		// connections. Individual handlers own their operation deadlines.
	}
}

func refreshPodDirectory(ctx context.Context, directory *PodDirectory, kube KubePodClient) {
	pods, err := kube.List(ctx)
	if err != nil {
		slog.WarnContext(ctx, "room router directory refresh failed", "event", "room_router_directory_refresh_failed", "error", err.Error())
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
