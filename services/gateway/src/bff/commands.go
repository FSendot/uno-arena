package bff

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
	"unoarena/shared/httpx"
)

func (s *Server) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	s.submitCommand(w, r, "")
}

func (s *Server) handleRoomScoped(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/rooms/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" {
		s.writeErr(w, r, http.StatusNotFound, "not_found", "route not found", "")
		return
	}
	roomID, action := parts[0], parts[1]
	switch action {
	case "commands":
		if r.Method != http.MethodPost {
			s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
			return
		}
		s.submitCommand(w, r, roomID)
	case "snapshot":
		if r.Method != http.MethodGet {
			s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
			return
		}
		s.handlePlayerSnapshot(w, r, roomID)
	default:
		s.writeErr(w, r, http.StatusNotFound, "not_found", "route not found", "")
	}
}

func (s *Server) handlePlayerSnapshot(w http.ResponseWriter, r *http.Request, roomID string) {
	if !s.allowEdge(w, r) {
		return
	}
	principal, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}
	corr := s.correlation(r)
	raw, err := s.room.PlayerSnapshot(r.Context(), roomID, principal.PlayerID, corr)
	if err != nil {
		if he, ok := err.(*httpStatusError); ok {
			switch he.status {
			case http.StatusNotFound:
				s.writeErr(w, r, http.StatusNotFound, "not_found", "room not found", "")
				return
			case http.StatusForbidden:
				s.writeErr(w, r, http.StatusForbidden, "forbidden", "not a room member", "")
				return
			}
		}
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "player snapshot failed", "")
		return
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (s *Server) handleSpectatorSnapshot(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/spectator/rooms/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "snapshot" {
		s.writeErr(w, r, http.StatusNotFound, "not_found", "route not found", "")
		return
	}
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.allowEdge(w, r) {
		return
	}
	roomID := parts[0]
	corr := s.correlation(r)

	var (
		token     string
		principal *Principal
	)
	if tok, ok := bearerToken(r); ok {
		token = tok
		p, err := s.identity.ValidateSession(r.Context(), token, corr)
		if err != nil {
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid session", "")
			return
		}
		principal = &p
	}

	admit := SpectatorAdmitRequest{
		RoomID:           roomID,
		Token:            token,
		Principal:        principal,
		InviteCapability: strings.TrimSpace(r.Header.Get("X-Room-Invite")),
		Correlation:      corr,
	}
	allowed, reason, err := s.spectator.Admit(r.Context(), admit)
	if err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "spectator admission failed", "")
		return
	}
	if !allowed {
		if reason == "" {
			reason = "spectator_admission_denied"
		}
		s.writeErr(w, r, http.StatusForbidden, "spectator_denied", reason, "")
		return
	}

	raw, err := s.spectator.Snapshot(r.Context(), admit)
	if err != nil {
		if errors.Is(err, ErrSpectatorDenied) {
			s.writeErr(w, r, http.StatusForbidden, "spectator_denied", err.Error(), "")
			return
		}
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "spectator snapshot failed", "")
		return
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (s *Server) submitCommand(w http.ResponseWriter, r *http.Request, pathRoomID string) {
	// Edge abuse throttle stays pre-auth.
	if !s.allowEdge(w, r) {
		return
	}

	corr := s.correlation(r)
	principal, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	body, err := readLimitedBody(r)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			s.writeErr(w, r, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds limit", "")
			return
		}
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "unable to read body", "")
		return
	}

	if err := requireExplicitSchemaVersion1(body); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", err.Error(), "")
		return
	}

	cmd, err := envelope.Decode(body)
	if err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", err.Error(), "")
		return
	}
	if cmd.SchemaVersion != envelope.CurrentSchemaVersion {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", "schemaVersion must be 1", cmd.CommandID)
		return
	}

	if headerCmd := strings.TrimSpace(r.Header.Get(correlation.HeaderCommandID)); headerCmd != "" && headerCmd != cmd.CommandID {
		s.writeErr(w, r, http.StatusBadRequest, "command_id_mismatch", "X-Command-Id does not match body commandId", cmd.CommandID)
		return
	}
	corr.CommandID = cmd.CommandID
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set(correlation.HeaderCommandID, cmd.CommandID)

	roomID := pathRoomID
	if roomID == "" {
		roomID = extractRoomID(cmd.Payload)
	}
	tournamentID := extractTournamentID(cmd.Payload)

	// Authenticated principal throttle runs after bounded envelope decode so
	// rejection audit includes commandId/principal/aggregate/sequence with no dispatch.
	pkey := "principal:" + principal.PlayerID
	if principal.SessionID != "" {
		pkey = pkey + ":" + principal.SessionID
	}
	if allowed, retry := s.principalLimiter.Allow(r.Context(), pkey); !allowed {
		if err := s.recordRejection(cmd, corr, principal, roomID, tournamentID, "rate_limited", nil); err != nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "audit_unavailable", "unable to record rejection audit", cmd.CommandID)
			return
		}
		if retry > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds(retry)))
		}
		s.writeErr(w, r, http.StatusTooManyRequests, "rate_limited", "principal rate limit exceeded", cmd.CommandID)
		return
	}

	backend := RouteBackend(cmd.Type)
	if backend == BackendUnknown {
		if err := s.recordRejection(cmd, corr, principal, pathRoomID, "", "unknown_command_type", nil); err != nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "audit_unavailable", "unable to record rejection audit", cmd.CommandID)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, envelope.Rejected(cmd.CommandID, cmd.Type, "unknown_command_type", nil))
		return
	}

	if pathRoomID != "" && backend == BackendTournament {
		if err := s.recordRejection(cmd, corr, principal, pathRoomID, extractTournamentID(cmd.Payload), "tournament_on_room_route", nil); err != nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "audit_unavailable", "unable to record rejection audit", cmd.CommandID)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, envelope.Rejected(cmd.CommandID, cmd.Type, "tournament_on_room_route", nil))
		return
	}

	if RequiresExpectedSequence(cmd.Type) && cmd.ExpectedSequenceNumber == nil {
		if err := s.recordRejection(cmd, corr, principal, roomID, tournamentID, "missing_expected_sequence_number", nil); err != nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "audit_unavailable", "unable to record rejection audit", cmd.CommandID)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, envelope.Rejected(cmd.CommandID, cmd.Type, "missing_expected_sequence_number", nil))
		return
	}

	if pathRoomID != "" {
		payloadRoom := extractRoomID(cmd.Payload)
		if payloadRoom != "" && payloadRoom != pathRoomID {
			if err := s.recordRejection(cmd, corr, principal, pathRoomID, tournamentID, "room_id_mismatch", nil); err != nil {
				s.writeErr(w, r, http.StatusServiceUnavailable, "audit_unavailable", "unable to record rejection audit", cmd.CommandID)
				return
			}
			_ = httpx.WriteJSON(w, http.StatusOK, envelope.Rejected(cmd.CommandID, cmd.Type, "room_id_mismatch", nil))
			return
		}
	}

	dispatch := CommandDispatch{
		Command:     cmd,
		Principal:   principal,
		Correlation: corr,
		RoomID:      roomID,
	}

	var result envelope.Result
	switch backend {
	case BackendRoom:
		result, err = s.room.SubmitCommand(r.Context(), dispatch)
	case BackendTournament:
		result, err = s.tournament.SubmitCommand(r.Context(), dispatch)
	}
	if err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "command dispatch failed", cmd.CommandID)
		return
	}

	if err := validateUpstreamResult(cmd, result); err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "invalid_upstream_result", err.Error(), cmd.CommandID)
		return
	}

	if result.Status == envelope.StatusRejected {
		reason := result.Reason
		if reason == "" {
			reason = "rejected"
		}
		if err := s.recordRejection(cmd, corr, principal, roomID, tournamentID, reason, result.Sequence); err != nil {
			s.writeErr(w, r, http.StatusServiceUnavailable, "audit_unavailable", "unable to record rejection audit", cmd.CommandID)
			return
		}
	}

	_ = httpx.WriteJSON(w, http.StatusOK, result)
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
	if err := json.Unmarshal(sv, &n); err != nil {
		return errors.New("schemaVersion must be 1")
	}
	if n != 1 {
		return errors.New("schemaVersion must be 1")
	}
	return nil
}

func validateUpstreamResult(cmd envelope.Command, result envelope.Result) error {
	switch result.Status {
	case envelope.StatusAccepted, envelope.StatusRejected:
	default:
		return errors.New("upstream result status must be accepted or rejected")
	}
	if result.CommandID != cmd.CommandID {
		return errors.New("upstream result commandId mismatch")
	}
	if result.Type != cmd.Type {
		return errors.New("upstream result type mismatch")
	}
	if result.SchemaVersion != envelope.CurrentSchemaVersion {
		return errors.New("upstream result schemaVersion must be 1")
	}
	if result.Status == envelope.StatusRejected && strings.TrimSpace(result.Reason) == "" {
		return errors.New("upstream rejected result requires reason")
	}
	return nil
}

func (s *Server) handlePlayerStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.allowEdge(w, r) {
		return
	}
	principal, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}
	roomID := strings.TrimSpace(r.URL.Query().Get("roomId"))
	if roomID == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "roomId query required", "")
		return
	}
	s.serveSSE(w, r, StreamPlayer, roomID, principal.SessionID, principal.PlayerID)
}

func (s *Server) handleSpectatorStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.allowEdge(w, r) {
		return
	}
	roomID := strings.TrimSpace(r.URL.Query().Get("roomId"))
	if roomID == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "roomId query required", "")
		return
	}
	corr := s.correlation(r)

	var (
		token     string
		principal *Principal
	)
	if tok, ok := bearerToken(r); ok {
		token = tok
		p, err := s.identity.ValidateSession(r.Context(), token, corr)
		if err != nil {
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid session", "")
			return
		}
		principal = &p
	}

	allowed, reason, err := s.spectator.Admit(r.Context(), SpectatorAdmitRequest{
		RoomID:           roomID,
		Token:            token,
		Principal:        principal,
		InviteCapability: strings.TrimSpace(r.Header.Get("X-Room-Invite")),
		Correlation:      corr,
	})
	if err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "spectator admission failed", "")
		return
	}
	if !allowed {
		if reason == "" {
			reason = "spectator_admission_denied"
		}
		s.writeErr(w, r, http.StatusForbidden, "spectator_denied", reason, "")
		return
	}

	sessionID, playerID := "", ""
	if principal != nil {
		sessionID, playerID = principal.SessionID, principal.PlayerID
	}
	s.serveSSE(w, r, StreamSpectator, roomID, sessionID, playerID)
}

func (s *Server) handleControlStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.allowEdge(w, r) {
		return
	}
	principal, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}
	s.serveSSE(w, r, StreamControl, "", principal.SessionID, principal.PlayerID)
}

func (s *Server) serveSSE(w http.ResponseWriter, r *http.Request, kind StreamKind, roomID, sessionID, playerID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeErr(w, r, http.StatusInternalServerError, "internal_error", "streaming unsupported", "")
		return
	}
	corr := s.correlation(r)
	lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID"))

	_, events, replay, cancel, err := s.hub.Subscribe(kind, roomID, sessionID, playerID, lastEventID)
	if err != nil {
		if errors.Is(err, ErrSnapshotRequired) {
			s.writeErr(w, r, http.StatusConflict, "snapshot_required",
				"Last-Event-ID unknown or evicted; resync snapshot required", "")
			return
		}
		if errors.Is(err, ErrStreamDenied) {
			msg := err.Error()
			code := "stream_denied"
			status := http.StatusForbidden
			if strings.Contains(msg, "session_invalidated") {
				code = "session_invalidated"
				status = http.StatusUnauthorized
			} else if strings.Contains(msg, "room_terminal") {
				code = "room_terminal"
			}
			s.writeErr(w, r, status, code, msg, "")
			return
		}
		s.writeErr(w, r, http.StatusInternalServerError, "internal_error", "subscribe failed", "")
		return
	}
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for _, ev := range replay {
		if err := writeSSE(w, flusher, ev); err != nil {
			return
		}
		if ev.Event == "session_invalidated" || ev.Event == "room_terminal" || ev.Event == "snapshot_required" {
			return
		}
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-events:
			if !open {
				return
			}
			if err := writeSSE(w, flusher, ev); err != nil {
				return
			}
			if ev.Event == "session_invalidated" || ev.Event == "room_terminal" || ev.Event == "snapshot_required" {
				return
			}
		}
	}
}

type streamIngestBody struct {
	SchemaVersion int             `json:"schemaVersion"`
	EventID       string          `json:"eventId"`
	Stream        string          `json:"stream"`
	RoomID        string          `json:"roomId"`
	SessionID     string          `json:"sessionId"`
	PlayerID      string          `json:"playerId"`
	Sequence      *int64          `json:"sequence"`
	Event         string          `json:"event"`
	Data          json.RawMessage `json:"data"`
}

func (s *Server) handleInternalStreamEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	body, err := readLimitedBody(r)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			s.writeErr(w, r, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds limit", "")
			return
		}
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "unable to read body", "")
		return
	}
	if err := requireExplicitSchemaVersion1(body); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", err.Error(), "")
		return
	}
	var req streamIngestBody
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid json body", "")
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	req.Stream = strings.TrimSpace(req.Stream)
	req.RoomID = strings.TrimSpace(req.RoomID)
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.PlayerID = strings.TrimSpace(req.PlayerID)
	req.Event = strings.TrimSpace(req.Event)
	if req.SchemaVersion != 1 {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", "schemaVersion must be 1", "")
		return
	}
	if req.EventID == "" || req.Event == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "eventId and event are required", "")
		return
	}
	kind := StreamKind(req.Stream)
	switch kind {
	case StreamPlayer:
		if !s.authorizeProducer(r, s.roomProducerCredential) {
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid room producer credential", "")
			return
		}
		if req.RoomID == "" || req.PlayerID == "" || req.SessionID == "" {
			s.writeErr(w, r, http.StatusBadRequest, "bad_request", "player stream requires roomId, playerId, sessionId", "")
			return
		}
	case StreamSpectator:
		roomOK := s.authorizeProducer(r, s.roomProducerCredential)
		specOK := s.authorizeProducer(r, s.spectatorProducerCredential)
		if !roomOK && !specOK {
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid room/spectator producer credential", "")
			return
		}
		if req.RoomID == "" {
			s.writeErr(w, r, http.StatusBadRequest, "bad_request", "spectator stream requires roomId", "")
			return
		}
	case StreamControl:
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "control stream ingest not permitted; use session control endpoint", "")
		return
	default:
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "stream must be player or spectator", "")
		return
	}
	if req.Sequence == nil || *req.Sequence < 1 {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "sequence must be >= 1", "")
		return
	}

	ev := StreamEvent{
		Event:         req.Event,
		Data:          req.Data,
		SchemaVersion: 1,
	}
	if len(ev.Data) == 0 {
		ev.Data = json.RawMessage(`{}`)
	}
	res, err := s.hub.IngestToStream(kind, req.RoomID, req.SessionID, req.PlayerID, req.EventID, *req.Sequence, ev)
	if err != nil {
		switch {
		case errors.Is(err, ErrSequenceConflict):
			s.writeErr(w, r, http.StatusConflict, "sequence_conflict", "eventId conflicts with a different sequence", "")
		case errors.Is(err, ErrSequenceNotIncreasing):
			s.writeErr(w, r, http.StatusConflict, "sequence_not_increasing", "sequence must be strictly increasing per audience", "")
		case errors.Is(err, ErrSequenceRequired):
			s.writeErr(w, r, http.StatusBadRequest, "bad_request", "sequence must be >= 1", "")
		default:
			s.writeErr(w, r, http.StatusBadRequest, "bad_request", err.Error(), "")
		}
		return
	}
	if res.Duplicate {
		s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "ok", "duplicate": true, "eventId": req.EventID})
		return
	}
	out := map[string]any{
		"status":  "ok",
		"eventId": req.EventID,
		"sent":    res.Sent,
	}
	if res.Closed > 0 {
		out["closed"] = res.Closed
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) handleInternalSessionInvalidated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.authorizeProducer(r, s.identityProducerCredential) {
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid identity producer credential", "")
		return
	}
	// /internal/v1/control/sessions/{sessionId}/invalidated
	path := strings.TrimPrefix(r.URL.Path, "/internal/v1/control/sessions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "invalidated" {
		s.writeErr(w, r, http.StatusNotFound, "not_found", "route not found", "")
		return
	}
	pathSessionID := parts[0]

	body, err := readLimitedBody(r)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			s.writeErr(w, r, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds limit", "")
			return
		}
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "unable to read body", "")
		return
	}
	if len(body) == 0 {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "versioned body with eventId is required", "")
		return
	}
	if err := requireExplicitSchemaVersion1(body); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", err.Error(), "")
		return
	}
	var req struct {
		SchemaVersion int    `json:"schemaVersion"`
		EventID       string `json:"eventId"`
		EventType     string `json:"eventType"`
		SessionID     string `json:"sessionId"`
		PlayerID      string `json:"playerId"`
		Reason        string `json:"reason"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid json body", "")
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	req.EventType = strings.TrimSpace(req.EventType)
	req.SessionID = strings.TrimSpace(req.SessionID)
	if req.EventID == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "eventId is required", "")
		return
	}
	if req.EventType != ControlEventSessionInvalidated {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "eventType must be SessionInvalidated", "")
		return
	}
	if req.SessionID == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "sessionId is required", "")
		return
	}
	if req.SessionID != pathSessionID {
		s.writeErr(w, r, http.StatusBadRequest, "id_mismatch", "body sessionId does not match path", "")
		return
	}
	n, dup, err := s.hub.ApplySessionInvalidation(req.EventID, pathSessionID)
	if err != nil {
		if errors.Is(err, ErrControlConflict) {
			s.writeErr(w, r, http.StatusConflict, "control_conflict", err.Error(), "")
			return
		}
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", err.Error(), "")
		return
	}
	if dup {
		s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "ok", "duplicate": true, "eventId": req.EventID, "sessionId": pathSessionID, "closed": n})
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "ok", "sessionId": pathSessionID, "closed": n, "eventId": req.EventID})
}

func (s *Server) handleInternalRoomTerminal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.authorizeProducer(r, s.roomProducerCredential) {
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid room producer credential", "")
		return
	}
	// /internal/v1/control/rooms/{roomId}/terminal
	path := strings.TrimPrefix(r.URL.Path, "/internal/v1/control/rooms/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "terminal" {
		s.writeErr(w, r, http.StatusNotFound, "not_found", "route not found", "")
		return
	}
	pathRoomID := parts[0]

	body, err := readLimitedBody(r)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			s.writeErr(w, r, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds limit", "")
			return
		}
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "unable to read body", "")
		return
	}
	if len(body) == 0 {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "versioned body with eventId is required", "")
		return
	}
	if err := requireExplicitSchemaVersion1(body); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "invalid_envelope", err.Error(), "")
		return
	}
	var req struct {
		SchemaVersion int    `json:"schemaVersion"`
		EventID       string `json:"eventId"`
		EventType     string `json:"eventType"`
		RoomID        string `json:"roomId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid json body", "")
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	req.EventType = strings.TrimSpace(req.EventType)
	req.RoomID = strings.TrimSpace(req.RoomID)
	if req.EventID == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "eventId is required", "")
		return
	}
	if req.EventType != ControlEventRoomTerminal {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "eventType must be RoomTerminal", "")
		return
	}
	if req.RoomID == "" {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "roomId is required", "")
		return
	}
	if req.RoomID != pathRoomID {
		s.writeErr(w, r, http.StatusBadRequest, "id_mismatch", "body roomId does not match path", "")
		return
	}
	n, dup, err := s.hub.ApplyRoomTerminal(req.EventID, pathRoomID)
	if err != nil {
		if errors.Is(err, ErrControlConflict) {
			s.writeErr(w, r, http.StatusConflict, "control_conflict", err.Error(), "")
			return
		}
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", err.Error(), "")
		return
	}
	if dup {
		s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "ok", "duplicate": true, "eventId": req.EventID, "roomId": pathRoomID, "closed": n})
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "ok", "roomId": pathRoomID, "closed": n, "eventId": req.EventID})
}
