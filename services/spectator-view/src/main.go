package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"unoarena/services/spectator-view/domain"
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

// Server wires HTTP handlers to the Spectator View application seam.
type Server struct {
	app                SpectatorApplication
	feed               SpectatorLiveFeed
	internalCredential string
	mode               string
	readyReason        string
	readyOverride      *bool // tests
	durableReady       func(context.Context) error
}

// NewServer constructs a Spectator View HTTP server (capability defaults).
func NewServer(app SpectatorApplication, internalCredential string) *Server {
	hub := NewStreamHub()
	return &Server{
		app:                app,
		feed:               hub,
		internalCredential: internalCredential,
		mode:               "capability",
	}
}

// NewServerWithFeed constructs a server with an explicit live feed (durable Redis).
func NewServerWithFeed(app SpectatorApplication, feed SpectatorLiveFeed, internalCredential, mode string) *Server {
	return &Server{
		app:                app,
		feed:               feed,
		internalCredential: internalCredential,
		mode:               mode,
	}
}

func newServerFromRuntime(rt spectatorRuntime) *Server {
	return &Server{
		app:                rt.app,
		feed:               rt.feed,
		internalCredential: rt.cred,
		mode:               rt.mode,
		readyReason:        rt.reason,
		durableReady:       rt.durableReady,
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)

	mux.HandleFunc("/internal/v1/rooms/{roomId}/spectator-admission", s.admissionHandler)
	mux.HandleFunc("/internal/v1/rooms/{roomId}/invites", s.registerInviteHandler)
	mux.HandleFunc("/internal/v1/spectator/rooms/{roomId}/events", s.ingestEventHandler)
	mux.HandleFunc("/internal/v1/spectator/rooms/{roomId}/rebuild", s.rebuildHandler)
	mux.HandleFunc("/internal/v1/spectator/rooms/{roomId}/rebuild-status", s.rebuildStatusHandler)

	mux.HandleFunc("/v1/spectator/rooms/{roomId}/snapshot", s.snapshotHandler)
	mux.HandleFunc("/v1/spectator/rooms/{roomId}/events", s.sseEventsHandler)
	return mux
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
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok", Service: "spectator-view"})
	logRequest(r, "/health")
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.isReady(r.Context()) {
		reason := s.readyReason
		if reason == "" {
			reason = "store_unconfigured"
			if s.app != nil && s.internalCredential == "" {
				reason = "internal_credential_unconfigured"
			}
		}
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{
			Status:  "not_ready",
			Service: "spectator-view",
			Reason:  reason,
		})
		logRequest(r, "/ready")
		return
	}
	mode := s.mode
	if mode == "" {
		mode = "offline"
	}
	writeJSON(w, http.StatusOK, readyResponse{
		Status:  "ready",
		Service: "spectator-view",
		Mode:    mode,
	})
	logRequest(r, "/ready")
}

func (s *Server) isReady(ctx context.Context) bool {
	if s.readyOverride != nil {
		return *s.readyOverride
	}
	if s.app == nil || s.internalCredential == "" {
		return false
	}
	if s.mode == "durable" && s.durableReady != nil {
		if err := s.durableReady(ctx); err != nil {
			if strings.Contains(err.Error(), "kafka_consumer_stopped") {
				s.readyReason = "kafka_consumer_stopped"
			} else {
				s.readyReason = "redis_unavailable"
			}
			return false
		}
		return true
	}
	if err := s.app.Ready(ctx); err != nil {
		s.readyReason = "redis_unavailable"
		return false
	}
	return true
}

func (s *Server) requireReady(w http.ResponseWriter, r *http.Request) bool {
	if s.isReady(r.Context()) {
		return true
	}
	writeError(w, r, http.StatusServiceUnavailable, "not_ready", "required dependencies not configured")
	return false
}

type admissionRequest struct {
	RoomID    string `json:"roomId"`
	PlayerID  string `json:"playerId"`
	SessionID string `json:"sessionId"`
	// Operator is trusted only on this internal route (Gateway credential required).
	Operator bool `json:"operator"`
	// Authorized is ignored — never trusted as blanket private admission.
	Authorized bool `json:"authorized"`
	// Deprecated boolean fields — ignored (never grant invite capability).
	Invite           bool `json:"invite"`
	InviteCapability bool `json:"inviteCapability"`
}

type admissionResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

const headerRoomInvite = "X-Room-Invite"

func (s *Server) registerInviteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var body struct {
		InviteToken string `json:"inviteToken"`
		Token       string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	token := body.InviteToken
	if token == "" {
		token = body.Token
	}
	ok, err := s.app.RegisterInvite(r.Context(), roomID, token)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "invite registry unavailable")
		return
	}
	if !ok {
		writeError(w, r, http.StatusBadRequest, "bad_request", "inviteToken is required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"registered": true, "roomId": string(roomID)})
	logRequest(r, r.URL.Path)
}

func (s *Server) admissionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var body admissionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	inviteToken := r.Header.Get(headerRoomInvite)
	hasInvite := false
	if inviteToken != "" {
		ok, err := s.app.HasInvite(r.Context(), roomID, inviteToken)
		if err != nil {
			writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "invite lookup unavailable")
			return
		}
		hasInvite = ok
	}
	auth := domain.SpectatorAuth{
		PlayerID:   domain.PlayerID(body.PlayerID),
		SessionID:  body.SessionID,
		HasInvite:  hasInvite,
		IsOperator: body.Operator,
	}
	dec, err := s.app.Admission(r.Context(), roomID, auth)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "admission unavailable")
		return
	}
	writeJSON(w, http.StatusOK, admissionResponse{Allowed: dec.Allowed, Reason: dec.Reason})
	logRequest(r, r.URL.Path)
}

// canonicalSpectatorEvent is the single Room/Gateway spectator-safe envelope.
type canonicalSpectatorEvent struct {
	SchemaVersion  int             `json:"schemaVersion"`
	EventID        string          `json:"eventId"`
	Stream         string          `json:"stream"`
	RoomID         string          `json:"roomId"`
	Sequence       uint64          `json:"sequence"`
	Event          string          `json:"event"`
	Data           map[string]any  `json:"data"`
	Facts          []canonicalFact `json:"facts"`
	EventType      string          `json:"eventType"`
	SequenceNumber uint64          `json:"sequenceNumber"`
	Payload        map[string]any  `json:"payload"`
}

type canonicalFact struct {
	EventID string         `json:"eventId"`
	Event   string         `json:"event"`
	Data    map[string]any `json:"data"`
}

func (b canonicalSpectatorEvent) sequence() uint64 {
	if b.Sequence > 0 {
		return b.Sequence
	}
	return b.SequenceNumber
}

func (b canonicalSpectatorEvent) eventType() string {
	if b.Event != "" {
		return b.Event
	}
	return b.EventType
}

func (b canonicalSpectatorEvent) data() map[string]any {
	if b.Data != nil {
		return b.Data
	}
	return b.Payload
}

func (b canonicalSpectatorEvent) toDomainEvents(roomID domain.RoomID) ([]domain.SpectatorSafeEvent, error) {
	seq := b.sequence()
	sv := b.SchemaVersion
	if sv == 0 {
		sv = domain.CurrentSchemaVersion
	}
	if len(b.Facts) > 0 {
		out := make([]domain.SpectatorSafeEvent, 0, len(b.Facts))
		for i, f := range b.Facts {
			et := f.Event
			if et == "" {
				return nil, errInvalidEnvelope
			}
			id := f.EventID
			if id == "" {
				id = b.EventID
				if len(b.Facts) > 1 {
					id = b.EventID + "-f" + itoa(i)
				}
			}
			out = append(out, domain.SpectatorSafeEvent{
				EventID:       domain.EventID(id),
				EventType:     domain.EventType(et),
				SchemaVersion: sv,
				RoomID:        roomID,
				Sequence:      domain.SequenceNumber(seq),
				Payload:       f.Data,
			})
		}
		return out, nil
	}
	et := b.eventType()
	if et == "" || b.EventID == "" || seq < 1 {
		return nil, errInvalidEnvelope
	}
	return []domain.SpectatorSafeEvent{{
		EventID:       domain.EventID(b.EventID),
		EventType:     domain.EventType(et),
		SchemaVersion: sv,
		RoomID:        roomID,
		Sequence:      domain.SequenceNumber(seq),
		Payload:       b.data(),
	}}, nil
}

var errInvalidEnvelope = errors.New("invalid spectator-safe envelope")

func (s *Server) ingestEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var body canonicalSpectatorEvent
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	if body.RoomID != "" && domain.RoomID(body.RoomID) != roomID {
		writeError(w, r, http.StatusBadRequest, "bad_request", "path roomId does not match body roomId")
		return
	}
	events, err := body.toDomainEvents(roomID)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "canonical spectator-safe envelope required")
		return
	}

	out, err := s.app.Apply(r.Context(), roomID, events)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "projection apply unavailable")
		return
	}
	if out.Kind == domain.OutcomeAccepted {
		if s.mode == "capability" || s.mode == "" {
			if hub, ok := s.feed.(*StreamHub); ok {
				meta, _ := s.app.RoomMeta(r.Context(), roomID)
				if raw, err := s.app.SnapshotJSON(r.Context(), roomID); err == nil {
					hub.PublishUpdate(roomID, uint64(out.Sequence), raw)
				}
				if meta.StreamClosed {
					hub.MarkTerminal(roomID)
				}
			}
		} else {
			meta, _ := s.app.RoomMeta(r.Context(), roomID)
			if meta.StreamClosed {
				s.feed.MarkTerminal(roomID)
			}
		}
	}
	status := http.StatusOK
	switch out.Kind {
	case domain.OutcomeQuarantined, domain.OutcomeDropped:
		status = http.StatusConflict
	case domain.OutcomeIgnored:
		// Ignored remains 200 so publishers can advance.
	}
	writeJSON(w, status, outcomeResponse(out))
	logRequest(r, r.URL.Path)
}

func (s *Server) rebuildHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	var body struct {
		Events []canonicalSpectatorEvent `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	events := make([]domain.SpectatorSafeEvent, 0, len(body.Events))
	for _, e := range body.Events {
		if e.RoomID == "" {
			e.RoomID = string(roomID)
		}
		if domain.RoomID(e.RoomID) != roomID {
			writeError(w, r, http.StatusBadRequest, "bad_request", "event roomId does not match path roomId")
			return
		}
		evs, err := e.toDomainEvents(roomID)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "bad_request", "canonical spectator-safe envelope required")
			return
		}
		events = append(events, evs...)
	}

	result, err := s.app.Rebuild(r.Context(), roomID, events)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "rebuild unavailable")
		return
	}
	if result.StreamClosed {
		s.feed.MarkTerminal(roomID)
	} else if hub, ok := s.feed.(*StreamHub); ok {
		if raw, err := s.app.SnapshotJSON(r.Context(), roomID); err == nil {
			hub.PublishUpdate(roomID, result.Sequence, raw)
		}
	}

	outs := make([]outcomeDTO, 0, len(result.Outcomes))
	for _, o := range result.Outcomes {
		outs = append(outs, outcomeResponse(o))
	}
	writeJSON(w, http.StatusOK, rebuildResponse{
		Outcomes:     outs,
		Sequence:     result.Sequence,
		Status:       result.Status,
		StreamClosed: result.StreamClosed,
		EventCount:   result.EventCount,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) rebuildStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeInternal(r) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	meta, err := s.app.RoomMeta(r.Context(), roomID)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "rebuild status unavailable")
		return
	}
	writeJSON(w, http.StatusOK, rebuildStatusResponse{
		Sequence:     meta.Sequence,
		Status:       meta.Status,
		StreamClosed: meta.StreamClosed,
		EventCount:   meta.EventCount,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) snapshotHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	auth, err := s.authFromRequest(r, roomID)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "auth lookup unavailable")
		return
	}
	dec, err := s.app.Admission(r.Context(), roomID, auth)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "admission unavailable")
		return
	}
	if !dec.Allowed {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(dec.Code),
			Message: dec.Reason,
		})
		logRequest(r, r.URL.Path)
		return
	}
	raw, err := s.app.SnapshotJSON(r.Context(), roomID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal_error", "snapshot encode failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
	logRequest(r, r.URL.Path)
}

func (s *Server) sseEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	roomID := domain.RoomID(r.PathValue("roomId"))
	if !roomID.Valid() {
		writeError(w, r, http.StatusBadRequest, "bad_request", "roomId is required")
		return
	}
	auth, err := s.authFromRequest(r, roomID)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "auth lookup unavailable")
		return
	}
	dec, err := s.app.Admission(r.Context(), roomID, auth)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "admission unavailable")
		return
	}
	if !dec.Allowed {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(dec.Code),
			Message: dec.Reason,
		})
		logRequest(r, r.URL.Path)
		return
	}
	meta, err := s.app.RoomMeta(r.Context(), roomID)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "room meta unavailable")
		return
	}
	if meta.StreamClosed || s.feed.IsTerminal(roomID) {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(domain.RejectSpectatorTerminal),
			Message: "spectator admission denied after terminal room/match state",
		})
		logRequest(r, r.URL.Path)
		return
	}

	lastID := r.Header.Get("Last-Event-ID")
	sess, subErr := s.feed.BeginSession(r.Context(), roomID, lastID)
	if errors.Is(subErr, errTerminalRoom) {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(domain.RejectSpectatorTerminal),
			Message: "spectator admission denied after terminal room/match state",
		})
		return
	}
	if sess == nil {
		writeError(w, r, http.StatusInternalServerError, "internal_error", "subscription missing")
		return
	}
	defer sess.Close()

	needSnap := errors.Is(subErr, errSnapshotRequired) || lastID == "" || sess.SnapshotRequired()
	var (
		raw []byte
		seq uint64
	)
	if needSnap {
		raw, err = s.app.SnapshotJSON(r.Context(), roomID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, "internal_error", "snapshot encode failed")
			return
		}
		meta, err = s.app.RoomMeta(r.Context(), roomID)
		if err != nil {
			writeError(w, r, http.StatusServiceUnavailable, "store_unavailable", "room meta unavailable")
			return
		}
		seq = meta.Sequence
		if meta.StreamClosed || s.feed.IsTerminal(roomID) {
			writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
				Reason:  string(domain.RejectSpectatorTerminal),
				Message: "spectator admission denied after terminal room/match state",
			})
			return
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, r, http.StatusInternalServerError, "internal_error", "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	if needSnap {
		snap := StreamEvent{
			ID:       "seq_" + strconv.FormatUint(seq, 10),
			Event:    "snapshot",
			Data:     raw,
			Sequence: seq,
		}
		if hub, ok := s.feed.(*StreamHub); ok {
			snap = hub.PublishSnapshot(roomID, seq, raw)
		}
		_ = writeSSE(w, snap)
	}
	for _, ev := range sess.Replay() {
		if err := writeSSE(w, ev); err != nil {
			return
		}
	}

	logRequest(r, r.URL.Path)
	ctx := r.Context()
	events := sess.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				_ = writeSSE(w, StreamEvent{Event: "stream_closed", Data: json.RawMessage(`{}`)})
				return
			}
			if needSnap && ev.Event == "projection_updated" && ev.Sequence <= seq {
				continue
			}
			if err := writeSSE(w, ev); err != nil {
				return
			}
			if ev.Event == "snapshot_required" || ev.Event == "stream_closed" {
				return
			}
		}
	}
}

func (s *Server) authFromRequest(r *http.Request, roomID domain.RoomID) (domain.SpectatorAuth, error) {
	trusted := s.authorizeInternal(r)
	if !trusted {
		return domain.SpectatorAuth{}, nil
	}
	playerID := r.Header.Get("X-Player-Id")
	sessionID := r.Header.Get("X-Session-Id")
	inviteToken := r.Header.Get(headerRoomInvite)
	hasInvite := false
	if inviteToken != "" {
		ok, err := s.app.HasInvite(r.Context(), roomID, inviteToken)
		if err != nil {
			return domain.SpectatorAuth{}, err
		}
		hasInvite = ok
	}
	return domain.SpectatorAuth{
		PlayerID:   domain.PlayerID(playerID),
		SessionID:  sessionID,
		HasInvite:  hasInvite,
		IsOperator: r.Header.Get("X-Operator-Scope") == "1",
	}, nil
}

type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type readyResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Mode    string `json:"mode,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type admissionDeniedResponse struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type rebuildResponse struct {
	Outcomes     []outcomeDTO `json:"outcomes"`
	Sequence     uint64       `json:"sequence"`
	Status       string       `json:"status"`
	StreamClosed bool         `json:"streamClosed"`
	EventCount   int          `json:"eventCount"`
}

type rebuildStatusResponse struct {
	Sequence     uint64 `json:"sequence"`
	Status       string `json:"status"`
	StreamClosed bool   `json:"streamClosed"`
	EventCount   int    `json:"eventCount"`
}

type outcomeDTO struct {
	Kind      string                `json:"kind"`
	EventID   string                `json:"eventId"`
	Sequence  domain.SequenceNumber `json:"sequence"`
	Rejection *rejectionDTO         `json:"rejection,omitempty"`
}

type rejectionDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func outcomeResponse(o domain.ApplyOutcome) outcomeDTO {
	dto := outcomeDTO{
		Kind:     string(o.Kind),
		EventID:  string(o.EventID),
		Sequence: o.Sequence,
	}
	if o.Rejection != nil {
		dto.Rejection = &rejectionDTO{
			Code:    string(o.Rejection.Code),
			Message: o.Rejection.Message,
		}
	}
	return dto
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
	route := r.Pattern
	if route == "" {
		route = path
	}
	processLogger().InfoContext(r.Context(), "request completed",
		"event", "http_request_completed", "route", route,
		"correlationId", r.Header.Get("X-Correlation-Id"))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func main() {
	if err := run(); err != nil {
		slog.Error("spectator view stopped", "event", "process_stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	telemetryRuntime, err := startProcessTelemetry(context.Background())
	if err != nil {
		return fmt.Errorf("start telemetry: %w", err)
	}
	defer shutdownProcessTelemetry(telemetryRuntime)

	if workerRoleFromEnv() == workerRoleSpectatorProjectionRebuilder {
		rt, err := wireProjectionRebuildWorker()
		if err != nil {
			return fmt.Errorf("projection rebuilder startup: %w", err)
		}
		return runProjectionRebuildWorker(rt)
	}

	rt, err := wireSpectatorRuntime()
	if err != nil {
		return err
	}
	defer rt.close()

	srv := newServerFromRuntime(rt)
	if !rt.ready {
		ready := false
		srv.readyOverride = &ready
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	if rt.kafka != nil {
		rt.kafka.start(rootCtx)
		processLogger().InfoContext(rootCtx, "Kafka consumer started", "event", "kafka_consumer_started",
			"group", rt.kafka.consumer.cfg.Group, "topic", rt.kafka.consumer.cfg.Topic)
	}

	httpSrv := &http.Server{Addr: ":8080", Handler: tracedHTTPHandler(srv.routes())}
	errCh := make(chan error, 1)
	go func() {
		processLogger().InfoContext(rootCtx, "spectator view started", "event", "service_started", "mode", rt.mode)
		errCh <- httpSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server: %w", err)
		}
	case <-sigCh:
	}

	rootCancel()
	if rt.kafka != nil {
		rt.kafka.stop()
		rt.kafka = nil // close() must not double-stop
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	return nil
}
