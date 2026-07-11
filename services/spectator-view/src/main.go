package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"unoarena/services/spectator-view/domain"
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

// Server wires HTTP handlers to the Spectator View domain projection store.
type Server struct {
	store              ProjectionStore
	hub                *StreamHub
	internalCredential string
}

// NewServer constructs a Spectator View HTTP server.
func NewServer(store ProjectionStore, internalCredential string) *Server {
	return &Server{
		store:              store,
		hub:                NewStreamHub(),
		internalCredential: internalCredential,
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
	if !s.isReady() {
		reason := "store_unconfigured"
		if s.store != nil && s.internalCredential == "" {
			reason = "internal_credential_unconfigured"
		}
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{
			Status:  "not_ready",
			Service: "spectator-view",
			Reason:  reason,
		})
		logRequest(r, "/ready")
		return
	}
	writeJSON(w, http.StatusOK, readyResponse{
		Status:  "ready",
		Service: "spectator-view",
		Mode:    "offline",
	})
	logRequest(r, "/ready")
}

func (s *Server) isReady() bool {
	return s.store != nil && s.internalCredential != ""
}

func (s *Server) requireReady(w http.ResponseWriter, r *http.Request) bool {
	if s.isReady() {
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
	// Raw client invite/operator booleans are never accepted; invites use X-Room-Invite.
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
	if !s.store.RegisterInvite(roomID, token) {
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
	auth := domain.SpectatorAuth{
		PlayerID:   domain.PlayerID(body.PlayerID),
		SessionID:  body.SessionID,
		HasInvite:  inviteToken != "" && s.store.HasInvite(roomID, inviteToken),
		IsOperator: body.Operator, // trusted Gateway body only on credentialed route
	}
	var dec domain.SpectatorAdmissionDecision
	s.store.WithRoom(roomID, func(proj *domain.SpectatorRoomProjection) {
		dec = proj.Admission(auth)
	})
	writeJSON(w, http.StatusOK, admissionResponse{Allowed: dec.Allowed, Reason: dec.Reason})
	logRequest(r, r.URL.Path)
}

// canonicalSpectatorEvent is the single Room/Gateway spectator-safe envelope.
// No nested envelope: data holds flat fact fields only.
type canonicalSpectatorEvent struct {
	SchemaVersion int            `json:"schemaVersion"`
	EventID       string         `json:"eventId"`
	Stream        string         `json:"stream"`
	RoomID        string         `json:"roomId"`
	Sequence      uint64         `json:"sequence"`
	Event         string         `json:"event"`
	Data          map[string]any `json:"data"`

	// Batch: multiple facts at one sequence applied atomically.
	Facts []canonicalFact `json:"facts"`

	// Legacy aliases accepted only as flat top-level fields (not nested envelopes).
	EventType      string         `json:"eventType"`
	SequenceNumber uint64         `json:"sequenceNumber"`
	Payload        map[string]any `json:"payload"`
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

	var out domain.ApplyOutcome
	s.store.WithRoomCreate(roomID, func(proj *domain.SpectatorRoomProjection) {
		if len(events) == 1 {
			out = proj.Apply(events[0])
		} else {
			out = proj.ApplyBatch(events)
		}
		if out.Kind == domain.OutcomeAccepted {
			if raw, err := proj.SnapshotJSON(); err == nil {
				// Publish under the same room lock so ingest order into the hub
				// is sequence-guarded with projection apply.
				s.hub.PublishUpdate(roomID, uint64(out.Sequence), raw)
			}
			if proj.StreamClosed() {
				s.hub.MarkTerminal(roomID)
			}
		}
	})
	if out.Kind == domain.OutcomeAccepted {
		s.store.IncrEventCount(roomID)
	}
	status := http.StatusOK
	switch out.Kind {
	case domain.OutcomeQuarantined, domain.OutcomeDropped:
		status = http.StatusConflict
	case domain.OutcomeIgnored:
		// Ignored (e.g. already-applied terminal) remains 200 so publishers can advance.
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

	proj, outcomes := domain.RebuildFrom(roomID, events)
	if ms, ok := s.store.(*MemoryProjectionStore); ok {
		ms.ReplaceProjection(roomID, proj, len(events))
	} else {
		s.store.WithRoomCreate(roomID, func(p *domain.SpectatorRoomProjection) {
			*p = *proj
		})
		s.store.SetEventCount(roomID, len(events))
	}
	if proj.StreamClosed() {
		s.hub.MarkTerminal(roomID)
	} else if raw, err := proj.SnapshotJSON(); err == nil {
		s.hub.PublishUpdate(roomID, uint64(proj.Sequence()), raw)
	}

	outs := make([]outcomeDTO, 0, len(outcomes))
	for _, o := range outcomes {
		outs = append(outs, outcomeResponse(o))
	}
	writeJSON(w, http.StatusOK, rebuildResponse{
		Outcomes:     outs,
		Sequence:     uint64(proj.Sequence()),
		Status:       string(proj.Status()),
		StreamClosed: proj.StreamClosed(),
		EventCount:   s.store.EventCount(roomID),
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
	seq, status, closed, count, _ := s.store.RoomMeta(roomID)
	writeJSON(w, http.StatusOK, rebuildStatusResponse{
		Sequence:     seq,
		Status:       status,
		StreamClosed: closed,
		EventCount:   count,
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
	auth := s.authFromRequest(r, roomID)
	var (
		allowed bool
		dec     domain.SpectatorAdmissionDecision
		raw     []byte
		err     error
	)
	s.store.WithRoomCreate(roomID, func(proj *domain.SpectatorRoomProjection) {
		dec = proj.Admission(auth)
		allowed = dec.Allowed
		if allowed {
			raw, err = proj.SnapshotJSON()
		}
	})
	if !allowed {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(dec.Code),
			Message: dec.Reason,
		})
		logRequest(r, r.URL.Path)
		return
	}
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
	auth := s.authFromRequest(r, roomID)
	var (
		allowed bool
		dec     domain.SpectatorAdmissionDecision
		closed  bool
	)
	s.store.WithRoomCreate(roomID, func(proj *domain.SpectatorRoomProjection) {
		dec = proj.Admission(auth)
		allowed = dec.Allowed
		if allowed {
			closed = proj.StreamClosed()
		}
	})
	if !allowed {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(dec.Code),
			Message: dec.Reason,
		})
		logRequest(r, r.URL.Path)
		return
	}
	if closed || s.hub.IsTerminal(roomID) {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(domain.RejectSpectatorTerminal),
			Message: "spectator admission denied after terminal room/match state",
		})
		logRequest(r, r.URL.Path)
		return
	}

	// Register before capturing/writing snapshot so live updates queue on the
	// channel and cannot be lost between snapshot and subscribe.
	lastID := r.Header.Get("Last-Event-ID")
	_, events, replay, cancel, subErr := s.hub.Subscribe(roomID, lastID)
	if errors.Is(subErr, errTerminalRoom) {
		writeJSON(w, http.StatusForbidden, admissionDeniedResponse{
			Reason:  string(domain.RejectSpectatorTerminal),
			Message: "spectator admission denied after terminal room/match state",
		})
		return
	}
	if cancel == nil {
		writeError(w, r, http.StatusInternalServerError, "internal_error", "subscription missing cancel")
		return
	}
	defer cancel()

	needSnap := errors.Is(subErr, errSnapshotRequired) || lastID == ""
	var (
		raw []byte
		seq uint64
		err error
	)
	if needSnap {
		s.store.WithRoomCreate(roomID, func(proj *domain.SpectatorRoomProjection) {
			raw, err = proj.SnapshotJSON()
			seq = uint64(proj.Sequence())
			closed = proj.StreamClosed()
		})
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, "internal_error", "snapshot encode failed")
			return
		}
		if closed || s.hub.IsTerminal(roomID) {
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
		snap := s.hub.PublishSnapshot(roomID, seq, raw)
		_ = writeSSE(w, snap)
	}
	for _, ev := range replay {
		if err := writeSSE(w, ev); err != nil {
			return
		}
	}

	logRequest(r, r.URL.Path)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				_ = writeSSE(w, StreamEvent{Event: "stream_closed", Data: json.RawMessage(`{}`)})
				return
			}
			// Queued updates already covered by the snapshot are skipped.
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

// authFromRequest builds SpectatorAuth for snapshot/SSE.
// Invite and operator authority never come from query strings. Opaque
// X-Room-Invite tokens are validated against the server-side invite registry
// only when the caller presents a trusted Gateway service credential.
// Operator scope is accepted only from Gateway-forwarded X-Operator-Scope
// under that same credential. Without Gateway credential, only anonymous
// public-room admission is possible (participant headers are ignored).
func (s *Server) authFromRequest(r *http.Request, roomID domain.RoomID) domain.SpectatorAuth {
	trusted := s.authorizeInternal(r)
	if !trusted {
		return domain.SpectatorAuth{}
	}
	playerID := r.Header.Get("X-Player-Id")
	sessionID := r.Header.Get("X-Session-Id")
	inviteToken := r.Header.Get(headerRoomInvite)
	return domain.SpectatorAuth{
		PlayerID:   domain.PlayerID(playerID),
		SessionID:  sessionID,
		HasInvite:  inviteToken != "" && s.store.HasInvite(roomID, inviteToken),
		IsOperator: r.Header.Get("X-Operator-Scope") == "1",
	}
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
	log.Printf(`{"level":"info","service":"spectator-view","event":"request","path":%q,"correlationId":%q}`,
		path, r.Header.Get("X-Correlation-Id"))
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
	cred := os.Getenv("SPECTATOR_VIEW_INTERNAL_CREDENTIAL")
	store := NewMemoryProjectionStore()
	srv := NewServer(store, cred)

	log.Printf(`{"level":"info","service":"spectator-view","event":"startup","mode":"offline"}`)
	log.Fatal(http.ListenAndServe(":8080", srv.routes()))
}
