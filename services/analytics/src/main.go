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

	"unoarena/services/analytics/domain"
)

// internalCredentialHeader authenticates service-to-service callers of internal routes.
const internalCredentialHeader = "X-Service-Credential"

// sourceTopicHeader is an optional identity hint; trusted SourceTopic is derived
// from producer credential mapping, never from the request body.
const sourceTopicHeader = "X-Source-Topic"

// ProducerCredentials maps least-privilege producer credentials to trusted sources.
type ProducerCredentials struct {
	Room       string
	Ranking    string
	Tournament string
	Ops        string // rebuild + ingestion-lag only
}

// Server wires HTTP handlers to an injectable analytics application.
type Server struct {
	app            AnalyticsApplication
	creds          ProducerCredentials
	notReadyReason string
	mode           string
	readyCheck     func(context.Context) error
}

// NewServer constructs an Analytics HTTP server in scoped/production mode.
// Ready fails when any role credential is missing or credentials collide.
// Offline and production both require distinct scoped credentials.
func NewServer(app AnalyticsApplication, creds ProducerCredentials) *Server {
	return &Server{
		app:            app,
		creds:          creds,
		notReadyReason: scopedCredentialReadyReason(creds),
		mode:           "scoped",
	}
}

// scopedCredentialReadyReason returns a non-empty reason when scoped credentials
// are incomplete or not pairwise distinct.
func scopedCredentialReadyReason(creds ProducerCredentials) string {
	if creds.Room == "" || creds.Ranking == "" || creds.Tournament == "" || creds.Ops == "" {
		return "producer_credentials_incomplete"
	}
	all := []string{creds.Room, creds.Ranking, creds.Tournament, creds.Ops}
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[i] == all[j] {
				return "producer_credentials_not_distinct"
			}
		}
	}
	return ""
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)

	mux.HandleFunc("/v1/analytics/public/gameplay", s.publicGameplayHandler)
	mux.HandleFunc("/v1/analytics/public/tournaments/", s.publicTournamentHandler)
	mux.HandleFunc("/v1/analytics/public", s.publicAnalyticsHandler)

	mux.HandleFunc("/internal/v1/analytics/room/events", s.ingestRoomEventsHandler)
	mux.HandleFunc("/internal/v1/analytics/ranking/events", s.ingestRankingEventsHandler)
	mux.HandleFunc("/internal/v1/analytics/tournament/events", s.ingestTournamentEventsHandler)
	mux.HandleFunc("/internal/v1/analytics/rebuild", s.rebuildHandler)
	mux.HandleFunc("/internal/v1/analytics/ingestion-lag", s.ingestionLagHandler)
	return mux
}

func (s *Server) authorizeCredential(r *http.Request, expected string) bool {
	if expected == "" {
		return false
	}
	got := r.Header.Get(internalCredentialHeader)
	if got == "" {
		return false
	}
	if len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "analytics"})
	logRequest(r, "/health")
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.app == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "analytics",
			"reason":  "store_unconfigured",
		})
		logRequest(r, "/ready")
		return
	}
	if s.notReadyReason != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "analytics",
			"reason":  s.notReadyReason,
		})
		logRequest(r, "/ready")
		return
	}
	if s.readyCheck != nil {
		if err := s.readyCheck(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status":  "not_ready",
				"service": "analytics",
				"reason":  "clickhouse_schema",
			})
			logRequest(r, "/ready")
			return
		}
	} else if err := s.app.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "not_ready",
			"service": "analytics",
			"reason":  "clickhouse_schema",
		})
		logRequest(r, "/ready")
		return
	}
	mode := s.mode
	if mode == "" {
		mode = "scoped"
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ready",
		"service": "analytics",
		"mode":    mode,
	})
	logRequest(r, "/ready")
}

func (s *Server) publicAnalyticsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.app == nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unconfigured")
		return
	}
	raw, err := s.app.SnapshotJSON(r.Context())
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "snapshot unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
	logRequest(r, "/v1/analytics/public")
}

func (s *Server) publicGameplayHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.app == nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unconfigured")
		return
	}
	snap, err := s.app.Snapshot(r.Context())
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "snapshot unavailable")
		return
	}
	metrics := snap.GameplayMetrics
	if metrics == nil {
		metrics = []domain.GameplayMetric{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"gameplayMetrics": metrics})
	logRequest(r, "/v1/analytics/public/gameplay")
}

func (s *Server) publicTournamentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.app == nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unconfigured")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/analytics/public/tournaments/")
	id = strings.Trim(id, "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, r, http.StatusNotFound, "not_found", "not found")
		return
	}
	snap, err := s.app.Snapshot(r.Context())
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "snapshot unavailable")
		return
	}
	filtered := make([]domain.TournamentStatistic, 0)
	for _, ts := range snap.TournamentStats {
		if string(ts.TournamentID) == id {
			filtered = append(filtered, ts)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tournamentId":    id,
		"tournamentStats": filtered,
	})
	logRequest(r, r.URL.Path)
}

func (s *Server) ingestRoomEventsHandler(w http.ResponseWriter, r *http.Request) {
	s.ingestWithSource(w, r, s.creds.Room, domain.SourceRoomGameplayMetrics)
}

func (s *Server) ingestRankingEventsHandler(w http.ResponseWriter, r *http.Request) {
	s.ingestWithSource(w, r, s.creds.Ranking, "")
}

func (s *Server) ingestTournamentEventsHandler(w http.ResponseWriter, r *http.Request) {
	s.ingestWithSource(w, r, s.creds.Tournament, "")
}

func (s *Server) ingestWithSource(w http.ResponseWriter, r *http.Request, cred string, fixed domain.SourceTopic) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeCredential(r, cred) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.app == nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unconfigured")
		return
	}
	var body upstreamEventBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	source, ok := resolveTrustedSource(cred, s.creds, fixed, r.Header.Get(sourceTopicHeader), body.EventType)
	if !ok {
		writeError(w, r, http.StatusForbidden, "forbidden", "credential cannot publish this event type/source")
		return
	}
	// Body source is ignored — trusted SourceTopic comes from credential mapping.
	evt := body.toDomain()
	evt.Source = source
	out, err := s.app.Apply(r.Context(), evt)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unavailable")
		return
	}
	status := http.StatusOK
	if out.Kind == domain.OutcomeQuarantined {
		status = http.StatusConflict
	}
	writeJSON(w, status, outcomeResponse(out))
	logRequest(r, r.URL.Path)
}

func resolveTrustedSource(gotCred string, creds ProducerCredentials, fixed domain.SourceTopic, headerHint, eventType string) (domain.SourceTopic, bool) {
	if fixed != "" {
		return fixed, true
	}
	if subtle.ConstantTimeCompare([]byte(gotCred), []byte(creds.Ranking)) == 1 && creds.Ranking != "" {
		switch domain.EventType(eventType) {
		case domain.EventRatingStatistic:
			return domain.SourceRankingPlayerRatingUpdated, true
		case domain.EventLeaderboardSnapshot:
			return domain.SourceRankingLeaderboardSnapshot, true
		}
		if headerHint != "" {
			st := domain.SourceTopic(headerHint)
			if st == domain.SourceRankingPlayerRatingUpdated || st == domain.SourceRankingLeaderboardSnapshot {
				return st, true
			}
		}
		return "", false
	}
	if subtle.ConstantTimeCompare([]byte(gotCred), []byte(creds.Tournament)) == 1 && creds.Tournament != "" {
		switch domain.EventType(eventType) {
		case domain.EventTournamentStatistic:
			if headerHint != "" {
				st := domain.SourceTopic(headerHint)
				switch st {
				case domain.SourceTournamentMatchAssigned,
					domain.SourceTournamentMatchResultRecorded,
					domain.SourceTournamentPlayersAdvanced,
					domain.SourceTournamentRoundCompleted:
					return st, true
				}
			}
			return domain.SourceTournamentRoundCompleted, true
		}
		return "", false
	}
	return "", false
}

func (s *Server) rebuildHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeCredential(r, s.creds.Ops) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.app == nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unconfigured")
		return
	}
	var body struct {
		Events []upstreamEventBody `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	events := make([]domain.UpstreamEvent, 0, len(body.Events))
	for _, e := range body.Events {
		evt := e.toDomain()
		// Rebuild is operational: SourceTopic must be supplied via header identity
		// per event, never trusted from an unauthenticated body alone. Ops rebuild
		// may set source explicitly in the envelope only when ops-credentialed.
		if !evt.Source.Valid() {
			writeError(w, r, http.StatusBadRequest, "bad_request", "trusted source required for rebuild events")
			return
		}
		events = append(events, evt)
	}
	outs, err := s.app.Rebuild(r.Context(), events)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "rebuild unavailable")
		return
	}
	resp := make([]map[string]any, 0, len(outs))
	for _, out := range outs {
		resp = append(resp, outcomeResponse(out))
	}
	writeJSON(w, http.StatusOK, map[string]any{"outcomes": resp})
	logRequest(r, "/internal/v1/analytics/rebuild")
}

func (s *Server) ingestionLagHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorizeCredential(r, s.creds.Ops) {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid service credential")
		return
	}
	if s.app == nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "store unconfigured")
		return
	}
	version, err := s.app.ProjectionVersion(r.Context())
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "projection unavailable")
		return
	}
	mode := s.mode
	if mode == "" {
		mode = "offline"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mode":              mode,
		"projectionVersion": version,
		"lagSeconds":        0,
	})
	logRequest(r, "/internal/v1/analytics/ingestion-lag")
}

type upstreamEventBody struct {
	EventID       string         `json:"eventId"`
	EventType     string         `json:"eventType"`
	Source        string         `json:"source"` // ignored on producer ingest routes
	SchemaVersion int            `json:"schemaVersion"`
	CorrelationID string         `json:"correlationId"`
	OccurredAt    time.Time      `json:"occurredAt"`
	Payload       map[string]any `json:"payload"`
}

func (b upstreamEventBody) toDomain() domain.UpstreamEvent {
	return domain.UpstreamEvent{
		EventID:       domain.EventID(b.EventID),
		EventType:     domain.EventType(b.EventType),
		Source:        domain.SourceTopic(b.Source),
		SchemaVersion: b.SchemaVersion,
		CorrelationID: b.CorrelationID,
		OccurredAt:    b.OccurredAt,
		Payload:       b.Payload,
	}
}

func outcomeResponse(out domain.ApplyOutcome) map[string]any {
	resp := map[string]any{
		"kind":    string(out.Kind),
		"eventId": string(out.EventID),
	}
	if out.Rejection != nil {
		resp["rejection"] = map[string]string{
			"code":    string(out.Rejection.Code),
			"message": out.Rejection.Message,
		}
	}
	return resp
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
	log.Printf(`{"level":"info","service":"analytics","event":"request","path":%q,"correlationId":%q}`,
		path, r.Header.Get("X-Correlation-Id"))
}

func main() {
	rt, err := wireAnalyticsRuntime()
	if err != nil {
		log.Fatal(err)
	}
	srv := NewServer(rt.app, rt.creds)
	srv.mode = rt.mode
	if !rt.ready {
		srv.notReadyReason = rt.readyReason
	} else if rt.mode == "durable" {
		srv.readyCheck = rt.durableReady
		srv.mode = "durable"
	} else if rt.mode == "capability" {
		srv.mode = "capability"
	}

	httpSrv := &http.Server{Addr: ":8080", Handler: srv.routes()}
	errCh := make(chan error, 1)
	go func() {
		log.Printf(`{"level":"info","service":"analytics","event":"startup","mode":%q}`, rt.mode)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}
