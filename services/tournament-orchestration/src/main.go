package main

import (
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
	"syscall"
	"time"

	"unoarena/services/tournament-orchestration/store"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
	"unoarena/shared/httpx"
)

const (
	headerServiceCredential = "X-Service-Credential"
	maxBodyBytes            = 1 << 20
)

// Server exposes Tournament Orchestration HTTP handlers (stdlib only).
type Server struct {
	svc                         *Service
	internalCredential          string
	analyticsBackfillCredential string
	mode                        string
	readyReason                 string
	durableReady                func(context.Context) error
}

func NewServer(svc *Service, internalCredential string) *Server {
	return NewServerWithScopedCreds(svc, internalCredential, "")
}

// NewServerWithScopedCreds constructs a server with a dedicated Analytics backfill credential.
func NewServerWithScopedCreds(svc *Service, internalCredential, analyticsBackfillCredential string) *Server {
	return &Server{
		svc:                         svc,
		internalCredential:          internalCredential,
		analyticsBackfillCredential: analyticsBackfillCredential,
		mode:                        "capability",
	}
}

func serverFromRuntime(rt tournamentRuntime, cred, analyticsBackfillCred string) *Server {
	return &Server{
		svc:                         rt.svc,
		internalCredential:          cred,
		analyticsBackfillCredential: analyticsBackfillCred,
		mode:                        rt.mode,
		readyReason:                 rt.readyReason,
		durableReady:                rt.durableReady,
	}
}

// Routes returns the injectable HTTP handler tree.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)

	// Gateway-configured command hop.
	mux.HandleFunc("/internal/v1/commands", s.handleInternalCommands)

	// Analytics recovery backfill (ADR-0039) — dedicated route before tournament catch-all.
	mux.HandleFunc("/internal/v1/tournaments/analytics-backfill", s.handleAnalyticsBackfill)

	// Documented internal worker/consumer routes.
	mux.HandleFunc("/internal/v1/tournaments/", s.handleInternalTournaments)

	// Operator repair: rebuild Redis bracket projection from Postgres (internal auth).
	mux.HandleFunc("/internal/v1/brackets/rebuild", s.handleBracketRebuild)

	// Documented BFF-aligned public/read and command surfaces.
	mux.HandleFunc("/v1/tournaments", s.handleTournamentsRoot)
	mux.HandleFunc("/v1/tournaments/", s.handleTournamentsScoped)
	return mux
}

func (s *Server) authorizeInternal(r *http.Request) bool {
	if s.internalCredential == "" {
		return false
	}
	got := r.Header.Get(headerServiceCredential)
	if got == "" || len(got) != len(s.internalCredential) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.internalCredential)) == 1
}

func (s *Server) authorizeAnalyticsBackfill(r *http.Request) bool {
	// Scoped Analytics pair credential only — never fall back to Gateway/generic internal.
	if s.analyticsBackfillCredential == "" {
		return false
	}
	got := r.Header.Get(headerServiceCredential)
	if got == "" || len(got) != len(s.analyticsBackfillCredential) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.analyticsBackfillCredential)) == 1
}

func (s *Server) handleAnalyticsBackfill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
		return
	}
	if !s.requireReady(w, r) {
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
	var req AnalyticsBackfillRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json", corr.CorrelationID, "")
		return
	}
	resp, err := s.svc.AnalyticsBackfill(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, ErrAnalyticsBackfillBadRequest):
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", err.Error(), corr.CorrelationID, "")
		case errors.Is(err, ErrAnalyticsBackfillUnavailable):
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", "analytics backfill unavailable", corr.CorrelationID, "")
		case errors.Is(err, ErrAnalyticsBackfillCorrupt):
			_ = httpx.WriteError(w, http.StatusInternalServerError, "corrupt_outbox", "stored envelope failed validation", corr.CorrelationID, "")
		default:
			_ = httpx.WriteError(w, http.StatusInternalServerError, "internal_error", "analytics backfill failed", corr.CorrelationID, "")
		}
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "tournament-orchestration"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	if !s.isReady() {
		reason := s.readyReason
		if reason == "" {
			reason = "not_ready"
		}
		_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", reason, correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	if s.durableReady != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.durableReady(ctx); err != nil {
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", "schema_or_database_unavailable", correlation.FromHTTP(r.Header).CorrelationID, "")
			return
		}
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "tournament-orchestration"})
}

func (s *Server) isReady() bool {
	if s.internalCredential == "" {
		return false
	}
	switch s.mode {
	case "misconfigured":
		return false
	case "durable":
		return s.durableReady != nil && s.readyReason == ""
	default:
		return s.readyReason == ""
	}
}

func (s *Server) requireReady(w http.ResponseWriter, r *http.Request) bool {
	if s.isReady() {
		return true
	}
	_ = httpx.WriteError(w, http.StatusServiceUnavailable, "not_ready", "required dependencies not configured", correlation.FromHTTP(r.Header).CorrelationID, "")
	return false
}

func (s *Server) handleInternalCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	s.submitCommandHTTP(w, r, "")
}

func (s *Server) handleBracketRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	tournamentID := strings.TrimSpace(r.URL.Query().Get("tournamentId"))
	if tournamentID == "" {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "tournamentId required", corr.CorrelationID, "")
		return
	}
	if err := s.svc.RebuildBracketProjection(r.Context(), tournamentID); err != nil {
		if errors.Is(err, store.ErrTournamentNotFound) {
			_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "tournament not found", corr.CorrelationID, "")
			return
		}
		_ = httpx.WriteError(w, http.StatusServiceUnavailable, "unavailable", "bracket rebuild unavailable", corr.CorrelationID, "")
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"status":       "rebuilt",
		"tournamentId": tournamentID,
	})
}

func (s *Server) handleTournamentsRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/tournaments" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "not found", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	if r.Method != http.MethodPost {
		_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	body, err := readBody(r)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	payload := map[string]any{}
	_ = json.Unmarshal(body, &payload)
	cmdID := stringFromRaw(raw, "commandId")
	if cmdID == "" {
		cmdID = s.svc.ids.NewID("cmd")
	}
	schemaVersion := intFromRaw(raw, "schemaVersion")
	if schemaVersion == 0 {
		schemaVersion = envelope.CurrentSchemaVersion
	}
	pl, _ := json.Marshal(map[string]any{
		"tournamentId": stringFromMap(payload, "tournamentId"),
		"capacity":     payload["capacity"],
		"retryBudget":  payload["retryBudget"],
		"batchSize":    payload["batchSize"],
		"visibility":   payload["visibility"],
	})
	req := CommandRequest{
		CommandID:     cmdID,
		Type:          CmdCreateTournament,
		SchemaVersion: schemaVersion,
		Payload:       pl,
		PlayerID:      stringFromMap(payload, "playerId"),
		SessionID:     stringFromMap(payload, "sessionId"),
	}
	s.writeCommandResult(w, r, req)
}

func (s *Server) handleTournamentsScoped(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/tournaments/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "not found", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	tournamentID := parts[0]
	corr := correlation.FromHTTP(r.Header).WithDefaults()

	isBracket := len(parts) == 2 && parts[1] == "bracket" && r.Method == http.MethodGet
	isStandings := len(parts) == 2 && parts[1] == "standings" && r.Method == http.MethodGet
	isAssignment := len(parts) == 4 && parts[1] == "players" && parts[3] == "assignment" && r.Method == http.MethodGet
	if isBracket || isStandings || isAssignment {
		if !s.authorizeInternal(r) {
			_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", corr.CorrelationID, "")
			return
		}
		principal := parseTrustedReadPrincipal(r.Header.Get("X-Player-Id"), r.Header.Get("X-Operator-Scope"))

		if isAssignment {
			playerID := parts[2]
			body, err := s.svc.PlayerAssignment(tournamentID, playerID, principal)
			if err != nil {
				if status, code, msg, ok := mapTournamentReadErr(err); ok {
					if errors.Is(err, store.ErrAssignmentNotFound) {
						msg = "assignment not found"
					}
					_ = httpx.WriteError(w, status, code, msg, corr.CorrelationID, "")
					return
				}
				_ = httpx.WriteError(w, http.StatusServiceUnavailable, "unavailable", "assignment read unavailable", corr.CorrelationID, "")
				return
			}
			_ = httpx.WriteJSON(w, http.StatusOK, body)
			return
		}

		if err := s.svc.AuthorizeBracketStandings(tournamentID, principal); err != nil {
			if status, code, msg, ok := mapTournamentReadErr(err); ok {
				_ = httpx.WriteError(w, status, code, msg, corr.CorrelationID, "")
				return
			}
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "unavailable", "tournament read unavailable", corr.CorrelationID, "")
			return
		}

		if isBracket {
			q := r.URL.Query()
			var roundNumber *int
			if raw := strings.TrimSpace(q.Get("roundNumber")); raw != "" {
				n, err := strconv.Atoi(raw)
				if err != nil || n < 1 {
					_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid roundNumber", corr.CorrelationID, "")
					return
				}
				roundNumber = &n
			}
			limit := store.DefaultBracketPageLimit
			if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
				n, err := strconv.Atoi(raw)
				if err != nil || n < 1 || n > store.MaxBracketPageLimit {
					_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid limit", corr.CorrelationID, "")
					return
				}
				limit = n
			}
			cursor := strings.TrimSpace(q.Get("cursor"))
			body, err := s.svc.Bracket(tournamentID, roundNumber, cursor, limit)
			if err != nil {
				if status, code, msg, ok := mapTournamentReadErr(err); ok {
					_ = httpx.WriteError(w, status, code, msg, corr.CorrelationID, "")
					return
				}
				_ = httpx.WriteError(w, http.StatusServiceUnavailable, "unavailable", "bracket read unavailable", corr.CorrelationID, "")
				return
			}
			_ = httpx.WriteJSON(w, http.StatusOK, body)
			return
		}

		body, err := s.svc.Standings(tournamentID)
		if err != nil {
			if status, code, msg, ok := mapTournamentReadErr(err); ok {
				_ = httpx.WriteError(w, status, code, msg, corr.CorrelationID, "")
				return
			}
			_ = httpx.WriteError(w, http.StatusServiceUnavailable, "unavailable", "standings read unavailable", corr.CorrelationID, "")
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, body)
		return
	}

	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", corr.CorrelationID, "")
		return
	}

	if len(parts) == 2 && parts[1] == "registrations" && r.Method == http.MethodPost {
		body, err := readBody(r)
		if err != nil {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", corr.CorrelationID, "")
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body", corr.CorrelationID, "")
			return
		}
		cmdID := stringFromMap(payload, "commandId")
		if cmdID == "" {
			cmdID = s.svc.ids.NewID("cmd")
		}
		schemaVersion := intFromAny(payload["schemaVersion"])
		if schemaVersion == 0 {
			schemaVersion = envelope.CurrentSchemaVersion
		}
		playerID := stringFromMap(payload, "playerId")
		pl, _ := json.Marshal(map[string]any{
			"tournamentId": tournamentID,
			"playerId":     playerID,
		})
		s.writeCommandResult(w, r, CommandRequest{
			CommandID:     cmdID,
			Type:          CmdRegisterPlayer,
			SchemaVersion: schemaVersion,
			Payload:       pl,
			PlayerID:      playerID,
			SessionID:     stringFromMap(payload, "sessionId"),
		})
		return
	}

	if len(parts) == 2 && parts[1] == "commands" && r.Method == http.MethodPost {
		s.submitCommandHTTP(w, r, tournamentID)
		return
	}

	_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "not found", corr.CorrelationID, "")
}

func (s *Server) handleInternalTournaments(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	if !s.authorizeInternal(r) {
		_ = httpx.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid service credential", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/internal/v1/tournaments/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "not found", correlation.FromHTTP(r.Header).CorrelationID, "")
		return
	}
	tournamentID := parts[0]

	// POST .../match-results
	if len(parts) == 2 && parts[1] == "match-results" {
		if r.Method != http.MethodPost {
			_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
			return
		}
		s.handleMatchResults(w, r, tournamentID)
		return
	}

	// POST .../rounds/{n}/provisioning-batches
	if len(parts) == 4 && parts[1] == "rounds" && parts[3] == "provisioning-batches" {
		if r.Method != http.MethodPost {
			_ = httpx.WriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", correlation.FromHTTP(r.Header).CorrelationID, "")
			return
		}
		roundNumber, err := strconv.Atoi(parts[2])
		if err != nil || roundNumber < 1 {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid roundNumber", correlation.FromHTTP(r.Header).CorrelationID, "")
			return
		}
		body, err := readBody(r)
		if err != nil {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", correlation.FromHTTP(r.Header).CorrelationID, "")
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body", correlation.FromHTTP(r.Header).CorrelationID, "")
			return
		}
		corr := correlation.FromHTTP(r.Header).WithDefaults()
		cmdID := stringFromMap(payload, "commandId")
		if cmdID == "" {
			cmdID = s.svc.ids.NewID("cmd")
		}
		work := ProvisioningBatchWork{
			CommandID:     cmdID,
			TournamentID:  tournamentID,
			RoundNumber:   roundNumber,
			BatchID:       stringFromMap(payload, "batchId"),
			SlotFrom:      stringFromMap(payload, "slotFrom"),
			SlotTo:        stringFromMap(payload, "slotTo"),
			SlotSize:      intFromAny(payload["slotSize"]),
			RetryAttempt:  intFromAny(payload["retryAttempt"]),
			CorrelationID: firstNonEmpty(stringFromMap(payload, "correlationId"), corr.CorrelationID),
		}
		result, err := s.svc.ProcessProvisioningBatch(r.Context(), work)
		if err != nil {
			_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", err.Error(), corr.CorrelationID, cmdID)
			return
		}
		w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
		w.Header().Set(correlation.HeaderCommandID, cmdID)
		_ = httpx.WriteJSON(w, http.StatusOK, result)
		return
	}

	_ = httpx.WriteError(w, http.StatusNotFound, "not_found", "not found", correlation.FromHTTP(r.Header).CorrelationID, "")
}

func (s *Server) handleMatchResults(w http.ResponseWriter, r *http.Request, tournamentID string) {
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	body, err := readBody(r)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", corr.CorrelationID, "")
		return
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body", corr.CorrelationID, "")
		return
	}
	bodyTournamentID := stringFromMap(raw, "tournamentId")
	if bodyTournamentID != "" && bodyTournamentID != tournamentID {
		_ = httpx.WriteError(w, http.StatusBadRequest, "tournament_id_mismatch", "path tournamentId does not match payload", corr.CorrelationID, "")
		return
	}
	evt := MatchCompletedEvent{
		EventID:           stringFromMap(raw, "eventId"),
		EventType:         stringFromMap(raw, "eventType"),
		SchemaVersion:     intFromAny(raw["schemaVersion"]),
		CorrelationID:     firstNonEmpty(stringFromMap(raw, "correlationId"), corr.CorrelationID),
		CausationID:       stringFromMap(raw, "causationId"),
		RoomID:            stringFromMap(raw, "roomId"),
		TournamentID:      firstNonEmpty(bodyTournamentID, tournamentID),
		RoundNumber:       intFromAny(raw["roundNumber"]),
		SlotID:            stringFromMap(raw, "slotId"),
		CompletionVersion: uintFromAny(raw["completionVersion"]),
	}
	if _, ok := raw["isAbandoned"]; ok {
		evt.HasIsAbandoned = true
		evt.IsAbandoned = boolFromAny(raw["isAbandoned"])
	}
	if ts := stringFromMap(raw, "occurredAt"); ts != "" {
		if at, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			evt.OccurredAt = at
		} else if at, err := time.Parse(time.RFC3339, ts); err == nil {
			evt.OccurredAt = at
		}
	}
	if players, ok := raw["players"].([]any); ok {
		for _, p := range players {
			pm, ok := p.(map[string]any)
			if !ok {
				continue
			}
			standing := MatchPlayerStanding{
				PlayerID:             stringFromMap(pm, "playerId"),
				MatchWins:            intFromAny(pm["matchWins"]),
				CumulativeCardPoints: intFromAny(pm["cumulativeCardPoints"]),
				Forfeited:            boolFromAny(pm["forfeited"]),
			}
			if ts := stringFromMap(pm, "finalGameCompletedAt"); ts != "" {
				if at, err := time.Parse(time.RFC3339Nano, ts); err == nil {
					standing.FinalGameCompletedAt = at
				} else if at, err := time.Parse(time.RFC3339, ts); err == nil {
					standing.FinalGameCompletedAt = at
				}
			}
			evt.Players = append(evt.Players, standing)
		}
	}
	if forfeits, ok := raw["forfeits"].([]any); ok {
		for _, f := range forfeits {
			if s, ok := f.(string); ok {
				evt.Forfeits = append(evt.Forfeits, s)
			}
		}
	}

	result, err := s.svc.IngestMatchCompleted(r.Context(), evt)
	if err != nil {
		if strings.Contains(err.Error(), "tournament_not_found") {
			_ = httpx.WriteError(w, http.StatusNotFound, "not_found", err.Error(), corr.CorrelationID, "")
			return
		}
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", err.Error(), corr.CorrelationID, "")
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) submitCommandHTTP(w http.ResponseWriter, r *http.Request, pathTournamentID string) {
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	body, err := readBody(r)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "bad_request", "unable to read body", corr.CorrelationID, "")
		return
	}
	if err := requireExplicitSchemaVersion1(body); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", err.Error(), corr.CorrelationID, "")
		return
	}

	var req CommandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", "invalid json body", corr.CorrelationID, "")
		return
	}
	if headerCmd := strings.TrimSpace(r.Header.Get(correlation.HeaderCommandID)); headerCmd != "" && headerCmd != req.CommandID {
		_ = httpx.WriteError(w, http.StatusBadRequest, "command_id_mismatch", "X-Command-Id does not match body commandId", corr.CorrelationID, req.CommandID)
		return
	}
	if pathTournamentID != "" {
		var payload map[string]any
		_ = json.Unmarshal(req.Payload, &payload)
		if payload == nil {
			payload = map[string]any{}
		}
		if tid, _ := payload["tournamentId"].(string); tid != "" && tid != pathTournamentID {
			_ = httpx.WriteError(w, http.StatusBadRequest, "tournament_id_mismatch", "path tournamentId does not match payload", corr.CorrelationID, req.CommandID)
			return
		}
		payload["tournamentId"] = pathTournamentID
		req.Payload, _ = json.Marshal(payload)
	}
	corr.CommandID = req.CommandID
	if corr.CorrelationID == "" {
		corr.CorrelationID = req.CommandID
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set(correlation.HeaderCommandID, req.CommandID)

	result, err := s.svc.SubmitCommand(r.Context(), req, corr.CorrelationID)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", err.Error(), corr.CorrelationID, req.CommandID)
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) writeCommandResult(w http.ResponseWriter, r *http.Request, req CommandRequest) {
	corr := correlation.FromHTTP(r.Header).WithDefaults()
	if corr.CorrelationID == "" {
		corr.CorrelationID = req.CommandID
	}
	corr.CommandID = req.CommandID
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set(correlation.HeaderCommandID, req.CommandID)
	if req.SchemaVersion != envelope.CurrentSchemaVersion {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", "schemaVersion must be 1", corr.CorrelationID, req.CommandID)
		return
	}
	result, err := s.svc.SubmitCommand(r.Context(), req, corr.CorrelationID)
	if err != nil {
		_ = httpx.WriteError(w, http.StatusBadRequest, "invalid_envelope", err.Error(), corr.CorrelationID, req.CommandID)
		return
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

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
}

func stringFromRaw(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return strings.TrimSpace(s)
	}
	return ""
}

func intFromRaw(raw map[string]json.RawMessage, key string) int {
	v, ok := raw[key]
	if !ok {
		return 0
	}
	var n int
	if err := json.Unmarshal(v, &n); err == nil {
		return n
	}
	return 0
}

func stringFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		return strings.TrimSpace(stringify(t))
	}
}

func stringify(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		return s
	}
	return strings.Trim(string(b), `"`)
}

func intFromAny(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(t))
		return n
	default:
		return 0
	}
}

func uintFromAny(v any) uint64 {
	switch t := v.(type) {
	case float64:
		return uint64(t)
	case int:
		return uint64(t)
	case int64:
		return uint64(t)
	case uint64:
		return t
	case json.Number:
		n, _ := t.Int64()
		return uint64(n)
	case string:
		n, _ := strconv.ParseUint(strings.TrimSpace(t), 10, 64)
		return n
	default:
		return 0
	}
}

func boolFromAny(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	default:
		return false
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func main() {
	if err := runTournamentProcess(); err != nil {
		if tournamentProcessTelemetry != nil {
			tournamentProcessTelemetry.Logger.Error("tournament process stopped", "event", "process_stopped", "error", err.Error())
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func runTournamentProcess() error {
	telemetryRuntime, err := startTournamentTelemetry(context.Background())
	if err != nil {
		return fmt.Errorf("tournament telemetry: %w", err)
	}
	defer shutdownTournamentTelemetry(telemetryRuntime)
	store.ConfigureTelemetry(telemetryRuntime.TracerProvider, telemetryRuntime.MeterProvider)

	cred := os.Getenv("TOURNAMENT_INTERNAL_CREDENTIAL")
	if cred == "" {
		cred = os.Getenv("SERVICE_CREDENTIAL")
	}
	rt, err := wireTournamentRuntime()
	if err != nil {
		return fmt.Errorf("tournament runtime: %w", err)
	}

	if rt.workerRole == workerRoleTournamentProvisioning {
		return runProvisioningWorker(rt)
	}
	if rt.workerRole == workerRoleTournamentSeeding {
		return runSeedingWorker(rt)
	}
	if rt.workerRole == workerRoleTournamentCompletion {
		return runCompletionWorker(rt)
	}

	srv := serverFromRuntime(rt, cred, strings.TrimSpace(os.Getenv("TOURNAMENT_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL")))
	if !rt.ready && rt.mode == "durable" {
		// Keep serving /health + /ready=503 with fail-closed wiring.
		srv.readyReason = rt.readyReason
	}
	if rt.mode == "misconfigured" {
		srv.readyReason = rt.readyReason
	}
	httpSrv := &http.Server{Addr: ":8080", Handler: tournamentHTTPHandler(telemetryRuntime, srv.Routes()), ReadHeaderTimeout: 5 * time.Second}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	if rt.kafka != nil {
		rt.kafka.start(rootCtx)
		slog.InfoContext(rootCtx, "Kafka consumer started", "event", "kafka_consumer_started",
			"group", rt.kafka.consumer.cfg.Group, "topic", rt.kafka.consumer.cfg.Topic)
	}

	errCh := make(chan error, 1)
	go func() {
		slog.InfoContext(rootCtx, "tournament API started", "event", "startup", "mode", rt.mode)
		errCh <- httpSrv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-sigCh:
	}
	rootCancel()
	if rt.kafka != nil {
		rt.kafka.stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	if rt.rdbClose != nil {
		_ = rt.rdbClose()
	}
	if rt.pool != nil {
		rt.pool.Close()
	}
	return nil
}

// runProvisioningWorker runs WORKER_ROLE=tournament-provisioning without the API HTTP server.
// SIGTERM stops claiming and waits for in-flight ProcessProvisioningBatch work.
func runProvisioningWorker(rt tournamentRuntime) error {
	if rt.store == nil || rt.svc == nil {
		return errors.New("provisioning worker requires durable store and service")
	}
	if rt.durableReady != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := rt.durableReady(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("provisioning worker schema readiness: %w", err)
		}
	}
	worker := NewProvisioningWorker(rt.store, func(ctx context.Context, work ProvisioningBatchWork) error {
		_, err := rt.svc.ProcessProvisioningBatch(ctx, work)
		return err
	}, "")
	worker.Start()
	defer worker.Stop()
	defer func() {
		if rt.rdbClose != nil {
			_ = rt.rdbClose()
		}
		if rt.pool != nil {
			rt.pool.Close()
		}
	}()
	slog.Info("provisioning worker started", "event", "provisioning_worker_startup", "mode", rt.mode)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("provisioning worker stopped", "event", "provisioning_worker_shutdown")
	return nil
}

// runSeedingWorker runs WORKER_ROLE=tournament-seeding without the API HTTP server.
// Only DATABASE_URL is required (no Room URL / cursor secret).
func runSeedingWorker(rt tournamentRuntime) error {
	if rt.store == nil {
		return errors.New("seeding worker requires durable store")
	}
	if rt.durableReady != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := rt.durableReady(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("seeding worker schema readiness: %w", err)
		}
	}
	worker := NewSeedingWorker(rt.store, "").WithFinalizeHook(func(tournamentID string, roundNumber int) {
		if rt.svc != nil {
			rt.svc.refreshBracketBestEffort(context.Background(), scopeRound(tournamentID, roundNumber))
		}
	})
	worker.Start()
	defer worker.Stop()
	defer func() {
		if rt.rdbClose != nil {
			_ = rt.rdbClose()
		}
		if rt.pool != nil {
			rt.pool.Close()
		}
	}()
	slog.Info("seeding worker started", "event", "seeding_worker_startup", "mode", rt.mode)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("seeding worker stopped", "event", "seeding_worker_shutdown")
	return nil
}

// runCompletionWorker runs WORKER_ROLE=tournament-round-completion without the API HTTP server.
// Only DATABASE_URL is required (same DB/credential boundaries as seeding; no Room/cursor).
func runCompletionWorker(rt tournamentRuntime) error {
	if rt.store == nil || rt.svc == nil {
		return errors.New("completion worker requires durable store and service")
	}
	if rt.durableReady != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := rt.durableReady(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("completion worker schema readiness: %w", err)
		}
	}
	worker := NewCompletionWorker(rt.store, rt.svc)
	worker.Start()
	defer worker.Stop()
	defer func() {
		if rt.rdbClose != nil {
			_ = rt.rdbClose()
		}
		if rt.pool != nil {
			rt.pool.Close()
		}
	}()
	slog.Info("completion worker started", "event", "completion_worker_startup", "mode", rt.mode)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("completion worker stopped", "event", "completion_worker_shutdown")
	return nil
}
