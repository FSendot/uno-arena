package bff

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"unoarena/shared/correlation"
)

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
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
	var reqBody struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid json body", "")
		return
	}
	corr := s.correlation(r)
	res, err := s.identity.Register(r.Context(), reqBody.Username, reqBody.Password, corr)
	if err != nil {
		if he, ok := err.(*httpStatusError); ok {
			if he.status == http.StatusTooManyRequests {
				if he.retryAfter != "" {
					w.Header().Set("Retry-After", he.retryAfter)
				}
				s.writeErr(w, r, http.StatusTooManyRequests, "rate_limited", "identity rate limit exceeded", "")
				return
			}
			if he.status == http.StatusBadRequest || he.status == http.StatusConflict || he.status == http.StatusUnprocessableEntity {
				code := he.safeCode()
				if code == "" {
					code = "bad_request"
				}
				s.writeErr(w, r, http.StatusBadRequest, code, "registration rejected", "")
				return
			}
		}
		s.writeErr(w, r, http.StatusBadGateway, "identity_unavailable", "registration unavailable", "")
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{
		"status":   res.Status,
		"username": res.Username,
	})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
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
	var reqBody struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid json body", "")
		return
	}
	corr := s.correlation(r)
	res, err := s.identity.Login(r.Context(), reqBody.Username, reqBody.Password, corr)
	if err != nil {
		if errors.Is(err, ErrInvalidCredentials) {
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid credentials", "")
			return
		}
		if he, ok := err.(*httpStatusError); ok && he.status == http.StatusTooManyRequests {
			if he.retryAfter != "" {
				w.Header().Set("Retry-After", he.retryAfter)
			}
			s.writeErr(w, r, http.StatusTooManyRequests, "rate_limited", "identity rate limit exceeded", "")
			return
		}
		s.writeErr(w, r, http.StatusBadGateway, "identity_unavailable", "login unavailable", "")
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{
		"sessionId": res.SessionID,
		"playerId":  res.PlayerID,
		"token":     res.Token,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	token, ok := bearerToken(r)
	if !ok {
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "missing bearer token", "")
		return
	}
	if err := s.identity.Logout(r.Context(), token, s.correlation(r)); err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "logout unavailable", "")
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	principal, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{
		"playerId":  principal.PlayerID,
		"sessionId": principal.SessionID,
		"username":  principal.Username,
	})
}

func (s *Server) requirePrincipal(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	token, ok := bearerToken(r)
	if !ok {
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "missing bearer token", "")
		return Principal{}, false
	}
	corr := s.correlation(r)
	principal, err := s.identity.ValidateSession(r.Context(), token, corr)
	if err != nil {
		s.writeIdentityValidationError(w, r, err)
		return Principal{}, false
	}
	return principal, true
}

func (s *Server) writeIdentityValidationError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, ErrUnauthorized) {
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid session", "")
		return
	}
	s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "identity validation unavailable", "")
}

func (s *Server) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	corr := s.correlation(r)
	raw, err := s.reads.Leaderboard(r.Context(), r.URL.RawQuery, corr)
	if err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "leaderboard unavailable", "")
		return
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
}

func (s *Server) handlePublicAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	corr := s.correlation(r)
	raw, err := s.reads.PublicAnalytics(r.Context(), corr)
	if err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "analytics unavailable", "")
		return
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
}

// handleTournamentReads proxies Tournament bracket/standings/assignment reads.
// Paths: GET /v1/tournaments/{tournamentId}/bracket|standings
//
//	GET /v1/tournaments/{tournamentId}/players/{playerId}/assignment
func (s *Server) handleTournamentReads(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/tournaments/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	corr := s.correlation(r)

	var (
		tournamentID string
		resource     string
		playerID     string
	)
	switch {
	case len(parts) == 2 && parts[0] != "" && (parts[1] == "bracket" || parts[1] == "standings"):
		tournamentID, resource = parts[0], parts[1]
	case len(parts) == 4 && parts[0] != "" && parts[1] == "players" && parts[2] != "" && parts[3] == "assignment":
		tournamentID, playerID, resource = parts[0], parts[2], "assignment"
	default:
		s.writeErr(w, r, http.StatusNotFound, "not_found", "not found", "")
		return
	}
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}

	var principal *Principal
	if tok, ok := bearerToken(r); ok {
		p, err := s.identity.ValidateSession(r.Context(), tok, corr)
		if err != nil {
			s.writeIdentityValidationError(w, r, err)
			return
		}
		principal = &p
	}

	if resource == "assignment" {
		if principal == nil {
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "missing bearer token", "")
			return
		}
		if !principal.OperatorScope && principal.PlayerID != playerID {
			s.writeErr(w, r, http.StatusForbidden, "forbidden", "forbidden", "")
			return
		}
	}

	var (
		raw json.RawMessage
		err error
	)
	switch resource {
	case "bracket":
		raw, err = s.tournament.Bracket(r.Context(), tournamentID, r.URL.RawQuery, corr, principal)
	case "standings":
		raw, err = s.tournament.Standings(r.Context(), tournamentID, corr, principal)
	case "assignment":
		raw, err = s.tournament.Assignment(r.Context(), tournamentID, playerID, corr, principal)
	}
	if err != nil {
		s.writeTournamentReadErr(w, r, err, resource)
		return
	}
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
}

func (s *Server) writeTournamentReadErr(w http.ResponseWriter, r *http.Request, err error, resource string) {
	if he, ok := err.(*httpStatusError); ok {
		switch he.status {
		case http.StatusBadRequest:
			s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid tournament read query", "")
			return
		case http.StatusUnauthorized:
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "authentication required", "")
			return
		case http.StatusForbidden:
			s.writeErr(w, r, http.StatusForbidden, "forbidden", "forbidden", "")
			return
		case http.StatusNotFound:
			msg := "tournament not found"
			if resource == "assignment" {
				msg = "assignment not found"
			}
			s.writeErr(w, r, http.StatusNotFound, "not_found", msg, "")
			return
		}
	}
	msg := "tournament " + resource + " unavailable"
	s.writeErr(w, r, http.StatusBadGateway, "upstream_error", msg, "")
}
