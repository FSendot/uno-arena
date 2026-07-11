package bff

import (
	"encoding/json"
	"errors"
	"net/http"

	"unoarena/shared/correlation"
)

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	if !s.allowEdge(w, r) {
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
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", err.Error(), "")
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
	if !s.allowEdge(w, r) {
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
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", err.Error(), "")
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{
		"sessionId": res.SessionID,
		"playerId":  res.PlayerID,
		"token":     res.Token,
	})
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
		s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "invalid session", "")
		return Principal{}, false
	}
	return principal, true
}

func (s *Server) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	corr := s.correlation(r)
	raw, err := s.reads.Leaderboard(r.Context(), corr)
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
