package bff

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"unoarena/shared/correlation"
)

const (
	publicRoomListDefaultLimit = 50
	publicRoomListMaxLimit     = 100
)

// handlePublicRoomList proxies GET /v1/rooms to Room internal public-list.
// Public-only matchmaking read: no player bearer required.
func (s *Server) handlePublicRoomList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "")
		return
	}
	corr := s.correlation(r)
	safeQuery, err := sanitizePublicRoomListQuery(r.URL.Query())
	if err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "bad_request", err.Error(), "")
		return
	}
	raw, err := s.room.PublicList(r.Context(), safeQuery, corr)
	if err != nil {
		s.writePublicRoomListErr(w, r, err)
		return
	}
	validated, err := validatePublicRoomListResponse(raw)
	if err != nil {
		s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "malformed public room list", "")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(correlation.HeaderCorrelationID, corr.CorrelationID)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(validated)
	if len(validated) == 0 || validated[len(validated)-1] != '\n' {
		_, _ = w.Write([]byte("\n"))
	}
}

func (s *Server) writePublicRoomListErr(w http.ResponseWriter, r *http.Request, err error) {
	if he, ok := err.(*httpStatusError); ok {
		switch he.status {
		case http.StatusBadRequest:
			s.writeErr(w, r, http.StatusBadRequest, "bad_request", "invalid room list query", "")
			return
		case http.StatusUnauthorized:
			s.writeErr(w, r, http.StatusUnauthorized, "unauthorized", "authentication required", "")
			return
		case http.StatusNotFound:
			s.writeErr(w, r, http.StatusNotFound, "not_found", "not found", "")
			return
		}
	}
	s.writeErr(w, r, http.StatusBadGateway, "upstream_error", "public room list unavailable", "")
}

func sanitizePublicRoomListQuery(q url.Values) (string, error) {
	status := strings.TrimSpace(q.Get("status"))
	if status == "" {
		status = "waiting"
	}
	switch status {
	case "waiting", "locked", "in_progress":
	default:
		return "", fmt.Errorf("status must be waiting, locked, or in_progress")
	}

	limit := publicRoomListDefaultLimit
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > publicRoomListMaxLimit {
			return "", fmt.Errorf("limit must be an integer between 1 and %d", publicRoomListMaxLimit)
		}
		limit = n
	}

	out := url.Values{}
	out.Set("status", status)
	out.Set("limit", strconv.Itoa(limit))
	if cursor := strings.TrimSpace(q.Get("cursor")); cursor != "" {
		if strings.Contains(cursor, "OFFSET") || strings.Contains(cursor, "offset=") {
			return "", fmt.Errorf("invalid cursor")
		}
		out.Set("cursor", cursor)
	}
	return out.Encode(), nil
}

func validatePublicRoomListResponse(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("invalid json")
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var body struct {
		SchemaVersion int `json:"schemaVersion"`
		Rooms         []struct {
			RoomID         string `json:"roomId"`
			Status         string `json:"status"`
			Visibility     string `json:"visibility"`
			MaxSeats       int    `json:"maxSeats"`
			CurrentPlayers int    `json:"currentPlayers"`
			HostID         string `json:"hostId"`
			RoomType       string `json:"roomType"`
			TournamentID   string `json:"tournamentId"`
		} `json:"rooms"`
		NextCursor string `json:"nextCursor"`
	}
	if err := dec.Decode(&body); err != nil {
		return nil, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("trailing json")
	}
	if body.SchemaVersion != 1 {
		return nil, fmt.Errorf("schemaVersion")
	}
	if body.Rooms == nil {
		return nil, fmt.Errorf("rooms required")
	}
	if len(body.Rooms) > publicRoomListMaxLimit {
		return nil, fmt.Errorf("rooms bound")
	}
	for _, room := range body.Rooms {
		if strings.TrimSpace(room.RoomID) == "" || strings.TrimSpace(room.HostID) == "" {
			return nil, fmt.Errorf("room identity")
		}
		if room.Visibility != "public" {
			return nil, fmt.Errorf("visibility")
		}
		switch room.Status {
		case "waiting", "locked", "in_progress":
		default:
			return nil, fmt.Errorf("status")
		}
		switch room.RoomType {
		case "ad_hoc", "tournament":
		default:
			return nil, fmt.Errorf("roomType")
		}
		if room.MaxSeats < 2 || room.MaxSeats > 10 {
			return nil, fmt.Errorf("maxSeats")
		}
		if room.CurrentPlayers < 0 || room.CurrentPlayers > room.MaxSeats {
			return nil, fmt.Errorf("currentPlayers")
		}
		if room.RoomType != "tournament" && room.TournamentID != "" {
			return nil, fmt.Errorf("tournamentId")
		}
	}
	// Preserve upstream omitempty encoding after shape/bounds checks.
	return raw, nil
}
