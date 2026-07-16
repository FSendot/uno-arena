package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

type RoomProvisionFailureKind string

const (
	RoomProvisionFailureHTTP          RoomProvisionFailureKind = "http"
	RoomProvisionFailureConfiguration RoomProvisionFailureKind = "configuration"
)

// RoomProvisionError is a sanitized classification boundary for Room failures.
// It intentionally excludes the upstream message/body so secrets and internal
// details cannot enter Tournament retry state or logs.
type RoomProvisionError struct {
	Kind       RoomProvisionFailureKind
	StatusCode int
	Code       string
	RetryAfter string
}

func (e *RoomProvisionError) Error() string {
	if e == nil {
		return "room provision failed"
	}
	if e.Kind == RoomProvisionFailureConfiguration {
		return "room provision configuration error"
	}
	class := fmt.Sprintf("HTTP %d", e.StatusCode)
	if e.StatusCode >= 500 {
		class = "HTTP 5xx"
	} else if e.StatusCode >= 400 {
		class = "HTTP 4xx"
	}
	if e.Code != "" {
		return "room provision " + class + " (" + e.Code + ")"
	}
	return "room provision " + class
}

// Retryable reports failures safe for the existing bounded provisioning retry
// budget. Room provisioning is idempotent, but deterministic client/auth/config
// failures cannot improve through replay.
func (e *RoomProvisionError) Retryable() bool {
	return e != nil && e.Kind != RoomProvisionFailureConfiguration &&
		(e.StatusCode == http.StatusRequestTimeout || e.StatusCode == http.StatusTooEarly ||
			e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500)
}

func roomProvisionFailureForcesQuarantine(err error) bool {
	if errors.Is(err, ErrRoomIDMismatch) {
		return true
	}
	var provisionErr *RoomProvisionError
	return errors.As(err, &provisionErr) && !provisionErr.Retryable()
}

func safeRoomProvisionCode(raw []byte) string {
	var body struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return ""
	}
	code := strings.TrimSpace(body.Code)
	if len(code) == 0 || len(code) > 64 {
		return ""
	}
	for _, r := range code {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.') {
			return ""
		}
	}
	return code
}

// HTTPRoomProvisioner calls Room Gameplay POST /internal/v1/rooms/provision.
type HTTPRoomProvisioner struct {
	BaseURL    string
	Credential string
	Client     *http.Client
}

// NewHTTPRoomProvisioner builds a real Room provision client.
func NewHTTPRoomProvisioner(baseURL, credential string, client *http.Client) *HTTPRoomProvisioner {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPRoomProvisioner{
		BaseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Credential: credential,
		Client:     client,
	}
}

func (p *HTTPRoomProvisioner) Provision(ctx context.Context, req RoomProvisionRequest) (RoomProvisionResult, error) {
	if p == nil || p.BaseURL == "" {
		return RoomProvisionResult{}, &RoomProvisionError{Kind: RoomProvisionFailureConfiguration}
	}
	players := make([]string, len(req.PlayerIDs))
	for i, id := range req.PlayerIDs {
		players[i] = string(id)
	}
	host := ""
	if len(players) > 0 {
		host = players[0]
	}
	body, err := json.Marshal(map[string]any{
		"commandId":    fmt.Sprintf("provision:%s:r%d:%s", req.TournamentID, req.RoundNumber, req.SlotID),
		"tournamentId": string(req.TournamentID),
		"roundNumber":  req.RoundNumber,
		"slotId":       string(req.SlotID),
		"roomId":       string(req.RoomID),
		"hostId":       host,
		"playerIds":    players,
		"visibility":   "private",
		"maxSeats":     domain.PlayersPerRoom,
	})
	if err != nil {
		return RoomProvisionResult{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/internal/v1/rooms/provision", bytes.NewReader(body))
	if err != nil {
		return RoomProvisionResult{}, &RoomProvisionError{Kind: RoomProvisionFailureConfiguration}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if p.Credential != "" {
		httpReq.Header.Set(headerServiceCredential, p.Credential)
	}
	httpReq.Header.Set(correlation.HeaderCorrelationID, fmt.Sprintf("provision-%s-%s", req.TournamentID, req.SlotID))
	resp, err := p.Client.Do(httpReq)
	if err != nil {
		return RoomProvisionResult{}, err
	}
	defer resp.Body.Close()
	// Drain body for connection reuse but never include raw bytes in returned errors.
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RoomProvisionResult{}, &RoomProvisionError{
			Kind:       RoomProvisionFailureHTTP,
			StatusCode: resp.StatusCode,
			Code:       safeRoomProvisionCode(raw),
			RetryAfter: resp.Header.Get("Retry-After"),
		}
	}
	returnedID, err := parseProvisionedRoomID(raw)
	if err != nil {
		return RoomProvisionResult{}, fmt.Errorf("room provision invalid response")
	}
	want := string(req.RoomID)
	if returnedID != want {
		return RoomProvisionResult{}, fmt.Errorf("%w: requested %s", ErrRoomIDMismatch, want)
	}
	return RoomProvisionResult{RoomID: returnedID}, nil
}

func parseProvisionedRoomID(raw []byte) (string, error) {
	var env envelope.Result
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", err
	}
	if env.Status != envelope.StatusAccepted {
		return "", fmt.Errorf("not accepted")
	}
	if len(env.Payload) == 0 {
		return "", fmt.Errorf("missing payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return "", err
	}
	roomID, _ := payload["roomId"].(string)
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return "", fmt.Errorf("missing roomId")
	}
	return roomID, nil
}
