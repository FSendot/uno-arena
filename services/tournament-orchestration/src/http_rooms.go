package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/correlation"
)

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

func (p *HTTPRoomProvisioner) Provision(ctx context.Context, req RoomProvisionRequest) error {
	if p == nil || p.BaseURL == "" {
		return fmt.Errorf("room provisioner unconfigured")
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
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/internal/v1/rooms/provision", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if p.Credential != "" {
		httpReq.Header.Set(headerServiceCredential, p.Credential)
	}
	httpReq.Header.Set(correlation.HeaderCorrelationID, fmt.Sprintf("provision-%s-%s", req.TournamentID, req.SlotID))
	resp, err := p.Client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("room provision HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}
