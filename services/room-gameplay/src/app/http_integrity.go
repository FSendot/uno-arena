package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"unoarena/services/room-gameplay/game"
)

// HTTPGameIntegrity is a stdlib HTTP adapter for the GameIntegrity port.
type HTTPGameIntegrity struct {
	BaseURL    string
	Credential string
	Client     *http.Client
}

// NewHTTPGameIntegrity constructs a Room→GI append adapter.
func NewHTTPGameIntegrity(baseURL, credential string, client *http.Client) *HTTPGameIntegrity {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPGameIntegrity{BaseURL: strings.TrimRight(baseURL, "/"), Credential: credential, Client: client}
}

// Append implements GameIntegrity.
func (h *HTTPGameIntegrity) Append(ctx context.Context, req AppendRequest) (AppendResult, error) {
	body := map[string]any{
		"eventId":          req.EventID,
		"expectedRevision": req.ExpectedRevision,
		"eventType":        req.EventType,
		"payload":          json.RawMessage(req.Payload),
	}
	if req.GameID != "" {
		body["gameId"] = req.GameID
	}
	if len(req.Payload) == 0 {
		body["payload"] = json.RawMessage("null")
	}
	var out struct {
		Kind      string `json:"kind"`
		LogOffset int64  `json:"logOffset"`
		Revision  int64  `json:"revision"`
		Code      string `json:"code"`
		Message   string `json:"message"`
	}
	status, err := h.postJSON(ctx, "/internal/v1/game-logs/"+req.RoomID+"/append", body, &out)
	if err != nil {
		return AppendResult{}, err
	}
	if status >= 400 {
		return AppendResult{}, fmt.Errorf("game integrity append %d: %s %s", status, out.Code, out.Message)
	}
	return AppendResult{LogOffset: out.LogOffset, Revision: out.Revision}, nil
}

// HTTPDealSource is a stdlib HTTP adapter for the DealSource reservation port.
type HTTPDealSource struct {
	BaseURL    string
	Credential string
	Client     *http.Client

	mu   sync.Mutex
	meta map[string]resMeta // pending + confirmed, for Confirm/Cancel routing
}

type resMeta struct {
	RoomID string
	GameID string
}

// NewHTTPDealSource constructs a Room→GI DealSource adapter.
func NewHTTPDealSource(baseURL, credential string, client *http.Client) *HTTPDealSource {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPDealSource{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		Credential: credential,
		Client:     client,
		meta:       map[string]resMeta{},
	}
}

func (h *HTTPDealSource) deckPath(roomID string) string {
	return "/internal/v1/game-logs/" + roomID + "/deck-operations"
}

func (h *HTTPDealSource) ensureDeck(ctx context.Context, roomID, gameID string) error {
	var out struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(roomID), map[string]any{
		"operation": "initialize", "gameId": gameID,
	}, &out)
	if err != nil {
		return err
	}
	// Accepted or conflicting reinit (deck already exists) are both fine.
	if status == http.StatusOK || out.Code == "conflicting_duplicate" {
		return nil
	}
	if status >= 400 {
		return fmt.Errorf("initialize deck %d: %s %s", status, out.Code, out.Message)
	}
	return nil
}

// ReserveDeal implements DealSource.
func (h *HTTPDealSource) ReserveDeal(ctx context.Context, roomID, gameID, operationID string, seats []string) (MaterialReservation, error) {
	if err := h.ensureDeck(ctx, roomID, gameID); err != nil {
		return MaterialReservation{}, err
	}
	var out struct {
		Kind            string                `json:"kind"`
		ReservationID   string                `json:"reservationId"`
		Hands           map[string][]cardJSON `json:"hands"`
		DiscardTop      cardJSON              `json:"discardTop"`
		ActiveColor     string                `json:"activeColor"`
		CurrentSeat     int                   `json:"currentSeat"`
		Direction       string                `json:"direction"`
		ApplyTopEffects bool                  `json:"applyTopEffects"`
		Code            string                `json:"code"`
		Message         string                `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(roomID), map[string]any{
		"operation": "reserve_deal", "gameId": gameID, "operationId": operationID, "seats": seats,
	}, &out)
	if err != nil {
		return MaterialReservation{}, err
	}
	if status >= 400 {
		return MaterialReservation{}, fmt.Errorf("reserve deal %d: %s %s", status, out.Code, out.Message)
	}
	hands := make(map[game.PlayerID][]game.Card, len(out.Hands))
	for seat, cards := range out.Hands {
		hands[game.PlayerID(seat)] = toGameCards(cards)
	}
	deal := game.DealMaterial{
		Hands: hands, DiscardTop: toGameCard(out.DiscardTop), ActiveColor: game.Color(out.ActiveColor),
		CurrentSeat: out.CurrentSeat, Direction: parseDirection(out.Direction), ApplyTopEffects: out.ApplyTopEffects,
	}
	h.mu.Lock()
	h.meta[out.ReservationID] = resMeta{RoomID: roomID, GameID: gameID}
	h.mu.Unlock()
	return MaterialReservation{ID: out.ReservationID, Deal: &deal}, nil
}

// ReserveDraw implements DealSource.
func (h *HTTPDealSource) ReserveDraw(ctx context.Context, roomID, gameID, operationID string, count int) (MaterialReservation, error) {
	if err := h.ensureDeck(ctx, roomID, gameID); err != nil {
		return MaterialReservation{}, err
	}
	var out struct {
		Kind          string     `json:"kind"`
		ReservationID string     `json:"reservationId"`
		Cards         []cardJSON `json:"cards"`
		Code          string     `json:"code"`
		Message       string     `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(roomID), map[string]any{
		"operation": "reserve_draw", "gameId": gameID, "operationId": operationID, "count": count,
	}, &out)
	if err != nil {
		return MaterialReservation{}, err
	}
	if status >= 400 {
		return MaterialReservation{}, fmt.Errorf("reserve draw %d: %s %s", status, out.Code, out.Message)
	}
	h.mu.Lock()
	h.meta[out.ReservationID] = resMeta{RoomID: roomID, GameID: gameID}
	h.mu.Unlock()
	return MaterialReservation{ID: out.ReservationID, Cards: toGameCards(out.Cards)}, nil
}

// Confirm implements DealSource.
func (h *HTTPDealSource) Confirm(ctx context.Context, reservationID string) error {
	meta, err := h.lookup(reservationID)
	if err != nil {
		return err
	}
	var out struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(meta.RoomID), map[string]any{
		"operation": "confirm", "gameId": meta.GameID, "reservationId": reservationID,
	}, &out)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("confirm %d: %s %s", status, out.Code, out.Message)
	}
	// Retain meta so Confirm(originalID) remains routable after lost response.
	return nil
}

// Cancel implements DealSource.
func (h *HTTPDealSource) Cancel(ctx context.Context, reservationID string) error {
	meta, err := h.lookup(reservationID)
	if err != nil {
		// Missing local meta cannot be treated as success — pending GI reservation
		// may still exist after restart/meta loss.
		return fmt.Errorf("cancel meta missing: %w", err)
	}
	var out struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(meta.RoomID), map[string]any{
		"operation": "cancel", "gameId": meta.GameID, "reservationId": reservationID,
	}, &out)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("cancel %d: %s %s", status, out.Code, out.Message)
	}
	h.mu.Lock()
	delete(h.meta, reservationID)
	h.mu.Unlock()
	return nil
}

func (h *HTTPDealSource) lookup(reservationID string) (resMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	meta, ok := h.meta[reservationID]
	if !ok {
		return resMeta{}, fmt.Errorf("unknown reservation")
	}
	return meta, nil
}

type cardJSON struct {
	ID    string `json:"id"`
	Color string `json:"color"`
	Face  string `json:"face"`
}

func parseDirection(s string) game.Direction {
	switch strings.ToLower(s) {
	case "counterclockwise", "counter_clockwise", "-1":
		return game.DirectionCounterClockwise
	default:
		return game.DirectionClockwise
	}
}

func toGameCard(c cardJSON) game.Card {
	return game.Card{ID: game.CardID(c.ID), Color: game.Color(c.Color), Face: game.Face(c.Face)}
}

func toGameCards(in []cardJSON) []game.Card {
	out := make([]game.Card, len(in))
	for i, c := range in {
		out[i] = toGameCard(c)
	}
	return out
}

func (h *HTTPGameIntegrity) postJSON(ctx context.Context, path string, body any, dest any) (int, error) {
	return postJSON(ctx, h.Client, h.BaseURL+path, h.Credential, body, dest)
}

func (h *HTTPDealSource) postJSON(ctx context.Context, path string, body any, dest any) (int, error) {
	return postJSON(ctx, h.Client, h.BaseURL+path, h.Credential, body, dest)
}

func postJSON(ctx context.Context, client *http.Client, url, credential string, body any, dest any) (int, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if credential != "" {
		req.Header.Set("X-Service-Credential", credential)
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, err
	}
	if dest != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, dest); err != nil {
			return res.StatusCode, fmt.Errorf("decode response: %w body=%s", err, string(raw))
		}
	}
	return res.StatusCode, nil
}
