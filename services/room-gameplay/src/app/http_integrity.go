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
		// Transport / decode / read / context errors are uncertain (may have written).
		return AppendResult{}, err
	}
	if status >= 400 && status < 500 {
		// Explicit HTTP 4xx is definitive no-write.
		return AppendResult{}, fmt.Errorf("%w: game integrity append %d: %s %s",
			ErrIntegrityAppendDefinitive, status, out.Code, out.Message)
	}
	if status >= 500 {
		// 5xx is uncertain — do not wrap as definitive.
		return AppendResult{}, fmt.Errorf("game integrity append uncertain %d: %s %s", status, out.Code, out.Message)
	}
	return AppendResult{LogOffset: out.LogOffset, Revision: out.Revision}, nil
}

// Replay implements GameIntegrity (GI audit replay endpoint).
// Uses AuditCredential when set; otherwise the Room service credential (for test doubles).
func (h *HTTPGameIntegrity) Replay(ctx context.Context, roomID string, fromOffset int64) (ReplayResult, error) {
	cred := h.AuditCredential
	if cred == "" {
		cred = h.Credential
	}
	actor := h.AuditActor
	if actor == "" {
		actor = "room-gameplay-reconciliation"
	}
	reason := h.AuditReason
	if reason == "" {
		reason = "integrity_reconciliation"
	}
	url := fmt.Sprintf("%s/internal/v1/game-logs/%s/replay?from=%d", h.BaseURL, roomID, fromOffset)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ReplayResult{}, err
	}
	if cred != "" {
		req.Header.Set("X-Service-Credential", cred)
	}
	req.Header.Set("X-Audit-Actor", actor)
	req.Header.Set("X-Audit-Reason", reason)
	res, err := h.Client.Do(req)
	if err != nil {
		return ReplayResult{}, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return ReplayResult{}, err
	}
	if res.StatusCode >= 400 {
		return ReplayResult{}, fmt.Errorf("game integrity replay %d: %s", res.StatusCode, string(raw))
	}
	var out struct {
		RoomID   string `json:"roomId"`
		Revision int64  `json:"revision"`
		Entries  []struct {
			Offset    int64           `json:"offset"`
			EventID   string          `json:"eventId"`
			EventType string          `json:"eventType"`
			GameID    string          `json:"gameId"`
			Payload   json.RawMessage `json:"payload"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return ReplayResult{}, fmt.Errorf("decode replay: %w", err)
	}
	result := ReplayResult{RoomID: out.RoomID, Revision: out.Revision}
	for _, e := range out.Entries {
		result.Entries = append(result.Entries, ReplayEntry{
			Offset: e.Offset, EventID: e.EventID, EventType: e.EventType, GameID: e.GameID, Payload: e.Payload,
		})
	}
	return result, nil
}

// HTTPGameIntegrity is a stdlib HTTP adapter for the GameIntegrity port.
type HTTPGameIntegrity struct {
	BaseURL         string
	Credential      string
	AuditCredential string // optional; used for Replay (GI authorizeAudit)
	AuditActor      string
	AuditReason     string
	Client          *http.Client
}

// NewHTTPGameIntegrity constructs a Room→GI append/replay adapter.
func NewHTTPGameIntegrity(baseURL, credential string, client *http.Client) *HTTPGameIntegrity {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPGameIntegrity{BaseURL: strings.TrimRight(baseURL, "/"), Credential: credential, Client: client}
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
		Remaining       *int                  `json:"remaining"`
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
	if out.Remaining == nil {
		return MaterialReservation{}, fmt.Errorf("reserve deal: missing remaining")
	}
	if *out.Remaining < 0 {
		return MaterialReservation{}, fmt.Errorf("reserve deal: remaining must be >= 0")
	}
	hands := make(map[game.PlayerID][]game.Card, len(out.Hands))
	for seat, cards := range out.Hands {
		hands[game.PlayerID(seat)] = toGameCards(cards)
	}
	deal := game.DealMaterial{
		Hands: hands, DiscardTop: toGameCard(out.DiscardTop), ActiveColor: game.Color(out.ActiveColor),
		CurrentSeat: out.CurrentSeat, Direction: parseDirection(out.Direction), ApplyTopEffects: out.ApplyTopEffects,
		DrawPileSize: *out.Remaining, HasDrawPileSize: true,
	}
	h.mu.Lock()
	h.meta[out.ReservationID] = resMeta{RoomID: roomID, GameID: gameID}
	h.mu.Unlock()
	return MaterialReservation{ID: out.ReservationID, Deal: &deal, DrawPileSize: *out.Remaining}, nil
}

// ReserveDraw implements DealSource.
func (h *HTTPDealSource) ReserveDraw(ctx context.Context, roomID, gameID, operationID string, count int) (MaterialReservation, error) {
	if err := h.ensureDeck(ctx, roomID, gameID); err != nil {
		return MaterialReservation{}, err
	}
	var out struct {
		Kind          string     `json:"kind"`
		ReservationID string     `json:"reservationId"`
		Remaining     *int       `json:"remaining"`
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
	if out.Remaining == nil {
		return MaterialReservation{}, fmt.Errorf("reserve draw: missing remaining")
	}
	if *out.Remaining < 0 {
		return MaterialReservation{}, fmt.Errorf("reserve draw: remaining must be >= 0")
	}
	h.mu.Lock()
	h.meta[out.ReservationID] = resMeta{RoomID: roomID, GameID: gameID}
	h.mu.Unlock()
	return MaterialReservation{ID: out.ReservationID, Cards: toGameCards(out.Cards), DrawPileSize: *out.Remaining}, nil
}

// Confirm implements DealSource via in-memory meta (convenience for live callers).
func (h *HTTPDealSource) Confirm(ctx context.Context, reservationID string) error {
	meta, err := h.lookup(reservationID)
	if err != nil {
		return err
	}
	return h.ConfirmAt(ctx, meta.RoomID, meta.GameID, reservationID)
}

// Cancel implements DealSource via in-memory meta (convenience for live callers).
func (h *HTTPDealSource) Cancel(ctx context.Context, reservationID string) error {
	meta, err := h.lookup(reservationID)
	if err != nil {
		// Missing local meta cannot be treated as success — pending GI reservation
		// may still exist after restart/meta loss.
		return fmt.Errorf("cancel meta missing: %w", err)
	}
	if err := h.CancelAt(ctx, meta.RoomID, meta.GameID, reservationID); err != nil {
		return err
	}
	h.mu.Lock()
	delete(h.meta, reservationID)
	h.mu.Unlock()
	return nil
}

// ConfirmAt confirms a reservation with explicit room+game (no in-memory meta required).
func (h *HTTPDealSource) ConfirmAt(ctx context.Context, roomID, gameID, reservationID string) error {
	var out struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(roomID), map[string]any{
		"operation": "confirm", "gameId": gameID, "reservationId": reservationID,
	}, &out)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("confirm %d: %s %s", status, out.Code, out.Message)
	}
	return nil
}

// CancelAt cancels a reservation with explicit room+game (no in-memory meta required).
func (h *HTTPDealSource) CancelAt(ctx context.Context, roomID, gameID, reservationID string) error {
	var out struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status, err := h.postJSON(ctx, h.deckPath(roomID), map[string]any{
		"operation": "cancel", "gameId": gameID, "reservationId": reservationID,
	}, &out)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("cancel %d: %s %s", status, out.Code, out.Message)
	}
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
