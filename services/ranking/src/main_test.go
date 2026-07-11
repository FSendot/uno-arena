package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"unoarena/services/ranking/domain"
)

const testInternalCredential = "test-internal-credential"

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return NewServer(NewMemoryRatingStore(), testInternalCredential)
}

func withCred(req *http.Request) *http.Request {
	req.Header.Set(internalCredentialHeader, testInternalCredential)
	return req
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	return out
}

func postGameResult(t *testing.T, mux http.Handler, body map[string]any) map[string]any {
	t.Helper()
	_, out := postGameResultStatus(t, mux, body)
	return out
}

func postGameResultStatus(t *testing.T, mux http.Handler, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := withCred(httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/games-results", bytes.NewReader(b)))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK && w.Code != http.StatusConflict {
		t.Fatalf("game-results: expected 200/409, got %d body=%s", w.Code, w.Body.String())
	}
	return w.Code, decodeJSON(t, w)
}

func TestHealthAndReadyOffline(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	hw := httptest.NewRecorder()
	mux.ServeHTTP(hw, httptest.NewRequest(http.MethodGet, "/health", nil))
	if hw.Code != http.StatusOK {
		t.Fatalf("health: %d", hw.Code)
	}
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rw.Code != http.StatusOK {
		t.Fatalf("ready: %d", rw.Code)
	}
}

func TestInternalUnauthorizedWithoutCredential(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/games-results", bytes.NewReader([]byte(`{}`)))
	w := httptest.NewRecorder()
	srv.routes().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAtomicGameCompletedAllParticipants(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()

	// Caller ratings are absurd — server must ignore and use defaults (1000).
	out := postGameResult(t, mux, map[string]any{
		"commandId": "cmd_1", "eventId": "evt_1",
		"gameId": "g1", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "rating": 1, "placement": 1},
			{"playerId": "p2", "rating": 99999, "placement": 2},
		},
	})
	if out["kind"] != "accepted" {
		t.Fatalf("outcome: %+v", out)
	}
	parts, _ := out["participants"].([]any)
	if len(parts) != 2 {
		t.Fatalf("expected both participants applied, got %+v", out["participants"])
	}

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rankings/leaderboards?boardType=casual_elo", nil))
	var board struct {
		Entries []struct {
			PlayerID string `json:"playerId"`
			Rating   int    `json:"rating"`
		} `json:"entries"`
	}
	_ = json.NewDecoder(w.Body).Decode(&board)
	if len(board.Entries) != 2 {
		t.Fatalf("entries=%+v", board.Entries)
	}
	if board.Entries[0].PlayerID != "p1" {
		t.Fatalf("winner first: %+v", board.Entries)
	}
	// Ignored caller ratings: both started at 1000, so deltas are modest.
	if board.Entries[0].Rating < 1000 || board.Entries[0].Rating > 1100 {
		t.Fatalf("p1 rating should be near 1000+delta, got %d (caller rating ignored)", board.Entries[0].Rating)
	}
	if board.Entries[1].Rating > 1000 || board.Entries[1].Rating < 900 {
		t.Fatalf("p2 rating should be near 1000-delta, got %d", board.Entries[1].Rating)
	}
}

func TestDuplicateEventIDNoPartialFanOut(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	body := map[string]any{
		"commandId": "cmd_1", "eventId": "evt_1",
		"gameId": "g1", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	}
	first := postGameResult(t, mux, body)
	if first["kind"] != "accepted" {
		t.Fatalf("first: %+v", first)
	}
	dup := postGameResult(t, mux, body)
	if dup["kind"] != "duplicate" {
		t.Fatalf("duplicate eventId: %+v", dup)
	}

	hw := httptest.NewRecorder()
	mux.ServeHTTP(hw, httptest.NewRequest(http.MethodGet, "/v1/players/p1/rating-history", nil))
	var hist []any
	_ = json.NewDecoder(hw.Body).Decode(&hist)
	if len(hist) != 1 {
		t.Fatalf("history len=%d want 1", len(hist))
	}
}

func TestRejectAbandonedAndTournamentRoomType(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	code, abandoned := postGameResultStatus(t, mux, map[string]any{
		"commandId": "cmd_a", "eventId": "evt_a",
		"gameId": "g_a", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": true, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	})
	if code != http.StatusConflict {
		t.Fatalf("abandoned status=%d want 409", code)
	}
	if abandoned["kind"] != "rejected" {
		t.Fatalf("abandoned: %+v", abandoned)
	}
	code, tour := postGameResultStatus(t, mux, map[string]any{
		"commandId": "cmd_t", "eventId": "evt_t",
		"gameId": "g_t", "roomId": "r1", "roomType": "tournament",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	})
	if code != http.StatusConflict {
		t.Fatalf("tournament status=%d want 409", code)
	}
	if tour["kind"] != "rejected" {
		t.Fatalf("tournament: %+v", tour)
	}
}

func TestConcurrentGameCompletedRace(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	var wg sync.WaitGroup
	results := make(chan string, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := postGameResult(t, mux, map[string]any{
				"commandId": "cmd_race", "eventId": "evt_race",
				"gameId": "g_race", "roomId": "r1", "roomType": "ad_hoc",
				"isAbandoned": false, "authoritative": true, "completed": true,
				"participants": []map[string]any{
					{"playerId": "p1", "placement": 1},
					{"playerId": "p2", "placement": 2},
				},
			})
			results <- out["kind"].(string)
		}()
	}
	wg.Wait()
	close(results)
	accepted, dup := 0, 0
	for k := range results {
		switch k {
		case "accepted":
			accepted++
		case "duplicate":
			dup++
		}
	}
	if accepted != 1 || dup != 9 {
		t.Fatalf("accepted=%d dup=%d", accepted, dup)
	}
}

func TestTournamentPlacementAndHistory(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	b, _ := json.Marshal(map[string]any{
		"commandId": "cmd_t1", "eventId": "evt_t1", "playerId": "p1",
		"tournamentId": "t1", "placementEventId": "pe1",
		"placement": 1, "delta": 25, "reason": "tournament_placement",
	})
	req := withCred(httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/tournament-placements", bytes.NewReader(b)))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("placement: %d", w.Code)
	}

	postGameResult(t, mux, map[string]any{
		"commandId": "cmd_1", "eventId": "evt_1",
		"gameId": "g1", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	})
	hw := httptest.NewRecorder()
	mux.ServeHTTP(hw, httptest.NewRequest(http.MethodGet, "/v1/players/p1/rating-history", nil))
	if hw.Code != http.StatusOK {
		t.Fatalf("history: %d", hw.Code)
	}
	var hist []map[string]any
	_ = json.NewDecoder(hw.Body).Decode(&hist)
	if len(hist) < 1 {
		t.Fatalf("hist=%+v", hist)
	}
	if hist[0]["playerId"] != "p1" && hist[len(hist)-1]["playerId"] != "p1" {
		t.Fatalf("camelCase playerId missing: %+v", hist[0])
	}
}

func TestRebuildStatus(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	postGameResult(t, mux, map[string]any{
		"commandId": "cmd_1", "eventId": "evt_1",
		"gameId": "g1", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	})
	req := withCred(httptest.NewRequest(http.MethodGet, "/internal/v1/rankings/rebuild-status", nil))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	resp := decodeJSON(t, w)
	if resp["playerCount"].(float64) != 2 {
		t.Fatalf("playerCount: %+v", resp)
	}
}

func TestFinding_ReadyRequiresInternalCredential(t *testing.T) {
	srv := NewServer(NewMemoryRatingStore(), "")
	mux := srv.routes()
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rw.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d", rw.Code)
	}
	b, _ := json.Marshal(map[string]any{"commandId": "c", "eventId": "e", "gameId": "g", "roomId": "r", "roomType": "ad_hoc",
		"authoritative": true, "completed": true,
		"participants": []map[string]any{{"playerId": "p1", "placement": 1}, {"playerId": "p2", "placement": 2}},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/games-results", bytes.NewReader(b))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("write status=%d want 503", w.Code)
	}
}

func TestFinding_DomainRejectionReturnsNon2xx(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	code, body := postGameResultStatus(t, mux, map[string]any{
		"commandId": "cmd_rej", "eventId": "evt_rej",
		"gameId": "g_rej", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": true, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	})
	if code == http.StatusOK {
		t.Fatalf("domain rejection must be non-2xx, body=%+v", body)
	}
	if code != http.StatusConflict {
		t.Fatalf("status=%d want 409 body=%+v", code, body)
	}
	if body["kind"] != "rejected" {
		t.Fatalf("kind=%v want rejected", body["kind"])
	}
}

func TestResolveInboundCorrelationID(t *testing.T) {
	cases := []struct {
		name, body, header, eventID, want string
	}{
		{"body wins", " body-corr ", "header-corr", "e1", "body-corr"},
		{"header when body blank", "  ", "header-corr", "e1", "header-corr"},
		{"eventId fallback", "", "", "e-fallback", "e-fallback"},
		{"unspecified last resort", "", "", "", "ranking-unspecified"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveInboundCorrelationID(tc.body, tc.header, tc.eventID)
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestGamesResultsPropagatesBodyCorrelation(t *testing.T) {
	var got GameCompletedRequest
	srv := NewServer(captureApp{onCasual: func(req GameCompletedRequest) {
		got = req
	}}, testInternalCredential)
	mux := srv.routes()
	b, _ := json.Marshal(map[string]any{
		"commandId": "cmd_corr", "eventId": "evt_corr", "correlationId": "body-corr",
		"gameId": "g_corr", "roomId": "r1", "roomType": "ad_hoc",
		"isAbandoned": false, "authoritative": true, "completed": true,
		"participants": []map[string]any{
			{"playerId": "p1", "placement": 1},
			{"playerId": "p2", "placement": 2},
		},
	})
	req := withCred(httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/games-results", bytes.NewReader(b)))
	req.Header.Set("X-Correlation-Id", "header-corr")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got.CorrelationID != "body-corr" {
		t.Fatalf("CorrelationID=%q", got.CorrelationID)
	}
	if got.CausationID != "cmd_corr" {
		t.Fatalf("CausationID=%q", got.CausationID)
	}
}

func TestTournamentPlacementPropagatesHeaderCorrelation(t *testing.T) {
	var got TournamentPlacementRequest
	srv := NewServer(captureApp{onPlacement: func(req TournamentPlacementRequest) {
		got = req
	}}, testInternalCredential)
	mux := srv.routes()
	b, _ := json.Marshal(map[string]any{
		"commandId": "cmd_t", "eventId": "evt_t", "playerId": "p1",
		"tournamentId": "t1", "placementEventId": "pe1",
		"placement": 1, "delta": 10, "reason": "tournament_placement",
	})
	req := withCred(httptest.NewRequest(http.MethodPost, "/internal/v1/rankings/tournament-placements", bytes.NewReader(b)))
	req.Header.Set("X-Correlation-Id", "header-place")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got.CorrelationID != "header-place" {
		t.Fatalf("CorrelationID=%q", got.CorrelationID)
	}
	if got.CausationID != "cmd_t" {
		t.Fatalf("CausationID=%q", got.CausationID)
	}
	if got.Command.PlayerID != "p1" {
		t.Fatalf("command not forwarded: %+v", got.Command)
	}
}

// captureApp records requests then delegates to memory for compatible outcomes.
type captureApp struct {
	mem         *MemoryRatingStore
	onCasual    func(GameCompletedRequest)
	onPlacement func(TournamentPlacementRequest)
}

func (c captureApp) store() *MemoryRatingStore {
	if c.mem != nil {
		return c.mem
	}
	return NewMemoryRatingStore()
}

func (c captureApp) ApplyCasualGameCompleted(ctx context.Context, req GameCompletedRequest) (GameCompletedResult, error) {
	if c.onCasual != nil {
		c.onCasual(req)
	}
	return c.store().ApplyCasualGameCompleted(ctx, req)
}

func (c captureApp) ApplyTournamentPlacement(ctx context.Context, req TournamentPlacementRequest) (domain.CommandOutcome, error) {
	if c.onPlacement != nil {
		c.onPlacement(req)
	}
	return c.store().ApplyTournamentPlacement(ctx, req)
}

func (c captureApp) Leaderboard(ctx context.Context, boardType domain.RatingSourceType) ([]domain.LeaderboardEntry, error) {
	return c.store().Leaderboard(ctx, boardType)
}

func (c captureApp) History(ctx context.Context, playerID domain.PlayerID) ([]domain.RatingHistoryEntry, bool, error) {
	return c.store().History(ctx, playerID)
}

func (c captureApp) RebuildStatus(ctx context.Context) (RebuildStatus, error) {
	return c.store().RebuildStatus(ctx)
}
