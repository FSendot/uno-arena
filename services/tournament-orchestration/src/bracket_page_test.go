package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"unoarena/services/tournament-orchestration/store"
)

func TestBracketPage_HandlerShapeAndCursor(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-bracket-page"}

	// 40 players → 4 non-final slots (PlayersPerRoom=10); batchSize=1 → 4 batches.
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bp-create", "CreateTournament", map[string]any{
		"tournamentId": "t-bracket-page",
		"capacity":     40,
		"batchSize":    1,
	}, "op", "s"), corr)
	for i := 0; i < 40; i++ {
		p := "p" + itoa(i)
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bp-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-bracket-page", "playerId": p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bp-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-bracket-page",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bp-seed", "SeedRound", map[string]any{
		"tournamentId": "t-bracket-page", "roundNumber": 1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bp-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-bracket-page", "roundNumber": 1,
	}, "op", "s"), corr)

	w := getJSON(t, mux, "/v1/tournaments/t-bracket-page/bracket?limit=1")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	body := decodeBracket(t, w)
	for _, key := range []string{"tournamentId", "projectionVersion", "generatedAt", "summary", "slots"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("missing %s in %v", key, body)
		}
	}
	slots := body["slots"].([]any)
	if len(slots) != 1 {
		t.Fatalf("want 1 slot page, got %d", len(slots))
	}
	next, _ := body["nextCursor"].(string)
	if next == "" {
		t.Fatal("expected nextCursor")
	}
	if _, err := store.DecodeBracketCursor(next); err != nil {
		t.Fatalf("nextCursor decode: %v", err)
	}

	w2 := getJSON(t, mux, "/v1/tournaments/t-bracket-page/bracket?limit=1&cursor="+next)
	if w2.Code != http.StatusOK {
		t.Fatalf("page2 status=%d %s", w2.Code, w2.Body.String())
	}
	body2 := decodeBracket(t, w2)
	slots2 := body2["slots"].([]any)
	if len(slots2) != 1 {
		t.Fatalf("page2 slots=%d", len(slots2))
	}
	s0 := slots[0].(map[string]any)
	s1 := slots2[0].(map[string]any)
	if s0["slotIndex"] == s1["slotIndex"] {
		t.Fatalf("pages must advance slotIndex: %v %v", s0, s1)
	}
	if body["projectionVersion"].(float64) < 1 {
		t.Fatalf("projectionVersion=%v", body["projectionVersion"])
	}
}

func TestBracketPage_MalformedCursor400(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-bad-cursor"}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bc-create", "CreateTournament", map[string]any{
		"tournamentId": "t-bad-cursor", "capacity": 4,
	}, "op", "s"), corr)

	w := getJSON(t, mux, "/v1/tournaments/t-bad-cursor/bracket?cursor=not-opaque")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %s", w.Code, w.Body.String())
	}
	var errBody map[string]any
	_ = json.NewDecoder(w.Body).Decode(&errBody)
	if errBody["code"] != "bad_request" {
		t.Fatalf("err=%v", errBody)
	}
}

func TestBracketPage_NotFound(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	w := getJSON(t, mux, "/v1/tournaments/missing/bracket")
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
}

func TestBracketPage_InvalidLimit(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-limit"}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("bl-create", "CreateTournament", map[string]any{
		"tournamentId": "t-limit", "capacity": 2,
	}, "op", "s"), corr)
	w := getJSON(t, mux, "/v1/tournaments/t-limit/bracket?limit=1001")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %s", w.Code, w.Body.String())
	}
}
