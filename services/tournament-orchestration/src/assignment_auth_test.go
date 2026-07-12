package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"unoarena/services/tournament-orchestration/domain"
	"unoarena/shared/envelope"
)

func TestPrivateBracketRequiresParticipantOrOperator(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("priv-create", "CreateTournament", map[string]any{
		"tournamentId": "t-priv",
		"capacity":     4,
		"visibility":   "private",
	}, "op1", "s1"), nil)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("create status=%s", res.Status)
	}

	w = getJSON(t, mux, "/v1/tournaments/t-priv/bracket")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous private bracket want 401, got %d %s", w.Code, w.Body.String())
	}

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-priv/bracket", map[string]string{
		"X-Player-Id": "stranger",
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("nonparticipant want 403, got %d %s", w.Code, w.Body.String())
	}

	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("priv-reg", "RegisterPlayer", map[string]any{
		"tournamentId": "t-priv",
		"playerId":     "p1",
	}, "p1", "s1"), nil)

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-priv/bracket", map[string]string{
		"X-Player-Id": "p1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("participant want 200, got %d %s", w.Code, w.Body.String())
	}

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-priv/standings", map[string]string{
		"X-Operator-Scope": "1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("operator want 200, got %d %s", w.Code, w.Body.String())
	}
}

func TestPublicBracketAllowsAnonymousWithServiceCred(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("pub-create", "CreateTournament", map[string]any{
		"tournamentId": "t-pub",
		"capacity":     4,
	}, "op1", "s1"), nil)
	w := getJSON(t, mux, "/v1/tournaments/t-pub/bracket")
	if w.Code != http.StatusOK {
		t.Fatalf("public anonymous want 200, got %d %s", w.Code, w.Body.String())
	}
}

func TestPlayerAssignment_AuthAndNullUntilSeeded(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("asg-create", "CreateTournament", map[string]any{
		"tournamentId": "t-asg",
		"capacity":     4,
		"batchSize":    10,
	}, "op1", "s1"), nil)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("asg-reg", "RegisterPlayer", map[string]any{
		"tournamentId": "t-asg", "playerId": "p1",
	}, "p1", "s1"), nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t-asg/players/p1/assignment", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no cred want 401, got %d", w.Code)
	}

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-asg/players/p1/assignment", map[string]string{
		"X-Player-Id": "other",
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("wrong player want 403, got %d %s", w.Code, w.Body.String())
	}

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-asg/players/p1/assignment", map[string]string{
		"X-Player-Id": "p1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("self assignment want 200, got %d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["assignment"] != nil {
		t.Fatalf("pre-seed assignment want null, got %#v", body["assignment"])
	}
	if body["visibility"] != "public" || body["registrationStatus"] != "registered" {
		t.Fatalf("body=%v", body)
	}

	for _, p := range []string{"p2", "p3", "p4"} {
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("asg-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-asg", "playerId": p,
		}, p, "s1"), nil)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("asg-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-asg",
	}, "op1", "s1"), nil)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("asg-seed", "SeedRound", map[string]any{
		"tournamentId": "t-asg", "roundNumber": 1,
	}, "op1", "s1"), nil)

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-asg/players/p1/assignment", map[string]string{
		"X-Player-Id": "p1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("seeded assignment want 200, got %d %s", w.Code, w.Body.String())
	}
	body = map[string]any{}
	_ = json.NewDecoder(w.Body).Decode(&body)
	asg, ok := body["assignment"].(map[string]any)
	if !ok || asg["slotId"] == nil || asg["roundNumber"].(float64) != 1 {
		t.Fatalf("expected round1 assignment, got %#v", body["assignment"])
	}
	if _, hasRoom := asg["roomId"]; hasRoom {
		t.Fatalf("pre-provision roomId should be omitted, got %#v", asg)
	}

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-asg/players/p2/assignment", map[string]string{
		"X-Player-Id":      "op1",
		"X-Operator-Scope": "1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("operator assignment want 200, got %d %s", w.Code, w.Body.String())
	}

	w = getJSONHeaders(t, mux, "/v1/tournaments/t-asg/players/missing/assignment", map[string]string{
		"X-Operator-Scope": "1",
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing player want 404, got %d", w.Code)
	}
}

func TestCreateTournament_VisibilityFactInMemory(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-mem", "CreateTournament", map[string]any{
		"tournamentId": "t-vis-mem",
		"capacity":     2,
		"visibility":   "private",
	}, "op1", "s1"), nil)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", res)
	}
	tr, ok := h.repo.Get(domain.TournamentID("t-vis-mem"))
	if !ok || tr.Visibility() != domain.TournamentVisibilityPrivate {
		t.Fatalf("memory visibility ok=%v", ok)
	}
}

func TestCreateTournament_RejectsUnknownVisibility(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("vis-bad", "CreateTournament", map[string]any{
		"tournamentId": "t-vis-bad",
		"capacity":     2,
		"visibility":   "secret",
	}, "op1", "s1"), nil)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("want rejected, got %+v", res)
	}
}

func TestBracketStandingsRequireServiceCredential(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cred-create", "CreateTournament", map[string]any{
		"tournamentId": "t-cred",
		"capacity":     2,
	}, "op1", "s1"), nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t-cred/bracket", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no service credential want 401, got %d", w.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/tournaments/t-cred/standings", nil)
	req.Header.Set("X-Service-Credential", "wrong")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong credential want 401, got %d", w.Code)
	}
}
