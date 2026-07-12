package bff

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"unoarena/shared/correlation"
)

func TestTournamentReads_BracketQueryPreservationAndBodyPassthrough(t *testing.T) {
	tournament := NewFakeTournament()
	tournament.BracketJSON = json.RawMessage(`{"tournamentId":"t-br","projectionVersion":3,"generatedAt":"2026-01-01T00:00:00Z","summary":{"phase":"in_progress"},"slots":[{"slotIndex":1}]}`)
	srv := NewServer(Dependencies{
		Identity:   NewFakeIdentity(),
		Room:       NewFakeRoom(),
		Tournament: tournament,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	mux := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t-br/bracket?limit=2&cursor=abc&roundNumber=1", nil)
	req.Header.Set("X-Correlation-Id", "corr-bracket-proxy")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if tournament.LastBracketQuery != "limit=2&cursor=abc&roundNumber=1" {
		t.Fatalf("rawQuery=%q", tournament.LastBracketQuery)
	}
	if tournament.LastCorr.CorrelationID != "corr-bracket-proxy" {
		t.Fatalf("corr=%+v", tournament.LastCorr)
	}
	if !strings.Contains(w.Body.String(), `"tournamentId":"t-br"`) {
		t.Fatalf("body=%s", w.Body.String())
	}
	if w.Header().Get("X-Correlation-Id") != "corr-bracket-proxy" {
		t.Fatalf("response corr=%s", w.Header().Get("X-Correlation-Id"))
	}
}

func TestTournamentReads_StandingsBodyAndCorrelation(t *testing.T) {
	tournament := NewFakeTournament()
	tournament.StandingsJSON = json.RawMessage(`{"tournamentId":"t-st","projectionVersion":2,"generatedAt":"2026-01-01T00:00:00Z","phase":"completed","registeredCount":4,"currentRound":1,"finalStandings":["p1","p2"]}`)
	srv := NewServer(Dependencies{
		Identity:   NewFakeIdentity(),
		Room:       NewFakeRoom(),
		Tournament: tournament,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	mux := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t-st/standings", nil)
	req.Header.Set("X-Correlation-Id", "corr-standings-proxy")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"finalStandings":["p1","p2"]`) {
		t.Fatalf("body=%s", w.Body.String())
	}
	if tournament.LastCorr.CorrelationID != "corr-standings-proxy" {
		t.Fatalf("corr=%+v", tournament.LastCorr)
	}
}

func TestTournamentReads_ErrorMappingAndPathMethod(t *testing.T) {
	tournament := NewFakeTournament()
	srv := NewServer(Dependencies{
		Identity:   NewFakeIdentity(),
		Room:       NewFakeRoom(),
		Tournament: tournament,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	mux := srv.Handler()

	tournament.BracketErr = &httpStatusError{status: http.StatusBadRequest, body: `{"code":"secret-upstream","message":"internal detail"}`}
	w400 := httptest.NewRecorder()
	mux.ServeHTTP(w400, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/bracket?cursor=x", nil))
	if w400.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w400.Code)
	}
	if strings.Contains(w400.Body.String(), "secret-upstream") || strings.Contains(w400.Body.String(), "internal detail") {
		t.Fatalf("leaked upstream: %s", w400.Body.String())
	}

	tournament.BracketErr = &httpStatusError{status: http.StatusNotFound, body: `{"code":"secret-upstream"}`}
	w404 := httptest.NewRecorder()
	mux.ServeHTTP(w404, httptest.NewRequest(http.MethodGet, "/v1/tournaments/missing/bracket", nil))
	if w404.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w404.Code)
	}
	if strings.Contains(w404.Body.String(), "secret-upstream") {
		t.Fatal("must not leak upstream body")
	}

	tournament.StandingsErr = &httpStatusError{status: http.StatusNotFound, body: `{"code":"secret-upstream"}`}
	w404s := httptest.NewRecorder()
	mux.ServeHTTP(w404s, httptest.NewRequest(http.MethodGet, "/v1/tournaments/missing/standings", nil))
	if w404s.Code != http.StatusNotFound {
		t.Fatalf("standings want 404, got %d", w404s.Code)
	}

	tournament.BracketErr = &httpStatusError{status: http.StatusServiceUnavailable, body: `{"code":"secret-upstream"}`}
	w502 := httptest.NewRecorder()
	mux.ServeHTTP(w502, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/bracket", nil))
	if w502.Code != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", w502.Code)
	}

	tournament.BracketErr = io.ErrUnexpectedEOF
	wNet := httptest.NewRecorder()
	mux.ServeHTTP(wNet, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/bracket", nil))
	if wNet.Code != http.StatusBadGateway {
		t.Fatalf("network want 502, got %d", wNet.Code)
	}

	wMeth := httptest.NewRecorder()
	mux.ServeHTTP(wMeth, httptest.NewRequest(http.MethodPost, "/v1/tournaments/t1/bracket", nil))
	if wMeth.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", wMeth.Code)
	}

	wPath := httptest.NewRecorder()
	mux.ServeHTTP(wPath, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/registrations", nil))
	if wPath.Code != http.StatusNotFound {
		t.Fatalf("want 404 for non-read path, got %d", wPath.Code)
	}

	wMalformed := httptest.NewRecorder()
	mux.ServeHTTP(wMalformed, httptest.NewRequest(http.MethodGet, "/v1/tournaments/", nil))
	if wMalformed.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", wMalformed.Code)
	}
}

func TestHTTPTournamentClient_ReadsForwardCredentialAndPrincipal(t *testing.T) {
	var sawCred bool
	var gotPath, gotRaw, gotCorr, gotPlayer, gotOp string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/tournaments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Credential") == "svc-tournament" {
			sawCred = true
		}
		gotPath = r.URL.Path
		gotRaw = r.URL.RawQuery
		gotCorr = r.Header.Get(correlation.HeaderCorrelationID)
		gotPlayer = r.Header.Get("X-Player-Id")
		gotOp = r.Header.Get("X-Operator-Scope")
		// Spoof headers from client must never be forwarded; only principal-derived.
		if r.Header.Get("X-Spoof") != "" {
			t.Fatal("must not forward unrelated client headers")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	upstream := httptest.NewServer(mux)
	t.Cleanup(upstream.Close)

	client := NewHTTPTournamentClient(HTTPClientConfig{
		BaseURL:           upstream.URL,
		ServiceCredential: "svc-tournament",
		HTTPClient:        upstream.Client(),
	})
	corr := correlation.Headers{CorrelationID: "corr-http-tournament-read"}
	principal := &Principal{PlayerID: "p1", OperatorScope: true}
	raw, err := client.Bracket(t.Context(), "tid-1", "limit=5&cursor=c1", corr, principal)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"ok":true}` {
		t.Fatalf("raw=%s", raw)
	}
	if !sawCred {
		t.Fatal("tournament bracket read must send service credential")
	}
	if gotPath != "/v1/tournaments/tid-1/bracket" {
		t.Fatalf("path=%s", gotPath)
	}
	if gotRaw != "limit=5&cursor=c1" {
		t.Fatalf("rawQuery=%s", gotRaw)
	}
	if gotCorr != "corr-http-tournament-read" {
		t.Fatalf("corr=%s", gotCorr)
	}
	if gotPlayer != "p1" || gotOp != "1" {
		t.Fatalf("principal headers player=%q op=%q", gotPlayer, gotOp)
	}

	sawCred = false
	gotPlayer, gotOp = "", ""
	_, err = client.Standings(t.Context(), "tid-2", corr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !sawCred {
		t.Fatal("tournament standings read must send service credential")
	}
	if gotPath != "/v1/tournaments/tid-2/standings" {
		t.Fatalf("standings path=%s", gotPath)
	}
	if gotPlayer != "" || gotOp != "" {
		t.Fatalf("anonymous must not set principal headers: player=%q op=%q", gotPlayer, gotOp)
	}

	sawCred = false
	_, err = client.Assignment(t.Context(), "tid-3", "p9", corr, &Principal{PlayerID: "p9"})
	if err != nil {
		t.Fatal(err)
	}
	if !sawCred {
		t.Fatal("assignment read must send service credential")
	}
	if gotPath != "/v1/tournaments/tid-3/players/p9/assignment" {
		t.Fatalf("assignment path=%s", gotPath)
	}
	if gotPlayer != "p9" || gotOp != "" {
		t.Fatalf("assignment principal player=%q op=%q", gotPlayer, gotOp)
	}
}

func TestHTTPTournamentClient_PublicReadsRejectEmptyAndMalformedJSON(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"malformed", `{not-json`},
		{"truncated", `{"ok":`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(upstream.Close)
			client := NewHTTPTournamentClient(HTTPClientConfig{
				BaseURL:    upstream.URL,
				HTTPClient: upstream.Client(),
			})
			_, err := client.Standings(t.Context(), "tid", correlation.Headers{CorrelationID: "c"}, nil)
			if err == nil {
				t.Fatal("expected decode error")
			}
			if strings.Contains(err.Error(), tc.body) && tc.body != "" {
				t.Fatalf("must not expose malformed body: %v", err)
			}
		})
	}

	valid := `{"tournamentId":"t1","projectionVersion":1,"generatedAt":"2026-01-01T00:00:00Z","phase":"registration","registeredCount":0,"currentRound":0,"finalStandings":[]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(valid))
	}))
	t.Cleanup(upstream.Close)
	client := NewHTTPTournamentClient(HTTPClientConfig{
		BaseURL:    upstream.URL,
		HTTPClient: upstream.Client(),
	})
	raw, err := client.Bracket(t.Context(), "t1", "", correlation.Headers{CorrelationID: "c"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != valid {
		t.Fatalf("passthrough want exact bytes, got %s", raw)
	}
}

func TestTournamentReads_UpstreamEmptyOrMalformedBodyMaps502(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"malformed", `{"broken`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(upstream.Close)

			client := NewHTTPTournamentClient(HTTPClientConfig{
				BaseURL:    upstream.URL,
				HTTPClient: upstream.Client(),
			})
			srv := NewServer(Dependencies{
				Identity:   NewFakeIdentity(),
				Room:       NewFakeRoom(),
				Tournament: client,
				Reads:      &FakeReads{},
				Spectator:  NewFakeSpectatorGate(),
				Ready:      true,
			})
			mux := srv.Handler()

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/standings", nil))
			if w.Code != http.StatusBadGateway {
				t.Fatalf("want 502, got %d body=%s", w.Code, w.Body.String())
			}
			if tc.body != "" && strings.Contains(w.Body.String(), tc.body) {
				t.Fatalf("must not expose malformed upstream body: %s", w.Body.String())
			}
		})
	}

	valid := `{"tournamentId":"t-ok","projectionVersion":1,"generatedAt":"2026-01-01T00:00:00Z","phase":"registration","registeredCount":0,"currentRound":0,"finalStandings":[]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(valid))
	}))
	t.Cleanup(upstream.Close)
	client := NewHTTPTournamentClient(HTTPClientConfig{
		BaseURL:    upstream.URL,
		HTTPClient: upstream.Client(),
	})
	srv := NewServer(Dependencies{
		Identity:   NewFakeIdentity(),
		Room:       NewFakeRoom(),
		Tournament: client,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t-ok/standings", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	got := strings.TrimSpace(w.Body.String())
	if got != valid {
		t.Fatalf("exact passthrough want %s got %s", valid, got)
	}
}

func TestClosedTournament_ReadsFailClosed(t *testing.T) {
	var c ClosedTournament
	if _, err := c.Bracket(t.Context(), "t", "", correlation.Headers{}, nil); err == nil {
		t.Fatal("expected error")
	}
	if _, err := c.Standings(t.Context(), "t", correlation.Headers{}, nil); err == nil {
		t.Fatal("expected error")
	}
	if _, err := c.Assignment(t.Context(), "t", "p", correlation.Headers{}, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestTournamentReads_OptionalBearerInvalidIs401(t *testing.T) {
	identity := NewFakeIdentity()
	tournament := NewFakeTournament()
	srv := NewServer(Dependencies{
		Identity:   identity,
		Room:       NewFakeRoom(),
		Tournament: tournament,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	mux := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/bracket", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	req.Header.Set("X-Correlation-Id", "corr-bad-bearer")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("invalid bearer want 401, got %d %s", w.Code, w.Body.String())
	}
	if tournament.LastCorr.CorrelationID == "corr-bad-bearer" {
		t.Fatal("invalid bearer must not proxy bracket upstream")
	}
}

func TestTournamentReads_AssignmentAuthOwnershipAndStatusMapping(t *testing.T) {
	identity := NewFakeIdentity()
	identity.SeedSession("tok-p1", Principal{PlayerID: "p1", SessionID: "s1"})
	identity.SeedSession("tok-op", Principal{PlayerID: "op1", SessionID: "sop", OperatorScope: true})
	tournament := NewFakeTournament()
	srv := NewServer(Dependencies{
		Identity:   identity,
		Room:       NewFakeRoom(),
		Tournament: tournament,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	mux := srv.Handler()

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/players/p1/assignment", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer want 401, got %d", w.Code)
	}

	reqWrong := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/players/p1/assignment", nil)
	reqWrong.Header.Set("Authorization", "Bearer tok-p1")
	// Spoofed operator header must not grant cross-player access.
	reqWrong.Header.Set("X-Operator-Scope", "1")
	reqWrong.Header.Set("X-Player-Id", "op1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, reqWrong)
	// tok-p1 is p1 requesting p1 — allowed. Use other player's path:
	reqWrong = httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/players/other/assignment", nil)
	reqWrong.Header.Set("Authorization", "Bearer tok-p1")
	reqWrong.Header.Set("X-Operator-Scope", "1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, reqWrong)
	if w.Code != http.StatusForbidden {
		t.Fatalf("spoofed operator want 403, got %d %s", w.Code, w.Body.String())
	}

	reqSelf := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/players/p1/assignment", nil)
	reqSelf.Header.Set("Authorization", "Bearer tok-p1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, reqSelf)
	if w.Code != http.StatusOK {
		t.Fatalf("self assignment want 200, got %d %s", w.Code, w.Body.String())
	}
	if tournament.LastPrincipal == nil || tournament.LastPrincipal.PlayerID != "p1" || tournament.LastPrincipal.OperatorScope {
		t.Fatalf("principal=%+v", tournament.LastPrincipal)
	}

	reqOp := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/players/p1/assignment", nil)
	reqOp.Header.Set("Authorization", "Bearer tok-op")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, reqOp)
	if w.Code != http.StatusOK {
		t.Fatalf("operator assignment want 200, got %d", w.Code)
	}
	if tournament.LastPrincipal == nil || !tournament.LastPrincipal.OperatorScope {
		t.Fatalf("operator principal=%+v", tournament.LastPrincipal)
	}

	tournament.AssignmentErr = &httpStatusError{status: http.StatusUnauthorized, body: `{"code":"secret"}`}
	reqSelf = httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/players/p1/assignment", nil)
	reqSelf.Header.Set("Authorization", "Bearer tok-p1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, reqSelf)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("upstream 401 want 401, got %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "secret") {
		t.Fatal("must not leak upstream body")
	}

	tournament.AssignmentErr = &httpStatusError{status: http.StatusForbidden, body: `{"code":"secret"}`}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, reqSelf)
	if w.Code != http.StatusForbidden {
		t.Fatalf("upstream 403 want 403, got %d", w.Code)
	}

	tournament.BracketErr = &httpStatusError{status: http.StatusForbidden, body: `{"code":"secret"}`}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/bracket", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("bracket upstream 403 want 403, got %d", w.Code)
	}
}

func TestTournamentReads_SpoofedOperatorHeaderIgnoredOnBracket(t *testing.T) {
	identity := NewFakeIdentity()
	identity.SeedSession("tok-p1", Principal{PlayerID: "p1", SessionID: "s1"})
	tournament := NewFakeTournament()
	srv := NewServer(Dependencies{
		Identity:   identity,
		Room:       NewFakeRoom(),
		Tournament: tournament,
		Reads:      &FakeReads{},
		Spectator:  NewFakeSpectatorGate(),
		Ready:      true,
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/tournaments/t1/standings", nil)
	req.Header.Set("Authorization", "Bearer tok-p1")
	req.Header.Set("X-Operator-Scope", "1")
	req.Header.Set("X-Player-Id", "spoofed")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	if tournament.LastPrincipal == nil || tournament.LastPrincipal.PlayerID != "p1" || tournament.LastPrincipal.OperatorScope {
		t.Fatalf("must use validated Identity principal only, got %+v", tournament.LastPrincipal)
	}
}
