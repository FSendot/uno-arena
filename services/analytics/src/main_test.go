package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const (
	testRoomCred       = "room-cred"
	testRankingCred    = "ranking-cred"
	testTournamentCred = "tournament-cred"
	testOpsCred        = "ops-cred"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return NewServer(NewMemoryAnalyticsStore(), ProducerCredentials{
		Room: testRoomCred, Ranking: testRankingCred,
		Tournament: testTournamentCred, Ops: testOpsCred,
	})
}

func doJSON(t *testing.T, mux http.Handler, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func withCred(cred string) map[string]string {
	return map[string]string{internalCredentialHeader: cred}
}

func validGameplayBody(eventID string) map[string]any {
	return map[string]any{
		"eventId":       eventID,
		"eventType":     "GameplayMetric",
		"source":        "attacker.forged.topic", // must be ignored
		"schemaVersion": 1,
		"correlationId": "corr_" + eventID,
		"occurredAt":    time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
		"payload": map[string]any{
			"visibility": "anonymized_adhoc",
			"metricType": "turn_advanced",
			"roomId":     "room_1",
		},
	}
}

func validTournamentBody(eventID, tournamentID string) map[string]any {
	return map[string]any{
		"eventId":       eventID,
		"eventType":     "TournamentStatistic",
		"source":        "attacker.forged.topic",
		"schemaVersion": 1,
		"correlationId": "corr_" + eventID,
		"occurredAt":    time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
		"payload": map[string]any{
			"tournamentId":    tournamentID,
			"phase":           "quarterfinal",
			"registeredCount": 64,
			"publicPayload": map[string]any{
				"bracketLabel": "QF-1",
			},
		},
	}
}

func TestHealthAndReadyOffline(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	hw := doJSON(t, mux, http.MethodGet, "/health", nil, nil)
	if hw.Code != http.StatusOK {
		t.Fatalf("health: %d", hw.Code)
	}
	rw := doJSON(t, mux, http.MethodGet, "/ready", nil, nil)
	if rw.Code != http.StatusOK {
		t.Fatalf("ready: %d", rw.Code)
	}
	var body map[string]string
	_ = json.NewDecoder(rw.Body).Decode(&body)
	if body["mode"] != "scoped" {
		t.Fatalf("mode=%q want scoped", body["mode"])
	}
}

func TestReady_FailsMissingOrCollidingCredentials(t *testing.T) {
	store := NewMemoryAnalyticsStore()

	missing := NewServer(store, ProducerCredentials{
		Room: "r", Ranking: "k", Tournament: "", Ops: "o",
	})
	w := doJSON(t, missing.routes(), http.MethodGet, "/ready", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing cred ready: %d", w.Code)
	}
	var body map[string]string
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["reason"] != "producer_credentials_incomplete" {
		t.Fatalf("reason=%q", body["reason"])
	}

	collide := NewServer(store, ProducerCredentials{
		Room: "same", Ranking: "same", Tournament: "t", Ops: "o",
	})
	w = doJSON(t, collide.routes(), http.MethodGet, "/ready", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("collide ready: %d", w.Code)
	}
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["reason"] != "producer_credentials_not_distinct" {
		t.Fatalf("reason=%q", body["reason"])
	}

	// Shared single credential across roles is not ready (no offline single-role mode).
	shared := NewServer(store, ProducerCredentials{
		Room: "shared", Ranking: "shared", Tournament: "shared", Ops: "shared",
	})
	w = doJSON(t, shared.routes(), http.MethodGet, "/ready", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("shared cred ready: %d want 503", w.Code)
	}
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["reason"] != "producer_credentials_not_distinct" {
		t.Fatalf("shared reason=%q", body["reason"])
	}
}

func TestLeastPrivilegeCredentials(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	body := validGameplayBody("auth_1")

	// Room cred on room route OK.
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/room/events", body, withCred(testRoomCred))
	if w.Code != http.StatusOK {
		t.Fatalf("room: %d %s", w.Code, w.Body.String())
	}

	// Ranking cred cannot publish room events.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/room/events", body, withCred(testRankingCred))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("ranking→room expected 401, got %d", w.Code)
	}

	// Room cred cannot rebuild.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/rebuild", map[string]any{"events": []any{}}, withCred(testRoomCred))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("room→rebuild expected 401, got %d", w.Code)
	}

	// Ops can rebuild.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/rebuild", map[string]any{"events": []any{}}, withCred(testOpsCred))
	if w.Code != http.StatusOK {
		t.Fatalf("ops rebuild: %d", w.Code)
	}
}

func TestSourceTopicFromCredentialNotBody(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	body := validGameplayBody("g1")
	body["source"] = "untrusted.topic"
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/room/events", body, withCred(testRoomCred))
	if w.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", w.Code, w.Body.String())
	}
	var out map[string]any
	_ = json.NewDecoder(w.Body).Decode(&out)
	if out["kind"] != "accepted" {
		t.Fatalf("forged body source must be ignored; got %+v", out)
	}

	pw := doJSON(t, mux, http.MethodGet, "/v1/analytics/public", nil, nil)
	var snap map[string]any
	_ = json.NewDecoder(pw.Body).Decode(&snap)
	if snap["authoritative"] != false {
		t.Fatalf("camelCase authoritative: %+v", snap)
	}
	metrics, _ := snap["gameplayMetrics"].([]any)
	if len(metrics) != 1 {
		t.Fatalf("gameplayMetrics=%v", snap["gameplayMetrics"])
	}
	m0 := metrics[0].(map[string]any)
	if m0["metricType"] != "turn_advanced" || m0["roomId"] != "room_1" {
		t.Fatalf("camelCase metric=%+v", m0)
	}
}

func TestTournamentAndRankingRoutes(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()

	w := doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/tournament/events",
		validTournamentBody("t1", "tour_9"), withCred(testTournamentCred))
	if w.Code != http.StatusOK {
		t.Fatalf("tournament: %d %s", w.Code, w.Body.String())
	}

	ratingBody := map[string]any{
		"eventId": "r1", "eventType": "RatingStatistic", "schemaVersion": 1,
		"correlationId": "c", "occurredAt": time.Now().UTC().Format(time.RFC3339),
		"payload": map[string]any{
			"playerId": "p1", "sourceType": "casual_elo",
			"previousRating": 1000, "newRating": 1016,
		},
	}
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/ranking/events", ratingBody, withCred(testRankingCred))
	if w.Code != http.StatusOK {
		t.Fatalf("ranking: %d %s", w.Code, w.Body.String())
	}
	var out map[string]any
	_ = json.NewDecoder(w.Body).Decode(&out)
	if out["kind"] != "accepted" {
		t.Fatalf("ranking out=%+v", out)
	}
}

func TestConcurrentIngestRace(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "race_" + itoa(i)
			doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/room/events",
				validGameplayBody(id), withCred(testRoomCred))
		}(i)
	}
	wg.Wait()
}

func TestIngestionLagOpsOnly(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/room/events", validGameplayBody("lag_1"), withCred(testRoomCred))
	w := doJSON(t, mux, http.MethodGet, "/internal/v1/analytics/ingestion-lag", nil, withCred(testOpsCred))
	if w.Code != http.StatusOK {
		t.Fatalf("lag: %d", w.Code)
	}
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["projectionVersion"] != float64(1) {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestRebuildRequiresTrustedSource(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	body := validGameplayBody("r1")
	body["source"] = "room.gameplay.metrics"
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/rebuild", map[string]any{
		"events": []any{body},
	}, withCred(testOpsCred))
	if w.Code != http.StatusOK {
		t.Fatalf("rebuild: %d %s", w.Code, w.Body.String())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestFinding_QuarantineIngestReturnsConflict(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	body := validTournamentBody("bad1", "tour_q")
	body["payload"] = map[string]any{
		"tournamentId": "tour_q",
		"phase":        "final",
		"publicPayload": map[string]any{
			"bracketLabel": "F-1",
			"playerEmail":  "leak@example.com",
		},
	}
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/analytics/tournament/events", body, withCred(testTournamentCred))
	if w.Code == http.StatusOK {
		t.Fatalf("quarantine must be non-2xx, body=%s", w.Body.String())
	}
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d want 409 body=%s", w.Code, w.Body.String())
	}
}
