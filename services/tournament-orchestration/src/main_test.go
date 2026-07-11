package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"unoarena/shared/envelope"
)

const testCred = "tournament-test-credential"

type testHarness struct {
	srv   *Server
	svc   *Service
	repo  *MemoryTournamentRepository
	audit *MemoryAudit
	rooms *FakeRoomProvisioner
	pub   *FakePublisher
}

func newTestHarness(t *testing.T) testHarness {
	t.Helper()
	auditSink := NewMemoryAudit()
	rooms := NewFakeRoomProvisioner()
	pub := NewFakePublisher()
	repo := NewMemoryTournamentRepository()
	svc := NewService(ServiceDeps{
		Repo:      repo,
		Rooms:     rooms,
		Publisher: pub,
		Audit:     auditSink,
		Clock:     fixedClock{now: time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC)},
		IDs:       &seqIDs{},
	})
	srv := NewServer(svc, testCred)
	return testHarness{srv: srv, svc: svc, repo: repo, audit: auditSink, rooms: rooms, pub: pub}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type seqIDs struct{ n int }

func (s *seqIDs) NewID(prefix string) string {
	s.n++
	return prefix + "-" + itoa(s.n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func postJSON(t *testing.T, mux http.Handler, path string, cred string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	switch v := body.(type) {
	case nil:
		rdr = nil
	case []byte:
		rdr = bytes.NewReader(v)
	case string:
		rdr = strings.NewReader(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(http.MethodPost, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cred != "" {
		req.Header.Set(headerServiceCredential, cred)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func getJSON(t *testing.T, mux http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func decodeResult(t *testing.T, w *httptest.ResponseRecorder) envelope.Result {
	t.Helper()
	var res envelope.Result
	if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
		t.Fatalf("decode result: %v body=%s", err, w.Body.String())
	}
	return res
}

func commandBody(commandID, typ string, payload map[string]any, playerID, sessionID string) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	return map[string]any{
		"commandId":     commandID,
		"type":          typ,
		"schemaVersion": 1,
		"payload":       payload,
		"playerId":      playerID,
		"sessionId":     sessionID,
	}
}

func provisionAllBatches(t *testing.T, h testHarness, mux http.Handler, tournamentID string, roundNumber int, corr map[string]string) {
	t.Helper()
	bracket := getJSON(t, mux, "/v1/tournaments/"+tournamentID+"/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	rounds, _ := body["rounds"].([]any)
	if len(rounds) == 0 {
		t.Fatal("no rounds")
	}
	round0 := rounds[0].(map[string]any)
	batches, _ := round0["batches"].([]any)
	for i, raw := range batches {
		b := raw.(map[string]any)
		w := postJSON(t, mux, "/internal/v1/tournaments/"+tournamentID+"/rounds/"+itoa(roundNumber)+"/provisioning-batches", testCred, map[string]any{
			"commandId":     "worker-" + tournamentID + "-" + itoa(i),
			"schemaVersion": 1,
			"batchId":       b["batchId"],
			"slotFrom":      b["slotFrom"],
			"slotTo":        b["slotTo"],
			"slotSize":      b["slotSize"],
		}, corr)
		if w.Code != http.StatusOK {
			t.Fatalf("worker batch %v: %d %s", b["batchId"], w.Code, w.Body.String())
		}
		res := decodeResult(t, w)
		if res.Status != envelope.StatusAccepted {
			t.Fatalf("worker batch rejected: %+v", res)
		}
	}
}

func TestHealthAndReady(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()

	w := getJSON(t, mux, "/health")
	if w.Code != http.StatusOK {
		t.Fatalf("health status=%d", w.Code)
	}
	var health map[string]string
	_ = json.NewDecoder(w.Body).Decode(&health)
	if health["status"] != "ok" || health["service"] != "tournament-orchestration" {
		t.Fatalf("health=%v", health)
	}

	w = getJSON(t, mux, "/ready")
	if w.Code != http.StatusOK {
		t.Fatalf("ready status=%d", w.Code)
	}
}

func TestInternalCommandsRequireServiceCredential(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()

	w := postJSON(t, mux, "/internal/v1/commands", "", commandBody("c1", "CreateTournament", map[string]any{
		"tournamentId": "t1",
		"capacity":     4,
	}, "p1", "s1"), map[string]string{"X-Correlation-Id": "corr-1"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}

	w = postJSON(t, mux, "/internal/v1/commands", "wrong", commandBody("c1", "CreateTournament", map[string]any{
		"tournamentId": "t1",
		"capacity":     4,
	}, "p1", "s1"), map[string]string{"X-Correlation-Id": "corr-1"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong cred, got %d", w.Code)
	}
}

func TestCreateRegisterCloseSeedProvisionAdvanceFinalize(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-flow"}

	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-create", "CreateTournament", map[string]any{
		"tournamentId": "tour-1",
		"capacity":     4,
		"batchSize":    10,
		"retryBudget":  2,
	}, "op1", "sess-op"), corr)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted || res.Type != "CreateTournament" || res.SchemaVersion != 1 {
		t.Fatalf("create result=%+v", res)
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-create", "CreateTournament", map[string]any{
		"tournamentId": "tour-1",
		"capacity":     4,
	}, "op1", "sess-op"), corr)
	res = decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("create replay: %+v", res)
	}

	for _, p := range []string{"p1", "p2", "p3", "p4"} {
		w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "tour-1",
			"playerId":     p,
		}, p, "sess-"+p), corr)
		res = decodeResult(t, w)
		if res.Status != envelope.StatusAccepted {
			t.Fatalf("register %s: %+v", p, res)
		}
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-close", "CloseRegistration", map[string]any{
		"tournamentId": "tour-1",
	}, "op1", "sess-op"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("close rejected: %s", w.Body.String())
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-seed", "SeedRound", map[string]any{
		"tournamentId": "tour-1",
		"roundNumber":  1,
	}, "op1", "sess-op"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("seed rejected: %s", w.Body.String())
	}

	beforeRooms := h.rooms.CallCount()
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "tour-1",
		"roundNumber":  1,
	}, "op1", "sess-op"), corr)
	res = decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("provision rejected: %s", w.Body.String())
	}
	if h.rooms.CallCount() != beforeRooms {
		t.Fatalf("ProvisionRoundMatches must not call Room synchronously, got %d calls", h.rooms.CallCount()-beforeRooms)
	}
	if h.repo.OutboxLen() == 0 {
		t.Fatal("expected outbox facts after accepted provision")
	}

	provisionAllBatches(t, h, mux, "tour-1", 1, corr)
	if h.rooms.CallCount() < 1 {
		t.Fatalf("expected room provisioning calls from worker, got %d", h.rooms.CallCount())
	}

	bracket := getJSON(t, mux, "/v1/tournaments/tour-1/bracket")
	if bracket.Code != http.StatusOK {
		t.Fatalf("bracket: %d %s", bracket.Code, bracket.Body.String())
	}
	var bracketBody map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&bracketBody)
	rounds, _ := bracketBody["rounds"].([]any)
	if len(rounds) != 1 {
		t.Fatalf("bracket rounds=%v", bracketBody)
	}
	round0 := rounds[0].(map[string]any)
	slots := round0["slots"].([]any)
	slot0 := slots[0].(map[string]any)
	roomID, _ := slot0["roomId"].(string)
	slotID, _ := slot0["slotId"].(string)
	if roomID == "" || slotID == "" {
		t.Fatalf("slot missing ids: %+v", slot0)
	}

	base := time.Date(2026, 7, 10, 19, 0, 0, 0, time.UTC)
	standings := []map[string]any{
		{"playerId": "p1", "matchWins": 2, "cumulativeCardPoints": 40, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
		{"playerId": "p2", "matchWins": 1, "cumulativeCardPoints": 30, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
		{"playerId": "p3", "matchWins": 0, "cumulativeCardPoints": 20, "finalGameCompletedAt": base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
		{"playerId": "p4", "matchWins": 0, "cumulativeCardPoints": 10, "finalGameCompletedAt": base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
	}
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-result", "RecordMatchResult", map[string]any{
		"tournamentId":      "tour-1",
		"roomId":            roomID,
		"roundNumber":       1,
		"slotId":            slotID,
		"completionVersion": 1,
		"eventId":           "evt-match-1",
		"standings":         standings,
	}, "op1", "sess-op"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("record result: %s", w.Body.String())
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-advance", "CompleteRound", map[string]any{
		"tournamentId": "tour-1",
		"roundNumber":  1,
	}, "op1", "sess-op"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("advance/complete round: %s", w.Body.String())
	}

	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-finalize", "CompleteTournament", map[string]any{
		"tournamentId": "tour-1",
	}, "op1", "sess-op"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("finalize: %s", w.Body.String())
	}

	standingsW := getJSON(t, mux, "/v1/tournaments/tour-1/standings")
	if standingsW.Code != http.StatusOK {
		t.Fatalf("standings: %d %s", standingsW.Code, standingsW.Body.String())
	}
	var standingsBody map[string]any
	_ = json.NewDecoder(standingsW.Body).Decode(&standingsBody)
	if standingsBody["phase"] != "completed" || standingsBody["championId"] != "p1" {
		t.Fatalf("standings=%v", standingsBody)
	}

	if h.audit.Len() != 0 {
		t.Fatalf("unexpected rejections: %+v", h.audit.Records())
	}
}

func TestRejectionIsOperationalAuditOnly(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-rej"}

	_ = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-c", "CreateTournament", map[string]any{
		"tournamentId": "t-rej",
		"capacity":     2,
	}, "op", "s"), corr)

	before := h.repo.OutboxLen()
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-bad-reg", "RegisterPlayer", map[string]any{
		"tournamentId": "t-rej",
	}, "", "s"), corr)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("expected rejected, got %+v", res)
	}
	if h.audit.Len() != 1 {
		t.Fatalf("expected 1 audit record, got %d", h.audit.Len())
	}
	rec := h.audit.Records()[0]
	if rec.CommandID != "cmd-bad-reg" || rec.CorrelationID != "corr-rej" || rec.Reason == "" {
		t.Fatalf("audit=%+v", rec)
	}
	if err := rec.Validate(); err != nil {
		t.Fatalf("audit validate: %v", err)
	}
	if h.repo.OutboxLen() != before {
		t.Fatalf("rejection must not append outbox facts")
	}

	// Stable rejected idempotency: no re-audit.
	w = postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("cmd-bad-reg", "RegisterPlayer", map[string]any{
		"tournamentId": "t-rej",
	}, "", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusRejected {
		t.Fatal("replay must stay rejected")
	}
	if h.audit.Len() != 1 {
		t.Fatalf("rejected replay must not re-audit, got %d", h.audit.Len())
	}
}

func TestStrictSchemaVersion(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	body := map[string]any{
		"commandId":     "cmd-sv",
		"type":          "CreateTournament",
		"schemaVersion": 2,
		"payload":       map[string]any{"tournamentId": "t", "capacity": 2},
		"playerId":      "p",
		"sessionId":     "s",
	}
	w := postJSON(t, mux, "/internal/v1/commands", testCred, body, map[string]string{"X-Correlation-Id": "corr-sv"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestMatchCompletedIngestionIdempotentAndConflictQuarantine(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-ingest"}

	setupFinalFour(t, h, mux, corr)

	bracket := getJSON(t, mux, "/v1/tournaments/tour-ingest/bracket")
	var bracketBody map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&bracketBody)
	slot0 := bracketBody["rounds"].([]any)[0].(map[string]any)["slots"].([]any)[0].(map[string]any)
	roomID := slot0["roomId"].(string)
	slotID := slot0["slotId"].(string)

	base := time.Date(2026, 7, 10, 20, 0, 0, 0, time.UTC)
	players := []map[string]any{
		{"playerId": "a1", "matchWins": 2, "cumulativeCardPoints": 50, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
		{"playerId": "a2", "matchWins": 1, "cumulativeCardPoints": 40, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
		{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 30, "finalGameCompletedAt": base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
		{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 20, "finalGameCompletedAt": base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
	}
	ingest := map[string]any{
		"eventId":           "evt-mc-1",
		"eventType":         "MatchCompleted",
		"schemaVersion":     1,
		"correlationId":     "corr-ingest",
		"occurredAt":        base.Format(time.RFC3339Nano),
		"roomId":            roomID,
		"tournamentId":      "tour-ingest",
		"roundNumber":       1,
		"slotId":            slotID,
		"completionVersion": 9,
		"isAbandoned":       false,
		"players":           players,
	}
	before := h.repo.OutboxLen()
	w := postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", w.Code, w.Body.String())
	}
	if h.repo.OutboxLen() <= before {
		t.Fatal("expected outbox growth after first ingest")
	}

	before = h.repo.OutboxLen()
	ingest["eventId"] = "evt-mc-1-dup"
	w = postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("dup ingest: %d %s", w.Code, w.Body.String())
	}
	if h.repo.OutboxLen() != before {
		t.Fatalf("duplicate ingest must not emit new facts")
	}

	conflictPlayers := []map[string]any{
		{"playerId": "a2", "matchWins": 2, "cumulativeCardPoints": 99, "finalGameCompletedAt": base.Format(time.RFC3339Nano)},
		{"playerId": "a1", "matchWins": 1, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Add(time.Minute).Format(time.RFC3339Nano)},
		{"playerId": "a3", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Add(2 * time.Minute).Format(time.RFC3339Nano)},
		{"playerId": "a4", "matchWins": 0, "cumulativeCardPoints": 1, "finalGameCompletedAt": base.Add(3 * time.Minute).Format(time.RFC3339Nano)},
	}
	ingest["eventId"] = "evt-mc-conflict"
	ingest["players"] = conflictPlayers
	w = postJSON(t, mux, "/internal/v1/tournaments/tour-ingest/match-results", testCred, ingest, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("conflict ingest: %d %s", w.Code, w.Body.String())
	}
	var conflictRes map[string]any
	_ = json.NewDecoder(w.Body).Decode(&conflictRes)
	if conflictRes["disposition"] != "quarantined" {
		found := false
		for _, e := range h.repo.Events() {
			if e.EventType == "TournamentResultQuarantined" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected quarantine disposition, body=%v", conflictRes)
		}
	}
}

func setupFinalFour(t *testing.T, h testHarness, mux http.Handler, corr map[string]string) {
	t.Helper()
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("ing-create", "CreateTournament", map[string]any{
		"tournamentId": "tour-ingest",
		"capacity":     4,
	}, "op", "s"), corr)
	for _, p := range []string{"a1", "a2", "a3", "a4"} {
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("ing-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "tour-ingest",
			"playerId":     p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("ing-close", "CloseRegistration", map[string]any{
		"tournamentId": "tour-ingest",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("ing-seed", "SeedRound", map[string]any{
		"tournamentId": "tour-ingest",
		"roundNumber":  1,
	}, "op", "s"), corr)
	w := postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("ing-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "tour-ingest",
		"roundNumber":  1,
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("setup provision failed: %s", w.Body.String())
	}
	provisionAllBatches(t, h, mux, "tour-ingest", 1, corr)
}

func TestPublicCreateAndRegisterRoutes(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-public"}

	w := postJSON(t, mux, "/v1/tournaments", testCred, map[string]any{
		"commandId":     "pub-create",
		"schemaVersion": 1,
		"tournamentId":  "pub-t1",
		"capacity":      8,
		"playerId":      "op",
		"sessionId":     "s",
	}, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /v1/tournaments: %d %s", w.Code, w.Body.String())
	}

	w = postJSON(t, mux, "/v1/tournaments/pub-t1/registrations", testCred, map[string]any{
		"commandId":     "pub-reg",
		"schemaVersion": 1,
		"playerId":      "player-x",
		"sessionId":     "sx",
	}, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("registrations: %d %s", w.Code, w.Body.String())
	}

	w = postJSON(t, mux, "/v1/tournaments/pub-t1/commands", testCred, commandBody("pub-close", "CloseRegistration", map[string]any{
		"tournamentId": "pub-t1",
	}, "op", "s"), corr)
	if decodeResult(t, w).Status != envelope.StatusAccepted {
		t.Fatalf("scoped commands: %s", w.Body.String())
	}
}

func TestProvisioningBatchRoute(t *testing.T) {
	h := newTestHarness(t)
	mux := h.srv.Routes()
	corr := map[string]string{"X-Correlation-Id": "corr-batch"}

	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("b-create", "CreateTournament", map[string]any{
		"tournamentId": "t-batch",
		"capacity":     4,
	}, "op", "s"), corr)
	for _, p := range []string{"b1", "b2", "b3", "b4"} {
		postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("b-reg-"+p, "RegisterPlayer", map[string]any{
			"tournamentId": "t-batch",
			"playerId":     p,
		}, p, "s"), corr)
	}
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("b-close", "CloseRegistration", map[string]any{
		"tournamentId": "t-batch",
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("b-seed", "SeedRound", map[string]any{
		"tournamentId": "t-batch",
		"roundNumber":  1,
	}, "op", "s"), corr)
	postJSON(t, mux, "/internal/v1/commands", testCred, commandBody("b-prov", "ProvisionRoundMatches", map[string]any{
		"tournamentId": "t-batch",
		"roundNumber":  1,
	}, "op", "s"), corr)

	// Missing batch bounds rejected.
	w := postJSON(t, mux, "/internal/v1/tournaments/t-batch/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":     "b-prov-batch-bad",
		"schemaVersion": 1,
	}, corr)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("expected reject without batch bounds, got %+v", res)
	}

	bracket := getJSON(t, mux, "/v1/tournaments/t-batch/bracket")
	var body map[string]any
	_ = json.NewDecoder(bracket.Body).Decode(&body)
	b0 := body["rounds"].([]any)[0].(map[string]any)["batches"].([]any)[0].(map[string]any)

	before := h.rooms.CallCount()
	w = postJSON(t, mux, "/internal/v1/tournaments/t-batch/rounds/1/provisioning-batches", testCred, map[string]any{
		"commandId":     "b-prov-batch",
		"schemaVersion": 1,
		"batchId":       b0["batchId"],
		"slotFrom":      b0["slotFrom"],
		"slotTo":        b0["slotTo"],
		"slotSize":      b0["slotSize"],
	}, corr)
	if w.Code != http.StatusOK {
		t.Fatalf("provisioning-batches: %d %s", w.Code, w.Body.String())
	}
	if h.rooms.CallCount() <= before {
		t.Fatal("expected provisioning calls from batch worker")
	}
}
