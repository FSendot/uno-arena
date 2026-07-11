package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"unoarena/services/game-integrity/domain"
)

const (
	testRoomCredential  = "test-gi-room-credential"
	testAuditCredential = "test-gi-audit-credential"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return NewServer(NewMemoryStreamRepository(), testRoomCredential, testAuditCredential, "offline", "")
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
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
	h.ServeHTTP(w, req)
	return w
}

func withRoomCred(extra map[string]string) map[string]string {
	h := map[string]string{internalCredentialHeader: testRoomCredential}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

func withAuditCred(extra map[string]string) map[string]string {
	h := map[string]string{internalCredentialHeader: testAuditCredential}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	return out
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
		t.Fatalf("ready: %d body=%s", rw.Code, rw.Body.String())
	}
	ready := decodeJSON(t, rw)
	if ready["status"] != "ready" || ready["mode"] != "offline" {
		t.Fatalf("ready: %+v", ready)
	}
}

func TestReadyUnwiredAndEventStoreBlocked(t *testing.T) {
	unwired := NewServer(nil, testRoomCredential, testAuditCredential, "", "repository_unwired")
	w := httptest.NewRecorder()
	unwired.routes().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable || decodeJSON(t, w)["reason"] != "repository_unwired" {
		t.Fatalf("unwired: %d %s", w.Code, w.Body.String())
	}

	blocked := NewServer(nil, testRoomCredential, testAuditCredential, "", "eventstore_adapter_blocked")
	w2 := httptest.NewRecorder()
	blocked.routes().ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w2.Code != http.StatusServiceUnavailable || decodeJSON(t, w2)["reason"] != "eventstore_adapter_blocked" {
		t.Fatalf("blocked: %d %s", w2.Code, w2.Body.String())
	}
}

func TestResolveRuntimeMemoryGateAndEventStore(t *testing.T) {
	t.Setenv("EVENTSTORE_URL", "")
	t.Setenv("GAME_INTEGRITY_ALLOW_MEMORY", "")
	repo, mode, reason := resolveRuntime()
	if repo != nil || mode != "" || reason != "memory_not_allowed" {
		t.Fatalf("default: repo=%v mode=%q reason=%q", repo != nil, mode, reason)
	}

	t.Setenv("GAME_INTEGRITY_ALLOW_MEMORY", "true")
	repo, mode, reason = resolveRuntime()
	if repo == nil || mode != "offline" || reason != "" {
		t.Fatalf("memory: repo=%v mode=%q reason=%q", repo != nil, mode, reason)
	}

	t.Setenv("EVENTSTORE_URL", "esdb://localhost:2113")
	repo, mode, reason = resolveRuntime()
	if repo != nil || reason != "eventstore_adapter_blocked" {
		t.Fatalf("eventstore: repo=%v mode=%q reason=%q", repo != nil, mode, reason)
	}
}

func TestAuthScopesRoomVsAudit(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()

	// Room credential cannot access audit/replay.
	w := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/room-1/replay?from=0", nil, withRoomCred(nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("room on replay: %d", w.Code)
	}
	w = doJSON(t, mux, http.MethodGet, "/internal/v1/audit/exports/g1", nil, withRoomCred(nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("room on export: %d", w.Code)
	}

	// Audit credential cannot append/deck.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/room-1/append", map[string]any{
		"eventId": "e1", "expectedRevision": 0, "eventType": "CreateRoom", "payload": map[string]any{},
	}, withAuditCred(nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("audit on append: %d", w.Code)
	}
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/room-1/deck-operations", map[string]any{
		"operation": "initialize", "gameId": "g1",
	}, withAuditCred(nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("audit on deck: %d", w.Code)
	}

	// Missing credential fails closed.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/room-1/append", map[string]any{
		"eventId": "e1", "expectedRevision": 0, "eventType": "CreateRoom",
	}, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no cred: %d", w.Code)
	}
}

func TestRejectedAppendNoConsume(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-rej/append"

	ok := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"eventId": "life-1", "expectedRevision": 0, "eventType": "CreateRoom",
		"payload": map[string]any{"host": "h"},
	}, withRoomCred(nil))
	if ok.Code != http.StatusOK || decodeJSON(t, ok)["revision"] != float64(1) {
		t.Fatalf("create: %d %s", ok.Code, ok.Body.String())
	}

	for _, badType := range []string{"CommandRejected", "RejectionRecord", "AuditRecord", "SomethingReject"} {
		w := doJSON(t, mux, http.MethodPost, path, map[string]any{
			"eventId": "bad-" + badType, "expectedRevision": 1, "eventType": badType,
			"payload": map[string]any{"reason": "x"},
		}, withRoomCred(nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: want 400 got %d %s", badType, w.Code, w.Body.String())
		}
		body := decodeJSON(t, w)
		if body["code"] != "invalid_command" {
			t.Fatalf("%s code: %+v", badType, body)
		}
	}

	// Revision unchanged — rejected types did not consume.
	next := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"eventId": "life-2", "expectedRevision": 1, "eventType": "JoinRoom",
		"payload": map[string]any{"player": "p2"},
	}, withRoomCred(nil))
	if next.Code != http.StatusOK || decodeJSON(t, next)["revision"] != float64(2) {
		t.Fatalf("join after rejects: %d %s", next.Code, next.Body.String())
	}
}

func TestLifecycleEmptyGameIDAndCumulativeRoomRevision(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-bo3/append"

	steps := []struct {
		eventID, eventType, gameID string
		rev                        int64
	}{
		{"c1", "CreateRoom", "", 0},
		{"c2", "JoinRoom", "", 1},
		{"c3", "LockRoom", "", 2},
		{"c4", "StartMatch", "game-1", 3},
		{"c5", "PlayCard", "game-1", 4},
		{"c6", "StartNextGame", "game-2", 5},
		{"c7", "PlayCard", "game-2", 6},
	}
	for _, step := range steps {
		body := map[string]any{
			"eventId": step.eventID, "expectedRevision": step.rev, "eventType": step.eventType,
			"payload": map[string]any{"n": step.rev},
		}
		if step.gameID != "" {
			body["gameId"] = step.gameID
		}
		w := doJSON(t, mux, http.MethodPost, path, body, withRoomCred(nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", step.eventID, w.Code, w.Body.String())
		}
		got := decodeJSON(t, w)
		if got["revision"] != float64(step.rev+1) {
			t.Fatalf("%s revision: %+v", step.eventID, got)
		}
	}

	replay := doJSON(t, mux, http.MethodGet, "/internal/v1/game-logs/room-bo3/replay?from=0", nil, withAuditCred(nil))
	if replay.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body.String())
	}
	rb := decodeJSON(t, replay)
	if rb["revision"] != float64(7) {
		t.Fatalf("cumulative revision: %+v", rb)
	}
	entries := rb["entries"].([]any)
	if len(entries) != 7 {
		t.Fatalf("entries=%d", len(entries))
	}
	e0 := entries[0].(map[string]any)
	if e0["eventType"] != "CreateRoom" {
		t.Fatalf("first: %+v", e0)
	}
	if _, hasGame := e0["gameId"]; hasGame && e0["gameId"] != "" {
		t.Fatalf("lifecycle entry must omit empty gameId: %+v", e0)
	}
	e3 := entries[3].(map[string]any)
	if e3["gameId"] != "game-1" {
		t.Fatalf("start match gameId: %+v", e3)
	}
}

func TestInitializeRejectsCallerSeedAndConflictingReinit(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-deck/deck-operations"

	bad := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "initialize", "gameId": "g1", "seed": "attacker-seed",
	}, withRoomCred(nil))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("seed supply: %d %s", bad.Code, bad.Body.String())
	}

	first := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "initialize", "gameId": "g1",
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("init: %d %s", first.Code, first.Body.String())
	}
	fb := decodeJSON(t, first)
	if fb["seedCommitment"] == nil || fb["seedCommitment"] == "" {
		t.Fatalf("missing commitment: %+v", fb)
	}
	if _, hasSeed := fb["seed"]; hasSeed {
		t.Fatal("room initialize must not return raw seed")
	}

	re := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"operation": "initialize", "gameId": "g1",
	}, withRoomCred(nil))
	if re.Code != http.StatusConflict || decodeJSON(t, re)["code"] != "conflicting_duplicate" {
		t.Fatalf("reinit: %d %s", re.Code, re.Body.String())
	}
}

func TestReservationNonConsumingConfirmCancelAndConflicts(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	deckPath := "/internal/v1/game-logs/room-res/deck-operations"

	init := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "initialize", "gameId": "g-res",
	}, withRoomCred(nil))
	if init.Code != http.StatusOK {
		t.Fatalf("init: %d %s", init.Code, init.Body.String())
	}

	reserve := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_deal", "gameId": "g-res", "operationId": "deal-op-1",
		"seats": []string{"a", "b"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if reserve.Code != http.StatusOK {
		t.Fatalf("reserve: %d %s", reserve.Code, reserve.Body.String())
	}
	rb := decodeJSON(t, reserve)
	resID := rb["reservationId"].(string)
	if rb["kind"] != "accepted" || resID == "" {
		t.Fatalf("reserve body: %+v", rb)
	}

	// Rejected conflicting shape does not consume — cancel then re-reserve same cards.
	conflict := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_deal", "gameId": "g-res", "operationId": "deal-op-1",
		"seats": []string{"a", "b", "c"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict: %d %s", conflict.Code, conflict.Body.String())
	}

	dup := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_deal", "gameId": "g-res", "operationId": "deal-op-1",
		"seats": []string{"a", "b"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if dup.Code != http.StatusOK || decodeJSON(t, dup)["kind"] != "duplicate" {
		t.Fatalf("dup: %d %s", dup.Code, dup.Body.String())
	}

	// Cancel releases without consume.
	cancel := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "cancel", "gameId": "g-res", "reservationId": resID,
	}, withRoomCred(nil))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", cancel.Code, cancel.Body.String())
	}

	again := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_deal", "gameId": "g-res", "operationId": "deal-op-2",
		"seats": []string{"a", "b"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	if again.Code != http.StatusOK {
		t.Fatalf("re-reserve: %d %s", again.Code, again.Body.String())
	}
	againID := decodeJSON(t, again)["reservationId"].(string)

	confirm := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "confirm", "gameId": "g-res", "reservationId": againID,
	}, withRoomCred(nil))
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", confirm.Code, confirm.Body.String())
	}

	// Draw reserve + confirm.
	draw := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_draw", "gameId": "g-res", "operationId": "draw-1", "count": 2,
	}, withRoomCred(nil))
	if draw.Code != http.StatusOK {
		t.Fatalf("draw reserve: %d %s", draw.Code, draw.Body.String())
	}
	drawBody := decodeJSON(t, draw)
	cards := drawBody["cards"].([]any)
	if len(cards) != 2 {
		t.Fatalf("cards: %+v", drawBody)
	}
	drawID := drawBody["reservationId"].(string)
	drawConfirm := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "confirm", "gameId": "g-res", "reservationId": drawID,
	}, withRoomCred(nil))
	if drawConfirm.Code != http.StatusOK {
		t.Fatalf("draw confirm: %d %s", drawConfirm.Code, drawConfirm.Body.String())
	}

	// Conflicting draw shape.
	badDraw := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_draw", "gameId": "g-res", "operationId": "draw-1", "count": 3,
	}, withRoomCred(nil))
	if badDraw.Code != http.StatusConflict {
		t.Fatalf("draw conflict: %d %s", badDraw.Code, badDraw.Body.String())
	}
}

func TestReservationConcurrencySerialized(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	deckPath := "/internal/v1/game-logs/room-race-deck/deck-operations"
	init := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "initialize", "gameId": "g-race",
	}, withRoomCred(nil))
	if init.Code != http.StatusOK {
		t.Fatalf("init: %d", init.Code)
	}

	const n = 8
	var wg sync.WaitGroup
	codes := make([]int, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			w := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
				"operation": "reserve_draw", "gameId": "g-race",
				"operationId": "same-op", "count": 1,
			}, withRoomCred(nil))
			codes[i] = w.Code
		}()
	}
	wg.Wait()
	var okCount, conflictCount int
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Fatalf("status %d", c)
		}
	}
	// One accepted + duplicates (200) or conflicts — all must be 200 (dup) after first.
	if okCount != n {
		t.Fatalf("ok=%d conflict=%d (expect all 200 with one accepted + dups)", okCount, conflictCount)
	}
}

func TestReplayReconstructionFromAuditExport(t *testing.T) {
	t.Setenv("GAME_INTEGRITY_EXPOSE_SEED", "true")
	srv := NewServer(NewMemoryStreamRepository(), testRoomCredential, testAuditCredential, "offline", "")
	srv.exposeSeedInAudit = true
	mux := srv.routes()

	deckPath := "/internal/v1/game-logs/room-audit/deck-operations"
	init := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "initialize", "gameId": "g-audit",
	}, withRoomCred(nil))
	if init.Code != http.StatusOK {
		t.Fatalf("init: %d %s", init.Code, init.Body.String())
	}
	commit := decodeJSON(t, init)["seedCommitment"].(string)

	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/room-audit/append", map[string]any{
		"eventId": "a1", "expectedRevision": 0, "eventType": "CreateRoom", "payload": map[string]any{},
	}, withRoomCred(nil))
	_ = doJSON(t, mux, http.MethodPost, "/internal/v1/game-logs/room-audit/append", map[string]any{
		"gameId": "g-audit", "eventId": "a2", "expectedRevision": 1, "eventType": "StartMatch",
		"payload": map[string]any{},
	}, withRoomCred(nil))

	reserve := doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "reserve_deal", "gameId": "g-audit", "operationId": "deal-a",
		"seats": []string{"p1", "p2"}, "cardsPerHand": 7,
	}, withRoomCred(nil))
	resID := decodeJSON(t, reserve)["reservationId"].(string)
	_ = doJSON(t, mux, http.MethodPost, deckPath, map[string]any{
		"operation": "confirm", "gameId": "g-audit", "reservationId": resID,
	}, withRoomCred(nil))

	export := doJSON(t, mux, http.MethodGet, "/internal/v1/audit/exports/g-audit?roomId=room-audit", nil, withAuditCred(nil))
	if export.Code != http.StatusOK {
		t.Fatalf("export: %d %s", export.Code, export.Body.String())
	}
	eb := decodeJSON(t, export)
	if eb["revision"] != float64(2) {
		t.Fatalf("export revision: %+v", eb)
	}
	deck, ok := eb["deck"].(map[string]any)
	if !ok {
		t.Fatalf("missing deck: %+v", eb)
	}
	if deck["seedCommitment"] != commit {
		t.Fatalf("commitment mismatch %v vs %v", deck["seedCommitment"], commit)
	}
	seedHex, _ := deck["protectedSeed"].(string)
	if seedHex == "" {
		t.Fatal("operator export missing protectedSeed")
	}
	seedBytes, err := hex.DecodeString(seedHex)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := domain.NewDeckSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	order := domain.ShuffleCards(seed, StandardDeckCards())
	if orderCommitment(order) != deck["orderCommitment"] {
		t.Fatal("order commitment does not reconstruct from protected seed")
	}
	if deck["pointer"] != float64(15) { // 7+7+1
		t.Fatalf("pointer after deal confirm: %+v", deck["pointer"])
	}
	ops := deck["confirmedOperations"].([]any)
	if len(ops) != 1 {
		t.Fatalf("ops: %+v", ops)
	}
}

func TestAppendExpectedRevisionIdempotencyAndConflict(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-1/append"
	payload := map[string]any{"commandId": "c1", "type": "PlayCard"}

	first := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"gameId": "g1", "eventId": "evt-1", "expectedRevision": 0,
		"eventType": "PlayCard", "payload": payload,
	}, withRoomCred(nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}

	stale := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"gameId": "g1", "eventId": "evt-2", "expectedRevision": 0,
		"eventType": "PlayCard", "payload": map[string]any{"n": 2},
	}, withRoomCred(nil))
	if stale.Code != http.StatusConflict || decodeJSON(t, stale)["code"] != "revision_mismatch" {
		t.Fatalf("stale: %d %s", stale.Code, stale.Body.String())
	}

	dup := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"gameId": "g1", "eventId": "evt-1", "expectedRevision": 99,
		"eventType": "PlayCard", "payload": payload,
	}, withRoomCred(nil))
	if dup.Code != http.StatusOK || decodeJSON(t, dup)["kind"] != "duplicate" {
		t.Fatalf("dup: %d %s", dup.Code, dup.Body.String())
	}

	conflict := doJSON(t, mux, http.MethodPost, path, map[string]any{
		"gameId": "g1", "eventId": "evt-1", "expectedRevision": 1,
		"eventType": "PlayCard", "payload": map[string]any{"other": true},
	}, withRoomCred(nil))
	if conflict.Code != http.StatusConflict || decodeJSON(t, conflict)["code"] != "conflicting_duplicate" {
		t.Fatalf("conflict: %d %s", conflict.Code, conflict.Body.String())
	}
}

func TestAppendConcurrentExpectedRevisionSerialized(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	path := "/internal/v1/game-logs/room-race/append"

	const n = 16
	var wg sync.WaitGroup
	codes := make([]int, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			w := doJSON(t, mux, http.MethodPost, path, map[string]any{
				"gameId": "g-race", "eventId": "race-" + itoa(i), "expectedRevision": 0,
				"eventType": "PlayCard", "payload": map[string]any{"i": i},
			}, withRoomCred(nil))
			codes[i] = w.Code
		}()
	}
	wg.Wait()

	var okCount, conflictCount int
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Fatalf("unexpected status %d", c)
		}
	}
	if okCount != 1 || conflictCount != n-1 {
		t.Fatalf("ok=%d conflict=%d want 1/%d", okCount, conflictCount, n-1)
	}
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
