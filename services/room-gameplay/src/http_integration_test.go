package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/shared/envelope"
)

const testCred = "room-test-credential"

type testEnv struct {
	srv       *Server
	mux       http.Handler
	clock     *app.FixedClock
	integrity *app.FakeGameIntegrity
	audit     *app.FakeAuditSink
	publisher *app.FakeEventPublisher
	sessions  *app.MemorySessionRepository
	deals     *app.FakeDealSource
	sessionsV *app.FakeSessionValidator
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	clock := app.NewFixedClock(time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC))
	integrity := app.NewFakeGameIntegrity()
	audit := app.NewFakeAuditSink()
	publisher := app.NewFakeEventPublisher()
	sessions := app.NewMemorySessionRepository()
	deals := app.NewFakeDealSource()
	sessionsV := app.NewFakeSessionValidator()
	// Allow common test sessions by default.
	sessionsV.Allow("sess-h", "host")
	sessionsV.Allow("sess-g", "guest")
	sessionsV.Allow("s", "host")
	sessionsV.Allow("s2", "guest")
	sessionsV.Allow("s1", "host")
	sessionsV.Allow("timer", "guest")
	sessionsV.Allow("timer", "host")
	svc := app.NewService(app.ServiceDeps{
		Sessions:  sessions,
		Integrity: integrity,
		Publisher: publisher,
		Audit:     audit,
		Deals:     deals,
		Clock:     clock,
		SessionsV: sessionsV,
	})
	srv := NewServerWithTimerCred(svc, testCred, testTimerCred, "room-gameplay")
	return &testEnv{
		srv: srv, mux: srv.routes(), clock: clock,
		integrity: integrity, audit: audit, publisher: publisher,
		sessions: sessions, deals: deals, sessionsV: sessionsV,
	}
}

const testTimerCred = "room-timer-credential"

func (e *testEnv) do(t *testing.T, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	return w
}

func (e *testEnv) auth() map[string]string {
	return map[string]string{
		"X-Service-Credential": testCred,
		"X-Correlation-Id":     "corr-test",
	}
}

func (e *testEnv) timerAuth() map[string]string {
	return map[string]string{
		"X-Service-Credential": testTimerCred,
		"X-Correlation-Id":     "corr-timer",
	}
}

func decodeResult(t *testing.T, w *httptest.ResponseRecorder) envelope.Result {
	t.Helper()
	var res envelope.Result
	if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
		t.Fatalf("decode result: %v body=%s", err, w.Body.String())
	}
	return res
}

func cmdBody(commandID, typ string, seq *int64, playerID, sessionID, roomID string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	body := map[string]any{
		"commandId":     commandID,
		"type":          typ,
		"schemaVersion": 1,
		"payload":       payload,
		"playerId":      playerID,
		"sessionId":     sessionID,
		"roomId":        roomID,
	}
	if seq != nil {
		body["expectedSequenceNumber"] = *seq
	}
	return body
}

func seq(n int64) *int64 { return &n }

func TestHealthHandler(t *testing.T) {
	e := newTestEnv(t)
	w := e.do(t, http.MethodGet, "/health", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]string
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" || resp["service"] != "room-gameplay" {
		t.Fatalf("unexpected health: %+v", resp)
	}
}

func TestInternalCommands_RequiresCredential(t *testing.T) {
	e := newTestEnv(t)
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c1", "CreateRoom", nil, "host", "s1", "room_1", map[string]any{
		"roomId": "room_1",
	}), map[string]string{"X-Correlation-Id": "c"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestInternalCommands_StrictSchemaVersion(t *testing.T) {
	e := newTestEnv(t)
	body := map[string]any{
		"commandId": "c1", "type": "CreateRoom", "payload": map[string]any{},
		"playerId": "host", "sessionId": "s1", "roomId": "room_1",
	}
	w := e.do(t, http.MethodPost, "/internal/v1/commands", body, e.auth())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without schemaVersion, got %d", w.Code)
	}
	body["schemaVersion"] = 2
	w = e.do(t, http.MethodPost, "/internal/v1/commands", body, e.auth())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for schemaVersion 2, got %d", w.Code)
	}
}

func TestCreateJoinLockStartPlayCard_HappyPath(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("create-1", "CreateRoom", nil, "host", "sess-h", "room_a", map[string]any{
		"roomId": "room_a", "visibility": "public",
	}), h)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted || res.Sequence == nil || *res.Sequence != 1 {
		t.Fatalf("create result=%+v", res)
	}
	if e.integrity.Len() != 1 {
		t.Fatalf("GI appends=%d want 1", e.integrity.Len())
	}
	if e.audit.Len() != 0 {
		t.Fatalf("audit on accept: %d", e.audit.Len())
	}
	if len(e.publisher.Events) == 0 {
		t.Fatal("expected published feed events")
	}

	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("join-1", "JoinRoom", seq(1), "guest", "sess-g", "room_a", map[string]any{}), h)
	res = decodeResult(t, w)
	if res.Status != envelope.StatusAccepted || *res.Sequence != 2 {
		t.Fatalf("join=%+v", res)
	}

	w = e.do(t, http.MethodPost, "/v1/rooms/room_a/commands", cmdBody("lock-1", "LockRoom", seq(2), "host", "sess-h", "room_a", map[string]any{}), h)
	res = decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("lock=%+v", res)
	}

	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("start-1", "StartMatch", seq(3), "host", "sess-h", "room_a", map[string]any{
		"gameId": "g1",
	}), h)
	res = decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("start=%+v body=%s", res, w.Body.String())
	}
	startSeq := *res.Sequence

	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("play-1", "PlayCard", seq(startSeq), "host", "sess-h", "room_a", map[string]any{
		"cardId": "host-c1",
	}), h)
	res = decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("play=%+v", res)
	}

	// Snapshot includes private hand for host.
	w = e.do(t, http.MethodGet, "/v1/rooms/room_a/snapshot?playerId=host", nil, h)
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}
	var snap map[string]any
	_ = json.NewDecoder(w.Body).Decode(&snap)
	if snap["hand"] == nil {
		t.Fatalf("expected private hand in snapshot: %+v", snap)
	}

	// Spectator-safe events must not include CardDrawn private facts from play path;
	// verify published spectator payloads lack forbidden keys.
	for _, ev := range e.publisher.Events {
		if ev.Topic != app.TopicSpectatorSafe {
			continue
		}
		s := string(ev.Payload)
		if bytes.Contains(ev.Payload, []byte(`"privateHand"`)) || bytes.Contains([]byte(s), []byte(`"deck"`)) {
			t.Fatalf("spectator payload leaked private fields: %s", s)
		}
	}
}

func TestRejection_AuditOnly_NoGI(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("create-1", "CreateRoom", nil, "host", "s", "room_b", map[string]any{
		"roomId": "room_b",
	}), h)
	giBefore := e.integrity.Len()

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("join-stale", "JoinRoom", seq(99), "guest", "s2", "room_b", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("expected rejected, got %+v", res)
	}
	if e.integrity.Len() != giBefore {
		t.Fatalf("GI must not append on reject: before=%d after=%d", giBefore, e.integrity.Len())
	}
	if e.audit.Len() != 1 {
		t.Fatalf("audit records=%d want 1", e.audit.Len())
	}
	rec := e.audit.Records[0]
	if rec.CommandID != "join-stale" || rec.Reason == "" || rec.RoomID != "room_b" {
		t.Fatalf("audit=%+v", rec)
	}
}

func TestIdempotentCommandId(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	body := cmdBody("create-idem", "CreateRoom", nil, "host", "s", "room_c", map[string]any{"roomId": "room_c"})
	w1 := e.do(t, http.MethodPost, "/internal/v1/commands", body, h)
	w2 := e.do(t, http.MethodPost, "/internal/v1/commands", body, h)
	r1, r2 := decodeResult(t, w1), decodeResult(t, w2)
	if r1.Status != envelope.StatusAccepted || r2.Status != envelope.StatusAccepted {
		t.Fatalf("r1=%+v r2=%+v", r1, r2)
	}
	if e.integrity.Len() != 1 {
		t.Fatalf("idempotent create must append GI once, got %d", e.integrity.Len())
	}

	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("join-idem", "JoinRoom", seq(1), "guest", "s2", "room_c", map[string]any{}), h)
	gi := e.integrity.Len()
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("join-idem", "JoinRoom", seq(1), "guest", "s2", "room_c", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("dup join=%+v", res)
	}
	if e.integrity.Len() != gi {
		t.Fatalf("duplicate must not re-append GI")
	}
}

func TestGIAppendFailure_LeavesStateUnchanged(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("create-1", "CreateRoom", nil, "host", "s", "room_d", map[string]any{
		"roomId": "room_d",
	}), h)
	e.integrity.FailNext = errGIBoom

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("join-fail", "JoinRoom", seq(1), "guest", "s2", "room_d", map[string]any{}), h)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 on GI failure, got %d %s", w.Code, w.Body.String())
	}

	// Retry join should still see sequence 1 (unchanged).
	e.integrity.FailNext = nil
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("join-ok", "JoinRoom", seq(1), "guest", "s2", "room_d", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted || res.Sequence == nil || *res.Sequence != 2 {
		t.Fatalf("after GI recovery join=%+v", res)
	}
}

var errGIBoom = errString("gi boom")

type errString string

func (e errString) Error() string { return string(e) }

func TestProvision_IdempotentByTournamentRoundSlot(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	body := map[string]any{
		"commandId": "prov-1", "tournamentId": "t1", "roundNumber": 1, "slotId": "slot-9",
		"roomId": "room_t1", "hostId": "p1", "playerIds": []string{"p1", "p2"},
		"visibility": "private", "maxSeats": 4,
	}
	w1 := e.do(t, http.MethodPost, "/internal/v1/rooms/provision", body, h)
	if w1.Code != http.StatusOK {
		t.Fatalf("provision: %d %s", w1.Code, w1.Body.String())
	}
	r1 := decodeResult(t, w1)
	if r1.Status != envelope.StatusAccepted {
		t.Fatalf("%+v", r1)
	}
	gi := e.integrity.Len()

	body["commandId"] = "prov-2" // different command id, same provision key
	w2 := e.do(t, http.MethodPost, "/internal/v1/rooms/provision", body, h)
	r2 := decodeResult(t, w2)
	if r2.Status != envelope.StatusAccepted {
		t.Fatalf("idempotent provision=%+v", r2)
	}
	if e.integrity.Len() != gi {
		t.Fatalf("provision key must not re-append GI")
	}
}

func TestTimerCommands_StaleUnoAndReconnectIdempotency(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()

	// Build in-progress room.
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_timer", map[string]any{"roomId": "room_timer"}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_timer", map[string]any{}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("l", "LockRoom", seq(2), "host", "s", "room_timer", map[string]any{}), h)
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("st", "StartMatch", seq(3), "host", "s", "room_timer", map[string]any{"gameId": "g1"}), h)
	start := decodeResult(t, w)
	if start.Status != envelope.StatusAccepted {
		t.Fatalf("start=%+v", start)
	}
	cur := *start.Sequence

	// Play card to open Uno window (host has 2 cards; playing one leaves 1).
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("play", "PlayCard", seq(cur), "host", "s", "room_timer", map[string]any{
		"cardId": "host-c1",
	}), h)
	play := decodeResult(t, w)
	if play.Status != envelope.StatusAccepted {
		t.Fatalf("play=%+v", play)
	}
	cur = *play.Sequence

	// Stale ExpireUnoWindow (wrong opening sequence) -> reject + audit, no GI bump beyond current.
	giBefore := e.integrity.Len()
	w = e.do(t, http.MethodPost, "/internal/v1/rooms/room_timer/timer-commands", cmdBody("exp-stale", "ExpireUnoWindow", seq(cur), "", "timer", "room_timer", map[string]any{
		"playerId": "host", "gameId": "g1", "triggeringGameEventId": "play", "openingSequence": 1,
	}), e.timerAuth())
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("stale uno expiry should reject: %+v", res)
	}
	if e.integrity.Len() != giBefore {
		t.Fatal("stale uno must not append GI")
	}
	if e.audit.Len() < 1 {
		t.Fatal("expected audit for stale uno")
	}

	// Disconnect guest and forfeit after deadline; duplicate forfeit is idempotent.
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("disc", "DisconnectPlayer", seq(cur), "guest", "s2", "room_timer", map[string]any{}), h)
	disc := decodeResult(t, w)
	if disc.Status != envelope.StatusAccepted {
		t.Fatalf("disconnect=%+v", disc)
	}
	cur = *disc.Sequence

	snapW := e.do(t, http.MethodGet, "/v1/rooms/room_timer/snapshot?playerId=guest", nil, h)
	var snap map[string]any
	_ = json.NewDecoder(snapW.Body).Decode(&snap)
	discInfo, _ := snap["disconnect"].(map[string]any)
	ver := int64(discInfo["disconnectVersion"].(float64))

	e.clock.Advance(60 * time.Second)
	forfeitBody := cmdBody("forfeit-1", "ForfeitPlayer", seq(cur), "guest", "timer", "room_timer", map[string]any{
		"playerId": "guest", "disconnectVersion": ver,
	})
	w = e.do(t, http.MethodPost, "/internal/v1/rooms/room_timer/timer-commands", forfeitBody, e.timerAuth())
	ff := decodeResult(t, w)
	if ff.Status != envelope.StatusAccepted {
		t.Fatalf("forfeit=%+v", ff)
	}
	giAfter := e.integrity.Len()

	// Idempotent reconnect/forfeit: same forfeit commandId returns prior outcome without new GI.
	w = e.do(t, http.MethodPost, "/internal/v1/rooms/room_timer/timer-commands", forfeitBody, e.timerAuth())
	ff2 := decodeResult(t, w)
	if ff2.Status != envelope.StatusAccepted {
		t.Fatalf("dup forfeit=%+v", ff2)
	}
	if e.integrity.Len() != giAfter {
		t.Fatal("duplicate forfeit must not re-append GI")
	}

	// Stale reconnect after forfeit rejects with audit only.
	giBefore = e.integrity.Len()
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("reconn", "ReconnectToRoom", seq(*ff.Sequence), "guest", "s2", "room_timer", map[string]any{
		"disconnectVersion": ver,
	}), h)
	rec := decodeResult(t, w)
	if rec.Status != envelope.StatusRejected {
		t.Fatalf("reconnect after forfeit should reject: %+v", rec)
	}
	if e.integrity.Len() != giBefore {
		t.Fatal("rejected reconnect must not append GI")
	}
}

func TestHTTPEventPublisher_PostsToGatewayStreamsOneEvent(t *testing.T) {
	var gotPath string
	var gotCred string
	var gotBody map[string]any
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		gotPath = r.URL.Path
		gotCred = r.Header.Get("X-Service-Credential")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	pub := app.NewHTTPEventPublisher(upstream.URL, "gw-cred", upstream.Client())
	err := pub.Publish(nil, app.PublishedEvent{
		Stream: app.StreamPlayer, RoomID: "r1", EventID: "e1",
		EventType: "RoomCreated", SequenceNumber: 1, SchemaVersion: 1,
		Payload: json.RawMessage(`{"roomId":"r1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/internal/v1/streams/events" {
		t.Fatalf("path=%s", gotPath)
	}
	if gotCred != "gw-cred" {
		t.Fatalf("cred=%s", gotCred)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	if gotBody["eventId"] != "e1" || gotBody["stream"] != "player" {
		t.Fatalf("body=%v", gotBody)
	}
	if _, ok := gotBody["events"]; ok {
		t.Fatal("must post one event per request, not a batch")
	}
}

func TestCancelRoom_DocumentedRoute(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/v1/rooms", cmdBody("c1", "CreateRoom", nil, "host", "s", "room_x", map[string]any{
		"roomId": "room_x",
	}), h)
	w := e.do(t, http.MethodPost, "/v1/rooms/room_x/commands", cmdBody("cancel", "CancelRoom", seq(1), "host", "s", "room_x", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("cancel=%+v", res)
	}
}
