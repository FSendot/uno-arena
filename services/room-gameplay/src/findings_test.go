package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/envelope"
)

func TestFinding_PublisherOneEventPerRequest_NeverIgnoresErrors(t *testing.T) {
	var posts atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		switch r.URL.Path {
		case "/internal/v1/streams/events", "/internal/v1/spectator/rooms/r1/events":
		default:
			t.Errorf("path=%s", r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if _, ok := body["events"]; ok {
			t.Error("batch events key must not be used")
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	pub := app.NewMultiDestinationPublisher(app.PublisherDestinations{
		GatewayURL: upstream.URL, GatewayCred: "cred",
		SpectatorURL: upstream.URL, SpectatorCred: "cred",
	}, upstream.Client())
	ev1 := app.PublishedEvent{
		Stream: app.StreamPlayer, RoomID: "r1", EventID: "e1", EventType: "X",
		SequenceNumber: 1, SchemaVersion: 1, PlayerID: "p1", SessionID: "s1",
		Payload: json.RawMessage(`{}`),
	}
	ev2 := app.PublishedEvent{
		Topic: app.TopicSpectatorSafe, Stream: app.StreamSpectator, RoomID: "r1", EventID: "e2", EventType: "Y",
		SequenceNumber: 2, SchemaVersion: 1, Payload: json.RawMessage(`{"visibility":"public"}`),
	}
	if err := pub.Publish(context.Background(), ev1); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(context.Background(), ev2); err != nil {
		t.Fatal(err)
	}
	if posts.Load() < 3 { // player→gw, spectator→gw+sv
		t.Fatalf("posts=%d want >=3", posts.Load())
	}

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(failing.Close)
	bad := app.NewMultiDestinationPublisher(app.PublisherDestinations{
		GatewayURL: failing.URL, GatewayCred: "cred",
	}, failing.Client())
	if err := bad.Publish(context.Background(), ev1); err == nil {
		t.Fatal("publish error must not be ignored")
	}
}

func TestFinding_RejectedDraw_NoMaterialNoGI(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_mat", map[string]any{"roomId": "room_mat"}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_mat", map[string]any{}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("l", "LockRoom", seq(2), "host", "s", "room_mat", map[string]any{}), h)
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("st", "StartMatch", seq(3), "host", "s", "room_mat", map[string]any{"gameId": "g1"}), h)
	start := decodeResult(t, w)
	if start.Status != envelope.StatusAccepted {
		t.Fatalf("start=%+v", start)
	}
	dealCalls := e.deals.DealCalls
	drawCalls := e.deals.DrawCalls
	gi := e.integrity.Len()

	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("draw-stale", "DrawCard", seq(99), "host", "s", "room_mat", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("expected reject %+v", res)
	}
	if e.deals.DealCalls != dealCalls || e.deals.DrawCalls != drawCalls {
		t.Fatalf("material touched: deal=%d/%d draw=%d/%d", e.deals.DealCalls, dealCalls, e.deals.DrawCalls, drawCalls)
	}
	if e.integrity.Len() != gi {
		t.Fatal("GI must not advance on rejected draw")
	}
	if e.deals.ConfirmedLen() != 1 {
		t.Fatalf("confirmed=%d", e.deals.ConfirmedLen())
	}
}

func TestFinding_RejectedStartMatch_NoDealMaterial(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_sm", map[string]any{"roomId": "room_sm"}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_sm", map[string]any{}), h)
	dealBefore := e.deals.DealCalls
	gi := e.integrity.Len()
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("st", "StartMatch", seq(2), "host", "s", "room_sm", map[string]any{"gameId": "g1"}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("expected reject %+v", res)
	}
	if e.deals.DealCalls != dealBefore {
		t.Fatal("deal material reserved before rejection checks")
	}
	if e.integrity.Len() != gi {
		t.Fatal("GI touched")
	}
}

func TestFinding_AtomicCommit_OutboxRetry_NoDirectIgnoredPublish(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	e.publisher.Fail = errors.New("publish down")

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_ob", map[string]any{"roomId": "room_ob"}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("create must accept despite publish failure: %+v", res)
	}
	if e.sessions.PendingOutboxLen() < 1 {
		t.Fatal("accepted command must leave pending outbox")
	}
	if len(e.publisher.Events) != 0 {
		t.Fatal("failed publish must not clear events as success")
	}

	e.publisher.Fail = nil
	n, err := e.srv.svc.DrainOutbox(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 || len(e.publisher.Events) == 0 {
		t.Fatalf("retry drain published=%d events=%d", n, len(e.publisher.Events))
	}
	if e.sessions.PendingOutboxLen() != 0 {
		t.Fatal("pending outbox should clear after successful drain")
	}
}

func TestFinding_CommitFailure_LeavesLiveConsistent(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_cf", map[string]any{"roomId": "room_cf"}), h)
	e.sessions.FailCommit = errors.New("commit boom")
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_cf", map[string]any{}), h)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d %s", w.Code, w.Body.String())
	}
	e.sessions.FailCommit = nil
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j2", "JoinRoom", seq(1), "guest", "s2", "room_cf", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted || res.Sequence == nil || *res.Sequence != 2 {
		t.Fatalf("after commit recovery %+v", res)
	}
}

func TestFinding_GameAndMatchCompleted_AsyncAPIPlusFeeds(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	wireQuickWinDeals(e)
	body := map[string]any{
		"commandId": "prov-mc", "tournamentId": "t-mc", "roundNumber": 3, "slotId": "slot-7",
		"roomId": "room_mc", "hostId": "host", "playerIds": []string{"host", "guest"},
		"visibility": "private", "maxSeats": 4,
	}
	w := e.do(t, http.MethodPost, "/internal/v1/rooms/provision", body, h)
	if w.Code != http.StatusOK {
		t.Fatalf("provision %d %s", w.Code, w.Body.String())
	}
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("l", "LockRoom", seq(2), "host", "s", "room_mc", map[string]any{}), h)
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("st", "StartMatch", seq(3), "host", "s", "room_mc", map[string]any{"gameId": "g1"}), h)
	start := decodeResult(t, w)
	if start.Status != envelope.StatusAccepted {
		t.Fatalf("start=%+v", start)
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("p1", "PlayCard", seq(*start.Sequence), "host", "s", "room_mc", map[string]any{"cardId": "host-w"}), h)
	g1 := decodeResult(t, w)
	if g1.Status != envelope.StatusAccepted {
		t.Fatalf("play1=%+v", g1)
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("n1", "StartNextGame", seq(*g1.Sequence), "host", "s", "room_mc", map[string]any{"gameId": "g2"}), h)
	n1 := decodeResult(t, w)
	if n1.Status != envelope.StatusAccepted {
		t.Fatalf("StartNextGame=%+v", n1)
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("p2", "PlayCard", seq(*n1.Sequence), "host", "s", "room_mc", map[string]any{"cardId": "host-w"}), h)
	done := decodeResult(t, w)
	if done.Status != envelope.StatusAccepted {
		t.Fatalf("play2=%+v", done)
	}

	var sawGame, sawMatch, sawPlayer, sawSpec bool
	for _, ev := range e.publisher.Events {
		switch ev.Topic {
		case app.TopicGameCompleted:
			sawGame = true
			var p map[string]any
			_ = json.Unmarshal(ev.Payload, &p)
			if p["eventType"] != "GameCompleted" || p["schemaVersion"].(float64) != 1 {
				t.Fatalf("game payload=%v", p)
			}
		case app.TopicMatchCompleted:
			sawMatch = true
			var p map[string]any
			_ = json.Unmarshal(ev.Payload, &p)
			if p["tournamentId"] != "t-mc" || int(p["roundNumber"].(float64)) != 3 || p["slotId"] != "slot-7" {
				t.Fatalf("match meta=%v", p)
			}
			if p["players"] == nil || p["isAbandoned"] == nil {
				t.Fatalf("match missing players/isAbandoned: %v", p)
			}
		}
		if ev.Stream == app.StreamPlayer {
			sawPlayer = true
		}
		if ev.Stream == app.StreamSpectator {
			sawSpec = true
		}
	}
	if !sawGame || !sawMatch {
		t.Fatalf("completion topics game=%v match=%v", sawGame, sawMatch)
	}
	if !sawPlayer || !sawSpec {
		t.Fatalf("feeds player=%v spectator=%v", sawPlayer, sawSpec)
	}
}

func TestFinding_StartNextGame_RuntimeMapping(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	wireQuickWinDeals(e)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_ng", map[string]any{"roomId": "room_ng"}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_ng", map[string]any{}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("l", "LockRoom", seq(2), "host", "s", "room_ng", map[string]any{}), h)
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("st", "StartMatch", seq(3), "host", "s", "room_ng", map[string]any{"gameId": "g1"}), h)
	start := decodeResult(t, w)
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("p1", "PlayCard", seq(*start.Sequence), "host", "s", "room_ng", map[string]any{"cardId": "host-w"}), h)
	g1 := decodeResult(t, w)
	if g1.Status != envelope.StatusAccepted {
		t.Fatalf("g1=%+v", g1)
	}
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("n1", "StartNextGame", seq(*g1.Sequence), "host", "s", "room_ng", map[string]any{"gameId": "g2"}), h)
	n1 := decodeResult(t, w)
	if n1.Status != envelope.StatusAccepted {
		t.Fatalf("StartNextGame not mapped: %+v", n1)
	}
}

func TestFinding_TimerAllowlist_DedicatedCred_ServerSideAsSystem(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_tm", map[string]any{"roomId": "room_tm"}), h)

	w := e.do(t, http.MethodPost, "/internal/v1/rooms/room_tm/timer-commands", cmdBody("x", "ExpireUnoWindow", seq(1), "host", "s", "room_tm", map[string]any{}), h)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("generic cred on timer: %d", w.Code)
	}

	w = e.do(t, http.MethodPost, "/internal/v1/rooms/room_tm/timer-commands", cmdBody("x2", "PlayCard", seq(1), "host", "s", "room_tm", map[string]any{
		"cardId": "x",
	}), e.timerAuth())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("allowlist: %d %s", w.Code, w.Body.String())
	}

	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_tm", map[string]any{}), h)
	body := cmdBody("lock-sys", "LockRoom", seq(2), "guest", "s2", "room_tm", map[string]any{})
	body["asSystem"] = true
	w = e.do(t, http.MethodPost, "/internal/v1/commands", body, h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("client asSystem must not elevate: %+v", res)
	}
}

func TestFinding_SessionValidator_RejectsStaleSession(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_sv", map[string]any{"roomId": "room_sv"}), h)

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "stale-sess", "room_sv", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected || res.Reason != "invalid_session" {
		t.Fatalf("stale session: %+v", res)
	}
	if e.integrity.Len() != 1 {
		t.Fatal("stale session must not append GI beyond create")
	}
}

func TestFinding_RejectedOutcomeCached_NoReauditOnReplay(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_rj", map[string]any{"roomId": "room_rj"}), h)
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_rj", map[string]any{}), h)
	r1 := decodeResult(t, w)
	if r1.Status != envelope.StatusRejected {
		t.Fatalf("%+v", r1)
	}
	audits := e.audit.Len()
	gi := e.integrity.Len()
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_rj", map[string]any{}), h)
	r2 := decodeResult(t, w)
	if r2.Status != envelope.StatusRejected || r2.Reason != r1.Reason {
		t.Fatalf("replay %+v vs %+v", r2, r1)
	}
	if e.audit.Len() != audits {
		t.Fatalf("re-audited on replay: before=%d after=%d", audits, e.audit.Len())
	}
	if e.integrity.Len() != gi {
		t.Fatal("GI changed on rejected replay")
	}
}

func TestFinding_PublisherSequenceStrictlyIncreasing(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_seq", map[string]any{"roomId": "room_seq"}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_seq", map[string]any{}), h)
	var lastPlayer, lastSpec int64
	var specCount int
	for _, ev := range e.publisher.Events {
		switch ev.Stream {
		case app.StreamPlayer:
			if ev.SequenceNumber <= lastPlayer {
				t.Fatalf("player sequence not increasing: %d after %d event=%s", ev.SequenceNumber, lastPlayer, ev.EventID)
			}
			lastPlayer = ev.SequenceNumber
		case app.StreamSpectator:
			specCount++
			if ev.SequenceNumber != lastSpec+1 {
				t.Fatalf("spectator sequence not contiguous room seq: got %d want %d event=%s type=%s",
					ev.SequenceNumber, lastSpec+1, ev.EventID, ev.EventType)
			}
			lastSpec = ev.SequenceNumber
			if ev.EventType != app.EventSnapshotSanitized && ev.EventType != app.EventRoomCompleted && ev.EventType != app.EventRoomCancelled {
				t.Fatalf("unexpected spectator event type %s", ev.EventType)
			}
		}
	}
	if lastPlayer < 1 {
		t.Fatal("expected player stream events")
	}
	if specCount < 2 || lastSpec < 2 {
		t.Fatalf("expected contiguous spectator snapshots, count=%d last=%d", specCount, lastSpec)
	}
}

func TestFinding_CommitFailure_DoesNotLeakRejectedDedupe(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_dedupe", map[string]any{"roomId": "room_dedupe"}), h)

	e.sessions.FailCommit = errors.New("commit boom")
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_dedupe", map[string]any{}), h)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("rejection store failure must surface, got %d %s", w.Code, w.Body.String())
	}
	e.sessions.FailCommit = nil

	// Same command id must not be treated as a stable prior rejection from the failed commit.
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(1), "guest", "s2", "room_dedupe", map[string]any{}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusAccepted {
		t.Fatalf("after failed reject-commit, valid join must accept: %+v", res)
	}
}

func TestFinding_MemoryGet_DeepCloneCopyOnWrite(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_clone", map[string]any{"roomId": "room_clone"}), h)

	got, ok := e.sessions.Get(context.Background(), "room_clone")
	if !ok {
		t.Fatal("missing room")
	}
	got.Room().RememberOutcome(domain.CommandOutcome{
		Kind:      domain.OutcomeRejected,
		CommandID: "ghost",
		Rejection: &domain.Rejection{Code: domain.RejectStaleSequence},
		Sequence:  got.Room().Sequence(),
	})
	again, ok := e.sessions.Get(context.Background(), "room_clone")
	if !ok {
		t.Fatal("missing room after mutate")
	}
	if _, leaked := again.Room().PriorOutcome("ghost"); leaked {
		t.Fatal("Get must return deep clone; uncommitted RememberOutcome must not leak")
	}
}

func TestFinding_SystemTimer_DoesNotOverwritePlayerSessionBinding(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_bind", map[string]any{"roomId": "room_bind"}), h)
	sid, ok := e.sessions.PlayerSession(context.Background(), "room_bind", "host")
	if !ok || sid != "s" {
		t.Fatalf("host binding=%q ok=%v", sid, ok)
	}

	// Timer path is AsSystem; even with a different sessionId it must not overwrite.
	body := cmdBody("tm", "ExpireUnoWindow", seq(1), "host", "timer-overwrite", "room_bind", map[string]any{})
	w := e.do(t, http.MethodPost, "/internal/v1/rooms/room_bind/timer-commands", body, e.timerAuth())
	if w.Code != http.StatusOK {
		t.Fatalf("timer cmd: %d %s", w.Code, w.Body.String())
	}
	sid, ok = e.sessions.PlayerSession(context.Background(), "room_bind", "host")
	if !ok || sid != "s" {
		t.Fatalf("timer must not overwrite host binding, got %q ok=%v", sid, ok)
	}
}

func TestFinding_HTTPSessionValidator_AndReady(t *testing.T) {
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sessions/validate" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("X-Service-Credential") != "id-cred" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") != "" {
			t.Errorf("sessionId must not be sent as Authorization Bearer")
		}
		var body struct {
			PlayerID  string `json:"playerId"`
			SessionID string `json:"sessionId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.SessionID != "good-sess" || body.PlayerID != "host" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"playerId": "host", "sessionId": "good-sess",
		})
	}))
	t.Cleanup(identity.Close)

	v := app.NewHTTPSessionValidator(identity.URL, "id-cred", identity.Client())
	if err := v.Validate(context.Background(), "good-sess", "host"); err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(context.Background(), "bad-sess", "host"); err == nil {
		t.Fatal("stale session must fail")
	}

	svc := app.NewService(app.ServiceDeps{
		Sessions:  app.NewMemorySessionRepository(),
		Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(),
		Audit:     app.NewFakeAuditSink(),
		Deals:     app.NewFakeDealSource(),
		Clock:     app.NewFixedClock(time.Now().UTC()),
		SessionsV: v,
	})
	srv := NewServerWithScopedCreds(svc, testCred, testTimerCred, testSpectatorRecoveryCred, testAnalyticsBackfillCred, "room-gameplay")
	srv.SetReady(false, "postgres_adapter_blocked")
	mux := srv.routes()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready blocked: %d", w.Code)
	}
	var body map[string]any
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["message"] != "postgres_adapter_blocked" && body["code"] != "not_ready" {
		// message carries the reason via WriteError
		if msg, _ := body["message"].(string); msg != "postgres_adapter_blocked" {
			t.Fatalf("ready body=%v", body)
		}
	}
	srv.SetReady(true, "")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("ready ok: %d", w.Code)
	}
}

func TestFinding_InvalidSession_PersistedNoReauditOnReplay(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_is", map[string]any{"roomId": "room_is"}), h)

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("stale-1", "JoinRoom", seq(1), "guest", "stale-sess", "room_is", map[string]any{}), h)
	r1 := decodeResult(t, w)
	if r1.Status != envelope.StatusRejected || r1.Reason != "invalid_session" {
		t.Fatalf("first: %+v", r1)
	}
	audits := e.audit.Len()

	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("stale-1", "JoinRoom", seq(1), "guest", "stale-sess", "room_is", map[string]any{}), h)
	r2 := decodeResult(t, w)
	if r2.Status != envelope.StatusRejected || r2.Reason != "invalid_session" {
		t.Fatalf("replay: %+v", r2)
	}
	if e.audit.Len() != audits {
		t.Fatalf("re-audited invalid_session replay: before=%d after=%d", audits, e.audit.Len())
	}
}

func TestFinding_RejectionAuditFailure_SurfacesError(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_af", map[string]any{"roomId": "room_af"}), h)

	e.audit.FailRecord = errors.New("audit boom")
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_af", map[string]any{}), h)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("audit failure must surface, got %d %s", w.Code, w.Body.String())
	}
}

func TestFinding_SystemTimer_FeedAudienceUsesStoredBinding(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_aud", map[string]any{"roomId": "room_aud"}), h)
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("j", "JoinRoom", seq(1), "guest", "s2", "room_aud", map[string]any{}), h)

	// Direct AsSystem LockRoom with a fake timer sessionId must not publish to that session.
	res := e.srv.svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID:              "lock-sys",
		Type:                   "LockRoom",
		SchemaVersion:          1,
		PlayerID:               "host",
		SessionID:              "timer-fake-session",
		RoomID:                 "room_aud",
		ExpectedSequenceNumber: seq(2),
		AsSystem:               true,
	})
	if res.Err != nil || res.Result.Status != envelope.StatusAccepted {
		t.Fatalf("system lock: %+v err=%v", res.Result, res.Err)
	}
	for _, ev := range e.publisher.Events {
		if ev.Stream != app.StreamPlayer {
			continue
		}
		if ev.SessionID == "timer-fake-session" {
			t.Fatalf("timer input session must not be player feed audience: %+v", ev)
		}
		if ev.PlayerID == "host" && ev.SessionID != "s" {
			t.Fatalf("host feed must use stored binding s, got %q", ev.SessionID)
		}
	}
}

func TestWireRuntime_ConfiguredWithoutDatabaseIsMisconfigured(t *testing.T) {
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		ServiceName:                 "room-gameplay",
		ServiceCredential:           "room-cred",
		TimerCredential:             "timer-cred",
		SpectatorRecoveryCredential: "spectator-recovery-cred",
		AnalyticsBackfillCredential: "analytics-room-cred",
		IdentityURL:                 "http://identity.example",
		IdentityCred:                "id-cred",
		GameIntegrityURL:            "http://gi.example",
		GameIntegrityCred:           "gi-cred",
		AllowFakes:                  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if wired.Ready {
		t.Fatal("misconfigured mode must not claim ready")
	}
	if wired.Mode != "misconfigured" || wired.NotReadyReason != "database_unconfigured" {
		t.Fatalf("mode=%s reason=%q", wired.Mode, wired.NotReadyReason)
	}
	if _, ok := wired.Deps.Sessions.(app.BlockedSessionRepository); !ok {
		t.Fatalf("sessions must be blocked, got %T", wired.Deps.Sessions)
	}
}

func TestWireRuntime_DurableMissingRedisStaysNotReady(t *testing.T) {
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		ServiceName:                 "room-gameplay",
		ServiceCredential:           "room-cred",
		TimerCredential:             "timer-cred",
		SpectatorRecoveryCredential: "spectator-recovery-cred",
		AnalyticsBackfillCredential: "analytics-room-cred",
		IdentityURL:                 "http://identity.example",
		IdentityCred:                "id-cred",
		GameIntegrityURL:            "http://gi.example",
		GameIntegrityCred:           "gi-cred",
		DatabaseURL:                 "postgres://room/db",
		AllowFakes:                  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if wired.Ready || wired.Mode != "durable" {
		t.Fatalf("ready=%v mode=%s", wired.Ready, wired.Mode)
	}
	if !strings.Contains(wired.NotReadyReason, "REDIS_URL") {
		t.Fatalf("reason=%q", wired.NotReadyReason)
	}
}

func TestWireRuntime_DurableMissingAnalyticsBackfillSecrets(t *testing.T) {
	t.Setenv("ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET", "")
	t.Setenv("ROOM_PUBLIC_LIST_CURSOR_SECRET", "")
	t.Setenv("DEPLOYMENT_ENV", "staging")
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		ServiceName:                 "room-gameplay",
		ServiceCredential:           "room-cred",
		TimerCredential:             "timer-cred",
		SpectatorRecoveryCredential: "spectator-recovery-cred",
		// AnalyticsBackfillCredential intentionally empty
		IdentityURL:       "http://identity.example",
		IdentityCred:      "id-cred",
		GameIntegrityURL:  "http://gi.example",
		GameIntegrityCred: "gi-cred",
		DatabaseURL:       "postgres://room/db",
		RedisURL:          "redis://localhost:6379/0",
		AllowFakes:        false,
		DeploymentEnv:     "staging",
	})
	if err != nil {
		t.Fatal(err)
	}
	if wired.Ready {
		t.Fatal("durable API must not be ready without analytics backfill secrets")
	}
	if !strings.Contains(wired.NotReadyReason, "ROOM_ANALYTICS_BACKFILL_SERVICE_CREDENTIAL") {
		t.Fatalf("reason=%q", wired.NotReadyReason)
	}
	if !strings.Contains(wired.NotReadyReason, "ROOM_ANALYTICS_BACKFILL_CURSOR_SECRET") {
		t.Fatalf("reason=%q", wired.NotReadyReason)
	}
	if !strings.Contains(wired.NotReadyReason, "ROOM_PUBLIC_LIST_CURSOR_SECRET") {
		t.Fatalf("reason=%q", wired.NotReadyReason)
	}
}

func TestWireRuntime_AllowFakesUsesMemoryAndFakes(t *testing.T) {
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		AllowFakes:        true,
		ServiceCredential: "c",
		GameIntegrityURL:  "http://gi.example", // ignored in fakes mode
		IdentityURL:       "",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !wired.Ready || wired.Mode != "capability-fakes" {
		t.Fatalf("fakes mode: ready=%v mode=%s", wired.Ready, wired.Mode)
	}
	if _, ok := wired.Deps.Sessions.(*app.MemorySessionRepository); !ok {
		t.Fatalf("sessions=%T", wired.Deps.Sessions)
	}
	if _, ok := wired.Deps.Integrity.(*app.FakeGameIntegrity); !ok {
		t.Fatalf("integrity=%T", wired.Deps.Integrity)
	}
	if _, ok := wired.Deps.Deals.(*app.FakeDealSource); !ok {
		t.Fatalf("deals=%T", wired.Deps.Deals)
	}
}

func TestWireRuntime_CapabilityMode_ReachesIdentityGIPublisher(t *testing.T) {
	var identityHits, giHits, publishHits int

	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sessions/validate" {
			http.NotFound(w, r)
			return
		}
		identityHits++
		if r.Header.Get("X-Service-Credential") != "id-cred" {
			http.Error(w, "cred", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"sessionId": "s1",
			"playerId":  "p1",
		})
	}))
	t.Cleanup(identity.Close)

	gi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/internal/v1/game-logs/") || !strings.HasSuffix(r.URL.Path, "/append") {
			http.NotFound(w, r)
			return
		}
		giHits++
		if r.Header.Get("X-Service-Credential") != "gi-cred" {
			http.Error(w, "cred", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "appended", "logOffset": 1, "revision": 1,
		})
	}))
	t.Cleanup(gi.Close)

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/streams/events" {
			http.NotFound(w, r)
			return
		}
		publishHits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(gateway.Close)

	spectator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(spectator.Close)
	ranking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ranking.Close)
	analytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(analytics.Close)
	tournament := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(tournament.Close)

	wired, err := wireRoomRuntime(roomRuntimeConfig{
		CapabilityMode:              true,
		ServiceCredential:           "room-cred",
		TimerCredential:             "timer-cred",
		SpectatorRecoveryCredential: "spectator-recovery-cred",
		AnalyticsBackfillCredential: "analytics-room-cred",
		IdentityURL:                 identity.URL,
		IdentityCred:                "id-cred",
		GameIntegrityURL:            gi.URL,
		GameIntegrityCred:           "gi-cred",
		GatewayURL:                  gateway.URL,
		GatewayCred:                 "gw-cred",
		SpectatorURL:                spectator.URL,
		SpectatorCred:               "sv-cred",
		RankingURL:                  ranking.URL,
		RankingCred:                 "rk-cred",
		AnalyticsURL:                analytics.URL,
		AnalyticsCred:               "an-cred",
		TournamentURL:               tournament.URL,
		TournamentCred:              "to-cred",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !wired.Ready || wired.Mode != "capability" {
		t.Fatalf("capability: ready=%v mode=%s reason=%q", wired.Ready, wired.Mode, wired.NotReadyReason)
	}
	if _, ok := wired.Deps.Sessions.(*app.MemorySessionRepository); !ok {
		t.Fatalf("capability must use memory sessions for missing Postgres, got %T", wired.Deps.Sessions)
	}
	if _, ok := wired.Deps.Integrity.(*app.HTTPGameIntegrity); !ok {
		t.Fatalf("integrity=%T", wired.Deps.Integrity)
	}
	if _, ok := wired.Deps.Deals.(*app.HTTPDealSource); !ok {
		t.Fatalf("deals=%T", wired.Deps.Deals)
	}
	if _, ok := wired.Deps.SessionsV.(*app.HTTPSessionValidator); !ok {
		t.Fatalf("sessionsV=%T", wired.Deps.SessionsV)
	}
	if _, ok := wired.Deps.Publisher.(*app.MultiDestinationPublisher); !ok {
		t.Fatalf("publisher=%T", wired.Deps.Publisher)
	}
	if _, ok := wired.Deps.Audit.(*app.JSONLAuditSink); !ok {
		t.Fatalf("audit=%T", wired.Deps.Audit)
	}

	if err := wired.Deps.SessionsV.Validate(context.Background(), "s1", "p1"); err != nil {
		t.Fatalf("identity validate: %v", err)
	}
	if identityHits != 1 {
		t.Fatalf("identity hits=%d", identityHits)
	}
	if _, err := wired.Deps.Integrity.Append(context.Background(), app.AppendRequest{
		RoomID: "r1", EventID: "e1", EventType: "Test", ExpectedRevision: 0,
		Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("gi append: %v", err)
	}
	if giHits != 1 {
		t.Fatalf("gi hits=%d", giHits)
	}
	if err := wired.Deps.Publisher.Publish(context.Background(), app.PublishedEvent{
		Stream: app.StreamPlayer, RoomID: "r1", EventID: "e1",
		EventType: "PlayerJoined", SequenceNumber: 1, SchemaVersion: 1,
		Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if publishHits != 1 {
		t.Fatalf("publish hits=%d", publishHits)
	}
}

func TestWireRuntime_CapabilityMode_MissingDepsNotReady(t *testing.T) {
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		CapabilityMode:    true,
		ServiceCredential: "room-cred",
		// missing timer, identity, GI, publisher URLs
	})
	if err != nil {
		t.Fatal(err)
	}
	if wired.Ready {
		t.Fatal("capability mode must not be ready when URLs/credentials missing")
	}
	if wired.Mode != "capability" {
		t.Fatalf("mode=%s", wired.Mode)
	}
	if wired.NotReadyReason == "" || wired.NotReadyReason == "postgres_adapter_blocked" {
		t.Fatalf("reason=%q", wired.NotReadyReason)
	}
	if _, ok := wired.Deps.Sessions.(*app.MemorySessionRepository); !ok {
		t.Fatalf("sessions=%T", wired.Deps.Sessions)
	}
}

func TestWireRuntime_CapabilityMode_DistinctFromAllowFakesAndConfigured(t *testing.T) {
	t.Setenv("ROOM_CAPABILITY_MODE", "1")
	t.Setenv("ROOM_ALLOW_FAKES", "")
	cfg := loadRoomRuntimeConfig()
	if !cfg.CapabilityMode {
		t.Fatal("expected capability mode from env")
	}
	if cfg.AllowFakes {
		t.Fatal("capability must not imply allow-fakes")
	}

	configured, err := wireRoomRuntime(roomRuntimeConfig{
		ServiceCredential:           "c",
		TimerCredential:             "t",
		SpectatorRecoveryCredential: "sr",
		IdentityURL:                 "http://identity.example",
		IdentityCred:                "id",
		GameIntegrityURL:            "http://gi.example",
		GameIntegrityCred:           "gi",
		// No DATABASE_URL → misconfigured (never silent memory).
	})
	if err != nil {
		t.Fatal(err)
	}
	if configured.Ready || configured.Mode != "misconfigured" {
		t.Fatalf("configured: ready=%v mode=%s", configured.Ready, configured.Mode)
	}
	if configured.NotReadyReason != "database_unconfigured" {
		t.Fatalf("reason=%q", configured.NotReadyReason)
	}
	if _, ok := configured.Deps.Sessions.(app.BlockedSessionRepository); !ok {
		t.Fatalf("configured must stay blocked, got %T", configured.Deps.Sessions)
	}
}

func TestWireRuntime_AllowFakesTakesPrecedenceOverCapability(t *testing.T) {
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		AllowFakes:     true,
		CapabilityMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if wired.Mode != "capability-fakes" {
		t.Fatalf("allow-fakes must win, mode=%s", wired.Mode)
	}
}

func TestFinding_SystemClock_WiredInAllRuntimeModes(t *testing.T) {
	modes := []roomRuntimeConfig{
		{AllowFakes: true, ServiceCredential: "c"},
		{CapabilityMode: true, ServiceCredential: "c", DeploymentEnv: "development"},
		{
			ServiceCredential: "c", TimerCredential: "t", SpectatorRecoveryCredential: "sr",
			IdentityURL: "http://identity.example", IdentityCred: "id",
			GameIntegrityURL: "http://gi.example", GameIntegrityCred: "gi",
			// No DATABASE_URL → misconfigured still wires SystemClock.
		},
	}
	for _, cfg := range modes {
		wired, err := wireRoomRuntime(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := wired.Deps.Clock.(app.SystemClock); !ok {
			t.Fatalf("mode=%s clock=%T want SystemClock", wired.Mode, wired.Deps.Clock)
		}
	}
}

func TestFinding_FixedClock_AdvanceAffectsAuditTimestamp(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_clk", map[string]any{"roomId": "room_clk"}), h)

	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad1", "JoinRoom", seq(99), "guest", "s2", "room_clk", map[string]any{}), h)
	if decodeResult(t, w).Status != envelope.StatusRejected {
		t.Fatalf("first reject: %s", w.Body.String())
	}
	if e.audit.Len() != 1 {
		t.Fatalf("audits=%d", e.audit.Len())
	}
	t0 := e.audit.Records[0].Timestamp

	e.clock.Advance(2 * time.Hour)
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad2", "JoinRoom", seq(99), "guest", "s2", "room_clk", map[string]any{}), h)
	if decodeResult(t, w).Status != envelope.StatusRejected {
		t.Fatalf("second reject: %s", w.Body.String())
	}
	if e.audit.Len() != 2 {
		t.Fatalf("audits=%d", e.audit.Len())
	}
	t1 := e.audit.Records[1].Timestamp
	if !t1.Equal(t0.Add(2 * time.Hour)) {
		t.Fatalf("audit timestamp did not advance with FixedClock: t0=%s t1=%s", t0, t1)
	}
}

func TestFinding_TransientAuditFailure_RecoveredOnReplay(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_ar", map[string]any{"roomId": "room_ar"}), h)

	e.audit.FailRecord = errors.New("audit boom")
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_ar", map[string]any{}), h)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("audit failure must surface, got %d %s", w.Code, w.Body.String())
	}
	if _, ok := e.sessions.GetPendingAudit(context.Background(), "bad"); !ok {
		t.Fatal("rejection must leave audit-pending record keyed by commandId")
	}
	if e.audit.Len() != 0 {
		t.Fatalf("failed sink must not count as delivered: %d", e.audit.Len())
	}

	e.audit.FailRecord = nil
	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_ar", map[string]any{}), h)
	r1 := decodeResult(t, w)
	if w.Code != http.StatusOK || r1.Status != envelope.StatusRejected {
		t.Fatalf("replay must return deduped rejection after audit recovery: %d %+v", w.Code, r1)
	}
	if e.audit.Len() != 1 {
		t.Fatalf("pending audit must be delivered exactly once on recovery, got %d", e.audit.Len())
	}
	if _, ok := e.sessions.GetPendingAudit(context.Background(), "bad"); ok {
		t.Fatal("audit must be marked complete after successful delivery")
	}

	w = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("bad", "JoinRoom", seq(99), "guest", "s2", "room_ar", map[string]any{}), h)
	r2 := decodeResult(t, w)
	if r2.Status != envelope.StatusRejected || r2.Reason != r1.Reason {
		t.Fatalf("stable replay %+v vs %+v", r2, r1)
	}
	if e.audit.Len() != 1 {
		t.Fatalf("completed audit must never duplicate on replay: %d", e.audit.Len())
	}
}

func TestFinding_NotReady_GatesCommandProvisionTimerSnapshot(t *testing.T) {
	e := newTestEnv(t)
	e.srv.SetReady(false, "capability_dependencies_missing")
	mux := e.srv.routes()

	assertNotReady := func(method, path string, body any) {
		t.Helper()
		w := e.do(t, method, path, body, nil) // no auth — gate must run first
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s: want 503, got %d %s", method, path, w.Code, w.Body.String())
		}
		var resp map[string]any
		_ = json.NewDecoder(w.Body).Decode(&resp)
		if resp["code"] != "not_ready" {
			t.Fatalf("%s %s body=%v", method, path, resp)
		}
	}

	assertNotReady(http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_nr", map[string]any{"roomId": "room_nr"}))
	assertNotReady(http.MethodPost, "/internal/v1/rooms/provision", map[string]any{
		"commandId": "p", "tournamentId": "t", "roundNumber": 1, "slotId": "s",
		"roomId": "r", "hostId": "host", "playerIds": []string{"host"},
	})
	assertNotReady(http.MethodPost, "/internal/v1/rooms/room_nr/timer-commands", cmdBody("tm", "ExpireUnoWindow", seq(1), "host", "s", "room_nr", map[string]any{}))
	assertNotReady(http.MethodGet, "/internal/v1/rooms/room_nr/spectator-recovery-snapshot?failedCheckpoint=1&recoveryJobId=j&schemaVersion=1", nil)
	assertNotReady(http.MethodPost, "/internal/v1/rooms/analytics-backfill", map[string]any{
		"recoveryJobId": "j", "sourceTopic": "room.gameplay.metrics", "schemaVersion": 1,
		"fromCheckpoint": "1", "toCheckpoint": "2",
	})
	assertNotReady(http.MethodGet, "/v1/rooms/room_nr/snapshot?playerId=host", nil)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("health must stay up: %d", w.Code)
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready must report not ready: %d", w.Code)
	}
}

func TestWireRuntime_CapabilityMissingDeps_ClosedAdaptersNoPanic(t *testing.T) {
	wired, err := wireRoomRuntime(roomRuntimeConfig{
		CapabilityMode:    true,
		ServiceCredential: "room-cred",
	})
	if err != nil {
		t.Fatal(err)
	}
	if wired.Ready {
		t.Fatal("expected not ready")
	}
	if _, ok := wired.Deps.SessionsV.(app.ClosedSessionValidator); !ok {
		t.Fatalf("sessionsV=%T", wired.Deps.SessionsV)
	}
	if _, ok := wired.Deps.Integrity.(app.ClosedGameIntegrity); !ok {
		t.Fatalf("integrity=%T", wired.Deps.Integrity)
	}
	if _, ok := wired.Deps.Deals.(app.ClosedDealSource); !ok {
		t.Fatalf("deals=%T", wired.Deps.Deals)
	}
	if err := wired.Deps.SessionsV.Validate(context.Background(), "s", "p"); err == nil {
		t.Fatal("closed identity must reject validation")
	}
	if _, err := wired.Deps.Integrity.Append(context.Background(), app.AppendRequest{RoomID: "r", EventID: "e", EventType: "X"}); err == nil {
		t.Fatal("closed GI must reject append")
	}
	if _, err := wired.Deps.Deals.ReserveDeal(context.Background(), "r", "g", "op", []string{"p"}); err == nil {
		t.Fatal("closed deals must reject reserve")
	}
}

func wireQuickWinDeals(e *testEnv) {
	e.deals.DealFn = func(roomID, gameID string, seats []string) (game.DealMaterial, error) {
		return quickWinDeal(seats), nil
	}
}

func quickWinDeal(seats []string) game.DealMaterial {
	hands := make(map[game.PlayerID][]game.Card, len(seats))
	for i, s := range seats {
		pid := game.PlayerID(s)
		if i == 0 {
			hands[pid] = []game.Card{
				{ID: "host-w", Color: game.ColorRed, Face: game.Face3},
			}
		} else {
			hands[pid] = []game.Card{
				{ID: game.CardID(s + "-c1"), Color: game.ColorBlue, Face: game.Face2},
			}
		}
	}
	return game.DealMaterial{
		Hands:       hands,
		DiscardTop:  game.Card{ID: "disc-top", Color: game.ColorRed, Face: game.Face5},
		ActiveColor: game.ColorRed,
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	}
}

func TestFinding_NewServiceDefaultsClosedSessionValidator(t *testing.T) {
	svc := app.NewService(app.ServiceDeps{
		Sessions:  app.NewMemorySessionRepository(),
		Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(),
		Deals:     app.NewFakeDealSource(),
		Audit:     app.NewFakeAuditSink(),
		Clock:     app.NewFixedClock(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)),
	})
	res := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "c1", Type: "CreateRoom", SchemaVersion: 1,
		PlayerID: "p1", SessionID: "s1", RoomID: "r1",
		Payload: mustJSON(map[string]any{"roomId": "r1"}),
	})
	if res.Result.Status != envelope.StatusRejected {
		t.Fatalf("nil SessionsV must default closed and reject, got %+v", res)
	}
}

func TestFinding_EmptyPlayerSessionRejectsUnlessSystem(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	w := e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c-empty", "CreateRoom", nil, "", "", "room_empty", map[string]any{"roomId": "room_empty"}), h)
	res := decodeResult(t, w)
	if res.Status != envelope.StatusRejected {
		t.Fatalf("empty player/session must reject, got %+v", res)
	}
	svc := app.NewService(app.ServiceDeps{
		Sessions:  app.NewMemorySessionRepository(),
		Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(),
		Deals:     app.NewFakeDealSource(),
		SessionsV: app.ClosedSessionValidator{},
		Clock:     app.NewFixedClock(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)),
		Audit:     app.NewFakeAuditSink(),
	})
	sys := svc.HandleCommand(context.Background(), app.CommandInput{
		CommandID: "sys1", Type: "ExpireUnoWindow", SchemaVersion: 1,
		AsSystem: true, RoomID: "missing-room",
		ExpectedSequenceNumber: int64Ptr(1),
		Payload:                mustJSON(map[string]any{}),
	})
	if sys.Result.Reason == "invalid_session" {
		t.Fatalf("AsSystem must skip session validation, got %+v", sys.Result)
	}
}

func int64Ptr(v int64) *int64 { return &v }

func TestFinding_DrainOutboxNilDepsError(t *testing.T) {
	svc := app.NewService(app.ServiceDeps{
		SessionsV: app.AllowAllSessionValidator{},
		Clock:     app.NewFixedClock(time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)),
	})
	if _, err := svc.DrainOutbox(context.Background(), 10); err == nil {
		t.Fatal("nil Sessions/Publisher must return error")
	}
}

func TestFinding_DrainOutboxSerialized(t *testing.T) {
	e := newTestEnv(t)
	h := e.auth()
	_ = e.do(t, http.MethodPost, "/internal/v1/commands", cmdBody("c", "CreateRoom", nil, "host", "s", "room_drain", map[string]any{"roomId": "room_drain"}), h)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.srv.svc.DrainOutbox(context.Background(), 10)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("drain err=%v", err)
		}
	}
}

func TestFinding_HTTPDealCancelMissingMetaErrors(t *testing.T) {
	deals := app.NewHTTPDealSource("http://127.0.0.1:9", "cred", nil)
	if err := deals.Cancel(context.Background(), "unknown-res"); err == nil {
		t.Fatal("missing cancel meta must not succeed")
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
