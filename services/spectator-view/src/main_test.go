package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"unoarena/services/spectator-view/domain"
)

const testCredential = "test-internal-credential"

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return NewServer(NewMemoryProjectionStore(), testCredential)
}

func withCred(req *http.Request) *http.Request {
	req.Header.Set(internalCredentialHeader, testCredential)
	return req
}

func doJSON(t *testing.T, mux http.Handler, method, path string, body any, cred bool) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cred {
		withCred(req)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func canonicalEvent(roomID, eventID, event string, seq uint64, data map[string]any) map[string]any {
	return map[string]any{
		"schemaVersion": 1,
		"eventId":       eventID,
		"roomId":        roomID,
		"sequence":      seq,
		"event":         event,
		"data":          data,
	}
}

func ingestRoomCreated(t *testing.T, mux http.Handler, roomID, eventID, visibility string, seq uint64) *httptest.ResponseRecorder {
	t.Helper()
	return doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+roomID+"/events",
		canonicalEvent(roomID, eventID, "RoomCreated", seq, map[string]any{
			"visibility": visibility,
			"status":     "waiting",
			"seats": []any{
				map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 0},
				map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 0},
			},
		}), true)
}

func TestHealthAndReadyOffline(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()

	hw := httptest.NewRecorder()
	mux.ServeHTTP(hw, httptest.NewRequest(http.MethodGet, "/health", nil))
	if hw.Code != http.StatusOK {
		t.Fatalf("health: expected 200, got %d", hw.Code)
	}
	var health map[string]string
	if err := json.NewDecoder(hw.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health["status"] != "ok" || health["service"] != "spectator-view" {
		t.Fatalf("health: %+v", health)
	}

	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rw.Code != http.StatusOK {
		t.Fatalf("ready: expected 200, got %d body=%s", rw.Code, rw.Body.String())
	}
}

func TestInternalRoutesRejectCredential(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/room_1/spectator-admission", map[string]any{
		"roomId": "room_1", "playerId": "p1", "sessionId": "s1",
	}, false)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestAdmissionIgnoresBlanketAuthorized(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	if w := ingestRoomCreated(t, mux, "priv_1", "evt_priv_1", "private", 1); w.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", w.Code, w.Body.String())
	}

	// authorized:true without roster/session must not admit.
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/priv_1/spectator-admission", map[string]any{
		"roomId": "priv_1", "authorized": true,
	}, true)
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["allowed"] != false {
		t.Fatalf("blanket authorized must be ignored: %+v", resp)
	}

	// Participant on roster + session context.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/priv_1/spectator-admission", map[string]any{
		"roomId": "priv_1", "playerId": "p1", "sessionId": "s1",
	}, true)
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["allowed"] != true {
		t.Fatalf("participant: %+v", resp)
	}

	// Invite / operator scopes — invite requires registered opaque token via header.
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/priv_1/spectator-admission", map[string]any{
		"roomId": "priv_1", "inviteCapability": true,
	}, true)
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["allowed"] != false {
		t.Fatalf("boolean inviteCapability must not admit: %+v", resp)
	}

	reg := doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/priv_1/invites", map[string]any{
		"inviteToken": "opaque-invite-1",
	}, true)
	if reg.Code != http.StatusOK {
		t.Fatalf("register invite: %d %s", reg.Code, reg.Body.String())
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/rooms/priv_1/spectator-admission",
		bytes.NewReader(mustJSON(map[string]any{"roomId": "priv_1"})))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalCredentialHeader, testCredential)
	req.Header.Set("X-Room-Invite", "opaque-invite-1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["allowed"] != true {
		t.Fatalf("invite token: %+v", resp)
	}

	w = doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/priv_1/spectator-admission", map[string]any{
		"roomId": "priv_1", "operator": true,
	}, true)
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["allowed"] != true {
		t.Fatalf("operator: %+v", resp)
	}
}

func TestCanonicalEnvelopeAndNestedRejected(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room_canon"

	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/events",
		canonicalEvent(room, "e1", "RoomCreated", 1, map[string]any{"visibility": "public"}), true)
	if w.Code != http.StatusOK {
		t.Fatalf("canonical: %d %s", w.Code, w.Body.String())
	}
	var out map[string]any
	_ = json.NewDecoder(w.Body).Decode(&out)
	if out["kind"] != "accepted" {
		t.Fatalf("out=%+v", out)
	}

	nested := map[string]any{
		"schemaVersion": 1, "eventId": "e2", "roomId": room, "sequence": 2,
		"event": "RoomLocked",
		"data": map[string]any{
			"roomId": room, "eventType": "RoomLocked", "sequenceNumber": 2,
			"payload": map[string]any{"status": "locked"},
		},
	}
	w = doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/events", nested, true)
	_ = json.NewDecoder(w.Body).Decode(&out)
	if out["kind"] == "accepted" {
		t.Fatalf("nested envelope must not apply: %+v", out)
	}
}

func TestBatchFactsAtomic(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room_batch"
	ingestRoomCreated(t, mux, room, "b0", "public", 1)

	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/events", map[string]any{
		"schemaVersion": 1, "eventId": "batch_1", "roomId": room, "sequence": 2,
		"facts": []map[string]any{
			{"eventId": "b1", "event": "CardPlayed", "data": map[string]any{"discardTop": "red-1", "activeColor": "red"}},
			{"eventId": "b2", "event": "TurnAdvanced", "data": map[string]any{"currentPlayerId": "p2"}},
		},
	}, true)
	if w.Code != http.StatusOK {
		t.Fatalf("batch: %d %s", w.Code, w.Body.String())
	}
	sw := httptest.NewRecorder()
	mux.ServeHTTP(sw, httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot", nil))
	var snap map[string]any
	_ = json.NewDecoder(sw.Body).Decode(&snap)
	if snap["sequence"] != float64(2) {
		t.Fatalf("sequence=%v", snap["sequence"])
	}
	if snap["currentPlayerId"] != "p2" {
		t.Fatalf("snap=%+v", snap)
	}
}

func TestMatchCompletedTerminalClosesStreams(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room_match"
	ingestRoomCreated(t, mux, room, "m0", "public", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/spectator/rooms/"+room+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(w, req)
		close(done)
	}()

	// Wait briefly for the SSE handler to subscribe, bounded by the request context.
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal("context expired before SSE subscribe")
	}

	doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/events",
		canonicalEvent(room, "m1", "MatchCompleted", 2, map[string]any{
			"matchWinner": "p1",
			"matchWins":   map[string]any{"p1": 2, "p2": 0},
		}), true)

	select {
	case <-done:
	case <-ctx.Done():
		cancel()
		t.Fatal("SSE did not close after MatchCompleted")
	}
	body := w.Body.String()
	if !strings.Contains(body, "event: snapshot") && !strings.Contains(body, "event: projection_updated") && !strings.Contains(body, "stream_closed") {
		// At least connection should have ended; stream_closed is ideal.
		if w.Body.Len() == 0 {
			t.Fatalf("empty SSE body")
		}
	}

	// New admission denied.
	aw := doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/"+room+"/spectator-admission", map[string]any{
		"roomId": room, "operator": true,
	}, true)
	var resp map[string]any
	_ = json.NewDecoder(aw.Body).Decode(&resp)
	if resp["allowed"] != false {
		t.Fatalf("terminal admission: %+v", resp)
	}
	nwCtx, nwCancel := context.WithTimeout(context.Background(), time.Second)
	defer nwCancel()
	nwReq, _ := http.NewRequestWithContext(nwCtx, http.MethodGet, "/v1/spectator/rooms/"+room+"/events", nil)
	nw := httptest.NewRecorder()
	mux.ServeHTTP(nw, nwReq)
	if nw.Code != http.StatusForbidden {
		t.Fatalf("new SSE expected 403, got %d", nw.Code)
	}
}

func TestPrivateSnapshotRequiresRosterOrScope(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "snap_priv"
	ingestRoomCreated(t, mux, room, "s1", "private", 1)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}

	// Query invite/operator must NOT work.
	req := httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot?invite=1&operator=1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("query invite/operator must fail: %d", w.Code)
	}

	// Participant headers without Gateway credential must NOT work.
	req = httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot", nil)
	req.Header.Set("X-Player-Id", "p1")
	req.Header.Set("X-Session-Id", "sess_1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("untrusted participant headers must fail: %d", w.Code)
	}

	// Gateway credential + participant context.
	req = httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot", nil)
	req.Header.Set(internalCredentialHeader, testCredential)
	req.Header.Set("X-Player-Id", "p1")
	req.Header.Set("X-Session-Id", "sess_1")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("participant snapshot: %d %s", w.Code, w.Body.String())
	}

	// Opaque invite via registry + Gateway credential.
	doJSON(t, mux, http.MethodPost, "/internal/v1/rooms/"+room+"/invites", map[string]any{
		"inviteToken": "snap-invite",
	}, true)
	req = httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot", nil)
	req.Header.Set(internalCredentialHeader, testCredential)
	req.Header.Set("X-Room-Invite", "snap-invite")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("invite snapshot: %d %s", w.Code, w.Body.String())
	}
}

func TestRebuildStatusNoNestedLockDeadlock(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room_rebuild_status"
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/rebuild", map[string]any{
		"events": []map[string]any{
			canonicalEvent(room, "new_1", "RoomCreated", 1, map[string]any{"visibility": "public"}),
		},
	}, true)
	if w.Code != http.StatusOK {
		t.Fatalf("rebuild: %d %s", w.Code, w.Body.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		sw := doJSON(t, mux, http.MethodGet, "/internal/v1/spectator/rooms/"+room+"/rebuild-status", nil, true)
		if sw.Code != http.StatusOK {
			t.Errorf("rebuild-status: %d %s", sw.Code, sw.Body.String())
		}
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("rebuild-status deadlocked")
	}
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestConcurrentApplyAndSnapshotRace(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "race_room"
	ingestRoomCreated(t, mux, room, "r0", "public", 1)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		seq := uint64(i + 2)
		go func(seq uint64) {
			defer wg.Done()
			doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/events",
				canonicalEvent(room, "e"+itoa(int(seq)), "TurnAdvanced", seq, map[string]any{
					"currentPlayerId": "p1", "direction": "clockwise",
				}), true)
		}(seq)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/spectator/rooms/"+room+"/snapshot", nil))
			if w.Code != http.StatusOK && w.Code != http.StatusForbidden {
				t.Errorf("snapshot status=%d", w.Code)
			}
		}()
	}
	wg.Wait()
}

func TestRebuildAndStatus(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room_rebuild"
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/rebuild", map[string]any{
		"events": []map[string]any{
			canonicalEvent(room, "new_1", "RoomCreated", 1, map[string]any{"visibility": "private"}),
			canonicalEvent(room, "new_2", "RoomLocked", 2, map[string]any{"status": "locked"}),
		},
	}, true)
	if w.Code != http.StatusOK {
		t.Fatalf("rebuild: %d %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "locked" || resp["eventCount"] != float64(2) {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestFinding_QuarantineOutOfOrderReturnsConflict(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	if w := ingestRoomCreated(t, mux, "room_oo", "e1", "public", 1); w.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", w.Code, w.Body.String())
	}
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/room_oo/events",
		canonicalEvent("room_oo", "e3", "PlayerJoinedRoom", 3, map[string]any{
			"playerId": "p3", "displayName": "Carol", "seatIndex": 2,
		}), true)
	if w.Code == http.StatusOK {
		t.Fatalf("out-of-order must be non-2xx, body=%s", w.Body.String())
	}
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d want 409", w.Code)
	}
}

func TestFinding_DroppedPrivacyIngestReturnsConflict(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	if w := ingestRoomCreated(t, mux, "room_drop", "e1", "public", 1); w.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", w.Code, w.Body.String())
	}
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/room_drop/events",
		canonicalEvent("room_drop", "e2", "CardPlayed", 2, map[string]any{
			"playerId": "p1", "hand": []any{"red-1", "blue-2"},
		}), true)
	if w.Code == http.StatusOK {
		t.Fatalf("dropped privacy ingest must be non-2xx, body=%s", w.Body.String())
	}
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d want 409 body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["kind"] != "dropped" && body["kind"] != "quarantined" {
		t.Fatalf("kind=%v want dropped/quarantined", body["kind"])
	}
}

func TestFinding_UnknownLastEventIDEmptyBufferSnapshotRequired(t *testing.T) {
	hub := NewStreamHub()
	id, ch, replay, cancel, err := hub.Subscribe(domain.RoomID("r_empty"), "seq_missing")
	if err == nil || !strings.Contains(err.Error(), "snapshot_required") {
		t.Fatalf("empty buffer unknown Last-Event-ID must require snapshot, err=%v", err)
	}
	if id == "" || ch == nil || cancel == nil {
		t.Fatalf("snapshot_required must still register a fresh sub: id=%q ch=%v cancel=%v", id, ch != nil, cancel != nil)
	}
	if replay != nil {
		t.Fatalf("unknown Last-Event-ID must not invent replay: %+v", replay)
	}
	if hub.ActiveCount() != 1 {
		t.Fatalf("fresh sub active=%d", hub.ActiveCount())
	}
	cancel()
	if hub.ActiveCount() != 0 {
		t.Fatalf("cancel left active=%d", hub.ActiveCount())
	}
}

func TestSubscribe_UnknownLastEventIDQueuesLiveBehindSnapshot(t *testing.T) {
	hub := NewStreamHub()
	room := domain.RoomID("r_resync")
	_, live, _, cancel, err := hub.Subscribe(room, "seq_gone")
	if err == nil || !strings.Contains(err.Error(), "snapshot_required") {
		t.Fatalf("err=%v", err)
	}
	if live == nil || cancel == nil {
		t.Fatal("unknown Last-Event-ID must return non-nil channel/cancel")
	}
	defer cancel()

	// Live update arrives after register, before snapshot write — must queue.
	hub.PublishUpdate(room, 2, json.RawMessage(`{"phase":"queued"}`))
	snap := hub.PublishSnapshot(room, 1, json.RawMessage(`{"phase":"snapshot"}`))
	if snap.Event != "snapshot" || snap.Sequence != 1 {
		t.Fatalf("snap=%+v", snap)
	}

	select {
	case ev := <-live:
		if ev.Sequence != 2 || ev.Event != "projection_updated" {
			t.Fatalf("queued live=%+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("live update queued behind snapshot was lost")
	}

	hub.PublishUpdate(room, 3, json.RawMessage(`{"phase":"after"}`))
	select {
	case ev := <-live:
		if ev.Sequence != 3 {
			t.Fatalf("live=%+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for post-snapshot live update")
	}
}

// Snapshot after a queued live update must not append seq1 after seq2 in the
// shared replay buffer. Reconnect from seq2 must never replay seq1.
func TestPublishSnapshot_KeepsReplayBufferMonotonic_ReconnectFromSeq2(t *testing.T) {
	hub := NewStreamHub()
	room := domain.RoomID("r_mono")
	hub.PublishUpdate(room, 2, json.RawMessage(`{"phase":"queued"}`))
	hub.PublishSnapshot(room, 1, json.RawMessage(`{"phase":"snapshot"}`))

	assertReplayBufferMonotonic(t, hub, room)

	_, _, replay, cancel, err := hub.Subscribe(room, "seq_2")
	if err != nil {
		t.Fatalf("resume from seq_2: %v", err)
	}
	defer cancel()
	for _, ev := range replay {
		if ev.Sequence <= 2 {
			t.Fatalf("reconnect from seq2 must never replay seq<=2: %+v", replay)
		}
	}
}

func TestPublishSnapshot_ConcurrentLiveThenSnapshot_ReconnectNeverReplaysEarlierSeq(t *testing.T) {
	hub := NewStreamHub()
	room := domain.RoomID("r_mono_conc")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			hub.PublishUpdate(room, uint64(n+2), json.RawMessage(`{"phase":"live"}`))
		}(i)
		go func() {
			defer wg.Done()
			hub.PublishSnapshot(room, 1, json.RawMessage(`{"phase":"snapshot"}`))
		}()
	}
	wg.Wait()

	assertReplayBufferMonotonic(t, hub, room)

	_, _, replay, cancel, err := hub.Subscribe(room, "seq_2")
	if err != nil {
		t.Fatalf("resume from seq_2 after concurrent race: %v", err)
	}
	defer cancel()
	for _, ev := range replay {
		if ev.Sequence <= 2 {
			t.Fatalf("reconnect from seq2 must never replay seq<=2: %+v", replay)
		}
	}
}

func assertReplayBufferMonotonic(t *testing.T, hub *StreamHub, room domain.RoomID) {
	t.Helper()
	hub.mu.Lock()
	buf := append([]StreamEvent(nil), hub.buffers[room]...)
	hub.mu.Unlock()
	for i := 1; i < len(buf); i++ {
		if buf[i].Sequence < buf[i-1].Sequence {
			t.Fatalf("replay buffer not monotonic at %d: %+v", i, buf)
		}
		if buf[i].Sequence == buf[i-1].Sequence {
			t.Fatalf("replay buffer duplicate sequence at %d: %+v", i, buf)
		}
	}
}

func TestSubscribe_SnapshotRequiredConcurrentNoLostUpdate(t *testing.T) {
	hub := NewStreamHub()
	room := domain.RoomID("r_conc")
	var wg sync.WaitGroup
	errCh := make(chan string, 64)
	subs := make(chan func(), 64)
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			id, ch, _, cancel, err := hub.Subscribe(room, "seq_missing")
			if err == nil || !strings.Contains(err.Error(), "snapshot_required") {
				errCh <- "want snapshot_required"
				return
			}
			if id == "" || ch == nil || cancel == nil {
				errCh <- "snapshot_required must register fresh sub"
				return
			}
			subs <- cancel
		}()
		go func(n int) {
			defer wg.Done()
			hub.PublishUpdate(room, uint64(n+1), json.RawMessage(`{}`))
		}(i)
	}
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Fatal(msg)
	}
	close(subs)
	for cancel := range subs {
		cancel()
	}

	_, ch, _, cancel, err := hub.Subscribe(room, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	hub.PublishUpdate(room, 101, json.RawMessage(`{"ok":true}`))
	select {
	case ev := <-ch:
		if ev.Sequence != 101 {
			t.Fatalf("ev=%+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout after concurrent snapshot_required resync")
	}
}

func TestFinding_FullChannelClosesWithResync(t *testing.T) {
	hub := NewStreamHub()
	room := domain.RoomID("r_full")
	_, ch, _, cancel, err := hub.Subscribe(room, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	for i := 0; i < 16; i++ {
		hub.PublishUpdate(room, uint64(i+1), json.RawMessage(`{}`))
	}
	hub.PublishUpdate(room, 99, json.RawMessage(`{}`))
	saw := false
	deadline := time.After(time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				if !saw {
					t.Fatal("expected snapshot_required before close")
				}
				return
			}
			if ev.Event == "snapshot_required" {
				saw = true
			}
		case <-deadline:
			t.Fatal("timeout waiting for resync/close")
		}
	}
}

func TestFinding_ReadyRequiresInternalCredential(t *testing.T) {
	srv := NewServer(NewMemoryProjectionStore(), "")
	mux := srv.routes()
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rw.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready without cred status=%d", rw.Code)
	}
	w := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/r1/events",
		canonicalEvent("r1", "e1", "RoomCreated", 1, map[string]any{"visibility": "public", "status": "waiting"}), false)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("write without ready status=%d", w.Code)
	}
}

func TestFinding_IngestPublishesUnderRoomLockOrder(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.routes()
	room := "room_ord"
	_, ch, _, cancel, err := srv.hub.Subscribe(domain.RoomID(room), "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if w := ingestRoomCreated(t, mux, room, "e1", "public", 1); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w2 := doJSON(t, mux, http.MethodPost, "/internal/v1/spectator/rooms/"+room+"/events",
		canonicalEvent(room, "e2", "PlayerJoinedRoom", 2, map[string]any{
			"playerId": "p3", "displayName": "Carol", "seatIndex": 2,
		}), true)
	if w2.Code != http.StatusOK {
		t.Fatalf("e2: %d %s", w2.Code, w2.Body.String())
	}
	var seqs []uint64
	deadline := time.After(time.Second)
	for len(seqs) < 2 {
		select {
		case ev := <-ch:
			seqs = append(seqs, ev.Sequence)
		case <-deadline:
			t.Fatalf("got seqs=%v", seqs)
		}
	}
	if seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("hub order=%v want 1,2", seqs)
	}
}
