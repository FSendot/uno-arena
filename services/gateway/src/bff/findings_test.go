package bff_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"unoarena/services/gateway/bff"
	"unoarena/shared/audit"
	"unoarena/shared/correlation"
)

func TestHub_PlayerPrivateNoCrossAudienceLeak(t *testing.T) {
	hub := bff.NewHub()
	_, chA, _, cancelA, err := hub.Subscribe(bff.StreamPlayer, "room_1", "session_a", "player_a", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancelA()
	_, chB, _, cancelB, err := hub.Subscribe(bff.StreamPlayer, "room_1", "session_b", "player_b", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancelB()

	sent := hub.PublishToStream(bff.StreamPlayer, "room_1", "session_a", "player_a", bff.StreamEvent{
		Event:         "private_hand",
		Data:          json.RawMessage(`{"card":"red-7"}`),
		SchemaVersion: 1,
	})
	if sent != 1 {
		t.Fatalf("sent=%d want 1 (only player_a)", sent)
	}

	select {
	case ev := <-chA:
		if ev.Event != "private_hand" {
			t.Fatalf("player_a got %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("player_a did not receive private event")
	}
	select {
	case ev := <-chB:
		t.Fatalf("player_b leaked private event: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHub_PlayerPrivateReplayIsolatedByAudience(t *testing.T) {
	hub := bff.NewHub()
	hub.SetReplayBound(8)
	hub.PublishToStream(bff.StreamPlayer, "room_1", "session_a", "player_a", bff.StreamEvent{
		Event: "a1", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})
	hub.PublishToStream(bff.StreamPlayer, "room_1", "session_b", "player_b", bff.StreamEvent{
		Event: "b1", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})
	hub.PublishToStream(bff.StreamPlayer, "room_1", "session_a", "player_a", bff.StreamEvent{
		Event: "a2", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})

	_, _, replayA, cancelA, err := hub.Subscribe(bff.StreamPlayer, "room_1", "session_a", "player_a", "1")
	if err != nil {
		t.Fatal(err)
	}
	defer cancelA()
	if len(replayA) != 1 || replayA[0].Event != "a2" {
		t.Fatalf("player_a replay=%+v", replayA)
	}
	for _, ev := range replayA {
		if ev.Event == "b1" {
			t.Fatal("player_a replay leaked player_b event")
		}
	}
}

func TestHub_UnknownLastEventIDRequiresSnapshot(t *testing.T) {
	hub := bff.NewHub()
	hub.SetReplayBound(2)
	hub.PublishToStream(bff.StreamSpectator, "room_1", "", "", bff.StreamEvent{
		Event: "t1", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})
	hub.PublishToStream(bff.StreamSpectator, "room_1", "", "", bff.StreamEvent{
		Event: "t2", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})
	hub.PublishToStream(bff.StreamSpectator, "room_1", "", "", bff.StreamEvent{
		Event: "t3", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})

	_, _, _, cancel, err := hub.Subscribe(bff.StreamSpectator, "room_1", "", "", "1")
	if cancel != nil {
		cancel()
	}
	if err == nil || !strings.Contains(err.Error(), "snapshot_required") {
		t.Fatalf("evicted Last-Event-ID must require snapshot, err=%v", err)
	}

	_, _, _, cancel2, err2 := hub.Subscribe(bff.StreamSpectator, "room_1", "", "", "missing")
	if cancel2 != nil {
		cancel2()
	}
	if err2 == nil || !strings.Contains(err2.Error(), "snapshot_required") {
		t.Fatalf("unknown Last-Event-ID must require snapshot, err=%v", err2)
	}
}

func TestSSE_UnknownLastEventIDHTTPSnapshotRequired(t *testing.T) {
	h := newHarness(t)
	h.hub.PublishToStream(bff.StreamSpectator, "room_1", "", "", bff.StreamEvent{
		Event: "tick", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})
	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_1", nil, map[string]string{
		"Last-Event-ID": "gone",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "snapshot_required") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestSSE_SessionInvalidationClosesAuthenticatedSpectator(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/streams/spectator?roomId=room_auth", nil)
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	done := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := resp.Body.Read(buf)
		done <- string(buf[:n])
	}()

	deadline := time.Now().Add(2 * time.Second)
	for h.hub.ActiveCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if n := h.hub.InvalidateSession(h.principal.SessionID); n < 1 {
		t.Fatalf("invalidate closed=%d", n)
	}

	select {
	case body := <-done:
		if !strings.Contains(body, "session_invalidated") {
			t.Fatalf("sse body=%q", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for authenticated spectator close")
	}

	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_auth", nil, h.authHeaders())
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("re-subscribe status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestInternal_EventIngestPublishesToHub(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                   h.identity,
		Room:                       h.room,
		Tournament:                 h.tournament,
		Reads:                      &bff.FakeReads{},
		Spectator:                  h.spectator,
		Audit:                      h.audit,
		Hub:                        h.hub,
		Ready:                      true,
		IdentityProducerCredential: "ident-cred",
		RoomProducerCredential:     "room-cred",
	})

	_, ch, _, cancel, err := h.hub.Subscribe(bff.StreamPlayer, "room_9", "session_1", "player_1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	body := []byte(`{
		"schemaVersion":1,
		"eventId":"evt_1",
		"stream":"player",
		"roomId":"room_9",
		"sessionId":"session_1",
		"playerId":"player_1",
		"sequence":7,
		"event":"CardDealt",
		"data":{"cardId":"blue-3"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "room-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	select {
	case ev := <-ch:
		if ev.Event != "CardDealt" || ev.ID != "7" {
			t.Fatalf("event=%+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("hub did not receive ingested event")
	}

	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Service-Credential", "room-cred")
	srv.Handler().ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("idempotent status=%d", w2.Code)
	}
	select {
	case ev := <-ch:
		t.Fatalf("duplicate publish: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestInternal_EventIngestRejectsIdentityCredential(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                   h.identity,
		Hub:                        h.hub,
		Ready:                      true,
		IdentityProducerCredential: "ident-cred",
		RoomProducerCredential:     "room-cred",
	})
	body := []byte(`{"schemaVersion":1,"eventId":"e1","stream":"spectator","roomId":"r1","event":"tick","data":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "ident-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestInternal_SessionInvalidationValidatesBodyPathAndIdempotency(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                    h.identity,
		Hub:                         h.hub,
		Ready:                       true,
		IdentityProducerCredential:  "ident-cred",
		RoomProducerCredential:      "room-cred",
		SpectatorProducerCredential: "spec-cred",
	})

	mismatch := []byte(`{"schemaVersion":1,"eventId":"inv_1","eventType":"SessionInvalidated","sessionId":"other","playerId":"p1","reason":"login"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(mismatch))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "ident-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("mismatch status=%d body=%s", w.Code, w.Body.String())
	}

	okBody := []byte(`{"schemaVersion":1,"eventId":"inv_1","eventType":"SessionInvalidated","sessionId":"session_1","playerId":"p1","reason":"login"}`)
	req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(okBody))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Service-Credential", "ident-cred")
	w2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("ok status=%d body=%s", w2.Code, w2.Body.String())
	}

	req3 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(okBody))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("X-Service-Credential", "room-cred")
	w3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w3, req3)
	if w3.Code != http.StatusUnauthorized {
		t.Fatalf("room cred on identity route status=%d", w3.Code)
	}

	w4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(okBody))
	req4.Header.Set("Content-Type", "application/json")
	req4.Header.Set("X-Service-Credential", "ident-cred")
	srv.Handler().ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Fatalf("idempotent status=%d", w4.Code)
	}
	if !strings.Contains(w4.Body.String(), `"duplicate":true`) {
		t.Fatalf("exact replay must report duplicate, body=%s", w4.Body.String())
	}
}

func TestRateLimit_PrincipalAfterDecodeAuditsWithoutDispatch(t *testing.T) {
	identity := bff.NewFakeIdentity()
	room := bff.NewFakeRoom()
	auditSink := bff.NewMemoryAudit()
	principal := bff.Principal{PlayerID: "p1", SessionID: "s1", Username: "a"}
	identity.SeedSession("tok", principal)
	srv := bff.NewServer(bff.Dependencies{
		Identity:         identity,
		Room:             room,
		Tournament:       bff.NewFakeTournament(),
		Reads:            &bff.FakeReads{},
		Spectator:        bff.NewFakeSpectatorGate(),
		Audit:            auditSink,
		Ready:            true,
		PrincipalLimiter: bff.NewMemoryRateLimiter(1, time.Minute),
		Clock:            func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
	})
	do := func(cmdID string) *httptest.ResponseRecorder {
		b := []byte(`{"commandId":"` + cmdID + `","type":"PlayCard","expectedSequenceNumber":4,"schemaVersion":1,"payload":{"roomId":"room_z"}}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/commands", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer tok")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(correlation.HeaderCorrelationID, "corr_rl")
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		return w
	}
	if w := do("cmd_a"); w.Code != http.StatusOK {
		t.Fatalf("first=%d %s", w.Code, w.Body.String())
	}
	w := do("cmd_b")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second=%d %s", w.Code, w.Body.String())
	}
	if room.DispatchCount() != 1 {
		t.Fatalf("dispatch=%d", room.DispatchCount())
	}
	if auditSink.Len() != 1 {
		t.Fatalf("audit=%d", auditSink.Len())
	}
	rec := auditSink.Records()[0]
	if rec.CommandID != "cmd_b" || rec.PlayerID != "p1" || rec.SessionID != "s1" || rec.RoomID != "room_z" {
		t.Fatalf("audit record=%+v", rec)
	}
	if rec.Reason != "rate_limited" {
		t.Fatalf("reason=%q", rec.Reason)
	}
	if rec.SubmittedSequence == nil || *rec.SubmittedSequence != 4 {
		t.Fatalf("submitted sequence=%v", rec.SubmittedSequence)
	}
}

func TestJSONLAudit_AppendOnlyFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rejections.jsonl")
	sink, err := bff.OpenJSONLAudit(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	at := time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
	rec := audit.NewRejection("cmd_jsonl", "corr_1", "s1", "p1", "unknown_command_type", at)
	if err := sink.RecordRejection(rec); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"commandId":"cmd_jsonl"`) {
		t.Fatalf("file=%s", raw)
	}
	if !bytes.HasSuffix(raw, []byte("\n")) {
		t.Fatal("jsonl must end with newline")
	}

	broken := bff.NewJSONLAudit(errWriter{})
	if err := broken.RecordRejection(rec); err == nil {
		t.Fatal("write failure must fail closed")
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) / 2, nil // nil error, short write
}

func TestJSONLAudit_RejectsNilErrorShortWrite(t *testing.T) {
	at := time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
	rec := audit.NewRejection("cmd_short", "corr_1", "s1", "p1", "unknown_command_type", at)
	sink := bff.NewJSONLAudit(shortWriter{})
	err := sink.RecordRejection(rec)
	if err == nil {
		t.Fatal("nil-error short write must fail closed")
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("want ErrShortWrite, got %v", err)
	}
}

func TestInternal_EmptyBodyControlRejected(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                    h.identity,
		Hub:                         h.hub,
		Ready:                       true,
		IdentityProducerCredential:  "ident-cred",
		RoomProducerCredential:      "room-cred",
		SpectatorProducerCredential: "spec-cred",
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", nil)
	req.Header.Set("X-Service-Credential", "ident-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty session body status=%d body=%s", w.Code, w.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/rooms/room_1/terminal", nil)
	req2.Header.Set("X-Service-Credential", "room-cred")
	w2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("empty room body status=%d body=%s", w2.Code, w2.Body.String())
	}
}

func TestInternal_ProducerScopesNoCrossKindInjection(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                    h.identity,
		Hub:                         h.hub,
		Ready:                       true,
		IdentityProducerCredential:  "ident-cred",
		RoomProducerCredential:      "room-cred",
		SpectatorProducerCredential: "spec-cred",
	})

	playerBody := []byte(`{"schemaVersion":1,"eventId":"p1","stream":"player","roomId":"r1","sessionId":"s1","playerId":"pl1","sequence":1,"event":"CardDealt","data":{}}`)
	specBody := []byte(`{"schemaVersion":1,"eventId":"s1","stream":"spectator","roomId":"r1","sequence":1,"event":"tick","data":{}}`)

	// Spectator cred cannot inject player-private events.
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(playerBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "spec-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("spectator→player status=%d", w.Code)
	}

	// Identity cred cannot inject spectator stream events.
	req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(specBody))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Service-Credential", "ident-cred")
	w2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("identity→spectator status=%d", w2.Code)
	}

	// Room cred can publish player + spectator-safe; spectator cred spectator only.
	req3 := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(playerBody))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("X-Service-Credential", "room-cred")
	w3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Fatalf("room→player status=%d %s", w3.Code, w3.Body.String())
	}

	req4 := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(specBody))
	req4.Header.Set("Content-Type", "application/json")
	req4.Header.Set("X-Service-Credential", "spec-cred")
	w4 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Fatalf("spec→spectator status=%d %s", w4.Code, w4.Body.String())
	}

	// Room cannot use identity session control; identity cannot close rooms.
	term := []byte(`{"schemaVersion":1,"eventId":"t1","eventType":"RoomTerminal","roomId":"r1"}`)
	req5 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/rooms/r1/terminal", bytes.NewReader(term))
	req5.Header.Set("Content-Type", "application/json")
	req5.Header.Set("X-Service-Credential", "ident-cred")
	w5 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w5, req5)
	if w5.Code != http.StatusUnauthorized {
		t.Fatalf("identity→terminal status=%d", w5.Code)
	}

	inv := []byte(`{"schemaVersion":1,"eventId":"i1","eventType":"SessionInvalidated","sessionId":"session_1","playerId":"p1","reason":"login"}`)
	req6 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(inv))
	req6.Header.Set("Content-Type", "application/json")
	req6.Header.Set("X-Service-Credential", "room-cred")
	w6 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w6, req6)
	if w6.Code != http.StatusUnauthorized {
		t.Fatalf("room→session status=%d", w6.Code)
	}
}

func TestInternal_StrictlyIncreasingSequenceRejectsBeforeBroadcast(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                    h.identity,
		Hub:                         h.hub,
		Ready:                       true,
		RoomProducerCredential:      "room-cred",
		SpectatorProducerCredential: "spec-cred",
	})
	_, ch, _, cancel, err := h.hub.Subscribe(bff.StreamSpectator, "room_seq", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	post := func(eventID string, seq int64) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{
			"schemaVersion": 1,
			"eventId":       eventID,
			"stream":        "spectator",
			"roomId":        "room_seq",
			"sequence":      seq,
			"event":         "tick",
			"data":          map[string]any{},
		})
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Service-Credential", "room-cred")
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		return w
	}

	if w := post("e1", 2); w.Code != http.StatusOK {
		t.Fatalf("first status=%d %s", w.Code, w.Body.String())
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected first broadcast")
	}

	// Non-increasing rejected before broadcast.
	if w := post("e2", 2); w.Code != http.StatusConflict {
		t.Fatalf("non-increasing status=%d %s", w.Code, w.Body.String())
	}
	select {
	case ev := <-ch:
		t.Fatalf("non-increasing must not broadcast: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	// Exact eventId duplicate is stable (no second broadcast).
	dup := post("e1", 2)
	if dup.Code != http.StatusOK || !strings.Contains(dup.Body.String(), `"duplicate":true`) {
		t.Fatalf("duplicate body=%s", dup.Body.String())
	}
	select {
	case ev := <-ch:
		t.Fatalf("duplicate must not re-broadcast: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	// Conflicting sequence for same eventId rejected.
	if w := post("e1", 3); w.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d %s", w.Code, w.Body.String())
	}
	select {
	case ev := <-ch:
		t.Fatalf("conflict must not broadcast: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	if w := post("e3", 3); w.Code != http.StatusOK {
		t.Fatalf("increasing status=%d %s", w.Code, w.Body.String())
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected increasing broadcast")
	}
}

func TestSnapshot_PlayerAndSpectatorRoutes(t *testing.T) {
	h := newHarness(t)
	h.room.SnapshotJSON = json.RawMessage(`{"roomId":"room_1","playerId":"player_1","hand":["red-7"],"schemaVersion":1}`)
	h.spectator.SnapshotJSON = json.RawMessage(`{"roomId":"room_1","topDiscard":"blue-3","schemaVersion":1}`)

	w := h.do(http.MethodGet, "/v1/rooms/room_1/snapshot", nil, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("player snapshot status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"hand"`) {
		t.Fatalf("player snapshot body=%s", w.Body.String())
	}
	if h.room.LastSnapshotRoomID != "room_1" || h.room.LastSnapshotPlayerID != "player_1" {
		t.Fatalf("room snapshot args room=%q player=%q", h.room.LastSnapshotRoomID, h.room.LastSnapshotPlayerID)
	}

	w2 := h.do(http.MethodGet, "/v1/spectator/rooms/room_1/snapshot", nil, nil)
	if w2.Code != http.StatusOK {
		t.Fatalf("spectator snapshot status=%d body=%s", w2.Code, w2.Body.String())
	}
	if !strings.Contains(w2.Body.String(), `"topDiscard"`) {
		t.Fatalf("spectator snapshot body=%s", w2.Body.String())
	}

	h.spectator.Deny("room_denied", "room_terminal")
	w3 := h.do(http.MethodGet, "/v1/spectator/rooms/room_denied/snapshot", nil, nil)
	if w3.Code != http.StatusForbidden {
		t.Fatalf("denied spectator snapshot status=%d", w3.Code)
	}

	w4 := h.do(http.MethodGet, "/v1/rooms/room_1/snapshot", nil, nil)
	if w4.Code != http.StatusUnauthorized {
		t.Fatalf("unauth player snapshot status=%d", w4.Code)
	}
}

func TestFinding_SpectatorPrivate_ParticipantAllowed(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	h.spectator.AllowParticipant("room_priv", h.principal.PlayerID)

	w := h.do(http.MethodGet, "/v1/spectator/rooms/room_priv/snapshot", nil, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("participant status=%d body=%s", w.Code, w.Body.String())
	}
	if h.spectator.LastReq.Principal == nil || h.spectator.LastReq.Principal.PlayerID != h.principal.PlayerID {
		t.Fatalf("gate principal=%+v", h.spectator.LastReq.Principal)
	}
}

func TestFinding_SpectatorPrivate_UnrelatedAuthenticatedDenied(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	h.spectator.AllowParticipant("room_priv", "other_player")

	w := h.do(http.MethodGet, "/v1/spectator/rooms/room_priv/snapshot", nil, h.authHeaders())
	if w.Code != http.StatusForbidden {
		t.Fatalf("unrelated auth status=%d body=%s want 403", w.Code, w.Body.String())
	}
}

func TestFinding_SpectatorPrivate_ValidInviteAllowed(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	h.spectator.AllowInvite("room_priv", "invite-opaque-ok")

	w := h.do(http.MethodGet, "/v1/spectator/rooms/room_priv/snapshot", nil, map[string]string{
		"X-Room-Invite": "invite-opaque-ok",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("valid invite status=%d body=%s", w.Code, w.Body.String())
	}
	if h.spectator.LastReq.InviteCapability != "invite-opaque-ok" {
		t.Fatalf("invite capability not forwarded: %+v", h.spectator.LastReq)
	}
}

func TestFinding_SpectatorPrivate_InvalidInviteDenied(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	h.spectator.AllowInvite("room_priv", "invite-opaque-ok")

	w := h.do(http.MethodGet, "/v1/spectator/rooms/room_priv/snapshot", nil, map[string]string{
		"X-Room-Invite": "invite-bogus",
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("invalid invite status=%d body=%s want 403", w.Code, w.Body.String())
	}
}

func TestFinding_SpectatorPrivate_OperatorAllowed(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	op := bff.Principal{
		PlayerID:      "ops_1",
		SessionID:     "ops_sess",
		Username:      "ops",
		Roles:         []string{"operator"},
		OperatorScope: true,
	}
	h.identity.SeedSession("tok_ops", op)

	w := h.do(http.MethodGet, "/v1/spectator/rooms/room_priv/snapshot", nil, map[string]string{
		"Authorization": "Bearer tok_ops",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("operator status=%d body=%s", w.Code, w.Body.String())
	}
	if h.spectator.LastReq.Principal == nil || !h.spectator.LastReq.Principal.OperatorScope {
		t.Fatalf("operator scope must propagate from Identity: %+v", h.spectator.LastReq.Principal)
	}
}

func TestFinding_SpectatorPrivate_ClientOperatorBooleanIgnored(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")

	// Raw client claim must never grant operator admission.
	w := h.do(http.MethodGet, "/v1/spectator/rooms/room_priv/snapshot", nil, map[string]string{
		"X-Operator-Scope": "1",
		"operator":         "true",
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("client operator boolean status=%d want 403", w.Code)
	}
}

func TestFinding_HTTPIdentity_ParsesOperatorScope(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/validate", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"playerId":  "ops_1",
			"sessionId": "ops_sess",
			"username":  "ops",
			"roles":     []string{"player"},
			"scopes":    []string{"operator"},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := bff.NewHTTPIdentityClient(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "cred",
		HTTPClient:        srv.Client(),
	})
	p, err := client.ValidateSession(context.Background(), "tok", correlation.Headers{CorrelationID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if !p.OperatorScope {
		t.Fatalf("expected OperatorScope from Identity scopes: %+v", p)
	}
	if len(p.Scopes) != 1 || p.Scopes[0] != "operator" {
		t.Fatalf("scopes=%v", p.Scopes)
	}
}

func TestFinding_HTTPSpectatorGate_ForwardsInviteAndOperator(t *testing.T) {
	var gotBody map[string]any
	var gotInvite, gotAuth string
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/rooms/room_priv/spectator-admission", func(w http.ResponseWriter, r *http.Request) {
		gotInvite = r.Header.Get("X-Room-Invite")
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"allowed": true})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	gate := bff.NewHTTPSpectatorGate(bff.HTTPClientConfig{
		BaseURL:           srv.URL,
		ServiceCredential: "cred",
		HTTPClient:        srv.Client(),
	})
	principal := &bff.Principal{
		PlayerID:      "ops_1",
		SessionID:     "ops_sess",
		OperatorScope: true,
	}
	ok, _, err := gate.Admit(context.Background(), bff.SpectatorAdmitRequest{
		RoomID:           "room_priv",
		Token:            "tok",
		Principal:        principal,
		InviteCapability: "invite-opaque",
		Correlation:      correlation.Headers{CorrelationID: "c1"},
	})
	if err != nil || !ok {
		t.Fatalf("admit ok=%v err=%v", ok, err)
	}
	if gotInvite != "invite-opaque" {
		t.Fatalf("X-Room-Invite=%q", gotInvite)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("Authorization=%q", gotAuth)
	}
	if gotBody["authorized"] != nil {
		t.Fatalf("must not send blanket authorized: %+v", gotBody)
	}
	if gotBody["operator"] != true {
		t.Fatalf("operator from Identity must be sent: %+v", gotBody)
	}
	if _, ok := gotBody["inviteCapability"]; ok {
		t.Fatalf("must not convert opaque invite to client boolean: %+v", gotBody)
	}
	if gotBody["playerId"] != "ops_1" || gotBody["sessionId"] != "ops_sess" {
		t.Fatalf("body=%+v", gotBody)
	}
}

func TestFinding_RoomIngest_TerminalSpectatorFamiliesPublishThenClose(t *testing.T) {
	families := []string{"RoomCompleted", "RoomCancelled", "SpectatorStreamsClose"}
	for _, family := range families {
		t.Run(family, func(t *testing.T) {
			h := newHarness(t)
			srv := bff.NewServer(bff.Dependencies{
				Identity:               h.identity,
				Spectator:              h.spectator,
				Hub:                    h.hub,
				Ready:                  true,
				RoomProducerCredential: "room-cred",
			})
			roomID := "room_" + family
			_, ch, _, cancel, err := h.hub.Subscribe(bff.StreamSpectator, roomID, "", "", "")
			if err != nil {
				t.Fatal(err)
			}
			defer cancel()

			body, _ := json.Marshal(map[string]any{
				"schemaVersion": 1,
				"eventId":       "term_" + family,
				"stream":        "spectator",
				"roomId":        roomID,
				"sequence":      1,
				"event":         family,
				"data":          map[string]any{"roomId": roomID},
			})
			req := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Service-Credential", "room-cred")
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("ingest status=%d body=%s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), `"duplicate":true`) {
				t.Fatalf("first ingest must not be duplicate: %s", w.Body.String())
			}

			gotFinal := false
			deadline := time.After(2 * time.Second)
		drain:
			for {
				select {
				case ev, ok := <-ch:
					if !ok {
						break drain
					}
					if ev.Event == family {
						gotFinal = true
					}
				case <-deadline:
					t.Fatal("timed out waiting for stream close after terminal ingest")
				}
			}
			if !gotFinal {
				t.Fatalf("must publish final %s before close", family)
			}
			if !h.hub.IsRoomTerminal(roomID) {
				t.Fatal("room must be terminal after ingest")
			}

			wDeny := h.do(http.MethodGet, "/v1/streams/spectator?roomId="+roomID, nil, nil)
			if wDeny.Code != http.StatusForbidden {
				t.Fatalf("new subscribe status=%d body=%s want 403", wDeny.Code, wDeny.Body.String())
			}

			// Exact eventId+sequence duplicate is stable and does not reopen.
			w2 := httptest.NewRecorder()
			req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/streams/events", bytes.NewReader(body))
			req2.Header.Set("Content-Type", "application/json")
			req2.Header.Set("X-Service-Credential", "room-cred")
			srv.Handler().ServeHTTP(w2, req2)
			if w2.Code != http.StatusOK || !strings.Contains(w2.Body.String(), `"duplicate":true`) {
				t.Fatalf("idempotent status=%d body=%s", w2.Code, w2.Body.String())
			}
			if !h.hub.IsRoomTerminal(roomID) {
				t.Fatal("duplicate must leave room terminal")
			}
			_, _, _, cancel2, err2 := h.hub.Subscribe(bff.StreamSpectator, roomID, "", "", "")
			if cancel2 != nil {
				cancel2()
			}
			if err2 == nil || !strings.Contains(err2.Error(), "room_terminal") {
				t.Fatalf("subscribe after duplicate err=%v", err2)
			}
		})
	}
}

func TestFinding_SSE_PrivateParticipantInviteOperatorCancellable(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	h.spectator.AllowParticipant("room_priv", h.principal.PlayerID)
	h.spectator.AllowInvite("room_priv", "invite-opaque-ok")

	srv := httptest.NewServer(h.srv.Handler())
	t.Cleanup(srv.Close)
	client := &http.Client{Timeout: 0}

	openSSE := func(t *testing.T, headers map[string]string) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/streams/spectator?roomId=room_priv", nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			t.Fatalf("status=%d body=%s", resp.StatusCode, body)
		}
		deadline := time.Now().Add(2 * time.Second)
		for h.hub.ActiveCount() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if h.hub.ActiveCount() == 0 {
			t.Fatal("subscribe did not register")
		}
		cancel()
	}

	openSSE(t, map[string]string{"Authorization": "Bearer " + h.token})
	if h.spectator.LastReq.Principal == nil || h.spectator.LastReq.Principal.PlayerID != h.principal.PlayerID {
		t.Fatalf("participant principal=%+v", h.spectator.LastReq.Principal)
	}

	openSSE(t, map[string]string{"X-Room-Invite": "invite-opaque-ok"})
	if h.spectator.LastReq.InviteCapability != "invite-opaque-ok" {
		t.Fatalf("invite not forwarded: %+v", h.spectator.LastReq)
	}

	op := bff.Principal{
		PlayerID:      "ops_1",
		SessionID:     "ops_sess",
		Username:      "ops",
		Roles:         []string{"operator"},
		OperatorScope: true,
	}
	h.identity.SeedSession("tok_ops", op)
	openSSE(t, map[string]string{"Authorization": "Bearer tok_ops"})
	if h.spectator.LastReq.Principal == nil || !h.spectator.LastReq.Principal.OperatorScope {
		t.Fatalf("operator scope=%+v", h.spectator.LastReq.Principal)
	}

	// Denied paths stay non-streaming (no hang via recorder).
	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_priv", nil, map[string]string{
		"X-Room-Invite": "invite-bogus",
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("invalid invite status=%d", w.Code)
	}
	w2 := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_priv", nil, map[string]string{
		"X-Operator-Scope": "1",
		"operator":         "true",
	})
	if w2.Code != http.StatusForbidden {
		t.Fatalf("spoofed operator status=%d", w2.Code)
	}
}

func TestFinding_ControlIdempotencyBindsEventIDAtomically(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:                   h.identity,
		Room:                       h.room,
		Tournament:                 h.tournament,
		Reads:                      &bff.FakeReads{},
		Spectator:                  h.spectator,
		Audit:                      h.audit,
		Hub:                        h.hub,
		Ready:                      true,
		IdentityProducerCredential: "ident-cred",
		RoomProducerCredential:     "room-cred",
	})
	handler := srv.Handler()

	_, ch, _, cancel, err := h.hub.Subscribe(bff.StreamControl, "", h.principal.SessionID, h.principal.PlayerID, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	body := map[string]any{
		"schemaVersion": 1,
		"eventId":       "inv-atomic",
		"eventType":     "SessionInvalidated",
		"sessionId":     h.principal.SessionID,
		"playerId":      h.principal.PlayerID,
		"reason":        "logout",
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/"+h.principal.SessionID+"/invalidated", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "ident-cred")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	select {
	case ev := <-ch:
		if ev.Event != "session_invalidated" {
			t.Fatalf("event=%s", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("first invalidation must close control stream")
	}

	// Exact replay: duplicate, no second invalidation fanout.
	_, ch2, _, cancel2, err := h.hub.Subscribe(bff.StreamControl, "", "session_other", "player_other", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel2()
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/"+h.principal.SessionID+"/invalidated", bytes.NewReader(raw))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Service-Credential", "ident-cred")
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK || !strings.Contains(w2.Body.String(), `"duplicate":true`) {
		t.Fatalf("exact replay status=%d body=%s", w2.Code, w2.Body.String())
	}
	select {
	case <-ch2:
		t.Fatal("exact duplicate must not apply a second invalidation side effect")
	case <-time.After(50 * time.Millisecond):
	}

	// Same eventId, different session target: conflict, no invalidation of other session.
	conflict := map[string]any{
		"schemaVersion": 1,
		"eventId":       "inv-atomic",
		"eventType":     "SessionInvalidated",
		"sessionId":     "session_other",
		"playerId":      "player_other",
		"reason":        "logout",
	}
	craw, _ := json.Marshal(conflict)
	w3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_other/invalidated", bytes.NewReader(craw))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("X-Service-Credential", "ident-cred")
	handler.ServeHTTP(w3, req3)
	if w3.Code != http.StatusConflict {
		t.Fatalf("rebind conflict status=%d body=%s", w3.Code, w3.Body.String())
	}
	if h.hub.IsSessionInvalidated("session_other") {
		t.Fatal("conflict must not invalidate the rebound target")
	}
	select {
	case <-ch2:
		t.Fatal("conflict must not close unrelated session streams")
	case <-time.After(50 * time.Millisecond):
	}

	// Missing/wrong eventType rejected.
	badType := []byte(`{"schemaVersion":1,"eventId":"inv-type","eventType":"Other","sessionId":"` + h.principal.SessionID + `","playerId":"p","reason":"x"}`)
	w4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/"+h.principal.SessionID+"/invalidated", bytes.NewReader(badType))
	req4.Header.Set("Content-Type", "application/json")
	req4.Header.Set("X-Service-Credential", "ident-cred")
	handler.ServeHTTP(w4, req4)
	if w4.Code != http.StatusBadRequest {
		t.Fatalf("eventType status=%d body=%s", w4.Code, w4.Body.String())
	}
}

func TestFinding_ControlCrossTargetConflictAndConcurrency(t *testing.T) {
	hub := bff.NewHub()
	closed, dup, err := hub.ApplySessionInvalidation("shared-id", "sess_a")
	if err != nil || dup || closed != 0 {
		t.Fatalf("first bind closed=%d dup=%v err=%v", closed, dup, err)
	}
	_, _, err = hub.ApplyRoomTerminal("shared-id", "room_a")
	if err == nil || !errors.Is(err, bff.ErrControlConflict) {
		t.Fatalf("cross-type rebound must conflict, err=%v", err)
	}
	if hub.IsRoomTerminal("room_a") {
		t.Fatal("conflict must not mark room terminal")
	}

	// Concurrent exact duplicates: one applies, rest are stable duplicates.
	var wg sync.WaitGroup
	results := make(chan string, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, dup, err := hub.ApplySessionInvalidation("conc-inv", "sess_conc")
			if err != nil {
				results <- "err:" + err.Error()
				return
			}
			if dup {
				results <- "dup"
				return
			}
			results <- "new"
		}()
	}
	wg.Wait()
	close(results)
	news, dups := 0, 0
	for r := range results {
		switch {
		case strings.HasPrefix(r, "err:"):
			t.Fatal(r)
		case r == "new":
			news++
		case r == "dup":
			dups++
		}
	}
	if news != 1 || dups != 31 {
		t.Fatalf("concurrent bind news=%d dups=%d", news, dups)
	}
	if !hub.IsSessionInvalidated("sess_conc") {
		t.Fatal("session must be invalidated exactly once")
	}

	var wg2 sync.WaitGroup
	termResults := make(chan string, 64)
	for i := 0; i < 32; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			_, dup, err := hub.ApplyRoomTerminal("conc-term", "room_conc")
			if err != nil {
				termResults <- "err:" + err.Error()
				return
			}
			if dup {
				termResults <- "dup"
				return
			}
			termResults <- "new"
		}()
	}
	wg2.Wait()
	close(termResults)
	news, dups = 0, 0
	for r := range termResults {
		switch {
		case strings.HasPrefix(r, "err:"):
			t.Fatal(r)
		case r == "new":
			news++
		case r == "dup":
			dups++
		}
	}
	if news != 1 || dups != 31 {
		t.Fatalf("concurrent room terminal news=%d dups=%d", news, dups)
	}
	if !hub.IsRoomTerminal("room_conc") {
		t.Fatal("room must be terminal exactly once")
	}
}

func TestFinding_NilAuditDefaultsFailClosed(t *testing.T) {
	identity := bff.NewFakeIdentity()
	principal := bff.Principal{PlayerID: "p1", SessionID: "s1", Username: "u"}
	identity.SeedSession("tok", principal)
	srv := bff.NewServer(bff.Dependencies{
		Identity: identity,
		Ready:    true,
		// Audit nil -> ClosedAudit
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/commands", bytes.NewReader([]byte(
		`{"commandId":"c2","type":"JoinRoom","schemaVersion":1,"payload":{"roomId":"r1"}}`,
	)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok")
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil audit must fail closed on rejection audit, status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "audit") {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestFinding_ReadyFalseGatesPublicTraffic(t *testing.T) {
	srv := bff.NewServer(bff.Dependencies{
		Ready:          false,
		NotReadyReason: "redis_adapter_blocked",
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/rankings/leaderboards", nil)
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("public traffic status=%d want 503", w.Code)
	}
	ready := httptest.NewRecorder()
	srv.Handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready status=%d", ready.Code)
	}
	health := httptest.NewRecorder()
	srv.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("/health must remain up, status=%d", health.Code)
	}
}

func TestFinding_LastIngestStableDuplicateAfterIdempotencyEviction(t *testing.T) {
	hub := bff.NewHub()
	hub.SetIngestIdempotencyBound(1)
	ev := bff.StreamEvent{Event: "tick", Data: json.RawMessage(`{}`), SchemaVersion: 1}
	if _, err := hub.IngestToStream(bff.StreamSpectator, "r1", "", "", "e1", 1, ev); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.IngestToStream(bff.StreamSpectator, "r1", "", "", "e2", 2, ev); err != nil {
		t.Fatal(err)
	}
	res, err := hub.IngestToStream(bff.StreamSpectator, "r1", "", "", "e2", 2, ev)
	if err != nil {
		t.Fatalf("exact last after eviction must be duplicate, err=%v", err)
	}
	if !res.Duplicate {
		t.Fatal("expected duplicate")
	}
}

func TestFinding_FullSubscriberChannelClosesWithResync(t *testing.T) {
	hub := bff.NewHub()
	_, ch, _, cancel, err := hub.Subscribe(bff.StreamSpectator, "r1", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	for i := 0; i < 16; i++ {
		hub.PublishToStream(bff.StreamSpectator, "r1", "", "", bff.StreamEvent{
			Event: "fill", Data: json.RawMessage(`{}`), SchemaVersion: 1,
		})
	}
	hub.PublishToStream(bff.StreamSpectator, "r1", "", "", bff.StreamEvent{
		Event: "overflow", Data: json.RawMessage(`{}`), SchemaVersion: 1,
	})
	sawResync := false
	closed := false
	deadline := time.After(time.Second)
	for !closed {
		select {
		case ev, ok := <-ch:
			if !ok {
				closed = true
				break
			}
			if ev.Event == "snapshot_required" {
				sawResync = true
			}
		case <-deadline:
			t.Fatal("expected channel close/resync after full buffer")
		}
	}
	if !sawResync {
		t.Fatal("expected snapshot_required resync signal before close")
	}
}
