package bff_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"unoarena/services/gateway/bff"
	"unoarena/shared/correlation"
	"unoarena/shared/envelope"
)

type harness struct {
	srv        *bff.Server
	identity   *bff.FakeIdentity
	room       *bff.FakeRoom
	tournament *bff.FakeTournament
	audit      *bff.MemoryAudit
	spectator  *bff.FakeSpectatorGate
	hub        *bff.Hub
	token      string
	principal  bff.Principal
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	identity := bff.NewFakeIdentity()
	room := bff.NewFakeRoom()
	tournament := bff.NewFakeTournament()
	audit := bff.NewMemoryAudit()
	spectator := bff.NewFakeSpectatorGate()
	hub := bff.NewHub()
	principal := bff.Principal{
		PlayerID:  "player_1",
		SessionID: "session_1",
		Username:  "alice",
	}
	token := "tok_alice"
	identity.SeedSession(token, principal)
	srv := bff.NewServer(bff.Dependencies{
		Identity:   identity,
		Room:       room,
		Tournament: tournament,
		Reads:      &bff.FakeReads{},
		Audit:      audit,
		Spectator:  spectator,
		Hub:        hub,
		Ready:      true,
		Clock:      func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
		NewID:      func(prefix string) string { return prefix + "fixed" },
	})
	return &harness{
		srv:        srv,
		identity:   identity,
		room:       room,
		tournament: tournament,
		audit:      audit,
		spectator:  spectator,
		hub:        hub,
		token:      token,
		principal:  principal,
	}
}

func (h *harness) do(method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.srv.Handler().ServeHTTP(w, req)
	return w
}

func (h *harness) authHeaders() map[string]string {
	return map[string]string{
		"Authorization":                 "Bearer " + h.token,
		correlation.HeaderCorrelationID: "corr_test",
		"Content-Type":                  "application/json",
	}
}

func TestRouting_CreateRoomToRoomBackend(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_create","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if h.room.DispatchCount() != 1 {
		t.Fatalf("room dispatch=%d", h.room.DispatchCount())
	}
	if h.tournament.DispatchCount() != 0 {
		t.Fatalf("tournament should not receive CreateRoom")
	}
	var res envelope.Result
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Status != envelope.StatusAccepted || res.Type != "CreateRoom" {
		t.Fatalf("result=%+v", res)
	}
}

func TestCommandIdentityDependencyFailureIsNotFalseUnauthorized(t *testing.T) {
	h := newHarness(t)
	h.identity.FailNext = true
	body := []byte(`{"commandId":"cmd_identity_down","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatalf("identity failure must stop before dispatch, got %d", h.room.DispatchCount())
	}
}

func TestRouting_PlayCardRoomScoped(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_play","type":"PlayCard","expectedSequenceNumber":3,"schemaVersion":1,"payload":{"roomId":"room_9","cardId":"red-7"}}`)
	w := h.do(http.MethodPost, "/v1/rooms/room_9/commands", body, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if h.room.DispatchCount() != 1 {
		t.Fatalf("room dispatch=%d", h.room.DispatchCount())
	}
	got := h.room.Dispatched[0]
	if got.RoomID != "room_9" {
		t.Fatalf("RoomID=%q", got.RoomID)
	}
	if got.Principal.PlayerID != h.principal.PlayerID {
		t.Fatalf("principal=%+v", got.Principal)
	}
	if got.Correlation.CorrelationID != "corr_test" {
		t.Fatalf("correlation=%+v", got.Correlation)
	}
}

func TestRouting_TournamentCommands(t *testing.T) {
	h := newHarness(t)
	cases := []string{"CreateTournament", "RegisterPlayer", "CloseRegistration"}
	for _, typ := range cases {
		body, _ := json.Marshal(map[string]any{
			"commandId":     "cmd_" + typ,
			"type":          typ,
			"schemaVersion": 1,
			"payload":       map[string]string{"tournamentId": "t1"},
		})
		w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
		if w.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", typ, w.Code, w.Body.String())
		}
	}
	if h.tournament.DispatchCount() != 3 {
		t.Fatalf("tournament dispatch=%d", h.tournament.DispatchCount())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatalf("room should not receive tournament commands")
	}
}

func TestRouting_AllRoomCatalogTypes(t *testing.T) {
	h := newHarness(t)
	types := []string{
		"JoinRoom", "LeaveRoom", "LockRoom", "StartMatch", "CancelRoom",
		"PlayCard", "DrawCard", "ChooseColor", "CallUno", "ReportMissingUno", "ReconnectToRoom",
	}
	for i, typ := range types {
		body, _ := json.Marshal(map[string]any{
			"commandId":              "cmd_room_" + typ,
			"type":                   typ,
			"expectedSequenceNumber": i + 1,
			"schemaVersion":          1,
			"payload":                map[string]string{"roomId": "room_1"},
		})
		w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
		if w.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", typ, w.Code, w.Body.String())
		}
	}
	if h.room.DispatchCount() != len(types) {
		t.Fatalf("room dispatch=%d want %d", h.room.DispatchCount(), len(types))
	}
}

func TestEnvelope_UnknownTypeHTTP400NoDispatchNoAudit(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_x","type":"NotARealCommand","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_envelope") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "unknown command type") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if h.room.DispatchCount()+h.tournament.DispatchCount() != 0 {
		t.Fatal("unknown type must not dispatch")
	}
	if h.audit.Len() != 0 {
		t.Fatal("unknown type is malformed envelope, not rejection audit")
	}
}

func TestEnvelope_MissingExpectedSequenceHTTP400(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_stale","type":"PlayCard","schemaVersion":1,"payload":{"roomId":"room_1"}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_envelope") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "expectedSequenceNumber") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("must not dispatch without expectedSequenceNumber")
	}
	if h.audit.Len() != 0 {
		t.Fatal("missing sequence is malformed envelope, not rejection audit")
	}
}

func TestRejection_CreateRoomDoesNotRequireSequence(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_cr","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if h.room.DispatchCount() != 1 {
		t.Fatal("CreateRoom should dispatch without sequence")
	}
	if h.audit.Len() != 0 {
		t.Fatal("no audit on accept")
	}
}

func TestRejection_BackendRejectedAudited(t *testing.T) {
	h := newHarness(t)
	seq := int64(7)
	h.room.Results["cmd_rej"] = envelope.Rejected("cmd_rej", "PlayCard", "room_full", &seq)
	body := []byte(`{"commandId":"cmd_rej","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":"room_2"}}`)
	w := h.do(http.MethodPost, "/v1/rooms/room_2/commands", body, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	var res envelope.Result
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Status != envelope.StatusRejected || res.Reason != "room_full" {
		t.Fatalf("result=%+v", res)
	}
	if h.room.DispatchCount() != 1 {
		t.Fatal("backend rejection still dispatched once")
	}
	if h.audit.Len() != 1 || h.audit.Records()[0].Reason != "room_full" {
		t.Fatalf("audit=%v", h.audit.Records())
	}
	rec := h.audit.Records()[0]
	if rec.CurrentSequence == nil || *rec.CurrentSequence != 7 {
		t.Fatalf("audit must include backend known sequence: %+v", rec)
	}
	if rec.SubmittedSequence == nil || *rec.SubmittedSequence != 1 {
		t.Fatalf("audit submitted sequence: %+v", rec)
	}
}

func TestRejection_InvalidEnvelopeNoDispatch(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"","type":"PlayCard","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("invalid envelope must not dispatch")
	}
	if h.audit.Len() != 0 {
		t.Fatal("envelope shape errors are HTTP 400, not command rejection audit")
	}
}

func TestRejection_CommandIdHeaderMismatch(t *testing.T) {
	h := newHarness(t)
	headers := h.authHeaders()
	headers[correlation.HeaderCommandID] = "other"
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, headers)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", w.Code)
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("mismatch must not dispatch")
	}
}

func TestRejection_RoomIdMismatch(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_m","type":"JoinRoom","expectedSequenceNumber":0,"schemaVersion":1,"payload":{"roomId":"room_a"}}`)
	w := h.do(http.MethodPost, "/v1/rooms/room_b/commands", body, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	var res envelope.Result
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Reason != "room_id_mismatch" {
		t.Fatalf("result=%+v", res)
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("mismatch must not dispatch")
	}
}

func TestAuth_CommandsRequireBearer(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, map[string]string{"Content-Type": "application/json"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("unauthenticated must not dispatch")
	}
}

func TestAuth_InvalidSession(t *testing.T) {
	h := newHarness(t)
	headers := h.authHeaders()
	headers["Authorization"] = "Bearer dead"
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, headers)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestAuth_RegisterLoginWhoami(t *testing.T) {
	h := newHarness(t)
	reg := h.do(http.MethodPost, "/v1/auth/register", []byte(`{"username":"bob","password":"secret"}`), map[string]string{"Content-Type": "application/json"})
	if reg.Code != http.StatusOK {
		t.Fatalf("register=%d %s", reg.Code, reg.Body.String())
	}
	login := h.do(http.MethodPost, "/v1/auth/login", []byte(`{"username":"bob","password":"secret"}`), map[string]string{"Content-Type": "application/json"})
	if login.Code != http.StatusOK {
		t.Fatalf("login=%d %s", login.Code, login.Body.String())
	}
	var loginBody map[string]string
	_ = json.Unmarshal(login.Body.Bytes(), &loginBody)
	who := h.do(http.MethodGet, "/v1/auth/whoami", nil, map[string]string{"Authorization": "Bearer " + loginBody["token"]})
	if who.Code != http.StatusOK {
		t.Fatalf("whoami=%d %s", who.Code, who.Body.String())
	}
}

func TestAuth_LogoutRevokesBearerAndRequiresAuthentication(t *testing.T) {
	h := newHarness(t)
	missing := h.do(http.MethodPost, "/v1/auth/logout", nil, nil)
	if missing.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer status=%d body=%s", missing.Code, missing.Body.String())
	}

	w := h.do(http.MethodPost, "/v1/auth/logout", nil, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("logout status=%d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), h.token) || strings.Contains(w.Body.String(), h.principal.SessionID) {
		t.Fatalf("logout response exposed session material: %s", w.Body.String())
	}
	if h.identity.LastCorr.CorrelationID != "corr_test" {
		t.Fatalf("logout correlation=%+v", h.identity.LastCorr)
	}

	w = h.do(http.MethodGet, "/v1/auth/whoami", nil, h.authHeaders())
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("copied token must be rejected after logout, status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestCorrelationHeadersPropagatedOnCommand(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_c","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	headers := h.authHeaders()
	headers[correlation.HeaderCorrelationID] = "corr_propagate"
	w := h.do(http.MethodPost, "/v1/commands", body, headers)
	if w.Header().Get(correlation.HeaderCorrelationID) != "corr_propagate" {
		t.Fatalf("response corr=%q", w.Header().Get(correlation.HeaderCorrelationID))
	}
	if w.Header().Get(correlation.HeaderCommandID) != "cmd_c" {
		t.Fatalf("response command id=%q", w.Header().Get(correlation.HeaderCommandID))
	}
	got := h.room.Dispatched[0].Correlation
	if got.CorrelationID != "corr_propagate" || got.CommandID != "cmd_c" {
		t.Fatalf("dispatch corr=%+v", got)
	}
}

func TestReads_LeaderboardAndAnalytics(t *testing.T) {
	h := newHarness(t)
	lb := h.do(http.MethodGet, "/v1/rankings/leaderboards", nil, nil)
	if lb.Code != http.StatusOK || !strings.Contains(lb.Body.String(), "entries") {
		t.Fatalf("leaderboard=%d %s", lb.Code, lb.Body.String())
	}
	an := h.do(http.MethodGet, "/v1/analytics/public", nil, nil)
	if an.Code != http.StatusOK || !strings.Contains(an.Body.String(), "metrics") {
		t.Fatalf("analytics=%d %s", an.Code, an.Body.String())
	}
}

func TestSSE_SpectatorTerminalAdmissionDenied(t *testing.T) {
	h := newHarness(t)
	h.spectator.Deny("room_done", "room_completed")
	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_done", nil, nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestSSE_SpectatorTerminalClose(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/streams/spectator?roomId=room_live", nil)
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

	// Allow subscribe to register.
	deadline := time.Now().Add(2 * time.Second)
	for h.hub.ActiveCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if h.hub.ActiveCount() == 0 {
		t.Fatal("spectator subscription not registered")
	}

	closed := h.hub.CloseSpectatorRoom("room_live")
	if closed != 1 {
		t.Fatalf("closed=%d", closed)
	}

	select {
	case body := <-done:
		if !strings.Contains(body, "room_terminal") {
			t.Fatalf("sse body=%q", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for terminal close")
	}
}

func TestSSE_SessionInvalidationClosesControl(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/streams/control", nil)
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

	if n := h.hub.InvalidateSession(h.principal.SessionID); n != 1 {
		t.Fatalf("invalidate closed=%d", n)
	}

	select {
	case body := <-done:
		if !strings.Contains(body, "session_invalidated") {
			t.Fatalf("sse body=%q", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for session invalidation close")
	}
}

func TestSSE_PlayerRequiresAuth(t *testing.T) {
	h := newHarness(t)
	w := h.do(http.MethodGet, "/v1/streams/player?roomId=room_1", nil, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestRouting_RouteBackendHelpers(t *testing.T) {
	if bff.RouteBackend("PlayCard") != bff.BackendRoom {
		t.Fatal("PlayCard")
	}
	if bff.RouteBackend("CreateTournament") != bff.BackendTournament {
		t.Fatal("CreateTournament")
	}
	if bff.RouteBackend("Nope") != bff.BackendUnknown {
		t.Fatal("unknown")
	}
	// Sequence rules live in shared/envelope (avoid duplicating the catalog helper in bff).
	if envelope.RequiresExpectedSequence("CreateRoom") {
		t.Fatal("CreateRoom exception")
	}
	if !envelope.RequiresExpectedSequence("JoinRoom") {
		t.Fatal("JoinRoom requires sequence")
	}
	if envelope.RequiresExpectedSequence("RegisterPlayer") {
		t.Fatal("tournament commands do not require room sequence")
	}
	if !envelope.IsPublicCommandType("PlayCard") || envelope.IsPublicCommandType("Nope") {
		t.Fatal("public catalog coherence")
	}
}

func TestRejection_TournamentOnRoomRoute(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_t","type":"CreateTournament","schemaVersion":1,"payload":{"tournamentId":"t1"}}`)
	w := h.do(http.MethodPost, "/v1/rooms/room_1/commands", body, h.authHeaders())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var res envelope.Result
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Reason != "tournament_on_room_route" {
		t.Fatalf("result=%+v", res)
	}
	if h.tournament.DispatchCount()+h.room.DispatchCount() != 0 {
		t.Fatal("must not dispatch tournament on room route")
	}
	if h.audit.Len() != 1 {
		t.Fatalf("audit=%d", h.audit.Len())
	}
}

func TestRejection_MissingSchemaVersion(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("must not dispatch without explicit schemaVersion")
	}
}

func TestRejection_SchemaVersionNotOne(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":2,"payload":{}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestAuditFailure_NoDispatchOnPreReject(t *testing.T) {
	identity := bff.NewFakeIdentity()
	room := bff.NewFakeRoom()
	principal := bff.Principal{PlayerID: "p1", SessionID: "s1", Username: "a"}
	identity.SeedSession("tok", principal)
	srv := bff.NewServer(bff.Dependencies{
		Identity:   identity,
		Room:       room,
		Tournament: bff.NewFakeTournament(),
		Reads:      &bff.FakeReads{},
		Spectator:  bff.NewFakeSpectatorGate(),
		Audit:      bff.FailingAudit{},
		Ready:      true,
		Clock:      func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
	})
	// Tournament on room route is a domain rejection that audits before dispatch.
	req := httptest.NewRequest(http.MethodPost, "/v1/rooms/room_1/commands",
		bytes.NewReader([]byte(`{"commandId":"cmd_t","type":"CreateTournament","schemaVersion":1,"payload":{"tournamentId":"t1"}}`)))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if room.DispatchCount() != 0 {
		t.Fatal("audit failure must not dispatch")
	}
}

func TestAuditFailure_AfterBackendReject(t *testing.T) {
	identity := bff.NewFakeIdentity()
	room := bff.NewFakeRoom()
	room.Results["cmd_rej"] = envelope.Rejected("cmd_rej", "PlayCard", "stale", nil)
	principal := bff.Principal{PlayerID: "p1", SessionID: "s1", Username: "a"}
	identity.SeedSession("tok", principal)
	srv := bff.NewServer(bff.Dependencies{
		Identity:   identity,
		Room:       room,
		Tournament: bff.NewFakeTournament(),
		Reads:      &bff.FakeReads{},
		Spectator:  bff.NewFakeSpectatorGate(),
		Audit:      bff.FailingAudit{},
		Ready:      true,
		Clock:      func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/commands",
		bytes.NewReader([]byte(`{"commandId":"cmd_rej","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":"r1"}}`)))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestUpstreamResult_InvalidShapeRejected(t *testing.T) {
	h := newHarness(t)
	h.room.Results["cmd_bad"] = envelope.Result{
		CommandID:     "wrong",
		Type:          "PlayCard",
		Status:        envelope.StatusAccepted,
		SchemaVersion: 1,
	}
	body := []byte(`{"commandId":"cmd_bad","type":"PlayCard","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":"r1"}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestCorrelation_SameValueThroughAuthBackendAudit(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_c","type":"CreateTournament","schemaVersion":1,"payload":{"tournamentId":"t1"}}`)
	headers := h.authHeaders()
	headers[correlation.HeaderCorrelationID] = "corr_once"
	w := h.do(http.MethodPost, "/v1/rooms/room_1/commands", body, headers)
	if w.Header().Get(correlation.HeaderCorrelationID) != "corr_once" {
		t.Fatalf("response corr=%q", w.Header().Get(correlation.HeaderCorrelationID))
	}
	if h.identity.LastCorr.CorrelationID != "corr_once" {
		t.Fatalf("auth corr=%+v", h.identity.LastCorr)
	}
	if h.audit.Len() != 1 {
		t.Fatalf("audit len=%d", h.audit.Len())
	}
	if h.audit.Records()[0].CorrelationID != "corr_once" {
		t.Fatalf("audit corr=%+v", h.audit.Records()[0])
	}
}

func TestCorrelation_GeneratedOncePerRequest(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_c","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	headers := map[string]string{
		"Authorization": "Bearer " + h.token,
		"Content-Type":  "application/json",
	}
	w := h.do(http.MethodPost, "/v1/commands", body, headers)
	respCorr := w.Header().Get(correlation.HeaderCorrelationID)
	if respCorr == "" {
		t.Fatal("missing correlation")
	}
	if h.identity.LastCorr.CorrelationID != respCorr {
		t.Fatalf("auth=%q response=%q", h.identity.LastCorr.CorrelationID, respCorr)
	}
	if h.room.Dispatched[0].Correlation.CorrelationID != respCorr {
		t.Fatalf("backend=%q response=%q", h.room.Dispatched[0].Correlation.CorrelationID, respCorr)
	}
}

func TestSSE_SpectatorInvalidBearerFails(t *testing.T) {
	h := newHarness(t)
	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_1", nil, map[string]string{
		"Authorization": "Bearer dead",
	})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestSSE_SpectatorPrivateRoomRequiresPrincipal(t *testing.T) {
	h := newHarness(t)
	h.spectator.MarkPrivate("room_priv")
	h.spectator.AllowParticipant("room_priv", h.principal.PlayerID)
	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_priv", nil, nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("anon status=%d", w.Code)
	}

	srv := httptest.NewServer(h.srv.Handler())
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/streams/spectator?roomId=room_priv", nil)
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authed status=%d", resp.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.hub.ActiveCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if h.spectator.LastReq.Principal == nil || h.spectator.LastReq.Principal.PlayerID != h.principal.PlayerID {
		t.Fatalf("gate must receive principal: %+v", h.spectator.LastReq)
	}
}

func TestSSE_SubscribeAfterTerminalDenied(t *testing.T) {
	h := newHarness(t)
	h.hub.CloseSpectatorRoom("room_term")
	w := h.do(http.MethodGet, "/v1/streams/spectator?roomId=room_term", nil, nil)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestSSE_SubscribeAfterSessionInvalidatedDenied(t *testing.T) {
	h := newHarness(t)
	h.hub.InvalidateSession(h.principal.SessionID)
	w := h.do(http.MethodGet, "/v1/streams/control", nil, h.authHeaders())
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestSSE_PublishCloseNoPanic(t *testing.T) {
	h := newHarness(t)
	id, ch, _, cancel, err := h.hub.Subscribe(bff.StreamControl, "", h.principal.SessionID, h.principal.PlayerID, "")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()
	for i := 0; i < 100; i++ {
		go func() {
			h.hub.Publish(id, bff.StreamEvent{ID: "1", Event: "ping", Data: json.RawMessage(`{}`), SchemaVersion: 1})
		}()
		go func() {
			h.hub.InvalidateSession(h.principal.SessionID)
		}()
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestHub_LastEventIDReplay(t *testing.T) {
	hub := bff.NewHub()
	hub.SetReplayBound(8)
	for i := 1; i <= 5; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		hub.PublishToStream(bff.StreamSpectator, "room_resume", "", "", bff.StreamEvent{
			Event:         "tick",
			Data:          payload,
			SchemaVersion: 1,
		})
	}
	_, _, replay, cancel, err := hub.Subscribe(bff.StreamSpectator, "room_resume", "", "", "2")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(replay) != 3 {
		t.Fatalf("replay len=%d want 3", len(replay))
	}
	if replay[0].ID != "3" || replay[2].ID != "5" {
		t.Fatalf("replay=%+v", replay)
	}
}

func TestSSE_LastEventIDResumeHTTP(t *testing.T) {
	h := newHarness(t)
	h.hub.SetReplayBound(8)
	for i := 1; i <= 5; i++ {
		payload, _ := json.Marshal(map[string]int{"n": i})
		h.hub.PublishToStream(bff.StreamSpectator, "room_resume", "", "", bff.StreamEvent{
			Event:         "tick",
			Data:          payload,
			SchemaVersion: 1,
		})
	}

	srv := httptest.NewServer(h.srv.Handler())
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/streams/spectator?roomId=room_resume", nil)
	req.Header.Set("Last-Event-ID", "2")
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
		buf := make([]byte, 8192)
		var out string
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				out += string(buf[:n])
			}
			if strings.Contains(out, "id: 5") || err != nil {
				done <- out
				return
			}
		}
	}()

	select {
	case body := <-done:
		if !strings.Contains(body, "id: 3") || !strings.Contains(body, "id: 5") {
			t.Fatalf("expected replay of events after 2, got %q", body)
		}
		if strings.Contains(body, "id: 1\n") || strings.Contains(body, "id: 2\n") {
			t.Fatalf("should not replay events at/before Last-Event-ID: %q", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replay")
	}
	cancel()
}

func TestRateLimit_EdgeRejectsBeforeDispatch(t *testing.T) {
	identity := bff.NewFakeIdentity()
	room := bff.NewFakeRoom()
	principal := bff.Principal{PlayerID: "p1", SessionID: "s1", Username: "a"}
	identity.SeedSession("tok", principal)
	limiter := bff.NewMemoryRateLimiter(1, time.Minute)
	srv := bff.NewServer(bff.Dependencies{
		Identity:    identity,
		Room:        room,
		Tournament:  bff.NewFakeTournament(),
		Reads:       &bff.FakeReads{},
		Spectator:   bff.NewFakeSpectatorGate(),
		Ready:       true,
		EdgeLimiter: limiter,
		Clock:       func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
	})
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
	do := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/commands", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer tok")
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = "10.0.0.1:1234"
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		return w
	}
	if w := do(); w.Code != http.StatusOK {
		t.Fatalf("first=%d %s", w.Code, w.Body.String())
	}
	if w := do(); w.Code != http.StatusTooManyRequests {
		t.Fatalf("second=%d %s", w.Code, w.Body.String())
	}
	if room.DispatchCount() != 1 {
		t.Fatalf("dispatch=%d", room.DispatchCount())
	}
}

func TestRateLimit_PrincipalRejectsBeforeDispatch(t *testing.T) {
	identity := bff.NewFakeIdentity()
	room := bff.NewFakeRoom()
	principal := bff.Principal{PlayerID: "p1", SessionID: "s1", Username: "a"}
	identity.SeedSession("tok", principal)
	srv := bff.NewServer(bff.Dependencies{
		Identity:         identity,
		Room:             room,
		Tournament:       bff.NewFakeTournament(),
		Reads:            &bff.FakeReads{},
		Spectator:        bff.NewFakeSpectatorGate(),
		Audit:            bff.NewMemoryAudit(),
		Ready:            true,
		PrincipalLimiter: bff.NewMemoryRateLimiter(1, time.Minute),
		Clock:            func() time.Time { return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC) },
	})
	do := func(cmdID string) *httptest.ResponseRecorder {
		b := []byte(`{"commandId":"` + cmdID + `","type":"CreateRoom","schemaVersion":1,"payload":{}}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/commands", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer tok")
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		return w
	}
	if w := do("cmd_a"); w.Code != http.StatusOK {
		t.Fatalf("first=%d %s", w.Code, w.Body.String())
	}
	if w := do("cmd_b"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("second=%d %s", w.Code, w.Body.String())
	}
	if room.DispatchCount() != 1 {
		t.Fatalf("dispatch=%d", room.DispatchCount())
	}
}

func TestBodyTooLarge(t *testing.T) {
	h := newHarness(t)
	big := bytes.Repeat([]byte("a"), bff.MaxRequestBodyBytes+10)
	body := append([]byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":1,"payload":{"x":"`), big...)
	body = append(body, []byte(`"}}`)...)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestInternalControl_SessionInvalidated(t *testing.T) {
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
		IdentityProducerCredential: "secret-cred",
	})
	body := []byte(`{"schemaVersion":1,"eventId":"inv_ctrl","eventType":"SessionInvalidated","sessionId":"session_1","playerId":"player_1","reason":"login"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "secret-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !h.hub.IsSessionInvalidated("session_1") {
		t.Fatal("session should be marked invalidated")
	}
	bad := httptest.NewRequest(http.MethodPost, "/internal/v1/control/sessions/session_1/invalidated", bytes.NewReader(body))
	bw := httptest.NewRecorder()
	srv.Handler().ServeHTTP(bw, bad)
	if bw.Code != http.StatusUnauthorized {
		t.Fatalf("missing cred status=%d", bw.Code)
	}
}

func TestInternalControl_RoomTerminal(t *testing.T) {
	h := newHarness(t)
	srv := bff.NewServer(bff.Dependencies{
		Identity:               h.identity,
		Room:                   h.room,
		Tournament:             h.tournament,
		Reads:                  &bff.FakeReads{},
		Spectator:              h.spectator,
		Hub:                    h.hub,
		Ready:                  true,
		RoomProducerCredential: "secret-cred",
	})
	body := []byte(`{"schemaVersion":1,"eventId":"term_ctrl","eventType":"RoomTerminal","roomId":"room_x"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/control/rooms/room_x/terminal", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Credential", "secret-cred")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if !h.hub.IsRoomTerminal("room_x") {
		t.Fatal("room should be terminal")
	}
}

func TestReady_FailsWhenNotConfigured(t *testing.T) {
	srv := bff.NewServer(bff.Dependencies{
		Identity: bff.ClosedIdentity{},
		Ready:    false,
	})
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestReady_OKWhenConfigured(t *testing.T) {
	h := newHarness(t)
	w := h.do(http.MethodGet, "/ready", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestClosedClients_FailRequests(t *testing.T) {
	srv := bff.NewServer(bff.Dependencies{Ready: false})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/register",
		bytes.NewReader([]byte(`{"username":"a","password":"b"}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest && w.Code != http.StatusServiceUnavailable && w.Code != http.StatusBadGateway {
		// Closed identity returns error -> bad_request from register handler
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	}
}

func TestEnvelope_UnknownTopLevelFieldHTTP400(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_1","type":"CreateRoom","schemaVersion":1,"payload":{},"extra":true}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_envelope") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("must not dispatch")
	}
	if h.audit.Len() != 0 {
		t.Fatal("shape errors are not rejection audits")
	}
}

func TestEnvelope_UnknownPayloadFieldHTTP400(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_1","type":"JoinRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":"r1","extra":1}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_envelope") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if h.room.DispatchCount() != 0 {
		t.Fatal("must not dispatch")
	}
}

func TestEnvelope_ForbiddenSequenceOnCreateRoomAndTournamentHTTP400(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		typ     string
		payload string
	}{
		{"CreateRoom", `{}`},
		{"CreateTournament", `{"tournamentId":"t1"}`},
		{"RegisterPlayer", `{"tournamentId":"t1"}`},
		{"CloseRegistration", `{"tournamentId":"t1"}`},
	}
	for _, tc := range cases {
		body := []byte(`{"commandId":"cmd_` + tc.typ + `","type":"` + tc.typ + `","expectedSequenceNumber":0,"schemaVersion":1,"payload":` + tc.payload + `}`)
		w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", tc.typ, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "invalid_envelope") {
			t.Fatalf("%s body=%s", tc.typ, w.Body.String())
		}
	}
	if h.room.DispatchCount()+h.tournament.DispatchCount() != 0 {
		t.Fatal("must not dispatch prohibited-sequence envelopes")
	}
	if h.audit.Len() != 0 {
		t.Fatal("shape errors are not rejection audits")
	}
}

func TestEnvelope_UnknownTypeHTTP400Malformed(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"commandId":"cmd_x","type":"NotARealCommand","schemaVersion":1,"payload":{"anything":true}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid_envelope") {
		t.Fatalf("body=%s", w.Body.String())
	}
	if h.audit.Len() != 0 {
		t.Fatal("unknown type must not audit")
	}
}

func TestEnvelope_InvalidPayloadTypesEnumsRangesHTTP400(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name string
		body string
	}{
		{"non-string roomId", `{"commandId":"c","type":"JoinRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":99}}`},
		{"invalid visibility", `{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"visibility":"friends"}}`},
		{"maxSeats 1", `{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":1}}`},
		{"maxSeats 11", `{"commandId":"c","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":11}}`},
		{"negative disconnectVersion", `{"commandId":"c","type":"ReconnectToRoom","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"disconnectVersion":-1}}`},
		{"invalid color", `{"commandId":"c","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"color":"purple"}}`},
		{"non-integer capacity", `{"commandId":"c","type":"CreateTournament","schemaVersion":1,"payload":{"capacity":"8"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := h.do(http.MethodPost, "/v1/commands", []byte(tc.body), h.authHeaders())
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "invalid_envelope") {
				t.Fatalf("body=%s", w.Body.String())
			}
		})
	}
	if h.room.DispatchCount()+h.tournament.DispatchCount() != 0 {
		t.Fatal("must not dispatch invalid payloads")
	}
	if h.audit.Len() != 0 {
		t.Fatal("shape errors are not rejection audits")
	}
}

func TestEnvelope_ValidPayloadBoundsDispatch(t *testing.T) {
	h := newHarness(t)
	cases := []string{
		`{"commandId":"c_ms0","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":0}}`,
		`{"commandId":"c_ms2","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":2}}`,
		`{"commandId":"c_ms10","type":"CreateRoom","schemaVersion":1,"payload":{"maxSeats":10,"visibility":"private"}}`,
		`{"commandId":"c_color","type":"ChooseColor","expectedSequenceNumber":1,"schemaVersion":1,"payload":{"roomId":"r1","color":"blue"}}`,
		`{"commandId":"c_tour","type":"CreateTournament","schemaVersion":1,"payload":{"tournamentId":"t1","capacity":8}}`,
	}
	for _, body := range cases {
		w := h.do(http.MethodPost, "/v1/commands", []byte(body), h.authHeaders())
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s for %s", w.Code, w.Body.String(), body)
		}
	}
}

func TestRejection_StaleSequenceReturnsHTTP409WithCommandResult(t *testing.T) {
	h := newHarness(t)
	seq := int64(7)
	h.room.Results["cmd_stale"] = envelope.Rejected("cmd_stale", "PlayCard", "stale_sequence", &seq)
	body := []byte(`{"commandId":"cmd_stale","type":"PlayCard","expectedSequenceNumber":3,"schemaVersion":1,"payload":{"roomId":"room_1"}}`)
	w := h.do(http.MethodPost, "/v1/commands", body, h.authHeaders())
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var res envelope.Result
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Status != envelope.StatusRejected || res.Reason != "stale_sequence" {
		t.Fatalf("result=%+v", res)
	}
	if h.room.DispatchCount() != 1 {
		t.Fatal("stale/wrong sequence must still dispatch")
	}
	if h.audit.Len() != 1 {
		t.Fatalf("audit len=%d", h.audit.Len())
	}
}

func TestOpenAPI_CommandEnvelopeHasExactlyFifteenVariants(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	path := filepath.Join(root, "contracts", "openapi", "bff-v1.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read openapi: %v", err)
	}
	text := string(raw)
	idx := strings.Index(text, "CommandEnvelope:")
	if idx < 0 {
		t.Fatal("CommandEnvelope missing")
	}
	oneOf := strings.Index(text[idx:], "oneOf:")
	if oneOf < 0 {
		t.Fatal("oneOf missing")
	}
	disc := strings.Index(text[idx+oneOf:], "discriminator:")
	if disc < 0 {
		t.Fatal("discriminator missing")
	}
	section := text[idx+oneOf : idx+oneOf+disc]
	refs := regexp.MustCompile(`\$ref:\s*"#/components/schemas/\w+Command"`).FindAllString(section, -1)
	if len(refs) != 15 {
		t.Fatalf("CommandEnvelope oneOf variants=%d want 15", len(refs))
	}
}

func TestOpenAPI_CreateRoomMaxSeatsContractShape(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	path := filepath.Join(root, "contracts", "openapi", "bff-v1.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read openapi: %v", err)
	}
	text := string(raw)
	idx := strings.Index(text, "CreateRoomCommand:")
	if idx < 0 {
		t.Fatal("CreateRoomCommand missing")
	}
	next := strings.Index(text[idx+1:], "\n    JoinRoomCommand:")
	if next < 0 {
		t.Fatal("JoinRoomCommand boundary missing")
	}
	section := text[idx : idx+1+next]
	if !strings.Contains(section, "maxSeats:") {
		t.Fatal("maxSeats missing from CreateRoomCommand")
	}
	if !strings.Contains(section, "anyOf:") {
		t.Fatal("maxSeats must use anyOf for nonpositive-default vs 2..10")
	}
	if !regexp.MustCompile(`(?s)maximum:\s*0`).MatchString(section) {
		t.Fatal("maxSeats anyOf must allow nonpositive (maximum: 0)")
	}
	if !regexp.MustCompile(`(?s)minimum:\s*2`).MatchString(section) {
		t.Fatal("maxSeats anyOf must require minimum 2 for explicit positive")
	}
	if !regexp.MustCompile(`(?s)maximum:\s*10`).MatchString(section) {
		t.Fatal("maxSeats anyOf must cap at maximum 10")
	}
	if regexp.MustCompile(`(?s)maxSeats:.*?minimum:\s*1\b`).MatchString(section) {
		t.Fatal("maxSeats must not claim minimum: 1 (domain rejects 1)")
	}
	if !strings.Contains(section, "default to 10") {
		t.Fatal("maxSeats description must document default 10 for nonpositive")
	}
}
