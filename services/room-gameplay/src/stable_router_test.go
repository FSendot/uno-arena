package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
)

func TestStableRoomHandlerAuthorizesExactCallerBeforeRuntimeLookup(t *testing.T) {
	srv, _ := newStableRouterTestServer(t, domain.RoomStatusWaiting)
	directory := NewPodDirectory()
	directory.Replace([]runtimePod{{RoomHash: runtimeRoomHash("room-stable"), Generation: 1, IP: "127.0.0.1", Ready: true}})
	var forwarded atomic.Int32
	router := NewRoomRouter(directory, "router-secret", func(*http.Request) (*http.Response, error) {
		forwarded.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	})
	handler := NewStableRoomHandler(srv, router)

	for _, tc := range []struct {
		name       string
		path       string
		credential string
		want       int
	}{
		{name: "gateway command rejects timer authority", path: "/v1/rooms/room-stable/commands", credential: "timer-secret", want: http.StatusUnauthorized},
		{name: "timer command rejects gateway authority", path: "/internal/v1/rooms/room-stable/timer-commands", credential: "gateway-secret", want: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := bytes.NewBufferString(`{"commandId":"c1","type":"PlayCard","schemaVersion":1,"roomId":"room-stable","payload":{}}`)
			req := httptest.NewRequest(http.MethodPost, tc.path, body)
			req.Header.Set(internalCredentialHeader, tc.credential)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if got := forwarded.Load(); got != 0 {
		t.Fatalf("unauthorized traffic reached runtime lookup/forwarding: calls=%d", got)
	}
}

func TestStableRoomHandlerRequiresGatewayDerivedSnapshotIdentityAndMembership(t *testing.T) {
	srv, _ := newStableRouterTestServer(t, domain.RoomStatusWaiting)
	handler := NewStableRoomHandler(srv, NewRoomRouter(NewPodDirectory(), "router-secret", nil))

	for _, tc := range []struct {
		name     string
		playerID string
		want     int
	}{
		{name: "query identity alone has no authority", want: http.StatusUnauthorized},
		{name: "non-member rejected before pod lookup", playerID: "intruder", want: http.StatusForbidden},
		{name: "member reaches runtime readiness decision", playerID: "host", want: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/rooms/room-stable/snapshot?playerId=host", nil)
			req.Header.Set(internalCredentialHeader, "gateway-secret")
			if tc.playerID != "" {
				req.Header.Set("X-Player-Id", tc.playerID)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestStableRoomHandlerUpgradesAuthorizedTrafficToRouterCredential(t *testing.T) {
	srv, _ := newStableRouterTestServer(t, domain.RoomStatusWaiting)
	directory := NewPodDirectory()
	directory.Replace([]runtimePod{{RoomHash: runtimeRoomHash("room-stable"), Generation: 4, IP: "127.0.0.1", Ready: true}})
	var upstream *http.Request
	router := NewRoomRouter(directory, "router-secret", func(r *http.Request) (*http.Response, error) {
		upstream = r
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	})
	handler := NewStableRoomHandler(srv, router)
	req := httptest.NewRequest(http.MethodGet, "/v1/rooms/room-stable/snapshot", nil)
	req.Header.Set(internalCredentialHeader, "gateway-secret")
	req.Header.Set("X-Player-Id", "host")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK || upstream == nil {
		t.Fatalf("status=%d upstream=%v body=%s", w.Code, upstream, w.Body.String())
	}
	if upstream.Header.Get(internalCredentialHeader) != "" || upstream.Header.Get(runtimeCredentialHeader) != "router-secret" || upstream.Header.Get(runtimeGenerationHeader) != "4" {
		t.Fatalf("upstream authority=%v", upstream.Header)
	}
}

func TestStableRoomHandlerKeepsRecoveryAndTerminalSnapshotsOnPostgres(t *testing.T) {
	for _, status := range []domain.RoomStatus{domain.RoomStatusCompleted, domain.RoomStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			srv, _ := newStableRouterTestServer(t, status)
			handler := NewStableRoomHandler(srv, NewRoomRouter(NewPodDirectory(), "router-secret", nil))
			req := httptest.NewRequest(http.MethodGet, "/v1/rooms/room-stable/snapshot", nil)
			req.Header.Set(internalCredentialHeader, "gateway-secret")
			req.Header.Set("X-Player-Id", "host")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("terminal snapshot status=%d body=%s", w.Code, w.Body.String())
			}
			var got map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got["status"] != string(status) {
				t.Fatalf("snapshot=%v err=%v", got, err)
			}
		})
	}

	srv, _ := newStableRouterTestServer(t, domain.RoomStatusWaiting)
	handler := NewStableRoomHandler(srv, NewRoomRouter(NewPodDirectory(), "router-secret", nil))
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/rooms/room-stable/spectator-recovery-snapshot?failedCheckpoint=1&recoveryJobId=job-1&schemaVersion=1", nil)
	req.Header.Set(internalCredentialHeader, "recovery-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("spectator recovery must stay stable: status=%d body=%s", w.Code, w.Body.String())
	}
}

func newStableRouterTestServer(t *testing.T, status domain.RoomStatus) (*Server, *app.MemorySessionRepository) {
	t.Helper()
	repo := app.NewMemorySessionRepository()
	roster := domain.RestoreRoster(2, []domain.Seat{{Index: 0, PlayerID: "host", Occupied: true}})
	room := domain.RestoreRoom(domain.RestoreRoomInput{
		ID: "room-stable", RoomType: domain.RoomTypeAdHoc, Status: status,
		Visibility: domain.VisibilityPrivate, HostID: "host", Roster: roster, Sequence: 3,
	})
	if err := repo.Commit(context.Background(), app.CommitRequest{Session: domain.OpenSession(room)}); err != nil {
		t.Fatal(err)
	}
	svc := app.NewService(app.ServiceDeps{
		Sessions: repo, Integrity: app.NewFakeGameIntegrity(), Publisher: app.NewFakeEventPublisher(),
		Audit: app.NewFakeAuditSink(), Deals: app.NewFakeDealSource(), Clock: app.SystemClock{},
		SessionsV: app.AllowAllSessionValidator{},
	})
	return NewServerWithScopedCreds(svc, "gateway-secret", "timer-secret", "recovery-secret", "analytics-secret", "room-gameplay"), repo
}
