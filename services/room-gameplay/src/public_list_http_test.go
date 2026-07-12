package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
)

func TestPublicListHTTP_AuthFailClosedAndPrivacy(t *testing.T) {
	restore := app.SetPublicListCursorMACKeyForTest("http-public-list-cursor")
	defer restore()

	e := newTestEnv(t)

	pub, _ := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c1", RoomID: "room_pub", HostID: "host", Visibility: domain.VisibilityPublic, MaxSeats: 4,
	})
	priv, _ := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c2", RoomID: "room_priv", HostID: "host", Visibility: domain.VisibilityPrivate, MaxSeats: 4,
	})
	_ = e.sessions.Commit(t.Context(), app.CommitRequest{Session: domain.OpenSession(pub)})
	_ = e.sessions.Commit(t.Context(), app.CommitRequest{Session: domain.OpenSession(priv)})

	w := e.do(t, http.MethodGet, "/internal/v1/rooms/public-list", nil, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no cred want 401, got %d", w.Code)
	}

	w = e.do(t, http.MethodGet, "/internal/v1/rooms/public-list?status=waiting&limit=10", nil, map[string]string{
		"X-Service-Credential": "wrong",
	})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong cred want 401, got %d", w.Code)
	}

	w = e.do(t, http.MethodGet, "/internal/v1/rooms/public-list?status=waiting&limit=10", nil, e.auth())
	if w.Code != http.StatusOK {
		t.Fatalf("ok want 200, got %d %s", w.Code, w.Body.String())
	}
	var page map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	rooms := page["rooms"].([]any)
	if len(rooms) != 1 {
		t.Fatalf("rooms=%v", rooms)
	}
	raw := w.Body.String()
	if strings.Contains(raw, "room_priv") {
		t.Fatal("private room leaked")
	}
	for _, bad := range []string{"\"hand\"", "sessionId", "invite", "deck", "password"} {
		if strings.Contains(raw, bad) {
			t.Fatalf("forbidden field %q in %s", bad, raw)
		}
	}

	w = e.do(t, http.MethodGet, "/internal/v1/rooms/public-list?status=completed", nil, e.auth())
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad status want 400, got %d", w.Code)
	}
}

func TestPublicListHTTP_ExactFullFinalPage(t *testing.T) {
	restore := app.SetPublicListCursorMACKeyForTest("http-exact-page-cursor")
	defer restore()
	e := newTestEnv(t)
	for _, id := range []string{"a", "b"} {
		r, _ := domain.CreateRoom(domain.CreateRoomCommand{
			CommandID: domain.CommandID("c-" + id), RoomID: domain.RoomID("room_" + id),
			HostID: "h", Visibility: domain.VisibilityPublic, MaxSeats: 4,
		})
		_ = e.sessions.Commit(t.Context(), app.CommitRequest{Session: domain.OpenSession(r)})
	}

	w := e.do(t, http.MethodGet, "/internal/v1/rooms/public-list?limit=2", nil, e.auth())
	if w.Code != http.StatusOK {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	var page1 struct {
		Rooms      []map[string]any `json:"rooms"`
		NextCursor string           `json:"nextCursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page1); err != nil {
		t.Fatal(err)
	}
	if len(page1.Rooms) != 2 || page1.NextCursor != "" {
		t.Fatalf("exact-full final page must omit nextCursor: %#v", page1)
	}
}
