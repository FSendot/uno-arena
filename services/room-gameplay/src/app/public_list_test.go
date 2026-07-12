package app_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"unoarena/services/room-gameplay/app"
	"unoarena/services/room-gameplay/domain"
)

func TestPublicListCursor_RoundTripAndTamper(t *testing.T) {
	restore := app.SetPublicListCursorMACKeyForTest("unit-public-list-cursor")
	defer restore()

	enc, err := app.EncodePublicListCursor(app.PublicListCursor{Status: "waiting", RoomID: "room_a"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := app.DecodePublicListCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "waiting" || got.RoomID != "room_a" || got.V != 1 {
		t.Fatalf("got %#v", got)
	}
	tampered := enc[:len(enc)-1] + "x"
	if _, err := app.DecodePublicListCursor(tampered); err == nil {
		t.Fatal("expected tamper reject")
	}
	if _, err := app.DecodePublicListCursor("OFFSET=1"); err == nil {
		t.Fatal("expected physical leakage reject")
	}
}

func TestPublicListCursor_ProductionRequiresSecret(t *testing.T) {
	t.Setenv("DEPLOYMENT_ENV", "production")
	t.Setenv("ROOM_PUBLIC_LIST_CURSOR_SECRET", "")
	restore := app.SetPublicListCursorMACKeyForTest("")
	defer restore()
	if _, err := app.EncodePublicListCursor(app.PublicListCursor{Status: "waiting", RoomID: "r1"}); !errors.Is(err, app.ErrPublicListCursorSecretRequired) {
		t.Fatalf("want ErrPublicListCursorSecretRequired, got %v", err)
	}
}

func TestPublicList_PrivacyExcludesPrivateAndPaginates(t *testing.T) {
	restore := app.SetPublicListCursorMACKeyForTest("list-privacy-cursor")
	defer restore()

	sessions := app.NewMemorySessionRepository()
	svc := app.NewService(app.ServiceDeps{
		Sessions:  sessions,
		Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(),
		Audit:     app.NewFakeAuditSink(),
		Deals:     app.NewFakeDealSource(),
		SessionsV: app.AllowAllSessionValidator{},
	})
	svc.SetPublicListReader(sessions)

	seed := func(id, host string, vis domain.Visibility) {
		r, out := domain.CreateRoom(domain.CreateRoomCommand{
			CommandID: domain.CommandID("c-" + id), RoomID: domain.RoomID(id),
			HostID: domain.PlayerID(host), Visibility: vis, MaxSeats: 4,
		})
		if out.Kind != domain.OutcomeAccepted || r == nil {
			t.Fatalf("create %s: %#v", id, out)
		}
		if err := sessions.Commit(context.Background(), app.CommitRequest{Session: domain.OpenSession(r)}); err != nil {
			t.Fatal(err)
		}
	}
	seed("room_a", "h1", domain.VisibilityPublic)
	seed("room_b", "h2", domain.VisibilityPublic)
	seed("room_c", "h3", domain.VisibilityPublic)
	seed("room_priv", "hp", domain.VisibilityPrivate)

	page1, err := svc.PublicList(context.Background(), app.PublicListQuery{Status: "waiting", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Rooms) != 2 || page1.NextCursor == "" {
		t.Fatalf("page1=%#v", page1)
	}
	for _, r := range page1.Rooms {
		if r.Visibility != "public" || r.RoomID == "room_priv" {
			t.Fatalf("privacy leak: %#v", r)
		}
		if strings.Contains(strings.ToLower(r.RoomID+r.HostID), "hand") {
			t.Fatal("unexpected hand material")
		}
	}

	page2, err := svc.PublicList(context.Background(), app.PublicListQuery{
		Status: "waiting", Limit: 2, Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Rooms) != 1 || page2.NextCursor != "" {
		t.Fatalf("exact-full final page want 1 no cursor, got %#v", page2)
	}
	seen := map[string]bool{}
	for _, r := range append(page1.Rooms, page2.Rooms...) {
		if seen[r.RoomID] {
			t.Fatalf("duplicate %s", r.RoomID)
		}
		seen[r.RoomID] = true
	}
	if !seen["room_a"] || !seen["room_b"] || !seen["room_c"] || seen["room_priv"] {
		t.Fatalf("seen=%v", seen)
	}

	_, err = svc.PublicList(context.Background(), app.PublicListQuery{
		Status: "locked", Cursor: page1.NextCursor, Limit: 2,
	})
	if err == nil || !errors.Is(err, app.ErrPublicListBadRequest) {
		t.Fatalf("cursor filter binding want bad request, got %v", err)
	}
}

func TestParsePublicListQuery_Validation(t *testing.T) {
	if _, err := app.ParsePublicListQuery("completed", "", "10"); err == nil {
		t.Fatal("completed status must fail")
	}
	if _, err := app.ParsePublicListQuery("waiting", "", "0"); err == nil {
		t.Fatal("limit 0 must fail")
	}
	if _, err := app.ParsePublicListQuery("waiting", "", "101"); err == nil {
		t.Fatal("limit 101 must fail")
	}
	q, err := app.ParsePublicListQuery("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if q.Status != "waiting" || q.Limit != 50 {
		t.Fatalf("defaults %#v", q)
	}
	q, err = app.ParsePublicListQuery("in_progress", "c", "100")
	if err != nil {
		t.Fatal(err)
	}
	if q.Status != "in_progress" || q.Limit != 100 {
		t.Fatalf("max ok %#v", q)
	}
}

func TestPublicList_Max100Clamp(t *testing.T) {
	restore := app.SetPublicListCursorMACKeyForTest("list-max-cursor")
	defer restore()
	sessions := app.NewMemorySessionRepository()
	svc := app.NewService(app.ServiceDeps{
		Sessions: sessions, Integrity: app.NewFakeGameIntegrity(),
		Publisher: app.NewFakeEventPublisher(), Audit: app.NewFakeAuditSink(),
		Deals: app.NewFakeDealSource(), SessionsV: app.AllowAllSessionValidator{},
	})
	svc.SetPublicListReader(sessions)
	for i := 0; i < 105; i++ {
		id := fmt.Sprintf("r%03d", i)
		r, _ := domain.CreateRoom(domain.CreateRoomCommand{
			CommandID: domain.CommandID("c" + id), RoomID: domain.RoomID(id),
			HostID: "h", Visibility: domain.VisibilityPublic, MaxSeats: 4,
		})
		_ = sessions.Commit(context.Background(), app.CommitRequest{Session: domain.OpenSession(r)})
	}
	page, err := svc.PublicList(context.Background(), app.PublicListQuery{Status: "waiting", Limit: 999})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rooms) != app.PublicListMaxLimit {
		t.Fatalf("want %d got %d", app.PublicListMaxLimit, len(page.Rooms))
	}
	if page.NextCursor == "" {
		t.Fatal("expected nextCursor when more remain")
	}
}
