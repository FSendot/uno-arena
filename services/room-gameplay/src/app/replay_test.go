package app_test

import (
	"context"
	"testing"

	"unoarena/services/room-gameplay/app"
)

func TestFakeGameIntegrity_ReplayFailClosedMissingOffset(t *testing.T) {
	gi := app.NewFakeGameIntegrity()
	_, err := gi.Append(context.Background(), app.AppendRequest{
		RoomID: "r1", EventID: "e1", ExpectedRevision: 0, EventType: "CreateRoom", Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gi.Replay(context.Background(), "r1", 99)
	if err == nil {
		t.Fatal("expected fail-closed missing offset")
	}
	res, err := gi.Replay(context.Background(), "r1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Offset != 0 {
		t.Fatalf("entries=%+v", res.Entries)
	}
}
