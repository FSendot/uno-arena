package store_test

import (
	"testing"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/store"
)

func TestEncodeDecodeSessionSnapshot_RoundTrip(t *testing.T) {
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c1", RoomID: "room_snap", HostID: "host",
		Visibility: domain.VisibilityPublic, MaxSeats: 4,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	sess := domain.OpenSession(room)
	raw, err := store.EncodeSessionSnapshot(sess)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.DecodeSessionSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Room().ID() != room.ID() || got.Room().HostID() != room.HostID() {
		t.Fatalf("roundtrip mismatch: got host=%s id=%s", got.Room().HostID(), got.Room().ID())
	}
	if got.Room().Sequence() != room.Sequence() || got.Room().Roster().Capacity() != 4 {
		t.Fatalf("seq/capacity mismatch")
	}
}
