package store_test

import (
	"strings"
	"testing"

	"unoarena/services/spectator-view/domain"
	"unoarena/services/spectator-view/store"
)

func redisHashTag(key string) (string, bool) {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return "", false
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end < 0 {
		return "", false
	}
	return key[start+1 : start+1+end], true
}

func TestKeySpace_PerRoomKeysShareHashTag(t *testing.T) {
	ks := store.NewKeySpace("spectator:")
	room := domain.RoomID("roomA")
	keys := []string{
		ks.Meta(room),
		ks.State(room),
		ks.Outcomes(room),
		ks.Invites(room),
		ks.Generation(room),
		ks.Stream(room, "1"),
		ks.Stream(room, "2"),
	}
	tag, ok := redisHashTag(keys[0])
	if !ok || tag != "roomA" {
		t.Fatalf("expected hash tag roomA in %q, got %q ok=%v", keys[0], tag, ok)
	}
	want := "room:{roomA}"
	for _, k := range keys {
		if !strings.Contains(k, want) {
			t.Fatalf("key %q missing room hash tag segment %q", k, want)
		}
		got, ok := redisHashTag(k)
		if !ok || got != tag {
			t.Fatalf("key %q tag=%q ok=%v want %q", k, got, ok, tag)
		}
	}
}

func TestKeySpace_NoCrossRoomHashTagCollision(t *testing.T) {
	ks := store.NewKeySpace("spectest:")
	a := domain.RoomID("roomA")
	b := domain.RoomID("roomB")
	ab := domain.RoomID("roomAB")
	tagA, _ := redisHashTag(ks.Meta(a))
	tagB, _ := redisHashTag(ks.Meta(b))
	tagAB, _ := redisHashTag(ks.Meta(ab))
	if tagA == tagB || tagA == tagAB || tagB == tagAB {
		t.Fatalf("cross-room tags collided: %q %q %q", tagA, tagB, tagAB)
	}
	if strings.Contains(ks.Meta(a), string(b)) {
		t.Fatalf("roomA key unexpectedly contains roomB: %s", ks.Meta(a))
	}
}

func TestValidateRoomID_RejectsBraces(t *testing.T) {
	for _, id := range []domain.RoomID{"{room}", "ro{om}", "room}", "{r"} {
		if err := store.ValidateRoomID(id); err == nil {
			t.Fatalf("expected reject for %q", id)
		}
	}
}
