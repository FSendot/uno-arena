package app

import "testing"

func TestPlayerFeedTargetStream_DeterministicPerRoom(t *testing.T) {
	got, err := PlayerFeedTargetStream("room_abc")
	if err != nil {
		t.Fatal(err)
	}
	if got != "room:room_abc:player" {
		t.Fatalf("got %q", got)
	}
	again, err := PlayerFeedTargetStream("room_abc")
	if err != nil || again != got {
		t.Fatalf("unstable: %q %v", again, err)
	}
}

func TestPlayerFeedTargetStream_InjectiveAcrossRooms(t *testing.T) {
	a, err := PlayerFeedTargetStream("a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := PlayerFeedTargetStream("b")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("collision: %q", a)
	}
	// Colon in room id would make room:x:y:player ambiguous vs room:x / y — rejected.
	if _, err := PlayerFeedTargetStream("x:y"); err == nil {
		t.Fatal("expected reject for colon in room id")
	}
}

func TestPlayerFeedTargetStream_RejectsInvalid(t *testing.T) {
	cases := []string{"", " ", " a", "a ", "a b", "a\nb", "a\x00b"}
	for _, c := range cases {
		if _, err := PlayerFeedTargetStream(c); err == nil {
			t.Fatalf("expected reject for %q", c)
		}
	}
}

func TestPlayerFeedTargetStream_NotGenericPlayerKind(t *testing.T) {
	got, err := PlayerFeedTargetStream("r1")
	if err != nil {
		t.Fatal(err)
	}
	if got == StreamPlayer {
		t.Fatalf("target_stream must not be generic %q", StreamPlayer)
	}
	if !stringsHasPrefixSuffix(got, PlayerFeedStreamPrefix, PlayerFeedStreamSuffix) {
		t.Fatalf("unexpected shape %q", got)
	}
}

func stringsHasPrefixSuffix(s, prefix, suffix string) bool {
	return len(s) >= len(prefix)+len(suffix) &&
		s[:len(prefix)] == prefix &&
		s[len(s)-len(suffix):] == suffix
}
