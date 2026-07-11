package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTimerID_RoundTripDelimiterContainingFields(t *testing.T) {
	exp := time.UnixMilli(1_700_000_000_000).UTC()
	id := TimerID{
		Family:     timerFamilyUno,
		RoomID:     "room|with|pipes",
		PlayerID:   "p|1",
		GameID:     "g|abc",
		Trigger:    "trig|x",
		Version:    3,
		OpeningSeq: 9,
		ExpiresAt:  exp,
	}
	s := id.String()
	if strings.Contains(id.RoomID, "|") && strings.Count(s, "|") == 7 && !strings.HasPrefix(strings.TrimSpace(s), "{") {
		t.Fatalf("pipe-split encoding is not injective for delimiter fields: %q", s)
	}
	got, err := ParseTimerID(s)
	if err != nil {
		t.Fatalf("parse: %v (encoded=%q)", err, s)
	}
	if got.Family != id.Family || got.RoomID != id.RoomID || got.PlayerID != id.PlayerID ||
		got.GameID != id.GameID || got.Trigger != id.Trigger || got.Version != id.Version ||
		got.OpeningSeq != id.OpeningSeq || !got.ExpiresAt.Equal(id.ExpiresAt) {
		t.Fatalf("roundtrip mismatch:\n got=%+v\nwant=%+v\n enc=%s", got, id, s)
	}
	if id.String() != got.String() {
		t.Fatalf("encoding not stable: %q vs %q", id.String(), got.String())
	}
}

func TestTimerID_ParseRejectsMalformed(t *testing.T) {
	valid := TimerID{
		Family: timerFamilyReconnect, RoomID: "r1", PlayerID: "p1",
		Version: 1, ExpiresAt: time.UnixMilli(1000).UTC(),
	}
	good := valid.String()

	cases := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"pipe_legacy", "uno|r1|p1|g1|trig|1|2|1000"},
		{"bad_json", "{not-json"},
		{"blank_room", mustMutateTimerJSON(t, good, map[string]any{"roomId": ""})},
		{"blank_room_ws", mustMutateTimerJSON(t, good, map[string]any{"roomId": "  "})},
		{"unknown_family", mustMutateTimerJSON(t, good, map[string]any{"family": "other"})},
		{"bad_version", mustMutateTimerJSON(t, good, map[string]any{"version": "x"})},
		{"bad_opening", mustMutateTimerJSON(t, good, map[string]any{"openingSeq": "nope"})},
		{"bad_expires", mustMutateTimerJSON(t, good, map[string]any{"expiresAtMs": "soon"})},
		{"zero_expires", mustMutateTimerJSON(t, good, map[string]any{"expiresAtMs": 0})},
		{"neg_expires", mustMutateTimerJSON(t, good, map[string]any{"expiresAtMs": -1})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseTimerID(tc.raw); err == nil {
				t.Fatalf("expected reject for %q", tc.raw)
			}
		})
	}
}

func mustMutateTimerJSON(t *testing.T, encoded string, patch map[string]any) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(encoded), &m); err != nil {
		m = map[string]any{
			"family": timerFamilyReconnect, "roomId": "r1", "playerId": "p1",
			"gameId": "", "trigger": "", "version": float64(1), "openingSeq": float64(0),
			"expiresAtMs": float64(1000),
		}
	}
	for k, v := range patch {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
