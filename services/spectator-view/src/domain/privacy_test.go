package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPrivacy_ForbiddenHandFieldsQuarantinedBeforeState(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)

	attacks := []struct {
		name    string
		payload map[string]any
	}{
		{"hand", map[string]any{"hand": []any{"red-1", "blue-2"}}},
		{"hands", map[string]any{"hands": map[string]any{"p1": []any{"red-1"}}}},
		{"cards", map[string]any{"playerId": "p1", "cards": []any{"yellow-3"}}},
		{"drawnCards", map[string]any{"drawnCards": []any{"green-4"}}},
		{"drawIdentity", map[string]any{"drawIdentity": "card_xyz"}},
		{"deck", map[string]any{"deck": []any{"red-0"}}},
		{"hiddenDeck", map[string]any{"hiddenDeck": []any{"wild"}}},
		{"deckOrder", map[string]any{"deckOrder": []any{"a", "b"}}},
		{"nested hand", map[string]any{"seats": []any{map[string]any{"playerId": "p1", "hand": []any{"red-1"}}}}},
	}
	seq := SequenceNumber(2)
	before := p.Snapshot()
	for _, tt := range attacks {
		t.Run(tt.name, func(t *testing.T) {
			ev := evt(seq, EventID("atk_"+tt.name), EventCardPlayed, tt.payload)
			out := p.Apply(ev)
			if out.Kind != OutcomeDropped {
				t.Fatalf("kind=%s want dropped", out.Kind)
			}
			if out.Rejection == nil || out.Rejection.Code != RejectForbiddenField {
				t.Fatalf("rejection=%+v", out.Rejection)
			}
			if !hasFact(out.Facts, FactProjectionEventQuarantined) || !hasFact(out.Facts, FactSpectatorEventDropped) {
				t.Fatalf("facts=%v", factNames(out.Facts))
			}
			if p.Sequence() != before.Sequence {
				t.Fatalf("sequence advanced on privacy attack: %d", p.Sequence())
			}
			seq++
		})
	}
}

func TestPrivacy_SessionTokenIdempotencyAuditRejected(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	fields := []string{
		"session", "sessionId", "sessionToken", "token", "accessToken",
		"idempotencyKey", "commandId", "audit", "auditId", "auditRecord",
	}
	for i, field := range fields {
		t.Run(field, func(t *testing.T) {
			ev := evt(SequenceNumber(2+i), EventID("priv_"+field), EventTurnAdvanced, map[string]any{
				field:             "secret-value",
				"currentPlayerId": "p1",
			})
			out := p.Apply(ev)
			if out.Kind != OutcomeDropped {
				t.Fatalf("kind=%s", out.Kind)
			}
			if out.Rejection == nil || out.Rejection.Code != RejectForbiddenField {
				t.Fatalf("rejection=%+v", out.Rejection)
			}
		})
	}
}

func TestPrivacy_UnknownEventTypeDropped(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	ev := evt(2, "unk", EventType("PrivateHandRevealed"), map[string]any{
		"playerId":  "p1",
		"cardCount": 3,
	})
	out := p.Apply(ev)
	if out.Kind != OutcomeDropped {
		t.Fatalf("kind=%s", out.Kind)
	}
	if out.Rejection == nil || out.Rejection.Code != RejectUnknownEventType {
		t.Fatalf("rejection=%+v", out.Rejection)
	}
	if p.Sequence() != 1 {
		t.Fatalf("sequence=%d", p.Sequence())
	}
}

func TestPrivacy_DisallowedFieldQuarantined(t *testing.T) {
	t.Parallel()
	p := seededWaiting(t)
	out := p.Apply(evt(2, "dis", EventCardPlayed, map[string]any{
		"discardTop":        "red-7",
		"internalDebugFlag": true,
	}))
	if out.Kind != OutcomeDropped {
		t.Fatalf("kind=%s", out.Kind)
	}
	if out.Rejection == nil || out.Rejection.Code != RejectDisallowedField {
		t.Fatalf("rejection=%+v", out.Rejection)
	}
}

func TestPrivacy_SnapshotJSONHasNoPrivateKeys(t *testing.T) {
	t.Parallel()
	p := seededInProgress(t)
	mustApply(t, p, evt(3, "play", EventCardPlayed, map[string]any{
		"discardTop":      "blue-2",
		"activeColor":     "blue",
		"currentPlayerId": "p2",
		"direction":       "clockwise",
		"penaltyAmount":   0,
		"seats": []any{
			map[string]any{"seatIndex": 0, "playerId": "p1", "displayName": "Alice", "cardCount": 6},
			map[string]any{"seatIndex": 1, "playerId": "p2", "displayName": "Bob", "cardCount": 7},
		},
	}))

	raw, err := p.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	alias, err := p.MarshalSanitizedJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(alias) {
		t.Fatal("MarshalSanitizedJSON must match SnapshotJSON")
	}
	s := strings.ToLower(string(raw))
	forbidden := []string{
		`"hand"`, `"hands"`, `"cards"`, `"deck"`, `"hiddendeck"`,
		`"session"`, `"token"`, `"idempotency"`, `"audit"`, `"commandid"`,
		`"drawncards"`, `"password"`, `"secret"`, `"privatehand"`,
	}
	for _, key := range forbidden {
		if strings.Contains(s, key) {
			t.Fatalf("snapshot JSON leaked %s: %s", key, s)
		}
	}

	var snap SanitizedSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.RoomID != "room_1" {
		t.Fatalf("roomId=%s", snap.RoomID)
	}
	if snap.Discard.DiscardTop != "blue-2" {
		t.Fatalf("discard=%q", snap.Discard.DiscardTop)
	}
	if len(snap.Seats) != 2 || snap.Seats[0].CardCount != 6 {
		t.Fatalf("seats=%+v", snap.Seats)
	}
}

func TestPrivacy_SnapshotDefensiveCopy(t *testing.T) {
	t.Parallel()
	p := seededInProgress(t)
	mustApply(t, p, evt(3, "uno", EventUnoWindowOpened, map[string]any{
		"playerId":        "p1",
		"expiresAt":       "2026-05-17T12:00:09Z",
		"openingSequence": 3,
	}))
	snap := p.Snapshot()
	if len(snap.Seats) == 0 || snap.Uno == nil {
		t.Fatal("expected seats and uno window")
	}
	snap.Seats[0].CardCount = 99
	snap.Seats[0].DisplayName = "MUTATED"
	snap.GameScore["p1"] = 999
	snap.Uno.Called = true

	again := p.Snapshot()
	if again.Seats[0].CardCount == 99 || again.Seats[0].DisplayName == "MUTATED" {
		t.Fatal("snapshot seats not defensively copied")
	}
	if again.GameScore["p1"] == 999 {
		t.Fatal("snapshot gameScore not defensively copied")
	}
	if again.Uno == nil || again.Uno.Called {
		t.Fatal("snapshot uno not defensively copied")
	}
}
