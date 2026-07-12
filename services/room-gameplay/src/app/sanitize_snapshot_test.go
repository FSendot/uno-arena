package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
)

func TestBuildFeedEvents_OneSpectatorSnapshotPerCommand(t *testing.T) {
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_ss", HostID: "host",
		Visibility: domain.VisibilityPublic, MaxSeats: 4,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	_ = room.JoinRoom(domain.JoinRoomCommand{CommandID: "c1", PlayerID: "guest", ExpectedSequence: 1})
	sess := domain.OpenSession(room)

	facts := out.Facts
	at := time.Date(2026, 7, 11, 15, 0, 0, 123456789, time.UTC)
	evs, playerHigh := BuildFeedEvents(sess, 1, 1, "corr", "c0", facts, []FeedAudience{
		{PlayerID: "host", SessionID: "s1"},
		{PlayerID: "guest", SessionID: "s2"},
	}, 0, at)
	var spec int
	var player int
	for _, ev := range evs {
		switch ev.Stream {
		case StreamSpectator:
			spec++
			if ev.SequenceNumber != 1 {
				t.Fatalf("spectator seq=%d want room seq 1", ev.SequenceNumber)
			}
			if ev.EventType != EventSnapshotSanitized {
				t.Fatalf("type=%s", ev.EventType)
			}
			var data map[string]any
			if err := json.Unmarshal(ev.Payload, &data); err != nil {
				t.Fatal(err)
			}
			assertSpectatorSafeEnvelope(t, data, "c0-ss", EventSnapshotSanitized, "room_ss", 1, "corr", "c0", at)
			payload, _ := data["payload"].(map[string]any)
			if payload["status"] != "waiting" {
				t.Fatalf("status=%v", payload["status"])
			}
			if _, ok := payload["sessionId"]; ok {
				t.Fatal("session leak")
			}
			roster, _ := payload["roster"].([]any)
			if len(roster) < 1 {
				t.Fatalf("roster=%v", payload["roster"])
			}
		case StreamPlayer:
			player++
		}
	}
	if spec != 1 {
		t.Fatalf("spectator events=%d want 1", spec)
	}
	if player < 2 {
		t.Fatalf("player events=%d want >=2 (2 audiences)", player)
	}
	if playerHigh < 1 {
		t.Fatalf("player high-water=%d", playerHigh)
	}
}

func TestBuildFeedEvents_SpectatorSafeEventCanonicalEnvelope(t *testing.T) {
	room, _ := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_env", HostID: "host", MaxSeats: 4,
	})
	_ = room.JoinRoom(domain.JoinRoomCommand{CommandID: "c1", PlayerID: "guest", ExpectedSequence: 1})
	_ = room.LockRoom(domain.LockRoomCommand{CommandID: "c2", ActorID: "host", ExpectedSequence: 2})
	sess := domain.OpenSession(room)
	out := sess.StartMatch(domain.StartMatchCommand{
		CommandID: "st", ActorID: "host", GameID: "g1", ExpectedSequence: 3,
	}, game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  {{ID: "secret-host", Color: game.ColorRed, Face: game.Face3}},
			"guest": {{ID: "secret-guest", Color: game.ColorBlue, Face: game.Face2}},
		},
		DiscardTop:  game.Card{ID: "secret-disc", Color: game.ColorRed, Face: game.Face5},
		ActiveColor: game.ColorRed,
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	})
	if out.Rejection != nil {
		t.Fatalf("start: %+v", out.Rejection)
	}
	at := time.Date(2026, 7, 11, 16, 30, 0, 0, time.UTC)
	evs, _ := BuildFeedEvents(sess, int64(room.Sequence()), 1, "corr-env", "st", out.Facts, nil, 2, at)
	var spec *PublishedEvent
	for i := range evs {
		if evs[i].Stream == StreamSpectator {
			spec = &evs[i]
			break
		}
	}
	if spec == nil {
		t.Fatal("missing spectator event")
	}
	var envelope map[string]any
	if err := json.Unmarshal(spec.Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	assertSpectatorSafeEnvelope(t, envelope, "st-ss", EventSnapshotSanitized, "room_env", int64(room.Sequence()), "corr-env", "st", at)

	payload, ok := envelope["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload want object, got %T", envelope["payload"])
	}
	raw, _ := json.Marshal(payload)
	s := string(raw)
	for _, leak := range []string{"secret-host", "secret-guest", "secret-disc", "sessionId", `"hand"`, `"hands"`} {
		if contains(s, leak) {
			t.Fatalf("private leak %q in nested payload %s", leak, s)
		}
	}
	if payload["discardTop"] != "red-5" {
		t.Fatalf("discardTop=%v", payload["discardTop"])
	}
	// Nested payload must not be double-wrapped.
	if _, nested := payload["payload"]; nested {
		t.Fatal("nested payload must not contain another payload wrapper")
	}
}

func TestBuildPublicSpectatorSnapshot_NoCardIdentities(t *testing.T) {
	room, _ := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_pub", HostID: "host", MaxSeats: 4,
	})
	_ = room.JoinRoom(domain.JoinRoomCommand{CommandID: "c1", PlayerID: "guest", ExpectedSequence: 1})
	_ = room.LockRoom(domain.LockRoomCommand{CommandID: "c2", ActorID: "host", ExpectedSequence: 2})
	sess := domain.OpenSession(room)
	out := sess.StartMatch(domain.StartMatchCommand{
		CommandID: "st", ActorID: "host", GameID: "g1", ExpectedSequence: 3,
	}, game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  {{ID: "secret-host", Color: game.ColorRed, Face: game.Face3}},
			"guest": {{ID: "secret-guest", Color: game.ColorBlue, Face: game.Face2}},
		},
		DiscardTop:  game.Card{ID: "secret-disc", Color: game.ColorRed, Face: game.Face5},
		ActiveColor: game.ColorRed,
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	})
	if out.Rejection != nil {
		t.Fatalf("start: %+v", out.Rejection)
	}
	data := BuildPublicSpectatorSnapshot(sess)
	raw, _ := json.Marshal(data)
	s := string(raw)
	for _, leak := range []string{"secret-host", "secret-guest", "secret-disc", "sessionId", `"hand"`} {
		if contains(s, leak) {
			t.Fatalf("leak %q in %s", leak, s)
		}
	}
	if data["discardTop"] != "red-5" {
		t.Fatalf("discardTop=%v", data["discardTop"])
	}
	if data["activeColor"] != "red" {
		t.Fatalf("activeColor=%v", data["activeColor"])
	}
	if data["currentPlayerId"] != "host" {
		t.Fatalf("current=%v", data["currentPlayerId"])
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}

func TestBuildFeedEvents_TerminalRoomCompleted(t *testing.T) {
	room, _ := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_term", HostID: "host", MaxSeats: 4,
	})
	_ = room.CancelRoom(domain.CancelRoomCommand{
		CommandID: "cancel", ActorID: "host", ExpectedSequence: 1,
	})
	sess := domain.OpenSession(room)
	at := time.Unix(2, 0).UTC()
	evs, _ := BuildFeedEvents(sess, int64(room.Sequence()), 1, "c", "cancel", nil, nil, 0, at)
	if len(evs) != 1 || evs[0].EventType != EventRoomCancelled {
		t.Fatalf("events=%v type=%v", len(evs), func() string {
			if len(evs) == 0 {
				return ""
			}
			return evs[0].EventType
		}())
	}
	if evs[0].SequenceNumber != int64(room.Sequence()) {
		t.Fatalf("seq=%d room=%d", evs[0].SequenceNumber, room.Sequence())
	}
	var data map[string]any
	if err := json.Unmarshal(evs[0].Payload, &data); err != nil {
		t.Fatal(err)
	}
	assertSpectatorSafeEnvelope(t, data, "cancel-ss", EventRoomCancelled, "room_term", int64(room.Sequence()), "c", "cancel", at)
}

func assertSpectatorSafeEnvelope(t *testing.T, data map[string]any, eventID, eventType, roomID string, seq int64, corr, causation string, at time.Time) {
	t.Helper()
	wantKeys := []string{
		"schemaVersion", "eventId", "eventType", "roomId", "sequenceNumber",
		"correlationId", "causationId", "occurredAt", "payload",
	}
	for _, k := range wantKeys {
		if _, ok := data[k]; !ok {
			t.Fatalf("missing envelope key %q in %v", k, data)
		}
	}
	if int(data["schemaVersion"].(float64)) != 1 {
		t.Fatalf("schemaVersion=%v", data["schemaVersion"])
	}
	if data["eventId"] != eventID {
		t.Fatalf("eventId=%v want %s", data["eventId"], eventID)
	}
	if data["eventType"] != eventType {
		t.Fatalf("eventType=%v want %s", data["eventType"], eventType)
	}
	if data["roomId"] != roomID {
		t.Fatalf("roomId=%v want %s", data["roomId"], roomID)
	}
	if int64(data["sequenceNumber"].(float64)) != seq {
		t.Fatalf("sequenceNumber=%v want %d", data["sequenceNumber"], seq)
	}
	if data["correlationId"] != corr {
		t.Fatalf("correlationId=%v", data["correlationId"])
	}
	if data["causationId"] != causation {
		t.Fatalf("causationId=%v", data["causationId"])
	}
	if data["occurredAt"] != at.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("occurredAt=%v want %s", data["occurredAt"], at.UTC().Format(time.RFC3339Nano))
	}
	if _, ok := data["payload"].(map[string]any); !ok {
		t.Fatalf("payload want object, got %T (%v)", data["payload"], data["payload"])
	}
}

func TestBuildFeedEvents_ZeroOccurredAtDeterministic(t *testing.T) {
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_zero_at", HostID: "host",
		Visibility: domain.VisibilityPublic, MaxSeats: 2,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	sess := domain.OpenSession(room)
	evs, _ := BuildFeedEvents(sess, 1, 1, "corr", "c0", out.Facts, []FeedAudience{
		{PlayerID: "host", SessionID: "s1"},
	}, 0, time.Time{})
	if len(evs) == 0 {
		t.Fatal("expected feed events")
	}
	want := time.Time{}.UTC().Format(time.RFC3339Nano)
	for _, ev := range evs {
		if ev.OccurredAt != want {
			t.Fatalf("OccurredAt=%q want deterministic zero %q (no wall-clock fallback)", ev.OccurredAt, want)
		}
	}
}
