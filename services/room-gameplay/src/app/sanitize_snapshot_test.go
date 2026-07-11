package app

import (
	"encoding/json"
	"strings"
	"testing"

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
	evs, playerHigh := BuildFeedEvents(sess, 1, 1, "corr", "c0", facts, []FeedAudience{
		{PlayerID: "host", SessionID: "s1"},
		{PlayerID: "guest", SessionID: "s2"},
	}, 0)
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
			if data["status"] != "waiting" {
				t.Fatalf("status=%v", data["status"])
			}
			if _, ok := data["sessionId"]; ok {
				t.Fatal("session leak")
			}
			roster, _ := data["roster"].([]any)
			if len(roster) < 1 {
				t.Fatalf("roster=%v", data["roster"])
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
	evs, _ := BuildFeedEvents(sess, int64(room.Sequence()), 1, "c", "cancel", nil, nil, 0)
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
}
