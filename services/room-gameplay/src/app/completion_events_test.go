package app

import (
	"encoding/json"
	"testing"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
)

func TestBuildCompletionEvents_GameCompletedAllParticipants(t *testing.T) {
	room, out := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_gc", HostID: "host",
		Visibility: domain.VisibilityPublic, MaxSeats: 4,
	})
	if out.Rejection != nil {
		t.Fatal(out.Rejection)
	}
	_ = room.JoinRoom(domain.JoinRoomCommand{CommandID: "c1", PlayerID: "guest", ExpectedSequence: 1})
	_ = room.LockRoom(domain.LockRoomCommand{CommandID: "c2", ActorID: "host", ExpectedSequence: 2})

	g, err := game.StartGame("g1", []game.PlayerID{"host", "guest"}, game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  {{ID: "h1", Color: game.ColorRed, Face: game.Face3}},
			"guest": {{ID: "g1", Color: game.ColorBlue, Face: game.FaceSkip}, {ID: "g2", Color: game.ColorNone, Face: game.FaceWild}},
		},
		DiscardTop:  game.Card{ID: "d1", Color: game.ColorRed, Face: game.Face5},
		ActiveColor: game.ColorRed,
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	})
	if err != nil {
		t.Fatal(err)
	}
	play := g.PlayCard(game.PlayCardCommand{
		CommandID: "play", PlayerID: "host", CardID: "h1", ExpectedSequence: g.Sequence(),
		NowUTC: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	})
	if play.Rejection != nil {
		t.Fatalf("play: %+v", play.Rejection)
	}
	if !g.Completed() {
		t.Fatal("expected completed game")
	}

	facts := []domain.Fact{{
		Name: domain.FactGameCompleted,
		Data: map[string]string{"winner": "host", "gameId": "g1"},
	}}
	evs := BuildCompletionEvents(room, g, "g1", 10, "corr", "cmd-done", facts, time.Now().UTC())
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	var payload map[string]any
	if err := json.Unmarshal(evs[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	parts, ok := payload["participants"].([]any)
	if !ok || len(parts) < 2 {
		t.Fatalf("participants must include all players, got %v", payload["participants"])
	}
	order, _ := payload["placementOrder"].([]any)
	if len(order) < 2 {
		t.Fatalf("placementOrder=%v", payload["placementOrder"])
	}
	seen := map[string]bool{}
	for _, p := range parts {
		m := p.(map[string]any)
		pid, _ := m["playerId"].(string)
		if pid == "" {
			t.Fatalf("missing playerId in %v", m)
		}
		if m["placement"] == nil {
			t.Fatalf("missing placement in %v", m)
		}
		if _, has := m["cardPoints"]; !has {
			t.Fatalf("missing cardPoints in %v", m)
		}
		if m["outcome"] == nil || m["outcome"] == "" {
			t.Fatalf("missing outcome in %v", m)
		}
		seen[pid] = true
	}
	if !seen["host"] || !seen["guest"] {
		t.Fatalf("seen=%v", seen)
	}
}
