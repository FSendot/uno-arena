package store

import (
	"testing"

	"unoarena/services/room-gameplay/game"
)

func TestEncodeDecodeGame_DrawPileSizeRoundTrip(t *testing.T) {
	g, err := game.StartGame("g1", []game.PlayerID{"a", "b"}, game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"a": {{ID: "a1", Color: game.ColorRed, Face: game.Face1}},
			"b": {{ID: "b1", Color: game.ColorBlue, Face: game.Face2}},
		},
		DiscardTop:      game.Card{ID: "d1", Color: game.ColorRed, Face: game.Face3},
		ActiveColor:     game.ColorRed,
		CurrentSeat:     0,
		Direction:       game.DirectionClockwise,
		DrawPileSize:    88,
		HasDrawPileSize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeGame(g)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.DrawPileSize() != 88 {
		t.Fatalf("drawPileSize=%d want 88", got.DrawPileSize())
	}
}

func TestDecodeGame_RejectsNegativeDrawPileSize(t *testing.T) {
	_, err := decodeGame([]byte(`{"id":"g1","seats":["a","b"],"hands":{},"discard":{"id":"d","color":"red","face":"1"},"active":"red","dir":1,"current":0,"sequence":1,"drawPileSize":-1}`))
	if err == nil {
		t.Fatal("expected rejection")
	}
}
