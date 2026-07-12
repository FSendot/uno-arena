package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/envelope"
)

var errConfirmDown = errors.New("confirm down")

func TestDrawPileSize_AcceptedDrawUpdatesAndRetryIdempotent(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)

	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	afterDeal := live.Game().DrawPileSize()
	// defaultTwoPlusDeal: 2 seats × 2 cards + discard = 5 → 108-5 = 103
	if afterDeal != 103 {
		t.Fatalf("after deal drawPileSize=%d want 103", afterDeal)
	}

	seq := currentSeq(t, env, "room_rec")
	mustAccept(t, env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-1", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	}))

	live, _ = env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	afterDraw := live.Game().DrawPileSize()
	if afterDraw != 102 {
		t.Fatalf("after draw drawPileSize=%d want 102", afterDraw)
	}

	retry := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-1", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if retry.Err != nil {
		t.Fatal(retry.Err)
	}
	live, _ = env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	if live.Game().DrawPileSize() != afterDraw {
		t.Fatalf("retry changed drawPileSize to %d", live.Game().DrawPileSize())
	}
}

func TestDrawPileSize_RejectedDrawDoesNotChange(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)
	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	before := live.Game().DrawPileSize()

	seq := currentSeq(t, env, "room_rec")
	rej := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-bad", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "guest", SessionID: "s2", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if rej.Err != nil {
		t.Fatal(rej.Err)
	}
	if rej.Result.Status != envelope.StatusRejected {
		t.Fatalf("want rejected, got %+v", rej.Result)
	}
	live, _ = env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	if live.Game().DrawPileSize() != before {
		t.Fatalf("rejected draw changed pile %d → %d", before, live.Game().DrawPileSize())
	}
	if env.deals.PendingLen() != 0 {
		t.Fatalf("pending left=%d", env.deals.PendingLen())
	}
}

func TestDrawPileSize_ConfirmFailureDoesNotChange(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)
	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	before := live.Game().DrawPileSize()
	env.deals.FailConfirm = errConfirmDown

	seq := currentSeq(t, env, "room_rec")
	res := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-fail", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if res.Err == nil {
		t.Fatal("confirm failure must surface")
	}
	live, _ = env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	if live.Game().DrawPileSize() != before {
		t.Fatalf("failed confirm changed pile %d → %d", before, live.Game().DrawPileSize())
	}
}

func TestDrawPileSize_SnapshotsIncludeCountOnly(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)

	snap, err := env.svc.PlayerSnapshot(context.Background(), "room_rec", "host")
	if err != nil {
		t.Fatal(err)
	}
	gameObj, _ := snap["game"].(map[string]any)
	if _, ok := gameObj["drawPileSize"]; !ok {
		t.Fatalf("player game missing drawPileSize: %+v", gameObj)
	}
	raw, _ := json.Marshal(snap)
	for _, leak := range []string{"seedCommitment", "shuffledOrder", "deckOrder"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("player snapshot leak %q", leak)
		}
	}

	live, _ := env.sessions.Get(context.Background(), domain.RoomID("room_rec"))
	pub := BuildPublicSpectatorSnapshot(live)
	if _, ok := pub["drawPileSize"]; !ok {
		t.Fatalf("spectator missing drawPileSize: %+v", pub)
	}
	pubRaw, _ := json.Marshal(pub)
	if strings.Contains(string(pubRaw), "drawPileCards") || strings.Contains(string(pubRaw), `"hand"`) {
		t.Fatalf("spectator must not expose pile/hand identities: %s", pubRaw)
	}
}

func TestDrawPileSize_CloneAndRestoreRoundTrip(t *testing.T) {
	g, err := game.StartGame("g1", []game.PlayerID{"a", "b"}, game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"a": {{ID: "a1", Color: game.ColorRed, Face: game.Face1}},
			"b": {{ID: "b1", Color: game.ColorBlue, Face: game.Face2}},
		},
		DiscardTop:      game.Card{ID: "d1", Color: game.ColorRed, Face: game.Face3},
		ActiveColor:     game.ColorRed,
		CurrentSeat:     0,
		Direction:       game.DirectionClockwise,
		DrawPileSize:    97,
		HasDrawPileSize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if g.Clone().DrawPileSize() != 97 {
		t.Fatalf("clone=%d", g.Clone().DrawPileSize())
	}
	restored := game.RestoreGame(game.RestoreGameInput{
		ID: g.ID(), Seats: g.Seats(), Hands: g.HandsMap(), Discard: g.DiscardTop(),
		Active: g.ActiveColor(), Dir: g.Direction(), Current: g.CurrentSeatIndex(),
		Sequence: g.Sequence(), Completed: g.Completed(), Abandoned: g.Abandoned(),
		Placement: g.PlacementOrder(), CardPoints: g.CardPoints(), Outcomes: g.OutcomesMap(),
		DrawPileSize: g.DrawPileSize(),
	})
	if restored.DrawPileSize() != 97 {
		t.Fatalf("restore=%d", restored.DrawPileSize())
	}
}

func TestFakeDealSource_RemainingConsistentOnDuplicate(t *testing.T) {
	deals := NewFakeDealSource()
	ctx := context.Background()
	first, err := deals.ReserveDraw(ctx, "r", "g", StableOperationID("c1", PurposeDraw), 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.DrawPileSize != 106 {
		t.Fatalf("remaining=%d want 106", first.DrawPileSize)
	}
	dup, err := deals.ReserveDraw(ctx, "r", "g", StableOperationID("c1", PurposeDraw), 2)
	if err != nil {
		t.Fatal(err)
	}
	if dup.DrawPileSize != first.DrawPileSize {
		t.Fatalf("dup remaining=%d want %d", dup.DrawPileSize, first.DrawPileSize)
	}
}
