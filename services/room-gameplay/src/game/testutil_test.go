package game

import (
	"testing"
	"time"
)

func mustStart(t *testing.T, seats []PlayerID, deal DealMaterial) *Game {
	t.Helper()
	g, err := StartGame("g1", seats, deal)
	if err != nil {
		t.Fatalf("StartGame: %v", err)
	}
	return g
}

func card(id string, color Color, face Face) Card {
	return Card{ID: CardID(id), Color: color, Face: face}
}

func c(id string, color Color, face Face) Card { return card(id, color, face) }

func assertAccepted(t *testing.T, out CommandOutcome, facts ...FactName) {
	t.Helper()
	if out.Kind != OutcomeAccepted {
		t.Fatalf("kind=%s rej=%+v facts=%v", out.Kind, out.Rejection, factNames(out.Facts))
	}
	for _, want := range facts {
		if !hasFact(out.Facts, want) {
			t.Fatalf("missing fact %s in %v", want, factNames(out.Facts))
		}
	}
}

func assertRejected(t *testing.T, out CommandOutcome, code RejectionCode) {
	t.Helper()
	if out.Kind != OutcomeRejected || out.Rejection == nil {
		t.Fatalf("want rejected %s, got kind=%s rej=%+v", code, out.Kind, out.Rejection)
	}
	if out.Rejection.Code != code {
		t.Fatalf("code=%s want %s", out.Rejection.Code, code)
	}
	if len(out.Facts) != 0 {
		t.Fatalf("rejected must not emit facts: %v", factNames(out.Facts))
	}
}

func hasFact(facts []Fact, name FactName) bool {
	for _, f := range facts {
		if f.Name == name {
			return true
		}
	}
	return false
}

func factNames(facts []Fact) []FactName {
	out := make([]FactName, len(facts))
	for i, f := range facts {
		out[i] = f.Name
	}
	return out
}

func twoPlayerDeal(aHand, bHand []Card, top Card) DealMaterial {
	return DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": cloneHand(aHand),
			"b": cloneHand(bHand),
		},
		DiscardTop:  top,
		CurrentSeat: 0,
		Direction:   DirectionClockwise,
	}
}

func threePlayerDeal(hands map[PlayerID][]Card, top Card, current int) DealMaterial {
	cp := make(map[PlayerID][]Card, len(hands))
	for k, v := range hands {
		cp[k] = cloneHand(v)
	}
	return DealMaterial{
		Hands:       cp,
		DiscardTop:  top,
		CurrentSeat: current,
		Direction:   DirectionClockwise,
	}
}

func twoPlayer(discard Card, active Color, handA, handB []Card) (*Game, []PlayerID) {
	seats := []PlayerID{"a", "b"}
	g, err := StartGame("g1", seats, DealMaterial{
		Hands:       map[PlayerID][]Card{"a": cloneHand(handA), "b": cloneHand(handB)},
		DiscardTop:  discard,
		ActiveColor: active,
		CurrentSeat: 0,
		Direction:   DirectionClockwise,
	})
	if err != nil {
		panic(err)
	}
	return g, seats
}

func play(g *Game, cmdID string, player PlayerID, cardID string) CommandOutcome {
	return g.PlayCard(PlayCardCommand{
		CommandID:        CommandID(cmdID),
		PlayerID:         player,
		CardID:           CardID(cardID),
		ExpectedSequence: g.Sequence(),
		NowUTC:           time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	})
}

func draw(g *Game, cmdID string, player PlayerID, cards ...Card) CommandOutcome {
	return g.DrawCard(DrawCardCommand{
		CommandID:        CommandID(cmdID),
		PlayerID:         player,
		Cards:            cards,
		ExpectedSequence: g.Sequence(),
	})
}

func choose(g *Game, cmdID string, player PlayerID, color Color) CommandOutcome {
	return g.ChooseColor(ChooseColorCommand{
		CommandID:        CommandID(cmdID),
		PlayerID:         player,
		Color:            color,
		ExpectedSequence: g.Sequence(),
	})
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func cardIDs(hand []Card) []CardID {
	ids := make([]CardID, len(hand))
	for i, card := range hand {
		ids[i] = card.ID
	}
	return ids
}

func equalIDs(a, b []CardID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
