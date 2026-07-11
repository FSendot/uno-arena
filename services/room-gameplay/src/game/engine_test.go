package game

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStandardDeckComposition_108(t *testing.T) {
	deck := StandardDeckComposition()
	if len(deck) != 108 {
		t.Fatalf("len=%d", len(deck))
	}
	var wilds, wd4, zeros int
	colored := map[Color]int{}
	for _, card := range deck {
		switch card.Face {
		case FaceWild:
			wilds++
		case FaceWildDrawFour:
			wd4++
		case Face0:
			zeros++
			colored[card.Color]++
		default:
			if card.Color != ColorNone {
				colored[card.Color]++
			}
		}
	}
	if wilds != 4 || wd4 != 4 || zeros != 4 {
		t.Fatalf("wilds=%d wd4=%d zeros=%d", wilds, wd4, zeros)
	}
	for _, color := range AllColors {
		if colored[color] != 25 {
			t.Fatalf("%s count=%d want 25", color, colored[color])
		}
	}
}

func TestCardPointsTable(t *testing.T) {
	cases := []struct {
		card Card
		want int
	}{
		{c("0", ColorRed, Face0), 0},
		{c("7", ColorRed, Face7), 7},
		{c("s", ColorRed, FaceSkip), 20},
		{c("r", ColorRed, FaceReverse), 20},
		{c("d", ColorRed, FaceDrawTwo), 20},
		{c("w", ColorNone, FaceWild), 50},
		{c("4", ColorNone, FaceWildDrawFour), 50},
	}
	for _, tc := range cases {
		if got := tc.card.Points(); got != tc.want {
			t.Fatalf("%v points=%d want %d", tc.card, got, tc.want)
		}
	}
}

func TestLegalPlayAndExactMatchTable(t *testing.T) {
	discard := c("d", ColorRed, Face5)
	cases := []struct {
		name string
		card Card
		act  Color
		want bool
	}{
		{"color", c("x", ColorRed, Face2), ColorRed, true},
		{"face", c("x", ColorBlue, Face5), ColorRed, true},
		{"active", c("x", ColorGreen, Face1), ColorGreen, true},
		{"miss", c("x", ColorBlue, Face3), ColorRed, false},
		{"wild", c("w", ColorNone, FaceWild), ColorRed, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canPlayOrdinary(tc.card, discard, tc.act); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	if !ExactMatch(c("a", ColorRed, Face7), c("b", ColorRed, Face7)) {
		t.Fatal("exact")
	}
	if ExactMatch(c("a", ColorBlue, Face7), c("b", ColorRed, Face7)) {
		t.Fatal("color")
	}
	if ExactMatch(c("w", ColorNone, FaceWild), c("b", ColorRed, Face7)) {
		t.Fatal("wild")
	}
	if ExactMatch(c("a", ColorRed, FaceSkip), c("b", ColorRed, FaceReverse)) {
		t.Fatal("different face")
	}
}

func TestStartGame_InjectedDeal(t *testing.T) {
	g := mustStart(t, []PlayerID{"a", "b"}, twoPlayerDeal(
		[]Card{c("a1", ColorRed, Face1)},
		[]Card{c("b1", ColorGreen, Face3)},
		c("d", ColorRed, Face5),
	))
	if g.ID() != "g1" || g.Sequence() != 1 {
		t.Fatalf("id=%s seq=%d", g.ID(), g.Sequence())
	}
	if g.ActiveColor() != ColorRed || g.CurrentPlayer() != "a" {
		t.Fatalf("active=%s current=%s", g.ActiveColor(), g.CurrentPlayer())
	}
}

func TestPlayCard_TurnSkipReverse(t *testing.T) {
	t.Run("number advances", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorBlue, Face1)},
			[]Card{c("b1", ColorGreen, Face2)},
		)
		assertAccepted(t, play(g, "p", "a", "a1"), FactCardPlayed, FactTurnAdvanced)
		if g.CurrentPlayer() != "b" || g.ActiveColor() != ColorRed {
			t.Fatalf("current=%s color=%s", g.CurrentPlayer(), g.ActiveColor())
		}
	})
	t.Run("skip three players", func(t *testing.T) {
		g := mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
			Hands: map[PlayerID][]Card{
				"a": {c("sk", ColorRed, FaceSkip), c("a2", ColorYellow, Face9)},
				"b": {c("b1", ColorBlue, Face1)},
				"c": {c("c1", ColorGreen, Face1)},
			},
			DiscardTop: c("d", ColorRed, Face1), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
		})
		assertAccepted(t, play(g, "1", "a", "sk"), FactTurnAdvanced)
		if g.CurrentPlayer() != "c" {
			t.Fatalf("got %s", g.CurrentPlayer())
		}
	})
	t.Run("reverse four players", func(t *testing.T) {
		g := mustStart(t, []PlayerID{"a", "b", "c", "d"}, DealMaterial{
			Hands: map[PlayerID][]Card{
				"a": {c("rv", ColorRed, FaceReverse), c("a2", ColorYellow, Face9)},
				"b": {c("b1", ColorBlue, Face1)},
				"c": {c("c1", ColorGreen, Face1)},
				"d": {c("d1", ColorYellow, Face1)},
			},
			DiscardTop: c("d", ColorRed, Face3), CurrentSeat: 0, Direction: DirectionClockwise,
		})
		assertAccepted(t, play(g, "1", "a", "rv"), FactTurnAdvanced)
		if g.Direction() != DirectionCounterClockwise || g.CurrentPlayer() != "d" {
			t.Fatalf("dir=%d current=%s", g.Direction(), g.CurrentPlayer())
		}
	})
	t.Run("two player reverse acts as skip", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face3), ColorRed,
			[]Card{c("rv", ColorRed, FaceReverse), c("a2", ColorYellow, Face9)},
			[]Card{c("b1", ColorBlue, Face1)},
		)
		assertAccepted(t, play(g, "1", "a", "rv"), FactCardPlayed)
		if g.CurrentPlayer() != "a" {
			t.Fatalf("2p reverse should return to a, got %s", g.CurrentPlayer())
		}
	})
}

func TestPlayCard_Rejects(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorBlue, Face3)},
		[]Card{c("b1", ColorRed, Face2)},
	)
	seq := g.Sequence()
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "bad", PlayerID: "a", CardID: "a1", ExpectedSequence: seq}), RejectIllegalCard)
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "own", PlayerID: "a", CardID: "b1", ExpectedSequence: seq}), RejectNotInHand)
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "stale", PlayerID: "a", CardID: "a1", ExpectedSequence: seq - 1}), RejectSequenceRequired) // seq-1 == 0
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "fut", PlayerID: "a", CardID: "a1", ExpectedSequence: seq + 1}), RejectFutureSequence)
	if g.Sequence() != seq {
		t.Fatal("mutated")
	}
}

func TestWildRequiresColor(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("w", ColorNone, FaceWild), c("a2", ColorBlue, Face1)},
		[]Card{c("b1", ColorGreen, Face2)},
	)
	assertAccepted(t, play(g, "w", "a", "w"), FactCardPlayed)
	if !g.PendingColorChoice() {
		t.Fatal("pending")
	}
	assertRejected(t, play(g, "x", "a", "a2"), RejectPendingColor)
	assertRejected(t, choose(g, "bad", "a", "purple"), RejectInvalidColor)
	assertAccepted(t, choose(g, "c", "a", ColorBlue), FactColorChosen, FactTurnAdvanced)
	if g.ActiveColor() != ColorBlue || g.CurrentPlayer() != "b" {
		t.Fatalf("color=%s current=%s", g.ActiveColor(), g.CurrentPlayer())
	}
}

func TestWildDrawFour_IllegalWhenMatchingColorHeld(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face3), ColorRed,
		[]Card{c("wd4", ColorNone, FaceWildDrawFour), c("r2", ColorRed, Face2)},
		[]Card{c("b1", ColorBlue, Face1)},
	)
	assertRejected(t, play(g, "1", "a", "wd4"), RejectIllegalCard)
}

func TestDrawStacking_MixedAndDecline(t *testing.T) {
	g := mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": {c("d2a", ColorRed, FaceDrawTwo), c("a2", ColorYellow, Face9)},
			"b": {c("d2b", ColorBlue, FaceDrawTwo), c("b2", ColorGreen, Face1)},
			"c": {c("c1", ColorYellow, Face1)},
		},
		DiscardTop: c("d", ColorRed, Face1), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
	})
	out := play(g, "1", "a", "d2a")
	assertAccepted(t, out, FactPenaltyStackIncreased)
	if g.PenaltyAmount() != 2 || g.PenaltyTarget() != "b" {
		t.Fatalf("penalty %d target %s", g.PenaltyAmount(), g.PenaltyTarget())
	}
	assertRejected(t, play(g, "j", "c", "c1"), RejectJumpInBlocked)

	out = play(g, "2", "b", "d2b")
	assertAccepted(t, out, FactPenaltyStackIncreased, FactCardPlayed)
	mode := ""
	for _, f := range out.Facts {
		if f.Name == FactCardPlayed {
			mode = f.Data["playMode"]
		}
	}
	if mode != string(PlayModeStack) {
		t.Fatalf("stack playMode=%q want stack", mode)
	}
	if g.PenaltyAmount() != 4 || g.CurrentPlayer() != "c" {
		t.Fatalf("amount=%d current=%s", g.PenaltyAmount(), g.CurrentPlayer())
	}

	batch := []Card{c("x1", ColorRed, Face1), c("x2", ColorRed, Face2), c("x3", ColorRed, Face3), c("x4", ColorRed, Face4)}
	assertAccepted(t, draw(g, "3", "c", batch...), FactCardDrawn, FactPenaltyStackResolved, FactTurnAdvanced)
	if g.PenaltyAmount() != 0 || len(g.Hand("c")) != 5 || g.CurrentPlayer() != "a" {
		t.Fatalf("after decline penalty=%d hand=%d current=%s", g.PenaltyAmount(), len(g.Hand("c")), g.CurrentPlayer())
	}
}

func TestWildDrawFour_StackOnDrawTwo(t *testing.T) {
	g := mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": {c("d2", ColorRed, FaceDrawTwo), c("a2", ColorYellow, Face9)},
			"b": {c("wd4", ColorNone, FaceWildDrawFour), c("b2", ColorYellow, Face8)},
			"c": {c("c1", ColorBlue, Face1)},
		},
		DiscardTop: c("d", ColorRed, Face1), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
	})
	play(g, "1", "a", "d2")
	assertAccepted(t, play(g, "2", "b", "wd4"), FactCardPlayed)
	if !g.PendingColorChoice() {
		t.Fatal("color pending")
	}
	assertAccepted(t, choose(g, "3", "b", ColorBlue), FactColorChosen)
	if g.PenaltyAmount() != 6 || g.CurrentPlayer() != "c" {
		t.Fatalf("amount=%d current=%s", g.PenaltyAmount(), g.CurrentPlayer())
	}
}

func TestJumpIn_ExactMatchAndStaleRace(t *testing.T) {
	makeG := func() *Game {
		return mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
			Hands: map[PlayerID][]Card{
				"a": {c("a1", ColorRed, Face3), c("a2", ColorYellow, Face9)},
				"b": {c("b1", ColorBlue, Face2)},
				"c": {c("c1", ColorRed, Face7), c("c2", ColorYellow, Face1)},
			},
			DiscardTop: c("d", ColorRed, Face7), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
		})
	}
	g := makeG()
	seq := g.Sequence()
	out := g.PlayCard(PlayCardCommand{CommandID: "j", PlayerID: "c", CardID: "c1", ExpectedSequence: seq})
	assertAccepted(t, out, FactCardPlayed, FactTurnAdvanced)
	mode := ""
	for _, f := range out.Facts {
		if f.Name == FactCardPlayed {
			mode = f.Data["playMode"]
		}
	}
	if mode != string(PlayModeJumpIn) {
		t.Fatalf("mode=%s", mode)
	}
	if g.CurrentPlayer() != "a" {
		t.Fatalf("resume after jumper want a got %s", g.CurrentPlayer())
	}
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "stale", PlayerID: "a", CardID: "a1", ExpectedSequence: seq}), RejectStaleSequence)

	for _, order := range [][]string{{"turn", "jump"}, {"jump", "turn"}} {
		t.Run(order[0]+"_then_"+order[1], func(t *testing.T) {
			g := makeG()
			seq := g.Sequence()
			run := func(name string) CommandOutcome {
				if name == "turn" {
					return g.PlayCard(PlayCardCommand{CommandID: CommandID(name), PlayerID: "a", CardID: "a1", ExpectedSequence: seq})
				}
				return g.PlayCard(PlayCardCommand{CommandID: CommandID(name), PlayerID: "c", CardID: "c1", ExpectedSequence: seq})
			}
			o1 := run(order[0])
			o2 := run(order[1])
			if o1.Kind != OutcomeAccepted {
				t.Fatalf("first: %+v", o1)
			}
			assertRejected(t, o2, RejectStaleSequence)
		})
	}
}

func TestUnoWindow_CallChallengeExpireNextTurn(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)

	t.Run("open public expiresAt and opening sequence", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorRed, Face4)},
			[]Card{c("b1", ColorBlue, Face9)},
		)
		before := g.Sequence()
		assertAccepted(t, g.PlayCard(PlayCardCommand{CommandID: "1", PlayerID: "a", CardID: "a1", ExpectedSequence: before, NowUTC: now}), FactUnoWindowOpened, FactTurnAdvanced)
		pub := g.PublicSnapshot()
		if pub.Uno == nil || pub.Uno.PlayerID != "a" || pub.Uno.ExpiresAt != now.Add(UnoWindowDuration) {
			t.Fatalf("%+v", pub.Uno)
		}
		if pub.Uno.OpeningSequence != before+1 || pub.Sequence != before+1 {
			t.Fatalf("opening=%d seq=%d", pub.Uno.OpeningSequence, pub.Sequence)
		}
		if g.UnoWindow() == nil {
			t.Fatal("window must stay open after opening play")
		}
	})

	t.Run("CallUno closes window; later ReportMissingUno inactive no challenger penalty", func(t *testing.T) {
		// Settled: successful CallUno closes/resolves the window; later ReportMissingUno
		// is RejectUnoWindowInactive with no challenger penalty and no domain facts.
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorRed, Face4)},
			[]Card{c("b1", ColorBlue, Face9)},
		)
		g.PlayCard(PlayCardCommand{CommandID: "1", PlayerID: "a", CardID: "a1", ExpectedSequence: g.Sequence(), NowUTC: now})
		assertAccepted(t, g.CallUno(CallUnoCommand{CommandID: "call", PlayerID: "a", ExpectedSequence: g.Sequence(), NowUTC: now.Add(time.Second)}), FactUnoCalled)
		if g.UnoWindow() != nil {
			t.Fatal("CallUno must resolve/close the missing-Uno challenge window")
		}
		beforeA, beforeB, beforeSeq := len(g.Hand("a")), len(g.Hand("b")), g.Sequence()
		idsA, idsB := cardIDs(g.Hand("a")), cardIDs(g.Hand("b"))
		batch := []Card{c("p1", ColorYellow, Face1), c("p2", ColorYellow, Face2)}
		out := g.ReportMissingUno(ReportMissingUnoCommand{
			CommandID: "ch", ChallengerID: "b", TargetID: "a", Cards: batch,
			ExpectedSequence: g.Sequence(), NowUTC: now.Add(2 * time.Second),
		})
		assertRejected(t, out, RejectUnoWindowInactive)
		if len(out.Facts) != 0 || hasFact(out.Facts, FactUnoPenaltyApplied) || hasFact(out.Facts, FactUnoChallengeIssued) {
			t.Fatalf("post-CallUno challenge must emit no facts: %v", factNames(out.Facts))
		}
		if len(g.Hand("a")) != beforeA || len(g.Hand("b")) != beforeB {
			t.Fatalf("hands mutated after closed challenge: a=%d b=%d", len(g.Hand("a")), len(g.Hand("b")))
		}
		if !equalIDs(cardIDs(g.Hand("a")), idsA) || !equalIDs(cardIDs(g.Hand("b")), idsB) {
			t.Fatal("neither target nor challenger may draw after successful CallUno")
		}
		if g.Sequence() != beforeSeq {
			t.Fatal("sequence bumped on rejected challenge after CallUno")
		}
		if g.UnoWindow() != nil {
			t.Fatal("rejected post-CallUno challenge must not reopen the window")
		}
	})

	t.Run("valid challenge target draws 2 with cardsDrawn payload", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorRed, Face4)},
			[]Card{c("b1", ColorBlue, Face9)},
		)
		g.PlayCard(PlayCardCommand{CommandID: "1", PlayerID: "a", CardID: "a1", ExpectedSequence: g.Sequence(), NowUTC: now})
		beforeB := len(g.Hand("b"))
		batch := []Card{c("p1", ColorYellow, Face1), c("p2", ColorYellow, Face2)}
		out := g.ReportMissingUno(ReportMissingUnoCommand{
			CommandID: "ch", ChallengerID: "b", TargetID: "a", Cards: batch,
			ExpectedSequence: g.Sequence(), NowUTC: now.Add(time.Second),
		})
		assertAccepted(t, out, FactUnoChallengeIssued, FactUnoPenaltyApplied)
		if len(g.Hand("a")) != 3 {
			t.Fatalf("hand=%d", len(g.Hand("a")))
		}
		if len(g.Hand("b")) != beforeB {
			t.Fatalf("challenger must not draw on valid challenge: b=%d", len(g.Hand("b")))
		}
		drawn, target := "", ""
		for _, f := range out.Facts {
			if f.Name == FactUnoPenaltyApplied {
				drawn = f.Data["cardsDrawn"]
				target = f.Data["targetPlayerId"]
			}
		}
		if drawn != "2" {
			t.Fatalf("cardsDrawn=%q want 2", drawn)
		}
		if target != "a" {
			t.Fatalf("penalty target=%q want a", target)
		}
	})

	t.Run("expire", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorRed, Face4)},
			[]Card{c("b1", ColorBlue, Face9)},
		)
		g.PlayCard(PlayCardCommand{CommandID: "1", PlayerID: "a", CardID: "a1", ExpectedSequence: g.Sequence(), NowUTC: now})
		uw := g.UnoWindow()
		assertRejected(t, g.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "early", PlayerID: "a", OpeningSequence: uw.OpeningSequence,
			ExpectedSequence: g.Sequence(), NowUTC: now.Add(2 * time.Second),
		}), RejectUnoWindowNotExpired)
		assertAccepted(t, g.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "exp", PlayerID: "a", OpeningSequence: uw.OpeningSequence,
			ExpectedSequence: g.Sequence(), NowUTC: now.Add(UnoWindowDuration),
		}), FactUnoWindowExpired)
		assertRejected(t, g.CallUno(CallUnoCommand{CommandID: "late", PlayerID: "a", ExpectedSequence: g.Sequence(), NowUTC: now.Add(UnoWindowDuration + time.Second)}), RejectUnoWindowInactive)
	})

	t.Run("call at exact deadline rejected", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorRed, Face4)},
			[]Card{c("b1", ColorBlue, Face9)},
		)
		g.PlayCard(PlayCardCommand{CommandID: "1", PlayerID: "a", CardID: "a1", ExpectedSequence: g.Sequence(), NowUTC: now})
		assertRejected(t, g.CallUno(CallUnoCommand{
			CommandID: "late", PlayerID: "a", ExpectedSequence: g.Sequence(),
			NowUTC: now.Add(UnoWindowDuration),
		}), RejectUnoWindowInactive)
	})

	t.Run("next turn closes", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("a1", ColorRed, Face3), c("a2", ColorBlue, Face9)},
			[]Card{c("b1", ColorRed, Face4), c("b2", ColorYellow, Face8), c("b3", ColorGreen, Face6)},
		)
		g.PlayCard(PlayCardCommand{CommandID: "1", PlayerID: "a", CardID: "a1", ExpectedSequence: g.Sequence(), NowUTC: now})
		assertAccepted(t, g.PlayCard(PlayCardCommand{CommandID: "2", PlayerID: "b", CardID: "b1", ExpectedSequence: g.Sequence(), NowUTC: now.Add(time.Second)}), FactUnoWindowClosed)
		assertRejected(t, g.CallUno(CallUnoCommand{CommandID: "late", PlayerID: "a", ExpectedSequence: g.Sequence(), NowUTC: now.Add(time.Second)}), RejectUnoWindowInactive)
	})
}

func TestForfeitPlayer_RemovesFromTurnRing(t *testing.T) {
	seats := []PlayerID{"a", "b", "c"}
	g, err := StartGame("g1", seats, DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": {c("a1", ColorRed, Face3), c("a2", ColorBlue, Face1)},
			"b": {c("b1", ColorRed, Face4), c("b2", ColorGreen, Face2)},
			"c": {c("c1", ColorRed, Face5), c("c2", ColorYellow, Face3)},
		},
		DiscardTop:  c("d", ColorRed, Face7),
		CurrentSeat: 0,
		Direction:   DirectionClockwise,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := g.ForfeitPlayer(ForfeitPlayerCommand{CommandID: "ff", PlayerID: "b", ExpectedSequence: g.Sequence()})
	assertAccepted(t, out, FactPlayerRemoved)
	if g.IsActive("b") {
		t.Fatal("b should leave ring")
	}
	if got := g.ActiveSeats(); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("seats=%v", got)
	}
	assertAccepted(t, play(g, "a-play", "a", "a1"), FactCardPlayed)
	if g.CurrentPlayer() != "c" {
		t.Fatalf("current=%s", g.CurrentPlayer())
	}
	assertAccepted(t, play(g, "c-play", "c", "c1"), FactCardPlayed)
	if g.CurrentPlayer() != "a" {
		t.Fatalf("current=%s", g.CurrentPlayer())
	}
}

func TestGameComplete_PlacementCardPoints(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorRed, Face3)},
		[]Card{c("b1", ColorBlue, FaceSkip), c("b2", ColorNone, FaceWild)},
	)
	assertAccepted(t, play(g, "win", "a", "a1"), FactGameCompleted)
	if !g.Completed() || g.PlacementOrder()[0] != "a" {
		t.Fatalf("placement %v", g.PlacementOrder())
	}
	pts := g.CardPoints()
	if pts["b"] != 70 {
		t.Fatalf("points=%d", pts["b"])
	}
	assertRejected(t, play(g, "x", "b", "b1"), RejectGameCompleted)
}

func TestMatchScore_BestOfThree(t *testing.T) {
	m := NewMatch([]PlayerID{"a", "b"})
	_, _, ok, done := m.RecordGameWin("a")
	if !ok || done {
		t.Fatal("first win")
	}
	m.RecordGameWin("b")
	_, facts, ok, done := m.RecordGameWin("a")
	if !ok || !done || !m.Completed() {
		t.Fatal("match should complete")
	}
	if len(facts) == 0 || facts[0].Name != FactMatchScoreUpdated {
		t.Fatalf("facts %v", facts)
	}
	w, _ := m.Winner()
	if w != "a" || m.Score().WinCount("a") != 2 {
		t.Fatalf("winner=%s wins=%d", w, m.Score().WinCount("a"))
	}

	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorRed, Face7)},
		[]Card{c("b1", ColorBlue, Face1)},
	)
	assertAccepted(t, play(g, "w", "a", "a1"), FactGameCompleted)
	m2 := NewMatch([]PlayerID{"a", "b"})
	_, facts, ok, done = m2.ApplyGameCompletion(g, time.Time{})
	if !ok || done || len(facts) != 1 {
		t.Fatalf("ok=%v done=%v facts=%v", ok, done, facts)
	}
}

func TestDuplicateCommand_Stable(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorRed, Face3), c("a2", ColorBlue, Face1)},
		[]Card{c("b1", ColorGreen, Face2)},
	)
	seq := g.Sequence()
	first := g.PlayCard(PlayCardCommand{CommandID: "same", PlayerID: "a", CardID: "a1", ExpectedSequence: seq})
	assertAccepted(t, first, FactCardPlayed)
	after := g.Sequence()
	handLen := len(g.Hand("a"))
	dup := g.PlayCard(PlayCardCommand{CommandID: "same", PlayerID: "a", CardID: "a2", ExpectedSequence: after})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("%+v", dup)
	}
	if g.Sequence() != after || len(g.Hand("a")) != handLen {
		t.Fatal("mutated on duplicate")
	}

	// rejected duplicate also stable
	g2, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorBlue, Face3)},
		[]Card{c("b1", ColorRed, Face2)},
	)
	seq2 := g2.Sequence()
	firstBad := g2.PlayCard(PlayCardCommand{CommandID: "bad", PlayerID: "a", CardID: "a1", ExpectedSequence: seq2})
	assertRejected(t, firstBad, RejectIllegalCard)
	dupBad := g2.PlayCard(PlayCardCommand{CommandID: "bad", PlayerID: "a", CardID: "a1", ExpectedSequence: seq2})
	if dupBad.Kind != OutcomeDuplicate || dupBad.Rejection == nil || dupBad.Rejection.Code != RejectIllegalCard {
		t.Fatalf("%+v", dupBad)
	}
}

func TestSequence_StaleFutureRequired(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorRed, Face3), c("a2", ColorRed, Face4)},
		[]Card{c("b1", ColorGreen, Face2)},
	)
	assertAccepted(t, play(g, "p1", "a", "a1"), FactCardPlayed)
	seq := g.Sequence()
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "stale", PlayerID: "b", CardID: "b1", ExpectedSequence: seq - 1}), RejectStaleSequence)
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "fut", PlayerID: "b", CardID: "b1", ExpectedSequence: seq + 1}), RejectFutureSequence)
	assertRejected(t, g.PlayCard(PlayCardCommand{CommandID: "zero", PlayerID: "b", CardID: "b1", ExpectedSequence: 0}), RejectSequenceRequired)
	if g.Sequence() != seq {
		t.Fatal("rejects must not advance")
	}
}

func TestPublicSnapshot_PrivacySafe(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("secret-card", ColorBlue, Face9), c("a2", ColorRed, Face3)},
		[]Card{c("b1", ColorGreen, Face2)},
	)
	pub := g.PublicSnapshot()
	if pub.HandCounts["a"] != 2 || pub.HandCounts["b"] != 1 {
		t.Fatalf("%+v", pub.HandCounts)
	}
	s := fmt.Sprintf("%+v", pub)
	if containsSub(s, "secret-card") {
		t.Fatalf("leaked private id: %s", s)
	}
	before := pub
	assertRejected(t, play(g, "x", "a", "secret-card"), RejectIllegalCard) // blue 9 on red 5 — wait, secret is blue 9, illegal
	// actually blue 9 on red 5 is illegal - good
	after := g.PublicSnapshot()
	if before.Sequence != after.Sequence || before.HandCounts["a"] != after.HandCounts["a"] {
		t.Fatal("state mutated on reject")
	}
}

func TestConcurrency_SerializedOneWinner(t *testing.T) {
	g := mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": {c("a1", ColorRed, Face3), c("a2", ColorYellow, Face9)},
			"b": {c("b1", ColorBlue, Face1)},
			"c": {c("c1", ColorRed, Face7), c("c2", ColorYellow, Face8)},
		},
		DiscardTop: c("d", ColorRed, Face7), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
	})
	seq := g.Sequence()
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]CommandOutcome, 2)
	cmds := []PlayCardCommand{
		{CommandID: "t", PlayerID: "a", CardID: "a1", ExpectedSequence: seq},
		{CommandID: "j", PlayerID: "c", CardID: "c1", ExpectedSequence: seq},
	}
	for i := range cmds {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			results[i] = g.PlayCard(cmds[i])
		}(i)
	}
	wg.Wait()
	accepted := 0
	for _, r := range results {
		if r.Kind == OutcomeAccepted {
			accepted++
		} else if r.Rejection == nil || r.Rejection.Code != RejectStaleSequence {
			t.Fatalf("unexpected %+v", r)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d", accepted)
	}
}

func TestDrawCard_OrdinaryAndBatchMismatch(t *testing.T) {
	g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorBlue, Face3)},
		[]Card{c("b1", ColorGreen, Face2)},
	)
	assertAccepted(t, draw(g, "d", "a", c("x", ColorYellow, Face9)), FactCardDrawn, FactTurnAdvanced)

	gRetain, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
		[]Card{c("a1", ColorYellow, Face9)},
		[]Card{c("b1", ColorGreen, Face3)},
	)
	assertAccepted(t, draw(gRetain, "d2", "a", c("x2", ColorRed, Face8)), FactCardDrawn, FactDrawTurnRetained)
	if gRetain.CurrentPlayer() != "a" {
		t.Fatalf("current=%s", gRetain.CurrentPlayer())
	}

	g2 := mustStart(t, []PlayerID{"a", "b"}, DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": {c("d2", ColorRed, FaceDrawTwo), c("a2", ColorYellow, Face9)},
			"b": {c("b1", ColorBlue, Face1)},
		},
		DiscardTop: c("d", ColorRed, Face1), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
	})
	play(g2, "1", "a", "d2")
	seq := g2.Sequence()
	assertRejected(t, g2.DrawCard(DrawCardCommand{
		CommandID: "bad", PlayerID: "b", Cards: []Card{c("x", ColorRed, Face1)}, ExpectedSequence: seq,
	}), RejectDrawBatchMismatch)
	if g2.Sequence() != seq || g2.PenaltyAmount() != 2 {
		t.Fatal("mutated")
	}
}

func TestDrawBatchMismatch_ApplyTopEffects(t *testing.T) {
	top := c("d", ColorRed, FaceDrawTwo)
	g := mustStart(t, []PlayerID{"a", "b"}, DealMaterial{
		Hands: map[PlayerID][]Card{
			"a": {c("a1", ColorBlue, Face1)},
			"b": {c("b1", ColorGreen, Face1)},
		},
		DiscardTop: top, CurrentSeat: 1, Direction: DirectionClockwise, ApplyTopEffects: true,
	})
	seq := g.Sequence()
	assertRejected(t, g.DrawCard(DrawCardCommand{
		CommandID: "bad", PlayerID: "b", ExpectedSequence: seq,
		Cards: []Card{c("only1", ColorYellow, Face1)},
	}), RejectDrawBatchMismatch)
	if g.Sequence() != seq || g.PenaltyAmount() != 2 {
		t.Fatal("mutated")
	}
}

func TestOptionalRules_Table(t *testing.T) {
	t.Run("jump mismatch rejected", func(t *testing.T) {
		g := mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
			Hands: map[PlayerID][]Card{
				"a": {c("a1", ColorBlue, Face1)},
				"b": {c("b1", ColorBlue, Face1)},
				"c": {c("c1", ColorBlue, Face7), c("c2", ColorYellow, Face9)},
			},
			DiscardTop: c("d", ColorRed, Face7), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
		})
		assertRejected(t, play(g, "j", "c", "c1"), RejectJumpInMismatch)
	})
	t.Run("stack only target", func(t *testing.T) {
		g := mustStart(t, []PlayerID{"a", "b", "c"}, DealMaterial{
			Hands: map[PlayerID][]Card{
				"a": {c("d2", ColorRed, FaceDrawTwo), c("a2", ColorYellow, Face9)},
				"b": {c("b1", ColorBlue, Face1)},
				"c": {c("d2c", ColorGreen, FaceDrawTwo), c("c2", ColorYellow, Face8)},
			},
			DiscardTop: c("d", ColorRed, Face1), ActiveColor: ColorRed, CurrentSeat: 0, Direction: DirectionClockwise,
		})
		play(g, "1", "a", "d2")
		assertRejected(t, play(g, "2", "c", "d2c"), RejectJumpInBlocked)
	})
	t.Run("no jump-in while color pending", func(t *testing.T) {
		g, _ := twoPlayer(c("d", ColorRed, Face5), ColorRed,
			[]Card{c("w", ColorNone, FaceWild), c("a2", ColorYellow, Face9)},
			[]Card{c("r5", ColorRed, Face5), c("b2", ColorYellow, Face8)},
		)
		assertAccepted(t, play(g, "1", "a", "w"), FactCardPlayed)
		assertRejected(t, play(g, "j", "b", "r5"), RejectPendingColor)
	})
	t.Run("wd4 legality table", func(t *testing.T) {
		ok := wildDrawFourLegal(
			[]Card{c("c", ColorNone, FaceWildDrawFour), c("x", ColorBlue, Face1)},
			ColorRed, "c",
		)
		if !ok {
			t.Fatal("wd4 should be legal without matching color")
		}
		bad := wildDrawFourLegal(
			[]Card{c("c", ColorNone, FaceWildDrawFour), c("x", ColorRed, Face1)},
			ColorRed, "c",
		)
		if bad {
			t.Fatal("wd4 illegal when matching color held")
		}
	})
}
