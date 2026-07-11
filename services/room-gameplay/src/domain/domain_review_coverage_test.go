package domain

import (
	"testing"
	"time"

	"unoarena/services/room-gameplay/game"
)

func TestJoinRoom_AtCapacity_RejectRoomFull_NoFacts(t *testing.T) {
	r, out := CreateRoom(CreateRoomCommand{
		CommandID: "c-full", RoomID: "room-full", HostID: "host", MaxSeats: 2,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("create: %+v", out)
	}
	mustJoin(t, r, "j1", "guest")
	before := r.Sequence()
	out = r.JoinRoom(JoinRoomCommand{
		CommandID: "j-over", PlayerID: "late", ExpectedSequence: before,
	})
	assertRejected(t, out, RejectRoomFull)
	if len(out.Facts) != 0 {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if r.Sequence() != before || r.Roster().OccupiedCount() != 2 {
		t.Fatalf("seq=%d occupied=%d", r.Sequence(), r.Roster().OccupiedCount())
	}
}

func TestSession_GameplayAfterTerminal_RejectsNoFacts(t *testing.T) {
	dealWin := func(id string) game.DealMaterial {
		return twoPlayerDeal(
			[]game.Card{gc(id, game.ColorRed, game.Face3)},
			[]game.Card{gc("g"+id, game.ColorBlue, game.Face1)},
			gc("d", game.ColorRed, game.Face5),
		)
	}
	s := mustSessionInProgress(t, "host", "guest", dealWin("h1"))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "w1", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "n1", GameID: "g2", ExpectedSequence: s.Room().Sequence(), Deal: dealWin("h2"),
	})
	assertAccepted(t, out)
	out = s.PlayCard(PlayCardCommand{
		CommandID: "w2", PlayerID: "host", CardID: "h2",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow.Add(time.Minute),
	})
	assertAccepted(t, out)
	if s.Room().Status() != RoomStatusCompleted {
		t.Fatalf("status=%s", s.Room().Status())
	}

	before := s.Room().Sequence()
	for _, name := range []string{"play", "draw", "call"} {
		var got CommandOutcome
		switch name {
		case "play":
			got = s.PlayCard(PlayCardCommand{
				CommandID: CommandID("late-" + name), PlayerID: "host", CardID: "h2",
				ExpectedSequence: before, NowUTC: testNow,
			})
		case "draw":
			got = s.DrawCard(DrawCardCommand{
				CommandID: CommandID("late-" + name), PlayerID: "host",
				Cards: []game.Card{gc("x", game.ColorYellow, game.Face1)}, ExpectedSequence: before,
			})
		case "call":
			got = s.CallUno(CallUnoCommand{
				CommandID: CommandID("late-" + name), PlayerID: "host",
				ExpectedSequence: before, NowUTC: testNow,
			})
		}
		assertRejected(t, got, RejectAlreadyTerminal)
		if len(got.Facts) != 0 {
			t.Fatalf("%s facts=%v", name, FactNames(got.Facts))
		}
	}
	if s.Room().Sequence() != before {
		t.Fatal("terminal gameplay must not bump sequence")
	}
}

func TestSession_StartNextGame_DuplicateWhileActive_NoFacts(t *testing.T) {
	dealWin := func(id string) game.DealMaterial {
		return twoPlayerDeal(
			[]game.Card{gc(id, game.ColorRed, game.Face3)},
			[]game.Card{gc("g"+id, game.ColorBlue, game.Face1)},
			gc("d", game.ColorRed, game.Face5),
		)
	}
	s := mustSessionInProgress(t, "host", "guest", dealWin("h1"))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "w1", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "n1", GameID: "g2", ExpectedSequence: s.Room().Sequence(), Deal: dealWin("h2"),
	})
	assertAccepted(t, out)
	if s.GameID() != "g2" || s.Game().Completed() {
		t.Fatalf("want active g2, id=%s done=%v", s.GameID(), s.Game().Completed())
	}

	before := s.Room().Sequence()
	origHand := append([]game.Card(nil), s.Game().Hand("host")...)
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "n1-dup", GameID: "g2", ExpectedSequence: before, Deal: dealWin("h9"),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("kind=%s want accepted idempotent %+v", out.Kind, out)
	}
	if len(out.Facts) != 0 {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if s.GameID() != "g2" || s.Room().Sequence() != before {
		t.Fatal("duplicate StartNextGame mutated active game")
	}
	if len(s.Game().Hand("host")) != len(origHand) || s.Game().Hand("host")[0].ID != origHand[0].ID {
		t.Fatal("duplicate StartNextGame replaced deal")
	}
}

func TestSession_CallUno_ThenReportMissingUno_NoChallengerPenalty(t *testing.T) {
	// Settled: successful CallUno closes/resolves the window; later ReportMissingUno
	// is RejectUnoWindowInactive with no challenger penalty and no facts.
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face9)},
		[]game.Card{gc("g1", game.ColorYellow, game.Face1), gc("g2", game.ColorGreen, game.Face2)},
		gc("d", game.ColorRed, game.Face5),
	))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "open", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.CallUno(CallUnoCommand{
		CommandID: "call", PlayerID: "host",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow.Add(time.Second),
	})
	assertAccepted(t, out)

	beforeHost, beforeGuest := len(s.Game().Hand("host")), len(s.Game().Hand("guest"))
	beforeSeq := s.Room().Sequence()
	out = s.ReportMissingUno(ReportMissingUnoCommand{
		CommandID: "post-call", ChallengerID: "guest", TargetID: "host",
		Cards: []game.Card{
			gc("p1", game.ColorYellow, game.Face3),
			gc("p2", game.ColorYellow, game.Face4),
		},
		ExpectedSequence: beforeSeq, NowUTC: testNow.Add(2 * time.Second),
	})
	assertRejected(t, out, RejectUnoWindowInactive)
	if HasFact(out.Facts, FactUnoPenaltyApplied) || HasFact(out.Facts, FactUnoChallengeIssued) {
		t.Fatalf("post-call challenge must not emit penalty facts: %v", FactNames(out.Facts))
	}
	if len(s.Game().Hand("host")) != beforeHost || len(s.Game().Hand("guest")) != beforeGuest {
		t.Fatal("neither target nor challenger may draw after successful CallUno")
	}
	if s.Room().Sequence() != beforeSeq {
		t.Fatal("sequence bumped on inactive challenge")
	}
}
