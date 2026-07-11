package domain

import (
	"strings"
	"testing"
	"time"

	"unoarena/services/room-gameplay/game"
)

func TestSession_StartMatch_DoesNotReplaceActiveGame(t *testing.T) {
	deal1 := twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorBlue, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	)
	deal2 := twoPlayerDeal(
		[]game.Card{gc("h9", game.ColorYellow, game.Face9)},
		[]game.Card{gc("g9", game.ColorGreen, game.Face9)},
		gc("d", game.ColorYellow, game.Face1),
	)
	s := mustSessionInProgress(t, "host", "guest", deal1)
	origID := s.GameID()
	origSeq := s.Game().Sequence()
	before := s.Room().Sequence()

	// Different command + game + deal while in_progress: reject, no facts, no replace.
	out := s.StartMatch(StartMatchCommand{
		CommandID: "start-other", ActorID: "host", GameID: "g-other",
		ExpectedSequence: before,
	}, deal2)
	assertRejected(t, out, RejectGameStillActive)
	if s.GameID() != origID || s.Game().Sequence() != origSeq {
		t.Fatalf("active game replaced: id=%s seq=%d", s.GameID(), s.Game().Sequence())
	}
	if s.Room().Sequence() != before {
		t.Fatalf("room seq bumped on reject: %d", s.Room().Sequence())
	}

	// Exact idempotent replay of original StartMatch command is stable.
	firstCmd := CommandID("start-" + t.Name())
	dup := s.StartMatch(StartMatchCommand{
		CommandID: firstCmd, ActorID: "host", GameID: "g1",
		ExpectedSequence: before,
	}, deal1)
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s want duplicate", dup.Kind)
	}
	if s.GameID() != origID || s.Game().Hand("host")[0].ID != "h1" {
		t.Fatal("idempotent replay mutated authoritative game")
	}
}

func TestSession_CallUno_ClosesRoomWindow_LaterExpiryRejects(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face9)},
		[]game.Card{gc("g1", game.ColorYellow, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "open", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	w, ok := s.Room().UnoWindow()
	if !ok {
		t.Fatal("room uno should be open")
	}

	out = s.CallUno(CallUnoCommand{
		CommandID: "call", PlayerID: "host",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow.Add(time.Second),
	})
	assertAccepted(t, out)
	if _, still := s.Room().UnoWindow(); still {
		t.Fatal("CallUno must close/sync room Uno window")
	}

	before := s.Room().Sequence()
	out = s.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "late-exp", PlayerID: "host", GameID: s.GameID(),
		TriggeringGameEventID: w.TriggeringGameEventID, OpeningSequence: w.OpeningSequence,
		NowUTC: testNow.Add(UnoWindowDuration), ExpectedSequence: before,
	})
	assertRejected(t, out, RejectUnoWindowInactive)
	if s.Room().Sequence() != before {
		t.Fatal("expiry after CallUno must not mutate room")
	}
}

func TestSession_CallUno_ClosesEngineChallenge_ReportMissingUnoRejects(t *testing.T) {
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
	uw := s.Game().UnoWindow()
	if uw != nil && uw.IsOpen() {
		t.Fatal("engine challenge window must close on CallUno")
	}

	beforeSeq := s.Room().Sequence()
	beforeHost := len(s.Game().Hand("host"))
	beforeGuest := len(s.Game().Hand("guest"))
	out = s.ReportMissingUno(ReportMissingUnoCommand{
		CommandID: "chal-late", ChallengerID: "guest", TargetID: "host",
		Cards: []game.Card{
			gc("p1", game.ColorYellow, game.Face3),
			gc("p2", game.ColorYellow, game.Face4),
		},
		ExpectedSequence: beforeSeq, NowUTC: testNow.Add(2 * time.Second),
	})
	assertRejected(t, out, RejectUnoWindowInactive)
	if len(out.Facts) != 0 {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if len(s.Game().Hand("host")) != beforeHost || len(s.Game().Hand("guest")) != beforeGuest {
		t.Fatal("ReportMissingUno after CallUno must not mutate hands")
	}
	if s.Room().Sequence() != beforeSeq {
		t.Fatal("room sequence must not bump on rejected challenge")
	}
}

func TestSession_ExpireUnoWindow_NoRoomMutateBeforeEngineAccept(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face9)},
		[]game.Card{gc("g1", game.ColorYellow, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "open", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	w, _ := s.Room().UnoWindow()
	beforeSeq := s.Room().Sequence()
	beforeOpen := w.IsOpen()

	// Early expiry: engine/room must reject without closing room window.
	out = s.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "early", PlayerID: "host", GameID: s.GameID(),
		TriggeringGameEventID: w.TriggeringGameEventID, OpeningSequence: w.OpeningSequence,
		NowUTC: testNow.Add(time.Second), ExpectedSequence: beforeSeq,
	})
	assertRejected(t, out, RejectUnoWindowNotExpired)
	w2, ok := s.Room().UnoWindow()
	if !ok || !w2.IsOpen() || w2.OpeningSequence != w.OpeningSequence {
		t.Fatalf("room mutated before engine accept: ok=%v open=%v beforeOpen=%v", ok, w2.IsOpen(), beforeOpen)
	}
	if s.Room().Sequence() != beforeSeq {
		t.Fatal("sequence bumped on rejected expiry")
	}
}

func TestSession_Forfeit_RemovesPlayerFromTurnRing(t *testing.T) {
	deal := game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  {gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face1)},
			"guest": {gc("g1", game.ColorRed, game.Face4), gc("g2", game.ColorGreen, game.Face2)},
			"p3":    {gc("p1", game.ColorRed, game.Face5), gc("p2", game.ColorYellow, game.Face3)},
		},
		DiscardTop:  gc("d", game.ColorRed, game.Face7),
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	}
	r, out := CreateRoom(CreateRoomCommand{
		CommandID: "c-ff", RoomID: "room-ff", HostID: "host", MaxSeats: 4,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%+v", out)
	}
	mustJoin(t, r, "j1", "guest")
	mustJoin(t, r, "j2", "p3")
	mustLock(t, r, "host", false)
	s := OpenSession(r)
	out = s.StartMatch(StartMatchCommand{
		CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence(),
	}, deal)
	assertAccepted(t, out)

	t0 := testNow
	disc := s.Room().DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: "disc", PlayerID: "guest", NowUTC: t0, ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, disc)
	st, _ := s.Room().DisconnectState("guest")

	out = s.ForfeitPlayer(ForfeitPlayerCommand{
		CommandID: "ff", PlayerID: "guest", DisconnectVersion: st.DisconnectVersion,
		NowUTC: t0.Add(ReconnectWindowDuration), ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactPlayerForfeited) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if s.Room().Roster().IsSeated("guest") {
		t.Fatal("guest should leave room roster")
	}
	if s.Game().IsActive("guest") {
		t.Fatal("guest must be removed from game turn ring")
	}

	// Turn cycle among remaining players: host -> p3 -> host.
	if s.Game().CurrentPlayer() != "host" {
		t.Fatalf("current=%s", s.Game().CurrentPlayer())
	}
	out = s.PlayCard(PlayCardCommand{
		CommandID: "h-play", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	if s.Game().CurrentPlayer() != "p3" {
		t.Fatalf("after host play current=%s want p3", s.Game().CurrentPlayer())
	}
	out = s.PlayCard(PlayCardCommand{
		CommandID: "p3-play", PlayerID: "p3", CardID: "p1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	if s.Game().CurrentPlayer() != "host" {
		t.Fatalf("after p3 play current=%s want host", s.Game().CurrentPlayer())
	}
	// Forfeited player cannot act.
	bad := s.PlayCard(PlayCardCommand{
		CommandID: "guest-late", PlayerID: "guest", CardID: "g1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertRejected(t, bad, RejectNotSeated)
}

func TestSession_CompleteGameMatch_Guarded_MatchCompletedAlwaysPresent(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorBlue, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	))
	before := s.Room().Sequence()
	out := s.Room().CompleteGame(CompleteGameCommand{
		CommandID: "cg-guard", GameID: "g1", ExpectedSequence: before,
	})
	assertRejected(t, out, RejectMatchOwnsCompletion)
	out = s.Room().CompleteMatch(CompleteMatchCommand{
		CommandID: "cm-guard", ExpectedSequence: before,
	})
	assertRejected(t, out, RejectMatchOwnsCompletion)
	if s.Room().Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", s.Room().Status())
	}

	// Authoritative completion via Session always emits MatchCompleted.
	out = s.PlayCard(PlayCardCommand{
		CommandID: "g1w", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "n1", GameID: "g2", ExpectedSequence: s.Room().Sequence(),
		Deal: twoPlayerDeal(
			[]game.Card{gc("h2", game.ColorRed, game.Face2)},
			[]game.Card{gc("g2", game.ColorBlue, game.Face3)},
			gc("d", game.ColorRed, game.Face5),
		),
	})
	assertAccepted(t, out)
	out = s.PlayCard(PlayCardCommand{
		CommandID: "g2w", PlayerID: "host", CardID: "h2",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow.Add(time.Minute),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactMatchCompleted) {
		t.Fatalf("MatchCompleted required: %v", FactNames(out.Facts))
	}
}

func TestSession_RoomForfeitPlayer_Guarded(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face1)},
		[]game.Card{gc("g1", game.ColorRed, game.Face4)},
		gc("d", game.ColorRed, game.Face5),
	))
	disc := s.Room().DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: "disc-guard", PlayerID: "guest", NowUTC: testNow, ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, disc)
	st, _ := s.Room().DisconnectState("guest")
	before := s.Room().Sequence()
	out := s.Room().ForfeitPlayer(ForfeitPlayerCommand{
		CommandID: "ff-guard", PlayerID: "guest", DisconnectVersion: st.DisconnectVersion,
		NowUTC: testNow.Add(ReconnectWindowDuration), ExpectedSequence: before,
	})
	assertRejected(t, out, RejectSessionOwnsForfeit)
	if len(out.Facts) != 0 {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if s.Room().Sequence() != before || !s.Room().Roster().IsSeated("guest") || !s.Game().IsActive("guest") {
		t.Fatal("guarded Room.ForfeitPlayer must not mutate room or game ring")
	}
}

func TestSession_RejectsPriorGameIDReuse(t *testing.T) {
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

	before := s.Room().Sequence()
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "reuse", GameID: "g1", ExpectedSequence: before, Deal: dealWin("h2"),
	})
	assertRejected(t, out, RejectGameIDReused)
	if s.Room().Sequence() != before || s.GameID() != "g1" {
		t.Fatal("reuse must not bind new game")
	}
}

func TestSession_DisconnectedPlayerCannotGameplay(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face1)},
		[]game.Card{gc("g1", game.ColorRed, game.Face4)},
		gc("d", game.ColorRed, game.Face5),
	))
	disc := s.Room().DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: "d1", PlayerID: "host", NowUTC: testNow, ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, disc)
	before := s.Room().Sequence()
	out := s.PlayCard(PlayCardCommand{
		CommandID: "play-disc", PlayerID: "host", CardID: "h1",
		ExpectedSequence: before, NowUTC: testNow,
	})
	assertRejected(t, out, RejectPlayerDisconnected)
	if s.Room().Sequence() != before {
		t.Fatal("seq bumped")
	}
	out = s.DrawCard(DrawCardCommand{
		CommandID: "draw-disc", PlayerID: "host",
		Cards: []game.Card{gc("x", game.ColorYellow, game.Face1)}, ExpectedSequence: before,
	})
	assertRejected(t, out, RejectPlayerDisconnected)
}

func TestSession_MatchCompleted_TieBreakPayload(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorBlue, game.FaceSkip), gc("g2", game.ColorNone, game.FaceWild)},
		gc("d", game.ColorRed, game.Face5),
	))
	doneAt := testNow.Add(2 * time.Minute)
	out := s.PlayCard(PlayCardCommand{
		CommandID: "w1", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "n", GameID: "g2", ExpectedSequence: s.Room().Sequence(),
		Deal: twoPlayerDeal(
			[]game.Card{gc("h2", game.ColorRed, game.Face4)},
			[]game.Card{gc("g3", game.ColorYellow, game.Face2)},
			gc("d", game.ColorRed, game.Face5),
		),
	})
	assertAccepted(t, out)
	out = s.PlayCard(PlayCardCommand{
		CommandID: "w2", PlayerID: "host", CardID: "h2",
		ExpectedSequence: s.Room().Sequence(), NowUTC: doneAt,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactMatchCompleted) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	wins, ok := factData(out.Facts, FactMatchCompleted, "matchWins")
	if !ok || !strings.Contains(wins, "host:2") {
		t.Fatalf("matchWins=%q", wins)
	}
	pts, ok := factData(out.Facts, FactMatchCompleted, "cardPoints")
	if !ok || pts == "" {
		t.Fatalf("cardPoints missing: %q", pts)
	}
	completedAt, ok := factData(out.Facts, FactMatchCompleted, "completedAt")
	if !ok || completedAt != doneAt.Format(time.RFC3339Nano) {
		t.Fatalf("completedAt=%q", completedAt)
	}
	abandoned, ok := factData(out.Facts, FactMatchCompleted, "isAbandoned")
	if !ok || abandoned != "false" {
		t.Fatalf("isAbandoned=%q", abandoned)
	}
	forfeits, ok := factData(out.Facts, FactMatchCompleted, "forfeits")
	if !ok {
		t.Fatal("forfeits marker required")
	}
	if forfeits != "" {
		t.Fatalf("forfeits=%q want empty", forfeits)
	}
}
