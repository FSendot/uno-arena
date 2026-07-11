package domain

import (
	"strconv"
	"testing"
	"time"

	"unoarena/services/room-gameplay/game"
)

func TestSession_PlayCard_MapsFactsToRoomSequence(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face1)},
		[]game.Card{gc("g1", game.ColorGreen, game.Face2)},
		gc("d", game.ColorRed, game.Face5),
	))
	before := s.Room().Sequence()
	out := s.PlayCard(PlayCardCommand{
		CommandID:        "play-1",
		PlayerID:         "host",
		CardID:           "h1",
		ExpectedSequence: before,
		NowUTC:           testNow,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactCardPlayed) || !HasFact(out.Facts, FactTurnAdvanced) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if out.Sequence != before+1 || s.Room().Sequence() != before+1 {
		t.Fatalf("room seq=%d out=%d want %d", s.Room().Sequence(), out.Sequence, before+1)
	}
	if s.Game().Sequence() != 2 {
		t.Fatalf("game seq=%d", s.Game().Sequence())
	}
}

func TestSession_Rejection_NoFactsNoSequenceBump(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorRed, game.Face4)},
		gc("d", game.ColorRed, game.Face5),
	))
	before := s.Room().Sequence()
	out := s.PlayCard(PlayCardCommand{
		CommandID:        "bad",
		PlayerID:         "guest", // out of turn, not exact-match jump-in
		CardID:           "g1",
		ExpectedSequence: before,
		NowUTC:           testNow,
	})
	assertRejected(t, out, RejectJumpInMismatch)
	if len(out.Facts) != 0 {
		t.Fatalf("rejection must emit no facts: %v", FactNames(out.Facts))
	}
	if s.Room().Sequence() != before {
		t.Fatalf("sequence bumped on reject: %d", s.Room().Sequence())
	}
}

func TestSession_StaleSequence_NoFacts(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorBlue, game.Face9)},
		gc("d", game.ColorRed, game.Face5),
	))
	before := s.Room().Sequence()
	out := s.PlayCard(PlayCardCommand{
		CommandID:        "stale",
		PlayerID:         "host",
		CardID:           "h1",
		ExpectedSequence: before - 1,
		NowUTC:           testNow,
	})
	assertRejected(t, out, RejectStaleSequence)
	if s.Room().Sequence() != before || len(out.Facts) != 0 {
		t.Fatalf("seq=%d facts=%v", s.Room().Sequence(), out.Facts)
	}
}

func TestSession_GameCompleted_DoesNotTerminalizeOrCloseSpectators(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorBlue, game.FaceSkip), gc("g2", game.ColorNone, game.FaceWild)},
		gc("d", game.ColorRed, game.Face5),
	))
	out := s.PlayCard(PlayCardCommand{
		CommandID:        "win",
		PlayerID:         "host",
		CardID:           "h1",
		ExpectedSequence: s.Room().Sequence(),
		NowUTC:           testNow,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactGameCompleted) || !HasFact(out.Facts, FactMatchScoreUpdated) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if HasFact(out.Facts, FactRoomCompleted) || HasFact(out.Facts, FactMatchCompleted) || HasFact(out.Facts, FactSpectatorStreamsClose) {
		t.Fatalf("game end must not terminalize room: %v", FactNames(out.Facts))
	}
	if s.Room().Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", s.Room().Status())
	}
	if !s.Room().GameCompletedInMatch() {
		t.Fatal("expected gameCompletedInMatch")
	}
	dec := s.Room().SpectatorAdmission(SpectatorAuth{})
	if !dec.Allowed {
		t.Fatalf("spectators must remain admitted after GameCompleted: %+v", dec)
	}
}

func TestSession_BestOfThree_EndsAtTwoWins_ClosesSpectators(t *testing.T) {
	dealWin := func(winnerHand []game.Card) game.DealMaterial {
		return twoPlayerDeal(winnerHand, []game.Card{gc("g1", game.ColorBlue, game.Face1)}, gc("d", game.ColorRed, game.Face5))
	}
	s := mustSessionInProgress(t, "host", "guest", dealWin([]game.Card{gc("h1", game.ColorRed, game.Face3)}))

	// Game 1: host wins
	out := s.PlayCard(PlayCardCommand{
		CommandID: "g1-win", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	if s.Match().Completed() {
		t.Fatal("match must not complete at 1 win")
	}

	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "next-1", GameID: "g2", ExpectedSequence: s.Room().Sequence(),
		Deal: dealWin([]game.Card{gc("h2", game.ColorRed, game.Face7)}),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactGameStarted) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if s.Room().GameCompletedInMatch() {
		t.Fatal("flag should clear on next game")
	}
	if s.Room().Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", s.Room().Status())
	}

	// Game 2: host wins again → match complete
	out = s.PlayCard(PlayCardCommand{
		CommandID: "g2-win", PlayerID: "host", CardID: "h2",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactGameCompleted) || !HasFact(out.Facts, FactMatchScoreUpdated) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if !HasFact(out.Facts, FactMatchCompleted) || !HasFact(out.Facts, FactRoomCompleted) || !HasFact(out.Facts, FactSpectatorStreamsClose) {
		t.Fatalf("match end facts missing: %v", FactNames(out.Facts))
	}
	if s.Room().Status() != RoomStatusCompleted || !s.Match().Completed() {
		t.Fatalf("status=%s matchDone=%v", s.Room().Status(), s.Match().Completed())
	}
	dec := s.Room().SpectatorAdmission(SpectatorAuth{})
	if dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("spectators must close on RoomCompleted: %+v", dec)
	}

	// StartNextGame rejected after match complete
	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "next-late", GameID: "g3", ExpectedSequence: s.Room().Sequence(),
		Deal: dealWin([]game.Card{gc("h3", game.ColorRed, game.Face2)}),
	})
	assertRejected(t, out, RejectAlreadyTerminal)
}

func TestSession_Uno_AbsoluteExpiresAtAndOpeningRoomSequence(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face9)},
		[]game.Card{gc("g1", game.ColorYellow, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	))
	before := s.Room().Sequence()
	out := s.PlayCard(PlayCardCommand{
		CommandID: "to-uno", PlayerID: "host", CardID: "h1",
		ExpectedSequence: before, NowUTC: testNow,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactUnoWindowOpened) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	expires, ok := factData(out.Facts, FactUnoWindowOpened, "expiresAt")
	if !ok {
		t.Fatal("missing expiresAt")
	}
	wantExpiry := testNow.Add(UnoWindowDuration).Format(time.RFC3339Nano)
	if expires != wantExpiry {
		t.Fatalf("expiresAt=%s want %s", expires, wantExpiry)
	}
	openSeqStr, ok := factData(out.Facts, FactUnoWindowOpened, "openingSequence")
	if !ok {
		t.Fatal("missing openingSequence")
	}
	openSeq, _ := strconv.ParseUint(openSeqStr, 10, 64)
	if SequenceNumber(openSeq) != out.Sequence || out.Sequence != before+1 {
		t.Fatalf("openingSequence=%d outSeq=%d before=%d", openSeq, out.Sequence, before)
	}
	w, ok := s.Room().UnoWindow()
	if !ok || w.OpeningSequence != out.Sequence {
		t.Fatalf("room uno opening=%v ok=%v", w.OpeningSequence, ok)
	}

	// CallUno timely
	out = s.CallUno(CallUnoCommand{
		CommandID: "call", PlayerID: "host", ExpectedSequence: s.Room().Sequence(), NowUTC: testNow.Add(time.Second),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactUnoCalled) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
}

func TestSession_UnoExpiry_StaleAndIdempotent(t *testing.T) {
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

	// early expiry rejected, no facts
	before := s.Room().Sequence()
	out = s.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "early", PlayerID: "host", GameID: s.GameID(),
		TriggeringGameEventID: w.TriggeringGameEventID, OpeningSequence: w.OpeningSequence,
		NowUTC: testNow.Add(2 * time.Second), ExpectedSequence: before,
	})
	assertRejected(t, out, RejectUnoWindowNotExpired)
	if s.Room().Sequence() != before {
		t.Fatal("seq bumped")
	}

	// stale opening sequence rejected
	out = s.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "stale-open", PlayerID: "host", GameID: s.GameID(),
		TriggeringGameEventID: w.TriggeringGameEventID, OpeningSequence: w.OpeningSequence + 9,
		NowUTC: testNow.Add(UnoWindowDuration), ExpectedSequence: s.Room().Sequence(),
	})
	assertRejected(t, out, RejectUnoWindowMismatch)

	// timely expiry accepted
	out = s.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "exp", PlayerID: "host", GameID: s.GameID(),
		TriggeringGameEventID: w.TriggeringGameEventID, OpeningSequence: w.OpeningSequence,
		NowUTC: testNow.Add(UnoWindowDuration), ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactUnoWindowExpired) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}

	// duplicate command id is stable
	dup := s.ExpireUnoWindow(ExpireUnoWindowCommand{
		CommandID: "exp", PlayerID: "host", GameID: s.GameID(),
		TriggeringGameEventID: w.TriggeringGameEventID, OpeningSequence: w.OpeningSequence,
		NowUTC: testNow.Add(UnoWindowDuration + time.Second), ExpectedSequence: s.Room().Sequence(),
	})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s", dup.Kind)
	}
}

func TestSession_HostPlayerParity_AfterLock(t *testing.T) {
	// Host has no gameplay privilege: illegal host play rejects like guest; legal plays equal.
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorBlue, game.Face9)}, // illegal on red discard
		[]game.Card{gc("g1", game.ColorRed, game.Face4), gc("g2", game.ColorGreen, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	))
	if s.Room().HostHasGameplayAuthority() {
		t.Fatal("host must not have gameplay authority after start")
	}

	before := s.Room().Sequence()
	hostBad := s.PlayCard(PlayCardCommand{
		CommandID: "host-bad", PlayerID: "host", CardID: "h1",
		ExpectedSequence: before, NowUTC: testNow,
	})
	assertRejected(t, hostBad, RejectIllegalCard)

	// Guest draws? Actually host is current - host draws to pass, then guest plays.
	// Rebuild with host holding legal card and guest holding illegal for parity.
	s2 := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorYellow, game.Face8)},
		[]game.Card{gc("g1", game.ColorBlue, game.Face9)}, // illegal when guest's turn after host plays
		gc("d", game.ColorRed, game.Face5),
	))
	out := s2.PlayCard(PlayCardCommand{
		CommandID: "host-ok", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s2.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	guestBad := s2.PlayCard(PlayCardCommand{
		CommandID: "guest-bad", PlayerID: "guest", CardID: "g1",
		ExpectedSequence: s2.Room().Sequence(), NowUTC: testNow,
	})
	assertRejected(t, guestBad, RejectIllegalCard)
	// Same rejection class for host and guest illegal plays — no host privilege.
	if hostBad.Rejection.Code != guestBad.Rejection.Code {
		t.Fatalf("host code=%s guest code=%s", hostBad.Rejection.Code, guestBad.Rejection.Code)
	}
}

func TestSession_SkipDisconnectedTurn(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorRed, game.Face4)},
		gc("d", game.ColorRed, game.Face5),
	))
	// host is current; disconnect host and skip their turn
	t0 := testNow
	disc := s.Room().DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: "disc", PlayerID: "host", NowUTC: t0, ExpectedSequence: s.Room().Sequence(),
	})
	if disc.Kind != OutcomeAccepted {
		t.Fatalf("%+v", disc)
	}
	turnVer := s.TurnVersion()
	before := s.Room().Sequence()
	out := s.SkipDisconnectedTurn(SkipDisconnectedTurnCommand{
		CommandID: "skip", PlayerID: "host", TurnVersion: turnVer, ExpectedSequence: before,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactTurnSkipped) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if s.Game().CurrentPlayer() != "guest" {
		t.Fatalf("current=%s", s.Game().CurrentPlayer())
	}

	// idempotent same turn version
	dup := s.SkipDisconnectedTurn(SkipDisconnectedTurnCommand{
		CommandID: "skip-again", PlayerID: "host", TurnVersion: turnVer, ExpectedSequence: s.Room().Sequence(),
	})
	if dup.Kind != OutcomeAccepted && dup.Kind != OutcomeDuplicate {
		t.Fatalf("%+v", dup)
	}
	if len(dup.Facts) != 0 && dup.Kind == OutcomeAccepted {
		// second skip for same turn version is no-op accept
		if s.Game().CurrentPlayer() != "guest" {
			t.Fatal("mutated on idempotent skip")
		}
	}
}

func TestSession_DrawChooseColor_ThroughSeam(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorNone, game.FaceWild), gc("h2", game.ColorYellow, game.Face8)},
		[]game.Card{gc("g1", game.ColorRed, game.Face2)},
		gc("d", game.ColorRed, game.Face5),
	))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "wild", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.ChooseColor(ChooseColorCommand{
		CommandID: "color", PlayerID: "host", Color: game.ColorBlue, ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactColorChosen) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}

	// guest draws
	out = s.DrawCard(DrawCardCommand{
		CommandID: "draw", PlayerID: "guest",
		Cards:            []game.Card{gc("x1", game.ColorYellow, game.Face1)},
		ExpectedSequence: s.Room().Sequence(),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactCardDrawn) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
}

func TestSession_ReportMissingUno(t *testing.T) {
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3), gc("h2", game.ColorBlue, game.Face9)},
		[]game.Card{gc("g1", game.ColorYellow, game.Face1), gc("g2", game.ColorGreen, game.Face2)},
		gc("d", game.ColorRed, game.Face5),
	))
	out := s.PlayCard(PlayCardCommand{
		CommandID: "to1", PlayerID: "host", CardID: "h1",
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	out = s.ReportMissingUno(ReportMissingUnoCommand{
		CommandID: "chal", ChallengerID: "guest", TargetID: "host",
		Cards:            []game.Card{gc("p1", game.ColorYellow, game.Face3), gc("p2", game.ColorYellow, game.Face4)},
		ExpectedSequence: s.Room().Sequence(), NowUTC: testNow.Add(time.Second),
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactUnoChallengeIssued) || !HasFact(out.Facts, FactUnoPenaltyApplied) {
		t.Fatalf("facts=%v", FactNames(out.Facts))
	}
	if _, ok := s.Room().UnoWindow(); ok {
		t.Fatal("room uno should close after challenge")
	}
}

// --- helpers ---

func gc(id string, color game.Color, face game.Face) game.Card {
	return game.Card{ID: game.CardID(id), Color: color, Face: face}
}

func twoPlayerDeal(hostHand, guestHand []game.Card, top game.Card) game.DealMaterial {
	return game.DealMaterial{
		Hands: map[game.PlayerID][]game.Card{
			"host":  append([]game.Card(nil), hostHand...),
			"guest": append([]game.Card(nil), guestHand...),
		},
		DiscardTop:  top,
		CurrentSeat: 0,
		Direction:   game.DirectionClockwise,
	}
}

func mustSessionInProgress(t *testing.T, host, guest PlayerID, deal game.DealMaterial) *Session {
	t.Helper()
	r, out := CreateRoom(CreateRoomCommand{
		CommandID: CommandID("create-" + t.Name()), RoomID: RoomID("room-" + t.Name()),
		HostID: host, Visibility: VisibilityPublic, MaxSeats: 4,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("create: %+v", out)
	}
	mustJoin(t, r, CommandID("join-"+t.Name()), guest)
	mustLock(t, r, host, false)
	s := OpenSession(r)
	out = s.StartMatch(StartMatchCommand{
		CommandID: CommandID("start-" + t.Name()), ActorID: host, GameID: "g1",
		ExpectedSequence: r.Sequence(),
	}, deal)
	if out.Kind != OutcomeAccepted {
		t.Fatalf("start: %+v", out)
	}
	return s
}

func assertAccepted(t *testing.T, out CommandOutcome) {
	t.Helper()
	if out.Kind != OutcomeAccepted {
		t.Fatalf("expected accepted, got kind=%s rej=%+v", out.Kind, out.Rejection)
	}
}
