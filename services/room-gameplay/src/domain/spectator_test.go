package domain

import (
	"testing"

	"unoarena/services/room-gameplay/game"
)

func TestSpectatorAdmission_StatusTable(t *testing.T) {
	tests := []struct {
		name     string
		status   RoomStatus
		auth     SpectatorAuth
		allowed  bool
		wantCode RejectionCode
	}{
		{"waiting public", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: true}, true, ""},
		{"locked public", RoomStatusLocked, SpectatorAuth{IsPublicRoom: true}, true, ""},
		{"in_progress public", RoomStatusInProgress, SpectatorAuth{IsPublicRoom: true}, true, ""},
		{"completed denied", RoomStatusCompleted, SpectatorAuth{IsPublicRoom: true, Authorized: true}, false, RejectSpectatorTerminal},
		{"cancelled denied", RoomStatusCancelled, SpectatorAuth{IsPublicRoom: true, Authorized: true}, false, RejectSpectatorTerminal},
		{"private unauthorized", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, Authorized: false}, false, RejectSpectatorUnauthorized},
		{"private authorized", RoomStatusWaiting, SpectatorAuth{IsPublicRoom: false, Authorized: true}, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := EvaluateSpectatorAdmission(tt.status, tt.auth)
			if dec.Allowed != tt.allowed {
				t.Fatalf("allowed=%v want %v (%+v)", dec.Allowed, tt.allowed, dec)
			}
			if !tt.allowed && dec.Code != tt.wantCode {
				t.Fatalf("code=%s want %s", dec.Code, tt.wantCode)
			}
			if tt.allowed && dec.Code != "" {
				t.Fatalf("unexpected code %s", dec.Code)
			}
		})
	}
}

func TestSpectatorAdmission_GameVsMatchTerminalDistinction(t *testing.T) {
	// Game vs match terminal distinction is owned by Session completion facts.
	s := mustSessionInProgress(t, "host", "guest", twoPlayerDeal(
		[]game.Card{gc("h1", game.ColorRed, game.Face3)},
		[]game.Card{gc("g1", game.ColorBlue, game.Face1)},
		gc("d", game.ColorRed, game.Face5),
	))
	r := s.Room()

	dec := r.SpectatorAdmission(SpectatorAuth{})
	if !dec.Allowed {
		t.Fatalf("in_progress must allow admission: %+v", dec)
	}

	out := r.CompleteGame(CompleteGameCommand{
		CommandID:        "cg1",
		GameID:           "g1",
		ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, out, RejectMatchOwnsCompletion)

	out = s.PlayCard(PlayCardCommand{
		CommandID: "win1", PlayerID: "host", CardID: "h1",
		ExpectedSequence: r.Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	if r.Status() != RoomStatusInProgress {
		t.Fatalf("individual game end must not terminalize room, status=%s", r.Status())
	}
	if !r.GameCompletedInMatch() {
		t.Fatal("expected gameCompletedInMatch marker")
	}
	dec = r.SpectatorAdmission(SpectatorAuth{})
	if !dec.Allowed {
		t.Fatalf("after GameCompleted (not match) admission must remain allowed: %+v", dec)
	}

	out = r.CompleteMatch(CompleteMatchCommand{
		CommandID:        "cm1",
		ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, out, RejectMatchOwnsCompletion)

	out = s.StartNextGame(StartNextGameCommand{
		CommandID: "n", GameID: "g2", ExpectedSequence: r.Sequence(),
		Deal: twoPlayerDeal(
			[]game.Card{gc("h2", game.ColorRed, game.Face2)},
			[]game.Card{gc("g2", game.ColorBlue, game.Face3)},
			gc("d", game.ColorRed, game.Face5),
		),
	})
	assertAccepted(t, out)
	out = s.PlayCard(PlayCardCommand{
		CommandID: "win2", PlayerID: "host", CardID: "h2",
		ExpectedSequence: r.Sequence(), NowUTC: testNow,
	})
	assertAccepted(t, out)
	if !HasFact(out.Facts, FactMatchCompleted) {
		t.Fatalf("MatchCompleted required: %v", FactNames(out.Facts))
	}
	dec = r.SpectatorAdmission(SpectatorAuth{Authorized: true})
	if dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("after RoomCompleted admission must deny: %+v", dec)
	}
}

func TestSpectatorAdmission_PrivateRoomUsesVisibility(t *testing.T) {
	r, out := CreateRoom(CreateRoomCommand{
		CommandID:  "c-priv",
		RoomID:     "priv-room",
		HostID:     "host",
		Visibility: VisibilityPrivate,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%+v", out)
	}
	dec := r.SpectatorAdmission(SpectatorAuth{Authorized: false})
	if dec.Allowed || dec.Code != RejectSpectatorUnauthorized {
		t.Fatalf("%+v", dec)
	}
	dec = r.SpectatorAdmission(SpectatorAuth{Authorized: true})
	if !dec.Allowed {
		t.Fatalf("%+v", dec)
	}
}

func TestSpectatorAdmission_CancelledEmptyHostLeave(t *testing.T) {
	r := mustCreateAdHoc(t, "spec-cancel", "host")
	_ = r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave",
		PlayerID:         "host",
		ExpectedSequence: r.Sequence(),
	})
	dec := r.SpectatorAdmission(SpectatorAuth{Authorized: true})
	if dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("%+v", dec)
	}
}
