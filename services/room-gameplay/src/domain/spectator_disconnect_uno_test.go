package domain

import (
	"testing"
	"time"
)

func TestSpectatorAdmission_GameVsMatchTerminal(t *testing.T) {
	r := mustCreate(t, "host")
	join(t, r, "j1", "p2")
	_ = r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
	_ = r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})

	auth := SpectatorAuth{Authorized: false} // public room overrides via room visibility

	tests := []struct {
		name     string
		mutate   func(t *testing.T, r *Room)
		wantOK   bool
		wantCode RejectionCode
	}{
		{
			name:   "in_progress allows",
			mutate: func(t *testing.T, r *Room) {},
			wantOK: true,
		},
		{
			name: "individual game completed still allows (match not terminal)",
			mutate: func(t *testing.T, r *Room) {
				out := r.CompleteGame(CompleteGameCommand{CommandID: "cg", GameID: "g1", ExpectedSequence: r.Sequence()})
				if out.Kind != OutcomeRejected || out.Rejection == nil || out.Rejection.Code != RejectMatchOwnsCompletion {
					t.Fatalf("CompleteGame must be guarded: %+v", out)
				}
				// Simulate Session game-complete marker without terminalizing room.
				r.gameCompletedInMatch = true
				if r.Status() != RoomStatusInProgress {
					t.Fatalf("status=%s", r.Status())
				}
				if !r.GameCompletedInMatch() {
					t.Fatal("expected gameCompletedInMatch")
				}
			},
			wantOK: true,
		},
		{
			name: "match/room completed denies",
			mutate: func(t *testing.T, r *Room) {
				out := r.CompleteMatch(CompleteMatchCommand{CommandID: "cm", ExpectedSequence: r.Sequence()})
				if out.Kind != OutcomeRejected || out.Rejection == nil || out.Rejection.Code != RejectMatchOwnsCompletion {
					t.Fatalf("CompleteMatch must be guarded: %+v", out)
				}
				// Terminal status is set only by Session-owned completion.
				r.status = RoomStatusCompleted
			},
			wantOK: false, wantCode: RejectSpectatorTerminal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mutate(t, r)
			dec := r.SpectatorAdmission(auth)
			if dec.Allowed != tt.wantOK {
				t.Fatalf("allowed=%v want %v (%s)", dec.Allowed, tt.wantOK, dec.Reason)
			}
			if !tt.wantOK && dec.Code != tt.wantCode {
				t.Fatalf("code=%s want %s", dec.Code, tt.wantCode)
			}
		})
	}
}

func TestSpectatorAdmission_StatusAndAuth_Table(t *testing.T) {
	tests := []struct {
		name       string
		status     RoomStatus
		visibility Visibility
		auth       SpectatorAuth
		wantOK     bool
		wantCode   RejectionCode
	}{
		{"waiting public", RoomStatusWaiting, VisibilityPublic, SpectatorAuth{}, true, ""},
		{"locked public", RoomStatusLocked, VisibilityPublic, SpectatorAuth{}, true, ""},
		{"in_progress public", RoomStatusInProgress, VisibilityPublic, SpectatorAuth{}, true, ""},
		{"completed denies", RoomStatusCompleted, VisibilityPublic, SpectatorAuth{}, false, RejectSpectatorTerminal},
		{"cancelled denies", RoomStatusCancelled, VisibilityPublic, SpectatorAuth{}, false, RejectSpectatorTerminal},
		{"private unauthorized", RoomStatusWaiting, VisibilityPrivate, SpectatorAuth{Authorized: false}, false, RejectSpectatorUnauthorized},
		{"private authorized", RoomStatusWaiting, VisibilityPrivate, SpectatorAuth{Authorized: true}, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := EvaluateSpectatorAdmission(tt.status, SpectatorAuth{
				IsPublicRoom: tt.visibility == VisibilityPublic,
				Authorized:   tt.auth.Authorized,
			})
			if dec.Allowed != tt.wantOK {
				t.Fatalf("allowed=%v reason=%s", dec.Allowed, dec.Reason)
			}
			if !tt.wantOK && dec.Code != tt.wantCode {
				t.Fatalf("code=%s want %s", dec.Code, tt.wantCode)
			}
		})
	}
}

func TestSpectatorAdmission_WaitingLockedInProgressViaRoom(t *testing.T) {
	r, _ := CreateRoom(CreateRoomCommand{
		CommandID: "c", RoomID: "r", HostID: "h", Visibility: VisibilityPrivate, MaxSeats: 4,
	})
	join(t, r, "j1", "p2")

	if dec := r.SpectatorAdmission(SpectatorAuth{Authorized: true}); !dec.Allowed {
		t.Fatal(dec)
	}
	_ = r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "h", ExpectedSequence: r.Sequence()})
	if dec := r.SpectatorAdmission(SpectatorAuth{Authorized: true}); !dec.Allowed {
		t.Fatal(dec)
	}
	_ = r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "h", GameID: "g", ExpectedSequence: r.Sequence()})
	if dec := r.SpectatorAdmission(SpectatorAuth{Authorized: true}); !dec.Allowed {
		t.Fatal(dec)
	}
	_ = r.CancelRoom(CancelRoomCommand{CommandID: "nope", ActorID: "h", ExpectedSequence: r.Sequence()})
	// cancel rejected while in progress — Session owns completion; force terminal for admission check
	out := r.CompleteMatch(CompleteMatchCommand{CommandID: "cm", ExpectedSequence: r.Sequence()})
	if out.Kind != OutcomeRejected || out.Rejection.Code != RejectMatchOwnsCompletion {
		t.Fatalf("CompleteMatch must be guarded: %+v", out)
	}
	r.status = RoomStatusCompleted
	if dec := r.SpectatorAdmission(SpectatorAuth{Authorized: true}); dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("%+v", dec)
	}
}

func TestDisconnectReconnectForfeit_DeadlineBoundaries(t *testing.T) {
	r := mustCreate(t, "host")
	join(t, r, "j1", "p2")
	_ = r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
	_ = r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})

	t0 := now()
	disc := r.DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: "d1", PlayerID: "p2", NowUTC: t0, ExpectedSequence: r.Sequence(),
	})
	if disc.Kind != OutcomeAccepted || !HasFact(disc.Facts, FactPlayerDisconnected) {
		t.Fatalf("%+v", disc)
	}
	state, ok := r.DisconnectState("p2")
	if !ok {
		t.Fatal("missing disconnect state")
	}
	deadline := state.DeadlineUTC
	if !deadline.Equal(t0.Add(ReconnectWindowDuration)) {
		t.Fatalf("deadline=%s", deadline)
	}
	ver := state.DisconnectVersion

	t.Run("reconnect just before deadline", func(t *testing.T) {
		r2 := cloneInProgressWithDisconnect(t, t0)
		st, _ := r2.DisconnectState("p2")
		out := r2.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID: "rc-before", PlayerID: "p2", DisconnectVersion: st.DisconnectVersion,
			NowUTC: deadline.Add(-time.Nanosecond), ExpectedSequence: r2.Sequence(),
		})
		if out.Kind != OutcomeAccepted {
			t.Fatal(out.Rejection)
		}
	})

	t.Run("reconnect at exact deadline denied", func(t *testing.T) {
		r2 := cloneInProgressWithDisconnect(t, t0)
		st, _ := r2.DisconnectState("p2")
		out := r2.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID: "rc-exact", PlayerID: "p2", DisconnectVersion: st.DisconnectVersion,
			NowUTC: deadline, ExpectedSequence: r2.Sequence(),
		})
		if out.Rejection == nil || out.Rejection.Code != RejectReconnectExpired {
			t.Fatalf("%+v", out)
		}
		if len(out.Facts) != 0 {
			t.Fatal(out.Facts)
		}
	})

	t.Run("forfeit before deadline denied", func(t *testing.T) {
		r2 := cloneInProgressWithDisconnect(t, t0)
		st, _ := r2.DisconnectState("p2")
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID: "ff-early", PlayerID: "p2", DisconnectVersion: st.DisconnectVersion,
			NowUTC: deadline.Add(-time.Nanosecond), ExpectedSequence: r2.Sequence(),
		})
		if out.Rejection == nil || out.Rejection.Code != RejectForfeitNotDue {
			t.Fatalf("%+v", out)
		}
	})

	t.Run("forfeit at exact deadline accepted", func(t *testing.T) {
		r2 := cloneInProgressWithDisconnect(t, t0)
		st, _ := r2.DisconnectState("p2")
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID: "ff-exact", PlayerID: "p2", DisconnectVersion: st.DisconnectVersion,
			NowUTC: deadline, ExpectedSequence: r2.Sequence(),
		})
		if out.Kind != OutcomeAccepted || !HasFact(out.Facts, FactPlayerForfeited) {
			t.Fatalf("%+v", out)
		}
		if r2.Roster().IsSeated("p2") {
			t.Fatal("forfeited player must leave roster")
		}
		if r2.Status() != RoomStatusInProgress {
			t.Fatalf("casual continuation required, status=%s", r2.Status())
		}
	})

	t.Run("stale disconnect version denied", func(t *testing.T) {
		out := r.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID: "rc-stale-ver", PlayerID: "p2", DisconnectVersion: ver + 9,
			NowUTC: t0.Add(time.Second), ExpectedSequence: r.Sequence(),
		})
		if out.Rejection == nil || out.Rejection.Code != RejectDisconnectVersion {
			t.Fatalf("%+v", out)
		}
	})

	t.Run("newer disconnect invalidates old forfeit key", func(t *testing.T) {
		r2 := cloneInProgressWithDisconnect(t, t0)
		st1, _ := r2.DisconnectState("p2")
		_ = r2.ReconnectPlayer(ReconnectPlayerCommand{
			CommandID: "rc1", PlayerID: "p2", DisconnectVersion: st1.DisconnectVersion,
			NowUTC: t0.Add(time.Second), ExpectedSequence: r2.Sequence(),
		})
		_ = r2.DisconnectPlayer(DisconnectPlayerCommand{
			CommandID: "d2", PlayerID: "p2", NowUTC: t0.Add(2 * time.Second), ExpectedSequence: r2.Sequence(),
		})
		st2, _ := r2.DisconnectState("p2")
		if st2.DisconnectVersion != st1.DisconnectVersion+1 {
			t.Fatalf("ver=%d", st2.DisconnectVersion)
		}
		out := r2.applyForfeitPlayer(ForfeitPlayerCommand{
			CommandID: "ff-old", PlayerID: "p2", DisconnectVersion: st1.DisconnectVersion,
			NowUTC: st2.DeadlineUTC, ExpectedSequence: r2.Sequence(),
		})
		if out.Rejection == nil || out.Rejection.Code != RejectDisconnectVersion {
			t.Fatalf("old forfeit should fail: %+v", out)
		}
	})
}

func cloneInProgressWithDisconnect(t *testing.T, t0 time.Time) *Room {
	t.Helper()
	r := mustCreate(t, "host")
	// unique create command ids via new rooms each time — CreateRoom uses fixed cmd in mustCreate
	// Remake with unique command ids:
	r, out := CreateRoom(CreateRoomCommand{
		CommandID: CommandID("create-" + t.Name()), RoomID: RoomID("room-" + t.Name()),
		HostID: "host", Visibility: VisibilityPublic, MaxSeats: 4,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatal(out)
	}
	join(t, r, CommandID("j-"+t.Name()), "p2")
	_ = r.LockRoom(LockRoomCommand{CommandID: CommandID("lock-" + t.Name()), ActorID: "host", ExpectedSequence: r.Sequence()})
	_ = r.StartMatch(StartMatchCommand{CommandID: CommandID("start-" + t.Name()), ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})
	_ = r.DisconnectPlayer(DisconnectPlayerCommand{
		CommandID: CommandID("d-" + t.Name()), PlayerID: "p2", NowUTC: t0, ExpectedSequence: r.Sequence(),
	})
	return r
}

func TestUnoWindow_OpennessAndExpiryBoundaries(t *testing.T) {
	r, _ := CreateRoom(CreateRoomCommand{
		CommandID: "c", RoomID: "r", HostID: "host", MaxSeats: 4,
	})
	join(t, r, "j1", "p2")
	_ = r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
	_ = r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})

	t0 := now()
	open := r.OpenUnoWindow(OpenUnoWindowCommand{
		CommandID: "uno-open", PlayerID: "host", GameID: "g1",
		TriggeringGameEventID: "ev-1", NowUTC: t0, ExpectedSequence: r.Sequence(),
	})
	if open.Kind != OutcomeAccepted || !HasFact(open.Facts, FactUnoWindowOpened) {
		t.Fatalf("%+v", open)
	}
	w, ok := r.UnoWindow()
	if !ok || !w.IsOpen() {
		t.Fatal("window not open")
	}
	if !w.ExpiresAt.Equal(t0.Add(UnoWindowDuration)) {
		t.Fatalf("expiresAt=%s", w.ExpiresAt)
	}
	if w.OpeningSequence != open.Sequence {
		t.Fatalf("openingSeq=%d want %d", w.OpeningSequence, open.Sequence)
	}

	t.Run("timely before deadline", func(t *testing.T) {
		if !w.IsTimely(w.ExpiresAt.Add(-time.Nanosecond)) {
			t.Fatal("should be timely")
		}
	})
	t.Run("not timely at exact deadline", func(t *testing.T) {
		if w.IsTimely(w.ExpiresAt) {
			t.Fatal("boundary should close")
		}
	})

	t.Run("expire before deadline rejected", func(t *testing.T) {
		out := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "exp-early", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "ev-1", OpeningSequence: w.OpeningSequence,
			NowUTC: w.ExpiresAt.Add(-time.Nanosecond), ExpectedSequence: r.Sequence(),
		})
		if out.Rejection == nil || out.Rejection.Code != RejectUnoWindowNotExpired {
			t.Fatalf("%+v", out)
		}
	})

	t.Run("expire at deadline accepted", func(t *testing.T) {
		out := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "exp-ok", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "ev-1", OpeningSequence: w.OpeningSequence,
			NowUTC: w.ExpiresAt, ExpectedSequence: r.Sequence(),
		})
		if out.Kind != OutcomeAccepted || !HasFact(out.Facts, FactUnoWindowExpired) {
			t.Fatalf("%+v", out)
		}
		if _, still := r.UnoWindow(); still {
			t.Fatal("window should be cleared")
		}
	})

	t.Run("duplicate expire command stable", func(t *testing.T) {
		dup := r.ExpireUnoWindow(ExpireUnoWindowCommand{
			CommandID: "exp-ok", PlayerID: "host", GameID: "g1",
			TriggeringGameEventID: "ev-1", OpeningSequence: w.OpeningSequence,
			NowUTC: w.ExpiresAt, ExpectedSequence: 0,
		})
		if dup.Kind != OutcomeDuplicate || !HasFact(dup.Facts, FactUnoWindowExpired) {
			t.Fatalf("%+v", dup)
		}
	})
}

func TestRoomUnoWindow_CloseByNextTurn(t *testing.T) {
	r, _ := CreateRoom(CreateRoomCommand{CommandID: "c", RoomID: "r", HostID: "host", MaxSeats: 4})
	join(t, r, "j1", "p2")
	_ = r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
	_ = r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})
	_ = r.OpenUnoWindow(OpenUnoWindowCommand{
		CommandID: "o", PlayerID: "host", GameID: "g1", TriggeringGameEventID: "e",
		NowUTC: now(), ExpectedSequence: r.Sequence(),
	})
	out := r.CloseUnoWindowByNextTurn(CloseUnoWindowByNextTurnCommand{CommandID: "close", ExpectedSequence: r.Sequence()})
	if out.Kind != OutcomeAccepted || !HasFact(out.Facts, FactUnoWindowClosed) {
		t.Fatalf("%+v", out)
	}
}

func TestUnoWindow_ValueObjectMismatch(t *testing.T) {
	w := OpenUnoWindow("p", "g", "ev", now(), 5)
	_, ok, code := w.Expire(now().Add(UnoWindowDuration), "g", "other", "ev", 5)
	if ok || code != RejectUnoWindowMismatch {
		t.Fatalf("ok=%v code=%s", ok, code)
	}
	_, ok, code = w.Expire(now().Add(UnoWindowDuration), "g", "p", "ev", 4)
	if ok || code != RejectUnoWindowMismatch {
		t.Fatalf("stale opening sequence ok=%v code=%s", ok, code)
	}
}
