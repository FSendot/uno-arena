package domain

import "testing"

func TestCreateRoom_LifecycleHappyPath(t *testing.T) {
	r, out := CreateRoom(CreateRoomCommand{
		CommandID:  "c1",
		RoomID:     "room-1",
		HostID:     "host",
		Visibility: VisibilityPublic,
	})
	assertAcceptedFacts(t, out, FactRoomCreated, FactPlayerJoinedRoom)
	if r.Status() != RoomStatusWaiting || r.Sequence() != 1 || r.HostID() != "host" {
		t.Fatalf("status=%s seq=%d host=%s", r.Status(), r.Sequence(), r.HostID())
	}
	if r.Type() != RoomTypeAdHoc || r.TournamentID() != "" {
		t.Fatalf("type=%s tournament=%s", r.Type(), r.TournamentID())
	}
	if r.Roster().OccupiedCount() != 1 {
		t.Fatalf("occupied=%d", r.Roster().OccupiedCount())
	}

	mustJoin(t, r, "j1", "p2")
	mustLock(t, r, "host", false)
	if r.Status() != RoomStatusLocked || r.HostHasGameplayAuthority() {
		t.Fatalf("after lock status=%s hostAuth=%v", r.Status(), r.HostHasGameplayAuthority())
	}
	mustStart(t, r, "host", "game-1", false)
	if r.Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", r.Status())
	}

	// Match/room completion is owned by Session; Room.CompleteMatch is guarded.
	out = r.CompleteMatch(CompleteMatchCommand{CommandID: "cm1", ExpectedSequence: r.Sequence()})
	assertRejected(t, out, RejectMatchOwnsCompletion)
	if r.Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", r.Status())
	}
}

func TestCancelRoom_WaitingOnly(t *testing.T) {
	r := mustCreateAdHoc(t, "room-cancel", "host")
	out := r.CancelRoom(CancelRoomCommand{
		CommandID:        "cancel-1",
		ActorID:          "host",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactRoomCancelled, FactSpectatorStreamsClose)
	if r.Status() != RoomStatusCancelled {
		t.Fatalf("status=%s", r.Status())
	}

	r2 := waitingRoomWithTwo(t, "room-cancel-2", "host", "p2")
	mustLock(t, r2, "host", false)
	out = r2.CancelRoom(CancelRoomCommand{
		CommandID:        "cancel-locked",
		ActorID:          "host",
		ExpectedSequence: r2.Sequence(),
	})
	assertRejected(t, out, RejectNotWaiting)
}

func TestLifecycle_TableDrivenTransitions(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) *Room
		act        func(r *Room) CommandOutcome
		wantStatus RoomStatus
		wantCode   RejectionCode
		wantFacts  []FactName
	}{
		{
			name: "join while waiting",
			setup: func(t *testing.T) *Room {
				return mustCreateAdHoc(t, "L1", "host")
			},
			act: func(r *Room) CommandOutcome {
				return r.JoinRoom(JoinRoomCommand{CommandID: "j", PlayerID: "p2", ExpectedSequence: r.Sequence()})
			},
			wantStatus: RoomStatusWaiting,
			wantFacts:  []FactName{FactPlayerJoinedRoom},
		},
		{
			name: "join after lock rejected",
			setup: func(t *testing.T) *Room {
				r := waitingRoomWithTwo(t, "L2", "host", "p2")
				mustLock(t, r, "host", false)
				return r
			},
			act: func(r *Room) CommandOutcome {
				return r.JoinRoom(JoinRoomCommand{CommandID: "j", PlayerID: "p3", ExpectedSequence: r.Sequence()})
			},
			wantStatus: RoomStatusLocked,
			wantCode:   RejectJoinAfterLock,
		},
		{
			name: "lock with one player rejected",
			setup: func(t *testing.T) *Room {
				return mustCreateAdHoc(t, "L3", "host")
			},
			act: func(r *Room) CommandOutcome {
				return r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
			},
			wantStatus: RoomStatusWaiting,
			wantCode:   RejectInsufficientRoster,
		},
		{
			name: "start from waiting rejected",
			setup: func(t *testing.T) *Room {
				return waitingRoomWithTwo(t, "L4", "host", "p2")
			},
			act: func(r *Room) CommandOutcome {
				return r.StartMatch(StartMatchCommand{CommandID: "s", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})
			},
			wantStatus: RoomStatusWaiting,
			wantCode:   RejectNotLocked,
		},
		{
			name: "non-host lock rejected",
			setup: func(t *testing.T) *Room {
				return waitingRoomWithTwo(t, "L5", "host", "p2")
			},
			act: func(r *Room) CommandOutcome {
				return r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "p2", ExpectedSequence: r.Sequence()})
			},
			wantStatus: RoomStatusWaiting,
			wantCode:   RejectNotHost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			out := tt.act(r)
			if r.Status() != tt.wantStatus {
				t.Fatalf("status=%s want %s", r.Status(), tt.wantStatus)
			}
			if tt.wantCode != "" {
				assertRejected(t, out, tt.wantCode)
				return
			}
			assertAcceptedFacts(t, out, tt.wantFacts...)
		})
	}
}

func TestHostLeave_ReassignsLowestOccupiedSeat(t *testing.T) {
	r := mustCreateAdHoc(t, "host-reassign", "host")
	mustJoin(t, r, "j-b", "bob")   // seat 1
	mustJoin(t, r, "j-a", "alice") // seat 2
	// Host leave -> lowest occupied seat is bob @ 1.
	out := r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-host",
		PlayerID:         "host",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerLeftRoom, FactHostReassigned)
	if r.HostID() != "bob" {
		t.Fatalf("host=%s want bob", r.HostID())
	}
	seat, ok := factData(out.Facts, FactHostReassigned, "seat")
	if !ok || seat != "1" {
		t.Fatalf("reassigned seat=%q", seat)
	}
}

func TestHostLeave_DeterministicLowestSeatWhenMiddleEmpty(t *testing.T) {
	r := mustCreateAdHoc(t, "host-gap", "host") // seat 0
	mustJoin(t, r, "j1", "p1")                  // seat 1
	mustJoin(t, r, "j2", "p2")                  // seat 2
	out := r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-p1",
		PlayerID:         "p1",
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("leave p1: %+v", out)
	}
	// seats: host@0, empty@1, p2@2 — host leaves -> lowest occupied is p2@2
	out = r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-host",
		PlayerID:         "host",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerLeftRoom, FactHostReassigned)
	if r.HostID() != "p2" {
		t.Fatalf("host=%s want p2", r.HostID())
	}
	seat, _ := factData(out.Facts, FactHostReassigned, "seat")
	if seat != "2" {
		t.Fatalf("seat=%s want 2", seat)
	}
}

func TestHostLeave_EmptyRoomCancelsImmediately(t *testing.T) {
	r := mustCreateAdHoc(t, "host-empty", "host")
	out := r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-only",
		PlayerID:         "host",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerLeftRoom, FactRoomCancelled, FactSpectatorStreamsClose)
	if r.Status() != RoomStatusCancelled || r.Roster().OccupiedCount() != 0 {
		t.Fatalf("status=%s occupied=%d", r.Status(), r.Roster().OccupiedCount())
	}
	dec := r.SpectatorAdmission(SpectatorAuth{Authorized: true})
	if dec.Allowed || dec.Code != RejectSpectatorTerminal {
		t.Fatalf("admission=%+v", dec)
	}
}

func TestHostLeave_AfterLockNoReassignmentOrAuthority(t *testing.T) {
	r := waitingRoomWithTwo(t, "host-after-lock", "host", "p2")
	mustLock(t, r, "host", false)
	if r.HostHasGameplayAuthority() {
		t.Fatal("host must lose gameplay authority after lock")
	}
	out := r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-host",
		PlayerID:         "host",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerLeftRoom)
	if HasFact(out.Facts, FactHostReassigned) {
		t.Fatal("must not reassign host after lock")
	}
	// Recorded host id remains; authority flag is status-based.
	if r.HostID() != "host" {
		t.Fatalf("host label changed to %s", r.HostID())
	}
	if r.HostHasGameplayAuthority() {
		t.Fatal("host still has authority")
	}
	// Non-system start by remaining player rejected (not recorded host).
	out = r.StartMatch(StartMatchCommand{
		CommandID:        "start-p2",
		ActorID:          "p2",
		GameID:           "g1",
		ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, out, RejectNotHost)
	// Departed recorded host cannot start while unseated.
	out = r.StartMatch(StartMatchCommand{
		CommandID:        "start-exhost",
		ActorID:          "host",
		GameID:           "g1",
		ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, out, RejectNotHost)
	// System may still start.
	mustStart(t, r, "", "g1", true)
}

func TestLeave_AlreadyAbsentIdempotent(t *testing.T) {
	r := waitingRoomWithTwo(t, "leave-absent", "host", "p2")
	out := r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-1",
		PlayerID:         "p2",
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%+v", out)
	}
	seq := r.Sequence()
	out = r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-2",
		PlayerID:         "p2",
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 || r.Sequence() != seq {
		t.Fatalf("absent leave should no-op: %+v seq=%d want %d", out, r.Sequence(), seq)
	}
}

func TestJoin_AlreadySeatedIdempotentNoSequenceBump(t *testing.T) {
	r := mustCreateAdHoc(t, "join-seated", "host")
	seq := r.Sequence()
	out := r.JoinRoom(JoinRoomCommand{
		CommandID:        "rejoin-host",
		PlayerID:         "host",
		ExpectedSequence: seq,
	})
	if out.Kind != OutcomeAccepted || len(out.Facts) != 0 || r.Sequence() != seq {
		t.Fatalf("already seated join should no-op: %+v", out)
	}
}
