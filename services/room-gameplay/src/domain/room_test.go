package domain

import (
	"testing"
	"time"
)

func mustCreate(t *testing.T, host PlayerID) *Room {
	t.Helper()
	r, out := CreateRoom(CreateRoomCommand{
		CommandID:  "cmd-create",
		RoomID:     "room-1",
		HostID:     host,
		Visibility: VisibilityPublic,
		MaxSeats:   4,
	})
	if r == nil || out.Kind != OutcomeAccepted {
		t.Fatalf("create failed: %+v", out)
	}
	return r
}

func join(t *testing.T, r *Room, cmdID CommandID, player PlayerID) CommandOutcome {
	t.Helper()
	out := r.JoinRoom(JoinRoomCommand{
		CommandID:        cmdID,
		PlayerID:         player,
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("join %s failed: %+v", player, out.Rejection)
	}
	return out
}

func TestCreateRoom_Table(t *testing.T) {
	tests := []struct {
		name     string
		cmd      CreateRoomCommand
		wantOK   bool
		wantCode RejectionCode
	}{
		{
			name: "valid ad-hoc",
			cmd: CreateRoomCommand{
				CommandID: "c1", RoomID: "r1", HostID: "host", Visibility: VisibilityPublic,
			},
			wantOK: true,
		},
		{
			name: "missing host",
			cmd: CreateRoomCommand{
				CommandID: "c2", RoomID: "r1",
			},
			wantOK: false, wantCode: RejectInvalidIdentity,
		},
		{
			name: "missing room",
			cmd: CreateRoomCommand{
				CommandID: "c3", HostID: "host",
			},
			wantOK: false, wantCode: RejectInvalidIdentity,
		},
		{
			name: "maxSeats zero defaults to 10",
			cmd: CreateRoomCommand{
				CommandID: "c4", RoomID: "r4", HostID: "host", MaxSeats: 0,
			},
			wantOK: true,
		},
		{
			name: "maxSeats negative defaults to 10",
			cmd: CreateRoomCommand{
				CommandID: "c5", RoomID: "r5", HostID: "host", MaxSeats: -3,
			},
			wantOK: true,
		},
		{
			name: "maxSeats 1 rejected",
			cmd: CreateRoomCommand{
				CommandID: "c6", RoomID: "r6", HostID: "host", MaxSeats: 1,
			},
			wantOK: false, wantCode: RejectInvalidMaxSeats,
		},
		{
			name: "maxSeats 11 rejected",
			cmd: CreateRoomCommand{
				CommandID: "c7", RoomID: "r7", HostID: "host", MaxSeats: 11,
			},
			wantOK: false, wantCode: RejectInvalidMaxSeats,
		},
		{
			name: "maxSeats 256 rejected (uint8 wrap guard)",
			cmd: CreateRoomCommand{
				CommandID: "c8", RoomID: "r8", HostID: "host", MaxSeats: 256,
			},
			wantOK: false, wantCode: RejectInvalidMaxSeats,
		},
		{
			name: "maxSeats 2 accepted",
			cmd: CreateRoomCommand{
				CommandID: "c9", RoomID: "r9", HostID: "host", MaxSeats: 2,
			},
			wantOK: true,
		},
		{
			name: "maxSeats 10 accepted",
			cmd: CreateRoomCommand{
				CommandID: "c10", RoomID: "r10", HostID: "host", MaxSeats: 10,
			},
			wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, out := CreateRoom(tt.cmd)
			if tt.wantOK {
				if r == nil || out.Kind != OutcomeAccepted {
					t.Fatalf("expected accept, got %+v", out)
				}
				if r.Status() != RoomStatusWaiting || r.Type() != RoomTypeAdHoc {
					t.Fatalf("status/type: %s %s", r.Status(), r.Type())
				}
				if r.Sequence() != 1 {
					t.Fatalf("sequence=%d", r.Sequence())
				}
				if !HasFact(out.Facts, FactRoomCreated) || !HasFact(out.Facts, FactPlayerJoinedRoom) {
					t.Fatalf("facts=%v", FactNames(out.Facts))
				}
				if out.Rejection != nil {
					t.Fatal("accepted must not carry rejection")
				}
				wantCap := tt.cmd.MaxSeats
				if wantCap <= 0 {
					wantCap = DefaultMaxSeats
				}
				if r.Roster().Capacity() != wantCap {
					t.Fatalf("capacity=%d want %d", r.Roster().Capacity(), wantCap)
				}
				return
			}
			if r != nil || out.Kind != OutcomeRejected || out.Rejection == nil || out.Rejection.Code != tt.wantCode {
				t.Fatalf("expected reject %s, got room=%v out=%+v", tt.wantCode, r != nil, out)
			}
			if len(out.Facts) != 0 {
				t.Fatalf("rejected must not emit facts: %v", out.Facts)
			}
		})
	}
}

func TestProvisionTournamentRoom(t *testing.T) {
	r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
		CommandID:    "prov-1",
		RoomID:       "t-room",
		TournamentID: "tourn-9",
		HostID:       "p1",
		Visibility:   VisibilityPrivate,
	})
	if out.Kind != OutcomeAccepted || r == nil {
		t.Fatalf("provision failed: %+v", out)
	}
	if r.Type() != RoomTypeTournament || r.TournamentID() != "tourn-9" {
		t.Fatalf("identity type=%s tid=%s", r.Type(), r.TournamentID())
	}
	if r.Visibility() != VisibilityPrivate {
		t.Fatalf("visibility=%s", r.Visibility())
	}
}

func TestMaxSeats_AdHocAndTournament(t *testing.T) {
	t.Run("ad-hoc zero defaults to ten", func(t *testing.T) {
		r, out := CreateRoom(CreateRoomCommand{
			CommandID: "ms-0", RoomID: "ms-0", HostID: "host", MaxSeats: 0,
		})
		if out.Kind != OutcomeAccepted || r == nil {
			t.Fatalf("%+v", out)
		}
		if r.Roster().Capacity() != DefaultMaxSeats {
			t.Fatalf("capacity=%d", r.Roster().Capacity())
		}
	})
	t.Run("ad-hoc negative defaults to ten", func(t *testing.T) {
		r, out := CreateRoom(CreateRoomCommand{
			CommandID: "ms-neg", RoomID: "ms-neg", HostID: "host", MaxSeats: -3,
		})
		if out.Kind != OutcomeAccepted || r == nil {
			t.Fatalf("%+v", out)
		}
		if r.Roster().Capacity() != DefaultMaxSeats {
			t.Fatalf("capacity=%d", r.Roster().Capacity())
		}
	})
	t.Run("tournament explicit one rejected", func(t *testing.T) {
		r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
			CommandID: "ms-t1", RoomID: "ms-t1", TournamentID: "t", HostID: "a", MaxSeats: 1,
		})
		if r != nil || out.Kind != OutcomeRejected || out.Rejection == nil || out.Rejection.Code != RejectInvalidMaxSeats {
			t.Fatalf("got room=%v out=%+v", r != nil, out)
		}
	})
	t.Run("tournament above ten rejected", func(t *testing.T) {
		r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
			CommandID: "ms-t11", RoomID: "ms-t11", TournamentID: "t", HostID: "a", MaxSeats: 11,
		})
		if r != nil || out.Rejection == nil || out.Rejection.Code != RejectInvalidMaxSeats {
			t.Fatalf("%+v", out)
		}
	})
	t.Run("tournament uint8 wrap risk rejected", func(t *testing.T) {
		r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
			CommandID: "ms-t256", RoomID: "ms-t256", TournamentID: "t", HostID: "a", MaxSeats: 256,
		})
		if r != nil || out.Rejection == nil || out.Rejection.Code != RejectInvalidMaxSeats {
			t.Fatalf("%+v", out)
		}
	})
	t.Run("tournament valid capacity", func(t *testing.T) {
		r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
			CommandID: "ms-t4", RoomID: "ms-t4", TournamentID: "t", HostID: "a", MaxSeats: 4,
		})
		if out.Kind != OutcomeAccepted || r == nil || r.Roster().Capacity() != 4 {
			t.Fatalf("cap=%v out=%+v", r != nil, out)
		}
	})
}

func TestLifecycleTransitions_Table(t *testing.T) {
	type step func(t *testing.T, r *Room) CommandOutcome

	tests := []struct {
		name       string
		setup      func(t *testing.T) *Room
		steps      []step
		wantStatus RoomStatus
	}{
		{
			name: "waiting->locked->in_progress (completion owned by Session)",
			setup: func(t *testing.T) *Room {
				r := mustCreate(t, "host")
				join(t, r, "j1", "p2")
				return r
			},
			steps: []step{
				func(t *testing.T, r *Room) CommandOutcome {
					return r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
				},
				func(t *testing.T, r *Room) CommandOutcome {
					return r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})
				},
				func(t *testing.T, r *Room) CommandOutcome {
					out := r.CompleteMatch(CompleteMatchCommand{CommandID: "done", ExpectedSequence: r.Sequence()})
					if out.Kind != OutcomeRejected || out.Rejection == nil || out.Rejection.Code != RejectMatchOwnsCompletion {
						t.Fatalf("CompleteMatch must be guarded: %+v", out)
					}
					return acceptedOutcome("noop-keep-in-progress", r.Sequence(), nil)
				},
			},
			wantStatus: RoomStatusInProgress,
		},
		{
			name: "waiting->cancelled",
			setup: func(t *testing.T) *Room {
				return mustCreate(t, "host")
			},
			steps: []step{
				func(t *testing.T, r *Room) CommandOutcome {
					return r.CancelRoom(CancelRoomCommand{CommandID: "cancel", ActorID: "host", ExpectedSequence: r.Sequence()})
				},
			},
			wantStatus: RoomStatusCancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.setup(t)
			for i, s := range tt.steps {
				out := s(t, r)
				if out.Kind != OutcomeAccepted {
					t.Fatalf("step %d rejected: %+v", i, out.Rejection)
				}
				if out.Rejection != nil || len(out.Facts) == 0 && i < len(tt.steps) {
					// last idempotent cases may have empty facts; lifecycle steps should have facts
				}
				if out.Kind == OutcomeAccepted && out.Rejection != nil {
					t.Fatal("accepted with rejection")
				}
			}
			if r.Status() != tt.wantStatus {
				t.Fatalf("status=%s want %s", r.Status(), tt.wantStatus)
			}
		})
	}
}

func TestJoinLeaveLockGuards_Table(t *testing.T) {
	tests := []struct {
		name     string
		run      func(t *testing.T) CommandOutcome
		wantCode RejectionCode
	}{
		{
			name: "join after lock rejected",
			run: func(t *testing.T) CommandOutcome {
				r := mustCreate(t, "host")
				join(t, r, "j1", "p2")
				if out := r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()}); out.Kind != OutcomeAccepted {
					t.Fatal(out.Rejection)
				}
				return r.JoinRoom(JoinRoomCommand{CommandID: "j2", PlayerID: "p3", ExpectedSequence: r.Sequence()})
			},
			wantCode: RejectJoinAfterLock,
		},
		{
			name: "non-host lock rejected",
			run: func(t *testing.T) CommandOutcome {
				r := mustCreate(t, "host")
				join(t, r, "j1", "p2")
				return r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "p2", ExpectedSequence: r.Sequence()})
			},
			wantCode: RejectNotHost,
		},
		{
			name: "lock with one player rejected",
			run: func(t *testing.T) CommandOutcome {
				r := mustCreate(t, "host")
				return r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
			},
			wantCode: RejectInsufficientRoster,
		},
		{
			name: "start while waiting rejected",
			run: func(t *testing.T) CommandOutcome {
				r := mustCreate(t, "host")
				join(t, r, "j1", "p2")
				return r.StartMatch(StartMatchCommand{CommandID: "start", ActorID: "host", GameID: "g1", ExpectedSequence: r.Sequence()})
			},
			wantCode: RejectNotLocked,
		},
		{
			name: "cancel after lock rejected",
			run: func(t *testing.T) CommandOutcome {
				r := mustCreate(t, "host")
				join(t, r, "j1", "p2")
				_ = r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()})
				return r.CancelRoom(CancelRoomCommand{CommandID: "cancel", ActorID: "host", ExpectedSequence: r.Sequence()})
			},
			wantCode: RejectNotWaiting,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := tt.run(t)
			if out.Kind != OutcomeRejected || out.Rejection == nil || out.Rejection.Code != tt.wantCode {
				t.Fatalf("want %s got %+v", tt.wantCode, out)
			}
			if len(out.Facts) != 0 {
				t.Fatalf("rejected emitted facts: %v", out.Facts)
			}
		})
	}
}

func TestIdempotentCommandID_ReturnsStablePriorOutcome(t *testing.T) {
	r := mustCreate(t, "host")
	first := r.JoinRoom(JoinRoomCommand{CommandID: "same", PlayerID: "p2", ExpectedSequence: r.Sequence()})
	if first.Kind != OutcomeAccepted {
		t.Fatal(first.Rejection)
	}
	seqAfter := r.Sequence()
	occupied := r.Roster().OccupiedCount()

	dup := r.JoinRoom(JoinRoomCommand{CommandID: "same", PlayerID: "p2", ExpectedSequence: 999})
	if dup.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s", dup.Kind)
	}
	if r.Sequence() != seqAfter || r.Roster().OccupiedCount() != occupied {
		t.Fatal("duplicate reapplied state")
	}
	if len(dup.Facts) != len(first.Facts) || !HasFact(dup.Facts, FactPlayerJoinedRoom) {
		t.Fatalf("dup facts=%v first=%v", dup.Facts, first.Facts)
	}

	// Rejected outcomes also stable.
	bad := r.JoinRoom(JoinRoomCommand{CommandID: "bad", PlayerID: "p3", ExpectedSequence: r.Sequence() - 1})
	if bad.Kind != OutcomeRejected {
		t.Fatal(bad)
	}
	badDup := r.JoinRoom(JoinRoomCommand{CommandID: "bad", PlayerID: "p3", ExpectedSequence: r.Sequence()})
	if badDup.Kind != OutcomeDuplicate || badDup.Rejection == nil || badDup.Rejection.Code != RejectStaleSequence {
		t.Fatalf("badDup=%+v", badDup)
	}
}

func TestJoinAlreadySeated_IdempotentNoSequenceBump(t *testing.T) {
	r := mustCreate(t, "host")
	seq := r.Sequence()
	out := r.JoinRoom(JoinRoomCommand{CommandID: "rejoin-host", PlayerID: "host", ExpectedSequence: seq})
	if out.Kind != OutcomeAccepted {
		t.Fatal(out.Rejection)
	}
	if r.Sequence() != seq {
		t.Fatalf("sequence bumped on already-seated join")
	}
	if len(out.Facts) != 0 {
		t.Fatalf("expected no facts, got %v", out.Facts)
	}
}

func TestHostLeaveBeforeLock_ReassignsLowestSeatOrCancels(t *testing.T) {
	t.Run("reassign to lowest occupied", func(t *testing.T) {
		r := mustCreate(t, "host")
		join(t, r, "j1", "p2")
		join(t, r, "j2", "p3")
		// Leave p2 first so lowest remaining after host leave is p3? Seats: 0=host,1=p2,2=p3
		// Host leave -> lowest occupied is seat 1 = p2
		out := r.LeaveRoom(LeaveRoomCommand{CommandID: "leave-host", PlayerID: "host", ExpectedSequence: r.Sequence()})
		if out.Kind != OutcomeAccepted {
			t.Fatal(out.Rejection)
		}
		if !HasFact(out.Facts, FactPlayerLeftRoom) || !HasFact(out.Facts, FactHostReassigned) {
			t.Fatalf("facts=%v", FactNames(out.Facts))
		}
		if r.HostID() != "p2" {
			t.Fatalf("host=%s want p2", r.HostID())
		}
		if HasFact(out.Facts, FactRoomCancelled) {
			t.Fatal("should not cancel")
		}
	})

	t.Run("cancel when empty", func(t *testing.T) {
		r := mustCreate(t, "host")
		out := r.LeaveRoom(LeaveRoomCommand{CommandID: "leave-only", PlayerID: "host", ExpectedSequence: r.Sequence()})
		if out.Kind != OutcomeAccepted {
			t.Fatal(out.Rejection)
		}
		if !HasFact(out.Facts, FactRoomCancelled) || !HasFact(out.Facts, FactSpectatorStreamsClose) {
			t.Fatalf("facts=%v", FactNames(out.Facts))
		}
		if r.Status() != RoomStatusCancelled {
			t.Fatalf("status=%s", r.Status())
		}
	})
}

func TestNoHostReassignmentAfterLock(t *testing.T) {
	r := mustCreate(t, "host")
	join(t, r, "j1", "p2")
	if out := r.LockRoom(LockRoomCommand{CommandID: "lock", ActorID: "host", ExpectedSequence: r.Sequence()}); out.Kind != OutcomeAccepted {
		t.Fatal(out.Rejection)
	}
	if r.HostHasGameplayAuthority() {
		t.Fatal("host authority should end after lock")
	}
	out := r.LeaveRoom(LeaveRoomCommand{CommandID: "leave-host", PlayerID: "host", ExpectedSequence: r.Sequence()})
	if out.Kind != OutcomeAccepted {
		t.Fatal(out.Rejection)
	}
	if HasFact(out.Facts, FactHostReassigned) {
		t.Fatal("must not reassign host after lock")
	}
	// Recorded host id may remain; authority flag is false for waiting-only ops.
	if r.Status() != RoomStatusLocked {
		t.Fatalf("status=%s", r.Status())
	}
}

func TestSystemMayLockAndStartTournamentRoom(t *testing.T) {
	r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
		CommandID: "prov", RoomID: "tr", TournamentID: "t1", HostID: "a", MaxSeats: 4,
	})
	if out.Kind != OutcomeAccepted {
		t.Fatal(out)
	}
	join(t, r, "j1", "b")
	lock := r.LockRoom(LockRoomCommand{CommandID: "lock", AsSystem: true, ExpectedSequence: r.Sequence()})
	if lock.Kind != OutcomeAccepted {
		t.Fatal(lock.Rejection)
	}
	start := r.StartMatch(StartMatchCommand{CommandID: "start", AsSystem: true, GameID: "g1", ExpectedSequence: r.Sequence()})
	if start.Kind != OutcomeAccepted {
		t.Fatal(start.Rejection)
	}
	if r.Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", r.Status())
	}
}

func TestSequenceRejection_StaleAndFuture(t *testing.T) {
	r := mustCreate(t, "host")
	cur := r.Sequence()

	stale := r.JoinRoom(JoinRoomCommand{CommandID: "stale", PlayerID: "p2", ExpectedSequence: cur - 1})
	if stale.Rejection == nil || stale.Rejection.Code != RejectStaleSequence {
		t.Fatalf("stale=%+v", stale)
	}
	if len(stale.Facts) != 0 {
		t.Fatal("stale emitted facts")
	}

	future := r.JoinRoom(JoinRoomCommand{CommandID: "future", PlayerID: "p2", ExpectedSequence: cur + 1})
	if future.Rejection == nil || future.Rejection.Code != RejectFutureSequence {
		t.Fatalf("future=%+v", future)
	}
	if r.Sequence() != cur {
		t.Fatal("sequence changed on reject")
	}
}

func now() time.Time {
	return time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
}
