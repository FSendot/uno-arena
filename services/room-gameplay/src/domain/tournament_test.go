package domain

import "testing"

func TestProvisionTournamentRoom_IdentityAndSystemLock(t *testing.T) {
	r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
		CommandID:    "prov-1",
		RoomID:       "t-room-1",
		TournamentID: "tourn-9",
		HostID:       "seed-a",
		Visibility:   VisibilityPrivate,
	})
	assertAcceptedFacts(t, out, FactRoomCreated, FactPlayerJoinedRoom)
	if r.Type() != RoomTypeTournament || r.TournamentID() != "tourn-9" {
		t.Fatalf("type=%s tid=%s", r.Type(), r.TournamentID())
	}
	if v, ok := factData(out.Facts, FactRoomCreated, "tournamentId"); !ok || v != "tourn-9" {
		t.Fatalf("tournamentId fact=%q", v)
	}

	mustJoin(t, r, "join-b", "seed-b")

	// Non-host without AsSystem rejected.
	out = r.LockRoom(LockRoomCommand{
		CommandID:        "lock-player",
		ActorID:          "seed-b",
		ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, out, RejectNotHost)

	// System lock succeeds for tournament provisioning path.
	out = r.LockRoom(LockRoomCommand{
		CommandID:        "lock-sys",
		AsSystem:         true,
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactRoomLocked)

	out = r.StartMatch(StartMatchCommand{
		CommandID:        "start-sys",
		AsSystem:         true,
		GameID:           "tg1",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactMatchStarted, FactGameStarted)
	if r.Status() != RoomStatusInProgress {
		t.Fatalf("status=%s", r.Status())
	}
}

func TestProvisionTournamentRoom_RequiresTournamentID(t *testing.T) {
	r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
		CommandID: "prov-bad",
		RoomID:    "t-room-bad",
		HostID:    "seed-a",
	})
	if r != nil {
		t.Fatal("expected nil room")
	}
	assertRejected(t, out, RejectTournamentIdentity)
}

func TestProvisionTournamentRoom_MaxSeatsBounds(t *testing.T) {
	tests := []struct {
		name     string
		maxSeats int
		wantOK   bool
		wantCap  int
	}{
		{"default zero", 0, true, DefaultMaxSeats},
		{"negative default", -1, true, DefaultMaxSeats},
		{"min 2", 2, true, 2},
		{"max 10", 10, true, 10},
		{"explicit 1", 1, false, 0},
		{"over 10", 11, false, 0},
		{"uint8 wrap 256", 256, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
				CommandID: CommandID("prov-" + tt.name), RoomID: RoomID("tr-" + tt.name),
				TournamentID: "t1", HostID: "a", MaxSeats: tt.maxSeats,
			})
			if tt.wantOK {
				if r == nil || out.Kind != OutcomeAccepted {
					t.Fatalf("%+v", out)
				}
				if r.Roster().Capacity() != tt.wantCap {
					t.Fatalf("capacity=%d want %d", r.Roster().Capacity(), tt.wantCap)
				}
				return
			}
			if r != nil {
				t.Fatal("expected nil room")
			}
			assertRejected(t, out, RejectInvalidMaxSeats)
		})
	}
}

func TestTournamentHostLeave_BeforeLockDoesNotUseAdHocReassignPath(t *testing.T) {
	// Ad-hoc host reassignment is ad-hoc only; tournament rooms keep host label
	// without HostReassigned on leave (system owns lock/start).
	r, out := ProvisionTournamentRoom(ProvisionTournamentRoomCommand{
		CommandID:    "prov-2",
		RoomID:       "t-room-2",
		TournamentID: "tourn-2",
		HostID:       "seed-a",
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("%+v", out)
	}
	mustJoin(t, r, "join-b", "seed-b")
	out = r.LeaveRoom(LeaveRoomCommand{
		CommandID:        "leave-a",
		PlayerID:         "seed-a",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerLeftRoom)
	if HasFact(out.Facts, FactHostReassigned) {
		t.Fatal("tournament leave must not emit HostReassigned")
	}
	if HasFact(out.Facts, FactRoomCancelled) {
		t.Fatal("tournament leave with remaining players must not cancel")
	}
}
