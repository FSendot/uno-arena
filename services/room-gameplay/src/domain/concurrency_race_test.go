package domain

import (
	"testing"
)

func TestSequentialRace_JoinSerializationOneWinner(t *testing.T) {
	r := mustCreateAdHoc(t, "race-join", "host")
	seq := r.Sequence()

	// Two joins race on the same sequence; applied order determines the winner.
	first := r.JoinRoom(JoinRoomCommand{
		CommandID: "race-a", PlayerID: "alice", ExpectedSequence: seq,
	})
	second := r.JoinRoom(JoinRoomCommand{
		CommandID: "race-b", PlayerID: "bob", ExpectedSequence: seq,
	})
	assertAcceptedFacts(t, first, FactPlayerJoinedRoom)
	assertRejected(t, second, RejectStaleSequence)
	if !r.Roster().IsSeated("alice") || r.Roster().IsSeated("bob") {
		t.Fatal("only first sequenced join may seat")
	}

	// Bob retries with current sequence and wins the next slot.
	retry := r.JoinRoom(JoinRoomCommand{
		CommandID: "race-b-retry", PlayerID: "bob", ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, retry, FactPlayerJoinedRoom)
}

func TestSequentialRace_HostLeaveDeterminism(t *testing.T) {
	// Same initial roster, host leave always assigns lowest occupied seat.
	type result struct {
		host PlayerID
		seat string
	}
	run := func() result {
		r := mustCreateAdHoc(t, "race-host", "host")
		mustJoin(t, r, "j1", "p1")
		mustJoin(t, r, "j2", "p2")
		out := r.LeaveRoom(LeaveRoomCommand{
			CommandID: "leave-host",
			PlayerID:  "host", ExpectedSequence: r.Sequence(),
		})
		seat, _ := factData(out.Facts, FactHostReassigned, "seat")
		return result{host: r.HostID(), seat: seat}
	}
	a, b := run(), run()
	if a != b || a.host != "p1" || a.seat != "1" {
		t.Fatalf("non-deterministic host reassignment: %+v %+v", a, b)
	}
}

func TestSequentialRace_ConcurrentLockAttempts(t *testing.T) {
	r := waitingRoomWithTwo(t, "race-lock", "host", "p2")
	seq := r.Sequence()

	hostLock := r.LockRoom(LockRoomCommand{
		CommandID: "lock-host", ActorID: "host", ExpectedSequence: seq,
	})
	systemLock := r.LockRoom(LockRoomCommand{
		CommandID: "lock-sys", AsSystem: true, ExpectedSequence: seq,
	})
	assertAcceptedFacts(t, hostLock, FactRoomLocked)
	assertRejected(t, systemLock, RejectStaleSequence)
	if r.Status() != RoomStatusLocked || r.Sequence() != seq+1 {
		t.Fatalf("status=%s seq=%d", r.Status(), r.Sequence())
	}

	// Reverse application order on a fresh room: system wins, host goes stale.
	r2 := waitingRoomWithTwo(t, "race-lock-2", "host", "p2")
	seq2 := r2.Sequence()
	sysFirst := r2.LockRoom(LockRoomCommand{
		CommandID: "lock-sys-2", AsSystem: true, ExpectedSequence: seq2,
	})
	hostSecond := r2.LockRoom(LockRoomCommand{
		CommandID: "lock-host-2", ActorID: "host", ExpectedSequence: seq2,
	})
	assertAcceptedFacts(t, sysFirst, FactRoomLocked)
	// After lock, duplicate-style no-op for already locked with matching... wait,
	// hostSecond has stale sequence so RejectStaleSequence, not idempotent lock.
	assertRejected(t, hostSecond, RejectStaleSequence)
}

func TestSequentialRace_HostLeaveThenLockUsesNewHost(t *testing.T) {
	r := waitingRoomWithTwo(t, "race-host-lock", "host", "p2")
	mustJoin(t, r, "join-p3", "p3")
	out := r.LeaveRoom(LeaveRoomCommand{
		CommandID: "leave-h", PlayerID: "host", ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, out, FactPlayerLeftRoom, FactHostReassigned)
	if r.HostID() != "p2" {
		t.Fatalf("host=%s", r.HostID())
	}
	// Old host cannot lock; new host can.
	old := r.LockRoom(LockRoomCommand{
		CommandID: "lock-old", ActorID: "host", ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, old, RejectNotHost)
	mustLock(t, r, "p2", false)
}

func TestSequentialRace_EmptyCancelVsJoinOrder(t *testing.T) {
	// If host leave empties the room first, join at stale sequence cannot revive it.
	r := mustCreateAdHoc(t, "race-empty", "host")
	seq := r.Sequence()
	leave := r.LeaveRoom(LeaveRoomCommand{
		CommandID: "leave", PlayerID: "host", ExpectedSequence: seq,
	})
	assertAcceptedFacts(t, leave, FactPlayerLeftRoom, FactRoomCancelled, FactSpectatorStreamsClose)
	join := r.JoinRoom(JoinRoomCommand{
		CommandID: "late-join", PlayerID: "p2", ExpectedSequence: seq,
	})
	assertRejected(t, join, RejectStaleSequence)
	if r.Status() != RoomStatusCancelled {
		t.Fatalf("status=%s", r.Status())
	}

	// Join with current sequence still fails because terminal.
	join2 := r.JoinRoom(JoinRoomCommand{
		CommandID: "join-term", PlayerID: "p2", ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, join2, RejectAlreadyTerminal)
}
