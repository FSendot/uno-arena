package domain

import (
	"sync"
	"testing"
)

func TestHostRace_DeterministicLowestSeatWins(t *testing.T) {
	// Two concurrent conceptual leave orders after host is gone are not the case;
	// host leave with multiple remaining players always picks lowest occupied seat.
	orders := [][]PlayerID{
		{"a", "b", "c"},
		{"a", "c", "b"},
	}
	for _, order := range orders {
		r, out := CreateRoom(CreateRoomCommand{
			CommandID: CommandID("c-" + string(order[0]) + string(order[1])),
			RoomID:    RoomID("r-" + string(order[0]) + string(order[1])),
			HostID:    "host",
			MaxSeats:  4,
		})
		if out.Kind != OutcomeAccepted {
			t.Fatal(out)
		}
		for i, p := range order {
			join(t, r, CommandID("j"+string(rune('1'+i))+string(p)), p)
		}
		leave := r.LeaveRoom(LeaveRoomCommand{
			CommandID: "leave-host", PlayerID: "host", ExpectedSequence: r.Sequence(),
		})
		if leave.Kind != OutcomeAccepted || !HasFact(leave.Facts, FactHostReassigned) {
			t.Fatalf("%+v", leave)
		}
		// Seats: 0 host(cleared), 1=order[0], 2=order[1], 3=order[2] depending on join order.
		// Join order is `order`, so lowest occupied is always order[0].
		if r.HostID() != order[0] {
			t.Fatalf("host=%s want %s for join order %v", r.HostID(), order[0], order)
		}
	}
}

func TestConcurrentJoinSerialization_SequenceContract(t *testing.T) {
	// Simulate racing joins: only commands with matching sequence succeed when applied serially.
	r, _ := CreateRoom(CreateRoomCommand{
		CommandID: "c", RoomID: "race-room", HostID: "host", MaxSeats: 4,
	})
	base := r.Sequence()

	type result struct {
		player PlayerID
		out    CommandOutcome
	}
	candidates := []PlayerID{"p2", "p3", "p4"}
	var accepted []PlayerID
	for _, p := range candidates {
		out := r.JoinRoom(JoinRoomCommand{
			CommandID:        CommandID("join-" + p),
			PlayerID:         p,
			ExpectedSequence: base, // all race on same sequence snapshot
		})
		if out.Kind == OutcomeAccepted {
			accepted = append(accepted, p)
			base = r.Sequence()
		} else if out.Rejection == nil || out.Rejection.Code != RejectStaleSequence {
			// first uses base and advances; subsequent with old base are stale
			if out.Rejection != nil && out.Rejection.Code == RejectStaleSequence {
				continue
			}
			t.Fatalf("unexpected %+v", out)
		}
	}
	// Re-run properly: first accept wins at base, others need refresh — verify only one accepted at same seq.
	r2, _ := CreateRoom(CreateRoomCommand{CommandID: "c2", RoomID: "race-2", HostID: "host", MaxSeats: 4})
	seq := r2.Sequence()
	var wins int
	for _, p := range candidates {
		out := r2.JoinRoom(JoinRoomCommand{CommandID: CommandID("j-" + p), PlayerID: p, ExpectedSequence: seq})
		if out.Kind == OutcomeAccepted {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("exactly one join should win at a fixed sequence, got %d", wins)
	}
	if r2.Roster().OccupiedCount() != 2 { // host + one
		t.Fatalf("occupied=%d", r2.Roster().OccupiedCount())
	}
	_ = accepted
}

func TestConcurrentLockRace_OneWinner(t *testing.T) {
	r, _ := CreateRoom(CreateRoomCommand{CommandID: "c", RoomID: "lock-race", HostID: "host", MaxSeats: 4})
	join(t, r, "j1", "p2")
	seq := r.Sequence()

	first := r.LockRoom(LockRoomCommand{CommandID: "lock-a", ActorID: "host", ExpectedSequence: seq})
	second := r.LockRoom(LockRoomCommand{CommandID: "lock-b", ActorID: "host", ExpectedSequence: seq})
	if first.Kind != OutcomeAccepted || !HasFact(first.Facts, FactRoomLocked) {
		t.Fatalf("first=%+v", first)
	}
	if second.Kind != OutcomeRejected || second.Rejection.Code != RejectStaleSequence {
		t.Fatalf("second=%+v", second)
	}
	if len(second.Facts) != 0 {
		t.Fatal("loser emitted facts")
	}
}

func TestGoroutineSerializedAccess_DeterministicFinalHost(t *testing.T) {
	// Contract: external serialization (mutex) + domain rules => deterministic host.
	var mu sync.Mutex
	r, _ := CreateRoom(CreateRoomCommand{CommandID: "c", RoomID: "goro", HostID: "host", MaxSeats: 6})

	players := []PlayerID{"a", "b", "c", "d"}
	var wg sync.WaitGroup
	for i, p := range players {
		wg.Add(1)
		go func(i int, p PlayerID) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			_ = r.JoinRoom(JoinRoomCommand{
				CommandID:        CommandID("join-" + p),
				PlayerID:         p,
				ExpectedSequence: r.Sequence(),
			})
		}(i, p)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	out := r.LeaveRoom(LeaveRoomCommand{CommandID: "lh", PlayerID: "host", ExpectedSequence: r.Sequence()})
	if out.Kind != OutcomeAccepted {
		t.Fatal(out.Rejection)
	}
	// Lowest seat after host(0) is whoever joined into seat 1 first under mutex order — nondeterministic join order.
	// But host must equal LowestOccupiedSeat player.
	seat, ok := r.Roster().LowestOccupiedSeat()
	if !ok || r.HostID() != seat.PlayerID {
		t.Fatalf("host=%s lowest=%v", r.HostID(), seat)
	}
}

func TestRejectedCommandsNeverEmitFacts_Table(t *testing.T) {
	r := mustCreate(t, "host")
	cases := []struct {
		name string
		run  func() CommandOutcome
	}{
		{"stale", func() CommandOutcome {
			return r.JoinRoom(JoinRoomCommand{CommandID: "s", PlayerID: "x", ExpectedSequence: 0})
		}},
		{"future", func() CommandOutcome {
			return r.JoinRoom(JoinRoomCommand{CommandID: "f", PlayerID: "x", ExpectedSequence: 99})
		}},
		{"not host", func() CommandOutcome {
			join(t, r, "jtmp", "p2")
			return r.LockRoom(LockRoomCommand{CommandID: "nh", ActorID: "p2", ExpectedSequence: r.Sequence()})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.run()
			if out.Kind != OutcomeRejected {
				t.Fatalf("%+v", out)
			}
			if out.Facts != nil && len(out.Facts) != 0 {
				t.Fatalf("facts=%v", out.Facts)
			}
		})
	}
}
