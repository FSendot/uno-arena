package domain

import "testing"

func TestDuplicateCommandID_ReturnsStablePriorOutcomeWithoutReapply(t *testing.T) {
	r := mustCreateAdHoc(t, "dup-room", "host")
	seqBefore := r.Sequence()

	first := r.JoinRoom(JoinRoomCommand{
		CommandID:        "same-join",
		PlayerID:         "p2",
		ExpectedSequence: seqBefore,
	})
	assertAcceptedFacts(t, first, FactPlayerJoinedRoom)
	seqAfter := r.Sequence()
	occupied := r.Roster().OccupiedCount()

	second := r.JoinRoom(JoinRoomCommand{
		CommandID:        "same-join",
		PlayerID:         "p3", // different payload must not reapply
		ExpectedSequence: seqAfter,
	})
	if second.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s want duplicate", second.Kind)
	}
	if second.Rejection != nil {
		t.Fatalf("duplicate of accepted must not carry rejection: %+v", second.Rejection)
	}
	if !HasFact(second.Facts, FactPlayerJoinedRoom) {
		t.Fatalf("duplicate must return prior facts, got %v", FactNames(second.Facts))
	}
	if r.Sequence() != seqAfter || r.Roster().OccupiedCount() != occupied {
		t.Fatalf("state mutated on duplicate: seq=%d occupied=%d", r.Sequence(), r.Roster().OccupiedCount())
	}
	if r.Roster().IsSeated("p3") {
		t.Fatal("p3 must not be seated from duplicate replay")
	}
}

func TestDuplicateCommandID_RejectedOutcomeStable(t *testing.T) {
	r := waitingRoomWithTwo(t, "dup-reject", "host", "p2")
	mustLock(t, r, "host", false)
	seq := r.Sequence()

	first := r.JoinRoom(JoinRoomCommand{
		CommandID:        "bad-join",
		PlayerID:         "p3",
		ExpectedSequence: seq,
	})
	assertRejected(t, first, RejectJoinAfterLock)

	second := r.JoinRoom(JoinRoomCommand{
		CommandID:        "bad-join",
		PlayerID:         "p3",
		ExpectedSequence: seq,
	})
	if second.Kind != OutcomeDuplicate {
		t.Fatalf("kind=%s", second.Kind)
	}
	if second.Rejection == nil || second.Rejection.Code != RejectJoinAfterLock {
		t.Fatalf("prior rejection not stable: %+v", second.Rejection)
	}
	if len(second.Facts) != 0 {
		t.Fatalf("facts on rejected duplicate: %v", second.Facts)
	}
	if r.Sequence() != seq {
		t.Fatalf("sequence changed on duplicate reject")
	}
}

func TestDuplicateLock_IdempotentAfterSuccess(t *testing.T) {
	r := waitingRoomWithTwo(t, "dup-lock", "host", "p2")
	first := r.LockRoom(LockRoomCommand{
		CommandID:        "lock-1",
		ActorID:          "host",
		ExpectedSequence: r.Sequence(),
	})
	assertAcceptedFacts(t, first, FactRoomLocked)
	seq := r.Sequence()

	// Same command id
	dup := r.LockRoom(LockRoomCommand{
		CommandID:        "lock-1",
		ActorID:          "host",
		ExpectedSequence: seq,
	})
	if dup.Kind != OutcomeDuplicate || !HasFact(dup.Facts, FactRoomLocked) {
		t.Fatalf("%+v", dup)
	}
	if r.Sequence() != seq {
		t.Fatal("duplicate lock by command id mutated sequence")
	}

	// Different command id after already locked: accepted no-op
	noop := r.LockRoom(LockRoomCommand{
		CommandID:        "lock-2",
		ActorID:          "host",
		ExpectedSequence: seq,
	})
	if noop.Kind != OutcomeAccepted || len(noop.Facts) != 0 || r.Sequence() != seq {
		t.Fatalf("second lock command should no-op: %+v", noop)
	}
}

func TestSequence_StaleAndFutureRejected(t *testing.T) {
	r := mustCreateAdHoc(t, "seq-room", "host")
	current := r.Sequence()

	tests := []struct {
		name string
		seq  SequenceNumber
		code RejectionCode
	}{
		{"stale zero", 0, RejectStaleSequence},
		{"stale behind", current - 1, RejectStaleSequence},
		{"future", current + 1, RejectFutureSequence},
		{"far future", current + 99, RejectFutureSequence},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := r.Sequence()
			out := r.JoinRoom(JoinRoomCommand{
				CommandID:        CommandID("join-" + tt.name),
				PlayerID:         PlayerID("p-" + tt.name),
				ExpectedSequence: tt.seq,
			})
			assertRejected(t, out, tt.code)
			if out.Rejection.SubmittedSequence != tt.seq || out.Rejection.CurrentSequence != before {
				t.Fatalf("seq fields %+v want submitted=%d current=%d", out.Rejection, tt.seq, before)
			}
			if r.Sequence() != before {
				t.Fatal("sequence must not advance on rejection")
			}
			if r.Roster().IsSeated(PlayerID("p-" + tt.name)) {
				t.Fatal("player seated despite rejection")
			}
		})
	}
}

func TestSequence_ExactMatchAcceptedThenStaleLoses(t *testing.T) {
	r := mustCreateAdHoc(t, "seq-race", "host")
	seq := r.Sequence()

	win := r.JoinRoom(JoinRoomCommand{
		CommandID:        "winner",
		PlayerID:         "alice",
		ExpectedSequence: seq,
	})
	assertAcceptedFacts(t, win, FactPlayerJoinedRoom)

	lose := r.JoinRoom(JoinRoomCommand{
		CommandID:        "loser",
		PlayerID:         "bob",
		ExpectedSequence: seq, // stale relative to advanced room
	})
	assertRejected(t, lose, RejectStaleSequence)
	if r.Roster().IsSeated("bob") {
		t.Fatal("stale join must not seat bob")
	}
}

func TestRejectedCommands_NeverEmitFacts(t *testing.T) {
	r := mustCreateAdHoc(t, "no-facts", "host")
	out := r.LockRoom(LockRoomCommand{
		CommandID:        "lock-solo",
		ActorID:          "host",
		ExpectedSequence: r.Sequence(),
	})
	assertRejected(t, out, RejectInsufficientRoster)
	if out.Facts != nil && len(out.Facts) != 0 {
		t.Fatalf("facts=%v", out.Facts)
	}
}
