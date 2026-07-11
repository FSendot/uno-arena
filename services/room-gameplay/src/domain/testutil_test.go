package domain

import (
	"testing"
	"time"
)

func mustCreateAdHoc(t *testing.T, roomID RoomID, host PlayerID) *Room {
	t.Helper()
	r, out := CreateRoom(CreateRoomCommand{
		CommandID: CommandID("create-" + string(roomID)),
		RoomID:    roomID,
		HostID:    host,
	})
	if r == nil || out.Kind != OutcomeAccepted {
		t.Fatalf("CreateRoom failed: %+v", out)
	}
	return r
}

func mustJoin(t *testing.T, r *Room, cmdID CommandID, player PlayerID) {
	t.Helper()
	out := r.JoinRoom(JoinRoomCommand{
		CommandID:        cmdID,
		PlayerID:         player,
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("JoinRoom(%s) failed: %+v", player, out)
	}
}

func mustLock(t *testing.T, r *Room, actor PlayerID, asSystem bool) {
	t.Helper()
	out := r.LockRoom(LockRoomCommand{
		CommandID:        CommandID("lock-" + string(r.ID()) + "-" + string(actor)),
		ActorID:          actor,
		AsSystem:         asSystem,
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("LockRoom failed: %+v", out)
	}
}

func mustStart(t *testing.T, r *Room, actor PlayerID, gameID GameID, asSystem bool) {
	t.Helper()
	out := r.StartMatch(StartMatchCommand{
		CommandID:        CommandID("start-" + string(r.ID()) + "-" + string(gameID)),
		ActorID:          actor,
		AsSystem:         asSystem,
		GameID:           gameID,
		ExpectedSequence: r.Sequence(),
	})
	if out.Kind != OutcomeAccepted {
		t.Fatalf("StartMatch failed: %+v", out)
	}
}

func waitingRoomWithTwo(t *testing.T, roomID RoomID, host, guest PlayerID) *Room {
	t.Helper()
	r := mustCreateAdHoc(t, roomID, host)
	mustJoin(t, r, CommandID("join-"+string(guest)), guest)
	return r
}

func inProgressRoom(t *testing.T, roomID RoomID, host, guest PlayerID, gameID GameID) *Room {
	t.Helper()
	r := waitingRoomWithTwo(t, roomID, host, guest)
	mustLock(t, r, host, false)
	mustStart(t, r, host, gameID, false)
	return r
}

func factData(facts []Fact, name FactName, key string) (string, bool) {
	for _, f := range facts {
		if f.Name == name {
			v, ok := f.Data[key]
			return v, ok
		}
	}
	return "", false
}

func assertRejected(t *testing.T, out CommandOutcome, code RejectionCode) {
	t.Helper()
	if out.Kind != OutcomeRejected {
		t.Fatalf("expected rejected, got kind=%s facts=%v", out.Kind, FactNames(out.Facts))
	}
	if out.Rejection == nil || out.Rejection.Code != code {
		t.Fatalf("expected code %s, got %+v", code, out.Rejection)
	}
	if len(out.Facts) != 0 {
		t.Fatalf("rejected command must not emit facts, got %v", FactNames(out.Facts))
	}
}

func assertAcceptedFacts(t *testing.T, out CommandOutcome, names ...FactName) {
	t.Helper()
	if out.Kind != OutcomeAccepted {
		t.Fatalf("expected accepted, got %+v", out)
	}
	got := FactNames(out.Facts)
	if len(got) != len(names) {
		t.Fatalf("facts=%v want %v", got, names)
	}
	for i := range names {
		if got[i] != names[i] {
			t.Fatalf("facts[%d]=%s want %s (all=%v)", i, got[i], names[i], got)
		}
	}
}

var testNow = time.Date(2026, 7, 10, 15, 0, 0, 0, time.UTC)
