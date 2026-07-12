package app

import (
	"context"
	"testing"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/shared/envelope"
)

// Accepted no-ops (new command IDs, no facts / no sequence advance) must not
// emit player or spectator projection events or enqueue empty outbox rows that
// block later real-event drains.
func TestAcceptedNoOp_LockRoomStartMatch_NoDuplicateSpectator_LaterDrains(t *testing.T) {
	env := newReservationEnv(t)
	sv := envelope.CurrentSchemaVersion
	roomID := "room_noop_proj"
	ctx := context.Background()

	mustAccept(t, env.svc.HandleCommand(ctx, CommandInput{
		CommandID: "c", Type: CmdCreateRoom, RoomID: roomID, PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, Payload: mustRaw(map[string]any{"roomId": roomID}),
	}))
	mustAccept(t, env.svc.HandleCommand(ctx, CommandInput{
		CommandID: "j", Type: CmdJoinRoom, RoomID: roomID, PlayerID: "guest", SessionID: "s2",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(1),
	}))
	mustAccept(t, env.svc.HandleCommand(ctx, CommandInput{
		CommandID: "l1", Type: CmdLockRoom, RoomID: roomID, PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(2),
	}))

	specAfterLock := countStream(env.publisher.Events, StreamSpectator)
	playerAfterLock := countStream(env.publisher.Events, StreamPlayer)
	if specAfterLock < 1 {
		t.Fatal("expected spectator events after real LockRoom")
	}
	if pending := env.sessions.PendingOutboxLen(); pending != 0 {
		t.Fatalf("outbox should drain after real lock, pending=%d", pending)
	}
	lockSeq := currentSeq(t, env, roomID)

	// Repeated LockRoom with new command IDs: accepted no-ops.
	for i, id := range []string{"l2", "l3", "l4"} {
		res := env.svc.HandleCommand(ctx, CommandInput{
			CommandID: id, Type: CmdLockRoom, RoomID: roomID, PlayerID: "host", SessionID: "s",
			SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(lockSeq),
		})
		mustAccept(t, res)
		if res.Result.Sequence == nil || *res.Result.Sequence != lockSeq {
			t.Fatalf("noop lock[%d] sequence=%v want %d", i, res.Result.Sequence, lockSeq)
		}
		if currentSeq(t, env, roomID) != lockSeq {
			t.Fatalf("noop lock[%d] advanced room sequence", i)
		}
	}

	if got := countStream(env.publisher.Events, StreamSpectator); got != specAfterLock {
		t.Fatalf("noop LockRoom emitted spectator events: before=%d after=%d", specAfterLock, got)
	}
	if got := countStream(env.publisher.Events, StreamPlayer); got != playerAfterLock {
		t.Fatalf("noop LockRoom emitted player events: before=%d after=%d", playerAfterLock, got)
	}
	if pending := env.sessions.PendingOutboxLen(); pending != 0 {
		t.Fatalf("noop LockRoom left pending/empty outbox entries: %d", pending)
	}

	// Real StartMatch must advance spectator sequence and drain despite prior no-ops.
	mustAccept(t, env.svc.HandleCommand(ctx, CommandInput{
		CommandID: "st1", Type: CmdStartMatch, RoomID: roomID, PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(lockSeq),
		Payload: mustRaw(map[string]any{"gameId": "g1"}),
	}))
	startSeq := currentSeq(t, env, roomID)
	if startSeq != lockSeq+1 {
		t.Fatalf("start seq=%d want %d", startSeq, lockSeq+1)
	}
	if got := countStream(env.publisher.Events, StreamSpectator); got != specAfterLock+1 {
		t.Fatalf("spectator after StartMatch=%d want %d", got, specAfterLock+1)
	}
	specSeqs := spectatorSequences(env.publisher.Events)
	if len(specSeqs) < 2 {
		t.Fatalf("spectator sequences=%v", specSeqs)
	}
	last := specSeqs[len(specSeqs)-1]
	prev := specSeqs[len(specSeqs)-2]
	if last != prev+1 {
		t.Fatalf("spectator not contiguous after StartMatch: %d then %d (all=%v)", prev, last, specSeqs)
	}
	if last != startSeq {
		t.Fatalf("last spectator seq=%d want room seq %d", last, startSeq)
	}
	if pending := env.sessions.PendingOutboxLen(); pending != 0 {
		t.Fatalf("StartMatch outbox did not drain: pending=%d", pending)
	}

	// LockRoom again while in_progress: accepted no-op with new command IDs.
	specBeforeRelock := countStream(env.publisher.Events, StreamSpectator)
	for _, id := range []string{"l5", "l6"} {
		res := env.svc.HandleCommand(ctx, CommandInput{
			CommandID: id, Type: CmdLockRoom, RoomID: roomID, PlayerID: "host", SessionID: "s",
			SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(startSeq),
		})
		mustAccept(t, res)
	}
	if got := countStream(env.publisher.Events, StreamSpectator); got != specBeforeRelock {
		t.Fatalf("in_progress noop LockRoom emitted spectator events")
	}
	if pending := env.sessions.PendingOutboxLen(); pending != 0 {
		t.Fatalf("in_progress noop Lock left pending outbox: %d", pending)
	}

	// New StartMatch command IDs reject once started — no projections, no outbox block.
	dup := env.svc.HandleCommand(ctx, CommandInput{
		CommandID: "st2", Type: CmdStartMatch, RoomID: roomID, PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(startSeq),
		Payload: mustRaw(map[string]any{"gameId": "g2"}),
	})
	if dup.Err != nil {
		t.Fatal(dup.Err)
	}
	if dup.Result.Status != envelope.StatusRejected {
		t.Fatalf("duplicate StartMatch status=%s want rejected", dup.Result.Status)
	}

	// Later real event (disconnect) still drains after no-ops.
	mustAccept(t, env.svc.HandleCommand(ctx, CommandInput{
		CommandID: "disc", Type: CmdDisconnectPlayer, RoomID: roomID, PlayerID: "guest", SessionID: "s2",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(startSeq),
	}))
	if pending := env.sessions.PendingOutboxLen(); pending != 0 {
		t.Fatalf("DisconnectPlayer outbox did not drain after no-ops: pending=%d", pending)
	}

	specSeqs = spectatorSequences(env.publisher.Events)
	seen := map[int64]int{}
	for _, s := range specSeqs {
		seen[s]++
		if seen[s] > 1 {
			t.Fatalf("duplicate spectator sequence %d in %v", s, specSeqs)
		}
	}
	for i := 1; i < len(specSeqs); i++ {
		if specSeqs[i] != specSeqs[i-1]+1 {
			t.Fatalf("spectator sequences not contiguous: %v", specSeqs)
		}
	}
}

func TestBuildFeedEvents_AcceptedNoOp_NoSpectator(t *testing.T) {
	room, _ := domain.CreateRoom(domain.CreateRoomCommand{
		CommandID: "c0", RoomID: "room_bf_noop", HostID: "host", MaxSeats: 4,
	})
	_ = room.JoinRoom(domain.JoinRoomCommand{CommandID: "c1", PlayerID: "guest", ExpectedSequence: 1})
	_ = room.LockRoom(domain.LockRoomCommand{CommandID: "l1", ActorID: "host", ExpectedSequence: 2})
	noop := room.LockRoom(domain.LockRoomCommand{CommandID: "l2", ActorID: "host", ExpectedSequence: 3})
	if noop.Kind != domain.OutcomeAccepted || len(noop.Facts) != 0 {
		t.Fatalf("expected accepted no-op lock: %+v", noop)
	}
	sess := domain.OpenSession(room)
	prev := int64(noop.Sequence) // unchanged by no-op
	evs, high := BuildFeedEvents(sess, int64(noop.Sequence), 1, "corr", "l2", noop.Facts, []FeedAudience{
		{PlayerID: "host", SessionID: "s"},
	}, prev, time.Unix(1, 0).UTC())
	if len(evs) != 0 || high != 0 {
		t.Fatalf("no-op must emit no feed events: evs=%d high=%d", len(evs), high)
	}
}

func countStream(events []PublishedEvent, stream string) int {
	n := 0
	for _, ev := range events {
		if ev.Stream == stream {
			n++
		}
	}
	return n
}

func spectatorSequences(events []PublishedEvent) []int64 {
	var out []int64
	for _, ev := range events {
		if ev.Stream == StreamSpectator {
			out = append(out, ev.SequenceNumber)
		}
	}
	return out
}
