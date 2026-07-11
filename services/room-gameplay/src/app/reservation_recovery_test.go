package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"unoarena/services/room-gameplay/domain"
	"unoarena/services/room-gameplay/game"
	"unoarena/shared/envelope"
)

func TestReservationRecovery_StableOperationIDNotProcessCounter(t *testing.T) {
	deals := NewFakeDealSource()
	ctx := context.Background()
	op := StableOperationID("cmd-abc", PurposeDeal)
	r1, err := deals.ReserveDeal(ctx, "r1", "g1", op, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	_ = deals.Cancel(ctx, r1.ID)
	r2, err := deals.ReserveDeal(ctx, "r1", "g1", op, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.ID != r2.ID {
		t.Fatalf("stable op must reuse reservation id: %q vs %q", r1.ID, r2.ID)
	}
	if r1.ID == "deal-res-1" || r1.ID == "deal-res-2" {
		t.Fatalf("must not use process counter id %q", r1.ID)
	}
}

func TestReservationRecovery_LostReserveResponseReusesMaterial(t *testing.T) {
	deals := NewFakeDealSource()
	ctx := context.Background()
	op := StableOperationID("cmd-lost-res", PurposeDraw)
	first, err := deals.ReserveDraw(ctx, "r1", "g1", op, 2)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := deals.ReserveDraw(ctx, "r1", "g1", op, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != retry.ID {
		t.Fatalf("id %q vs %q", first.ID, retry.ID)
	}
	if len(first.Cards) != 2 || len(retry.Cards) != 2 || first.Cards[0].ID != retry.Cards[0].ID {
		t.Fatalf("material mismatch: %+v vs %+v", first.Cards, retry.Cards)
	}
}

func TestReservationRecovery_ConfirmFailureNoLocalCommitThenRetry(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)
	env.deals.FailConfirm = errors.New("confirm down")

	seq := currentSeq(t, env, "room_rec")

	res := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-1", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if res.Err == nil {
		t.Fatal("confirm failure must surface as error")
	}
	live, ok := env.sessions.Get(domain.RoomID("room_rec"))
	if !ok {
		t.Fatal("missing room")
	}
	if _, prior := live.Room().PriorOutcome("draw-1"); prior {
		t.Fatal("PriorOutcome must not exist before local commit")
	}
	if env.deals.PendingLen() != 1 {
		t.Fatalf("draw reservation must remain pending after confirm failure, pending=%d", env.deals.PendingLen())
	}

	env.deals.FailConfirm = nil
	retry := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-1", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if retry.Err != nil {
		t.Fatalf("retry: %v", retry.Err)
	}
	if retry.Result.Status != envelope.StatusAccepted {
		t.Fatalf("retry status=%v", retry.Result.Status)
	}
	live, _ = env.sessions.Get(domain.RoomID("room_rec"))
	if _, prior := live.Room().PriorOutcome("draw-1"); !prior {
		t.Fatal("PriorOutcome must exist after successful local commit")
	}
}

func TestReservationRecovery_LocalCommitFailureAfterConfirmThenRetry(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)
	seq := currentSeq(t, env, "room_rec")

	env.sessions.FailCommit = errors.New("commit boom")
	res := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-2", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if res.Err == nil {
		t.Fatal("commit failure must surface")
	}
	live, _ := env.sessions.Get(domain.RoomID("room_rec"))
	if _, prior := live.Room().PriorOutcome("draw-2"); prior {
		t.Fatal("PriorOutcome must not exist when local commit failed")
	}
	if env.deals.ConfirmedLen() < 2 {
		t.Fatalf("reservation must stay confirmed after commit failure, confirmed=%d", env.deals.ConfirmedLen())
	}

	env.sessions.FailCommit = nil
	retry := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-2", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if retry.Err != nil {
		t.Fatalf("retry: %v", retry.Err)
	}
	if retry.Result.Status != envelope.StatusAccepted {
		t.Fatalf("retry %+v", retry.Result)
	}
}

func TestReservationRecovery_LostConfirmResponseRetry(t *testing.T) {
	deals := NewFakeDealSource()
	ctx := context.Background()
	op := StableOperationID("cmd-lost-c", PurposeDraw)
	res, err := deals.ReserveDraw(ctx, "r1", "g1", op, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := deals.Confirm(ctx, res.ID); err != nil {
		t.Fatal(err)
	}
	// Lost confirm response: Confirm again with original id.
	if err := deals.Confirm(ctx, res.ID); err != nil {
		t.Fatalf("idempotent confirm: %v", err)
	}
}

func TestReservationRecovery_ConcurrentRoomsNoIDCollision(t *testing.T) {
	deals := NewFakeDealSource()
	ctx := context.Background()
	const n = 32
	ids := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			room := "room-" + itoa(i)
			op := StableOperationID("same-cmd", PurposeDraw)
			res, err := deals.ReserveDraw(ctx, room, "g1", op, 1)
			if err != nil {
				t.Errorf("%s: %v", room, err)
				return
			}
			ids[i] = res.ID
		}()
	}
	wg.Wait()
	seen := map[string]int{}
	for i, id := range ids {
		if id == "" {
			t.Fatalf("empty at %d", i)
		}
		if prev, ok := seen[id]; ok {
			t.Fatalf("collision %d and %d: %q", prev, i, id)
		}
		seen[id] = i
	}
}

func TestReservationRecovery_CancelThenNewReservationCorrectCards(t *testing.T) {
	deals := NewFakeDealSource()
	ctx := context.Background()
	r1, err := deals.ReserveDraw(ctx, "r1", "g1", StableOperationID("c1", PurposeDraw), 2)
	if err != nil {
		t.Fatal(err)
	}
	cards1 := append([]game.Card(nil), r1.Cards...)
	if err := deals.Cancel(ctx, r1.ID); err != nil {
		t.Fatal(err)
	}
	r2, err := deals.ReserveDraw(ctx, "r1", "g1", StableOperationID("c2", PurposeDraw), 2)
	if err != nil {
		t.Fatal(err)
	}
	if r2.ID == r1.ID {
		t.Fatal("new op must get distinct reservation id")
	}
	if len(r2.Cards) != 2 || r2.Cards[0].ID != cards1[0].ID || r2.Cards[1].ID != cards1[1].ID {
		t.Fatalf("cancel must not shift cards: got %+v want %+v", r2.Cards, cards1)
	}
}

func TestReservationRecovery_AppendIncludesReservedMaterial(t *testing.T) {
	env := newReservationEnv(t)
	setupTwoPlayerMatch(t, env)
	seq := currentSeq(t, env, "room_rec")

	res := env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "draw-mat", Type: CmdDrawCard, RoomID: "room_rec",
		SchemaVersion: envelope.CurrentSchemaVersion,
		PlayerID:      "host", SessionID: "s", ExpectedSequenceNumber: int64Ptr(seq),
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if env.integrity.Len() < 2 {
		t.Fatal("expected append")
	}
	last := env.integrity.Last()
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["reservationId"] == nil || payload["reservationId"] == "" {
		t.Fatalf("append missing reservationId: %+v", payload)
	}
	cards, ok := payload["cards"].([]any)
	if !ok || len(cards) < 1 {
		t.Fatalf("append missing card identities: %+v", payload)
	}
}

type reservationEnv struct {
	svc       *Service
	sessions  *MemorySessionRepository
	deals     *FakeDealSource
	integrity *FakeGameIntegrity
	publisher *FakeEventPublisher
}

func newReservationEnv(t *testing.T) *reservationEnv {
	t.Helper()
	sessions := NewMemorySessionRepository()
	deals := NewFakeDealSource()
	integrity := NewFakeGameIntegrity()
	publisher := &FakeEventPublisher{}
	svc := NewService(ServiceDeps{
		Sessions: sessions, Integrity: integrity, Publisher: publisher,
		SessionsV: AllowAllSessionValidator{},
		Audit:     &FakeAuditSink{}, Deals: deals, Clock: NewFixedClock(time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)),
	})
	return &reservationEnv{svc: svc, sessions: sessions, deals: deals, integrity: integrity, publisher: publisher}
}

func setupTwoPlayerMatch(t *testing.T, env *reservationEnv) {
	t.Helper()
	sv := envelope.CurrentSchemaVersion
	mustAccept(t, env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "c", Type: CmdCreateRoom, RoomID: "room_rec", PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, Payload: mustRaw(map[string]any{"roomId": "room_rec"}),
	}))
	mustAccept(t, env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "j", Type: CmdJoinRoom, RoomID: "room_rec", PlayerID: "guest", SessionID: "s2",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(1),
	}))
	mustAccept(t, env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "l", Type: CmdLockRoom, RoomID: "room_rec", PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(2),
	}))
	mustAccept(t, env.svc.HandleCommand(context.Background(), CommandInput{
		CommandID: "st", Type: CmdStartMatch, RoomID: "room_rec", PlayerID: "host", SessionID: "s",
		SchemaVersion: sv, ExpectedSequenceNumber: int64Ptr(3),
		Payload: mustRaw(map[string]any{"gameId": "g1"}),
	}))
}

func mustAccept(t *testing.T, res CommandResult) {
	t.Helper()
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.Result.Status != envelope.StatusAccepted {
		t.Fatalf("want accepted, got %+v", res.Result)
	}
}

func currentSeq(t *testing.T, env *reservationEnv, roomID string) int64 {
	t.Helper()
	live, ok := env.sessions.Get(domain.RoomID(roomID))
	if !ok {
		t.Fatal("room missing")
	}
	return int64(live.Room().Sequence())
}

func int64Ptr(v int64) *int64 { return &v }

func mustRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
